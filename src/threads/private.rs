// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::time::Instant;

use constellation_auth::authn::MsgAuthNTypes;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::net::PrivateMsgs;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;
use mio::Token;

use crate::config::PrivateDatagramModeConfig;
use crate::config::PrivateLargeObjModeConfig;
use crate::frags::Frags;
use crate::large_obj::LargeObjMsg;
use crate::large_obj::LargeObjMsgs;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjSendError;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::threads::LargeObjEntry;
use crate::threads::PushMode;

pub trait PrivateLargeObjPushModeTypes<Ctx> {
    type Frags: Frags;
    type BatchID: Clone;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq + Send;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddErrorCompletable: ScopedError;
    type AddError: RecoverableError<Completable = Self::AddErrorCompletable>;
    type FinishBatchErrorCompletable: ScopedError;
    type FinishBatchError: RecoverableError<Completable = Self::FinishBatchErrorCompletable>;
    type StartBatchErrorCompletable: ScopedError;
    type StartBatchError: RecoverableError<Completable = Self::StartBatchErrorCompletable>;
    type PushFragError: RecoverableError;
    type PushOfferError: RecoverableError;
    type Stream: PushStreamReportBatchError<
            <Self::FinishBatchError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::PushFragError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::PushOfferError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Self::AddError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamPrivate<Ctx, StartBatchError = Self::StartBatchError>
        + PushStreamAdd<LargeObjMsg<Self::HashID>, Ctx, AddError = Self::AddError>
        + PushStream<
            Ctx,
            BatchID = Self::BatchID,
            FinishBatchError = Self::FinishBatchError
        > + LargeObjOfferStream<
            Self::HashID,
            Ctx,
            PushOfferError = Self::PushOfferError
        > + LargeObjStream<
            Ctx,
            Frags = Self::Frags,
            PushFragError = Self::PushFragError
        > + Send;
}

/// Backlog entry for push threads.
///
/// This records the state of a partially-completed operation.
enum PushEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send,
    Msg: Clone + Send {
    Batch {
        msgs: Vec<Msg>,
        retry: Stream::StartBatchRetry
    },
    Abort {
        flags: Stream::StreamFlags,
        retry: Stream::AbortBatchRetry
    },
    Add {
        msgs: Vec<Msg>,
        msg: Msg,
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::AddRetry
    },
    Finish {
        batch: Stream::BatchID,
        retry: Stream::FinishBatchRetry
    },
    Cancel {
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::CancelBatchRetry
    }
}

struct IndefEntry<Msg> {
    /// When the messages were originally sent; used for timeouts.
    origin: Instant,
    /// The messages to send.
    msgs: Vec<Msg>
}

pub struct PrivateDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send,
    Msg: Clone + Send {
    /// Buffer for sends in progress.
    pending: Vec<PushEntry<Msg, Stream, Ctx>>,
    /// Pending operations that stalled with `WouldBlock`
    completes: Vec<PushEntryRecoverableError<
        Vec<Msg>,
        Stream::BatchID,
        Stream::StreamFlags,
        Msg,
        <Stream::StartBatchError as RecoverableError>::Completable,
        <Stream::AddError as RecoverableError>::Completable,
        <Stream::FinishBatchError as RecoverableError>::Completable
    >>,
    /// Pending operations that produced indefinite waits.
    indefs: Option<Vec<IndefEntry<Msg>>>
}

pub struct PrivateLargeObjPushMode<Types, Ctx>
where
    Types: PrivateLargeObjPushModeTypes<Ctx> {
    /// Buffer for sends in progress.
    pending_msgs:
        Vec<PushEntry<LargeObjMsg<Types::HashID>, Types::Stream, Ctx>>,
    pending_frags: Vec<LargeObjEntry<Types::Stream, Types::Hash, Ctx>>
}

#[derive(Clone)]
pub struct PrivatePrin;

#[derive(Debug)]
pub enum PrivateLargeObjPushModeSendError<Frags, Msgs> {
    Frags { err: Frags },
    Msgs { err: Msgs }
}

enum PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch, Add, Finish> {
    Batch {
        /// Messages to be sent.
        msgs: Msgs,
        /// Error that occurred starting the batch.
        err: Batch
    },
    Add {
        batch_id: ID,
        flags: Flags,
        msgs: Msgs,
        msg: Msg,
        err: Add
    },
    Finish {
        batch_id: ID,
        flags: Flags,
        err: Finish
    }
}

/// Type of permanent errors that can occur creating and sending a batch.
#[derive(Debug)]
pub enum PushEntryError<Batch, Add, Finish> {
    /// An error occurred creating the batch.
    Batch {
        /// The error that occurred creating the batch.
        err: Batch
    },
    /// An error occurred adding messages.
    Add {
        /// The error that occurred adding messages.
        err: Add
    },
    /// An error occurred finishing the batch.
    Finish {
        /// The error that occurred finishing the batch.
        err: Finish
    }
}

impl<Msg, Stream, Ctx> RetryWhen for PushEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        > + Send,
    Msg: Clone + Send
{
    fn when(&self) -> Instant {
        match self {
            PushEntry::Batch { retry, .. } => retry.when(),
            PushEntry::Abort { retry, .. } => retry.when(),
            PushEntry::Add { retry, .. } => retry.when(),
            PushEntry::Finish { retry, .. } => retry.when(),
            PushEntry::Cancel { retry, .. } => retry.when()
        }
    }
}

impl<Msg, Stream, Ctx> PushEntry<Msg, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send,
    Msg: 'static + Clone + Send
{
    fn complete_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::CancelBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while cancelling message");

        match err.split() {
            (Some(completable), None) => match stream.complete_cancel_batch(
                ctx,
                &mut flags,
                &batch_id,
                completable
            ) {
                // It succeeded.
                Ok(RetryResult::Success(_)) => {
                    trace!(target: "push-entry",
                       "successfully completed cancellation");

                    RetryResult::Success(())
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Cancel {
                        batch: batch_id,
                        retry: retry,
                        flags: flags
                    })
                }
                // More errors; recurse again.
                Err(err) => Self::complete_cancel_batch(
                    ctx, stream, flags, batch_id, err
                )
            },
            // Unrecoverable errors occurred canceling the batch.
            (_, Some(permanent)) => {
                error!(target: "push-entry",
                       "unrecoverable error canceling batch: {}",
                       permanent);

                RetryResult::Success(())
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.cancel_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Cancel {
                    batch: batch_id,
                    retry: retry,
                    flags: flags
                })
            }
            Err(err) => {
                Self::complete_cancel_batch(ctx, stream, flags, batch_id, err)
            }
        }
    }

    fn complete_finish(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: <Stream::FinishBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        trace!(target: "push-entry",
               "attempting to recover from error while finishing batch");

        match stream.complete_finish_batch(ctx, &mut flags, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(())) => {
                trace!(target: "push-entry",
                       "successfully finished batch");

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Finish {
                    batch: batch_id,
                    retry: retry
                }))
            }
            Err(err) => Err(PushEntryRecoverableError::Finish {
                batch_id: batch_id,
                flags: flags,
                err: err
            })
        }
    }

    fn try_finish_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        let mut flags = stream.empty_flags();

        match stream.finish_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => Ok(RetryResult::Success(())),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Finish {
                    batch: batch_id.clone(),
                    retry: retry
                }))
            }
            Err(err) => Err(PushEntryRecoverableError::Finish {
                batch_id: batch_id,
                flags: flags,
                err: err
            })
        }
    }

    fn complete_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID,
        err: <Stream::AddError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        trace!(target: "push-entry",
               "attempting to recover from error while adding message");

        match stream.complete_add(ctx, &mut flags, &msg, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(())) => {
                trace!(target: "push-entry",
                       "successfully added message");

                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Add {
                    msgs: msgs,
                    msg: msg,
                    flags: flags,
                    batch: batch_id,
                    retry: retry
                }))
            }
            Err(err) => Err(PushEntryRecoverableError::Add {
                batch_id: batch_id,
                flags: flags,
                msgs: msgs,
                msg: msg,
                err: err
            })
        }
    }

    fn try_add_msg(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        match stream.add(ctx, &mut flags, &msg, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Add {
                    msgs: msgs,
                    msg: msg,
                    flags: flags,
                    batch: batch_id,
                    retry: retry
                }))
            }
            Err(err) => Err(PushEntryRecoverableError::Add {
                batch_id: batch_id,
                flags: flags,
                msgs: msgs,
                msg: msg,
                err: err
            })
        }
    }

    fn try_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        match msgs.pop() {
            Some(msg) => {
                let flags = stream.empty_flags();

                Self::try_add_msg(ctx, stream, flags, msgs, msg, batch_id)
            }
            None => Self::try_finish_batch(ctx, stream, batch_id)
        }
    }

    fn complete_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>,
        err: <Stream::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(), Self, Vec<Msg>>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        trace!(target: "push-entry",
               "attempting to recover from error while creating batch");

        match stream.complete_start_batch(ctx, err) {
            // It succeeded.
            Ok(RetryIndefResult::Success(batch_id)) => {
                trace!(target: "push-entry",
                       "successfully created batch");

                Self::try_add(ctx, stream, msgs, batch_id)
                    .map(RetryIndefResult::from)
            }
            // We got a retry.
            Ok(RetryIndefResult::Retry(retry)) => {
                Ok(RetryIndefResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    retry: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) =>
                Ok(RetryIndefResult::Indef(msgs)),
            Err(err) => Err(PushEntryRecoverableError::Batch {
                msgs: msgs,
                err: err
            })
        }
    }

    fn try_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>
    ) -> Result<
        RetryIndefResult<(), Self, Vec<Msg>>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        match stream.start_batch(ctx) {
            // It succeeded.
            Ok(RetryIndefResult::Success(batch_id)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
                    .map(RetryIndefResult::from)
            }
            // We got a retry.
            Ok(RetryIndefResult::Retry(retry)) => {
                Ok(RetryIndefResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    retry: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) =>
                Ok(RetryIndefResult::Indef(msgs)),
            Err(err) => Err(PushEntryRecoverableError::Batch {
                msgs: msgs,
                err: err
            })
        }
    }

    fn complete(
        ctx: &mut Ctx,
        stream: &mut Stream,
        err: PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            <Stream::StartBatchError as RecoverableError>::Completable,
            <Stream::AddError as RecoverableError>::Completable,
            <Stream::FinishBatchError as RecoverableError>::Completable
        >
    ) -> Result<
        RetryIndefResult<(), Self, Vec<Msg>>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        match err {
            PushEntryRecoverableError::Batch { msgs, err } =>
                Self::complete_start_batch(ctx, stream, msgs, err),
            PushEntryRecoverableError::Add { batch_id, flags, msgs, msg, err } =>
                Self::complete_add(ctx, stream, flags, msgs, msg, batch_id, err)
                .map(RetryIndefResult::from),
            PushEntryRecoverableError::Finish { batch_id, err, flags } =>
                Self::complete_finish(ctx, stream, flags, batch_id, err)
                .map(RetryIndefResult::from)
        }
    }

    fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> Result<
        RetryIndefResult<(), Self, Vec<Msg>>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        match self {
            PushEntry::Batch { msgs, retry } => match stream
                .retry_start_batch(ctx, retry) {
                // It succeeded.
                Ok(RetryIndefResult::Success(batch_id)) => {
                    Self::try_add(ctx, stream, msgs, batch_id)
                        .map(RetryIndefResult::from)
                }
                // We got a retry.
                Ok(RetryIndefResult::Retry(retry)) => {
                    Ok(RetryIndefResult::Retry(PushEntry::Batch {
                        msgs: msgs,
                        retry: retry
                    }))
                }
                Ok(RetryIndefResult::Indef(())) =>
                   Ok(RetryIndefResult::Indef(msgs)),
                Err(err) => Err(PushEntryRecoverableError::Batch {
                    msgs: msgs,
                    err: err
                })
            },
            PushEntry::Abort { mut flags, retry } =>
                Ok(RetryIndefResult::from(stream
                                          .retry_abort_start_batch(ctx, &mut flags, retry)
                                          .map_retry(|retry| PushEntry::Abort {
                                              flags: flags,
                                              retry: retry
                                          }))),
            PushEntry::Add {
                msgs,
                msg,
                batch,
                mut flags,
                retry
            } => match stream.retry_add(ctx, &mut flags, &msg, &batch, retry) {
                // It succeeded.
                Ok(RetryResult::Success(_)) => {
                    Self::try_add(ctx, stream, msgs, batch)
                        .map(RetryIndefResult::from)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    Ok(RetryIndefResult::Retry(PushEntry::Add {
                        msgs: msgs,
                        msg: msg,
                        flags: flags,
                        batch: batch,
                        retry: retry
                    }))
                }
                Err(err) => Err(PushEntryRecoverableError::Add {
                    batch_id: batch,
                    flags: flags,
                    msgs: msgs,
                    msg: msg,
                    err: err
                })
            },
            PushEntry::Finish { batch, retry } => {
                let mut flags = stream.empty_flags();

                match stream
                    .retry_finish_batch(ctx, &mut flags, &batch, retry) {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) =>
                        Ok(RetryIndefResult::Success(())),
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        Ok(RetryIndefResult::Retry(PushEntry::Finish {
                            batch: batch,
                            retry: retry
                        }))
                    }
                    Err(err) => Err(PushEntryRecoverableError::Finish {
                        batch_id: batch,
                        flags: flags,
                        err: err
                    })
                }
            }
            PushEntry::Cancel {
                batch,
                retry,
                mut flags
            } => match stream.retry_cancel_batch(ctx, &mut flags, &batch, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(_)) =>
                    Ok(RetryIndefResult::Success(())),
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    Ok(RetryIndefResult::Retry(PushEntry::Cancel {
                        batch: batch,
                        retry: retry,
                        flags: flags
                    }))
                }
                Err(err) => {
                    Ok(RetryIndefResult::from(
                        Self::complete_cancel_batch(ctx, stream, flags,
                                                    batch, err)
                    ))
                }
            }
        }
    }

    #[inline]
    fn try_send(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>
    ) -> Result<
        RetryIndefResult<(), Self, Vec<Msg>>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    > {
        Self::try_start_batch(ctx, stream, msgs)
    }
}

impl<Msg, Stream, Ctx> PrivateDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    Msg: 'static + Clone + Send
{
    fn handle_error(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        err: PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError
        >
    ) -> Option<Instant> {
        let (completable, permanent) = err.split();

        if let Some(permanent) = permanent {
            error!(target: "private-datagram-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);
        }

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                self.completes.push(completable);

                None
            } else {
                match PushEntry::complete(ctx, stream, completable) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        let ent = IndefEntry {
                            origin: Instant::now(),
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.indefs {
                            error!(target: "private-datagram-push-mode",
                                   "indefs should be empty");

                            indefs.push(ent)
                        } else {
                            self.indefs = Some(vec![ent])
                        }

                        None
                    }
                    // Error occurred.
                    Err(err) => {
                        self.handle_error(ctx, stream, err);

                        None
                    }
                }
            }
        } else {
            None
        }
    }
}

impl<Msg, Msgs, Stream, Ctx> PushMode<Stream, Msgs, Ctx>
    for PrivateDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        >
        + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    Msgs: 'static + PrivateMsgs<Msg> + Send,
    Msg: 'static + Clone + Send
{
    type Config = PrivateDatagramModeConfig;
    type CreateError = Infallible;
    type RetryError = Infallible;
    type SendError = Msgs::MsgsError;
    type RetryIndefError = Infallible;

    fn create(
        _stream: &Stream,
        config: Self::Config
    ) -> Result<Self, Self::CreateError> {
        let retries_hint = config.take();

        match retries_hint {
            Some(hint) => Ok(PrivateDatagramPushMode {
                pending: Vec::with_capacity(hint),
                completes: Vec::with_capacity(hint),
                indefs: None
            }),
            None => Ok(PrivateDatagramPushMode {
                pending: Vec::new(),
                completes: Vec::new(),
                indefs: None
            })
        }
    }

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>
    ) -> Result<Option<Instant>, Self::SendError> {
        if self.indefs.is_none() {
            debug!(target: "private-datagram-push-mode",
                   "fetching new outbound messages");

            let (msgs, next) = msgs.msgs()?;

            let retry = if let Some(msgs) = msgs {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        let ent = IndefEntry {
                            origin: Instant::now(),
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.indefs {
                            error!(target: "private-datagram-push-mode",
                                   "indefs should be empty");

                            indefs.push(ent)
                        } else {
                            self.indefs = Some(vec![ent])
                        }

                        None
                    }
                    // Error occurred.
                    Err(err) => self.handle_error(ctx, stream, err),
                }
            } else {
                None
            };

            let next = next.map_or(retry, |next| {
                Some(retry.map_or(next, |retry: Instant| retry.min(next)))
            });

            Ok(next)
        } else {
            Ok(None)
        }
    }

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError> {
        debug!(target: "private-datagram-push-mode",
               "retrying pending operations");

        let mut curr = Vec::with_capacity(self.pending.len());

        // ISSUE #29: Use a better data structure to avoid sorting
        // this array over and over.

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.pending
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        // Go through the sorted pending items and get all the ones
        // whose times are less than the present.
        while self.pending.last().is_some_and(|ent| now > ent.when()) {
            debug!(target: "private-datagram-push-mode",
                   "retrying pending operation");

            match self.pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "private-datagram-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let mut out = self.pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream) {
                // Send succeeded; nothing to do.
                Ok(RetryIndefResult::Success(())) => {}
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    let when = retry.when();

                    self.pending.push(retry);
                    out = Some(out.map_or(when, |curr| curr.max(when)));
                },
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(msgs)) => {
                    let ent = IndefEntry {
                        origin: Instant::now(),
                        msgs: msgs
                    };

                    if let Some(indefs) = &mut self.indefs {
                        error!(target: "private-datagram-push-mode",
                               "indefs should be empty");

                        indefs.push(ent)
                    } else {
                        self.indefs = Some(vec![ent])
                    }
                }
                // Error occurred.
                Err(err) => {
                    let when = self.handle_error(ctx, stream, err);

                    out = out.map_or(when, |curr| {
                        Some(when.map_or(curr, |when| curr.max(when)))
                    });
                }
            }
        }

        Ok(out)
    }

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::RetryIndefError> {
        let mut out = None;

        if let Some(indefs) = self.indefs.take() {
            for IndefEntry { msgs, origin } in indefs.into_iter() {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {},
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        out = Some(out.map_or(when,
                                              |curr: Instant| curr.max(when)));
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        let ent = IndefEntry {
                            origin: origin,
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.indefs {
                            error!(target: "private-datagram-push-mode",
                                   "indefs should be empty");

                            indefs.push(ent)
                        } else {
                            self.indefs = Some(vec![ent])
                        }
                    }
                    // Error occurred.
                    Err(err) => {
                        let when = self.handle_error(ctx, stream, err);

                        out = out.map_or(when, |curr| {
                            Some(when.map_or(curr, |when| curr.max(when)))
                        });
                    }
                }
            }
        }

        Ok(out)
    }
}

impl<Types, Ctx> Create for PrivateLargeObjPushMode<Types, Ctx>
where
    Types: PrivateLargeObjPushModeTypes<Ctx>
{
    type Config = PrivateLargeObjModeConfig;
    type CreateError = Infallible;

    fn create(config: Self::Config) -> Result<Self, Self::CreateError> {
        let (msg_retries_hint, frag_retries_hint) = config.take();
        let pending_msgs = match msg_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };
        let pending_frags = match frag_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };

        Ok(PrivateLargeObjPushMode {
            pending_msgs: pending_msgs,
            pending_frags: pending_frags
        })
    }
}

impl<InMsg, OutMsg, LargeObjTypes, Types, Ctx>
    PushMode<
        Types::Stream,
        LargeObjProto<InMsg, OutMsg, (), Types::Frags, LargeObjTypes>,
        Ctx
    > for PrivateLargeObjPushMode<Types, Ctx>
where
    LargeObjTypes: LargeObjProtoTypes<
        InMsg,
        OutMsg,
        Hash = Types::Hash,
        HashID = Types::HashID
    >,
    Types: PrivateLargeObjPushModeTypes<Ctx>,
    LargeObjTypes::HashID: 'static,
    Types::Stream: 'static
{
    type RetryError = Infallible;
    type SendError = PrivateLargeObjPushModeSendError<
        LargeObjPushError<
            Types::HashID,
            <Types::PushFragError as RecoverableError>::Permanent,
            <Types::PushOfferError as RecoverableError>::Permanent
        >,
        LargeObjSendError<
            Types::HashID,
            <LargeObjTypes::AuthNTypes as MsgAuthNTypes<InMsg>>::SessionPrin,
            <LargeObjTypes::Msgs as LargeObjMsgs<Types::Hash, OutMsg>
             >::AddMsgsError<LargeObjTypes::EncodeError>
        >
    >;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            (),
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream
    ) -> Result<Option<Instant>, Self::SendError> {
        debug!(target: "private-large-obj-push-mode",
               "fetching new outbound protocol messages");

        // Send the protocol messages.
        let (msgs, msgs_next) = proto.msgs().map_err(|err| {
            PrivateLargeObjPushModeSendError::Msgs { err: err }
        })?;

        if let Some(msgs) = msgs {
            if let RetryResult::Retry(retry) =
                PushEntry::from_try_send(ctx, stream, msgs)
            {
                // We got a retry somewhere along the process, store it.
                self.pending_msgs.push(retry)
            }
        }

        debug!(target: "private-large-obj-push-mode",
               "sending data fragments");

        let frags_next = match LargeObjEntry::from_try_send(ctx, stream, proto)
            .map_err(|err| PrivateLargeObjPushModeSendError::Frags {
                err: err
            })? {
            RetryResult::Success(next) => next,
            RetryResult::Retry(retry) => {
                self.pending_frags.push(retry);

                None
            }
        };

        let next = msgs_next.map_or(frags_next, |msgs| {
            Some(frags_next.map_or(msgs, |frags| msgs.min(frags)))
        });

        Ok(next)
    }

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            (),
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError> {
        debug!(target: "private-large-obj-push-mode",
               "retrying pending operations");

        let mut curr = Vec::with_capacity(self.pending_msgs.len());

        // ISSUE #29: Use a better data structure to avoid sorting
        // this array over and over.

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.pending_msgs
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        // Go through the sorted pending items and get all the ones
        // whose times are less than the present.
        while self.pending_msgs.last().is_some_and(|ent| now > ent.when()) {
            debug!(target: "private-large-obj-push-mode",
                   "retrying pending operation");

            match self.pending_msgs.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "private-large-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let msgs_next = self.pending_msgs.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            if let RetryResult::Retry(retry) = ent.exec(ctx, stream) {
                // We got a retry somewhere along the process, store it.
                self.pending_msgs.push(retry)
            }
        }

        // Now do the fragments.

        let mut curr = Vec::with_capacity(self.pending_frags.len());

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.pending_frags
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        // Go through the sorted pending items and get all the ones
        // whose times are less than the present.
        while self
            .pending_frags
            .last()
            .is_some_and(|ent| now > ent.when())
        {
            debug!(target: "private-large-obj-push-mode",
                   "retrying pending fragment");

            match self.pending_frags.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "private-large-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let mut frags_next = self.pending_frags.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream, proto) {
                Ok(RetryResult::Success(retry)) => {
                    frags_next = frags_next.map_or(retry, |next| {
                        retry.map(|retry| retry.min(next))
                    });
                }
                Ok(RetryResult::Retry(retry)) => {
                    frags_next =
                        Some(frags_next.map_or(retry.when(), |next| {
                            next.min(retry.when())
                        }));

                    self.pending_frags.push(retry)
                }
                Err(err) => {
                    error!(target: "private-large-obj-push-mode",
                           "unrecoverable error retrying push frags: {}",
                           err);
                }
            }
        }

        let out = msgs_next.map_or(frags_next, |msgs| {
            Some(frags_next.map_or(msgs, |frags| frags.min(msgs)))
        });

        Ok(out)
    }
}

impl<Msgs, ID, Flags, Msg, Batch, Add, Finish> ScopedError
    for PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch, Add, Finish>
where
    Finish: ScopedError,
    Batch: ScopedError,
    Add: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            PushEntryRecoverableError::Batch { err, .. } => err.scope(),
            PushEntryRecoverableError::Add { err, .. } => err.scope(),
            PushEntryRecoverableError::Finish { err, .. } => err.scope()
        }
    }
}

impl<Msgs, ID, Flags, Msg, Batch, Add, Finish> RecoverableError
    for PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch, Add, Finish>
where
    Finish: RecoverableError,
    Batch: RecoverableError,
    Add: RecoverableError
{
    type Completable = PushEntryRecoverableError<
        Msgs,
        ID,
        Flags,
        Msg,
        Batch::Completable,
        Add::Completable,
        Finish::Completable
    >;
    type Permanent = PushEntryError<
        Batch::Permanent,
        Add::Permanent,
        Finish::Permanent
    >;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            PushEntryRecoverableError::Batch { msgs, err } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| PushEntryRecoverableError::Batch {
                    msgs: msgs,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Batch { err: err }))
            }
            PushEntryRecoverableError::Add {
                batch_id, msgs, msg, flags, err
            } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| PushEntryRecoverableError::Add {
                    batch_id: batch_id,
                    flags: flags,
                    msgs: msgs,
                    msg: msg,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Add { err: err }))
            }
            PushEntryRecoverableError::Finish { batch_id, flags, err } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| PushEntryRecoverableError::Finish {
                    batch_id: batch_id,
                    flags: flags,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Finish { err: err }))
            }
        }
    }
}

impl<Batch, Add, Finish> ScopedError for PushEntryError<Batch, Add, Finish>
where
    Finish: ScopedError,
    Batch: ScopedError,
    Add: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            PushEntryError::Finish { err } => err.scope(),
            PushEntryError::Batch { err } => err.scope(),
            PushEntryError::Add { err } => err.scope()
        }
    }
}

impl<Frags, Msgs> ScopedError for PrivateLargeObjPushModeSendError<Frags, Msgs>
where
    Frags: ScopedError,
    Msgs: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            PrivateLargeObjPushModeSendError::Frags { err } => err.scope(),
            PrivateLargeObjPushModeSendError::Msgs { err } => err.scope()
        }
    }
}

impl<Batch, Add, Finish> Display for PushEntryError<Batch, Add, Finish>
where
    Finish: Display,
    Batch: Display,
    Add: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PushEntryError::Finish { err } => err.fmt(f),
            PushEntryError::Batch { err } => err.fmt(f),
            PushEntryError::Add { err } => err.fmt(f)
        }
    }
}

impl<Frags, Msgs> Display for PrivateLargeObjPushModeSendError<Frags, Msgs>
where
    Frags: Display,
    Msgs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PrivateLargeObjPushModeSendError::Frags { err } => err.fmt(f),
            PrivateLargeObjPushModeSendError::Msgs { err } => err.fmt(f)
        }
    }
}

impl Display for PrivatePrin {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "private recipient")
    }
}
