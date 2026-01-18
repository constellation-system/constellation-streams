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
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;

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
    type HashID: Clone + Display + Hash + HashID + Eq + Send;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddError: RecoverableError;
    type StartBatchError: RecoverableError;
    type FinishBatchError: RecoverableError;
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
    pending: Vec<PushEntry<Msg, Stream, Ctx>>
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
        flags: &mut Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::FinishBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while finishing batch");

        match err.split() {
            (Some(completable), None) => match stream.complete_finish_batch(
                ctx,
                flags,
                &batch_id,
                completable
            ) {
                // It succeeded.
                Ok(RetryResult::Success(())) => {
                    trace!(target: "push-entry",
                       "successfully finished batch");

                    RetryResult::Success(())
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Finish {
                        batch: batch_id,
                        retry: retry
                    })
                }
                // More errors; recurse again.
                Err(err) => {
                    Self::complete_finish(ctx, stream, flags, batch_id, err)
                }
            },
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred canceling the batch.
                error!(target: "push-entry",
                       "unrecoverable error finishing batch: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_finish_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.finish_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Finish {
                    batch: batch_id.clone(),
                    retry: retry
                })
            }
            Err(err) => Self::complete_finish(
                ctx,
                stream,
                &mut flags,
                batch_id.clone(),
                err
            )
        }
    }

    fn complete_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID,
        err: Stream::AddError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while adding message");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_add(
                    ctx,
                    &mut flags,
                    &msg,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "push-entry",
                           "successfully added message");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Add {
                            msgs: msgs,
                            msg: msg,
                            flags: flags,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_add(
                        ctx, stream, flags, msgs, msg, batch_id, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "push-entry",
                       "unrecoverable error adding message: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_add_msg(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match stream.add(ctx, &mut flags, &msg, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Add {
                    msgs: msgs,
                    msg: msg,
                    flags: flags,
                    batch: batch_id,
                    retry: retry
                })
            }
            Err(err) => {
                Self::complete_add(ctx, stream, flags, msgs, msg, batch_id, err)
            }
        }
    }

    fn try_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
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
        err: Stream::StartBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while creating batch");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_start_batch(ctx, completable) {
                    // It succeeded.
                    Ok(RetryResult::Success(batch_id)) => {
                        trace!(target: "push-entry",
                           "successfully created batch");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Batch {
                            msgs: msgs,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => {
                        Self::complete_start_batch(ctx, stream, msgs, err)
                    }
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "push-entry",
                       "unrecoverable error adding parties: {}",
                       permanent);

                // Report the failure
                if let Err(err) = stream.report_error(&permanent) {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                let mut flags = stream.empty_flags();

                stream
                    .abort_start_batch(ctx, &mut flags, permanent)
                    .map_retry(|retry| PushEntry::Abort {
                        flags: flags,
                        retry: retry
                    })
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        match stream.start_batch(ctx) {
            // It succeeded.
            Ok(RetryResult::Success(batch_id)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    retry: retry
                })
            }
            Err(err) => Self::complete_start_batch(ctx, stream, msgs, err)
        }
    }

    fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> RetryResult<(), Self> {
        match self {
            PushEntry::Batch { msgs, retry } => match stream
                .retry_start_batch(ctx, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(batch_id)) => {
                    Self::try_add(ctx, stream, msgs, batch_id)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Batch {
                        msgs: msgs,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_start_batch(ctx, stream, msgs, err)
            },
            PushEntry::Abort { mut flags, retry } => stream
                .retry_abort_start_batch(ctx, &mut flags, retry)
                .map_retry(|retry| PushEntry::Abort {
                    flags: flags,
                    retry: retry
                }),
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
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Add {
                        msgs: msgs,
                        msg: msg,
                        flags: flags,
                        batch: batch,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_add(
                    ctx, stream, flags, msgs, msg, batch, err
                )
            },
            PushEntry::Finish { batch, retry } => {
                let mut flags = stream.empty_flags();

                match stream.retry_finish_batch(ctx, &mut flags, &batch, retry)
                {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Finish {
                            batch: batch,
                            retry: retry
                        })
                    }
                    Err(err) => Self::complete_finish(
                        ctx, stream, &mut flags, batch, err
                    )
                }
            }
            PushEntry::Cancel {
                batch,
                retry,
                mut flags
            } => match stream.retry_cancel_batch(ctx, &mut flags, &batch, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Cancel {
                        batch: batch,
                        retry: retry,
                        flags: flags
                    })
                }
                Err(err) => {
                    Self::complete_cancel_batch(ctx, stream, flags, batch, err)
                }
            }
        }
    }

    #[inline]
    fn from_try_send(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        Self::try_start_batch(ctx, stream, msgs)
    }
}

impl<Msg, Stream, Ctx> Create
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
    Msg: 'static + Clone + Send
{
    type Config = PrivateDatagramModeConfig;

    fn create(config: Self::Config) -> Self {
        let retries_hint = config.take();

        match retries_hint {
            Some(hint) => PrivateDatagramPushMode {
                pending: Vec::with_capacity(hint)
            },
            None => PrivateDatagramPushMode {
                pending: Vec::new()
            }
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
    Msgs: 'static + PrivateMsgs<Msg> + Send,
    Msg: 'static + Clone + Send
{
    type RetryError = Infallible;
    type SendError = Msgs::MsgsError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::SendError> {
        debug!(target: "private-small-obj-push-mode",
               "fetching new outbound messages");

        let (msgs, next) = msgs.msgs()?;

        if let Some(msgs) = msgs {
            // Try sending the messages.
            if let RetryResult::Retry(retry) =
                PushEntry::from_try_send(ctx, stream, msgs)
            {
                // We got a retry somewhere along the process, store it.
                self.pending.push(retry)
            }
        }

        Ok(next)
    }

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError> {
        debug!(target: "private-small-obj-push-mode",
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
            debug!(target: "private-small-obj-push-mode",
                   "retrying pending operation");

            match self.pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "private-small-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let out = self.pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            if let RetryResult::Retry(retry) = ent.exec(ctx, stream) {
                // We got a retry somewhere along the process, store it.
                self.pending.push(retry)
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
