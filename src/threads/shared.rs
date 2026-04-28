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
use std::iter::IntoIterator;
use std::time::Instant;

use constellation_auth::authn::MsgAuthNTypes;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::net::SharedMsgs;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;
use log::warn;
use mio::Token;

use crate::config::SharedDatagramModeConfig;
use crate::config::SharedLargeObjModeConfig;
use crate::frags::Frags;
use crate::large_obj::FragsOrOffer;
use crate::large_obj::LargeObjMsg;
use crate::large_obj::LargeObjMsgs;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjSendError;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::Parties;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::threads::LargeObjEntry;
use crate::threads::PushMode;

pub trait SharedLargeObjPushModeTypes<Ctx> {
    type Parties: IntoIterator<Item = Self::PartyID>;
    type Frags: Frags;
    type BatchID: Clone + Debug + Display;
    type PartyID: Clone + Debug + Display + From<usize> + Eq + Hash + Ord;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddErrorCompletable: ScopedError;
    type AddError: RecoverableError<Completable = Self::AddErrorCompletable>;
    type CancelBatchErrorCompletable: ScopedError;
    type CancelBatchError: RecoverableError<Completable = Self::CancelBatchErrorCompletable>;
    type FinishBatchErrorCompletable: ScopedError;
    type FinishBatchError: RecoverableError<Completable = Self::FinishBatchErrorCompletable>;
    type StartBatchErrorCompletable: ScopedError;
    type StartBatchError: RecoverableError<Completable = Self::StartBatchErrorCompletable>;
    type PushFragErrorCompletable: ScopedError;
    type PushFragError: RecoverableError<Completable = Self::PushFragErrorCompletable>;
    type PushOfferErrorCompletable: ScopedError;
    type PushOfferError: RecoverableError<Completable = Self::PushOfferErrorCompletable>;
    type IndefParties: IntoIterator<Item = Self::PartyID>;
    type PartiesError: Debug + Display + ScopedError;
    type StreamFlags: Default;
    type Stream: PushStreamReportBatchError<
            <Self::FinishBatchError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Self::AddError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::PushFragError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::PushOfferError as RecoverableError>::Permanent
        > + LargeObjStream<
            Ctx,
            Frags = Self::Frags,
            Parties = Self::Parties,
            PushFragError = Self::PushFragError
        > + LargeObjOfferStream<
            Self::HashID,
            Ctx,
            PushOfferError = Self::PushOfferError
        > + PushStreamShared<Ctx,
                             IndefParties = Self::IndefParties,
                             StartBatchError = Self::StartBatchError>
        + PushStreamPartyID<PartyID = Self::PartyID>
        + PushStreamParties<PartiesError = Self::PartiesError>
        + PushStreamAdd<LargeObjMsg<Self::HashID>, Ctx, AddError = Self::AddError>
        + PushStream<
            Ctx,
            BatchID = Self::BatchID,
            StreamFlags = Self::StreamFlags,
            CancelBatchError = Self::CancelBatchError,
            FinishBatchError = Self::FinishBatchError
        >;
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
        + PushStreamShared<Ctx>,
    Msg: Clone {
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

struct IndefEntry<Msg, PartyID> {
    /// When the messages were originally sent; used for timeouts.
    origin: Instant,
    /// The parties for which this is stalled.
    parties: Vec<PartyID>,
    /// The messages to send.
    msgs: Vec<Msg>
}

pub struct SharedDatagramPushMode<Msg, Stream, Ctx>
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
        + PushStreamShared<Ctx>
        + PushStreamParties,
    Msg: Clone {
    /// Buffer for sends in progress.
    pending: Vec<PushEntry<Msg, Stream, Ctx>>,
    /// Pending operations that stalled with `WouldBlock`
    completes: Option<Vec<PushEntryRecoverableError<
        Vec<Msg>,
        Stream::BatchID,
        Stream::StreamFlags,
        Msg,
        <Stream::StartBatchError as RecoverableError>::Completable,
        <Stream::AddError as RecoverableError>::Completable,
        <Stream::FinishBatchError as RecoverableError>::Completable,
        <Stream::CancelBatchError as RecoverableError>::Completable
    >>>,
    // XXX figure out a way to ensure dense IDs, so we don't have to
    // use HashSets here..

    /// All currently-live parties.
    live: HashSet<Stream::PartyID>,
    /// Indefinitely-delayed messages.
    indefs: Option<Vec<IndefEntry<Msg, Stream::PartyID>>>,
    /// Size hint.
    retries_hint: Option<usize>
}

pub struct SharedLargeObjPushMode<Types, Ctx>
where
    Types: SharedLargeObjPushModeTypes<Ctx> {
    /// Buffer for sends in progress.
    msgs_pending:
        Vec<PushEntry<LargeObjMsg<Types::HashID>, Types::Stream, Ctx>>,
    /// Pending message sends that stalled with `WouldBlock`
    msgs_completes: Option<Vec<PushEntryRecoverableError<
        Vec<LargeObjMsg<Types::HashID>>,
        Types::BatchID,
        Types::StreamFlags,
        LargeObjMsg<Types::HashID>,
        Types::StartBatchErrorCompletable,
        Types::AddErrorCompletable,
        Types::FinishBatchErrorCompletable,
        Types::CancelBatchErrorCompletable,
    >>>,
    frags_pending: Vec<LargeObjEntry<Types::Stream, Types::Hash, Ctx>>,
    frags_completes: Option<Vec<FragsOrOffer<
        Types::HashID,
        Types::PushFragErrorCompletable,
        Types::PushOfferErrorCompletable
    >>>,
    /// Pending message sends that produced indefinite waits.
    msgs_indefs: Option<Vec<IndefEntry<LargeObjMsg<Types::HashID>,
                                       Types::PartyID>>>,
    frags_indef: HashSet<Types::PartyID>,
    /// All currently-live parties.
    live: HashSet<Types::PartyID>,
    /// Size hint for message arrays.
    msg_retries_hint: Option<usize>,
    /// Size hint for frags arrays.
    frags_retries_hint: Option<usize>
}

#[derive(Debug)]
pub enum SharedLargeObjPushModeSendError<Frags, Msgs> {
    Frags { err: Frags },
    Msgs { err: Msgs }
}

enum PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch,
                               Add, Finish, Cancel> {
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
    },
    Cancel {
        batch_id: ID,
        flags: Flags,
        err: Cancel
    }
}

/// Type of permanent errors that can occur creating and sending a batch.
#[derive(Debug)]
pub enum PushEntryError<ID, Batch, Add, Finish, Cancel> {
    /// An error occurred creating the batch.
    Batch {
        /// The error that occurred creating the batch.
        err: Batch
    },
    /// An error occurred adding messages.
    Add {
        /// Batch ID to which the error corresponds.
        batch_id: ID,
        /// The error that occurred adding messages.
        err: Add
    },
    /// An error occurred finishing the batch.
    Finish {
        /// Batch ID to which the error corresponds.
        batch_id: ID,
        /// The error that occurred finishing the batch.
        err: Finish
    },
    /// An error occurred canceling the batch.
    Cancel {
        /// Batch ID to which the error corresponds.
        batch_id: ID,
        /// The error that occurred canceling the batch.
        err: Cancel
    }
}

impl<Msg, Stream, Ctx> RetryWhen for PushEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamAdd<Msg, Ctx>
        + PushStreamShared<Ctx>
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        >,
    Msg: Clone
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
    Stream: PushStreamReportBatchError<
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
        + PushStreamShared<Ctx>,
    Stream::PartyID: From<usize>,
    Msg: Clone
{
    fn complete_cancel(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: <Stream::CancelBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<(), Self>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        trace!(target: "push-entry",
               "attempting to recover from error while cancelling message");

        match stream.complete_cancel_batch(ctx, &mut flags, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                trace!(target: "push-entry",
                       "successfully completed cancellation");

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Cancel {
                    batch: batch_id,
                    retry: retry,
                    flags: flags
                }))
            }
            // More errors; recurse again.
            Err(err) => Err(PushEntryRecoverableError::Cancel {
                batch_id: batch_id,
                flags: flags,
                err: err
            })
        }
    }

    fn try_cancel_batch(
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        let mut flags = stream.empty_flags();

        match stream.cancel_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => Ok(RetryResult::Success(())),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                Ok(RetryResult::Retry(PushEntry::Cancel {
                    batch: batch_id,
                    retry: retry,
                    flags: flags
                }))
            }
            Err(err) => Err(PushEntryRecoverableError::Cancel {
                batch_id: batch_id,
                flags: flags,
                err: err
            })
        }
    }

    fn try_abort_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        err: <Stream::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.abort_start_batch(ctx, &mut flags, err) {
            // It succeeded.
            RetryResult::Success(_) => RetryResult::Success(()),
            // We got a retry.
            RetryResult::Retry(retry) => {
                RetryResult::Retry(PushEntry::Abort {
                    retry: retry,
                    flags: flags
                })
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
        RetryIndefResult<(), Self, (Vec<Msg>, Parties<Stream::IndefParties>)>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
            Ok(RetryIndefResult::Indef(parties)) =>
                Ok(RetryIndefResult::Indef((msgs, parties))),
            Err(err) => Err(PushEntryRecoverableError::Batch {
                msgs: msgs,
                err: err
            })
        }
    }

    fn try_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> Result<
        RetryIndefResult<(), Self, (Vec<Msg>, Parties<Stream::IndefParties>)>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        match stream.start_batch(ctx, parties.iter()) {
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
            Ok(RetryIndefResult::Indef(parties)) =>
                Ok(RetryIndefResult::Indef((msgs, parties))),
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
            <Stream::FinishBatchError as RecoverableError>::Completable,
            <Stream::CancelBatchError as RecoverableError>::Completable
        >
    ) -> Result<
        RetryIndefResult<(), Self, (Vec<Msg>, Parties<Stream::IndefParties>)>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
                .map(RetryIndefResult::from),
            PushEntryRecoverableError::Cancel { batch_id, err, flags } =>
                Self::complete_cancel(ctx, stream, flags, batch_id, err)
                .map(RetryIndefResult::from)
        }
    }

    fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> Result<
        RetryIndefResult<(), Self, (Vec<Msg>, Parties<Stream::IndefParties>)>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
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
                Ok(RetryIndefResult::Indef(parties)) =>
                   Ok(RetryIndefResult::Indef((msgs, parties))),
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
                Err(err) => Err(PushEntryRecoverableError::Cancel {
                    batch_id: batch,
                    flags: flags,
                    err: err
                })
            }
        }
    }

    #[inline]
    fn try_send(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> Result<
        RetryIndefResult<(), Self, (Vec<Msg>, Parties<Stream::IndefParties>)>,
        PushEntryRecoverableError<
            Vec<Msg>,
            Stream::BatchID,
            Stream::StreamFlags,
            Msg,
            Stream::StartBatchError,
            Stream::AddError,
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        Self::try_start_batch(ctx, stream, parties, msgs)
    }
}

impl<Msg, Stream, Ctx> SharedDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
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
        + PushStreamShared<Ctx>
        + PushStreamParties,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Stream::PartyID: Display + From<usize>,
    Msg: Clone
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    ) -> Option<Instant> {
        let (completable, permanent) = err.split();

        let next = if let Some(permanent) = permanent {
            error!(target: "shared-datagram-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);

            // Report the error.
            match permanent {
                PushEntryError::Batch { err } => {
                    if let Err(err) = stream.report_error(&err) {
                        error!(target: "private-datagram-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    if let RetryResult::Retry(retry) =
                        PushEntry::try_abort_batch(ctx, stream, err) {
                        let when = retry.when();

                        self.pending.push(retry);

                        Some(when)
                    } else {
                        None
                    }
                },
                PushEntryError::Add { batch_id, err } => {
                    if let Err(err) = stream
                        .report_error_with_batch(&batch_id, &err) {
                        error!(target: "private-datagram-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => None,
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.pending.push(retry);

                            Some(when)
                        },
                        Err(err) => self.handle_error(ctx, stream, err),
                    }
                },
                PushEntryError::Finish { batch_id, err } => {
                    if let Err(err) = stream
                        .report_error_with_batch(&batch_id, &err) {
                        error!(target: "private-datagram-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => None,
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.pending.push(retry);

                            Some(when)
                        },
                        Err(err) => self.handle_error(ctx, stream, err),
                    }
                }
                // Don't report cancel errors.
                PushEntryError::Cancel { .. } => None,
            }
        } else {
            None
        };

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                if let Some(completes) = &mut self.completes {
                    completes.push(completable)
                } else {
                    let mut vec = match self.retries_hint {
                        Some(hint) => Vec::with_capacity(hint),
                        None => Vec::new()
                    };

                    vec.push(completable);

                    self.completes = Some(vec);
                }

                next
            } else {
                match PushEntry::complete(ctx, stream, completable) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        Some(next_retry_definite(&next, &when))
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => {
                        self.indef_delay(stream, msgs, parties);

                        next
                    }
                    // Error occurred.
                    Err(err) => {
                        self.handle_error(ctx, stream, err);

                        next
                    }
                }
            }
        } else {
            next
        }
    }

    fn indef_delay(
        &mut self,
        stream: &mut Stream,
        msgs: Vec<Msg>,
        parties: Parties<Stream::IndefParties>
    ) {
        let parties: Option<Vec<Stream::PartyID>> = match parties {
            Parties::Some(parties) => Some(parties.into_iter().collect()),
            Parties::All => match stream.parties() {
                Ok(parties) =>
                    Some(parties.into_iter().map(|(id, _)| id).collect()),
                Err(err) => {
                    error!(target: "shared-datagram-push-mode",
                           "error obtaining parties: {}",
                           err);

                    None
                }
            }
        };

        if let Some(parties) = parties {
            // Add to the set of blocked parties.
            for party in parties.iter() {
                if self.live.remove(party) {
                    warn!(target: "shared-datagram-push-mode",
                          "party {} was not in live set",
                          party)
                }
            }

            // Record the indefinite wait.
            let ent = IndefEntry {
                origin: Instant::now(),
                parties: parties,
                msgs: msgs
            };

            if let Some(indefs) = &mut self.indefs {
                indefs.push(ent)
            } else {
                let mut vec = match self.retries_hint {
                    Some(hint) => Vec::with_capacity(hint),
                    None => Vec::new()
                };

                vec.push(ent);

                self.indefs = Some(vec)
            }
        }
    }
}

impl<Msg, Stream, Ctx> CreateWithParam<&'_ Stream>
    for SharedDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
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
        + PushStreamShared<Ctx>
        + PushStreamParties,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Stream::PartyID: Display + From<usize>,
    Msg: Clone
{
    type Config = SharedDatagramModeConfig;
    type CreateError = Stream::PartiesError;

    fn create(
        config: Self::Config,
        stream: &Stream
    ) -> Result<Self, Self::CreateError> {
        let retries_hint = config.take();
        let all_parties: HashSet<Stream::PartyID> = stream.parties()?
            .map(|(id, _)| id).collect();

        match retries_hint {
            Some(hint) => Ok(SharedDatagramPushMode {
                completes: None,
                pending: Vec::with_capacity(hint),
                live: all_parties,
                indefs: None,
                retries_hint: retries_hint
            }),
            None => Ok(SharedDatagramPushMode {
                completes: None,
                pending: Vec::new(),
                live: all_parties,
                indefs: None,
                retries_hint: retries_hint
            })
        }
    }
}

impl<Msg, Msgs, Stream, Ctx> PushMode<Stream, Msgs, Ctx>
    for SharedDatagramPushMode<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
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
        + PushStreamShared<Ctx>
        + PushStreamParties,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Stream::PartyID: Display + From<usize>,
    Msgs: SharedMsgs<Stream::PartyID, Msg>,
    Msg: Clone
{
    type RetryError = Infallible;
    type SendError = Msgs::MsgsError;
    type RetryIndefError = Infallible;

    #[inline]
    fn has_complete_pending(&self) -> bool {
        self.completes.is_some()
    }

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>
    ) -> Result<Option<Instant>, Self::SendError> {
        if !self.live.is_empty() {
            debug!(target: "shared-small-obj-push-mode",
                   "fetching new outbound messages");

            let (groups, mut next) = msgs.msgs(&self.live)?;

            if let Some(groups) = groups {
                // Go through each group and try sending it
                for (parties, msgs) in groups {
                    match PushEntry::try_send(ctx, stream, parties, msgs) {
                        // Send succeeded; nothing to do.
                        Ok(RetryIndefResult::Success(())) => {}
                        // Retry delay; store to pending.
                        Ok(RetryIndefResult::Retry(retry)) => {
                            let when = retry.when();

                            self.pending.push(retry);

                            next = Some(next_retry_definite(&next, &when));
                        },
                        // Indefinite delay; store to indefs.
                        Ok(RetryIndefResult::Indef((msgs, parties))) => self
                            .indef_delay(stream, msgs, parties),
                        // Error occurred.
                        Err(err) => {
                            let when = self.handle_error(ctx, stream, err);

                            next = next_retry(&next, &when);
                        }
                    }
                }
            }

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
        debug!(target: "shared-small-obj-push-mode",
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
        while self.pending.last().is_some_and(|ent| now >= ent.when()) {
            debug!(target: "shared-small-obj-push-mode",
                   "retrying pending operation");

            match self.pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "shared-small-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let mut next = self.pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream) {
                // Send succeeded; nothing to do.
                Ok(RetryIndefResult::Success(())) => {}
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    let when = retry.when();

                    self.pending.push(retry);

                    next = Some(next_retry_definite(&next, &when));
                },
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef((msgs, parties))) => self
                    .indef_delay(stream, msgs, parties),
                // Error occurred.
                Err(err) => {
                    let when = self.handle_error(ctx, stream, err);

                    next = next_retry(&next, &when);
                }
            }
        }

        Ok(next)
    }

    fn complete_pending(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>,
    ) -> Result<Option<Instant>, Self::RetryError> {
        if let Some(completes) = self.completes.take() {
            let mut next = None;

            // First complete any pending messages.
            for complete in completes.into_iter() {
                let retry = match PushEntry::complete(ctx, stream, complete) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => {
                        self.indef_delay(stream, msgs, parties);

                        None
                    }
                    // Error occurred.
                    Err(err) => self.handle_error(ctx, stream, err),
                };

                next = next_retry(&next, &retry);
            }

            Ok(next)
        } else {
            Ok(None)
        }
    }

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::RetryIndefError> {
        let mut out = None;

        if let Some(indefs) = self.indefs.take() {
            for IndefEntry { msgs, parties, .. } in indefs.into_iter() {
                match PushEntry::try_send(ctx, stream, parties, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {},
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.pending.push(retry);

                        out = Some(next_retry_definite(&out, &when));
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => self
                        .indef_delay(stream, msgs, parties),
                    // Error occurred.
                    Err(err) => {
                        let when = self.handle_error(ctx, stream, err);

                        out = next_retry(&out, &when);
                    }
                }
            }
        }

        Ok(out)
    }
}

impl<Types, Ctx> SharedLargeObjPushMode<Types, Ctx>
where
    Types: SharedLargeObjPushModeTypes<Ctx>,
{
    fn handle_msg_error<InMsg, OutMsg, LargeObjTypes>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Types::Stream,
        err: PushEntryRecoverableError<
            Vec<LargeObjMsg<Types::HashID>>,
            Types::BatchID,
            Types::StreamFlags,
            LargeObjMsg<Types::HashID>,
            Types::StartBatchError,
            Types::AddError,
            Types::FinishBatchError,
            Types::CancelBatchError
        >
    ) -> Option<Instant>
    where
        LargeObjTypes: LargeObjProtoTypes<
            InMsg,
            OutMsg,
            Hash = Types::Hash,
            HashID = Types::HashID
        >
    {
        let (completable, permanent) = err.split();

        if let Some(permanent) = permanent {
            error!(target: "shared-large-obj-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);
        }

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                if let Some(completes) = &mut self.msgs_completes {
                    completes.push(completable)
                } else {
                    let mut vec = match self.msg_retries_hint {
                        Some(hint) => Vec::with_capacity(hint),
                        None => Vec::new()
                    };

                    vec.push(completable);

                    self.msgs_completes = Some(vec);
                }

                None
            } else {
                match PushEntry::complete(ctx, stream, completable) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.msgs_pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => {
                        self.indef_delay(stream, msgs, parties, Instant::now());

                        None
                    }
                    // Error occurred.
                    Err(err) => {
                        self.handle_msg_error::<_, _, LargeObjTypes>(ctx, stream, err);

                        None
                    }
                }
            }
        } else {
            None
        }
    }

    fn handle_frags_error<InMsg, OutMsg, LargeObjTypes>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Types::Stream,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        err: LargeObjPushError<
            Types::HashID,
            Types::PushFragError,
            Types::PushOfferError
        >
    ) -> Option<Instant>
    where
        LargeObjTypes: LargeObjProtoTypes<
            InMsg,
            OutMsg,
            Hash = Types::Hash,
            HashID = Types::HashID
        >
    {
        let (completable, permanent) = err.split();

        if let Some(permanent) = permanent {
            error!(target: "private-large-obj-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);
        }

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                if let Some(completes) = &mut self.frags_completes {
                    completes.push(completable)
                } else {
                    let mut vec = match self.frags_retries_hint {
                        Some(hint) => Vec::with_capacity(hint),
                        None => Vec::new()
                    };

                    vec.push(completable);

                    self.frags_completes = Some(vec);
                }

                None
            } else {
                match LargeObjEntry::complete_send(ctx, stream, proto,
                                                   completable) {
                    // Succeeded; nothing to do.
                    Ok(RetryIndefResult::Success((next, _))) => next,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        self.frags_pending.push(retry);

                        None
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(parties)) => match parties {
                        Parties::Some(parties) => {
                            self.frags_indef = parties.into_iter().collect();

                            None
                        },
                        Parties::All => match stream.parties() {
                            Ok(parties) => {
                                self.frags_indef = parties.into_iter()
                                    .map(|(id, _)| id)
                                    .collect();

                                None
                            },
                            Err(err) => {
                                error!(target: "shared-datagram-push-mode",
                                       "error obtaining parties: {}",
                                       err);

                                None
                            }
                        }
                    }
                    // Error occurred.
                    Err(err) => {
                        self.handle_frags_error::<_, _, LargeObjTypes>(ctx, stream, proto, err);

                        None
                    }
                }
            }
        } else {
            None
        }
    }

    fn indef_delay(
        &mut self,
        stream: &mut Types::Stream,
        msgs: Vec<LargeObjMsg<Types::HashID>>,
        parties: Parties<Types::IndefParties>,
        origin: Instant
    ) {
        let parties: Option<Vec<Types::PartyID>> = match parties {
            Parties::Some(parties) => Some(parties.into_iter().collect()),
            Parties::All => match stream.parties() {
                Ok(parties) =>
                    Some(parties.into_iter().map(|(id, _)| id).collect()),
                Err(err) => {
                    error!(target: "shared-datagram-push-mode",
                           "error obtaining parties: {}",
                           err);

                    None
                }
            }
        };

        if let Some(parties) = parties {
            // Add to the set of blocked parties.
            for party in parties.iter() {
                if self.live.remove(party) {
                    warn!(target: "shared-large-obj-push-mode",
                          "party {} was not in live set",
                          party)
                }
            }

            let ent = IndefEntry {
                origin: origin,
                parties: parties,
                msgs: msgs
            };

            if let Some(indefs) = &mut self.msgs_indefs {
                indefs.push(ent)
            } else {
                let mut vec = match self.msg_retries_hint {
                    Some(hint) => Vec::with_capacity(hint),
                    None => Vec::new()
                };

                vec.push(ent);

                self.msgs_indefs = Some(vec)
            }
        }
    }
}

impl<Types, Ctx> CreateWithParam<&'_ Types::Stream>
    for SharedLargeObjPushMode<Types, Ctx>
where
    Types: SharedLargeObjPushModeTypes<Ctx>,
{
    type Config = SharedLargeObjModeConfig;
    type CreateError = Types::PartiesError;

    fn create(
        config: Self::Config,
        stream: &Types::Stream
    ) -> Result<Self, Self::CreateError> {
        let (msg_retries_hint, frag_retries_hint) = config.take();
        let msgs_pending = match msg_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new(),
        };
        let frags_pending = match frag_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };
        let all_parties: HashSet<Types::PartyID> = stream.parties()?
            .map(|(id, _)| id).collect();
        let frags_indefs = HashSet::with_capacity(all_parties.len());

        Ok(SharedLargeObjPushMode {
            msgs_pending: msgs_pending,
            msgs_completes: None,
            msgs_indefs: None,
            frags_pending: frags_pending,
            frags_completes: None,
            frags_indef: frags_indefs,
            live: all_parties,
            msg_retries_hint: msg_retries_hint,
            frags_retries_hint: frag_retries_hint,
        })
    }
}

impl<InMsg, OutMsg, LargeObjTypes, Types, Ctx>
    PushMode<
        Types::Stream,
        LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        Ctx
    > for SharedLargeObjPushMode<Types, Ctx>
where
    LargeObjTypes: LargeObjProtoTypes<
        InMsg,
        OutMsg,
        Hash = Types::Hash,
        HashID = Types::HashID
    >,
    Types: SharedLargeObjPushModeTypes<Ctx>,
{
    type RetryError = Infallible;
    type SendError = SharedLargeObjPushModeSendError<
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
    type RetryIndefError = Infallible;

    #[inline]
    fn has_complete_pending(&self) -> bool {
        self.msgs_completes.is_some() || self.frags_completes.is_some()
    }

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
        live: &HashSet<Token>
    ) -> Result<Option<Instant>, Self::SendError> {
        if self.msgs_indefs.is_none() {
            debug!(target: "shared-large-obj-push-mode",
                   "fetching new outbound protocol messages");

            // Send the low-level protocol messages.
            let (groups, msgs_next) = proto.msgs(&self.live).map_err(|err| {
                SharedLargeObjPushModeSendError::Msgs { err: err }
            })?;

            let mut out = msgs_next;

            if let Some(groups) = groups {
                // Go through each group and try sending it
                for (parties, msgs) in groups {
                    let retry = match PushEntry::try_send(ctx, stream,
                                                          parties, msgs) {
                        // Send succeeded; nothing to do.
                        Ok(RetryIndefResult::Success(())) => None,
                        // Retry delay; store to pending.
                        Ok(RetryIndefResult::Retry(retry)) => {
                            let when = retry.when();

                            self.msgs_pending.push(retry);

                            Some(when)
                        },
                        // Indefinite delay; store to indefs.
                        Ok(RetryIndefResult::Indef((msgs, parties))) => {
                            self.indef_delay(stream, msgs, parties,
                                             Instant::now());

                            None
                        }
                        // Error occurred.
                        Err(err) => self
                            .handle_msg_error::<_, _, LargeObjTypes>(
                                ctx, stream, err
                            )
                    };

                    out = next_retry(&out, &retry);
                }
            }

            debug!(target: "shared-large-obj-push-mode",
                   "sending data fragments");

            match LargeObjEntry::try_send(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((next, _))) => {
                    out = next_retry(&out, &next);
                }
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    self.frags_pending.push(retry);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(parties)) => match parties {
                    Parties::Some(parties) => {
                        self.frags_indef = parties.into_iter().collect();
                    },
                    Parties::All => match stream.parties() {
                        Ok(parties) => {
                            self.frags_indef = parties.into_iter()
                                 .map(|(id, _)| id)
                                .collect();
                        },
                        Err(err) => {
                            error!(target: "shared-datagram-push-mode",
                                   "error obtaining parties: {}",
                                   err);
                        }
                    }
                }
                // Error occurred.
                Err(err) => {
                    let next = self.handle_frags_error::<_, _, LargeObjTypes>(ctx, stream, proto, err);

                    out = next_retry(&out, &next);
                }
            };

            Ok(out)
        } else {
            Ok(None)
        }
    }

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
        live: &HashSet<Token>,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError> {
        debug!(target: "shared-large-obj-push-mode",
               "retrying pending operations");

        let mut curr = Vec::with_capacity(self.msgs_pending.len());

        // ISSUE #29: Use a better data structure to avoid sorting
        // this array over and over.

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.msgs_pending
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        // Go through the sorted pending items and get all the ones
        // whose times are less than the present.
        while self.msgs_pending.last().is_some_and(|ent| now > ent.when()) {
            debug!(target: "shared-large-obj-push-mode",
                   "retrying pending operation");

            match self.msgs_pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "shared-large-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        let mut out = self.msgs_pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream) {
                // Send succeeded; nothing to do.
                Ok(RetryIndefResult::Success(())) => {},
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    let when = retry.when();

                    self.msgs_pending.push(retry);
                    out = Some(next_retry_definite(&out, &when));
                },
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef((msgs, parties))) => self
                    .indef_delay(stream, msgs, parties, Instant::now()),
                // Error occurred.
                Err(err) => {
                    let when = self
                        .handle_msg_error::<_, _, LargeObjTypes>(ctx, stream, err);

                    out = next_retry(&out, &when);
                }
            }
        }

        // Now do the fragments.

        let mut curr = Vec::with_capacity(self.frags_pending.len());

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.frags_pending
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        // Go through the sorted pending items and get all the ones
        // whose times are less than the present.
        while self
            .frags_pending
            .last()
            .is_some_and(|ent| now > ent.when())
        {
            debug!(target: "shared-large-obj-push-mode",
                   "retrying pending fragment");

            match self.frags_pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "shared-large-obj-push-mode",
                           "pop should not be empty");

                    break;
                }
            }
        }

        // The last entry should now be the first time past the present.
        out =
            next_retry(&out, &self.frags_pending.last().map(|ent| ent.when()));

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((next, _))) => {
                    out = next_retry(&out, &next);
                }
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    self.frags_pending.push(retry);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(parties)) => match parties {
                    Parties::Some(parties) => {
                        self.frags_indef = parties.into_iter().collect();
                    },
                    Parties::All => match stream.parties() {
                        Ok(parties) => {
                            self.frags_indef = parties.into_iter()
                                .map(|(id, _)| id)
                                .collect();
                        },
                        Err(err) => {
                            error!(target: "shared-datagram-push-mode",
                                   "error obtaining parties: {}",
                                   err);
                        }
                    }
                }
                // Error occurred.
                Err(err) => {
                    let next = self.handle_frags_error::<_, _, LargeObjTypes>(ctx, stream, proto, err);

                    out = next_retry(&out, &next);
                }
            }
        }

        Ok(out)
    }

    fn complete_pending(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
        _live: &HashSet<Token>,
    ) -> Result<Option<Instant>, Self::RetryError> {
        let mut next = None;

        if let Some(completes) = self.msgs_completes.take() {
            // First complete any pending messages.
            for complete in completes.into_iter() {
                let retry = match PushEntry::complete(ctx, stream, complete) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => None,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.msgs_pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => {
                        self.indef_delay(stream, msgs, parties, Instant::now());

                        None
                    }
                    // Error occurred.
                    Err(err) => self.handle_msg_error::<_, _, LargeObjTypes>(ctx, stream, err),
                };

                next = next_retry(&next, &retry);
            }
        }

        if let Some(completes) = self.frags_completes.take() {
            // First complete any pending messages.
            for complete in completes.into_iter() {
                let retry = match LargeObjEntry::complete_send(ctx, stream,
                                                               proto,
                                                               complete) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success((next, _))) => next,
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.frags_pending.push(retry);

                        Some(when)
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(parties)) => match parties {
                        Parties::Some(parties) => {
                            self.frags_indef = parties.into_iter().collect();

                            None
                        },
                        Parties::All => match stream.parties() {
                            Ok(parties) => {
                                self.frags_indef = parties.into_iter()
                                    .map(|(id, _)| id)
                                    .collect();

                                None
                            },
                            Err(err) => {
                                error!(target: "shared-datagram-push-mode",
                                       "error obtaining parties: {}",
                                       err);

                                None
                            }
                        }
                    }
                    // Error occurred.
                    Err(err) => self.handle_frags_error::<_, _, LargeObjTypes>(ctx, stream, proto, err)
                };

                next = next_retry(&next, &retry);
            }
        }

        Ok(next)
    }

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            Types::PartyID,
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
    ) -> Result<Option<Instant>, Self::RetryIndefError> {
        let mut out = None;

        if let Some(indefs) = self.msgs_indefs.take() {
            for IndefEntry { msgs, parties, origin } in indefs.into_iter() {
                match PushEntry::try_send(ctx, stream, parties, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {},
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        let when = retry.when();

                        self.msgs_pending.push(retry);

                        out = Some(next_retry_definite(&out, &when));
                    },
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef((msgs, parties))) => self
                        .indef_delay(stream, msgs, parties, origin),
                    // Error occurred.
                    Err(err) => {
                        let when = self.handle_msg_error::<_, _, LargeObjTypes>(
                            ctx, stream, err
                        );

                        out = next_retry(&out, &when);
                    }
                }
            }
        }

        if !self.frags_indef.is_empty() {
            match LargeObjEntry::try_send(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((when, _))) => {
                    out = next_retry(&out, &when);
                },
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    self.frags_pending.push(retry);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(parties)) => match parties {
                    Parties::Some(parties) => {
                        self.frags_indef = parties.into_iter().collect();
                    },
                    Parties::All => match stream.parties() {
                        Ok(parties) => {
                            self.frags_indef = parties.into_iter()
                                .map(|(id, _)| id)
                                .collect();
                        },
                        Err(err) => {
                            error!(target: "shared-datagram-push-mode",
                                   "error obtaining parties: {}",
                                   err);
                        }
                    }
                }
                // Error occurred.
                Err(err) => {
                    self.handle_frags_error::<_, _, LargeObjTypes>(ctx, stream, proto, err);
                }
            }

            self.frags_indef.clear()
        }

        Ok(out)
    }
}

impl<Frags, Msgs> ScopedError for SharedLargeObjPushModeSendError<Frags, Msgs>
where
    Frags: ScopedError,
    Msgs: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            SharedLargeObjPushModeSendError::Frags { err } => err.scope(),
            SharedLargeObjPushModeSendError::Msgs { err } => err.scope()
        }
    }
}

impl<ID, Batch, Add, Finish, Cancel> ScopedError
    for PushEntryError<ID, Batch, Add, Finish, Cancel>
where
    Cancel: ScopedError,
    Finish: ScopedError,
    Batch: ScopedError,
    Add: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            PushEntryError::Cancel { err, .. } => err.scope(),
            PushEntryError::Finish { err, .. } => err.scope(),
            PushEntryError::Batch { err } => err.scope(),
            PushEntryError::Add { err, .. } => err.scope()
        }
    }
}

impl<Msgs, ID, Flags, Msg, Batch, Add, Finish, Cancel> ScopedError
    for PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch, Add, Finish, Cancel>
where
    Cancel: ScopedError,
    Finish: ScopedError,
    Batch: ScopedError,
    Add: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            PushEntryRecoverableError::Batch { err, .. } => err.scope(),
            PushEntryRecoverableError::Add { err, .. } => err.scope(),
            PushEntryRecoverableError::Finish { err, .. } => err.scope(),
            PushEntryRecoverableError::Cancel { err, .. } => err.scope()
        }
    }
}

impl<Msgs, ID, Flags, Msg, Batch, Add, Finish, Cancel> RecoverableError
    for PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch,
                                  Add, Finish, Cancel>
where
    Cancel: RecoverableError,
    Finish: RecoverableError,
    Batch: RecoverableError,
    Add: RecoverableError,
    ID: Clone + Debug
{
    type Completable = PushEntryRecoverableError<
        Msgs,
        ID,
        Flags,
        Msg,
        Batch::Completable,
        Add::Completable,
        Finish::Completable,
        Cancel::Completable
    >;
    type Permanent = PushEntryError<
        ID,
        Batch::Permanent,
        Add::Permanent,
        Finish::Permanent,
        Cancel::Permanent
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
                    batch_id: batch_id.clone(),
                    flags: flags,
                    msgs: msgs,
                    msg: msg,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Add {
                     batch_id: batch_id,
                     err: err
                 }))
            }
            PushEntryRecoverableError::Finish { batch_id, flags, err } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| PushEntryRecoverableError::Finish {
                    batch_id: batch_id.clone(),
                    flags: flags,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Finish {
                     batch_id: batch_id,
                     err: err
                 }))
            }
            PushEntryRecoverableError::Cancel { batch_id, flags, err } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| PushEntryRecoverableError::Cancel {
                    batch_id: batch_id.clone(),
                    flags: flags,
                    err: err
                }),
                 permanent.map(|err| PushEntryError::Cancel {
                     batch_id: batch_id,
                     err: err
                 }))
            }
        }
    }
}

impl<ID, Batch, Add, Finish, Cancel> Display
    for PushEntryError<ID, Batch, Add, Finish, Cancel>
where
    Cancel: Display,
    Finish: Display,
    Batch: Display,
    Add: Display,
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PushEntryError::Cancel { err, .. } => err.fmt(f),
            PushEntryError::Finish { err, .. } => err.fmt(f),
            PushEntryError::Batch { err } => err.fmt(f),
            PushEntryError::Add { err, .. } => err.fmt(f)
        }
    }
}

impl<Frags, Msgs> Display for SharedLargeObjPushModeSendError<Frags, Msgs>
where
    Frags: Display,
    Msgs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SharedLargeObjPushModeSendError::Frags { err } => err.fmt(f),
            SharedLargeObjPushModeSendError::Msgs { err } => err.fmt(f)
        }
    }
}
