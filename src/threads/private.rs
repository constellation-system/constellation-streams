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
use std::marker::PhantomData;
use std::time::Instant;

use constellation_auth::authn::MsgAuthNTypes;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
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

use crate::addrs::Addrs;
use crate::channels::Channels;
use crate::config::PrivateDatagramModeConfig;
use crate::config::PrivateLargeObjModeConfig;
use crate::frags::Frags;
use crate::large_obj::FragsOrOffer;
use crate::large_obj::LargeObjMsg;
use crate::large_obj::LargeObjMsgs;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjSendError;
use crate::select::ConnChannelID;
use crate::select::SelectorBatchError;
use crate::select::SelectorBatchSelectError;
use crate::select::StreamSelector;
use crate::select::StreamSelectorBatch;
use crate::select::StreamSelectorSelectRefreshError;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::StreamID;
use crate::threads::LargeObjEntry;
use crate::threads::PushMode;
use crate::threads::PushModeResult;

pub trait PrivateLargeObjPushModeTypes<Ctx> {
    type Frags: Frags;
    type BatchID: Clone + Debug + Display;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddErrorCompletable: ScopedError;
    type AddError: RecoverableError<Completable = Self::AddErrorCompletable>;
    type CancelBatchErrorCompletable: ScopedError;
    type CancelBatchError: RecoverableError<
        Completable = Self::CancelBatchErrorCompletable
    >;
    type FinishBatchErrorCompletable: ScopedError;
    type FinishBatchError: RecoverableError<
        Completable = Self::FinishBatchErrorCompletable
    >;
    type StartBatchErrorCompletable: ScopedError;
    type StartBatchError: RecoverableError<
        Completable = Self::StartBatchErrorCompletable
    >;
    type PushFragErrorCompletable: ScopedError;
    type PushFragError: RecoverableError<
        Completable = Self::PushFragErrorCompletable
    >;
    type PushOfferErrorCompletable: ScopedError;
    type PushOfferError: RecoverableError<
        Completable = Self::PushOfferErrorCompletable
    >;
    type StreamFlags: Default;
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
            StreamFlags = Self::StreamFlags,
            CancelBatchError = Self::CancelBatchError,
            FinishBatchError = Self::FinishBatchError
        > + LargeObjOfferStream<
            Self::HashID,
            Ctx,
            PushOfferError = Self::PushOfferError
        > + LargeObjStream<
            Ctx,
            Frags = Self::Frags,
            PushFragError = Self::PushFragError
        >;
}

pub struct SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    H: Clone + HashAlgo,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx> + PushStream<Ctx>,
    Resolve: Addrs<Addr = Ctx::Addr> {
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    ctx: PhantomData<Ctx>,
    hash: PhantomData<H>
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
        + PushStreamPrivate<Ctx>,
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
        + PushStreamPrivate<Ctx>,
    Msg: Clone {
    /// Buffer for sends in progress.
    pending: Vec<PushEntry<Msg, Stream, Ctx>>,
    /// Pending sends that stalled with `WouldBlock`
    completes: Option<
        Vec<
            PushEntryRecoverableError<
                Vec<Msg>,
                Stream::BatchID,
                Stream::StreamFlags,
                Msg,
                <Stream::StartBatchError as RecoverableError>::Completable,
                <Stream::AddError as RecoverableError>::Completable,
                <Stream::FinishBatchError as RecoverableError>::Completable,
                <Stream::CancelBatchError as RecoverableError>::Completable
            >
        >
    >,
    /// Pending operations that produced indefinite waits.
    indefs: Option<Vec<IndefEntry<Msg>>>,
    /// Size hint.
    retries_hint: Option<usize>
}

pub struct PrivateLargeObjPushMode<Types, Ctx>
where
    Types: PrivateLargeObjPushModeTypes<Ctx> {
    /// Buffer for message sends in progress.
    msgs_pending:
        Vec<PushEntry<LargeObjMsg<Types::HashID>, Types::Stream, Ctx>>,
    /// Pending message sends that stalled with `WouldBlock`
    msgs_completes: Option<
        Vec<
            PushEntryRecoverableError<
                Vec<LargeObjMsg<Types::HashID>>,
                Types::BatchID,
                Types::StreamFlags,
                LargeObjMsg<Types::HashID>,
                Types::StartBatchErrorCompletable,
                Types::AddErrorCompletable,
                Types::FinishBatchErrorCompletable,
                Types::CancelBatchErrorCompletable
            >
        >
    >,
    frags_pending: Vec<LargeObjEntry<Types::Stream, Types::Hash, Ctx>>,
    frags_completes: Option<
        Vec<
            FragsOrOffer<
                Types::HashID,
                Types::PushFragErrorCompletable,
                Types::PushOfferErrorCompletable
            >
        >
    >,
    /// Pending message sends that produced indefinite waits.
    msgs_indefs: Option<Vec<IndefEntry<LargeObjMsg<Types::HashID>>>>,
    frags_indef: bool,
    /// Size hint for message arrays.
    msg_retries_hint: Option<usize>,
    /// Size hint for frags arrays.
    frags_retries_hint: Option<usize>
}

#[derive(Clone)]
pub struct PrivatePrin;

#[derive(Debug)]
pub enum PrivateLargeObjPushModeSendError<Frags, Msgs> {
    Frags { err: Frags },
    Msgs { err: Msgs }
}

enum PushEntryRecoverableError<Msgs, ID, Flags, Msg, Batch, Add, Finish, Cancel>
{
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

impl<Epochs, H, Resolve, Ctx> Clone
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    H: Clone + HashAlgo,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx> + PushStream<Ctx>,
    Resolve: Addrs<Addr = Ctx::Addr>
{
    #[inline]
    fn clone(&self) -> Self {
        SelectorLargeObjDatagramPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

unsafe impl<Epochs, H, Resolve, Ctx> Send
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    H: Clone + HashAlgo,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx> + PushStream<Ctx>,
    Resolve: Addrs<Addr = Ctx::Addr>
{
}

unsafe impl<Epochs, H, Resolve, Ctx> Sync
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    H: Clone + HashAlgo,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx> + PushStream<Ctx>,
    Resolve: Addrs<Addr = Ctx::Addr>
{
}

impl<Epochs, H, Resolve, Ctx> PrivateLargeObjPushModeTypes<Ctx>
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type Frags = <Ctx::Stream as LargeObjStream<Ctx>>::Frags;
    type BatchID = StreamSelectorBatch<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::BatchID
    >;
    type HashID = H::HashID;
    type Hash = H;
    type AddErrorCompletable = <
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
            as RecoverableError
    >::Completable;
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
    >;
    type CancelBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError as RecoverableError
    >::Completable;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type FinishBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError as RecoverableError
    >::Completable;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type StartBatchErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type PushFragErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushFragError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjStream<Ctx>>::PushFragError,
            Epochs::Item
        >
    >;
    type PushOfferErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushOfferError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;
    type Stream = StreamSelector<Epochs, Resolve, Ctx>;
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
        > + PushStreamReportError<
            <Stream::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as RecoverableError>::Permanent,
            Stream::BatchID
        > + PushStreamAdd<Msg, Ctx>
        + PushStreamPrivate<Ctx>,
    Stream::BatchID: Display,
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
               "attempting to recover from error while cancelling batch {}",
               batch_id);

        match stream.complete_cancel_batch(ctx, &mut flags, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                trace!(target: "push-entry",
                       "successfully completed cancellation");

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying cancelling of batch {}",
                       batch_id);

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
        trace!(target: "push-entry",
               "cancelling batch {}",
               batch_id);

        let mut flags = stream.empty_flags();

        match stream.cancel_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                trace!(target: "push-entry",
                       "successfully cancelled batch {}",
                       batch_id);

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying cancelling of batch {}",
                       batch_id);

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
        trace!(target: "push-entry",
               "aborting starting batch");

        let mut flags = stream.empty_flags();

        match stream.abort_start_batch(ctx, &mut flags, err) {
            // It succeeded.
            RetryResult::Success(_) => {
                trace!(target: "push-entry",
                       "aborted starting batch");

                RetryResult::Success(())
            }
            // We got a retry.
            RetryResult::Retry(retry) => {
                trace!(target: "push-entry",
                       "delaying abort starting batch");

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
               "attempting to recover from error while finishing batch {}",
               batch_id);

        match stream.complete_finish_batch(ctx, &mut flags, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(())) => {
                trace!(target: "push-entry",
                       "successfully finished batch, {}",
                       batch_id);

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying finishing batch {}",
                       batch_id);

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
        trace!(target: "push-entry",
               "finishing batch {}",
               batch_id);

        let mut flags = stream.empty_flags();

        match stream.finish_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                trace!(target: "push-entry",
                       "finished batch {}",
                       batch_id);

                Ok(RetryResult::Success(()))
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying finishing batch {}",
                       batch_id);

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
               "attempting to recover from error while \
                adding message to batch {}",
               batch_id);

        match stream.complete_add(ctx, &mut flags, &msg, &batch_id, err) {
            // It succeeded.
            Ok(RetryResult::Success(())) => {
                trace!(target: "push-entry",
                       "successfully added message to batch {}",
                       batch_id);

                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying adding message to batch {}",
                       batch_id);

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
        trace!(target: "push-entry",
               "adding message to batch {}",
               batch_id);

        match stream.add(ctx, &mut flags, &msg, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                trace!(target: "push-entry",
                       "added message to batch {}",
                       batch_id);

                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying adding message to batch {}",
                       batch_id);

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
        RetryIndefResult<(), Self, Vec<Msg>>,
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
                       "successfully created batch {}",
                       batch_id);

                Self::try_add(ctx, stream, msgs, batch_id)
                    .map(RetryIndefResult::from)
            }
            // We got a retry.
            Ok(RetryIndefResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying creating batch");

                Ok(RetryIndefResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    retry: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) => {
                Ok(RetryIndefResult::Indef(msgs))
            }
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        trace!(target: "push-entry",
               "creating batch");

        match stream.start_batch(ctx) {
            // It succeeded.
            Ok(RetryIndefResult::Success(batch_id)) => {
                trace!(target: "push-entry",
                       "created batch {}",
                       batch_id);

                Self::try_add(ctx, stream, msgs, batch_id)
                    .map(RetryIndefResult::from)
            }
            // We got a retry.
            Ok(RetryIndefResult::Retry(retry)) => {
                trace!(target: "push-entry",
                       "delaying creating batch");

                Ok(RetryIndefResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    retry: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) => {
                Ok(RetryIndefResult::Indef(msgs))
            }
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
        RetryIndefResult<(), Self, Vec<Msg>>,
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
            PushEntryRecoverableError::Batch { msgs, err } => {
                Self::complete_start_batch(ctx, stream, msgs, err)
            }
            PushEntryRecoverableError::Add {
                batch_id,
                flags,
                msgs,
                msg,
                err
            } => {
                Self::complete_add(ctx, stream, flags, msgs, msg, batch_id, err)
                    .map(RetryIndefResult::from)
            }
            PushEntryRecoverableError::Finish {
                batch_id,
                err,
                flags
            } => Self::complete_finish(ctx, stream, flags, batch_id, err)
                .map(RetryIndefResult::from),
            PushEntryRecoverableError::Cancel {
                batch_id,
                err,
                flags
            } => Self::complete_cancel(ctx, stream, flags, batch_id, err)
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        match self {
            PushEntry::Batch { msgs, retry } => {
                match stream.retry_start_batch(ctx, retry) {
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
                    Ok(RetryIndefResult::Indef(())) => {
                        Ok(RetryIndefResult::Indef(msgs))
                    }
                    Err(err) => Err(PushEntryRecoverableError::Batch {
                        msgs: msgs,
                        err: err
                    })
                }
            }
            PushEntry::Abort { mut flags, retry } => {
                Ok(RetryIndefResult::from(
                    stream
                        .retry_abort_start_batch(ctx, &mut flags, retry)
                        .map_retry(|retry| PushEntry::Abort {
                            flags: flags,
                            retry: retry
                        })
                ))
            }
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

                match stream.retry_finish_batch(ctx, &mut flags, &batch, retry)
                {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        Ok(RetryIndefResult::Success(()))
                    }
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
            } => {
                match stream.retry_cancel_batch(ctx, &mut flags, &batch, retry)
                {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        Ok(RetryIndefResult::Success(()))
                    }
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
            Stream::FinishBatchError,
            Stream::CancelBatchError
        >
    > {
        Self::try_start_batch(ctx, stream, msgs)
    }
}

impl<Msg, Stream, Ctx> PrivateDatagramPushMode<Msg, Stream, Ctx>
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
        + PushStreamPrivate<Ctx>,
    Stream::BatchID: Display,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::CancelBatchError as RecoverableError>::Completable: ScopedError,
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
    ) -> PushModeResult {
        let (completable, permanent) = err.split();

        let mut next = if let Some(permanent) = permanent {
            error!(target: "private-datagram-push-mode",
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
                        PushEntry::try_abort_batch(ctx, stream, err)
                    {
                        let when = retry.when();

                        self.pending.push(retry);

                        PushModeResult::from_next_retry(when)
                    } else {
                        PushModeResult::default()
                    }
                }
                PushEntryError::Add { batch_id, err } => {
                    if let Err(err) =
                        stream.report_error_with_batch(&batch_id, &err)
                    {
                        error!(target: "private-datagram-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => {
                            PushModeResult::default()
                        }
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.pending.push(retry);

                            PushModeResult::from_next_retry(when)
                        }
                        Err(err) => self.handle_error(ctx, stream, err)
                    }
                }
                PushEntryError::Finish { batch_id, err } => {
                    if let Err(err) =
                        stream.report_error_with_batch(&batch_id, &err)
                    {
                        error!(target: "private-datagram-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => {
                            PushModeResult::default()
                        }
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.pending.push(retry);

                            PushModeResult::from_next_retry(when)
                        }
                        Err(err) => self.handle_error(ctx, stream, err)
                    }
                }
                // Don't report cancel errors.
                PushEntryError::Cancel { .. } => PushModeResult::default()
            }
        } else {
            PushModeResult::default()
        };

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                trace!(target: "private-datagram-push-mode",
                       "delaying completion of error");

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

                next.set_has_completes();
            } else {
                trace!(target: "private-datagram-push-mode",
                       "completing error immediately");

                match PushEntry::complete(ctx, stream, completable) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying completed operation");

                        let when = retry.when();

                        self.pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying completed operation indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_error(ctx, stream, err);

                        next.merge(&res);
                    }
                }
            }
        }

        next
    }
}

impl<Msg, Stream, Ctx> Create for PrivateDatagramPushMode<Msg, Stream, Ctx>
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
        + PushStreamPrivate<Ctx>,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    Msg: Clone
{
    type Config = PrivateDatagramModeConfig;
    type CreateError = Infallible;

    fn create(config: Self::Config) -> Result<Self, Self::CreateError> {
        let retries_hint = config.take();

        match retries_hint {
            Some(hint) => Ok(PrivateDatagramPushMode {
                pending: Vec::with_capacity(hint),
                completes: None,
                indefs: None,
                retries_hint: retries_hint
            }),
            None => Ok(PrivateDatagramPushMode {
                pending: Vec::new(),
                completes: None,
                indefs: None,
                retries_hint: retries_hint
            })
        }
    }
}

impl<Msg, Stream, Ctx> CreateWithParam<&'_ Stream>
    for PrivateDatagramPushMode<Msg, Stream, Ctx>
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
        + PushStreamPrivate<Ctx>,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    Msg: Clone
{
    type Config = PrivateDatagramModeConfig;
    type CreateError = Infallible;

    fn create(
        config: Self::Config,
        _stream: &Stream
    ) -> Result<Self, Self::CreateError> {
        let retries_hint = config.take();

        match retries_hint {
            Some(hint) => Ok(PrivateDatagramPushMode {
                pending: Vec::with_capacity(hint),
                completes: None,
                indefs: None,
                retries_hint: retries_hint
            }),
            None => Ok(PrivateDatagramPushMode {
                pending: Vec::new(),
                completes: None,
                indefs: None,
                retries_hint: retries_hint
            })
        }
    }
}

impl<Msg, Msgs, Stream, Ctx> PushMode<Stream, Msgs, Ctx>
    for PrivateDatagramPushMode<Msg, Stream, Ctx>
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
        + PushStreamPrivate<Ctx>,
    Stream::BatchID: Display,
    <Stream::StartBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::AddError as RecoverableError>::Completable: ScopedError,
    <Stream::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <Stream::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Msgs: PrivateMsgs<Msg>,
    Msg: Clone
{
    type RetryError = Infallible;
    type RetryIndefError = Infallible;
    type SendError = Msgs::MsgsError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError> {
        if self.indefs.is_none() {
            debug!(target: "private-datagram-push-mode",
                   "fetching new outbound messages");

            let (msgs, next) = msgs.msgs(Instant::now())?;
            let mut next = PushModeResult::new(next, None, false);

            if let Some(msgs) = msgs {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying send");

                        let when = retry.when();

                        self.pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying send indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.indefs {
                            error!(target: "private-datagram-push-mode",
                                   "indefs should be empty");

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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_error(ctx, stream, err);

                        next.merge(&res)
                    }
                }
            }

            Ok(next)
        } else {
            Ok(PushModeResult::default())
        }
    }

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>,
        now: Instant
    ) -> Result<PushModeResult, Self::RetryError> {
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
        while self.pending.last().is_some_and(|ent| now >= ent.when()) {
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
        let out = self.pending.last().map(|ent| ent.when());
        let mut out = PushModeResult::new(out, None, false);

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream) {
                // Send succeeded; nothing to do.
                Ok(RetryIndefResult::Success(())) => {}
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    trace!(target: "private-datagram-push-mode",
                           "delaying retried send");

                    let when = retry.when();

                    self.pending.push(retry);
                    out.merge_next_retry_definite(&when);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(msgs)) => {
                    trace!(target: "private-datagram-push-mode",
                           "delaying retried send indefinitely");

                    let ent = IndefEntry {
                        origin: Instant::now(),
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
                // Error occurred.
                Err(err) => {
                    let res = self.handle_error(ctx, stream, err);

                    out.merge(&res)
                }
            }
        }

        Ok(out)
    }

    fn complete_pending(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::RetryError> {
        let mut next = PushModeResult::default();

        if let Some(completes) = self.completes.take() {
            debug!(target: "private-datagram-push-mode",
                   "completing pending operations");

            // First complete any pending messages.
            for complete in completes.into_iter() {
                match PushEntry::complete(ctx, stream, complete) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying completed send");

                        let when = retry.when();

                        self.pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying completed send indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_error(ctx, stream, err);

                        next.merge(&res)
                    }
                }
            }
        }

        Ok(next)
    }

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        _msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<PushModeResult, Self::RetryIndefError> {
        let mut next = PushModeResult::default();

        if let Some(indefs) = self.indefs.take() {
            debug!(target: "private-datagram-push-mode",
                   "retrying indefinitely delayed operations");

            for IndefEntry { msgs, origin } in indefs.into_iter() {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying retried send");

                        let when = retry.when();

                        self.pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-datagram-push-mode",
                               "delaying retried send indefinitely");

                        let ent = IndefEntry {
                            origin: origin,
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.indefs {
                            error!(target: "private-datagram-push-mode",
                                   "indefs should be empty");

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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_error(ctx, stream, err);

                        next.merge(&res);
                    }
                }
            }
        }

        Ok(next)
    }
}

impl<Types, Ctx> PrivateLargeObjPushMode<Types, Ctx>
where
    Types: PrivateLargeObjPushModeTypes<Ctx>
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
    ) -> PushModeResult
    where
        LargeObjTypes: LargeObjProtoTypes<
                InMsg,
                OutMsg,
                Hash = Types::Hash,
                HashID = Types::HashID
            > {
        let (completable, permanent) = err.split();
        let mut next = PushModeResult::default();

        if let Some(permanent) = permanent {
            error!(target: "private-large-obj-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);

            // Report the error.
            match permanent {
                PushEntryError::Batch { err } => {
                    if let Err(err) = stream.report_error(&err) {
                        error!(target: "private-large-obj-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    if let RetryResult::Retry(retry) =
                        PushEntry::try_abort_batch(ctx, stream, err)
                    {
                        let when = retry.when();

                        self.msgs_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                }
                PushEntryError::Add { batch_id, err } => {
                    if let Err(err) =
                        stream.report_error_with_batch(&batch_id, &err)
                    {
                        error!(target: "private-large-obj-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => {}
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.msgs_pending.push(retry);
                            next.merge_next_retry_definite(&when);
                        }
                        Err(err) => {
                            let res = self
                                .handle_msg_error::<_, _, LargeObjTypes>(
                                    ctx, stream, err
                                );

                            next.merge(&res);
                        }
                    }
                }
                PushEntryError::Finish { batch_id, err } => {
                    if let Err(err) =
                        stream.report_error_with_batch(&batch_id, &err)
                    {
                        error!(target: "private-large-obj-push-mode",
                               "failure reporting error to stream: {}",
                               err);
                    }

                    match PushEntry::try_cancel_batch(ctx, stream, batch_id) {
                        Ok(RetryResult::Success(())) => {}
                        Ok(RetryResult::Retry(retry)) => {
                            let when = retry.when();

                            self.msgs_pending.push(retry);
                            next.merge_next_retry_definite(&when);
                        }
                        Err(err) => {
                            let res = self
                                .handle_msg_error::<_, _, LargeObjTypes>(
                                    ctx, stream, err
                                );

                            next.merge(&res);
                        }
                    }
                }
                // Don't report cancel errors.
                PushEntryError::Cancel { .. } => {}
            }
        }

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                trace!(target: "private-large-obj-push-mode",
                       "delaying completion of error");

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

                next.set_has_completes();
            } else {
                trace!(target: "private-large-obj-push-mode",
                       "completing error immediately");

                match PushEntry::complete(ctx, stream, completable) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed operation");

                        let when = retry.when();

                        self.msgs_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed operation indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_msg_error::<_, _, LargeObjTypes>(
                            ctx, stream, err
                        );

                        next.merge(&res);
                    }
                }
            }
        }

        next
    }

    fn handle_frags_error<InMsg, OutMsg, LargeObjTypes>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Types::Stream,
        proto: &mut LargeObjProto<
            InMsg,
            OutMsg,
            (),
            Types::Frags,
            LargeObjTypes
        >,
        err: LargeObjPushError<
            Types::HashID,
            Types::PushFragError,
            Types::PushOfferError
        >
    ) -> PushModeResult
    where
        LargeObjTypes: LargeObjProtoTypes<
                InMsg,
                OutMsg,
                Hash = Types::Hash,
                HashID = Types::HashID
            > {
        let (completable, permanent) = err.split();

        if let Some(permanent) = permanent {
            error!(target: "private-large-obj-push-mode",
                   "unrecoverable error sending batch: {}",
                   permanent);
        }

        let mut next = PushModeResult::default();

        if let Some(completable) = completable {
            if completable.scope() == ErrorScope::WouldBlock {
                trace!(target: "private-large-obj-push-mode",
                       "delaying completion of error");

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

                next.set_has_completes();
            } else {
                trace!(target: "private-large-obj-push-mode",
                       "completing error immediately");

                match LargeObjEntry::complete_send(
                    ctx,
                    stream,
                    proto,
                    completable
                ) {
                    // Succeeded; nothing to do.
                    Ok(RetryIndefResult::Success((when, _))) => {
                        next.merge_next_outbound(&when);
                    }
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed frags");

                        let when = retry.when();

                        self.frags_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(_)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed frags indefinitely");

                        self.frags_indef = true;
                    }
                    // Error occurred.
                    Err(err) => {
                        let res = self
                            .handle_frags_error::<_, _, LargeObjTypes>(
                                ctx, stream, proto, err
                            );

                        next.merge(&res)
                    }
                }
            }
        }

        next
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
        let msgs_pending = match msg_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };
        let frags_pending = match frag_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };

        Ok(PrivateLargeObjPushMode {
            msgs_pending: msgs_pending,
            msgs_completes: None,
            frags_pending: frags_pending,
            frags_completes: None,
            msgs_indefs: None,
            frags_indef: false,
            msg_retries_hint: msg_retries_hint,
            frags_retries_hint: frag_retries_hint
        })
    }
}

impl<Types, Ctx> CreateWithParam<&'_ Types::Stream>
    for PrivateLargeObjPushMode<Types, Ctx>
where
    Types: PrivateLargeObjPushModeTypes<Ctx>
{
    type Config = PrivateLargeObjModeConfig;
    type CreateError = Infallible;

    fn create(
        config: Self::Config,
        _stream: &Types::Stream
    ) -> Result<Self, Self::CreateError> {
        let (msg_retries_hint, frag_retries_hint) = config.take();
        let msgs_pending = match msg_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };
        let frags_pending = match frag_retries_hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };

        Ok(PrivateLargeObjPushMode {
            msgs_pending: msgs_pending,
            msgs_completes: None,
            frags_pending: frags_pending,
            frags_completes: None,
            msgs_indefs: None,
            frags_indef: false,
            msg_retries_hint: msg_retries_hint,
            frags_retries_hint: frag_retries_hint
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
    Types: PrivateLargeObjPushModeTypes<Ctx>
{
    type RetryError = Infallible;
    type RetryIndefError = Infallible;
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
        stream: &mut Types::Stream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError> {
        if self.msgs_indefs.is_none() {
            debug!(target: "private-large-obj-push-mode",
                   "fetching new outbound protocol messages");

            // Send the low-level protocol messages.
            let (msgs, next) = proto.msgs(Instant::now()).map_err(|err| {
                PrivateLargeObjPushModeSendError::Msgs { err: err }
            })?;
            let mut next = PushModeResult::new(next, None, false);

            if let Some(msgs) = msgs {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying protocol messages");

                        let when = retry.when();

                        self.msgs_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying protocol messages indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.msgs_indefs {
                            error!(target: "private-large-obj-push-mode",
                                   "indefs should be empty");

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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_msg_error::<_, _, LargeObjTypes>(
                            ctx, stream, err
                        );

                        next.merge(&res)
                    }
                }
            }

            debug!(target: "private-large-obj-push-mode",
                   "sending data fragments");

            match LargeObjEntry::try_send(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((when, _))) => {
                    next.merge_next_outbound(&when);
                }
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying data fragments");

                    let when = retry.when();

                    self.frags_pending.push(retry);
                    next.merge_next_retry_definite(&when);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(_)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying data fragments indefinitely");

                    self.frags_indef = true;
                }
                // Error occurred.
                Err(err) => {
                    let res = self.handle_frags_error::<_, _, LargeObjTypes>(
                        ctx, stream, proto, err
                    );

                    next.merge(&res)
                }
            }

            Ok(next)
        } else {
            Ok(PushModeResult::default())
        }
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
        _live: &HashSet<Token>,
        now: Instant
    ) -> Result<PushModeResult, Self::RetryError> {
        debug!(target: "private-large-obj-push-mode",
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
        while self
            .msgs_pending
            .last()
            .is_some_and(|ent| now >= ent.when())
        {
            debug!(target: "private-large-obj-push-mode",
                   "retrying pending operation");

            match self.msgs_pending.pop() {
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
        let out = self.msgs_pending.last().map(|ent| ent.when());
        let mut out = PushModeResult::new(out, None, false);

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream) {
                // Send succeeded; nothing to do.
                Ok(RetryIndefResult::Success(())) => {}
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried protocol messages");

                    let when = retry.when();

                    self.msgs_pending.push(retry);
                    out.merge_next_retry_definite(&when);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(msgs)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried protocol messages indefinitely");

                    let ent = IndefEntry {
                        origin: Instant::now(),
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
                // Error occurred.
                Err(err) => {
                    let res = self.handle_msg_error::<_, _, LargeObjTypes>(
                        ctx, stream, err
                    );

                    out.merge(&res);
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
            debug!(target: "private-large-obj-push-mode",
                   "retrying pending fragment");

            match self.frags_pending.pop() {
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
        let when = self.frags_pending.last().map(|ent| ent.when());

        out.merge_next_outbound(&when);

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            match ent.exec(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((when, _))) => {
                    out.merge_next_outbound(&when);
                }
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried data fragments");

                    let when = retry.when();

                    self.frags_pending.push(retry);
                    out.merge_next_retry_definite(&when);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(_)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried data fragments indefinitely");

                    self.frags_indef = true;
                }
                // Error occurred.
                Err(err) => {
                    let res = self.handle_frags_error::<_, _, LargeObjTypes>(
                        ctx, stream, proto, err
                    );

                    out.merge(&res);
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
            (),
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::RetryError> {
        let mut next = PushModeResult::default();

        if let Some(completes) = self.msgs_completes.take() {
            debug!(target: "private-large-obj-push-mode",
                   "completing pending operations");

            // First complete any pending messages.
            for complete in completes.into_iter() {
                match PushEntry::complete(ctx, stream, complete) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed protocol messages");

                        let when = retry.when();

                        self.msgs_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed protocol \
                                messages indefinitely");

                        let ent = IndefEntry {
                            origin: Instant::now(),
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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_msg_error::<_, _, LargeObjTypes>(
                            ctx, stream, err
                        );

                        next.merge(&res);
                    }
                }
            }
        }

        if let Some(completes) = self.frags_completes.take() {
            // First complete any pending messages.
            for complete in completes.into_iter() {
                match LargeObjEntry::complete_send(ctx, stream, proto, complete)
                {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success((when, _))) => {
                        next.merge_next_outbound(&when);
                    }
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed data fragments");

                        let when = retry.when();

                        self.frags_pending.push(retry);
                        next.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(_)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying completed data \
                                fragments indefinitely");

                        self.frags_indef = true;
                    }
                    // Error occurred.
                    Err(err) => {
                        let res = self
                            .handle_frags_error::<_, _, LargeObjTypes>(
                                ctx, stream, proto, err
                            );

                        next.merge(&res);
                    }
                };
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
            (),
            Types::Frags,
            LargeObjTypes
        >,
        stream: &mut Types::Stream
    ) -> Result<PushModeResult, Self::RetryIndefError> {
        let mut out = PushModeResult::default();

        if let Some(indefs) = self.msgs_indefs.take() {
            debug!(target: "private-large-obj-push-mode",
                   "retrying pending indefinitely delayed operations");

            for IndefEntry { msgs, origin } in indefs.into_iter() {
                match PushEntry::try_send(ctx, stream, msgs) {
                    // Send succeeded; nothing to do.
                    Ok(RetryIndefResult::Success(())) => {}
                    // Retry delay; store to pending.
                    Ok(RetryIndefResult::Retry(retry)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying retried protocol messages");

                        let when = retry.when();

                        self.msgs_pending.push(retry);
                        out.merge_next_retry_definite(&when);
                    }
                    // Indefinite delay; store to indefs.
                    Ok(RetryIndefResult::Indef(msgs)) => {
                        trace!(target: "private-large-obj-push-mode",
                               "delaying retried protocol messages \
                                indefinitely");

                        let ent = IndefEntry {
                            origin: origin,
                            msgs: msgs
                        };

                        if let Some(indefs) = &mut self.msgs_indefs {
                            error!(target: "private-large-obj-push-mode",
                                   "indefs should be empty");

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
                    // Error occurred.
                    Err(err) => {
                        let res = self.handle_msg_error::<_, _, LargeObjTypes>(
                            ctx, stream, err
                        );

                        out.merge(&res);
                    }
                }
            }
        }

        if self.frags_indef {
            self.frags_indef = false;

            match LargeObjEntry::try_send(ctx, stream, proto) {
                // Succeeded; nothing to do.
                Ok(RetryIndefResult::Success((when, _))) => {
                    out.merge_next_outbound(&when);
                }
                // Retry delay; store to pending.
                Ok(RetryIndefResult::Retry(retry)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried data fragments");

                    let when = retry.when();

                    self.frags_pending.push(retry);
                    out.merge_next_retry_definite(&when);
                }
                // Indefinite delay; store to indefs.
                Ok(RetryIndefResult::Indef(_)) => {
                    trace!(target: "private-large-obj-push-mode",
                           "delaying retried data fragments indefinitely");

                    self.frags_indef = true;
                }
                // Error occurred.
                Err(err) => {
                    let res = self.handle_frags_error::<_, _, LargeObjTypes>(
                        ctx, stream, proto, err
                    );

                    out.merge(&res);
                }
            }
        }

        Ok(out)
    }
}

impl<Msgs, ID, Flags, Msg, Batch, Add, Finish, Cancel> ScopedError
    for PushEntryRecoverableError<
        Msgs,
        ID,
        Flags,
        Msg,
        Batch,
        Add,
        Finish,
        Cancel
    >
where
    Finish: ScopedError,
    Cancel: ScopedError,
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
    for PushEntryRecoverableError<
        Msgs,
        ID,
        Flags,
        Msg,
        Batch,
        Add,
        Finish,
        Cancel
    >
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

                (
                    completable.map(|err| PushEntryRecoverableError::Batch {
                        msgs: msgs,
                        err: err
                    }),
                    permanent.map(|err| PushEntryError::Batch { err: err })
                )
            }
            PushEntryRecoverableError::Add {
                batch_id,
                msgs,
                msg,
                flags,
                err
            } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| PushEntryRecoverableError::Add {
                        batch_id: batch_id.clone(),
                        flags: flags,
                        msgs: msgs,
                        msg: msg,
                        err: err
                    }),
                    permanent.map(|err| PushEntryError::Add {
                        batch_id: batch_id,
                        err: err
                    })
                )
            }
            PushEntryRecoverableError::Finish {
                batch_id,
                flags,
                err
            } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| PushEntryRecoverableError::Finish {
                        batch_id: batch_id.clone(),
                        flags: flags,
                        err: err
                    }),
                    permanent.map(|err| PushEntryError::Finish {
                        batch_id: batch_id,
                        err: err
                    })
                )
            }
            PushEntryRecoverableError::Cancel {
                batch_id,
                flags,
                err
            } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| PushEntryRecoverableError::Cancel {
                        batch_id: batch_id.clone(),
                        flags: flags,
                        err: err
                    }),
                    permanent.map(|err| PushEntryError::Cancel {
                        batch_id: batch_id,
                        err: err
                    })
                )
            }
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

impl<ID, Batch, Add, Finish, Cancel> Display
    for PushEntryError<ID, Batch, Add, Finish, Cancel>
where
    Cancel: Display,
    Finish: Display,
    Batch: Display,
    Add: Display
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
