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

//! Core traits and utilities for streams.
pub mod test;

use std::cell::RefCell;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Instant;

use bitvec::bitvec;
use bitvec::vec::BitVec;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashID;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;
use log::warn;

use crate::config::BatchSlotsConfig;
use crate::error::ErrorReportInfo;
use crate::frags::Frags;
use crate::large_obj::LargeObjID;

/// Core trait for "pull" streams.
///
/// These are streams that operate as "listeners", and will wait on
/// incoming messages.
pub trait PullStream<T> {
    /// Type of errors that can occur in a [pull](PullStream::pull)
    /// operation.
    type PullError: Debug + Display + ScopedError;

    /// Wait for an incoming message.
    fn pull(&mut self) -> Result<T, Self::PullError>;
}

/// Trait for types that can accept a new stream directly.
///
/// This is primarily intended to allow the push-side and the
/// pull-side to report streams to one another.
pub trait StreamReporter<Party, ID, Stream>
where
    ID: Clone + Debug + Display + Eq + Hash {
    /// Type of errors that can happen reporting a stream.
    type ReportStreamError: Debug + Display + ScopedError;

    /// Report a new stream for a counterparty address.
    ///
    /// If a stream already existed for this counterparty, `Some` will
    /// be returned with that stream, and the caller should insert
    /// that stream into its own data structures in place of the
    /// argument stream.  If `None` is returned, then the argument
    /// stream was accepted.
    fn report_stream(
        &mut self,
        party: &Party,
        id: ID,
        stream: Stream
    ) -> Result<Option<Stream>, Self::ReportStreamError>;
}

pub trait StreamRefresh<Ctx> {
    type RefreshRetry: RetryWhen + Clone + Debug;
    type RefreshError: RecoverableError + Debug;

    fn refresh(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    >;

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::RefreshRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    >;

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    >;
}

/// Basic interface for a push stream.
///
/// Push streams support an interface where messages are supplied by a
/// producer, and the consumer is assumed to be continuously receiving.
///
/// Batching is the foundational abstraction for push streams.  The
/// basic functionality is implemented in terms of batching, and
/// sending of single messages is then implemented as a derived form.
/// Thus, the functionality in this trait is concerned with creating,
/// finishing, and canceling batches.  It is expected that any type
/// implementing this trait will also implement [PushStreamAdd], which
/// provides the functionality for adding messages to a batch.
///
/// # Atomicity
///
/// `PushStream` and its sub-traits *do not* in general guarantee
/// atomic semantics regarding the sending of messages.  As these
/// traits represent an abstraction for low-level communications, it
/// is not generally possible to make such a guarantee.  As such, this
/// interface *does not* make guarantees about the existence of a
/// single, discrete point in time where the transmission of messages
/// along some underlying channel can be said to occur (i.e. a
/// linearization point).
pub trait PushStream<Ctx> {
    /// ID for batches.
    type BatchID: Clone + Debug;
    /// Type of errors that can occur when canceling a batch.
    type CancelBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for canceling a new batch.
    type CancelBatchRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when sending a batch.
    type FinishBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for finishing a new batch.
    type FinishBatchRetry: RetryWhen + Clone + Debug;
    /// Type of stream flags used in [add](PushStreamAdd::add).
    type StreamFlags: Default;
    /// Type of error that can occur when reporting failures.
    type ReportError: Debug + Display;

    /// Create an empty
    /// [StreamFlags](PushStream::StreamFlags).
    #[inline]
    fn empty_flags(&self) -> Self::StreamFlags {
        Self::StreamFlags::default()
    }

    /// Create an empty
    /// [StreamFlags](PushStream::StreamFlags).
    fn empty_flags_with_capacity(
        #[allow(unused_variables)] size: usize
    ) -> Self::StreamFlags {
        Self::StreamFlags::default()
    }

    /// Finish a batch, and guarantee that all of its messages have
    /// been sent.
    ///
    /// Note that streams are do not in general guarantee atomic
    /// semantics.  Depending on the underlying stream, the messages
    /// comprising the batch may have already been sent before this
    /// function is called.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>;

    /// Retry a previous call to [finish_batch](PushStream::finish_batch).
    ///
    /// This allows a call to `finish_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `retry`: Retry value from a previous
    ///   [finish_batch](PushStream::finish_batch).
    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>;

    /// Retry a previously-failed call to
    /// [finish_batch](PushStream::finish_batch).
    ///
    /// This allows a call to `finish_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `err`: Completable error returned by a previous call to
    ///   [finish_batch](PushStream::finish_batch).
    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>;

    /// Cancel a pending batch and release any resources allocated to
    /// it.
    ///
    /// This function *does not* make any guarantees that any of the
    /// messages previously added to the batch have not been sent.
    /// Streams are do not in general guarantee atomic semantics.
    /// Depending on the underlying stream, the messages comprising
    /// the batch may have already been sent before this function is
    /// called.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   canceling a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>;

    /// Retry a previous call to [cancel_batch](PushStream::cancel_batch).
    ///
    /// This allows a call to `cancel_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   canceling a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `retry`: Retry value from a previous
    ///   [cancel_batch](PushStream::cancel_batch).
    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>;

    /// Retry a previously-failed call to
    /// [cancel_batch](PushStream::cancel_batch).
    ///
    /// This allows a call to `cancel_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   canceling a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `err`: Completable error returned by a previous call to
    ///   [cancel_batch](PushStream::cancel_batch).
    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>;

    /// Cancel all pending batches.
    ///
    /// This represents a "best-effort" to cancel all pending batches
    /// and clear out all resources allocated to them.
    fn cancel_batches(&mut self);

    /// Report a failure.
    ///
    /// This will report a failure back given a batch ID.  If only an
    /// error is available, use the [PushStreamReportError] instance
    /// instead.
    ///
    /// # Parameters
    ///
    /// - `batch`: ID of the batch for which to report a failure.
    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError>;
}

/// Report an error on a stream.
///
/// This is used to propagate errors back up to a scheduling or
/// selection mechanism, usually
/// [StreamSelector](crate::select::StreamSelector).
pub trait PushStreamReportError<Error> {
    /// Type of errors that can occur reporting the original error.
    type ReportError: Debug + Display;

    /// Report an error that occurred during some stream operation.
    ///
    /// # Parameters
    ///
    /// - `error`: Error to report.
    fn report_error(
        &mut self,
        error: &Error
    ) -> Result<(), Self::ReportError>;
}

/// Report an error on a stream, with a known associated batch.
///
/// This is generally used to report errors associated with compound
/// batches and streams, meaning, those that are composed of multilpe
/// independent sub-streams and therefore whose batches are composed
/// of multiple different batches on each stream.
///
/// In such cases, it is necessary to have both the error and the
/// batch, as batches may not include every sub-stream, and errors may
/// have only occurred on a subset of streams.
///
/// An example of a case where this should be used is found in
/// [ErrorSet](crate::error::ErrorSet) and
/// [CompoundBatchError](crate::error::CompoundBatchError), both of
/// which are associated with
/// [StreamMulticaster](crate::multicast::StreamMulticaster).
pub trait PushStreamReportBatchError<Error, Batch> {
    type ReportBatchError: Debug + Display;

    fn report_error_with_batch(
        &mut self,
        batch: &Batch,
        error: &Error
    ) -> Result<(), Self::ReportBatchError>;
}

/// Interface for adding messages to a batch in a [PushStream].
///
/// This trait provides the functions for adding messages to batches
/// created by the basic `PushStream` functionality.
///
/// # Atomicity
///
/// `PushStream` and its sub-traits *do not* in general guarantee
/// atomic semantics regarding the sending of messages.  As these
/// traits represent an abstraction for low-level communications, it
/// is not generally possible to make such a guarantee.  As such, this
/// interface *does not* make guarantees about the existence of a
/// single, discrete point in time where the transmission of messages
/// along some underlying channel can be said to occur (i.e. a
/// linearization point).
///
/// Regarding this trait, the actual transmission of messages along
/// the underlying channel can happen at *any point* after the message
/// is added to the batch.  It is *not* the case that transmission can
///  happen only when [finish_batch](PushStream::finish_batch) is
///  called successfully.
pub trait PushStreamAdd<T, Ctx>: PushStream<Ctx> {
    /// Type of errors that can occur when adding a message to a batch.
    type AddError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for adding a
    /// message to a batch.
    type AddRetry: RetryWhen + Clone + Debug;

    /// Add a message to a pending batch.
    ///
    /// This will cause the message to be transmitted along the
    /// underlying channel at some point after this function is
    /// called.  The only guarantee made about the timing of the
    /// message transmission is that it will occur no later than a
    /// successful call to [finish_batch](PushStream::finish_batch) on
    /// the same batch.
    ///
    /// There is *no* general guarantee that the transmission will be
    /// delayed, or will occur atomically with the transmission of any
    /// other message.  It is semantically valid for an implementation
    /// to immediately send a message when this function is called.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `msg`: Message to add.
    ///
    /// - `batch`: ID of the batch to finish.
    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError>;

    /// Retry a previous call to [add](PushStreamAdd::add).
    ///
    /// This allows a call to `add` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `msg`: Message to add.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `retry`: Retry value from a previous [add](PushStreamAdd::add).
    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError>;

    /// Retry a previously-failed call to
    /// [add](PushStreamAdd::add).
    ///
    /// This allows a call to `add` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Context to use.
    ///
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to avoid
    ///   finishing a substream multiple times.
    ///
    /// - `msg`: Message to add.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `err`: Completable error returned by a previous call to
    ///   [add](PushStreamAdd::add).
    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError>;
}

pub trait PushStreamPartyID {
    /// Type of party IDs.
    ///
    /// This should typically be a wrapper around a dense integer value.
    type PartyID: Clone + Eq + Hash + Ord;
}

/// Trait for obtaining the list of parties associated with a
/// [PushStreamShared] instance.
pub trait PushStreamParties: PushStreamPartyID {
    /// Iterator for parties.
    type PartiesIter: Iterator<Item = (Self::PartyID, Self::PartyInfo)>;
    /// Detailed information about a party.
    type PartyInfo;
    /// Error that can occur obtaining parties.
    type PartiesError: Debug + Display + ScopedError;

    /// Get an iterator for all parties and their dense IDs.
    fn parties(&self) -> Result<Self::PartiesIter, Self::PartiesError>;
}

pub trait PushStreamShared<Ctx>: PushStream<Ctx> + PushStreamPartyID {
    /// Type of errors that can occur when selecting streams for a new
    /// batch.
    type SelectError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for selecting
    /// streams for a new batch.
    type SelectRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when creating a new batch.
    type CreateBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for creating a new batch.
    type CreateBatchRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when creating a new batch.
    type StartBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for creating a new batch.
    type StartBatchRetry: RetryWhen + Clone + Debug;
    /// Type of information given by a [RetryResult] for aborting a
    /// batch creation.
    type AbortBatchRetry: RetryWhen + Clone + Debug;
    /// Type of selection cache used in
    /// [select](PushStreamShared::select).
    type Selections: Clone + Default;
    /// Type of batch cache used in
    /// [start_batch](PushStreamShared::start_batch).
    type StartBatchStreamBatches: Clone + Default;
    /// Iterator for parties.
    type BatchPartiesIter: Iterator<Item = Self::PartyID>;
    type BatchPartiesError: Debug + Display + ScopedError;
    type IndefParties: IntoIterator<Item = Self::PartyID>;

    /// Create an empty
    /// [Selections](PushStreamShared::Selections).
    #[inline]
    fn empty_selections(&self) -> Self::Selections {
        Self::Selections::default()
    }

    /// Create an empty
    /// [Selections](PushStreamShared::Selections).
    fn empty_selections_with_capacity(
        #[allow(unused_variables)] size: usize
    ) -> Self::Selections {
        Self::Selections::default()
    }

    /// Create an empty
    /// [StartBatchStreamBatches](PushStreamShared::StartBatchStreamBatches).
    #[inline]
    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::default()
    }

    /// Create an empty
    /// [StartBatchStreamBatches](PushStreamShared::StartBatchStreamBatches).
    fn empty_batches_with_capacity(
        #[allow(unused_variables)] size: usize
    ) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::default()
    }

    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError>;

    /// Select streams for a new batch.
    ///
    /// This will do any stream selection, and will record decisions
    /// in `selections`.
    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a;

    /// Retry a previous call to
    /// [start_batch](PushStreamShared::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    >;

    /// Retry a previously-failed call to
    /// [select](PushStreamShared::select).
    ///
    /// This allows a call to `select` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    >;

    /// Create a new batch.
    ///
    /// This creates a new batch, referenced by a
    /// [BatchID](PushStream::BatchID).  This is not meant to be used
    /// directly; [start_batch](PushStreamShared::start_batch) should
    /// be used instead.
    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Retry a previous call to
    /// [create_batch](PushStreamShared::create_batch).
    ///
    /// This allows a call to `create_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Retry a previously-failed call to
    /// [create_batch](PushStreamShared::create_batch).
    ///
    /// This allows a call to `create_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Start a new batch.
    ///
    /// This creates a new batch, referenced by a
    /// [BatchID](PushStream::BatchID), to which messages can be added
    /// using functionality in [PushStreamAdd].
    ///
    /// Depending on the implementation, this may allocate resources
    /// on the underlying stream that will need to be freed using
    /// [finish_batch](PushStream::finish_batch) or
    /// [cancel_batch](PushStream::cancel_batch).
    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a;

    /// Retry a previous call to
    /// [start_batch](PushStreamShared::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    >;

    /// Retry a previously-failed call to
    /// [start_batch](PushStreamShared::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    >;

    /// Abort a previously-failed call to
    /// [start_batch](PushStreamShared::start_batch).
    ///
    /// This will release any resources that were allocated in the
    /// call to [start_batch](PushStreamShared::start_batch).
    ///
    /// In order to avoid an endless cycle, this represents a
    /// "best-effort", and will not return an error.
    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry>;

    /// Retry a previous call to
    /// [abort_start_batch](PushStreamShared::abort_start_batch).
    ///
    /// This allows a call to `abort_start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry>;
}

pub trait PushStreamPrivate<Ctx>: PushStream<Ctx> {
    /// Type of errors that can occur when selecting streams for a new
    /// batch.
    type SelectError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for selecting
    /// streams for a new batch.
    type SelectRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when creating a new batch.
    type CreateBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for creating a new batch.
    type CreateBatchRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when starting a new batch.
    type StartBatchError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for starting a new batch.
    type StartBatchRetry: RetryWhen + Clone + Debug;
    /// Type of information given by a [RetryResult] for aborting a
    /// batch creation.
    type AbortBatchRetry: RetryWhen + Clone + Debug;
    /// Type of selection cache used in
    /// [select](PushStreamPrivate::select).
    type Selections: Clone + Default;
    /// Type of batch cache used in
    /// [start_batch](PushStreamPrivate::create_batch).
    type StartBatchStreamBatches: Clone + Default;

    /// Create an empty
    /// [Selections](PushStreamPrivate::Selections).
    #[inline]
    fn empty_selections(&self) -> Self::Selections {
        Self::Selections::default()
    }

    /// Create an empty
    /// [Selections](PushStreamPrivate::Selections).
    fn empty_selections_with_capacity(
        #[allow(unused_variables)] size: usize
    ) -> Self::Selections {
        Self::Selections::default()
    }

    /// Create an empty
    /// [StartBatchStreamBatches](PushStreamPrivate::StartBatchStreamBatches).
    #[inline]
    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::default()
    }

    /// Create an empty
    /// [StartBatchStreamBatches](PushStreamPrivate::StartBatchStreamBatches).
    fn empty_batches_with_capacity(
        #[allow(unused_variables)] size: usize
    ) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::default()
    }

    /// Select streams for a new batch.
    ///
    /// This will do any stream selection, and will record decisions
    /// in `selections`.
    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>;

    /// Retry a previous call to
    /// [start_batch](PushStreamPrivate::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>;

    /// Retry a previously-failed call to
    /// [select](PushStreamPrivate::select).
    ///
    /// This allows a call to `select` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>;

    /// Create a new batch.
    ///
    /// This creates a new batch, referenced by a
    /// [BatchID](PushStream::BatchID).  This is not meant to be used
    /// directly; [start_batch](PushStreamPrivate::start_batch) should
    /// be used instead.
    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Retry a previous call to
    /// [create_batch](PushStreamPrivate::create_batch).
    ///
    /// This allows a call to `create_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Retry a previously-failed call to
    /// [create_batch](PushStreamPrivate::create_batch).
    ///
    /// This allows a call to `create_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    >;

    /// Start a new batch.
    ///
    /// This creates a new batch, referenced by a
    /// [BatchID](PushStream::BatchID), to which messages can be added
    /// using functionality in [PushStreamAdd].
    ///
    /// Depending on the implementation, this may allocate resources
    /// on the underlying stream that will need to be freed using
    /// [finish_batch](PushStream::finish_batch) or
    /// [cancel_batch](PushStream::cancel_batch).
    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Retry a previous call to
    /// [start_batch](PushStreamPrivate::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Retry a previously-failed call to
    /// [start_batch](PushStreamPrivate::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Abort a previously-failed call to
    /// [start_batch](PushStreamPrivate::start_batch).
    ///
    /// This will release any resources that were allocated in the
    /// call to [start_batch](PushStreamPrivate::start_batch).
    ///
    /// In order to avoid an endless cycle, this represents a
    /// "best-effort", and will not return an error.
    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry>;

    /// Retry a previous call to
    /// [abort_start_batch](PushStreamPrivate::abort_start_batch).
    ///
    /// This allows a call to `abort_start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry>;
}

pub trait LargeObjStream<Ctx> {
    /// Type of errors that can occur when sending a fragment.
    type PushFragError: RecoverableError;
    /// Type of information given by a [RetryResult] for sending a
    /// single message.
    type PushFragRetry: RetryWhen + Clone + Debug;
    /// Type of outbound fragment structures.
    type Frags: Frags;
    type Parties;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    >;

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    >;

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    >;
}

pub trait LargeObjOfferStream<H, Ctx>: LargeObjStream<Ctx>
where
    H: HashID {
    /// Type of errors that can occur when sending an offer.
    type PushOfferError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for sending an
    /// offer.
    type PushOfferRetry: RetryWhen + Clone + Debug;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    >;

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    >;

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    >;
}

/// Helper trait for sending single messages on shared streams.
///
/// This trait implements functionality for sending a single message
/// on a shared [PushStream] instance, where multiple recipient
/// parties may be specified.  In many cases, this will be a "derived
/// form", using the batching functionality to send a batch of size
/// one.  In some cases; however, it may be a more efficient
/// implementation.
pub trait PushStreamSharedSingle<T, Ctx>:
    PushStreamAdd<T, Ctx> + PushStreamShared<Ctx> {
    /// Type of errors that can occur when sending a single message.
    type PushError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for sending a
    /// single message.
    type PushRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when canceling a failed single
    /// message.
    type CancelPushError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for canceling a
    /// single message.
    type CancelPushRetry: RetryWhen + Clone + Debug;

    /// Push a single message into the stream.
    ///
    /// Semantically, this is the equivalent of generating a
    /// single-element batch, and may often be implemented that way.
    /// The batch ID returned is for downstream tracking purposes, and
    /// can be used as the equivalent of a message ID.
    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &T
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a;

    /// Retry a previous call to [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    >;

    /// Retry a previously-failed call to
    /// [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    >;

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;
}

/// Helper trait for sending single messages.
///
/// This trait implements functionality for sending a single message
/// on a [PushStream].  In many cases, this will be a "derived form",
/// using the batching functionality to send a batch of size one.  In
/// some cases; however, it may be a more efficient implementation.
pub trait PushStreamPrivateSingle<T, Ctx>:
    PushStreamAdd<T, Ctx> + PushStreamPrivate<Ctx> {
    /// Type of errors that can occur when sending a single message.
    type PushError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for sending a
    /// single message.
    type PushRetry: RetryWhen + Clone + Debug;
    /// Type of errors that can occur when canceling a failed single
    /// message.
    type CancelPushError: RecoverableError + Debug;
    /// Type of information given by a [RetryResult] for canceling a
    /// single message.
    type CancelPushRetry: RetryWhen + Clone + Debug;

    /// Push a single message into the stream.
    ///
    /// Semantically, this is the equivalent of generating a
    /// single-element batch, and may often be implemented that way.
    /// The batch ID returned is for downstream tracking purposes, and
    /// can be used as the equivalent of a message ID.
    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    /// Retry a previous call to [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    /// Retry a previously-failed call to
    /// [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;
}

/// Indicator for some or all parties.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Parties<P> {
    /// Some subset of parties.
    Some(P),
    /// All parties.
    All
}

/// Unique identifier for streams.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StreamID<Addr, ChannelID, Param> {
    /// Counterparty address.
    party_addr: Addr,
    /// Specific channel ID.
    channel: ChannelID,
    /// Channel parameter set.
    param: Param
}

/// Batch IDs used by [CompoundBatches].
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompoundBatchID(usize);

/// Structure for maintaining a collection of compound batches,
/// consisting of multiple individual batches on other streams.
///
/// This type represents common functionality used in the
/// implementation of [PushStream] and its sub-traits.  It is often
/// the case that stream combinators will need to store information
/// about a batch that consists of multiple sub-batches on child
/// streams.  This type implements this functionality.
pub struct CompoundBatches<Batch> {
    batches: Vec<Option<Batch>>,
    avail: BitVec,
    config: BatchSlotsConfig
}

/// Wrapper around [PushStream] and sub-traits that adds
/// clonability.
pub struct RefCellStream<Inner> {
    inner: Rc<RefCell<Inner>>
}

/// Errors that can occur from [RefCellStream]s.
#[derive(Debug)]
pub enum RefCellStreamError<Inner> {
    Inner { error: Inner },
    Borrow
}

#[derive(Debug)]
pub struct RefCellStreamBorrowError;

/// Type used to combine results from
/// [finish_batch](PushStream::finish_batch) and
/// [cancel_batch](PushStream::cancel_batch).
#[derive(Clone, Debug)]
pub enum StreamFinishCancel<Finish, Cancel> {
    /// Result from [finish_batch](PushStream::finish_batch).
    Finish {
        /// Result value from [finish_batch](PushStream::finish_batch).
        finish: Finish
    },
    /// Result from [cancel_batch](PushStream::cancel_batch).
    Cancel {
        /// Result value from [cancel_batch](PushStream::cancel_batch).
        cancel: Cancel
    }
}

/// A [StreamReporter] that simply ignores all reported streams.
///
/// This is intended primarily for testing.
pub struct PassthruReporter<Addr, Prin, Stream> {
    stream: PhantomData<Stream>,
    prin: PhantomData<Prin>,
    addr: PhantomData<Addr>
}

impl<P> Parties<P> {
    #[inline]
    pub fn map<F, Q>(
        self,
        f: F
    ) -> Parties<Q>
    where
        F: FnOnce(P) -> Q {
        match self {
            Parties::All => Parties::All,
            Parties::Some(parties) => Parties::Some(f(parties))
        }
    }
}

impl<Finish, Cancel, T> ErrorReportInfo<T>
    for StreamFinishCancel<Finish, Cancel>
where
    Finish: ErrorReportInfo<T>,
    Cancel: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        match self {
            StreamFinishCancel::Finish { finish } => finish.report_info(),
            StreamFinishCancel::Cancel { cancel } => cancel.report_info()
        }
    }
}

impl<Finish, Cancel> RecoverableError for StreamFinishCancel<Finish, Cancel>
where
    Finish: RecoverableError,
    Cancel: RecoverableError
{
    type Completable =
        StreamFinishCancel<Finish::Completable, Cancel::Completable>;
    type Permanent = StreamFinishCancel<Finish::Permanent, Cancel::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            StreamFinishCancel::Finish { finish } => {
                let (completable, permanent) = finish.split();

                (
                    completable
                        .map(|err| StreamFinishCancel::Finish { finish: err }),
                    permanent
                        .map(|err| StreamFinishCancel::Finish { finish: err })
                )
            }
            StreamFinishCancel::Cancel { cancel } => {
                let (completable, permanent) = cancel.split();

                (
                    completable
                        .map(|err| StreamFinishCancel::Cancel { cancel: err }),
                    permanent
                        .map(|err| StreamFinishCancel::Cancel { cancel: err })
                )
            }
        }
    }
}

impl<Finish, Cancel> ScopedError for StreamFinishCancel<Finish, Cancel>
where
    Finish: ScopedError,
    Cancel: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamFinishCancel::Finish { finish } => finish.scope(),
            StreamFinishCancel::Cancel { cancel } => cancel.scope()
        }
    }
}

impl<Finish, Cancel> RetryWhen for StreamFinishCancel<Finish, Cancel>
where
    Finish: RetryWhen,
    Cancel: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            StreamFinishCancel::Finish { finish } => finish.when(),
            StreamFinishCancel::Cancel { cancel } => cancel.when()
        }
    }
}

impl<Inner, T> ErrorReportInfo<T> for RefCellStreamError<Inner>
where
    Inner: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let RefCellStreamError::Inner { error } = self {
            error.report_info()
        } else {
            None
        }
    }
}

impl<Addr, ChannelID, Param> StreamID<Addr, ChannelID, Param>
where
    Addr: Display,
    ChannelID: Display,
    Param: Display
{
    /// Create a new `StreamID`
    #[inline]
    pub fn new(
        party_addr: Addr,
        channel: ChannelID,
        param: Param
    ) -> Self {
        StreamID {
            party_addr: party_addr,
            channel: channel,
            param: param
        }
    }

    /// Get the counterparty address.
    #[inline]
    pub fn party_addr(&self) -> &Addr {
        &self.party_addr
    }

    /// Get the channel ID.
    #[inline]
    pub fn channel(&self) -> &ChannelID {
        &self.channel
    }

    /// Get the parameter.
    #[inline]
    pub fn param(&self) -> &Param {
        &self.param
    }

    /// Decompose into a counterparty address, channel ID, and parameter.
    #[inline]
    pub fn take(self) -> (Addr, ChannelID, Param) {
        (self.party_addr, self.channel, self.param)
    }
}

impl<Party, ID, Stream, Inner> StreamReporter<Party, ID, Stream>
    for RefCellStream<Inner>
where
    ID: Clone + Debug + Display + Eq + Hash,
    Inner: StreamReporter<Party, ID, Stream>
{
    type ReportStreamError = RefCellStreamError<Inner::ReportStreamError>;

    fn report_stream(
        &mut self,
        party: &Party,
        id: ID,
        stream: Stream
    ) -> Result<Option<Stream>, Self::ReportStreamError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .report_stream(party, id, stream)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> PushStream<Ctx> for RefCellStream<Inner>
where
    Inner: PushStream<Ctx>
{
    type BatchID = Inner::BatchID;
    type CancelBatchError = RefCellStreamError<Inner::CancelBatchError>;
    type CancelBatchRetry = Inner::CancelBatchRetry;
    type FinishBatchError = RefCellStreamError<Inner::FinishBatchError>;
    type FinishBatchRetry = Inner::FinishBatchRetry;
    type ReportError = RefCellStreamError<Inner::ReportError>;
    type StreamFlags = Inner::StreamFlags;

    #[inline]
    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
        Inner::empty_flags_with_capacity(size)
    }

    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .finish_batch(ctx, flags, batch)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_finish_batch(ctx, flags, batch, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_finish_batch(ctx, flags, batch, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .cancel_batch(ctx, flags, batch)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_cancel_batch(ctx, flags, batch, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_cancel_batch(ctx, flags, batch, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn cancel_batches(&mut self) {
        match self.inner.try_borrow_mut() {
            Ok(mut guard) => guard.cancel_batches(),
            Err(_) => {
                error!(target: "ref-cell-stream",
                       "try_borrow_mut failed");
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .report_failure(batch)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> StreamRefresh<Ctx> for RefCellStream<Inner>
where
    Inner: StreamRefresh<Ctx>
{
    type RefreshError = RefCellStreamError<Inner::RefreshError>;
    type RefreshRetry = Inner::RefreshRetry;

    fn refresh(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .refresh(ctx)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::RefreshRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_refresh(ctx, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_refresh(ctx, errs)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Inner, Error> PushStreamReportError<Error> for RefCellStream<Inner>
where
    Inner: PushStreamReportError<Error>
{
    type ReportError = RefCellStreamError<
        <Inner as PushStreamReportError<Error>>::ReportError
    >;

    fn report_error(
        &mut self,
        error: &Error
    ) -> Result<(), Self::ReportError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .report_error(error)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Inner, Error, Batch> PushStreamReportBatchError<Error, Batch>
    for RefCellStream<Inner>
where
    Inner: PushStreamReportBatchError<Error, Batch>
{
    type ReportBatchError = RefCellStreamError<
        <Inner as PushStreamReportBatchError<Error, Batch>>::ReportBatchError
    >;

    fn report_error_with_batch(
        &mut self,
        batch: &Batch,
        error: &Error
    ) -> Result<(), Self::ReportBatchError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .report_error_with_batch(batch, error)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<T, Ctx, Inner> PushStreamAdd<T, Ctx> for RefCellStream<Inner>
where
    Inner: PushStreamAdd<T, Ctx>
{
    type AddError = RefCellStreamError<Inner::AddError>;
    type AddRetry = Inner::AddRetry;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .add(ctx, flags, msg, batch)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_add(ctx, flags, msg, batch, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_add(ctx, flags, msg, batch, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Inner> PushStreamPartyID for RefCellStream<Inner>
where
    Inner: PushStreamPartyID
{
    type PartyID = Inner::PartyID;
}

impl<Inner> PushStreamParties for RefCellStream<Inner>
where
    Inner: PushStreamParties
{
    type PartiesError = RefCellStreamError<Inner::PartiesError>;
    type PartiesIter = Inner::PartiesIter;
    type PartyInfo = Inner::PartyInfo;

    fn parties(&self) -> Result<Inner::PartiesIter, Self::PartiesError> {
        self.inner
            .try_borrow()
            .map_err(|_| RefCellStreamError::Borrow)?
            .parties()
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> PushStreamPrivate<Ctx> for RefCellStream<Inner>
where
    Inner: PushStreamPrivate<Ctx>
{
    type AbortBatchRetry = Inner::AbortBatchRetry;
    type CreateBatchError = RefCellStreamError<Inner::CreateBatchError>;
    type CreateBatchRetry = Inner::CreateBatchRetry;
    type SelectError = RefCellStreamError<Inner::SelectError>;
    type SelectRetry = Inner::SelectRetry;
    type Selections = Inner::Selections;
    type StartBatchError = RefCellStreamError<Inner::StartBatchError>;
    type StartBatchRetry = Inner::StartBatchRetry;
    type StartBatchStreamBatches = Inner::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        Inner::empty_selections_with_capacity(size)
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Inner::empty_batches_with_capacity(size)
    }

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .select(ctx, selections)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_select(ctx, selections, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_select(ctx, selections, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .create_batch(ctx, batches, selections)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_create_batch(ctx, batches, selections, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_create_batch(ctx, batches, selections, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .start_batch(ctx)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_start_batch(ctx, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_start_batch(ctx, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match err {
            RefCellStreamError::Inner { error } => {
                match self.inner.try_borrow_mut() {
                    Ok(mut guard) => guard.abort_start_batch(ctx, flags, error),
                    Err(_) => {
                        error!(target: "ref-cell-stream",
                           "try_borrow_mut failed");

                        RetryResult::Success(())
                    }
                }
            }
            RefCellStreamError::Borrow => {
                warn!(target: "threaded-stream",
                      "could not cancel batch with error: mutex poisoned");

                RetryResult::Success(())
            }
        }
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match self.inner.try_borrow_mut() {
            Ok(mut guard) => guard.retry_abort_start_batch(ctx, flags, retry),
            Err(_) => {
                error!(target: "ref-cell-stream",
                       "try_borrow_mut failed");

                RetryResult::Success(())
            }
        }
    }
}

impl<Ctx, Inner> PushStreamShared<Ctx> for RefCellStream<Inner>
where
    Inner: PushStreamShared<Ctx>
{
    type AbortBatchRetry = Inner::AbortBatchRetry;
    type BatchPartiesError = RefCellStreamError<Inner::BatchPartiesError>;
    type BatchPartiesIter = Inner::BatchPartiesIter;
    type CreateBatchError = RefCellStreamError<Inner::CreateBatchError>;
    type CreateBatchRetry = Inner::CreateBatchRetry;
    type IndefParties = Inner::IndefParties;
    type SelectError = RefCellStreamError<Inner::SelectError>;
    type SelectRetry = Inner::SelectRetry;
    type Selections = Inner::Selections;
    type StartBatchError = RefCellStreamError<Inner::StartBatchError>;
    type StartBatchRetry = Inner::StartBatchRetry;
    type StartBatchStreamBatches = Inner::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        Inner::empty_selections_with_capacity(size)
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Inner::empty_batches_with_capacity(size)
    }

    #[inline]
    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError> {
        self.inner
            .try_borrow()
            .map_err(|_| RefCellStreamError::Borrow)?
            .batch_parties(batch_id)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .select(ctx, selections, parties)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_select(ctx, selections, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_select(ctx, selections, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .create_batch(ctx, batches, selections)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_create_batch(ctx, batches, selections, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_create_batch(ctx, batches, selections, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .start_batch(ctx, parties)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_start_batch(ctx, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_start_batch(ctx, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match err {
            RefCellStreamError::Inner { error } => {
                match self.inner.try_borrow_mut() {
                    Ok(mut guard) => guard.abort_start_batch(ctx, flags, error),
                    Err(_) => {
                        error!(target: "ref-cell-stream",
                           "try_borrow_mut failed");

                        RetryResult::Success(())
                    }
                }
            }
            RefCellStreamError::Borrow => {
                warn!(target: "threaded-stream",
                      "could not cancel batch with error: mutex poisoned");

                RetryResult::Success(())
            }
        }
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match self.inner.try_borrow_mut() {
            Ok(mut guard) => guard.retry_abort_start_batch(ctx, flags, retry),
            Err(_) => {
                error!(target: "ref-cell-stream",
                       "try_borrow_mut failed");

                RetryResult::Success(())
            }
        }
    }
}

impl<T, Ctx, Inner> PushStreamSharedSingle<T, Ctx> for RefCellStream<Inner>
where
    Inner: PushStreamSharedSingle<T, Ctx>
{
    type CancelPushError = RefCellStreamError<Inner::CancelPushError>;
    type CancelPushRetry = Inner::CancelPushRetry;
    type PushError = RefCellStreamError<Inner::PushError>;
    type PushRetry = Inner::PushRetry;

    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &T
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .push(ctx, parties, msg)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_push(ctx, msg, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_push(ctx, msg, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            RefCellStreamError::Inner { error } => self
                .inner
                .try_borrow_mut()
                .map_err(|_| RefCellStreamError::Borrow)?
                .cancel_push(ctx, error)
                .map_err(|err| RefCellStreamError::Inner { error: err }),
            RefCellStreamError::Borrow => Ok(RetryResult::Success(()))
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_cancel_push(ctx, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_cancel_push(ctx, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<T, Ctx, Inner> PushStreamPrivateSingle<T, Ctx> for RefCellStream<Inner>
where
    Inner: PushStreamPrivateSingle<T, Ctx>
{
    type CancelPushError = RefCellStreamError<Inner::CancelPushError>;
    type CancelPushRetry = Inner::CancelPushRetry;
    type PushError = RefCellStreamError<Inner::PushError>;
    type PushRetry = Inner::PushRetry;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .push(ctx, msg)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_push(ctx, msg, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_push(ctx, msg, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            RefCellStreamError::Inner { error } => self
                .inner
                .try_borrow_mut()
                .map_err(|_| RefCellStreamError::Borrow)?
                .cancel_push(ctx, error)
                .map_err(|err| RefCellStreamError::Inner { error: err }),
            RefCellStreamError::Borrow => Ok(RetryResult::Success(()))
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_cancel_push(ctx, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_cancel_push(ctx, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<T, Inner> PullStream<T> for RefCellStream<Inner>
where
    Inner: PullStream<T>
{
    type PullError = RefCellStreamError<Inner::PullError>;

    fn pull(&mut self) -> Result<T, Self::PullError> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .pull()
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> LargeObjStream<Ctx> for RefCellStream<Inner>
where
    Inner: LargeObjStream<Ctx>
{
    type Frags = Inner::Frags;
    type Parties = Inner::Parties;
    type PushFragError = RefCellStreamError<Inner::PushFragError>;
    type PushFragRetry = Inner::PushFragRetry;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .push_frags(ctx, id, frags)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_push_frags(ctx, id, frags, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_push_frags(ctx, id, frags, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Ctx, H, Inner> LargeObjOfferStream<H, Ctx> for RefCellStream<Inner>
where
    Inner: LargeObjOfferStream<H, Ctx>,
    H: HashID
{
    type PushOfferError = RefCellStreamError<Inner::PushOfferError>;
    type PushOfferRetry = Inner::PushOfferRetry;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .push_offer(ctx, hash, frags)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .retry_push_offer(ctx, hash, frags, retry)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.inner
            .try_borrow_mut()
            .map_err(|_| RefCellStreamError::Borrow)?
            .complete_push_offer(ctx, hash, frags, err)
            .map_err(|err| RefCellStreamError::Inner { error: err })
    }
}

impl<Inner> Clone for RefCellStream<Inner> {
    #[inline]
    fn clone(&self) -> Self {
        RefCellStream {
            inner: self.inner.clone()
        }
    }
}

impl<Inner> RefCellStream<Inner> {
    /// Create a new `RefCellStream` from its inner stream.
    #[inline]
    pub fn new(inner: Inner) -> Self {
        RefCellStream {
            inner: Rc::new(RefCell::new(inner))
        }
    }

    #[inline]
    pub fn into_inner(self) -> Option<Inner> {
        Rc::into_inner(self.inner).map(|val| val.into_inner())
    }
}

impl<Batch> CompoundBatches<Batch>
where
    Batch: Clone
{
    #[inline]
    pub fn create(config: BatchSlotsConfig) -> CompoundBatches<Batch> {
        CompoundBatches {
            batches: vec![None; config.min_batch_slots()],
            avail: bitvec![1; config.min_batch_slots()],
            config: config
        }
    }

    #[inline]
    pub fn get(
        &self,
        id: &CompoundBatchID
    ) -> Option<&Batch> {
        let idx: usize = id.into();

        self.batches[idx].as_ref()
    }

    #[inline]
    pub fn get_mut(
        &mut self,
        id: &CompoundBatchID
    ) -> Option<&mut Batch> {
        let idx: usize = id.into();

        self.batches[idx].as_mut()
    }

    pub fn alloc_batch(
        &mut self,
        batch: Batch
    ) -> CompoundBatchID {
        match self.avail.first_one() {
            // We got a batch slot.
            Some(avail) => {
                self.avail.set(avail, false);

                // Smoke-check, make sure there's not a batch still here.
                match self.batches[avail] {
                    None => {
                        self.batches[avail] = Some(batch);

                        trace!(target: "compound-batches",
                               "allocated batch slot {}",
                               avail);

                        CompoundBatchID::from(avail)
                    }
                    // This should never happen.
                    Some(_) => {
                        error!(target: "compound-batches",
                               "batch slot {} has a lingering batch",
                               avail);

                        self.alloc_batch(batch)
                    }
                }
            }
            // We need to expand the batch slots.
            None => {
                let newlen =
                    (self.batches.len() as f32) * self.config.extend_ratio();
                let newlen = newlen.ceil() as usize;
                let newlen = if newlen > self.batches.len() {
                    newlen
                } else {
                    self.batches.len() + 1
                };

                debug!(target: "compound-batches",
                       "expanding batch slots from {} to {}",
                       self.batches.len(), newlen);

                self.avail.resize(newlen, true);
                self.batches.resize(newlen, None);

                self.alloc_batch(batch)
            }
        }
    }

    pub fn free_batch(
        &mut self,
        batch: &CompoundBatchID
    ) {
        // Free the batch slot.
        let idx: usize = batch.into();

        self.batches[idx] = None;
        self.avail.set(idx, true);

        trace!(target: "compound-batches",
               "freed batch slot {}",
               idx);

        // Reduce the number of slots if we need to.
        let nfilled = self.avail.count_ones();
        let fill_ratio = (nfilled as f32) / (self.batches.len() as f32);

        if fill_ratio < self.config.min_fill_ratio() &&
            self.batches.len() > self.config.min_batch_slots()
        {
            trace!(target: "compound-batches",
                   "trying to reduce batch slots (fill ratio = {})",
                   fill_ratio);

            let last_used =
                self.avail.last_zero().unwrap_or(self.batches.len());
            let target =
                (self.batches.len() as f32) * (self.config.reduce_ratio());
            let target = target.floor() as usize;
            let newlen = target.max(last_used);

            debug!(target: "compound-batches",
                   "reducing batch slots from {} to {}",
                   self.batches.len(), newlen);

            self.batches.truncate(newlen);
            self.avail.truncate(newlen);
        }
    }

    pub fn clear(&mut self) {
        self.batches.truncate(self.config.min_batch_slots());
        self.avail.truncate(self.config.min_batch_slots());

        for i in 0..self.config.min_batch_slots() {
            self.batches[i] = None;
            self.avail.set(i, true);
        }
    }
}

impl From<usize> for CompoundBatchID {
    #[inline]
    fn from(val: usize) -> CompoundBatchID {
        CompoundBatchID(val)
    }
}

impl From<&CompoundBatchID> for usize {
    #[inline]
    fn from(val: &CompoundBatchID) -> usize {
        val.0
    }
}

impl From<CompoundBatchID> for usize {
    #[inline]
    fn from(val: CompoundBatchID) -> usize {
        usize::from(&val)
    }
}

impl<Inner> ScopedError for RefCellStreamError<Inner>
where
    Inner: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            RefCellStreamError::Inner { error } => error.scope(),
            RefCellStreamError::Borrow => ErrorScope::Unrecoverable
        }
    }
}

impl ScopedError for RefCellStreamBorrowError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        ErrorScope::Unrecoverable
    }
}

impl<Inner> RecoverableError for RefCellStreamError<Inner>
where
    Inner: RecoverableError
{
    type Completable = Inner::Completable;
    type Permanent = RefCellStreamError<Inner::Permanent>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            RefCellStreamError::Inner { error } => {
                let (completable, permanent) = error.split();

                (
                    completable,
                    permanent
                        .map(|err| RefCellStreamError::Inner { error: err })
                )
            }
            RefCellStreamError::Borrow => {
                (None, Some(RefCellStreamError::Borrow))
            }
        }
    }
}

impl Display for CompoundBatchID {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl<Inner> Display for RefCellStreamError<Inner>
where
    Inner: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            RefCellStreamError::Inner { error } => error.fmt(f),
            RefCellStreamError::Borrow => write!(f, "try_borrow failed")
        }
    }
}

impl Display for RefCellStreamBorrowError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        write!(f, "try_borrow failed")
    }
}

impl<Addr, ChannelID, Param> Display for StreamID<Addr, ChannelID, Param>
where
    Addr: Display,
    ChannelID: Display,
    Param: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(
            f,
            "to {} over {} ({})",
            self.party_addr, self.channel, self.param
        )
    }
}

impl<Finish, Cancel> Display for StreamFinishCancel<Finish, Cancel>
where
    Finish: Display,
    Cancel: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamFinishCancel::Finish { finish } => finish.fmt(f),
            StreamFinishCancel::Cancel { cancel } => cancel.fmt(f)
        }
    }
}
