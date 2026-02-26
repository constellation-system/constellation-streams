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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use bitvec::bitvec;
use bitvec::vec::BitVec;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
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
where ID: Clone + Debug + Display + Eq + Hash
{
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

pub trait StreamRefresh<Ctx>
{
    type RefreshRetry: RetryWhen + Clone + Debug;
    type RefreshError: RecoverableError + Debug;

    fn refresh(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError>;

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::RefreshRetry
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError>;

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError>;
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
    type BatchID: Clone;
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
    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
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
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
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
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid canceling a substream multiple times.
    ///
    /// - `batch`: ID of the batch to finish.
    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid canceling a substream multiple times.
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
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid canceling a substream multiple times.
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
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError>;

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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
    ///
    /// - `msg`: Message to add.
    ///
    /// - `batch`: ID of the batch to finish.
    ///
    /// - `retry`: Retry value from a previous
    ///   [add](PushStreamAdd::add).
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
    /// - `flags`: A [StreamFlags](PushStream::StreamFlags) to use to
    ///   avoid finishing a substream multiple times.
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
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
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
        size: usize
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
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError>
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
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError>;

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
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Self::IndefParties>,
                Self::SelectError>;

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
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Parties<Self::IndefParties>>,
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
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Parties<Self::IndefParties>>,
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
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Self::IndefParties>,
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
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
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
        size: usize
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
        ctx: &mut Ctx,
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
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    >;

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    >;

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
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
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    >;

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    >;

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
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
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>
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
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>;

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
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>;

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
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>;

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
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>;

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
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>;

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
#[derive(Clone)]
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
/// synchronization.
pub struct ThreadedStream<Inner> {
    shutdown: ShutdownFlag,
    inner: Arc<Mutex<Inner>>
}

/// Errors that can occur from [ThreadedStream]s.
#[derive(Debug)]
pub enum ThreadedStreamError<Inner> {
    Inner { error: Inner },
    MutexPoison,
    Shutdown
}

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

impl<Inner, T> ErrorReportInfo<T> for ThreadedStreamError<Inner>
where
    Inner: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let ThreadedStreamError::Inner { error } = self {
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
    for ThreadedStream<Inner>
where
    ID: Clone + Debug + Display + Eq + Hash,
    Inner: StreamReporter<Party, ID, Stream>
{
    type ReportStreamError = WithMutexPoison<Inner::ReportStreamError>;

    fn report_stream(
        &mut self,
        party: &Party,
        id: ID,
        stream: Stream
    ) -> Result<Option<Stream>, Self::ReportStreamError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?;

        guard
            .report_stream(party, id, stream)
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }
}

impl<Ctx, Inner> PushStream<Ctx> for ThreadedStream<Inner>
where
    Inner: PushStream<Ctx>
{
    type BatchID = Inner::BatchID;
    type CancelBatchError = ThreadedStreamError<Inner::CancelBatchError>;
    type CancelBatchRetry = Inner::CancelBatchRetry;
    type FinishBatchError = ThreadedStreamError<Inner::FinishBatchError>;
    type FinishBatchRetry = Inner::FinishBatchRetry;
    type ReportError = ThreadedStreamError<Inner::ReportError>;
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .finish_batch(ctx, flags, batch)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_finish_batch(ctx, flags, batch, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_finish_batch(ctx, flags, batch, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .cancel_batch(ctx, flags, batch)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_cancel_batch(ctx, flags, batch, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_cancel_batch(ctx, flags, batch, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn cancel_batches(&mut self) {
        match self.inner.lock() {
            Ok(mut guard) => guard.cancel_batches(),
            Err(_) => {
                error!(target: "threaded-stream",
                       "mutex poisoned");
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .report_failure(batch)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> StreamRefresh<Ctx> for ThreadedStream<Inner>
where
    Inner: StreamRefresh<Ctx>
{
    type RefreshRetry = Inner::RefreshRetry;
    type RefreshError = ThreadedStreamError<Inner::RefreshError>;

    fn refresh(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError> {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .refresh(ctx)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::RefreshRetry
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError> {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .retry_refresh(ctx, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError> {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .complete_refresh(ctx, errs)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Inner, Error> PushStreamReportError<Error> for ThreadedStream<Inner>
where
    Inner: PushStreamReportError<Error>
{
    type ReportError = ThreadedStreamError<
        <Inner as PushStreamReportError<Error>>::ReportError
    >;

    fn report_error(
        &mut self,
        error: &Error
    ) -> Result<(), Self::ReportError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .report_error(error)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Inner, Error, Batch> PushStreamReportBatchError<Error, Batch>
    for ThreadedStream<Inner>
where
    Inner: PushStreamReportBatchError<Error, Batch>
{
    type ReportBatchError = ThreadedStreamError<
        <Inner as PushStreamReportBatchError<Error, Batch>>::ReportBatchError
    >;

    fn report_error_with_batch(
        &mut self,
        batch: &Batch,
        error: &Error
    ) -> Result<(), Self::ReportBatchError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .report_error_with_batch(batch, error)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<T, Ctx, Inner> PushStreamAdd<T, Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamAdd<T, Ctx>
{
    type AddError = ThreadedStreamError<Inner::AddError>;
    type AddRetry = Inner::AddRetry;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .add(ctx, flags, msg, batch)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_add(ctx, flags, msg, batch, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_add(ctx, flags, msg, batch, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Inner> PushStreamPartyID for ThreadedStream<Inner>
where
    Inner: PushStreamPartyID
{
    type PartyID = Inner::PartyID;
}

impl<Inner> PushStreamParties for ThreadedStream<Inner>
where
    Inner: PushStreamParties
{
    type PartiesError = ThreadedStreamError<Inner::PartiesError>;
    type PartiesIter = Inner::PartiesIter;
    type PartyInfo = Inner::PartyInfo;

    fn parties(&self) -> Result<Inner::PartiesIter, Self::PartiesError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .parties()
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Ctx, Inner> PushStreamPrivate<Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamPrivate<Ctx>
{
    type AbortBatchRetry = Inner::AbortBatchRetry;
    type CreateBatchError = ThreadedStreamError<Inner::CreateBatchError>;
    type CreateBatchRetry = Inner::CreateBatchRetry;
    type SelectError = ThreadedStreamError<Inner::SelectError>;
    type SelectRetry = Inner::SelectRetry;
    type Selections = Inner::Selections;
    type StartBatchError = ThreadedStreamError<Inner::StartBatchError>;
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
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .select(ctx, selections)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_select(ctx, selections, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_select(ctx, selections, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .create_batch(ctx, batches, selections)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_create_batch(ctx, batches, selections, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_create_batch(ctx, batches, selections, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .start_batch(ctx)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_start_batch(ctx, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_start_batch(ctx, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match err {
            ThreadedStreamError::Inner { error } => match self.inner.lock() {
                Ok(mut guard) => guard.abort_start_batch(ctx, flags, error),
                Err(_) => {
                    error!(target: "threaded-stream",
                           "mutex poisoned");

                    RetryResult::Success(())
                }
            },
            ThreadedStreamError::MutexPoison => {
                warn!(target: "threaded-stream",
                      "could not cancel batch with error: mutex poisoned");

                RetryResult::Success(())
            }
            ThreadedStreamError::Shutdown => RetryResult::Success(())
        }
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match self.inner.lock() {
            Ok(mut guard) => guard.retry_abort_start_batch(ctx, flags, retry),
            Err(_) => {
                error!(target: "threaded-stream",
                       "mutex poisoned");

                RetryResult::Success(())
            }
        }
    }
}

impl<Ctx, Inner> PushStreamShared<Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamShared<Ctx>
{
    type AbortBatchRetry = Inner::AbortBatchRetry;
    type CreateBatchError = ThreadedStreamError<Inner::CreateBatchError>;
    type CreateBatchRetry = Inner::CreateBatchRetry;
    type SelectError = ThreadedStreamError<Inner::SelectError>;
    type SelectRetry = Inner::SelectRetry;
    type Selections = Inner::Selections;
    type StartBatchError = ThreadedStreamError<Inner::StartBatchError>;
    type StartBatchRetry = Inner::StartBatchRetry;
    type StartBatchStreamBatches = Inner::StartBatchStreamBatches;
    type BatchPartiesIter = Inner::BatchPartiesIter;
    type BatchPartiesError = ThreadedStreamError<Inner::BatchPartiesError>;
    type IndefParties = Inner::IndefParties;

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
        let guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .batch_parties(batch_id)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .select(ctx, selections, parties)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_select(ctx, selections, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Self::IndefParties>,
                Self::SelectError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_select(ctx, selections, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .create_batch(ctx, batches, selections)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_create_batch(ctx, batches, selections, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_create_batch(ctx, batches, selections, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::StartBatchRetry,
                                 Parties<Self::IndefParties>>,
                Self::StartBatchError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .start_batch(ctx, parties)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::StartBatchRetry,
                                 Parties<Self::IndefParties>>,
                Self::StartBatchError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_start_batch(ctx, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::StartBatchRetry,
                                 Self::IndefParties>,
                Self::StartBatchError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_start_batch(ctx, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match err {
            ThreadedStreamError::Inner { error } => match self.inner.lock() {
                Ok(mut guard) => guard.abort_start_batch(ctx, flags, error),
                Err(_) => {
                    error!(target: "threaded-stream",
                           "mutex poisoned");

                    RetryResult::Success(())
                }
            },
            ThreadedStreamError::MutexPoison => {
                warn!(target: "threaded-stream",
                      "could not cancel batch with error: mutex poisoned");

                RetryResult::Success(())
            }
            ThreadedStreamError::Shutdown => RetryResult::Success(())
        }
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match self.inner.lock() {
            Ok(mut guard) => guard.retry_abort_start_batch(ctx, flags, retry),
            Err(_) => {
                error!(target: "threaded-stream",
                       "mutex poisoned");

                RetryResult::Success(())
            }
        }
    }
}

impl<T, Ctx, Inner> PushStreamSharedSingle<T, Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamSharedSingle<T, Ctx>
{
    type CancelPushError = ThreadedStreamError<Inner::CancelPushError>;
    type CancelPushRetry = Inner::CancelPushRetry;
    type PushError = ThreadedStreamError<Inner::PushError>;
    type PushRetry = Inner::PushRetry;

    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &T
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .push(ctx, parties, msg)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_push(ctx, msg, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Parties<Self::IndefParties>>,
                Self::PushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_push(ctx, msg, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            ThreadedStreamError::Inner { error } => {
                let mut guard = self
                    .inner
                    .lock()
                    .map_err(|_| ThreadedStreamError::MutexPoison)?;

                guard
                    .cancel_push(ctx, error)
                    .map_err(|err| ThreadedStreamError::Inner { error: err })
            }
            ThreadedStreamError::MutexPoison => Ok(RetryResult::Success(())),
            ThreadedStreamError::Shutdown => Ok(RetryResult::Success(()))
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_cancel_push(ctx, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_cancel_push(ctx, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<T, Ctx, Inner> PushStreamPrivateSingle<T, Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamPrivateSingle<T, Ctx>
{
    type CancelPushError = ThreadedStreamError<Inner::CancelPushError>;
    type CancelPushRetry = Inner::CancelPushRetry;
    type PushError = ThreadedStreamError<Inner::PushError>;
    type PushRetry = Inner::PushRetry;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .push(ctx, msg)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_push(ctx, msg, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_push(ctx, msg, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            ThreadedStreamError::Inner { error } => {
                let mut guard = self
                    .inner
                    .lock()
                    .map_err(|_| ThreadedStreamError::MutexPoison)?;

                guard
                    .cancel_push(ctx, error)
                    .map_err(|err| ThreadedStreamError::Inner { error: err })
            }
            ThreadedStreamError::MutexPoison => Ok(RetryResult::Success(())),
            ThreadedStreamError::Shutdown => Ok(RetryResult::Success(()))
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_cancel_push(ctx, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_cancel_push(ctx, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<T, Inner> PullStream<T> for ThreadedStream<Inner>
where
    Inner: PullStream<T>
{
    type PullError = ThreadedStreamError<Inner::PullError>;

    fn pull(&mut self) -> Result<T, Self::PullError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        while self.shutdown.is_live() {
            match guard.pull() {
                Ok(msg) => return Ok(msg),
                Err(err) => {
                    if !err.is_retryable() {
                        return Err(ThreadedStreamError::Inner { error: err });
                    }
                }
            }
        }

        Err(ThreadedStreamError::Shutdown)
    }
}

impl<Ctx, Inner> LargeObjStream<Ctx> for ThreadedStream<Inner>
where
    Inner: LargeObjStream<Ctx>
{
    type Frags = Inner::Frags;
    type PushFragError = ThreadedStreamError<Inner::PushFragError>;
    type PushFragRetry = Inner::PushFragRetry;
    type Parties = Inner::Parties;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .push_frags(ctx, id, frags)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .retry_push_frags(ctx, id, frags, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .complete_push_frags(ctx, id, frags, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Ctx, H, Inner> LargeObjOfferStream<H, Ctx> for ThreadedStream<Inner>
where
    Inner: LargeObjOfferStream<H, Ctx>,
    H: HashID
{
    type PushOfferError = ThreadedStreamError<Inner::PushOfferError>;
    type PushOfferRetry = Inner::PushOfferRetry;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .push_offer(ctx, hash, frags)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .retry_push_offer(ctx, hash, frags, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .complete_push_offer(ctx, hash, frags, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Inner> Clone for ThreadedStream<Inner> {
    #[inline]
    fn clone(&self) -> Self {
        ThreadedStream {
            shutdown: self.shutdown.clone(),
            inner: self.inner.clone()
        }
    }
}

impl<Inner> ThreadedStream<Inner> {
    /// Create a new `ThreadedStream` from its inner stream.
    #[inline]
    pub fn new(
        shutdown: ShutdownFlag,
        inner: Inner
    ) -> Self {
        ThreadedStream {
            shutdown: shutdown,
            inner: Arc::new(Mutex::new(inner))
        }
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

impl<Inner> ScopedError for ThreadedStreamError<Inner>
where
    Inner: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            ThreadedStreamError::Inner { error } => error.scope(),
            ThreadedStreamError::Shutdown => ErrorScope::Shutdown,
            ThreadedStreamError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Inner> RecoverableError for ThreadedStreamError<Inner>
where
    Inner: RecoverableError
{
    type Completable = Inner::Completable;
    type Permanent = ThreadedStreamError<Inner::Permanent>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            ThreadedStreamError::Inner { error } => {
                let (completable, permanent) = error.split();

                (
                    completable,
                    permanent
                        .map(|err| ThreadedStreamError::Inner { error: err })
                )
            }
            ThreadedStreamError::Shutdown => {
                (None, Some(ThreadedStreamError::Shutdown))
            }
            ThreadedStreamError::MutexPoison => {
                (None, Some(ThreadedStreamError::MutexPoison))
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

impl<Inner> Display for ThreadedStreamError<Inner>
where
    Inner: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            ThreadedStreamError::Inner { error } => error.fmt(f),
            ThreadedStreamError::Shutdown => write!(f, "shutdown"),
            ThreadedStreamError::MutexPoison => write!(f, "mutex poisoned")
        }
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

#[cfg(test)]
use std::rc::Rc;

#[cfg(test)]
use constellation_common::hashid::SHA3ID;

#[cfg(test)]
use crate::frags::OutboundFrags;

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestBatchState<T> {
    Live {
        msgs: Vec<T>
    },
    Finished {
        msgs: Vec<T>
    },
    Canceled,
    StartError,
    Aborted
}

#[cfg(test)]
#[derive(Clone)]
pub struct TestPrivateStreamScript<In> {
    pub select: Vec<Result<RetryIndefResult<(), TestPrivateRetry>,
                           TestPrivateError<TestIndefAction<()>>>>,
    pub create_batch: Vec<Result<RetryResult<(), TestPrivateRetry>,
                                 TestPrivateError<TestPrivateAction<()>>>>,
    pub finish_batch: Vec<Result<RetryResult<(), TestPrivateRetry>,
                                 TestPrivateError<TestPrivateAction<()>>>>,
    pub cancel_batch: Vec<Result<RetryResult<(), TestPrivateRetry>,
                                 TestPrivateError<TestPrivateAction<()>>>>,
    pub abort_start_batch: Vec<RetryResult<(), TestPrivateAbortRetry>>,
    pub add: Vec<Result<RetryResult<(), TestPrivateRetry>,
                        TestPrivateError<TestPrivateAction<()>>>>,
    pub push_frags: Vec<Result<
        RetryIndefResult<(Option<Instant>, ()),
                         TestPrivateRetry,
                         Parties<()>>,
        TestPrivateError<TestIndefAction<Option<Instant>>>
    >>,
    pub push_offers: Vec<Result<
        RetryIndefResult<(Option<Instant>, ()),
                         TestPrivateRetry,
                         Parties<()>>,
        TestPrivateError<TestIndefAction<Option<Instant>>>
    >>,
    pub report_failure: Vec<Result<(), TestPrivatePermanentError>>,
    pub inbound: Vec<Result<In, TestPrivatePermanentError>>
}

#[cfg(test)]
#[derive(Clone)]
pub struct TestPrivateStream<In, Out, H>
where H: HashID {
    pub batches: Rc<Vec<TestBatchState<Out>>>,
    pub frags: Rc<Vec<LargeObjID>>,
    pub offers: Rc<Vec<H>>,
    pub failures: Rc<Vec<usize>>,
    script: Rc<TestPrivateStreamScript<In>>
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPrivateRetry {
    when: Instant
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPrivatePermanentError {
    scope: ErrorScope
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPrivateAbortRetry {
    when: Instant,
    batch: usize
}

#[cfg(test)]
#[derive(Clone, Eq, Debug, PartialEq)]
pub struct TestPrivateStartBatchPermanentError {
    scope: ErrorScope,
    batch: usize
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPrivateCompletableError<Act> {
    scope: ErrorScope,
    action: Act
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateError<Act> {
    Permanent {
        err: TestPrivatePermanentError
    },
    Completable {
        err: TestPrivateCompletableError<Act>
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateStartBatchError<Act> {
    Permanent {
        err: TestPrivateStartBatchPermanentError
    },
    Completable {
        err: TestPrivateCompletableError<Act>
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateInboundAction<In> {
    Success {
        msg: In
    },
    Error {
        err: Box<TestPrivateError<TestPrivateInboundAction<In>>>
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateAction<T> {
    Success {
        val: T
    },
    Retry {
        retry: TestPrivateRetry
    },
    Error {
        err: Box<TestPrivateError<TestPrivateAction<T>>>
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestIndefAction<T> {
    Success {
        val: T
    },
    Retry {
        retry: TestPrivateRetry
    },
    Indef,
    Error {
        err: Box<TestPrivateError<TestIndefAction<T>>>
    }
}

#[cfg(test)]
impl<In, Out, H> TestPrivateStream<In, Out, H>
where H: HashID {
    #[inline]
    pub fn new(
        mut script: TestPrivateStreamScript<In>
    ) -> Self {
        script.select.reverse();
        script.create_batch.reverse();
        script.finish_batch.reverse();
        script.cancel_batch.reverse();
        script.abort_start_batch.reverse();
        script.add.reverse();
        script.push_frags.reverse();
        script.push_offers.reverse();
        script.report_failure.reverse();
        script.inbound.reverse();

        TestPrivateStream {
            failures: Rc::new(Vec::new()),
            batches: Rc::new(Vec::new()),
            frags: Rc::new(Vec::new()),
            offers: Rc::new(Vec::new()),
            script: Rc::new(script)
        }
    }
}

#[cfg(test)]
impl RetryWhen for TestPrivateRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

#[cfg(test)]
impl RetryWhen for TestPrivateAbortRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

#[cfg(test)]
impl<T> ScopedError for TestPrivateCompletableError<T> {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

#[cfg(test)]
impl ScopedError for TestPrivatePermanentError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

#[cfg(test)]
impl ScopedError for TestPrivateStartBatchPermanentError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

#[cfg(test)]
impl<T> RecoverableError for TestPrivateError<T> {
    type Completable = TestPrivateCompletableError<T>;
    type Permanent = TestPrivatePermanentError;

    #[inline]
    fn split(self) -> (Option<TestPrivateCompletableError<T>>,
                       Option<TestPrivatePermanentError>) {
        match self {
            TestPrivateError::Completable { err } => (Some(err), None),
            TestPrivateError::Permanent { err } => (None, Some(err))
        }
    }
}

#[cfg(test)]
impl<T> RecoverableError for TestPrivateStartBatchError<T> {
    type Completable = TestPrivateCompletableError<T>;
    type Permanent = TestPrivateStartBatchPermanentError;

    #[inline]
    fn split(self) -> (Option<TestPrivateCompletableError<T>>,
                       Option<TestPrivateStartBatchPermanentError>) {
        match self {
            TestPrivateStartBatchError::Completable { err } =>
                (Some(err), None),
            TestPrivateStartBatchError::Permanent { err } => (None, Some(err))
        }
    }
}

#[cfg(test)]
impl<In, Out, H> PullStream<In> for TestPrivateStream<In, Out, H>
where H: HashID {
    type PullError = TestPrivatePermanentError;

    fn pull(&mut self) -> Result<In, Self::PullError> {
        Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .inbound
            .pop().expect("Expected scripted action")
    }
}

#[cfg(test)]
impl<Ctx, In, Out, H> PushStream<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type BatchID = usize;
    type CancelBatchError = TestPrivateError<TestPrivateAction<()>>;
    type CancelBatchRetry = TestPrivateRetry;
    type FinishBatchError = TestPrivateError<TestPrivateAction<()>>;
    type FinishBatchRetry = TestPrivateRetry;
    type StreamFlags = ();
    type ReportError = TestPrivatePermanentError;

    fn finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .finish_batch
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            let msgs = if let TestBatchState::Live { msgs } = &self
                .batches[*batch] {
                msgs.clone()
            } else {
                panic!("Expected live batch")
            };

            Rc::get_mut(&mut self.batches)
                .expect("get_mut failed")[*batch] =
                TestBatchState::Finished { msgs: msgs };
        }

        out
    }

    #[inline]
    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        self.finish_batch(ctx, flags, batch)
    }

    fn complete_finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        match err.action {
            TestPrivateAction::Success { .. } => {
                let msgs = if let TestBatchState::Live { msgs } = &self
                    .batches[*batch] {
                    msgs.clone()
                } else {
                    panic!("Expected live batch")
                };

                Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed")[*batch] =
                    TestBatchState::Finished { msgs: msgs };

                Ok(RetryResult::Success(()))
            }
            TestPrivateAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestPrivateAction::Error { err } => Err(*err)
        }
    }

    fn cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .cancel_batch
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            Rc::get_mut(&mut self.batches)
                .expect("get_mut failed")[*batch] =
                TestBatchState::Canceled;
        }

        out
    }

    #[inline]
    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        self.cancel_batch(ctx, flags, batch)
    }

    fn complete_cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        match err.action {
            TestPrivateAction::Success { .. } => {
                Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed")[*batch] =
                    TestBatchState::Canceled;

                Ok(RetryResult::Success(()))
            }
            TestPrivateAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestPrivateAction::Error { err } => Err(*err)
        }
    }

    fn cancel_batches(&mut self) {
        for i in 0..self.batches.len() {
            if let TestBatchState::Live { .. } = &self.batches[i] {
                Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed")[i] = TestBatchState::Canceled;
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .report_failure
            .pop().expect("Expected scripted action");

        if out.is_ok() {
            Rc::get_mut(&mut self.failures)
                .expect("get_mut failed")
                .push(*batch)
        }

        out
    }
}

#[cfg(test)]
impl<Ctx, In, Out, H> PushStreamAdd<Out, Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type AddError = TestPrivateError<TestPrivateAction<()>>;
    type AddRetry = TestPrivateRetry;

    fn add(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .add
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            if let TestBatchState::Live { msgs } =
                &mut Rc::get_mut(&mut self.batches)
                .expect("get_mut failed")[*batch] {
                msgs.push(msg.clone())
            } else {
                panic!("Expected live batch")
            };
        }

        out
    }

    #[inline]
    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        _retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.add(ctx, flags, msg, batch)
    }

    fn complete_add(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match err.action {
            TestPrivateAction::Success { .. } => {
                if let TestBatchState::Live { msgs } =
                    &mut Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed")[*batch] {
                        msgs.push(msg.clone())
                    } else {
                        panic!("Expected live batch")
                    };

                Ok(RetryResult::Success(()))
            }
            TestPrivateAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestPrivateAction::Error { err } => Err(*err)
        }
    }
}

#[cfg(test)]
impl<Ctx, In, Out, H> PushStreamPrivate<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type SelectError = TestPrivateError<TestIndefAction<()>>;
    type SelectRetry = TestPrivateRetry;
    type CreateBatchError = TestPrivateError<TestPrivateAction<()>>;
    type CreateBatchRetry = TestPrivateRetry;
    type StartBatchError = TestPrivateStartBatchError<TestPrivateAction<()>>;
    type StartBatchRetry = TestPrivateRetry;
    type AbortBatchRetry = TestPrivateAbortRetry;
    type Selections = ();
    type StartBatchStreamBatches = ();

    fn select(
        &mut self,
        _ctx: &mut Ctx,
        _selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .select
            .pop().expect("Expected scripted action");

        out
    }

    #[inline]
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        _retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        self.select(ctx, selections)
    }

    fn complete_select(
        &mut self,
        _ctx: &mut Ctx,
        _selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        match err.action {
            TestIndefAction::Success { .. } =>
                Ok(RetryIndefResult::Success(())),
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef => Ok(RetryIndefResult::Indef(())),
            TestIndefAction::Error { err } => Err(*err),
        }
    }

    fn create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        _selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .create_batch
            .pop().expect("Expected scripted action")
            .map(|res| res.map(|_| {
                let batches = Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed");
                let out = batches.len();

                batches.push(TestBatchState::Live {
                    msgs: Vec::new()
                });

                out
            }))
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        _retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.create_batch(ctx, batches, selections)
    }

    fn complete_create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        _selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        match err.action {
            TestPrivateAction::Success { .. } => {
                let batches = Rc::get_mut(&mut self.batches)
                    .expect("get_mut failed");
                let out = batches.len();

                batches.push(TestBatchState::Live {
                    msgs: Vec::new()
                });

                Ok(RetryResult::Success(out))
            }
            TestPrivateAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestPrivateAction::Error { err } => Err(*err)
        }
    }

    #[inline]
    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.create_batch(ctx, &mut (), &())
            .map_err(|err| match err {
                TestPrivateError::Completable { err } =>
                    TestPrivateStartBatchError::Completable { err },
                TestPrivateError::Permanent { err } => {
                    let batches = Rc::get_mut(&mut self.batches)
                        .expect("get_mut failed");
                    let out = batches.len();

                    batches.push(TestBatchState::StartError);

                    TestPrivateStartBatchError::Permanent {
                        err: TestPrivateStartBatchPermanentError {
                            scope: err.scope,
                            batch: out
                        }
                    }
                }
            })
            .map(RetryIndefResult::from)
    }

    #[inline]
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.retry_create_batch(ctx, &mut (), &(), retry)
            .map_err(|err| match err {
                TestPrivateError::Completable { err } =>
                    TestPrivateStartBatchError::Completable { err },
                TestPrivateError::Permanent { err } => {
                    let batches = Rc::get_mut(&mut self.batches)
                        .expect("get_mut failed");
                    let out = batches.len();

                    batches.push(TestBatchState::StartError);

                    TestPrivateStartBatchError::Permanent {
                        err: TestPrivateStartBatchPermanentError {
                            scope: err.scope,
                            batch: out
                        }
                    }
                }
            })
            .map(RetryIndefResult::from)
    }

    #[inline]
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.complete_create_batch(ctx, &mut (), &(), err)
            .map_err(|err| match err {
                TestPrivateError::Completable { err } =>
                    TestPrivateStartBatchError::Completable { err },
                TestPrivateError::Permanent { err } => {
                    let batches = Rc::get_mut(&mut self.batches)
                        .expect("get_mut failed");
                    let out = batches.len();

                    batches.push(TestBatchState::StartError);

                    TestPrivateStartBatchError::Permanent {
                        err: TestPrivateStartBatchPermanentError {
                            scope: err.scope,
                            batch: out
                        }
                    }
                }
            })
            .map(RetryIndefResult::from)
    }

    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .abort_start_batch
            .pop().expect("Expected scripted action")
            .map_retry(|res| TestPrivateAbortRetry {
                when: res.when,
                batch: err.batch
            });

        if out.is_success() {
            Rc::get_mut(&mut self.batches)
                .expect("get_mut failed")[err.batch] =
                TestBatchState::Aborted;
        }

        out
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let err = TestPrivateStartBatchPermanentError {
            scope: ErrorScope::Retryable,
            batch: retry.batch
        };

        self.abort_start_batch(ctx, flags, err)
    }
}

#[cfg(test)]
impl<Ctx, In, Out, H> LargeObjStream<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushFragError = TestPrivateError<TestIndefAction<Option<Instant>>>;
    type PushFragRetry = TestPrivateRetry;
    type Frags = OutboundFrags;
    type Parties = ();

    fn push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .push_frags
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            Rc::get_mut(&mut self.frags).expect("get_mut failed").push(id)
        }

        out
    }

    #[inline]
    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.push_frags(ctx, id, frags)
    }

    fn complete_push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                Rc::get_mut(&mut self.frags)
                    .expect("get_mut failed").push(id);

                Ok(RetryIndefResult::Success((val, ())))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

#[cfg(test)]
impl<Ctx, In, Out, H> LargeObjOfferStream<H, Ctx>
    for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushOfferError = TestPrivateError<TestIndefAction<Option<Instant>>>;
    type PushOfferRetry = TestPrivateRetry;

    fn push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        let out = Rc::get_mut(&mut self.script)
            .expect("get_mut failed")
            .push_offers
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            Rc::get_mut(&mut self.offers).expect("get_mut failed").push(hash)
        }

        out
    }

    #[inline]
    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        _retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.push_offer(ctx, hash, frags)
    }

    fn complete_push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                Rc::get_mut(&mut self.offers)
                    .expect("get_mut failed").push(hash);

                Ok(RetryIndefResult::Success((val, ())))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

#[cfg(test)]
impl Display for TestPrivatePermanentError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test error")
    }
}

#[cfg(test)]
impl Display for TestPrivateStartBatchPermanentError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test start batch error")
    }
}

#[test]
fn test_test_stream_private_pull() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let first = stream.pull();
    let second = stream.pull();
    let third = stream.pull();

    assert_eq!(first, Ok("hello"));
    assert_eq!(second, Ok("goodbye"));
    assert_eq!(third, Err(TestPrivatePermanentError {
        scope: ErrorScope::Session
    }));
}

#[test]
fn test_test_stream_private_select() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
            Ok(RetryIndefResult::Retry(TestPrivateRetry {
                when: now
            })),
            Ok(RetryIndefResult::Indef(())),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Indef
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    assert_eq!(stream.select(&mut (), &mut ()),
               Ok(RetryIndefResult::Success(())));

    let retry = stream.select(&mut (), &mut ())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    let indef = stream.retry_select(&mut (), &mut (), retry)
        .expect("Expected success");

    assert!(indef.is_indef());

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_select(&mut (), &mut (), completable),
               Ok(RetryIndefResult::Success(())));

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_select(&mut (), &mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    let err = stream.retry_select(&mut (), &mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let indef = stream.complete_select(&mut (), &mut (), completable)
        .expect("Expected success");
    assert!(indef.is_indef());

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_create_batch() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");

    assert!(batch.is_success());

    let retry = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_create_batch(&mut (), &mut (), &(), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_create_batch(&mut (), &mut (), &(), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_create_batch(&mut (), &mut (), &(), completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.retry_create_batch(&mut (), &mut (), &(), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_create_batch(&mut (), &mut (), &(), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.start_batch(&mut ())
        .expect("Expected success");

    assert!(batch.is_success());

    let retry = stream.start_batch(&mut ())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::StartError
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, TestPrivateStartBatchPermanentError {
        scope: ErrorScope::Session,
        ..
    }));

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, TestPrivateStartBatchPermanentError {
        scope: ErrorScope::Session,
        ..
    }));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::StartError,
                   TestBatchState::StartError
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch_1 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.cancel_batch(&mut (), &mut (), &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
               ]);

    let batch_2 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_2 = if let RetryResult::Success(batch_2) = batch_2 {
        batch_2
    } else {
        panic!("Expected success")
    };

    let retry = stream.cancel_batch(&mut (), &mut (), &batch_2)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_cancel_batch(&mut (), &mut (), &batch_2, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_2, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
               ]);

    let batch_3 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_3 = if let RetryResult::Success(batch_3) = batch_3 {
        batch_3
    } else {
        panic!("Expected success")
    };

    let err = stream.cancel_batch(&mut (), &mut (), &batch_3);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_3, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_cancel_batch(&mut (), &mut (), &batch_3, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_3, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let batch_4 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_4 = if let RetryResult::Success(batch_4) = batch_4 {
        batch_4
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch_4);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch_1 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.finish_batch(&mut (), &mut (), &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
               ]);

    let batch_2 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_2 = if let RetryResult::Success(batch_2) = batch_2 {
        batch_2
    } else {
        panic!("Expected success")
    };

    let retry = stream.finish_batch(&mut (), &mut (), &batch_2)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_finish_batch(&mut (), &mut (), &batch_2, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_finish_batch(&mut (), &mut (), &batch_2, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
               ]);

    let batch_3 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_3 = if let RetryResult::Success(batch_3) = batch_3 {
        batch_3
    } else {
        panic!("Expected success")
    };

    let err = stream.finish_batch(&mut (), &mut (), &batch_3);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut (), &mut (), &batch_3, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_finish_batch(&mut (), &mut (), &batch_3, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_finish_batch(&mut (), &mut (), &batch_3, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let batch_4 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_4 = if let RetryResult::Success(batch_4) = batch_4 {
        batch_4
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch_4);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Finished {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

/*
#[test]
fn test_test_stream_private_abort_start_batch() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
        ],
        create_batch: vec![
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch_1 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.cancel_batch(&mut (), &mut (), &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
               ]);

    let batch_2 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_2 = if let RetryResult::Success(batch_2) = batch_2 {
        batch_2
    } else {
        panic!("Expected success")
    };

    let retry = stream.cancel_batch(&mut (), &mut (), &batch_2)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_cancel_batch(&mut (), &mut (), &batch_2, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_2, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
               ]);

    let batch_3 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_3 = if let RetryResult::Success(batch_3) = batch_3 {
        batch_3
    } else {
        panic!("Expected success")
    };

    let err = stream.cancel_batch(&mut (), &mut (), &batch_3);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_3, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_cancel_batch(&mut (), &mut (), &batch_3, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_cancel_batch(&mut (), &mut (), &batch_3, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let batch_4 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_4 = if let RetryResult::Success(batch_4) = batch_4 {
        batch_4
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch_4);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Canceled,
                   TestBatchState::Canceled,
                   TestBatchState::Live {
                       msgs: vec![]
                   },
                   TestBatchState::Live {
                       msgs: vec![]
                   }
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}
*/
#[test]
fn test_test_stream_private_add() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestPrivateRetry {
                when: now
            })),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Success {
                        val: ()
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Retry {
                        retry: TestPrivateRetry {
                            when: now
                        }
                    }
                }
            }),
            Err(TestPrivateError::Completable {
                err: TestPrivateCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestPrivateAction::Error {
                        err: Box::new(TestPrivateError::Permanent {
                            err: TestPrivatePermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
            Err(TestPrivateError::Permanent {
                err: TestPrivatePermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPrivatePermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch_1 = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.add(&mut (), &mut (), &"hello", &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);

    let retry = stream.add(&mut (), &mut (), &"goodbye", &batch_1)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);

    let err = stream.retry_add(&mut (), &mut (), &"goodbye", &batch_1, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut (), &"goodbye", &batch_1, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"nothing", &batch_1);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(&mut (), &mut (), &"nothing", &batch_1, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    let err = stream.retry_add(&mut (), &mut (), &"nothing", &batch_1, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_add(&mut (), &mut (), &"hello", &batch_1, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.add(&mut (), &mut (), &"nothing", &batch_1);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPrivatePermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestBatchState::Live {
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}
