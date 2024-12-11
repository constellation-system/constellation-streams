// Copyright © 2024 The Johns Hopkins Applied Physics Laboratory LLC.
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
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Instant;

use bitvec::bitvec;
use bitvec::vec::BitVec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;
use log::warn;

use crate::config::BatchSlotsConfig;
use crate::error::BatchError;
use crate::error::ErrorReportInfo;

pub mod pull;

/// Trait for caches used in stream creation to allow shared streams.
///
/// This is a trait for types of caches that are used in the
/// (start_batch)[PushStream::start_batch] process.  This is primarily
/// used with shared streams, to ensure that only one batch is created
/// for any given shared stream.
pub trait StreamBatches {
    /// ID for batches.
    type BatchID: Clone;
    /// ID for streams.
    type StreamID: Clone;

    /// Create an empty instance.
    fn new() -> Self;

    /// Create an empty instance with a size hint.
    fn with_capacity(size: usize) -> Self;

    /// Get the batch corresponding to the `stream`.
    fn batch(
        &self,
        stream: &Self::StreamID
    ) -> Option<Self::BatchID>;

    /// Add `batch` as the batch ID corresponding to `stream`.
    fn add_batch(
        &mut self,
        stream: Self::StreamID,
        batch: Self::BatchID
    );
}

/// Trait for flags used to manage adding messages to shared streams.
///
/// This is a trait for types of flags that are used in the
/// [add](PushStreamAdd::add),
/// [finish_batch](PushStream::finish_batch), and
/// [cancel_batch](PushStream::cancel_batch) process.  This is
/// primarily used with shared streams, to ensure that only one batch
/// is created for any given shared stream.
pub trait StreamFlags {
    /// Type of stream IDs.
    type StreamID: Clone;

    /// Create an empty instance.
    fn new() -> Self;

    /// Create an empty instance with a size hint.
    fn with_capacity(size: usize) -> Self;

    /// Check if the flag is set for the given stream.
    fn is_set(
        &self,
        stream: &Self::StreamID
    ) -> bool;

    /// Set the flag for the given stream.
    fn set(
        &mut self,
        stream: Self::StreamID
    );
}

// XXX this is a workaround for an OpenSSL implementation issue.

pub trait ConcurrentStream {
    fn condvar(&self) -> Arc<Condvar>;
}

/// Core trait for "pull" streams.
///
/// These are streams that operate as "listeners", and will wait on
/// incoming messages.
pub trait PullStream<T> {
    /// Type of errors that can occur in a [pull](PullStream::pull)
    /// operation.
    type PullError: Display + ScopedError;

    /// Wait for an incoming message.
    fn pull(&mut self) -> Result<T, Self::PullError>;
}

/// Trait for types that can accept a new stream directly.
///
/// This is primarily intended to allow the push-side and the
/// pull-side to report streams to one another.
pub trait StreamReporter {
    /// Type of streams being reported.
    type Stream: Send;
    /// Source address.
    type Src: Clone + Display + Eq + Hash;
    /// Session principal from authenication.
    type Prin: Clone + Display + Eq + Hash;
    /// Type of errors that can happen reporting a stream.
    type ReportError: Display + ScopedError;

    /// Report a new stream for a counterparty address.
    ///
    /// If a stream already existed for this counterparty, `Some` will
    /// be returned with that stream, and the caller should insert
    /// that stream into its own data structures in place of the
    /// argument stream.  If `None` is returned, then the argument
    /// stream was accepted.
    fn report(
        &mut self,
        src: Self::Src,
        prin: Self::Prin,
        stream: Self::Stream
    ) -> Result<Option<Self::Stream>, Self::ReportError>;
}

/// Trait for types that produce new [PullStream]s.
///
/// This is used to allow the pull side to acquire incoming sessions.
pub trait PullStreamListener<T> {
    /// Type of streams being listened for.
    type Stream: PullStream<T> + Send;
    /// Type of counterparty addresses.
    type Addr: Clone + Display + Eq + Hash;
    /// Type of session principals.
    type Prin: Clone + Display + Eq + Hash;
    /// Type of errors that can occur listening.
    type ListenError: Display;

    /// Listen for a new incoming stream.
    ///
    /// This call will generally block the calling thread.
    fn listen(
        &mut self
    ) -> Result<(Self::Addr, Self::Prin, Self::Stream), Self::ListenError>;
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

    /// Type of [StreamBatches] used in [start_batch](PushStream::start_batch).
    type StartBatchStreamBatches: StreamBatches;
    /// Type of errors that can occur when creating a new batch.
    type StartBatchError: BatchError;
    /// Type of information given by a [RetryResult] for creating a new batch.
    type StartBatchRetry: RetryWhen + Clone;
    /// Type of errors that can occur when canceling a batch.
    type CancelBatchError: BatchError;
    /// Type of information given by a [RetryResult] for canceling a new batch.
    type CancelBatchRetry: RetryWhen + Clone;
    /// Type of errors that can occur when sending a batch.
    type FinishBatchError: BatchError;
    /// Type of information given by a [RetryResult] for finishing a new batch.
    type FinishBatchRetry: RetryWhen + Clone;
    /// Type of information given by a [RetryResult] for aborting a
    /// batch creation.
    type AbortBatchRetry: RetryWhen + Clone;
    /// Type of [StreamFlags] used in [add](PushStreamAdd::add).
    type StreamFlags: StreamFlags;
    /// Type of error that can occur when reporting failures.
    type ReportError: Display;

    /// Create an empty
    /// [StreamFlags](PushStream::StreamFlags).
    fn empty_flags(&self) -> Self::StreamFlags {
        Self::StreamFlags::new()
    }

    /// Create an empty
    /// [StartBatchStreamBatches](PushStream::StartBatchStreamBatches).
    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::new()
    }

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
    ///
    /// The [StreamBatches] implementation `batches` will be used to
    /// ensure that shared streams have only one batch created for
    /// them.
    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Retry a previous call to [start_batch](PushStream::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Retry a previously-failed call to
    /// [start_batch](PushStream::start_batch).
    ///
    /// This allows a call to `start_batch` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        err: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >;

    /// Abort a previously-failed call to
    /// [start_batch](PushStream::start_batch).
    ///
    /// This will release any resources that were allocated in the
    /// call to [start_batch](PushStream::start_batch).
    ///
    /// In order to avoid an endless cycle, this represents a
    /// "best-effort", and will not return an error.
    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as BatchError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry>;

    /// Retry a previous call to
    /// [abort_start_batch](PushStream::abort_start_batch).
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

    /// Finish a batch, and guarantee that all of its messages have
    /// been sent.
    ///
    /// Note that streams are do not in general guarantee atomic
    /// semantics.  Depending on the underlying stream, the messages
    /// comprising the batch may have already been sent before this
    /// function is called.
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
    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as BatchError>::Completable
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
    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as BatchError>::Completable
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
    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError>;
}

// XXX these traits are error-prone as implemented.  There should be
// one function that takes an Option<BatchID>

pub trait PushStreamReportError<Error> {
    type ReportError: Display;

    fn report_error(
        &mut self,
        error: &Error
    ) -> Result<(), Self::ReportError>;
}

pub trait PushStreamReportBatchError<Error, Batch> {
    type ReportBatchError: Display;

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
    type AddError: BatchError;
    /// Type of information given by a [RetryResult] for adding a
    /// message to a batch.
    type AddRetry: RetryWhen + Clone;

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
    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError>;
}

/// Common super-trait for all shared (multi-party) streams.
pub trait PushStreamShared {
    /// Type of party IDs.
    ///
    /// This should typically be a wrapper around a dense integer value.
    type PartyID: Clone + Eq + Hash + Ord;
}

/// Trait for obtaining a reporter for new [PushStream]s.
pub trait PushStreamReporter<Inner: StreamReporter> {
    /// Type of [StreamReporter] instance provided by
    /// [reporter](PushStreamReporter::reporter).
    type Reporter: StreamReporter;

    /// Obtain a [StreamReporter] for new streams.
    ///
    /// The `inner` parameter is an inner reporter that will also be
    /// called when new streams are reported.
    fn reporter(
        &self,
        inner: Inner
    ) -> Self::Reporter;
}

/// Trait for obtaining the list of parties associated with a
/// [PushStreamShared] instance.
pub trait PushStreamParties: PushStreamShared {
    type PartiesIter: Iterator<Item = (Self::PartyID, Self::PartyInfo)>;
    /// Detailed information about a party.
    type PartyInfo;
    /// Error that can occur obtaining parties.
    type PartiesError: Display;

    /// Get an iterator for all parties and their dense IDs.
    fn parties(&self) -> Result<Self::PartiesIter, Self::PartiesError>;
}

/// Interface for [PushStream]s that support the notion of multiple
/// possible recipient parties.
///
/// The most direct use for this trait is for `PushStream` instances
/// that represent a multicast-style channel, where messages can be
/// sent to a collection of possible recipients.
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
/// The actual transmission of messages along the underlying channel
/// can happen at *any point* after the message is added to the batch.
/// Because the functionality in this trait affects the recipients, it
/// is generally necessary to ensure that all recipients are added to
/// the batch *before* any messages are added.  Failure to do this may
/// result in some messages not being sent to all the parties that are
/// indicated.
pub trait PushStreamAddParty<Ctx>: PushStream<Ctx> + PushStreamShared {
    /// Type of errors that can occur when adding a recipient party to
    /// a batch.
    type AddPartiesError: BatchError;
    /// Type of information given by a [RetryResult] for adding a
    /// recipient party to a batch.
    type AddPartiesRetry: RetryWhen + Clone;
    /// Iterator for parties.

    /// Add recipient parties to a batch.
    ///
    /// This will cause messages added to this batch using
    /// [add](PushStreamAdd::add) to be sent to `parties`.  This is
    /// guaranteed for any calls to `add` that happen *after* this
    /// call on the same batch; no general guarantees are made
    /// regarding calls to `add` that occur *before* this call
    /// however.
    fn add_parties<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    where
        I: Iterator<Item = Self::PartyID>;

    /// Retry a previous call to [add](PushStreamAddParty::add_parties).
    ///
    /// This allows a call to `add_parties` that had returned a
    /// [Retry](RetryResult::Retry) to be retried in an
    /// implementation-agnostic manner.
    fn retry_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        retry: Self::AddPartiesRetry
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>;

    /// Retry a previously-failed call to
    /// [add](PushStreamAddParty::add_parties).
    ///
    /// This allows a call to `add_parties` that had returned a
    /// recoverable error to be retried in an implementation-agnostic
    /// manner.
    fn complete_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        err: <Self::AddPartiesError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>;
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
    PushStreamAdd<T, Ctx> + PushStreamAddParty<Ctx> {
    /// Type of errors that can occur when sending a single message.
    type PushError: BatchError;
    /// Type of information given by a [RetryResult] for sending a
    /// single message.
    type PushRetry: RetryWhen + Clone;
    /// Type of errors that can occur when canceling a failed single
    /// message.
    type CancelPushError: BatchError;
    /// Type of information given by a [RetryResult] for canceling a
    /// single message.
    type CancelPushRetry: RetryWhen + Clone;

    /// Push a single message into the stream.
    ///
    /// Semantically, this is the equivalent of generating a
    /// single-element batch, and may often be implemented that way.
    /// The batch ID returned is for downstream tracking purposes, and
    /// can be used as the equivalent of a message ID.
    fn push<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &T
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    where
        I: Iterator<Item = Self::PartyID>;

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
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    /// Retry a previously-failed call to
    /// [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;
}

/// Helper trait for sending single messages.
///
/// This trait implements functionality for sending a single message
/// on a [PushStream].  In many cases, this will be a "derived form",
/// using the batching functionality to send a batch of size one.  In
/// some cases; however, it may be a more efficient implementation.
pub trait PushStreamSingle<T, Ctx>: PushStreamAdd<T, Ctx> {
    /// Type of errors that can occur when sending a single message.
    type PushError: BatchError;
    /// Type of information given by a [RetryResult] for sending a
    /// single message.
    type PushRetry: RetryWhen + Clone;
    /// Type of errors that can occur when canceling a failed single
    /// message.
    type CancelPushError: BatchError;
    /// Type of information given by a [RetryResult] for canceling a
    /// single message.
    type CancelPushRetry: RetryWhen + Clone;

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
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

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
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    /// Retry a previously-failed call to
    /// [push](PushStreamSharedSingle::push).
    ///
    /// This allows a call to `push` that had returned a recoverable
    /// error to be retried in an implementation-agnostic manner.
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>;

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>;
}

/// Unique identifier for streams.
#[derive(Clone, Eq, Hash, PartialEq)]
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

/// Implementation of [StreamBatches] that does not perform caching.
///
/// This is intended for use when implementing low-level streams with
/// private unicast channels, which *should not* cache batch creation.
pub struct NullBatches<BatchID: Clone> {
    batch: PhantomData<BatchID>
}

/// Wrapper around [PushStream] and sub-traits that adds
/// synchronization.
pub struct ThreadedStream<Inner> {
    inner: Arc<Mutex<Inner>>
}

/// Errors that can occur from [ThreadedStream]s.
#[derive(Debug)]
pub enum ThreadedStreamError<Inner> {
    Inner { error: Inner },
    MutexPoison
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

impl<Finish, Cancel> BatchError for StreamFinishCancel<Finish, Cancel>
where
    Finish: BatchError,
    Cancel: BatchError
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

impl<Addr, Prin, Stream> StreamReporter for PassthruReporter<Addr, Prin, Stream>
where
    Stream: Send,
    Prin: Clone + Display + Eq + Hash,
    Addr: Clone + Display + Eq + Hash
{
    type Prin = Prin;
    type ReportError = Infallible;
    type Src = Addr;
    type Stream = Stream;

    #[inline]
    fn report(
        &mut self,
        _src: Self::Src,
        _prin: Self::Prin,
        _stream: Self::Stream
    ) -> Result<Option<Self::Stream>, Self::ReportError> {
        Ok(None)
    }
}

impl<Ctx, Inner> PushStream<Ctx> for ThreadedStream<Inner>
where
    Inner: PushStream<Ctx>
{
    type AbortBatchRetry = Inner::AbortBatchRetry;
    type BatchID = Inner::BatchID;
    type CancelBatchError = ThreadedStreamError<Inner::CancelBatchError>;
    type CancelBatchRetry = Inner::CancelBatchRetry;
    type FinishBatchError = ThreadedStreamError<Inner::FinishBatchError>;
    type FinishBatchRetry = Inner::FinishBatchRetry;
    type ReportError = ThreadedStreamError<Inner::ReportError>;
    type StartBatchError = ThreadedStreamError<Inner::StartBatchError>;
    type StartBatchRetry = Inner::StartBatchRetry;
    type StartBatchStreamBatches = Inner::StartBatchStreamBatches;
    type StreamFlags = Inner::StreamFlags;

    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .start_batch(ctx, batches)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_start_batch(ctx, batches, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        err: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_start_batch(ctx, batches, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as BatchError>::Permanent
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
        err: <Self::FinishBatchError as BatchError>::Completable
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
        err: <Self::CancelBatchError as BatchError>::Completable
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
        err: <Self::AddError as BatchError>::Completable
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

impl<Inner> PushStreamShared for ThreadedStream<Inner>
where
    Inner: PushStreamShared
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

impl<Ctx, Inner> PushStreamAddParty<Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamAddParty<Ctx>
{
    type AddPartiesError = ThreadedStreamError<Inner::AddPartiesError>;
    type AddPartiesRetry = Inner::AddPartiesRetry;

    fn add_parties<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    where
        I: Iterator<Item = Self::PartyID> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .add_parties(ctx, parties, batch)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn retry_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        retry: Self::AddPartiesRetry
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .retry_add_parties(ctx, batch, retry)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }

    fn complete_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        err: <Self::AddPartiesError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        guard
            .complete_add_parties(ctx, batch, err)
            .map_err(|err| ThreadedStreamError::Inner { error: err })
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

    fn push<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &T
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    where
        I: Iterator<Item = Self::PartyID> {
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
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
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
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
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
        err: <Self::PushError as BatchError>::Permanent
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
            ThreadedStreamError::MutexPoison => Ok(RetryResult::Success(()))
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
        err: <Self::CancelPushError as BatchError>::Completable
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

impl<T, Ctx, Inner> PushStreamSingle<T, Ctx> for ThreadedStream<Inner>
where
    Inner: PushStreamSingle<T, Ctx>
{
    type CancelPushError = ThreadedStreamError<Inner::CancelPushError>;
    type CancelPushRetry = Inner::CancelPushRetry;
    type PushError = ThreadedStreamError<Inner::PushError>;
    type PushRetry = Inner::PushRetry;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
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
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
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
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
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
        err: <Self::PushError as BatchError>::Permanent
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
            ThreadedStreamError::MutexPoison => Ok(RetryResult::Success(()))
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
        err: <Self::CancelPushError as BatchError>::Completable
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
    Inner: PullStream<T> + ConcurrentStream
{
    type PullError = ThreadedStreamError<Inner::PullError>;

    fn pull(&mut self) -> Result<T, Self::PullError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?;

        loop {
            match guard.pull() {
                Ok(msg) => return Ok(msg),
                Err(err) => {
                    if !err.is_retryable() {
                        return Err(ThreadedStreamError::Inner { error: err });
                    }
                }
            }

            trace!(target: "threaded-stream",
                   "waiting on condvar");

            match guard.condvar().wait(guard) {
                Ok(newguard) => guard = newguard,
                Err(_) => return Err(ThreadedStreamError::MutexPoison)
            }
        }
    }
}

impl<Inner> Clone for ThreadedStream<Inner> {
    #[inline]
    fn clone(&self) -> Self {
        ThreadedStream {
            inner: self.inner.clone()
        }
    }
}

impl<Inner> ThreadedStream<Inner> {
    /// Create a new `ThreadedStream` from its inner stream.
    #[inline]
    pub fn new(inner: Inner) -> Self {
        ThreadedStream {
            inner: Arc::new(Mutex::new(inner))
        }
    }
}

impl<BatchID> Default for NullBatches<BatchID>
where
    BatchID: Clone
{
    #[inline]
    fn default() -> Self {
        NullBatches { batch: PhantomData }
    }
}

impl<BatchID> StreamBatches for NullBatches<BatchID>
where
    BatchID: Clone
{
    type BatchID = BatchID;
    type StreamID = ();

    #[inline]
    fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn with_capacity(_size: usize) -> Self {
        Self::default()
    }

    #[inline]
    fn batch(
        &self,
        _stream: &Self::StreamID
    ) -> Option<Self::BatchID> {
        None
    }

    #[inline]
    fn add_batch(
        &mut self,
        _stream: Self::StreamID,
        _batch: Self::BatchID
    ) {
    }
}

impl StreamFlags for () {
    type StreamID = ();

    #[inline]
    fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn with_capacity(_size: usize) -> Self {
        Self::default()
    }

    #[inline]
    fn is_set(
        &self,
        _stream: &Self::StreamID
    ) -> bool {
        false
    }

    #[inline]
    fn set(
        &mut self,
        _stream: Self::StreamID
    ) {
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
            ThreadedStreamError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Inner> BatchError for ThreadedStreamError<Inner>
where
    Inner: BatchError
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
