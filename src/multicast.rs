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

//! Synthetic multicasting combinator for streams.
//!
//! This module implements [StreamMulticaster], which provides a
//! synthetic multicasting capability for [PushStream]s.  See its
//! documentation for full details.
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::once;
use std::marker::PhantomData;
use std::time::Instant;
use std::vec::IntoIter;

use bitvec::bitvec;
use bitvec::vec::BitVec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;

use crate::config::BatchSlotsConfig;
use crate::error::BatchError;
use crate::error::CompoundBatchError;
use crate::error::ErrorSet;
use crate::stream::CompoundBatchID;
use crate::stream::CompoundBatches;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamAddParty;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamReporter;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::StreamBatches;
use crate::stream::StreamFinishCancel;
use crate::stream::StreamFlags;
use crate::stream::StreamReporter;

/// Information about counterparty streams.
struct StreamMulticasterParty<Msg, Party, Stream: PushStreamAdd<Msg, Ctx>, Ctx>
{
    msg: PhantomData<Msg>,
    ctx: PhantomData<Ctx>,
    /// The counterparty for this stream.
    party: Party,
    /// The lower-level stream to use.
    stream: Stream
}

/// Type of batch IDs used by [StreamMulticaster].
#[derive(Clone)]
struct StreamMulticasterBatch<BatchID> {
    /// Array of batch IDs for each party stream.
    batch_ids: Vec<BatchID>,
    /// Which party streams are actually sending this batch.
    active: BitVec
}

/// Synthetic multicasting combinator for [PushStream]s.
///
/// This combinator maintains a separate stream for each of a set of
/// counterparties, and replicates batching commands on each of these
/// streams.  It also allows for the recipients for each batch to be
/// controlled through the [PushStreamAddParty] interface.
///
/// In its most simple use case, this can be used to implement
/// synthetic multicasting functionality on top of unicast streams.
/// However, this combinator can also work with
/// [StreamSelector](crate::select::StreamSelector) and
/// [SharedPrivateChannelStream](crate::channels::SharedPrivateChannelStream)
/// to manage a combination of unicast and true multicast channels.
pub struct StreamMulticaster<
    Party: Clone + Display + Eq + Hash,
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Msg,
    Stream: PushStreamAdd<Msg, Ctx>,
    Ctx
> {
    /// Map from the `Party` type to a dense index type.
    fwd_map: HashMap<Party, Idx>,
    /// Map from dense index types to party and stream data.
    rev_map: Vec<StreamMulticasterParty<Msg, Party, Stream, Ctx>>,
    /// Currently-live batches.
    batches: CompoundBatches<StreamMulticasterBatch<Stream::BatchID>>
}

/// [StreamReporter] instance for [StreamMulticaster].
pub struct StreamMulticasterReporter<
    Idx: Clone + Into<usize>,
    Reporter: StreamReporter
> {
    /// Map from the `Party` type to a dense index type.
    fwd_map: HashMap<Reporter::Prin, Idx>,
    /// Map from dense index types to party and stream data.
    rev_map: Vec<Reporter>
}

/// Errors that can occur while canceling a push operation.
#[derive(Clone)]
pub enum StreamMulticasterCancelPushError<Cancel, Flags, BatchID> {
    /// Error while canceling the push.
    Cancel {
        /// Error that occurred while cancelling.
        cancel: Cancel,
        /// The batch ID.
        batch_id: BatchID,
        /// Flags to use to retry the cancel.
        flags: Flags
    }
}

/// Errors that can occur in a complete
/// [push](PushStreamSharedSingle::push) implementation.
#[derive(Clone)]
pub enum StreamMulticasterPushError<Start, Party, Add, Finish, PartyID, BatchID>
{
    /// Error occurred while starting the batch.
    Start {
        /// Error from starting the batch.
        start: Start,
        /// Parties that will need to be added.
        parties: Vec<PartyID>
    },
    /// Error occurred while adding parties.
    Party {
        /// Error while adding parties.
        party: Party,
        /// The batch ID.
        batch: BatchID
    },
    /// Error occurred while adding the message.
    Add {
        /// Error while adding the message.
        add: Add,
        /// The batch ID.
        batch: BatchID
    },
    /// Error occurred while finishing the batch.
    Finish {
        /// Error while finishing the batch.
        finish: Finish,
        /// The batch ID.
        batch: BatchID
    }
}

/// Errors that can occur when attempting to report another error that
/// occurred while performing a push.
pub enum StreamMulticasterPushReportError<Start, Party, Add, Finish> {
    /// Error reporting an error from the start phase.
    Start {
        /// Err while reporting.
        start: Start
    },
    /// Error reporting an error from the add parties phase.
    Party {
        /// Err while reporting.
        party: Party
    },
    /// Error reporting an error from the add phase.
    Add {
        /// Err while reporting.
        add: Add
    },
    /// Error reporting an error from the finish phase.
    Finish {
        /// Err while reporting.
        finish: Finish
    }
}

/// Retry information for an attempt to cancel a push operation.
#[derive(Clone)]
pub enum StreamMulticasterCancelPushRetry<Start, Cancel, Flags, BatchID> {
    /// Error occurred while starting the batch.
    Start {
        /// Error from starting the batch.
        start: Start,
        /// Flags to use to retry the abort.
        flags: Flags
    },
    /// Error occurred while adding parties.
    Cancel {
        /// Error while canceling the batch.
        cancel: Cancel,
        /// The batch ID.
        batch_id: BatchID,
        /// Flags to use to retry the cancel.
        flags: Flags
    }
}

/// Retry information for an attempt to abort a batch.
#[derive(Clone)]
pub struct StreamMulticasterAbortRetry<Idx, BatchID, Retry> {
    idx: Idx,
    batch: BatchID,
    retry: Retry
}

/// Errors that can occur when reporting a success or failure.
#[derive(Debug)]
pub enum StreamMulticasterReportError<Report, Party> {
    /// Error occurred reporting the success or failure.
    Report {
        /// Error that occurred reporting the success or failure.
        error: Report
    },
    /// The specified party was not found.
    NotFound {
        /// Party that was specified.
        party: Party
    }
}

impl<Idx, Reporter> StreamReporter for StreamMulticasterReporter<Idx, Reporter>
where
    Idx: Clone + Into<usize>,
    Reporter: StreamReporter
{
    type Prin = Reporter::Prin;
    type ReportError =
        StreamMulticasterReportError<Reporter::ReportError, Reporter::Prin>;
    type Src = Reporter::Src;
    type Stream = Reporter::Stream;

    fn report(
        &mut self,
        src: Self::Src,
        prin: Self::Prin,
        stream: Self::Stream
    ) -> Result<Option<Self::Stream>, Self::ReportError> {
        debug!(target: "stream-multicaster",
               "reporting stream {} for {}",
               src, prin);

        match self.fwd_map.get(&prin) {
            Some(idx) => {
                let idx: usize = idx.clone().into();

                self.rev_map[idx].report(src, prin, stream).map_err(|err| {
                    StreamMulticasterReportError::Report { error: err }
                })
            }
            None => Err(StreamMulticasterReportError::NotFound { party: prin })
        }
    }
}

impl<Idx, BatchRetry, Retry> RetryWhen
    for StreamMulticasterAbortRetry<Idx, BatchRetry, Retry>
where
    Retry: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        self.retry.when()
    }
}

impl<Start, Cancel, Flags, BatchID> RetryWhen
    for StreamMulticasterCancelPushRetry<Start, Cancel, Flags, BatchID>
where
    Cancel: RetryWhen,
    Start: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            StreamMulticasterCancelPushRetry::Start { start, .. } => {
                start.when()
            }
            StreamMulticasterCancelPushRetry::Cancel { cancel, .. } => {
                cancel.when()
            }
        }
    }
}

impl<Cancel, Flags, BatchID> ScopedError
    for StreamMulticasterCancelPushError<Cancel, Flags, BatchID>
where
    Cancel: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterCancelPushError::Cancel { cancel, .. } => {
                cancel.scope()
            }
        }
    }
}

impl<Report, Party> ScopedError for StreamMulticasterReportError<Report, Party>
where
    Report: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterReportError::Report { error } => error.scope(),
            StreamMulticasterReportError::NotFound { .. } => ErrorScope::System
        }
    }
}

impl<Start, Party, Add, Finish, PartyID, BatchID> ScopedError
    for StreamMulticasterPushError<Start, Party, Add, Finish, PartyID, BatchID>
where
    Start: ScopedError,
    Party: ScopedError,
    Add: ScopedError,
    Finish: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterPushError::Start { start, .. } => start.scope(),
            StreamMulticasterPushError::Party { party, .. } => party.scope(),
            StreamMulticasterPushError::Add { add, .. } => add.scope(),
            StreamMulticasterPushError::Finish { finish, .. } => finish.scope()
        }
    }
}

impl<Start, Party, Add, Finish> ScopedError
    for StreamMulticasterPushReportError<Start, Party, Add, Finish>
where
    Start: ScopedError,
    Party: ScopedError,
    Add: ScopedError,
    Finish: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterPushReportError::Start { start } => start.scope(),
            StreamMulticasterPushReportError::Party { party } => party.scope(),
            StreamMulticasterPushReportError::Add { add } => add.scope(),
            StreamMulticasterPushReportError::Finish { finish } => {
                finish.scope()
            }
        }
    }
}

impl<Start, Party, Add, Finish, PartyID, BatchID> RetryWhen
    for StreamMulticasterPushError<Start, Party, Add, Finish, PartyID, BatchID>
where
    Start: RetryWhen,
    Party: RetryWhen,
    Add: RetryWhen,
    Finish: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            StreamMulticasterPushError::Start { start, .. } => start.when(),
            StreamMulticasterPushError::Party { party, .. } => party.when(),
            StreamMulticasterPushError::Add { add, .. } => add.when(),
            StreamMulticasterPushError::Finish { finish, .. } => finish.when()
        }
    }
}

impl<Reporter, Party, Idx, Msg, Stream, Ctx> PushStreamReporter<Reporter>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx> + PushStreamReporter<Reporter>,
    Stream::Reporter: StreamReporter<Prin = Party>,
    Reporter: Clone + StreamReporter
{
    type Reporter = StreamMulticasterReporter<Idx, Stream::Reporter>;

    #[inline]
    fn reporter(
        &self,
        reporter: Reporter
    ) -> StreamMulticasterReporter<Idx, Stream::Reporter> {
        let fwd_map = self.fwd_map.clone();
        let rev_map = self
            .rev_map
            .iter()
            .map(|party| party.stream.reporter(reporter.clone()))
            .collect();

        StreamMulticasterReporter {
            fwd_map: fwd_map,
            rev_map: rev_map
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx, Success, Err>
    PushStreamReportError<ErrorSet<Idx, Success, Err>>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx> + PushStreamReportError<Err>,
    Err: Display
{
    type ReportError =
        ErrorSet<Idx, (), <Stream as PushStreamReportError<Err>>::ReportError>;

    fn report_error(
        &mut self,
        errors: &ErrorSet<Idx, Success, Err>
    ) -> Result<(), Self::ReportError> {
        let mut successes = Vec::with_capacity(errors.errors().len());
        let mut failures = Vec::with_capacity(errors.errors().len());

        trace!(target: "stream-multicaster",
               "reporting errors {}",
               errors);

        for (idx, error) in errors.errors() {
            let i: usize = idx.clone().into();

            trace!(target: "stream-multicaster",
                   "reporting error {} to {}",
                   error, i);

            if let Err(err) = self.rev_map[i].stream.report_error(error) {
                failures.push((idx.clone(), err))
            } else {
                successes.push((idx.clone(), ()))
            }
        }

        if !failures.is_empty() {
            Err(ErrorSet::create(successes, failures))
        } else {
            Ok(())
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx, Success, Err>
    PushStreamReportBatchError<ErrorSet<Idx, Success, Err>, CompoundBatchID>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>
{
    type ReportBatchError = CompoundBatchError<Idx, (), Stream::ReportError>;

    fn report_error_with_batch(
        &mut self,
        batch: &CompoundBatchID,
        errors: &ErrorSet<Idx, Success, Err>
    ) -> Result<(), Self::ReportBatchError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::ReportError)>> = None;

                // Run through each error, get the batch ID, and
                // report the error up.
                for (idx, _) in errors.errors() {
                    let i: usize = idx.clone().into();
                    let batch_id = &batch_ids[i];

                    if let Err(err) =
                        self.rev_map[i].stream.report_failure(batch_id)
                    {
                        match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                match errs {
                    // There were errors
                    Some(errs) => Err(CompoundBatchError::Batch {
                        errs: ErrorSet::create(results, errs)
                    }),
                    // No errors, check if there are retries.
                    None => Ok(())
                }
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx, Success, Err>
    PushStreamReportError<CompoundBatchError<Idx, Success, Err>>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx> + PushStreamReportError<Err>,
    Err: Display
{
    type ReportError =
        ErrorSet<Idx, (), <Stream as PushStreamReportError<Err>>::ReportError>;

    fn report_error(
        &mut self,
        errors: &CompoundBatchError<Idx, Success, Err>
    ) -> Result<(), Self::ReportError> {
        if let CompoundBatchError::Batch { errs } = errors {
            self.report_error(errs)
        } else {
            Ok(())
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx, Success, Err>
    PushStreamReportBatchError<
        CompoundBatchError<Idx, Success, Err>,
        CompoundBatchID
    > for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>,
    StreamMulticaster<Party, Idx, Msg, Stream, Ctx>: PushStreamReportBatchError<
        ErrorSet<Idx, Success, Err>,
        CompoundBatchID
    >
{
    type ReportBatchError = <Self as PushStreamReportBatchError<
        ErrorSet<Idx, Success, Err>,
        CompoundBatchID
    >>::ReportBatchError;

    fn report_error_with_batch(
        &mut self,
        batch: &CompoundBatchID,
        errors: &CompoundBatchError<Idx, Success, Err>
    ) -> Result<(), Self::ReportBatchError> {
        if let CompoundBatchError::Batch { errs } = errors {
            self.report_error_with_batch(batch, errs)
        } else {
            Ok(())
        }
    }
}

impl<
        Party,
        Idx,
        Msg,
        Stream,
        Ctx,
        Start,
        AddParty,
        Add,
        Finish,
        PartyID,
        BatchID
    >
    PushStreamReportError<
        StreamMulticasterPushError<
            Start,
            AddParty,
            Add,
            Finish,
            PartyID,
            BatchID
        >
    > for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>,
    StreamMulticaster<Party, Idx, Msg, Stream, Ctx>: PushStreamReportError<Start>
        + PushStreamReportBatchError<Add, BatchID>
        + PushStreamReportBatchError<AddParty, BatchID>
        + PushStreamReportBatchError<Finish, BatchID>
{
    type ReportError = StreamMulticasterPushReportError<
        <Self as PushStreamReportError<Start>>::ReportError,
        <Self as PushStreamReportBatchError<AddParty, BatchID>>::ReportBatchError,
        <Self as PushStreamReportBatchError<Add, BatchID>>::ReportBatchError,
        <Self as PushStreamReportBatchError<Finish, BatchID>>::ReportBatchError,
    >;

    fn report_error(
        &mut self,
        errors: &StreamMulticasterPushError<
            Start,
            AddParty,
            Add,
            Finish,
            PartyID,
            BatchID
        >
    ) -> Result<(), Self::ReportError> {
        match errors {
            StreamMulticasterPushError::Start { start, .. } => {
                self.report_error(start).map_err(|err| {
                    StreamMulticasterPushReportError::Start { start: err }
                })
            }
            StreamMulticasterPushError::Party { party, batch } => {
                self.report_error_with_batch(batch, party).map_err(|err| {
                    StreamMulticasterPushReportError::Party { party: err }
                })
            }
            StreamMulticasterPushError::Add { add, batch } => {
                self.report_error_with_batch(batch, add).map_err(|err| {
                    StreamMulticasterPushReportError::Add { add: err }
                })
            }
            StreamMulticasterPushError::Finish { finish, batch } => {
                self.report_error_with_batch(batch, finish).map_err(|err| {
                    StreamMulticasterPushReportError::Finish { finish: err }
                })
            }
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx>
    StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>
{
    /// Create a `StreamMulticaster` from an iterator over the
    /// counterparties.
    #[inline]
    pub fn create<I>(
        iter: I,
        config: BatchSlotsConfig
    ) -> Self
    where
        I: Iterator<Item = (Party, Stream)> {
        let rev_map: Vec<StreamMulticasterParty<Msg, Party, Stream, Ctx>> =
            iter.map(|(party, stream)| StreamMulticasterParty {
                msg: PhantomData,
                ctx: PhantomData,
                party: party,
                stream: stream
            })
            .collect();
        let mut fwd_map = HashMap::with_capacity(rev_map.len());

        for (i, item) in rev_map.iter().enumerate() {
            fwd_map.insert(item.party.clone(), Idx::from(i));
        }

        StreamMulticaster {
            batches: CompoundBatches::create(config),
            fwd_map: fwd_map,
            rev_map: rev_map
        }
    }

    /// Get the number of counterparties.
    #[inline]
    pub fn nparties(&self) -> usize {
        self.rev_map.len()
    }

    /// Get the dense index value representing `party`.
    #[inline]
    pub fn party_idx(
        &self,
        party: &Party
    ) -> Option<&Idx> {
        self.fwd_map.get(party)
    }

    /// Get the counterparty represented by `idx`.
    #[inline]
    pub fn idx_party(
        &self,
        idx: usize
    ) -> Option<&Party> {
        if idx < self.rev_map.len() {
            Some(&self.rev_map[idx].party)
        } else {
            None
        }
    }
}

impl<Cancel, Flags, BatchID> BatchError
    for StreamMulticasterCancelPushError<Cancel, Flags, BatchID>
where
    Cancel: BatchError,
    Flags: Clone,
    BatchID: Clone
{
    type Completable =
        StreamMulticasterCancelPushError<Cancel::Completable, Flags, BatchID>;
    type Permanent =
        StreamMulticasterCancelPushError<Cancel::Permanent, Flags, BatchID>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            StreamMulticasterCancelPushError::Cancel {
                cancel,
                batch_id,
                flags
            } => {
                let (completable, permanent) = cancel.split();

                (
                    completable.map(|err| {
                        StreamMulticasterCancelPushError::Cancel {
                            cancel: err,
                            batch_id: batch_id.clone(),
                            flags: flags.clone()
                        }
                    }),
                    permanent.map(|err| {
                        StreamMulticasterCancelPushError::Cancel {
                            cancel: err,
                            batch_id: batch_id,
                            flags: flags.clone()
                        }
                    })
                )
            }
        }
    }
}

impl<Start, Party, Add, Finish, PartyID, BatchID> BatchError
    for StreamMulticasterPushError<Start, Party, Add, Finish, PartyID, BatchID>
where
    Start: BatchError,
    Party: BatchError,
    Add: BatchError,
    Finish: BatchError,
    PartyID: Clone,
    BatchID: Clone
{
    type Completable = StreamMulticasterPushError<
        Start::Completable,
        Party::Completable,
        Add::Completable,
        Finish::Completable,
        PartyID,
        BatchID
    >;
    type Permanent = StreamMulticasterPushError<
        Start::Permanent,
        Party::Permanent,
        Add::Permanent,
        Finish::Permanent,
        PartyID,
        BatchID
    >;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            StreamMulticasterPushError::Start { start, parties } => {
                let (completable, permanent) = start.split();

                (
                    completable.map(|err| StreamMulticasterPushError::Start {
                        parties: parties.clone(),
                        start: err
                    }),
                    permanent.map(|err| StreamMulticasterPushError::Start {
                        parties: parties.clone(),
                        start: err
                    })
                )
            }
            StreamMulticasterPushError::Party { party, batch } => {
                let (completable, permanent) = party.split();

                (
                    completable.map(|err| StreamMulticasterPushError::Party {
                        batch: batch.clone(),
                        party: err
                    }),
                    permanent.map(|err| StreamMulticasterPushError::Party {
                        batch: batch.clone(),
                        party: err
                    })
                )
            }
            StreamMulticasterPushError::Add { add, batch } => {
                let (completable, permanent) = add.split();

                (
                    completable.map(|err| StreamMulticasterPushError::Add {
                        batch: batch.clone(),
                        add: err
                    }),
                    permanent.map(|err| StreamMulticasterPushError::Add {
                        batch: batch.clone(),
                        add: err
                    })
                )
            }
            StreamMulticasterPushError::Finish { finish, batch } => {
                let (completable, permanent) = finish.split();

                (
                    completable.map(|err| StreamMulticasterPushError::Finish {
                        batch: batch.clone(),
                        finish: err
                    }),
                    permanent.map(|err| StreamMulticasterPushError::Finish {
                        batch: batch.clone(),
                        finish: err
                    })
                )
            }
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx>
    StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx> + PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    fn complete_abort(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut <Self as PushStream<Ctx>>::StreamFlags,
        retries: &mut Option<
            Vec<
                StreamMulticasterAbortRetry<
                    Idx,
                    Stream::BatchID,
                    Stream::CancelBatchRetry
                >
            >
        >,
        idx: Idx,
        len: usize,
        batch_id: Stream::BatchID,
        err: Stream::CancelBatchError
    ) {
        let i: usize = idx.clone().into();

        match err.split() {
            (_, Some(err)) => {
                error!(target: "stream-multicaster",
                       "error canceling batch: {}",
                       err);
            }
            (Some(complete), _) => match self.rev_map[i]
                .stream
                .complete_cancel_batch(ctx, flags, &batch_id, complete)
            {
                Ok(val) => val.app_retry(|retry| match retries {
                    Some(retries) => {
                        retries.push(StreamMulticasterAbortRetry {
                            idx: idx,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push(StreamMulticasterAbortRetry {
                            idx: idx,
                            batch: batch_id,
                            retry: retry
                        });

                        *retries = Some(vec)
                    }
                }),
                Err(err) => self.complete_abort(
                    ctx, flags, retries, idx, len, batch_id, err
                )
            },
            _ => {
                error!(target: "stream-multicaster",
                       "split yielded no errors")
            }
        }
    }

    fn decide_start_result(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryResult<
                <Stream as PushStream<Ctx>>::BatchID,
                <Stream as PushStream<Ctx>>::StartBatchRetry
            >
        )>,
        errs: Option<Vec<(Idx, <Stream as PushStream<Ctx>>::StartBatchError)>>
    ) -> Result<
        RetryResult<
            <Self as PushStream<Ctx>>::BatchID,
            <Self as PushStream<Ctx>>::StartBatchRetry
        >,
        <Self as PushStream<Ctx>>::StartBatchError
    > {
        match errs {
            // There were errors.
            Some(errs) => Err(ErrorSet::create(elems, errs)),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = elems.len();
                let mut successes = Vec::with_capacity(len);
                let mut results = Vec::with_capacity(len);
                let mut all_success = true;

                for (_, res) in elems.into_iter() {
                    if let RetryResult::Success(id) = &res {
                        successes.push(id.clone())
                    } else {
                        all_success = false
                    }

                    results.push(res);
                }

                if all_success {
                    let active = bitvec![0; len];
                    let batch = StreamMulticasterBatch {
                        batch_ids: successes,
                        active: active
                    };

                    Ok(RetryResult::Success(self.batches.alloc_batch(batch)))
                } else {
                    Ok(RetryResult::Retry(results))
                }
            }
        }
    }

    fn decide_outcome<Retry, Err>(
        &mut self,
        mut results: Vec<(Idx, RetryResult<(), Retry>)>,
        errs: Option<Vec<(Idx, Err)>>
    ) -> Result<
        RetryResult<(), Vec<RetryResult<(), Retry>>>,
        CompoundBatchError<Idx, RetryResult<(), Retry>, Err>
    >
    where
        Retry: RetryWhen {
        match errs {
            // There were errors
            Some(errs) => Err(CompoundBatchError::Batch {
                errs: ErrorSet::create(results, errs)
            }),
            // No errors, check if there are retries.
            None => {
                if results
                    .iter()
                    .all(|(_, res)| matches!(res, RetryResult::Success(_)))
                {
                    // No retries.
                    Ok(RetryResult::Success(()))
                } else {
                    results.sort_by(|(a, _), (b, _)| a.cmp(b));

                    let results =
                        results.into_iter().map(|(_, res)| res).collect();

                    // There were retries
                    Ok(RetryResult::Retry(results))
                }
            }
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx> PushStream<Ctx>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx> + PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    type AbortBatchRetry = Vec<
        StreamMulticasterAbortRetry<
            Idx,
            Stream::BatchID,
            Stream::CancelBatchRetry
        >
    >;
    type BatchID = CompoundBatchID;
    type CancelBatchError = CompoundBatchError<
        Idx,
        RetryResult<(), Stream::CancelBatchRetry>,
        Stream::CancelBatchError
    >;
    type CancelBatchRetry = Vec<RetryResult<(), Stream::CancelBatchRetry>>;
    type FinishBatchError = CompoundBatchError<
        Idx,
        RetryResult<
            (),
            StreamFinishCancel<
                Stream::FinishBatchRetry,
                Stream::CancelBatchRetry
            >
        >,
        StreamFinishCancel<Stream::FinishBatchError, Stream::CancelBatchError>
    >;
    type FinishBatchRetry = Vec<
        RetryResult<
            (),
            StreamFinishCancel<
                Stream::FinishBatchRetry,
                Stream::CancelBatchRetry
            >
        >
    >;
    type ReportError = CompoundBatchError<Idx, (), Stream::ReportError>;
    type StartBatchError = ErrorSet<
        Idx,
        RetryResult<Stream::BatchID, Stream::StartBatchRetry>,
        Stream::StartBatchError
    >;
    type StartBatchRetry =
        Vec<RetryResult<Stream::BatchID, Stream::StartBatchRetry>>;
    type StartBatchStreamBatches = Stream::StartBatchStreamBatches;
    type StreamFlags = Stream::StreamFlags;

    #[inline]
    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        Self::StartBatchStreamBatches::with_capacity(self.rev_map.len())
    }

    #[inline]
    fn empty_flags(&self) -> Self::StreamFlags {
        Self::StreamFlags::with_capacity(self.rev_map.len())
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let len = self.rev_map.len();
        let mut ids = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::StartBatchError)>> = None;

        // Have each party create a new batch.
        for (i, party) in self.rev_map.iter_mut().enumerate() {
            match party.stream.start_batch(ctx, batches) {
                // We're good; add this to the output.
                Ok(id) => ids.push((Idx::from(i), id)),
                // An error happened; record the fact that we still
                // need to create a batch for this party.
                Err(err) => match &mut errs {
                    Some(errs) => errs.push((Idx::from(i), err)),
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push((Idx::from(i), err));

                        errs = Some(vec)
                    }
                }
            }
        }

        self.decide_start_result(ids, errs)
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as BatchError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let (results, _) = err.take();
        let mut retries: Option<
            Vec<
                StreamMulticasterAbortRetry<
                    Idx,
                    Stream::BatchID,
                    Stream::CancelBatchRetry
                >
            >
        > = None;
        let len = results.len();

        for (idx, result) in results {
            let i: usize = idx.clone().into();

            result.app(|batch_id| {
                match self.rev_map[i].stream.cancel_batch(ctx, flags, &batch_id)
                {
                    Ok(val) => val.app_retry(|retry| match &mut retries {
                        Some(retries) => {
                            retries.push(StreamMulticasterAbortRetry {
                                idx: idx,
                                batch: batch_id,
                                retry: retry
                            })
                        }
                        None => {
                            let mut vec = Vec::with_capacity(len);

                            vec.push(StreamMulticasterAbortRetry {
                                idx: idx,
                                batch: batch_id,
                                retry: retry
                            });

                            retries = Some(vec)
                        }
                    }),
                    Err(err) => self.complete_abort(
                        ctx,
                        flags,
                        &mut retries,
                        idx,
                        len,
                        batch_id,
                        err
                    )
                }
            });
        }

        match retries {
            Some(retries) => RetryResult::Retry(retries),
            None => RetryResult::Success(())
        }
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let mut retries: Option<
            Vec<
                StreamMulticasterAbortRetry<
                    Idx,
                    Stream::BatchID,
                    Stream::CancelBatchRetry
                >
            >
        > = None;
        let len = retry.len();

        for ent in retry {
            let StreamMulticasterAbortRetry {
                batch: batch_id,
                idx,
                retry
            } = ent;
            let i: usize = idx.clone().into();

            match self.rev_map[i]
                .stream
                .retry_cancel_batch(ctx, flags, &batch_id, retry)
            {
                Ok(val) => val.app_retry(|retry| match &mut retries {
                    Some(retries) => {
                        retries.push(StreamMulticasterAbortRetry {
                            idx: idx,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push(StreamMulticasterAbortRetry {
                            idx: idx,
                            batch: batch_id,
                            retry: retry
                        });

                        retries = Some(vec)
                    }
                }),
                Err(err) => self.complete_abort(
                    ctx,
                    flags,
                    &mut retries,
                    idx,
                    len,
                    batch_id,
                    err
                )
            }
        }

        match retries {
            Some(retries) => RetryResult::Retry(retries),
            None => RetryResult::Success(())
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        retries: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        // Decompose the error set into successes and retries.
        let mut ids = Vec::with_capacity(self.rev_map.len());
        let mut errs: Option<Vec<(Idx, Stream::StartBatchError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (i, res) in retries.into_iter().enumerate() {
            let idx = Idx::from(i);

            match res {
                // Actually do retries.
                RetryResult::Retry(retry) => match self.rev_map[i]
                    .stream
                    .retry_start_batch(ctx, batches, retry)
                {
                    // We're good; add this to the output.
                    Ok(id) => ids.push((Idx::from(i), id)),
                    // An error happened; record the fact that we still
                    // need to create a batch for this party.
                    Err(err) => match &mut errs {
                        Some(errs) => errs.push((Idx::from(i), err)),
                        None => {
                            let mut vec = Vec::with_capacity(len);

                            vec.push((Idx::from(i), err));

                            errs = Some(vec)
                        }
                    }
                },
                // Retain prior successes.
                res => ids.push((idx, res))
            }
        }

        self.decide_start_result(ids, errs)
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        retries: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        let (mut ids, retries) = retries.take();
        let mut errs: Option<Vec<(Idx, Stream::StartBatchError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (idx, err) in retries {
            let i: usize = idx.into();

            match self.rev_map[i]
                .stream
                .complete_start_batch(ctx, batches, err)
            {
                // We're good; add this to the output.
                Ok(id) => ids.push((Idx::from(i), id)),
                // An error happened; record the fact that we still
                // need to create a batch for this party.
                Err(err) => match &mut errs {
                    Some(errs) => errs.push((Idx::from(i), err)),
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push((Idx::from(i), err));

                        errs = Some(vec)
                    }
                }
            }
        }

        self.decide_start_result(ids, errs)
    }

    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, active }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<
                    Vec<(
                        Idx,
                        StreamFinishCancel<
                            Stream::FinishBatchError,
                            Stream::CancelBatchError
                        >
                    )>
                > = None;

                // Run through each party and try to finish the batch.
                for (i, batch_id) in batch_ids.iter().enumerate() {
                    let idx = Idx::from(i);

                    if active[i] {
                        // If this party is active, finish the batch.
                        match self.rev_map[idx.clone().into()]
                            .stream
                            .finish_batch(ctx, flags, batch_id)
                        {
                            Ok(res) => {
                                let res = res.map_retry(|retry| {
                                    StreamFinishCancel::Finish { finish: retry }
                                });

                                results.push((idx, res))
                            }
                            Err(err) => {
                                let err =
                                    StreamFinishCancel::Finish { finish: err };

                                // An error occurred; add this to the error set.
                                match &mut errs {
                                    Some(errs) => errs.push((idx, err)),
                                    None => {
                                        let mut vec = Vec::with_capacity(len);

                                        vec.push((idx, err));

                                        errs = Some(vec)
                                    }
                                }
                            }
                        }
                    } else {
                        // Otherwise, cancel the batch.
                        match self.rev_map[idx.clone().into()]
                            .stream
                            .cancel_batch(ctx, flags, batch_id)
                        {
                            Ok(res) => {
                                let res = res.map_retry(|retry| {
                                    StreamFinishCancel::Cancel { cancel: retry }
                                });

                                results.push((idx, res))
                            }
                            Err(err) => {
                                let err =
                                    StreamFinishCancel::Cancel { cancel: err };

                                // An error occurred; add this to the error set.
                                match &mut errs {
                                    Some(errs) => errs.push((idx, err)),
                                    None => {
                                        let mut vec = Vec::with_capacity(len);

                                        vec.push((idx, err));

                                        errs = Some(vec)
                                    }
                                }
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retries: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<
                    Vec<(
                        Idx,
                        StreamFinishCancel<
                            Stream::FinishBatchError,
                            Stream::CancelBatchError
                        >
                    )>
                > = None;

                for (i, retry) in retries.into_iter().enumerate() {
                    let idx = Idx::from(i);

                    match retry {
                        RetryResult::Success(()) => {
                            results.push((idx, RetryResult::Success(())))
                        }
                        RetryResult::Retry(StreamFinishCancel::Finish {
                            finish
                        }) => match self.rev_map[idx.clone().into()]
                            .stream
                            .retry_finish_batch(
                                ctx,
                                flags,
                                &batch_ids[i],
                                finish
                            ) {
                            Ok(res) => {
                                let res = res.map_retry(|retry| {
                                    StreamFinishCancel::Finish { finish: retry }
                                });

                                results.push((idx, res))
                            }
                            Err(err) => {
                                let err =
                                    StreamFinishCancel::Finish { finish: err };

                                // An error occurred; add this to the error set.
                                match &mut errs {
                                    Some(errs) => errs.push((idx, err)),
                                    None => {
                                        let mut vec = Vec::with_capacity(len);

                                        vec.push((idx, err));

                                        errs = Some(vec)
                                    }
                                }
                            }
                        },
                        RetryResult::Retry(StreamFinishCancel::Cancel {
                            cancel
                        }) => match self.rev_map[idx.clone().into()]
                            .stream
                            .retry_cancel_batch(
                                ctx,
                                flags,
                                &batch_ids[i],
                                cancel
                            ) {
                            Ok(res) => {
                                let res = res.map_retry(|retry| {
                                    StreamFinishCancel::Cancel { cancel: retry }
                                });

                                results.push((idx, res))
                            }
                            Err(err) => {
                                let err =
                                    StreamFinishCancel::Cancel { cancel: err };

                                // An error occurred; add this to the error set.
                                match &mut errs {
                                    Some(errs) => errs.push((idx, err)),
                                    None => {
                                        let mut vec = Vec::with_capacity(len);

                                        vec.push((idx, err));

                                        errs = Some(vec)
                                    }
                                }
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        errs: <Self::FinishBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<
                    Vec<(
                        Idx,
                        StreamFinishCancel<
                            Stream::FinishBatchError,
                            Stream::CancelBatchError
                        >
                    )>
                > = None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    match err {
                        StreamFinishCancel::Finish { finish } => {
                            match self.rev_map[idx.clone().into()]
                                .stream
                                .complete_finish_batch(
                                    ctx,
                                    flags,
                                    &batch_ids[i],
                                    finish
                                ) {
                                Ok(res) => {
                                    let res = res.map_retry(|retry| {
                                        StreamFinishCancel::Finish {
                                            finish: retry
                                        }
                                    });

                                    results.push((idx, res))
                                }
                                Err(err) => {
                                    let err = StreamFinishCancel::Finish {
                                        finish: err
                                    };

                                    // An error occurred; add this to the error
                                    // set.
                                    match &mut errs {
                                        Some(errs) => errs.push((idx, err)),
                                        None => {
                                            let mut vec =
                                                Vec::with_capacity(len);

                                            vec.push((idx, err));

                                            errs = Some(vec)
                                        }
                                    }
                                }
                            }
                        }
                        StreamFinishCancel::Cancel { cancel } => {
                            match self.rev_map[idx.clone().into()]
                                .stream
                                .complete_cancel_batch(
                                    ctx,
                                    flags,
                                    &batch_ids[i],
                                    cancel
                                ) {
                                Ok(res) => {
                                    let res = res.map_retry(|retry| {
                                        StreamFinishCancel::Cancel {
                                            cancel: retry
                                        }
                                    });

                                    results.push((idx, res))
                                }
                                Err(err) => {
                                    let err = StreamFinishCancel::Cancel {
                                        cancel: err
                                    };

                                    // An error occurred; add this to the error
                                    // set.
                                    match &mut errs {
                                        Some(errs) => errs.push((idx, err)),
                                        None => {
                                            let mut vec =
                                                Vec::with_capacity(len);

                                            vec.push((idx, err));

                                            errs = Some(vec)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn cancel_batches(&mut self) {
        for party in self.rev_map.iter_mut() {
            party.stream.cancel_batches()
        }

        self.batches.clear();
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                // Run through each party and try to cancel the batch.
                for (i, party) in self.rev_map.iter_mut().enumerate() {
                    let idx = Idx::from(i);

                    match party.stream.cancel_batch(ctx, flags, &batch_ids[i]) {
                        Ok(res) => results.push((idx, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retries: Vec<RetryResult<(), Stream::CancelBatchRetry>>
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                for (i, retry) in retries.into_iter().enumerate() {
                    let idx = Idx::from(i);

                    match retry {
                        RetryResult::Success(()) => {
                            results.push((idx, RetryResult::Success(())))
                        }
                        RetryResult::Retry(retry) => {
                            match self.rev_map[i].stream.retry_cancel_batch(
                                ctx,
                                flags,
                                &batch_ids[i],
                                retry
                            ) {
                                Ok(res) => results.push((idx, res)),
                                Err(err) => match &mut errs {
                                    Some(errs) => errs.push((idx.clone(), err)),
                                    None => {
                                        let mut vec = Vec::with_capacity(len);

                                        vec.push((idx.clone(), err));

                                        errs = Some(vec)
                                    }
                                }
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        errs: <Self::CancelBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    match self.rev_map[i].stream.complete_cancel_batch(
                        ctx,
                        flags,
                        &batch_ids[i],
                        err
                    ) {
                        Ok(res) => results.push((idx, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                // Free the batch if we succeed.
                let out = self.decide_outcome(results, errs);

                if let Ok(RetryResult::Success(_)) = out {
                    self.batches.free_batch(batch);
                }

                out
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, active }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::ReportError)>> = None;

                // Run through each party and try to finish the batch.
                for (i, batch_id) in batch_ids.iter().enumerate() {
                    let idx = Idx::from(i);

                    if active[i] {
                        // If this party is active, finish the batch.
                        match self.rev_map[idx.clone().into()]
                            .stream
                            .report_failure(batch_id)
                        {
                            Ok(res) => results.push((idx, res)),
                            Err(err) => match &mut errs {
                                Some(errs) => errs.push((idx, err)),
                                None => {
                                    let mut vec = Vec::with_capacity(len);

                                    vec.push((idx, err));

                                    errs = Some(vec)
                                }
                            }
                        }
                    }
                }

                match errs {
                    // There were errors
                    Some(errs) => Err(CompoundBatchError::Batch {
                        errs: ErrorSet::create(results, errs)
                    }),
                    // No errors, check if there are retries.
                    None => Ok(())
                }
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx> PushStreamAdd<Msg, Ctx>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    type AddError = CompoundBatchError<
        Idx,
        RetryResult<(), Stream::AddRetry>,
        Stream::AddError
    >;
    type AddRetry = Vec<RetryResult<(), Stream::AddRetry>>;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::AddError)>> = None;

                // Run through each party and try to add the message.
                for (i, party) in self.rev_map.iter_mut().enumerate() {
                    let idx = Idx::from(i);

                    match party.stream.add(ctx, flags, msg, &batch_ids[i]) {
                        Ok(res) => results.push((idx, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        retries: Vec<RetryResult<(), Stream::AddRetry>>
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::AddError)>> = None;

                for (i, retry) in retries.into_iter().enumerate() {
                    let idx = Idx::from(i);

                    match retry {
                        RetryResult::Success(()) => {
                            results.push((idx, RetryResult::Success(())))
                        }
                        RetryResult::Retry(retry) => match self.rev_map[i]
                            .stream
                            .retry_add(ctx, flags, msg, &batch_ids[i], retry)
                        {
                            Ok(res) => results.push((idx, res)),
                            Err(err) => match &mut errs {
                                Some(errs) => errs.push((idx.clone(), err)),
                                None => {
                                    let mut vec = Vec::with_capacity(len);

                                    vec.push((idx.clone(), err));

                                    errs = Some(vec)
                                }
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        errs: <Self::AddError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<Vec<(Idx, Stream::AddError)>> = None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    match self.rev_map[i].stream.complete_add(
                        ctx,
                        flags,
                        msg,
                        &batch_ids[i],
                        err
                    ) {
                        Ok(res) => results.push((idx, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx> PushStreamShared
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    type PartyID = Idx;
}

impl<Party, Idx, Msg, Stream, Ctx> PushStreamParties
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    type PartiesError = Infallible;
    type PartiesIter = IntoIter<(Idx, Party)>;
    type PartyInfo = Party;

    #[inline]
    fn parties(&self) -> Result<IntoIter<(Idx, Party)>, Infallible> {
        let vec: Vec<(Idx, Party)> = self
            .fwd_map
            .iter()
            .map(|(a, b)| (b.clone(), a.clone()))
            .collect();

        Ok(vec.into_iter())
    }
}

impl<Party, Idx, Msg, Stream, Ctx> PushStreamAddParty<Ctx>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAddParty<Ctx, PartyID = ()> + PushStreamAdd<Msg, Ctx>,
    Stream::BatchID: Clone
{
    type AddPartiesError = CompoundBatchError<
        Idx,
        RetryResult<(), Stream::AddPartiesRetry>,
        Stream::AddPartiesError
    >;
    type AddPartiesRetry = Vec<RetryResult<(), Stream::AddPartiesRetry>>;

    fn add_parties<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    where
        I: Iterator<Item = Idx> {
        match self.batches.get_mut(batch) {
            Some(StreamMulticasterBatch { batch_ids, active }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::AddPartiesError)>> =
                    None;

                for party in parties {
                    let i: usize = party.clone().into();

                    active.set(i, true);

                    match self.rev_map[i].stream.add_parties(
                        ctx,
                        once(()),
                        &batch_ids[i]
                    ) {
                        Ok(res) => results.push((party, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((party.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((party.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn retry_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        retries: Self::AddPartiesRetry
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::AddPartiesError)>> =
                    None;

                for (i, retry) in retries.into_iter().enumerate() {
                    let idx = Idx::from(i);

                    match retry {
                        RetryResult::Success(()) => {
                            results.push((idx, RetryResult::Success(())))
                        }
                        RetryResult::Retry(retry) => match self.rev_map[i]
                            .stream
                            .retry_add_parties(ctx, &batch_ids[i], retry)
                        {
                            Ok(res) => results.push((idx, res)),
                            Err(err) => match &mut errs {
                                Some(errs) => errs.push((idx.clone(), err)),
                                None => {
                                    let mut vec = Vec::with_capacity(len);

                                    vec.push((idx.clone(), err));

                                    errs = Some(vec)
                                }
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }

    fn complete_add_parties(
        &mut self,
        ctx: &mut Ctx,
        batch: &Self::BatchID,
        errs: <Self::AddPartiesError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddPartiesRetry>, Self::AddPartiesError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<Vec<(Idx, Stream::AddPartiesError)>> =
                    None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    match self.rev_map[i].stream.complete_add_parties(
                        ctx,
                        &batch_ids[i],
                        err
                    ) {
                        Ok(res) => results.push((idx, res)),
                        Err(err) => match &mut errs {
                            Some(errs) => errs.push((idx.clone(), err)),
                            None => {
                                let mut vec = Vec::with_capacity(len);

                                vec.push((idx.clone(), err));

                                errs = Some(vec)
                            }
                        }
                    }
                }

                self.decide_outcome(results, errs)
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }
}

impl<Party, Idx, Msg, Stream, Ctx> PushStreamSharedSingle<Msg, Ctx>
    for StreamMulticaster<Party, Idx, Msg, Stream, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx> + PushStreamAddParty<Ctx, PartyID = ()>,
    Stream::StreamFlags: Clone,
    Stream::BatchID: Clone
{
    type CancelPushError = StreamMulticasterCancelPushError<
        Self::CancelBatchError,
        Self::StreamFlags,
        Self::BatchID
    >;
    type CancelPushRetry = StreamMulticasterCancelPushRetry<
        Self::AbortBatchRetry,
        Self::CancelBatchRetry,
        Self::StreamFlags,
        Self::BatchID
    >;
    type PushError = StreamMulticasterPushError<
        Self::StartBatchError,
        Self::AddPartiesError,
        Self::AddError,
        Self::FinishBatchError,
        Self::PartyID,
        Self::BatchID
    >;
    type PushRetry = StreamMulticasterPushError<
        Self::StartBatchRetry,
        Self::AddPartiesRetry,
        Self::AddRetry,
        Self::FinishBatchRetry,
        Self::PartyID,
        Self::BatchID
    >;

    fn push<I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    where
        I: Iterator<Item = Self::PartyID> {
        let parties: Vec<Self::PartyID> = parties.collect();

        // Create the batch.
        let mut batches = self.empty_batches();
        let batch =
            match self.start_batch(ctx, &mut batches).map_err(|err| {
                StreamMulticasterPushError::Start {
                    parties: parties.clone(),
                    start: err
                }
            })? {
                RetryResult::Success(batch) => batch,
                RetryResult::Retry(retry) => {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Start {
                            parties: parties.clone(),
                            start: retry
                        }
                    ))
                }
            };

        // Add the parties.
        if let RetryResult::Retry(retry) = self
            .add_parties(ctx, parties.into_iter(), &batch)
            .map_err(|err| StreamMulticasterPushError::Party {
                batch: batch,
                party: err
            })?
        {
            return Ok(RetryResult::Retry(StreamMulticasterPushError::Party {
                batch: batch,
                party: retry
            }));
        }

        // Add the message.
        let mut flags = self.empty_flags();

        if let RetryResult::Retry(retry) = self
            .add(ctx, &mut flags, msg, &batch)
            .map_err(|err| StreamMulticasterPushError::Add {
                batch: batch,
                add: err
            })?
        {
            return Ok(RetryResult::Retry(StreamMulticasterPushError::Add {
                batch: batch,
                add: retry
            }));
        }

        // Finish the batch.
        let mut flags = self.empty_flags();

        Ok(self
            .finish_batch(ctx, &mut flags, &batch)
            .map_err(|err| StreamMulticasterPushError::Finish {
                batch: batch,
                finish: err
            })?
            .map_retry(|retry| StreamMulticasterPushError::Finish {
                batch: batch,
                finish: retry
            })
            .map(|()| batch))
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match retry {
            StreamMulticasterPushError::Start {
                parties,
                start: retry
            } => {
                // Create the batch.
                let mut batches = self.empty_batches();
                let batch = match self
                    .retry_start_batch(ctx, &mut batches, retry)
                    .map_err(|err| StreamMulticasterPushError::Start {
                        parties: parties.clone(),
                        start: err
                    })? {
                    RetryResult::Success(batch) => batch,
                    RetryResult::Retry(retry) => {
                        return Ok(RetryResult::Retry(
                            StreamMulticasterPushError::Start {
                                parties: parties,
                                start: retry
                            }
                        ))
                    }
                };

                // Add the parties.
                if let RetryResult::Retry(retry) = self
                    .add_parties(ctx, parties.into_iter(), &batch)
                    .map_err(|err| StreamMulticasterPushError::Party {
                        batch: batch,
                        party: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Party {
                            batch: batch,
                            party: retry
                        }
                    ));
                }

                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .add(ctx, &mut flags, msg, &batch)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Party {
                batch,
                party: retry
            } => {
                // Add the parties.
                if let RetryResult::Retry(retry) = self
                    .retry_add_parties(ctx, &batch, retry)
                    .map_err(|err| StreamMulticasterPushError::Party {
                        batch: batch,
                        party: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Party {
                            batch: batch,
                            party: retry
                        }
                    ));
                }

                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .add(ctx, &mut flags, msg, &batch)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Add { batch, add: retry } => {
                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .retry_add(ctx, &mut flags, msg, &batch, retry)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Finish {
                batch,
                finish: retry
            } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .retry_finish_batch(ctx, &mut flags, &batch, retry)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match err {
            StreamMulticasterPushError::Start {
                parties,
                start: err
            } => {
                // Create the batch.
                let mut batches = self.empty_batches();
                let batch = match self
                    .complete_start_batch(ctx, &mut batches, err)
                    .map_err(|err| StreamMulticasterPushError::Start {
                        parties: parties.clone(),
                        start: err
                    })? {
                    RetryResult::Success(batch) => batch,
                    RetryResult::Retry(retry) => {
                        return Ok(RetryResult::Retry(
                            StreamMulticasterPushError::Start {
                                parties: parties,
                                start: retry
                            }
                        ))
                    }
                };

                // Add the parties.
                if let RetryResult::Retry(retry) = self
                    .add_parties(ctx, parties.into_iter(), &batch)
                    .map_err(|err| StreamMulticasterPushError::Party {
                        batch: batch,
                        party: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Party {
                            batch: batch,
                            party: retry
                        }
                    ));
                }

                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .add(ctx, &mut flags, msg, &batch)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Party { batch, party: err } => {
                // Add the parties.
                if let RetryResult::Retry(retry) = self
                    .complete_add_parties(ctx, &batch, err)
                    .map_err(|err| StreamMulticasterPushError::Party {
                        batch: batch,
                        party: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Party {
                            batch: batch,
                            party: retry
                        }
                    ));
                }

                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .add(ctx, &mut flags, msg, &batch)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Add { batch, add: err } => {
                // Add the message.
                let mut flags = self.empty_flags();

                if let RetryResult::Retry(retry) = self
                    .complete_add(ctx, &mut flags, msg, &batch, err)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                {
                    return Ok(RetryResult::Retry(
                        StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        }
                    ));
                }

                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .finish_batch(ctx, &mut flags, &batch)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
            StreamMulticasterPushError::Finish { batch, finish: err } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .complete_finish_batch(ctx, &mut flags, &batch, err)
                    .map_err(|err| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Finish {
                        batch: batch,
                        finish: retry
                    })
                    .map(|()| batch))
            }
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        let mut flags = self.empty_flags();

        match err {
            // The batch was never fully created.  The right move is
            // to call abort_start_batch.
            StreamMulticasterPushError::Start { start, .. } => Ok(self
                .abort_start_batch(ctx, &mut flags, start)
                .map_retry(|retry| StreamMulticasterCancelPushRetry::Start {
                    start: retry,
                    flags: flags
                })),
            StreamMulticasterPushError::Party { batch, .. } |
            StreamMulticasterPushError::Add { batch, .. } |
            StreamMulticasterPushError::Finish { batch, .. } => Ok(self
                .cancel_batch(ctx, &mut flags, &batch)
                .map_err(|err| StreamMulticasterCancelPushError::Cancel {
                    batch_id: batch,
                    cancel: err,
                    flags: flags.clone()
                })?
                .map_retry(|retry| StreamMulticasterCancelPushRetry::Cancel {
                    batch_id: batch,
                    cancel: retry,
                    flags: flags
                }))
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match retry {
            StreamMulticasterCancelPushRetry::Start { start, mut flags } => {
                Ok(self
                    .retry_abort_start_batch(ctx, &mut flags, start)
                    .map_retry(|retry| {
                        StreamMulticasterCancelPushRetry::Start {
                            start: retry,
                            flags: flags
                        }
                    }))
            }
            StreamMulticasterCancelPushRetry::Cancel {
                cancel,
                batch_id,
                mut flags
            } => Ok(self
                .retry_cancel_batch(ctx, &mut flags, &batch_id, cancel)
                .map_err(|err| StreamMulticasterCancelPushError::Cancel {
                    batch_id: batch_id,
                    cancel: err,
                    flags: flags.clone()
                })?
                .map_retry(|retry| StreamMulticasterCancelPushRetry::Cancel {
                    batch_id: batch_id,
                    cancel: retry,
                    flags: flags
                }))
        }
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            StreamMulticasterCancelPushError::Cancel {
                batch_id,
                cancel,
                mut flags
            } => Ok(self
                .complete_cancel_batch(ctx, &mut flags, &batch_id, cancel)
                .map_err(|err| StreamMulticasterCancelPushError::Cancel {
                    batch_id: batch_id,
                    cancel: err,
                    flags: flags.clone()
                })?
                .map_retry(|retry| StreamMulticasterCancelPushRetry::Cancel {
                    batch_id: batch_id,
                    cancel: retry,
                    flags: flags
                }))
        }
    }
}

impl<BatchID> From<Vec<BatchID>> for StreamMulticasterBatch<BatchID> {
    #[inline]
    fn from(val: Vec<BatchID>) -> StreamMulticasterBatch<BatchID> {
        let len = val.len();

        StreamMulticasterBatch {
            batch_ids: val,
            active: bitvec![0; len]
        }
    }
}

impl<Report, Party> Display for StreamMulticasterReportError<Report, Party>
where
    Report: Display,
    Party: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterReportError::Report { error } => error.fmt(f),
            StreamMulticasterReportError::NotFound { party } => {
                write!(f, "no stream for party {}", party)
            }
        }
    }
}

impl<Cancel, Flags, BatchID> Display
    for StreamMulticasterCancelPushError<Cancel, Flags, BatchID>
where
    Cancel: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterCancelPushError::Cancel { cancel, .. } => {
                cancel.fmt(f)
            }
        }
    }
}

impl<Start, Party, Add, Finish, PartyID, BatchID> Display
    for StreamMulticasterPushError<Start, Party, Add, Finish, PartyID, BatchID>
where
    Start: Display,
    Party: Display,
    Add: Display,
    Finish: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterPushError::Start { start, .. } => start.fmt(f),
            StreamMulticasterPushError::Party { party, .. } => party.fmt(f),
            StreamMulticasterPushError::Add { add, .. } => add.fmt(f),
            StreamMulticasterPushError::Finish { finish, .. } => finish.fmt(f)
        }
    }
}

impl<Start, Party, Add, Finish> Display
    for StreamMulticasterPushReportError<Start, Party, Add, Finish>
where
    Start: Display,
    Party: Display,
    Add: Display,
    Finish: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterPushReportError::Start { start } => start.fmt(f),
            StreamMulticasterPushReportError::Party { party } => party.fmt(f),
            StreamMulticasterPushReportError::Add { add } => add.fmt(f),
            StreamMulticasterPushReportError::Finish { finish } => finish.fmt(f)
        }
    }
}
