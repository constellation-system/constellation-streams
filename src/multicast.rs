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

//! Synthetic multicasting combinator for streams.
//!
//! This module implements [StreamMulticaster], which provides a
//! synthetic multicasting capability for [PushStream]s.  See its
//! documentation for full details.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Instant;
use std::vec::IntoIter;

use bitvec::bitvec;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashID;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
use log::debug;
use log::error;
use log::trace;

use crate::config::StreamMulticasterConfig;
use crate::error::CompoundBatchError;
use crate::error::ErrorSet;
use crate::error::SelectionsError;
use crate::frags::Frags;
use crate::generated::large_obj::LargeObjFragReq;
use crate::large_obj::LargeObjID;
use crate::stream::CompoundBatchID;
use crate::stream::CompoundBatches;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::Parties;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::StreamFinishCancel;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;

/// Information about counterparty streams.
struct StreamMulticasterParty<Party, Stream, Frags> {
    /// The counterparty for this stream.
    party: Party,
    /// The lower-level stream to use.
    stream: Stream,
    /// Parameter for creating fragments.
    frags: Frags
}

/// Type of batch IDs used by [StreamMulticaster].
#[derive(Clone)]
struct StreamMulticasterBatch<BatchID> {
    /// Array of batch IDs for each party stream that is a part of
    /// this batch.
    batch_ids: Vec<Option<BatchID>>
}

#[derive(Clone)]
pub struct StreamMulticasterSelections<Inner> {
    inner: Vec<Option<Inner>>
}

pub type DatagramStreamMulticaster<Party, Idx, Stream, Ctx> =
    StreamMulticaster<Party, Idx, Stream, (), Ctx>;

pub type LargeObjStreamMulticaster<Party, Idx, Stream, Ctx> = StreamMulticaster<
    Party,
    Idx,
    Stream,
    <<Stream as LargeObjStream<Ctx>>::Frags as Frags>::Param,
    Ctx
>;

/// Synthetic multicasting combinator for [PushStream]s.
///
/// This combinator maintains a separate stream for each of a set of
/// counterparties, and replicates batching commands on each of these
/// streams.
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
    Stream: PushStream<Ctx>,
    Frags,
    Ctx
> {
    ctx: PhantomData<Ctx>,
    /// Map from the `Party` type to a dense index type.
    fwd_map: HashMap<Party, Idx>,
    /// Map from dense index types to party and stream data.
    rev_map: Vec<StreamMulticasterParty<Party, Stream, Frags>>,
    /// Currently-live batches.
    batches: CompoundBatches<StreamMulticasterBatch<Stream::BatchID>>
}

pub struct StreamMulticasterFrags<Idx, F>
where
    Idx: Clone + Display + From<usize> + Into<usize>,
    F: Frags {
    idx: PhantomData<Idx>,
    frags: Vec<F>
}

#[derive(Clone, Debug)]
pub struct MulticastRetry<Idx, Retry>
where
    Retry: RetryWhen {
    retries: Vec<Retry>,
    indefs: Vec<Idx>
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
/// [start_batch](PushStreamShared::start_batch) implementation.
#[derive(Clone)]
pub enum StreamMulticasterStartError<Select, Create, Selections, Batches> {
    /// Error occurred while selecting streams for the batch.
    Select {
        /// Error from selecting streams for the batch.
        select: Select,
        /// Selection information.
        selections: Selections
    },
    /// Error occurred while starting the batch.
    Create {
        /// Error from starting the batch.
        create: Create,
        /// Selection information.
        selections: Selections,
        /// Batch cache.
        batches: Batches
    }
}

/// Errors that can occur when attempting to report another error that
/// occurred while starting a batch.
#[derive(Debug)]
pub enum StreamMulticasterStartReportError<Select, Create> {
    /// Error reporting an error from the select phase.
    Select {
        /// Err while reporting.
        select: Select
    },
    /// Error reporting an error from the create phase.
    Create {
        /// Err while reporting.
        create: Create
    }
}

/// Errors that can occur in a complete
/// [push](PushStreamSharedSingle::push) implementation.
#[derive(Clone, Debug)]
pub enum StreamMulticasterPushError<Start, Add, Finish, BatchID> {
    /// Error occurred while starting the batch.
    Start {
        /// Error from starting the batch.
        start: Start
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
#[derive(Debug)]
pub enum StreamMulticasterPushReportError<Start, Add, Finish> {
    /// Error reporting an error from the start phase.
    Start {
        /// Err while reporting.
        start: Start
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

#[derive(Debug)]
pub enum StreamMulticasterCreateError<Stream, Refresh> {
    Stream { err: Stream },
    Refresh { err: Refresh }
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
#[derive(Clone, Debug)]
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

#[derive(Debug)]
pub enum StreamMulticasterBatchPartiesError {
    NotFound { batch_id: CompoundBatchID }
}

impl<Inner> Default for StreamMulticasterSelections<Inner> {
    #[inline]
    fn default() -> Self {
        StreamMulticasterSelections { inner: Vec::new() }
    }
}

impl<Idx, Retry> RetryWhen for MulticastRetry<Idx, Retry>
where
    Retry: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        self.retries.when()
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

impl<Select, Create, Selections, Batches> RetryWhen
    for StreamMulticasterStartError<Select, Create, Selections, Batches>
where
    Select: RetryWhen,
    Create: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            StreamMulticasterStartError::Select { select, .. } => select.when(),
            StreamMulticasterStartError::Create { create, .. } => create.when()
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

impl ScopedError for StreamMulticasterBatchPartiesError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterBatchPartiesError::NotFound { .. } => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<Select, Create, Selections, Batches> ScopedError
    for StreamMulticasterStartError<Select, Create, Selections, Batches>
where
    Select: ScopedError,
    Create: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterStartError::Select { select, .. } => {
                select.scope()
            }
            StreamMulticasterStartError::Create { create, .. } => create.scope()
        }
    }
}

impl<Select, Create> ScopedError
    for StreamMulticasterStartReportError<Select, Create>
where
    Select: ScopedError,
    Create: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterStartReportError::Select { select } => {
                select.scope()
            }
            StreamMulticasterStartReportError::Create { create } => {
                create.scope()
            }
        }
    }
}

impl<Start, Add, Finish, BatchID> ScopedError
    for StreamMulticasterPushError<Start, Add, Finish, BatchID>
where
    Start: ScopedError,
    Add: ScopedError,
    Finish: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterPushError::Start { start } => start.scope(),
            StreamMulticasterPushError::Add { add, .. } => add.scope(),
            StreamMulticasterPushError::Finish { finish, .. } => finish.scope()
        }
    }
}

impl<Start, Add, Finish> ScopedError
    for StreamMulticasterPushReportError<Start, Add, Finish>
where
    Start: ScopedError,
    Add: ScopedError,
    Finish: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            StreamMulticasterPushReportError::Start { start } => start.scope(),
            StreamMulticasterPushReportError::Add { add } => add.scope(),
            StreamMulticasterPushReportError::Finish { finish } => {
                finish.scope()
            }
        }
    }
}

impl<Start, Add, Finish, BatchID> RetryWhen
    for StreamMulticasterPushError<Start, Add, Finish, BatchID>
where
    Start: RetryWhen,
    Add: RetryWhen,
    Finish: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            StreamMulticasterPushError::Start { start } => start.when(),
            StreamMulticasterPushError::Add { add, .. } => add.when(),
            StreamMulticasterPushError::Finish { finish, .. } => finish.when()
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx, Inner, Info>
    PushStreamReportError<SelectionsError<Inner, Info>>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>:
        PushStreamReportError<Inner>
{
    type ReportError = <Self as PushStreamReportError<Inner>>::ReportError;

    fn report_error(
        &mut self,
        errors: &SelectionsError<Inner, Info>
    ) -> Result<(), Self::ReportError> {
        if let SelectionsError::Inner { inner: err } = errors {
            self.report_error(err)
        } else {
            Ok(())
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx, Select, Create, Selections, Batches>
    PushStreamReportError<
        StreamMulticasterStartError<Select, Create, Selections, Batches>
    > for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>:
        PushStreamReportError<Select> + PushStreamReportError<Create>
{
    type ReportError = StreamMulticasterStartReportError<
        <Self as PushStreamReportError<Select>>::ReportError,
        <Self as PushStreamReportError<Create>>::ReportError
    >;

    fn report_error(
        &mut self,
        errors: &StreamMulticasterStartError<
            Select,
            Create,
            Selections,
            Batches
        >
    ) -> Result<(), Self::ReportError> {
        match errors {
            StreamMulticasterStartError::Select { select, .. } => {
                self.report_error(select).map_err(|err| {
                    StreamMulticasterStartReportError::Select { select: err }
                })
            }
            StreamMulticasterStartError::Create { create, .. } => {
                self.report_error(create).map_err(|err| {
                    StreamMulticasterStartReportError::Create { create: err }
                })
            }
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx, Success, Err>
    PushStreamReportError<ErrorSet<Idx, Success, Err>>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx> + PushStreamReportError<Err>,
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

impl<Party, Idx, Stream, Frags, Ctx, Success, Err>
    PushStreamReportBatchError<ErrorSet<Idx, Success, Err>, CompoundBatchID>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>
{
    type ReportBatchError = CompoundBatchError<Idx, (), Stream::ReportError>;

    fn report_error_with_batch(
        &mut self,
        batch: &CompoundBatchID,
        errors: &ErrorSet<Idx, Success, Err>
    ) -> Result<(), Self::ReportBatchError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::ReportError)>> = None;

                // Run through each error, get the batch ID, and
                // report the error up.
                for (idx, _) in errors.errors() {
                    let i: usize = idx.clone().into();

                    if let Some(batch_id) = &batch_ids[i] &&
                        let Err(err) =
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

impl<Party, Idx, Stream, Frags, Ctx, Success, Err>
    PushStreamReportBatchError<
        CompoundBatchError<Idx, Success, Err>,
        CompoundBatchID
    > for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>:
        PushStreamReportBatchError<
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

impl<Party, Idx, Stream, Frags, Ctx, Start, Add, Finish, BatchID>
    PushStreamReportError<
        StreamMulticasterPushError<Start, Add, Finish, BatchID>
    > for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>:
        PushStreamReportError<Start>
            + PushStreamReportBatchError<Add, BatchID>
            + PushStreamReportBatchError<Finish, BatchID>
{
    type ReportError = StreamMulticasterPushReportError<
        <Self as PushStreamReportError<Start>>::ReportError,
        <Self as PushStreamReportBatchError<Add, BatchID>>::ReportBatchError,
        <Self as PushStreamReportBatchError<Finish, BatchID>>::ReportBatchError
    >;

    fn report_error(
        &mut self,
        errors: &StreamMulticasterPushError<Start, Add, Finish, BatchID>
    ) -> Result<(), Self::ReportError> {
        match errors {
            StreamMulticasterPushError::Start { start } => {
                self.report_error(start).map_err(|err| {
                    StreamMulticasterPushReportError::Start { start: err }
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

impl<'a, Party, Idx, Stream, F, Ctx>
    CreateWithParam<(&'a mut Ctx, Option<&'a Party>)>
    for StreamMulticaster<Party, Idx, Stream, F, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: CreateWithParam<&'a Ctx> + PushStream<Ctx>,
    F: Default
{
    type Config = StreamMulticasterConfig<Party, F, Stream::Config>;
    type CreateError = Stream::CreateError;

    fn create(
        config: Self::Config,
        param: (&'a mut Ctx, Option<&'a Party>)
    ) -> Result<Self, Self::CreateError> {
        let (ctx, self_party) = param;
        let (parties, slots_config) = config.take();
        let mut rev_map = Vec::with_capacity(parties.len());
        let mut fwd_map = HashMap::with_capacity(rev_map.len());

        debug!(target: "stream-multicaster",
               "creating stream multicaster");

        for (i, config) in parties.into_iter().enumerate() {
            let (party, stream, frags) = config.take();

            debug!(target: "stream-multicaster",
                   "creating individual stream for party {}",
                   party);

            if self_party != Some(&party) {
                let stream = Stream::create(stream, ctx)?;
                let ent = StreamMulticasterParty {
                    party: party.clone(),
                    stream: stream,
                    frags: frags
                };

                rev_map.push(ent);
                fwd_map.insert(party, Idx::from(i));
            }
        }

        Ok(StreamMulticaster {
            ctx: PhantomData,
            batches: CompoundBatches::create(slots_config),
            fwd_map: fwd_map,
            rev_map: rev_map
        })
    }
}

impl<Party, Idx, Stream, F, Ctx> StreamMulticaster<Party, Idx, Stream, F, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Stream::BatchID: Clone,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>
{
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

    #[inline]
    pub fn stream(
        &self,
        idx: Idx
    ) -> &Stream {
        let i: usize = idx.into();

        &self.rev_map[i].stream
    }
}

impl<Cancel, Flags, BatchID> RecoverableError
    for StreamMulticasterCancelPushError<Cancel, Flags, BatchID>
where
    Cancel: RecoverableError,
    Flags: Clone,
    BatchID: Clone + Debug
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

impl<Select, Create, Selections, Batches> RecoverableError
    for StreamMulticasterStartError<Select, Create, Selections, Batches>
where
    Select: RecoverableError,
    Create: RecoverableError,
    Selections: Clone,
    Batches: Clone
{
    type Completable = StreamMulticasterStartError<
        Select::Completable,
        Create::Completable,
        Selections,
        Batches
    >;
    type Permanent = StreamMulticasterStartError<
        Select::Permanent,
        Create::Permanent,
        Selections,
        Batches
    >;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            StreamMulticasterStartError::Select { select, selections } => {
                let (completable, permanent) = select.split();

                (
                    completable.map(|err| {
                        StreamMulticasterStartError::Select {
                            selections: selections.clone(),
                            select: err
                        }
                    }),
                    permanent.map(|err| StreamMulticasterStartError::Select {
                        selections: selections,
                        select: err
                    })
                )
            }
            StreamMulticasterStartError::Create {
                create,
                selections,
                batches
            } => {
                let (completable, permanent) = create.split();

                (
                    completable.map(|err| {
                        StreamMulticasterStartError::Create {
                            selections: selections.clone(),
                            batches: batches.clone(),
                            create: err
                        }
                    }),
                    permanent.map(|err| StreamMulticasterStartError::Create {
                        selections: selections,
                        batches: batches,
                        create: err
                    })
                )
            }
        }
    }
}

impl<Start, Add, Finish, BatchID> RecoverableError
    for StreamMulticasterPushError<Start, Add, Finish, BatchID>
where
    Start: RecoverableError,
    Add: RecoverableError,
    Finish: RecoverableError,
    BatchID: Clone + Debug
{
    type Completable = StreamMulticasterPushError<
        Start::Completable,
        Add::Completable,
        Finish::Completable,
        BatchID
    >;
    type Permanent = StreamMulticasterPushError<
        Start::Permanent,
        Add::Permanent,
        Finish::Permanent,
        BatchID
    >;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            StreamMulticasterPushError::Start { start } => {
                let (completable, permanent) = start.split();

                (
                    completable.map(|err| StreamMulticasterPushError::Start {
                        start: err
                    }),
                    permanent.map(|err| StreamMulticasterPushError::Start {
                        start: err
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

impl<Party, Idx, Stream, Frags, Ctx>
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
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

impl<Party, Idx, Stream, Ctx>
    StreamMulticaster<
        Party,
        Idx,
        Stream,
        <<Stream as LargeObjStream<Ctx>>::Frags as Frags>::Param,
        Ctx
    >
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: LargeObjStream<Ctx, Parties = ()> + PushStream<Ctx>,
    Stream::BatchID: Clone
{
    fn decide_push_frag_result(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryIndefResult<
                Option<Instant>,
                <Stream as LargeObjStream<Ctx>>::PushFragRetry
            >
        )>,
        errs: Option<
            Vec<(Idx, <Stream as LargeObjStream<Ctx>>::PushFragError)>
        >
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            <Self as LargeObjStream<Ctx>>::PushFragRetry,
            Parties<Vec<Idx>>
        >,
        <Self as LargeObjStream<Ctx>>::PushFragError
    > {
        match errs {
            // There were errors.
            Some(errs) => Err(ErrorSet::create(elems, errs)),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut indefs = Vec::with_capacity(len);
                let mut ids = Vec::with_capacity(len);
                let mut has_retry = false;
                let mut all_indef = true;
                let mut min = None;

                for (id, res) in elems.into_iter() {
                    match res {
                        RetryIndefResult::Success(val) => {
                            all_indef = false;
                            results.push(RetryResult::Success(val));
                            ids.push(id);
                        }
                        RetryIndefResult::Retry(retry) => {
                            all_indef = false;
                            has_retry = true;
                            min =
                                Some(next_retry_definite(&min, &retry.when()));
                            results.push(RetryResult::Retry(retry));
                        }
                        RetryIndefResult::Indef(()) => indefs.push(id)
                    }
                }

                if all_indef {
                    Ok(RetryIndefResult::Indef(Parties::Some(indefs)))
                } else if has_retry {
                    let out = MulticastRetry {
                        retries: results,
                        indefs: indefs
                    };

                    Ok(RetryIndefResult::Retry(out))
                } else {
                    Ok(RetryIndefResult::Success((min, ids)))
                }
            }
        }
    }

    fn decide_push_offer_result<H>(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryIndefResult<
                Option<Instant>,
                <Stream as LargeObjOfferStream<H, Ctx>>::PushOfferRetry
            >
        )>,
        errs: Option<
            Vec<(Idx, <Stream as LargeObjOfferStream<H, Ctx>>::PushOfferError)>
        >
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            <Self as LargeObjOfferStream<H, Ctx>>::PushOfferRetry,
            Parties<Vec<Idx>>
        >,
        <Self as LargeObjOfferStream<H, Ctx>>::PushOfferError
    >
    where
        Stream: LargeObjOfferStream<H, Ctx> + PushStream<Ctx>,
        H: Clone + HashID {
        match errs {
            // There were errors.
            Some(errs) => Err(ErrorSet::create(elems, errs)),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut indefs = Vec::with_capacity(len);
                let mut ids = Vec::with_capacity(len);
                let mut has_retry = false;
                let mut all_indef = true;
                let mut min = None;

                for (id, res) in elems.into_iter() {
                    match res {
                        RetryIndefResult::Success(val) => {
                            all_indef = false;
                            results.push(RetryResult::Success(val));
                            ids.push(id);
                        }
                        RetryIndefResult::Retry(retry) => {
                            all_indef = false;
                            has_retry = true;
                            min =
                                Some(next_retry_definite(&min, &retry.when()));
                            results.push(RetryResult::Retry(retry));
                        }
                        RetryIndefResult::Indef(()) => indefs.push(id)
                    }
                }

                if all_indef {
                    Ok(RetryIndefResult::Indef(Parties::Some(indefs)))
                } else if has_retry {
                    let out = MulticastRetry {
                        retries: results,
                        indefs: indefs
                    };

                    Ok(RetryIndefResult::Retry(out))
                } else {
                    Ok(RetryIndefResult::Success((min, ids)))
                }
            }
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx>
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStreamPrivate<Ctx> + PushStream<Ctx>,
    Stream::BatchID: Clone + Debug
{
    fn decide_select_result(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryIndefResult<
                (),
                <Stream as PushStreamPrivate<Ctx>>::SelectRetry
            >
        )>,
        errs: Option<
            Vec<(Idx, <Stream as PushStreamPrivate<Ctx>>::SelectError)>
        >
    ) -> Result<
        RetryIndefResult<
            Vec<Idx>,
            <Self as PushStreamShared<Ctx>>::SelectRetry,
            Vec<Idx>
        >,
        <Self as PushStreamShared<Ctx>>::SelectError
    > {
        match errs {
            // There were errors.
            Some(errs) => Err(SelectionsError::Inner {
                inner: ErrorSet::create(elems, errs)
            }),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut indefs = Vec::with_capacity(len);
                let mut ids = Vec::with_capacity(len);
                let mut has_retry = false;
                let mut all_indef = true;

                for (id, res) in elems.into_iter() {
                    match res {
                        RetryIndefResult::Success(val) => {
                            all_indef = false;
                            results.push(RetryResult::Success(val));
                            ids.push(id);
                        }
                        RetryIndefResult::Retry(when) => {
                            all_indef = false;
                            has_retry = true;
                            results.push(RetryResult::Retry(when));
                        }
                        RetryIndefResult::Indef(()) => indefs.push(id)
                    }
                }

                if all_indef {
                    Ok(RetryIndefResult::Indef(indefs))
                } else if has_retry {
                    let out = MulticastRetry {
                        retries: results,
                        indefs: indefs
                    };

                    Ok(RetryIndefResult::Retry(out))
                } else {
                    Ok(RetryIndefResult::Success(ids))
                }
            }
        }
    }

    fn decide_select_retry_result(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryIndefResult<
                (),
                <Stream as PushStreamPrivate<Ctx>>::SelectRetry
            >
        )>,
        mut indefs: Vec<Idx>,
        errs: Option<
            Vec<(Idx, <Stream as PushStreamPrivate<Ctx>>::SelectError)>
        >
    ) -> Result<
        RetryIndefResult<
            Vec<Idx>,
            <Self as PushStreamShared<Ctx>>::SelectRetry,
            Vec<Idx>
        >,
        <Self as PushStreamShared<Ctx>>::SelectError
    > {
        match errs {
            // There were errors.
            // XXX need to capture the indefinite retries here
            Some(errs) => Err(SelectionsError::Inner {
                inner: ErrorSet::create(elems, errs)
            }),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut ids = Vec::with_capacity(len);
                let mut has_retry = false;
                let mut all_indef = true;

                for (id, res) in elems.into_iter() {
                    match res {
                        RetryIndefResult::Success(val) => {
                            all_indef = false;
                            results.push(RetryResult::Success(val));
                            ids.push(id);
                        }
                        RetryIndefResult::Retry(when) => {
                            all_indef = false;
                            has_retry = true;
                            results.push(RetryResult::Retry(when));
                        }
                        RetryIndefResult::Indef(()) => indefs.push(id)
                    }
                }

                if all_indef {
                    Ok(RetryIndefResult::Indef(indefs))
                } else if has_retry {
                    let out = MulticastRetry {
                        retries: results,
                        indefs: indefs
                    };

                    Ok(RetryIndefResult::Retry(out))
                } else {
                    Ok(RetryIndefResult::Success(ids))
                }
            }
        }
    }

    fn decide_create_result(
        &mut self,
        mut elems: Vec<(
            Idx,
            RetryResult<
                <Stream as PushStream<Ctx>>::BatchID,
                <Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry
            >
        )>,
        errs: Option<
            Vec<(Idx, <Stream as PushStreamPrivate<Ctx>>::CreateBatchError)>
        >
    ) -> Result<
        RetryResult<
            <Self as PushStream<Ctx>>::BatchID,
            <Self as PushStreamShared<Ctx>>::CreateBatchRetry
        >,
        <Self as PushStreamShared<Ctx>>::CreateBatchError
    > {
        match errs {
            // There were errors.
            Some(errs) => Err(SelectionsError::Inner {
                inner: ErrorSet::create(elems, errs)
            }),
            // No errors, check for retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut successes = vec![None; len];
                let mut results = Vec::with_capacity(len);
                let mut all_success = true;

                for (idx, res) in elems.into_iter() {
                    if let RetryResult::Success(id) = &res {
                        let i: usize = idx.into();

                        successes[i] = Some(id.clone())
                    } else {
                        all_success = false
                    }

                    results.push(res);
                }

                if all_success {
                    let batch = StreamMulticasterBatch {
                        batch_ids: successes
                    };

                    Ok(RetryResult::Success(self.batches.alloc_batch(batch)))
                } else {
                    Ok(RetryResult::Retry(results))
                }
            }
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx>
    StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStream<Ctx> + StreamRefresh<Ctx>,
    Stream::BatchID: Clone
{
    fn decide_refresh_outcome<Retry, Err>(
        &mut self,
        mut elems: Vec<(Idx, RetryResult<Option<Instant>, Retry>)>,
        errs: Option<Vec<(Idx, Err)>>
    ) -> Result<
        RetryResult<Option<Instant>, Vec<RetryResult<Option<Instant>, Retry>>>,
        ErrorSet<Idx, RetryResult<Option<Instant>, Retry>, Err>
    >
    where
        Retry: RetryWhen {
        match errs {
            // There were errors
            Some(errs) => Err(ErrorSet::create(elems, errs)),
            // No errors, check if there are retries.
            None => {
                elems.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Try to convert to straightforward batch IDs.
                let len = self.rev_map.len();
                let mut min = None;
                let mut results = Vec::with_capacity(len);
                let mut all_success = true;

                for (_, res) in elems.into_iter() {
                    if let RetryResult::Success(when) = &res {
                        min = next_retry(&min, when);
                    } else {
                        all_success = false
                    }

                    results.push(res);
                }

                if all_success {
                    Ok(RetryResult::Success(min))
                } else {
                    Ok(RetryResult::Retry(results))
                }
            }
        }
    }
}

impl<Party, Idx, Stream, Frags, ChannelID, Chan, Ctx>
    StreamReporter<Party, ChannelID, Chan>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    ChannelID: Clone + Debug + Display + Eq + Hash,
    Stream: PushStream<Ctx> + StreamReporter<Party, ChannelID, Chan>,
    Stream::BatchID: Clone
{
    type ReportStreamError =
        StreamMulticasterReportError<Stream::ReportStreamError, Party>;

    fn report_stream(
        &mut self,
        party: &Party,
        id: ChannelID,
        stream: Chan
    ) -> Result<Option<Chan>, Self::ReportStreamError> {
        debug!(target: "stream-multicaster",
               "reporting stream {} for {}",
               id, party);

        match self.fwd_map.get(party) {
            Some(idx) => {
                let idx: usize = idx.clone().into();

                self.rev_map[idx]
                    .stream
                    .report_stream(party, id, stream)
                    .map_err(|err| StreamMulticasterReportError::Report {
                        error: err
                    })
            }
            None => Err(StreamMulticasterReportError::NotFound {
                party: party.clone()
            })
        }
    }
}

impl<Party, Idx, Stream, Frags, Ctx> StreamRefresh<Ctx>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStream<Ctx> + StreamRefresh<Ctx>,
    Stream::BatchID: Clone
{
    type RefreshError = ErrorSet<
        Idx,
        RetryResult<Option<Instant>, Stream::RefreshRetry>,
        Stream::RefreshError
    >;
    type RefreshRetry = Vec<RetryResult<Option<Instant>, Stream::RefreshRetry>>;

    fn refresh(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        let len = self.rev_map.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::RefreshError)>> = None;

        // Run through each party and try to add the message.
        for (i, party) in self.rev_map.iter_mut().enumerate() {
            let idx = Idx::from(i);

            match party.stream.refresh(ctx) {
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

        self.decide_refresh_outcome(results, errs)
    }

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        retries: Self::RefreshRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        let len = self.rev_map.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::RefreshError)>> = None;

        for (i, retry) in retries.into_iter().enumerate() {
            let idx = Idx::from(i);

            match retry {
                RetryResult::Success(when) => {
                    results.push((idx, RetryResult::Success(when)))
                }
                RetryResult::Retry(retry) => {
                    match self.rev_map[i].stream.retry_refresh(ctx, retry) {
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

        self.decide_refresh_outcome(results, errs)
    }

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        let len = self.rev_map.len();
        let (mut results, retries) = errs.take();
        let mut errs: Option<Vec<(Idx, Stream::RefreshError)>> = None;

        for (idx, err) in retries {
            let i: usize = idx.clone().into();

            match self.rev_map[i].stream.complete_refresh(ctx, err) {
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

        self.decide_refresh_outcome(results, errs)
    }
}

impl<Party, Idx, Stream, Frags, Ctx> PushStream<Ctx>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    Stream::BatchID: Clone
{
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
    type StreamFlags = Stream::StreamFlags;

    #[inline]
    fn empty_flags(&self) -> Self::StreamFlags {
        Self::empty_flags_with_capacity(self.rev_map.len())
    }

    #[inline]
    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
        Stream::empty_flags_with_capacity(size)
    }

    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids }) => {
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
                    if let Some(batch_id) = batch_id {
                        let idx = Idx::from(i);

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
            Some(StreamMulticasterBatch { batch_ids }) => {
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

                    if let Some(batch_id) = &batch_ids[i] {
                        match retry {
                            RetryResult::Success(()) => {
                                results.push((idx, RetryResult::Success(())))
                            }
                            RetryResult::Retry(
                                StreamFinishCancel::Finish { finish }
                            ) => match self.rev_map[idx.clone().into()]
                                .stream
                                .retry_finish_batch(
                                    ctx, flags, batch_id, finish
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

                                    // An error occurred; add this to
                                    // the error set.
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
                            },
                            RetryResult::Retry(
                                StreamFinishCancel::Cancel { cancel }
                            ) => match self.rev_map[idx.clone().into()]
                                .stream
                                .retry_cancel_batch(
                                    ctx, flags, batch_id, cancel
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

                                    // An error occurred; add this to
                                    // the error set.
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

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        errs: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids }) => {
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

                    if let Some(batch_id) = &batch_ids[i] {
                        match err {
                            StreamFinishCancel::Finish { finish } => {
                                match self.rev_map[idx.clone().into()]
                                    .stream
                                    .complete_finish_batch(
                                        ctx, flags, batch_id, finish
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

                                        // An error occurred; add this
                                        // to the error set.
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
                                        ctx, flags, batch_id, cancel
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

                                        // An error occurred; add this
                                        // to the error set.
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
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                // Run through each party and try to cancel the batch.
                for (i, party) in self.rev_map.iter_mut().enumerate() {
                    let idx = Idx::from(i);

                    if let Some(batch_id) = &batch_ids[i] {
                        match party.stream.cancel_batch(ctx, flags, batch_id) {
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
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                for (i, retry) in retries.into_iter().enumerate() {
                    let idx = Idx::from(i);

                    if let Some(batch_id) = &batch_ids[i] {
                        match retry {
                            RetryResult::Success(()) => {
                                results.push((idx, RetryResult::Success(())))
                            }
                            RetryResult::Retry(retry) => {
                                match self.rev_map[i].stream.retry_cancel_batch(
                                    ctx, flags, batch_id, retry
                                ) {
                                    Ok(res) => results.push((idx, res)),
                                    Err(err) => match &mut errs {
                                        Some(errs) => {
                                            errs.push((idx.clone(), err))
                                        }
                                        None => {
                                            let mut vec =
                                                Vec::with_capacity(len);

                                            vec.push((idx.clone(), err));

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

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        errs: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<Vec<(Idx, Stream::CancelBatchError)>> =
                    None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    if let Some(batch_id) = &batch_ids[i] {
                        match self.rev_map[i]
                            .stream
                            .complete_cancel_batch(ctx, flags, batch_id, err)
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
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::ReportError)>> = None;

                // Run through each party and try to finish the batch.
                for (i, batch_id) in batch_ids.iter().enumerate() {
                    let idx = Idx::from(i);

                    if let Some(batch_id) = batch_id {
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
                    // No errors.
                    None => Ok(())
                }
            }
            None => Err(CompoundBatchError::BadID { id: *batch })
        }
    }
}

impl<Party, Idx, Msg, Stream, Frags, Ctx> PushStreamAdd<Msg, Ctx>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
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
            Some(StreamMulticasterBatch { batch_ids }) => {
                let len = self.rev_map.len();
                let mut results = Vec::with_capacity(len);
                let mut errs: Option<Vec<(Idx, Stream::AddError)>> = None;

                // Run through each party and try to add the message.
                for (i, party) in self.rev_map.iter_mut().enumerate() {
                    let idx = Idx::from(i);

                    if let Some(batch_id) = &batch_ids[i] {
                        match party.stream.add(ctx, flags, msg, batch_id) {
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

                    if let Some(batch_id) = &batch_ids[i] {
                        match retry {
                            RetryResult::Success(()) => {
                                results.push((idx, RetryResult::Success(())))
                            }
                            RetryResult::Retry(retry) => match self.rev_map[i]
                                .stream
                                .retry_add(ctx, flags, msg, batch_id, retry)
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
        errs: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match self.batches.get(batch) {
            Some(StreamMulticasterBatch { batch_ids, .. }) => {
                let len = self.rev_map.len();
                let (mut results, retries) = errs.take();
                let mut errs: Option<Vec<(Idx, Stream::AddError)>> = None;

                for (idx, err) in retries {
                    let i: usize = idx.clone().into();

                    if let Some(batch_id) = &batch_ids[i] {
                        match self.rev_map[i]
                            .stream
                            .complete_add(ctx, flags, msg, batch_id, err)
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
}

impl<Party, Idx, Stream, Frags, Ctx> PushStreamPartyID
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
    Stream::BatchID: Clone
{
    type PartyID = Idx;
}

impl<Party, Idx, Stream, Frags, Ctx> PushStreamParties
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Display + Eq + Hash,
    Stream: PushStream<Ctx>,
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

impl<Party, Idx, Stream, Frags, Ctx> PushStreamShared<Ctx>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStreamPrivate<Ctx> + PushStream<Ctx>,
    Stream::BatchID: Clone + Debug
{
    type AbortBatchRetry = Vec<
        StreamMulticasterAbortRetry<
            Idx,
            Stream::BatchID,
            Stream::CancelBatchRetry
        >
    >;
    type BatchPartiesError = StreamMulticasterBatchPartiesError;
    type BatchPartiesIter = IntoIter<Idx>;
    type CreateBatchError = SelectionsError<
        ErrorSet<
            Idx,
            RetryResult<Stream::BatchID, Stream::CreateBatchRetry>,
            Stream::CreateBatchError
        >,
        usize
    >;
    type CreateBatchRetry =
        Vec<RetryResult<Stream::BatchID, Stream::CreateBatchRetry>>;
    type IndefParties = Vec<Idx>;
    type SelectError = SelectionsError<
        ErrorSet<
            Idx,
            RetryIndefResult<(), Stream::SelectRetry>,
            Stream::SelectError
        >,
        usize
    >;
    type SelectRetry =
        MulticastRetry<Idx, RetryResult<(), Stream::SelectRetry>>;
    type Selections = StreamMulticasterSelections<Stream::Selections>;
    type StartBatchError = StreamMulticasterStartError<
        Self::SelectError,
        Self::CreateBatchError,
        Self::Selections,
        Self::StartBatchStreamBatches
    >;
    type StartBatchRetry = StreamMulticasterStartError<
        Self::SelectRetry,
        Self::CreateBatchRetry,
        Self::Selections,
        Self::StartBatchStreamBatches
    >;
    type StartBatchStreamBatches = Stream::StartBatchStreamBatches;

    #[inline]
    fn empty_selections(&self) -> Self::Selections {
        Self::empty_selections_with_capacity(self.rev_map.len())
    }

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        StreamMulticasterSelections {
            inner: Vec::with_capacity(size)
        }
    }

    #[inline]
    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        Self::empty_batches_with_capacity(self.rev_map.len())
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Stream::empty_batches_with_capacity(size)
    }

    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError> {
        let StreamMulticasterBatch { batch_ids } =
            self.batches.get(batch_id).ok_or(
                StreamMulticasterBatchPartiesError::NotFound {
                    batch_id: *batch_id
                }
            )?;
        let ids: Vec<Idx> = batch_ids
            .iter()
            .enumerate()
            .flat_map(|(i, batch_id)| batch_id.as_ref().map(|_| Idx::from(i)))
            .collect();

        Ok(ids.into_iter())
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
        I: Iterator<Item = &'a Idx>,
        Idx: 'a {
        let len = self.rev_map.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::SelectError)>> = None;

        for _ in 0..len {
            selections.inner.push(None);
        }

        // Have each party create a new batch.
        for idx in parties {
            let i: usize = idx.clone().into();

            if selections.inner[i].is_none() {
                let selection = selections.inner[i]
                    .insert(self.rev_map[i].stream.empty_selections());

                match self.rev_map[i].stream.select(ctx, selection) {
                    // We're good; add this to the output.
                    Ok(id) => {
                        if id.is_indef() {
                            selections.inner[i] = None;
                        }

                        results.push((Idx::from(i), id))
                    }
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
        }

        self.decide_select_result(results, errs)
            .map(|res| res.map_indef(Parties::Some))
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retries: Self::SelectRetry
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        // Decompose the error set into successes and retries.
        let mut results = Vec::with_capacity(self.rev_map.len());
        let mut errs: Option<Vec<(Idx, Stream::SelectError)>> = None;
        let MulticastRetry { retries, indefs } = retries;
        let len = retries.len();
        let mut skip = bitvec![0; self.rev_map.len()];
        let mut offset = 0;

        for idx in indefs.iter() {
            let i: usize = idx.clone().into();

            skip.set(i, true);
        }

        // Go through the retries and try to create the batch.
        for (i, res) in retries.into_iter().enumerate() {
            while skip[i + offset] {
                offset += 1;
            }
            let i = i + offset;

            let idx = Idx::from(i);
            let selection = match &mut selections.inner[i] {
                Some(selections) => Ok(selections),
                None => Err(SelectionsError::NoSelections { info: i })
            }?;

            match res {
                // Actually do retries.
                RetryResult::Retry(retry) => match self.rev_map[i]
                    .stream
                    .retry_select(ctx, selection, retry)
                {
                    // We're good; add this to the output.
                    Ok(id) => {
                        if id.is_indef() {
                            selections.inner[i] = None;
                        }

                        results.push((Idx::from(i), id))
                    }
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
                RetryResult::Success(val) => {
                    results.push((idx, RetryIndefResult::Success(val)))
                }
            }
        }

        self.decide_select_retry_result(results, indefs, errs)
            .map(|res| res.map_indef(Parties::Some))
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retries: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        let (mut results, retries) = retries.take();
        let mut errs: Option<Vec<(Idx, Stream::SelectError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (idx, err) in retries {
            let i: usize = idx.into();
            let selection = match &mut selections.inner[i] {
                Some(selection) => Ok(selection),
                None => Err(SelectionsError::NoSelections { info: i })
            }?;

            match self.rev_map[i].stream.complete_select(ctx, selection, err) {
                // We're good; add this to the output.
                Ok(id) => {
                    if id.is_indef() {
                        selections.inner[i] = None;
                    }

                    results.push((Idx::from(i), id))
                }
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

        self.decide_select_result(results, errs)
            .map(|res| res.map_indef(Parties::Some))
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
        let len = self.rev_map.len();
        let mut ids = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::CreateBatchError)>> = None;

        // Have each party create a new batch.
        for (i, selections) in selections.inner.iter().enumerate() {
            if let Some(selections) = selections {
                match self.rev_map[i]
                    .stream
                    .create_batch(ctx, batches, selections)
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
        }

        self.decide_create_result(ids, errs)
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retries: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        // Decompose the error set into successes and retries.
        let mut ids = Vec::with_capacity(self.rev_map.len());
        let mut errs: Option<Vec<(Idx, Stream::CreateBatchError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (i, res) in retries.into_iter().enumerate() {
            let idx = Idx::from(i);
            let selections = match &selections.inner[i] {
                Some(selections) => Ok(selections),
                None => Err(SelectionsError::NoSelections { info: i })
            }?;

            match res {
                // Actually do retries.
                RetryResult::Retry(retry) => match self.rev_map[i]
                    .stream
                    .retry_create_batch(ctx, batches, selections, retry)
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

        self.decide_create_result(ids, errs)
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retries: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        let (mut ids, retries) = retries.take();
        let mut errs: Option<Vec<(Idx, Stream::CreateBatchError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (idx, err) in retries {
            let i: usize = idx.into();
            let selections = match &selections.inner[i] {
                Some(selections) => Ok(selections),
                None => Err(SelectionsError::NoSelections { info: i })
            }?;

            match self.rev_map[i]
                .stream
                .complete_create_batch(ctx, batches, selections, err)
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

        self.decide_create_result(ids, errs)
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
        I: Iterator<Item = &'a Idx>,
        Idx: 'a {
        let mut selections = self.empty_selections();

        match self.select(ctx, &mut selections, parties).map_err(|err| {
            StreamMulticasterStartError::Select {
                selections: selections.clone(),
                select: err
            }
        })? {
            RetryIndefResult::Success(_) => {
                let mut batches = self.empty_batches();

                match self.create_batch(ctx, &mut batches, &selections) {
                    Ok(res) => {
                        Ok(RetryIndefResult::from(res.map_retry(|retry| {
                            StreamMulticasterStartError::Create {
                                selections: selections,
                                batches: batches,
                                create: retry
                            }
                        })))
                    }
                    Err(err) => Err(StreamMulticasterStartError::Create {
                        selections: selections.clone(),
                        batches: batches.clone(),
                        create: err
                    })
                }
            }
            RetryIndefResult::Retry(retry) => Ok(RetryIndefResult::Retry(
                StreamMulticasterStartError::Select {
                    selections: selections,
                    select: retry
                }
            )),
            RetryIndefResult::Indef(parties) => {
                Ok(RetryIndefResult::Indef(parties))
            }
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retries: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        match retries {
            StreamMulticasterStartError::Select {
                select,
                mut selections
            } => match self.retry_select(ctx, &mut selections, select).map_err(
                |err| StreamMulticasterStartError::Select {
                    selections: selections.clone(),
                    select: err
                }
            )? {
                RetryIndefResult::Success(_) => {
                    let mut batches = self.empty_batches();

                    match self.create_batch(ctx, &mut batches, &selections) {
                        Ok(res) => {
                            Ok(RetryIndefResult::from(res.map_retry(|retry| {
                                StreamMulticasterStartError::Create {
                                    selections: selections,
                                    batches: batches,
                                    create: retry
                                }
                            })))
                        }
                        Err(err) => Err(StreamMulticasterStartError::Create {
                            selections: selections.clone(),
                            batches: batches.clone(),
                            create: err
                        })
                    }
                }
                RetryIndefResult::Retry(retry) => Ok(RetryIndefResult::Retry(
                    StreamMulticasterStartError::Select {
                        selections: selections,
                        select: retry
                    }
                )),
                RetryIndefResult::Indef(parties) => {
                    Ok(RetryIndefResult::Indef(parties))
                }
            },
            StreamMulticasterStartError::Create {
                selections,
                mut batches,
                create
            } => match self.retry_create_batch(
                ctx,
                &mut batches,
                &selections,
                create
            ) {
                Ok(res) => Ok(RetryIndefResult::from(res.map_retry(|retry| {
                    StreamMulticasterStartError::Create {
                        selections: selections,
                        batches: batches,
                        create: retry
                    }
                }))),
                Err(err) => Err(StreamMulticasterStartError::Create {
                    selections: selections.clone(),
                    batches: batches.clone(),
                    create: err
                })
            }
        }
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retries: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        match retries {
            StreamMulticasterStartError::Select {
                select,
                mut selections
            } => match self
                .complete_select(ctx, &mut selections, select)
                .map_err(|err| StreamMulticasterStartError::Select {
                    selections: selections.clone(),
                    select: err
                })? {
                RetryIndefResult::Success(_) => {
                    let mut batches = self.empty_batches();

                    match self.create_batch(ctx, &mut batches, &selections) {
                        Ok(res) => {
                            Ok(RetryIndefResult::from(res.map_retry(|retry| {
                                StreamMulticasterStartError::Create {
                                    selections: selections,
                                    batches: batches,
                                    create: retry
                                }
                            })))
                        }
                        Err(err) => Err(StreamMulticasterStartError::Create {
                            selections: selections.clone(),
                            batches: batches.clone(),
                            create: err
                        })
                    }
                }
                RetryIndefResult::Retry(retry) => Ok(RetryIndefResult::Retry(
                    StreamMulticasterStartError::Select {
                        selections: selections,
                        select: retry
                    }
                )),
                RetryIndefResult::Indef(parties) => {
                    Ok(RetryIndefResult::Indef(parties))
                }
            },
            StreamMulticasterStartError::Create {
                selections,
                mut batches,
                create
            } => match self.complete_create_batch(
                ctx,
                &mut batches,
                &selections,
                create
            ) {
                Ok(res) => Ok(RetryIndefResult::from(res.map_retry(|retry| {
                    StreamMulticasterStartError::Create {
                        selections: selections,
                        batches: batches,
                        create: retry
                    }
                }))),
                Err(err) => Err(StreamMulticasterStartError::Create {
                    selections: selections.clone(),
                    batches: batches.clone(),
                    create: err
                })
            }
        }
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        if let StreamMulticasterStartError::Create {
            create: SelectionsError::Inner { inner: err },
            ..
        } = err
        {
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
                    match self.rev_map[i]
                        .stream
                        .cancel_batch(ctx, flags, &batch_id)
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
        } else {
            RetryResult::Success(())
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
}

impl<Idx, F> Frags for StreamMulticasterFrags<Idx, F>
where
    Idx: Clone + Debug + Display + From<usize> + Into<usize>,
    F: Frags
{
    type Param = Vec<F::Param>;
    type RecvReqError = ErrorSet<Idx, (), F::RecvReqError>;

    #[inline]
    fn param(_retry: Retry) -> Self::Param {
        vec![]
    }

    #[inline]
    fn from_data(
        params: Vec<F::Param>,
        data: Vec<u8>
    ) -> Self {
        // XXX this requires cloning the data for each frags instance.
        let frags = params
            .into_iter()
            .map(|param| F::from_data(param, data.clone()))
            .collect();

        StreamMulticasterFrags {
            idx: PhantomData,
            frags: frags
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.frags.iter().all(|val| val.is_empty())
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.frags[0].nbytes()
    }

    fn recv_req(
        &mut self,
        req: &LargeObjFragReq
    ) -> Result<(), Self::RecvReqError> {
        let len = self.frags.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, F::RecvReqError)>> = None;

        // Go through each sub-stream and try to receive.
        for (i, frags) in self.frags.iter_mut().enumerate() {
            match frags.recv_req(req) {
                Ok(()) => results.push((Idx::from(i), ())),
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

        match errs {
            // There were errors.
            Some(errs) => Err(ErrorSet::create(results, errs)),
            // No errors, check for retries.
            None => Ok(())
        }
    }
}

impl<Party, Idx, Stream, Ctx> LargeObjStream<Ctx>
    for StreamMulticaster<
        Party,
        Idx,
        Stream,
        <<Stream as LargeObjStream<Ctx>>::Frags as Frags>::Param,
        Ctx
    >
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: LargeObjStream<Ctx, Parties = ()> + PushStream<Ctx>
{
    // ISSUE #27: This requires a separate copy of the data for each party.
    type Frags = StreamMulticasterFrags<Idx, Stream::Frags>;
    type Parties = Vec<Idx>;
    type PushFragError = ErrorSet<
        Idx,
        RetryIndefResult<Option<Instant>, Stream::PushFragRetry>,
        Stream::PushFragError
    >;
    type PushFragRetry = MulticastRetry<
        Idx,
        RetryResult<Option<Instant>, Stream::PushFragRetry>
    >;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushFragRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushFragError
    > {
        let len = self.rev_map.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::PushFragError)>> = None;

        // Go through each sub-stream and try to push the fragment.
        for (i, frag) in frags.frags.iter_mut().enumerate() {
            match self.rev_map[i].stream.push_frags(ctx, id.clone(), frag) {
                // We're good; add this to the output.
                Ok(res) => {
                    let res = res.map(|(res, _)| res).map_indef(|_| ());

                    results.push((Idx::from(i), res))
                }
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

        self.decide_push_frag_result(results, errs)
    }

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retries: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushFragRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushFragError
    > {
        // Decompose the error set into successes and retries.
        let mut results = Vec::with_capacity(self.rev_map.len());
        let mut errs: Option<Vec<(Idx, Stream::PushFragError)>> = None;
        let MulticastRetry { retries, indefs } = retries;
        let len = retries.len();
        let mut skip = bitvec![0; self.rev_map.len()];
        let mut offset = 0;

        for idx in indefs.iter() {
            let i: usize = idx.clone().into();

            skip.set(i, true);
        }

        // Go through the retries and try to create the batch.
        for (i, res) in retries.into_iter().enumerate() {
            while skip[i + offset] {
                offset += 1;
            }
            let i = i + offset;

            match res {
                // Actually do retries.
                RetryResult::Retry(retry) => {
                    match self.rev_map[i].stream.retry_push_frags(
                        ctx,
                        id.clone(),
                        &mut frags.frags[i],
                        retry
                    ) {
                        // We're good; add this to the output.
                        Ok(res) => {
                            let res = res.map(|(res, _)| res).map_indef(|_| ());

                            results.push((Idx::from(i), res))
                        }
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
                // Retain prior successes.
                RetryResult::Success(val) => {
                    let idx = Idx::from(i);

                    results.push((idx, RetryIndefResult::Success(val)))
                }
            }
        }

        self.decide_push_frag_result(results, errs)
    }

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retries: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushFragRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushFragError
    > {
        let (mut results, retries) = retries.take();
        let mut errs: Option<Vec<(Idx, Stream::PushFragError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (idx, err) in retries {
            let i: usize = idx.into();

            match self.rev_map[i].stream.complete_push_frags(
                ctx,
                id.clone(),
                &mut frags.frags[i],
                err
            ) {
                // We're good; add this to the output.
                Ok(res) => {
                    let res = res.map(|(res, _)| res).map_indef(|_| ());

                    results.push((Idx::from(i), res))
                }
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

        self.decide_push_frag_result(results, errs)
    }
}

impl<Party, Idx, H, Stream, Ctx> LargeObjOfferStream<H, Ctx>
    for StreamMulticaster<
        Party,
        Idx,
        Stream,
        <<Stream as LargeObjStream<Ctx>>::Frags as Frags>::Param,
        Ctx
    >
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: LargeObjOfferStream<H, Ctx, Parties = ()> + PushStream<Ctx>,
    H: Clone + HashID
{
    // ISSUE #27: This requires a separate copy of the data for each party.
    type PushOfferError = ErrorSet<
        Idx,
        RetryIndefResult<Option<Instant>, Stream::PushOfferRetry>,
        Stream::PushOfferError
    >;
    type PushOfferRetry = MulticastRetry<
        Idx,
        RetryResult<Option<Instant>, Stream::PushOfferRetry>
    >;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushOfferRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushOfferError
    > {
        let len = self.rev_map.len();
        let mut results = Vec::with_capacity(len);
        let mut errs: Option<Vec<(Idx, Stream::PushOfferError)>> = None;

        // Go through each sub-stream and try to push the fragment.
        for (i, frag) in frags.frags.iter_mut().enumerate() {
            match self.rev_map[i].stream.push_offer(ctx, hash.clone(), frag) {
                // We're good; add this to the output.
                Ok(res) => {
                    let res = res.map(|(res, _)| res).map_indef(|_| ());

                    results.push((Idx::from(i), res))
                }
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

        self.decide_push_offer_result(results, errs)
    }

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retries: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushOfferRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushOfferError
    > {
        // Decompose the error set into successes and retries.
        let mut results = Vec::with_capacity(self.rev_map.len());
        let mut errs: Option<Vec<(Idx, Stream::PushOfferError)>> = None;
        let MulticastRetry { retries, indefs } = retries;
        let len = retries.len();
        let mut skip = bitvec![0; self.rev_map.len()];
        let mut offset = 0;

        for idx in indefs.iter() {
            let i: usize = idx.clone().into();

            skip.set(i, true);
        }

        // Go through the retries and try to create the batch.
        for (i, res) in retries.into_iter().enumerate() {
            while skip[i + offset] {
                offset += 1;
            }
            let i = i + offset;

            match res {
                // Actually do retries.
                RetryResult::Retry(retry) => {
                    match self.rev_map[i].stream.retry_push_offer(
                        ctx,
                        hash.clone(),
                        &mut frags.frags[i],
                        retry
                    ) {
                        // We're good; add this to the output.
                        // We're good; add this to the output.
                        Ok(res) => {
                            let res = res.map(|(res, _)| res).map_indef(|_| ());

                            results.push((Idx::from(i), res))
                        }
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
                // Retain prior successes.
                RetryResult::Success(val) => {
                    let idx = Idx::from(i);

                    results.push((idx, RetryIndefResult::Success(val)))
                }
            }
        }

        self.decide_push_offer_result(results, errs)
    }

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retries: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Vec<Idx>),
            Self::PushOfferRetry,
            Parties<Vec<Idx>>
        >,
        Self::PushOfferError
    > {
        let (mut results, retries) = retries.take();
        let mut errs: Option<Vec<(Idx, Stream::PushOfferError)>> = None;
        let len = retries.len();

        // Go through the retries and try to create the batch.
        for (idx, err) in retries {
            let i: usize = idx.into();

            match self.rev_map[i].stream.complete_push_offer(
                ctx,
                hash.clone(),
                &mut frags.frags[i],
                err
            ) {
                // We're good; add this to the output.
                Ok(res) => {
                    let res = res.map(|(res, _)| res).map_indef(|_| ());

                    results.push((Idx::from(i), res))
                }
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

        self.decide_push_offer_result(results, errs)
    }
}

impl<Party, Idx, Msg, Stream, Frags, Ctx> PushStreamSharedSingle<Msg, Ctx>
    for StreamMulticaster<Party, Idx, Stream, Frags, Ctx>
where
    Idx: Clone + Debug + Display + Eq + Hash + From<usize> + Into<usize> + Ord,
    Party: Clone + Debug + Display + Eq + Hash,
    Stream: PushStreamAdd<Msg, Ctx> + PushStreamPrivate<Ctx>,
    Stream::StreamFlags: Clone,
    Stream::BatchID: Clone + Debug
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
        Self::AddError,
        Self::FinishBatchError,
        Self::BatchID
    >;
    type PushRetry = StreamMulticasterPushError<
        Self::StartBatchRetry,
        Self::AddRetry,
        Self::FinishBatchRetry,
        Self::BatchID
    >;

    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &Msg
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
        self.start_batch(ctx, parties)
            .map_err(|err| StreamMulticasterPushError::Start { start: err })?
            .map_retry(|retry| StreamMulticasterPushError::Start {
                start: retry
            })
            .flat_map_ok(|batch| {
                // Add the message.
                let mut flags = self.empty_flags();

                self.add(ctx, &mut flags, msg, &batch)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: retry
                    })
                    .flat_map_ok(|()| {
                        // Finish the batch.
                        let mut flags = self.empty_flags();

                        Ok(self
                            .finish_batch(ctx, &mut flags, &batch)
                            .map_err(|err| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: err
                                }
                            })?
                            .map_retry(|retry| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: retry
                                }
                            })
                            .map(|()| batch))
                    })
                    .map(RetryIndefResult::from)
            })
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    > {
        match retry {
            StreamMulticasterPushError::Start { start: retry } => self
                .retry_start_batch(ctx, retry)
                .map_err(|err| StreamMulticasterPushError::Start {
                    start: err
                })?
                .map_retry(|retry| StreamMulticasterPushError::Start {
                    start: retry
                })
                .flat_map_ok(|batch| {
                    // Add the message.
                    let mut flags = self.empty_flags();

                    self.add(ctx, &mut flags, msg, &batch)
                        .map_err(|err| StreamMulticasterPushError::Add {
                            batch: batch,
                            add: err
                        })?
                        .map_retry(|retry| StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        })
                        .flat_map_ok(|()| {
                            // Finish the batch.
                            let mut flags = self.empty_flags();

                            Ok(self
                                .finish_batch(ctx, &mut flags, &batch)
                                .map_err(|err| {
                                    StreamMulticasterPushError::Finish {
                                        batch: batch,
                                        finish: err
                                    }
                                })?
                                .map_retry(|retry| {
                                    StreamMulticasterPushError::Finish {
                                        batch: batch,
                                        finish: retry
                                    }
                                })
                                .map(|()| batch))
                        })
                        .map(RetryIndefResult::from)
                }),
            StreamMulticasterPushError::Add { batch, add: retry } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                self.retry_add(ctx, &mut flags, msg, &batch, retry)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: retry
                    })
                    .flat_map_ok(|()| {
                        // Finish the batch.
                        let mut flags = self.empty_flags();

                        Ok(self
                            .finish_batch(ctx, &mut flags, &batch)
                            .map_err(|err| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: err
                                }
                            })?
                            .map_retry(|retry| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: retry
                                }
                            })
                            .map(|()| batch))
                    })
                    .map(RetryIndefResult::from)
            }
            StreamMulticasterPushError::Finish {
                batch,
                finish: retry
            } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .retry_finish_batch(ctx, &mut flags, &batch, retry)
                    .map(RetryIndefResult::from)
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
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::PushRetry,
            Parties<Self::IndefParties>
        >,
        Self::PushError
    > {
        match err {
            StreamMulticasterPushError::Start { start: err } => self
                .complete_start_batch(ctx, err)
                .map_err(|err| StreamMulticasterPushError::Start {
                    start: err
                })?
                .map_retry(|retry| StreamMulticasterPushError::Start {
                    start: retry
                })
                .flat_map_ok(|batch| {
                    // Add the message.
                    let mut flags = self.empty_flags();

                    self.add(ctx, &mut flags, msg, &batch)
                        .map_err(|err| StreamMulticasterPushError::Add {
                            batch: batch,
                            add: err
                        })?
                        .map_retry(|retry| StreamMulticasterPushError::Add {
                            batch: batch,
                            add: retry
                        })
                        .flat_map_ok(|()| {
                            // Finish the batch.
                            let mut flags = self.empty_flags();

                            Ok(self
                                .finish_batch(ctx, &mut flags, &batch)
                                .map_err(|err| {
                                    StreamMulticasterPushError::Finish {
                                        batch: batch,
                                        finish: err
                                    }
                                })?
                                .map_retry(|retry| {
                                    StreamMulticasterPushError::Finish {
                                        batch: batch,
                                        finish: retry
                                    }
                                })
                                .map(|()| batch))
                        })
                        .map(RetryIndefResult::from)
                }),
            StreamMulticasterPushError::Add { batch, add: err } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                self.complete_add(ctx, &mut flags, msg, &batch, err)
                    .map_err(|err| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: err
                    })?
                    .map_retry(|retry| StreamMulticasterPushError::Add {
                        batch: batch,
                        add: retry
                    })
                    .flat_map_ok(|()| {
                        // Finish the batch.
                        let mut flags = self.empty_flags();

                        Ok(self
                            .finish_batch(ctx, &mut flags, &batch)
                            .map_err(|err| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: err
                                }
                            })?
                            .map_retry(|retry| {
                                StreamMulticasterPushError::Finish {
                                    batch: batch,
                                    finish: retry
                                }
                            })
                            .map(|()| batch))
                    })
                    .map(RetryIndefResult::from)
            }
            StreamMulticasterPushError::Finish { batch, finish: err } => {
                // Finish the batch.
                let mut flags = self.empty_flags();

                Ok(self
                    .complete_finish_batch(ctx, &mut flags, &batch, err)
                    .map(RetryIndefResult::from)
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
        err: <Self::PushError as RecoverableError>::Permanent
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
        err: <Self::CancelPushError as RecoverableError>::Completable
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

impl<BatchID> From<Vec<Option<BatchID>>> for StreamMulticasterBatch<BatchID> {
    #[inline]
    fn from(val: Vec<Option<BatchID>>) -> StreamMulticasterBatch<BatchID> {
        StreamMulticasterBatch { batch_ids: val }
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

impl<Stream, Refresh> Display for StreamMulticasterCreateError<Stream, Refresh>
where
    Stream: Display,
    Refresh: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterCreateError::Stream { err } => err.fmt(f),
            StreamMulticasterCreateError::Refresh { err } => err.fmt(f)
        }
    }
}

impl Display for StreamMulticasterBatchPartiesError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterBatchPartiesError::NotFound { batch_id } => {
                write!(f, "batch {} not found", batch_id)
            }
        }
    }
}

impl<Start, Cancel, Flags, BatchID> Debug
    for StreamMulticasterCancelPushRetry<Start, Cancel, Flags, BatchID>
where
    Start: Debug,
    Cancel: Debug,
    BatchID: Debug
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterCancelPushRetry::Start { start, .. } => {
                write!(f, "Start {{ start: {:?}, }}", start)
            }
            StreamMulticasterCancelPushRetry::Cancel {
                cancel,
                batch_id,
                ..
            } => write!(
                f,
                "Cancel {{ cancel: {:?}, batch_id: {:?} }}",
                cancel, batch_id
            )
        }
    }
}

impl<Cancel, Flags, BatchID> Debug
    for StreamMulticasterCancelPushError<Cancel, Flags, BatchID>
where
    Cancel: Debug
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

impl<Select, Create, Selections, Batches> Debug
    for StreamMulticasterStartError<Select, Create, Selections, Batches>
where
    Select: Debug,
    Create: Debug
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterStartError::Select { select, .. } => select.fmt(f),
            StreamMulticasterStartError::Create { create, .. } => create.fmt(f)
        }
    }
}

impl<Select, Create, Selections, Batches> Display
    for StreamMulticasterStartError<Select, Create, Selections, Batches>
where
    Select: Display,
    Create: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterStartError::Select { select, .. } => select.fmt(f),
            StreamMulticasterStartError::Create { create, .. } => create.fmt(f)
        }
    }
}

impl<Select, Create> Display
    for StreamMulticasterStartReportError<Select, Create>
where
    Select: Display,
    Create: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterStartReportError::Select { select } => {
                select.fmt(f)
            }
            StreamMulticasterStartReportError::Create { create } => {
                create.fmt(f)
            }
        }
    }
}

impl<Start, Add, Finish, BatchID> Display
    for StreamMulticasterPushError<Start, Add, Finish, BatchID>
where
    Start: Display,
    Add: Display,
    Finish: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterPushError::Start { start } => start.fmt(f),
            StreamMulticasterPushError::Add { add, .. } => add.fmt(f),
            StreamMulticasterPushError::Finish { finish, .. } => finish.fmt(f)
        }
    }
}

impl<Start, Add, Finish> Display
    for StreamMulticasterPushReportError<Start, Add, Finish>
where
    Start: Display,
    Add: Display,
    Finish: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamMulticasterPushReportError::Start { start } => start.fmt(f),
            StreamMulticasterPushReportError::Add { add } => add.fmt(f),
            StreamMulticasterPushReportError::Finish { finish } => finish.fmt(f)
        }
    }
}
