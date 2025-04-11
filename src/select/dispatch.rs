// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;
use constellation_common::sched::DenseItemID;
use constellation_common::sched::EpochChange;
use constellation_common::sched::PassthruPolicy;
use constellation_common::sched::RefreshError;
use constellation_common::sched::ReportError;
use constellation_common::sched::Scheduler;
use constellation_common::sched::SelectError;
use log::debug;
use log::error;
use log::trace;

use crate::config::DispatchConfig;
use crate::config::FarSchedulerConfig;
use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::large_obj::LargeObjID;
use crate::select::sched::FarHistory;
use crate::select::sched::FarHistoryConfig;
use crate::select::PartiesBatchError;
use crate::select::SelectionsError;
use crate::select::SelectorBatchError;
use crate::select::SelectorBatchSelectError;
use crate::select::SelectorReportFailureError;
use crate::select::SelectorSelections;
use crate::select::SelectorStartRetry;
use crate::select::SelectorStreamError;
use crate::select::StreamSelectorBatch;
use crate::select::StreamSelectorReportError;
use crate::select::StreamsIdx;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamReporter;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::StreamReporter;

pub struct DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Default,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send {
    /// Mutable state.
    state: Arc<RwLock<DispatchSelectorState<Epochs, StreamID, Stream, Ctx>>>,
    /// When to next refresh the set of possible streams.
    refresh_when: Arc<RwLock<Option<Instant>>>,
    reporter: Reporter
}

pub struct DispatchSelectorReporter<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Default,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send {
    state: Arc<RwLock<DispatchSelectorState<Epochs, StreamID, Stream, Ctx>>>,
    reporter: Reporter
}

struct DispatchSelectorState<Epochs, StreamID, Stream, Ctx>
where
    Epochs: Iterator,
    StreamID: Clone + Display + Eq + Hash,
    Stream: Clone + PushStream<Ctx> + Send {
    ctx: PhantomData<Ctx>,
    /// Scheduler to use for selecting a raw stream.
    sched: Scheduler<Epochs, FarHistory, PassthruPolicy<StreamID>, ()>,
    /// A mapping from the endpoint address, channel, and parameter
    /// set to dense IDs for this epoch.
    ///
    /// This is regenerated at the start of each epoch.
    stream_ids: HashMap<StreamID, StreamsIdx>,
    /// The current set of possible streams, and any active stream objects.
    ///
    /// This is regenerated at the start of each epoch.
    streams: Vec<StreamEntry<StreamID, Stream>>
}

/// Entry in the streams array, representing a possible stream.
struct StreamEntry<StreamID, Stream> {
    /// Identifier of the stream.
    id: StreamID,
    /// Stream, if it has been created.
    stream: Stream
}

/// Errors that can occur when selecting a stream on a [StreamSelector].
#[derive(Debug)]
pub enum DispatchSelectorSelectError<Item> {
    /// Error occurred while selecting the stream.
    Select {
        /// Error while selecting the stream.
        err: SelectError
    },
    /// Error occurred while reporting failure.
    Report {
        /// Error while reporting failure.
        err: ReportError<Item>
    },
    /// Mutex poisoned.
    MutexPoison
}

/// Errors that can occur when reporting a success or failure.
#[derive(Debug)]
pub enum DispatchSelectorReportError<Report, StreamID> {
    /// Error occurred reporting the stream.
    Report { err: Report },
    /// Error occurred refreshing the scheduler.
    Refresh {
        err: DispatchSelectorRefreshError<StreamID>
    },
    /// Mutex poisoned.
    MutexPoison
}

#[derive(Debug)]
pub enum DispatchSelectorRefreshError<StreamID> {
    /// Error occurred refreshing the scheduler.
    Refresh { err: RefreshError },
    /// Index assignment skipped an index.
    BadID { id: StreamID }
}

impl<StreamID> ScopedError for DispatchSelectorRefreshError<StreamID> {
    fn scope(&self) -> ErrorScope {
        match self {
            DispatchSelectorRefreshError::Refresh { err } => err.scope(),
            DispatchSelectorRefreshError::BadID { .. } => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<Report, StreamID> ScopedError
    for DispatchSelectorReportError<Report, StreamID>
where
    Report: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            DispatchSelectorReportError::Report { err } => err.scope(),
            DispatchSelectorReportError::Refresh { err } => err.scope(),
            DispatchSelectorReportError::MutexPoison => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<Item> ScopedError for DispatchSelectorSelectError<Item> {
    fn scope(&self) -> ErrorScope {
        match self {
            DispatchSelectorSelectError::Select { err } => err.scope(),
            DispatchSelectorSelectError::Report { err } => err.scope(),
            DispatchSelectorSelectError::MutexPoison => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<Item> BatchError for DispatchSelectorSelectError<Item>
where
    Item: Display
{
    type Completable = Infallible;
    type Permanent = Self;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<Epochs, StreamID, Stream, Ctx>
    DispatchSelectorState<Epochs, StreamID, Stream, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Stream: Clone + PushStream<Ctx> + Send
{
    fn create(
        config: FarSchedulerConfig,
        retry: Retry,
        epochs: Epochs
    ) -> Result<Self, RefreshError> {
        let config = FarHistoryConfig::from(&config);
        let sched =
            Scheduler::new(config, retry, PassthruPolicy::default(), epochs)?;

        Ok(DispatchSelectorState {
            ctx: PhantomData,
            sched: sched,
            stream_ids: HashMap::new(),
            streams: Vec::new()
        })
    }

    fn with_capacity(
        config: FarSchedulerConfig,
        retry: Retry,
        epochs: Epochs,
        size: usize
    ) -> Result<Self, RefreshError> {
        let config = FarHistoryConfig::from(&config);
        let sched =
            Scheduler::new(config, retry, PassthruPolicy::default(), epochs)?;

        Ok(DispatchSelectorState {
            ctx: PhantomData,
            sched: sched,
            stream_ids: HashMap::with_capacity(size),
            streams: Vec::new()
        })
    }

    #[inline]
    fn epoch(&self) -> Epochs::Item {
        self.sched.epoch().clone()
    }

    fn add_stream(
        &mut self,
        stream_id: StreamID,
        stream: Stream
    ) -> Result<(), DispatchSelectorRefreshError<StreamID>> {
        self.streams.push(StreamEntry {
            id: stream_id,
            stream: stream
        });

        let ids = self.streams.iter().map(|entry| (entry.id.clone(), ()));
        let now = Instant::now();

        // Update the scheduler, possibly get a new epoch
        if let Some(epoch) = self
            .sched
            .refresh(now, ids)
            .map_err(|err| DispatchSelectorRefreshError::Refresh { err: err })?
        {
            self.epoch_change(epoch)?;
        }

        Ok(())
    }

    fn epoch_change(
        &mut self,
        epoch: EpochChange<Epochs::Item, StreamID, ()>
    ) -> Result<(), DispatchSelectorRefreshError<StreamID>> {
        let (_, dense_ids, _, _) = epoch.take();
        let mut old_streams: HashMap<StreamID, Stream> = self
            .streams
            .drain(..)
            .map(|StreamEntry { id, stream }| (id, stream))
            .collect();
        let mut new_streams = Vec::with_capacity(dense_ids.len());
        let mut new_stream_ids = HashMap::with_capacity(dense_ids.len());

        for (i, (id, ())) in dense_ids.into_iter().enumerate() {
            let stream = old_streams.remove(&id).ok_or(
                DispatchSelectorRefreshError::BadID { id: id.clone() }
            )?;

            new_streams.push(StreamEntry {
                stream: stream,
                id: id.clone()
            });
            new_stream_ids.insert(id, StreamsIdx::from(i));
        }

        self.streams = new_streams;
        self.stream_ids = new_stream_ids;

        Ok(())
    }

    /// Report a success for a given stream.
    fn success(
        &mut self,
        stream_id: StreamID
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.stream_ids.get(&stream_id) {
            Some(idx) => {
                let id = DenseItemID::new(self.sched.epoch().clone(), idx.0);

                self.success_id(&id).map_err(|err| {
                    StreamSelectorReportError::Report { err: err }
                })
            }
            None => Err(StreamSelectorReportError::NotFound)
        }
    }

    /// Report a success for the stream identified by `id`.
    #[inline]
    fn success_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<(), ReportError<StreamID>> {
        self.sched.success_id(id)
    }

    // XXX Need to clear out streams that have failed with an error
    // indicating the stream is no longer viable.

    /// Report a success for a given stream.
    fn failure(
        &mut self,
        stream_id: StreamID
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.stream_ids.get(&stream_id) {
            Some(idx) => {
                let id = DenseItemID::new(self.sched.epoch().clone(), idx.0);

                self.failure_id(&id).map_err(|err| {
                    StreamSelectorReportError::Report { err: err }
                })
            }
            None => Err(StreamSelectorReportError::NotFound)
        }
    }

    /// Report a failure for the stream identified by `id`.
    #[inline]
    pub fn failure_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<(), ReportError<StreamID>> {
        self.sched.failure_id(id)
    }

    fn do_select(
        &mut self
    ) -> Result<
        RetryResult<(Stream, DenseItemID<Epochs::Item>)>,
        DispatchSelectorSelectError<StreamID>
    > {
        Ok(self
            .sched
            .select()
            .map_err(|err| DispatchSelectorSelectError::Select { err: err })?
            .flat_map(move |(_, _, dense_id)| {
                let StreamEntry { stream, .. } = &self.streams[dense_id.idx()];

                RetryResult::Success((stream.clone(), dense_id))
            }))
    }

    fn report<Reporter>(
        &mut self,
        reporter: &mut Reporter,
        stream_id: StreamID,
        prin: <Reporter as StreamReporter>::Prin,
        stream: Stream
    ) -> Result<
        Option<Stream>,
        DispatchSelectorReportError<
            <Reporter as StreamReporter>::ReportError,
            StreamID
        >
    >
    where
        Reporter: StreamReporter<Src = StreamID, Stream = Stream> {
        match self.stream_ids.get(&stream_id) {
            Some(idx) => {
                let idx: usize = idx.into();

                trace!(target: "dispatch-selector",
                       "stream {} already existed",
                       stream_id);

                Ok(Some(self.streams[idx].stream.clone()))
            }
            None => {
                trace!(target: "dispatch-selector",
                       "reporting stream {} to inner reporter",
                       stream_id);

                match reporter
                    .report(stream_id.clone(), prin, stream.clone())
                    .map_err(|err| DispatchSelectorReportError::Report {
                    err: err
                })? {
                    Some(stream) => {
                        trace!(target: "dispatch-selector",
                               "inner reporter already had stream for {}",
                               stream_id);

                        self.add_stream(stream_id, stream.clone()).map_err(
                            |err| DispatchSelectorReportError::Refresh {
                                err: err
                            }
                        )?;

                        Ok(Some(stream))
                    }
                    None => {
                        trace!(target: "dispatch-selector",
                               "adding stream {}",
                               stream_id);

                        self.add_stream(stream_id, stream).map_err(|err| {
                            DispatchSelectorReportError::Refresh { err: err }
                        })?;

                        Ok(None)
                    }
                }
            }
        }
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Stream, SelectorStreamError<Epochs::Item>> {
        // Check that the epochs match.
        let curr_epoch = self.epoch();

        if batch.stream.epoch() == &curr_epoch {
            Ok(self.streams[batch.stream.idx()].stream.clone())
        } else {
            Err(SelectorStreamError::EpochMismatch {
                curr: curr_epoch,
                batch: batch.stream.epoch().clone()
            })
        }
    }

    fn dense_id_stream(
        &self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<Stream, SelectorStreamError<Epochs::Item>> {
        // Check that the epochs match.
        let curr_epoch = self.epoch();

        if id.epoch() == &curr_epoch {
            Ok(self.streams[id.idx()].stream.clone())
        } else {
            Err(SelectorStreamError::EpochMismatch {
                curr: curr_epoch,
                batch: id.epoch().clone()
            })
        }
    }

    fn cancel_batches(&mut self) {
        for stream in self
            .streams
            .iter_mut()
            .map(|StreamEntry { stream, .. }| stream)
        {
            stream.cancel_batches()
        }
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> Clone
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Default,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: Clone + StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    fn clone(&self) -> Self {
        DispatchSelector {
            refresh_when: self.refresh_when.clone(),
            reporter: self.reporter.clone(),
            state: self.state.clone()
        }
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> StreamReporter
    for DispatchSelectorReporter<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Default + Display + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    type Prin = <Reporter as StreamReporter>::Prin;
    type ReportError = DispatchSelectorReportError<
        <Reporter as StreamReporter>::ReportError,
        StreamID
    >;
    type Src = StreamID;
    type Stream = Stream;

    fn report(
        &mut self,
        src: StreamID,
        prin: Self::Prin,
        stream: Self::Stream
    ) -> Result<Option<Self::Stream>, Self::ReportError> {
        debug!(target: "dispatch-selector",
               "reporting stream for {}",
               src);

        match self.state.write() {
            Ok(mut guard) => {
                guard.report(&mut self.reporter, src, prin, stream)
            }
            Err(_) => Err(DispatchSelectorReportError::MutexPoison)
        }
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> PushStreamReporter
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Default + Display + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: Clone + StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    type Reporter =
        DispatchSelectorReporter<Epochs, StreamID, Stream, Reporter, Ctx>;

    #[inline]
    fn reporter(
        &self
    ) -> DispatchSelectorReporter<Epochs, StreamID, Stream, Reporter, Ctx> {
        DispatchSelectorReporter {
            reporter: self.reporter.clone(),
            state: self.state.clone()
        }
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx>
    DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Default + Display + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    /// Create a new [StreamSelector] from a configuration and other
    /// necessary objects.
    ///
    /// The `reporter` parameter is a [StreamReporter] instance that
    /// will be used to report *both* newly-created streams as well as
    /// incoming streams reported to *this* `StreamSelector` by a
    /// [StreamSelectorReporter].  (This is necessary to avoid deadlocks.)
    pub fn create(
        reporter: Reporter,
        config: DispatchConfig<Epochs::Config>
    ) -> Result<Self, RefreshError> {
        let (scheduler, epochs, retry, size_hint) = config.take();
        let epochs = Epochs::create(epochs);
        let state = match size_hint {
            Some(size) => DispatchSelectorState::with_capacity(
                scheduler, retry, epochs, size
            ),
            None => DispatchSelectorState::create(scheduler, retry, epochs)
        }?;
        let now = Instant::now();

        Ok(DispatchSelector {
            state: Arc::new(RwLock::new(state)),
            refresh_when: Arc::new(RwLock::new(Some(now))),
            reporter: reporter
        })
    }

    /// Report a success for a given stream.
    #[inline]
    pub fn success(
        &mut self,
        stream_id: StreamID
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.state.write() {
            Ok(mut guard) => guard.success(stream_id),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Report a success for the stream identified by `id`.
    #[inline]
    pub fn success_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.state.write() {
            Ok(mut guard) => guard
                .success_id(id)
                .map_err(|err| StreamSelectorReportError::Report { err: err }),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Report a success for a given stream.
    #[inline]
    pub fn failure(
        &mut self,
        stream_id: StreamID
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.state.write() {
            Ok(mut guard) => guard.failure(stream_id),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Report a failure for the stream identified by `id`.
    #[inline]
    pub fn failure_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<(), StreamSelectorReportError<ReportError<StreamID>>> {
        match self.state.write() {
            Ok(mut guard) => guard
                .failure_id(id)
                .map_err(|err| StreamSelectorReportError::Report { err: err }),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Select a stream from among the set of possible streams.
    ///
    /// This will first refresh the set of possible streams as per
    /// [refresh](StreamSelector::refresh).  Then, the scheduler
    /// will be used to select from among the possible streams.  Both
    /// the stream and its dense index will be returned.
    pub fn select_stream(
        &mut self
    ) -> Result<
        RetryResult<(Stream, DenseItemID<Epochs::Item>)>,
        DispatchSelectorSelectError<StreamID>
    > {
        // First try to refresh, if needed.
        match self.state.write() {
            Ok(mut guard) => guard.do_select(),
            Err(_) => Err(DispatchSelectorSelectError::MutexPoison)
        }
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Stream, SelectorStreamError<Epochs::Item>> {
        match self.state.read() {
            Ok(guard) => guard.batch_stream(batch),
            Err(_) => Err(SelectorStreamError::MutexPoison)
        }
    }

    fn dense_id_stream(
        &self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<Stream, SelectorStreamError<Epochs::Item>> {
        match self.state.read() {
            Ok(guard) => guard.dense_id_stream(id),
            Err(_) => Err(SelectorStreamError::MutexPoison)
        }
    }
}

// XXX Eliminate code duplication here

impl<Epochs, StreamID, Stream, Reporter, Ctx> PushStream<Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    type BatchID =
        StreamSelectorBatch<Epochs::Item, <Stream as PushStream<Ctx>>::BatchID>;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type CancelBatchRetry = <Stream as PushStream<Ctx>>::CancelBatchRetry;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type FinishBatchRetry = <Stream as PushStream<Ctx>>::FinishBatchRetry;
    type ReportError = SelectorReportFailureError<
        Epochs::Item,
        StreamID,
        <Stream as PushStream<Ctx>>::ReportError
    >;
    type StreamFlags = <Stream as PushStream<Ctx>>::StreamFlags;

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
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .finish_batch(ctx, flags, &batch.batch_id)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .retry_finish_batch(ctx, flags, &batch.batch_id, retry)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .complete_finish_batch(ctx, flags, &batch.batch_id, err)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .cancel_batch(ctx, flags, &batch.batch_id)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .retry_cancel_batch(ctx, flags, &batch.batch_id, retry)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .complete_cancel_batch(ctx, flags, &batch.batch_id, err)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    #[inline]
    fn cancel_batches(&mut self) {
        match self.state.write() {
            Ok(mut guard) => guard.cancel_batches(),
            Err(_) => {
                error!(target: "dispatch-selector",
                       "mutex poisoned in cancel_batches")
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        self.failure_id(&batch.stream)
            .map_err(|err| SelectorReportFailureError::Report { err: err })?;

        self.batch_stream(batch)
            .map_err(|err| SelectorReportFailureError::Stream { err: err })?
            .report_failure(&batch.batch_id)
            .map_err(|err| SelectorReportFailureError::Inner { err: err })
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx>
    PushStreamReportError<DenseItemID<Epochs::Item>>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    type ReportError = StreamSelectorReportError<ReportError<StreamID>>;

    fn report_error(
        &mut self,
        selected: &DenseItemID<Epochs::Item>
    ) -> Result<(), Self::ReportError> {
        trace!(target: "dispatch-selector",
               "reporting error to {}",
               selected);

        self.failure_id(selected)
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx, Error>
    PushStreamReportError<Error>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send,
    Error: ErrorReportInfo<DenseItemID<Epochs::Item>>
{
    type ReportError = StreamSelectorReportError<ReportError<StreamID>>;

    fn report_error(
        &mut self,
        error: &Error
    ) -> Result<(), Self::ReportError> {
        if let Some(selected) = error.report_info() {
            self.report_error(&selected)
        } else {
            Ok(())
        }
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx, Error>
    PushStreamReportBatchError<
        Error,
        StreamSelectorBatch<Epochs::Item, <Stream as PushStream<Ctx>>::BatchID>
    > for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + Send
{
    type ReportBatchError = StreamSelectorReportError<ReportError<StreamID>>;

    fn report_error_with_batch(
        &mut self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Stream as PushStream<Ctx>>::BatchID
        >,
        _error: &Error
    ) -> Result<(), Self::ReportBatchError> {
        self.report_error(&batch.stream)
    }
}

impl<Msg, Epochs, StreamID, Stream, Reporter, Ctx> PushStreamAdd<Msg, Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStreamAdd<Msg, Ctx> + Send
{
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Stream as PushStreamAdd<Msg, Ctx>>::AddError
    >;
    type AddRetry = <Stream as PushStreamAdd<Msg, Ctx>>::AddRetry;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .add(ctx, flags, msg, &batch.batch_id)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .retry_add(ctx, flags, msg, &batch.batch_id, retry)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        err: <Self::AddError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .complete_add(ctx, flags, msg, &batch.batch_id, err)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> PushStreamPartyID
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + PushStreamPartyID + Send
{
    type PartyID = <Stream as PushStreamPartyID>::PartyID;
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> PushStreamShared<Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + PushStreamShared<Ctx> + Send
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Stream as PushStreamShared<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry = <Stream as PushStreamShared<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            Vec<<Stream as PushStreamPartyID>::PartyID>,
            <Stream as PushStreamShared<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Stream as PushStreamPartyID>::PartyID>,
        <Stream as PushStreamShared<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Stream as PushStreamShared<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            Vec<<Stream as PushStreamPartyID>::PartyID>,
            <Stream as PushStreamShared<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Stream as PushStreamPartyID>::PartyID>,
        <Stream as PushStreamShared<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Stream as PushStreamShared<Ctx>>::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Stream::empty_batches_with_capacity(size)
    }

    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let parties: Vec<Self::PartyID> = parties.cloned().collect();

        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.clone(),
                    select: err
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => {
                selections.id = Some(id.clone());

                match stream
                    .select(ctx, &mut selections.inner, parties.iter())
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: id.clone(),
                            stream: err
                        }
                    })? {
                    // We succeeded.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We got a retry when selecting on the inner stream.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: id,
                            stream: retry
                        }
                    ))
                }
            }
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    parties: parties,
                    select: retry
                }))
            }
        }
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { parties, .. } => {
                self.select(ctx, selections, parties.iter())
            }
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream
                    .retry_select(ctx, &mut selections.inner, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        match err {
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            SelectorBatchSelectError::Select { .. } => {
                panic!("Impossible case!")
            }
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream
                    .complete_select(ctx, &mut selections.inner, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
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
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .create_batch(ctx, batches, &selections.inner)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
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
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .retry_create_batch(ctx, batches, &selections.inner, retry)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .complete_create_batch(ctx, batches, &selections.inner, err)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
    }

    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let parties: Vec<Self::PartyID> = parties.cloned().collect();

        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.clone(),
                    select: err
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => match stream
                .start_batch(ctx, parties.iter())
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })? {
                // We succeeded, so we can package it up and return it.
                RetryResult::Success(batch_id) => {
                    Ok(RetryResult::Success(StreamSelectorBatch {
                        stream: id,
                        batch_id: batch_id
                    }))
                }
                // We got a retry when creating the batch on the stream.
                RetryResult::Retry(retry) => {
                    Ok(RetryResult::Retry(SelectorBatchSelectError::Stream {
                        selected: id,
                        stream: retry
                    }))
                }
            },
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    parties: parties,
                    select: retry
                }))
            }
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { parties, .. } => {
                self.start_batch(ctx, parties.iter())
            }
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.retry_start_batch(ctx, retry).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match err {
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            SelectorBatchSelectError::Select { .. } => {
                panic!("Impossible case!")
            }
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.complete_start_batch(ctx, err).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    #[inline]
    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _err: <Self::StartBatchError as BatchError>::Permanent
    ) -> RetryResult<(), Infallible> {
        // We don't actually have to do anything here.  There's no
        // state prior to creating a batch on the underlying stream.

        RetryResult::Success(())
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _retry: Infallible
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        error!(target: "dispatch-selector",
               "should never call retry_abort_start_batch on this stream");

        RetryResult::Success(())
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> PushStreamPrivate<Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + PushStreamPrivate<Ctx> + Send
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Stream as PushStreamPrivate<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry =
        <Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as PushStreamPrivate<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as PushStreamPrivate<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Stream as PushStreamPrivate<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as PushStreamPrivate<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Stream::empty_batches_with_capacity(size)
    }

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: (),
                    select: err
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => {
                selections.id = Some(id.clone());

                match stream.select(ctx, &mut selections.inner).map_err(
                    |err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: id.clone(),
                            stream: err
                        }
                    }
                )? {
                    // We succeeded.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We got a retry when selecting on the inner stream.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: id,
                            stream: retry
                        }
                    ))
                }
            }
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    parties: (),
                    select: retry
                }))
            }
        }
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => {
                self.select(ctx, selections)
            }
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream
                    .retry_select(ctx, &mut selections.inner, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        match err {
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            SelectorBatchSelectError::Select { .. } => {
                panic!("Impossible case!")
            }
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream
                    .complete_select(ctx, &mut selections.inner, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(()) => Ok(RetryResult::Success(())),
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
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
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .create_batch(ctx, batches, &selections.inner)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
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
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .retry_create_batch(ctx, batches, &selections.inner, retry)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        let selected = match &selections.id {
            Some(selections) => Ok(selections),
            None => Err(SelectionsError::NoSelections { info: () })
        }?;
        let mut stream = self.dense_id_stream(selected).map_err(|err| {
            SelectionsError::Inner {
                inner: SelectorBatchError::Stream { err: err }
            }
        })?;

        Ok(stream
            .complete_create_batch(ctx, batches, &selections.inner, err)
            .map_err(|err| SelectionsError::Inner {
                inner: SelectorBatchError::Batch { batch: err }
            })?
            .map(|batch_id| StreamSelectorBatch {
                stream: selected.clone(),
                batch_id: batch_id
            }))
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: (),
                    select: err
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => match stream
                .start_batch(ctx)
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })? {
                // We succeeded, so we can package it up and return it.
                RetryResult::Success(batch_id) => {
                    Ok(RetryResult::Success(StreamSelectorBatch {
                        stream: id,
                        batch_id: batch_id
                    }))
                }
                // We got a retry when creating the batch on the stream.
                RetryResult::Retry(retry) => {
                    Ok(RetryResult::Retry(SelectorBatchSelectError::Stream {
                        selected: id,
                        stream: retry
                    }))
                }
            },
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    parties: (),
                    select: retry
                }))
            }
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => self.start_batch(ctx),
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.retry_start_batch(ctx, retry).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match err {
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            SelectorBatchSelectError::Select { .. } => {
                panic!("Impossible case!")
            }
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.complete_start_batch(ctx, err).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    #[inline]
    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _err: <Self::StartBatchError as BatchError>::Permanent
    ) -> RetryResult<(), Infallible> {
        // We don't actually have to do anything here.  There's no
        // state prior to creating a batch on the underlying stream.

        RetryResult::Success(())
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _retry: Infallible
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        error!(target: "dispatch-selector",
               "should never call retry_abort_start_batch on this stream");

        RetryResult::Success(())
    }
}

impl<Epochs, StreamID, Stream, Reporter, Ctx> LargeObjStream<Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + LargeObjStream<Ctx> + Send
{
    type Frags = <Stream as LargeObjStream<Ctx>>::Frags;
    type PushFragError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as LargeObjStream<Ctx>>::PushFragError,
            Epochs::Item
        >
    >;
    type PushFragRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as LargeObjStream<Ctx>>::PushFragRetry,
        Epochs::Item
    >;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushFragRetry>,
        Self::PushFragError
    > {
        // Try to select a stream.
        self.select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: err,
                    parties: ()
                }
            })?
            .map_retry(|retry| SelectorBatchSelectError::Select {
                select: retry,
                parties: ()
            })
            .flat_map_ok(|(mut stream, selected)| {
                Ok(stream
                    .push_frags(ctx, id, frags)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            })
    }

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushFragRetry>,
        Self::PushFragError
    > {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => {
                self.push_frags(ctx, id, frags)
            }
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .retry_push_frags(ctx, id, frags, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as BatchError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushFragRetry>,
        Self::PushFragError
    > {
        match err {
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .complete_push_frags(ctx, id, frags, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }
}

impl<H, Epochs, StreamID, Stream, Reporter, Ctx> LargeObjOfferStream<H, Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    H: HashID,
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + LargeObjOfferStream<H, Ctx> + Send
{
    type PushOfferError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as LargeObjOfferStream<H, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type PushOfferRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as LargeObjOfferStream<H, Ctx>>::PushOfferRetry,
        Epochs::Item
    >;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushOfferRetry>,
        Self::PushOfferError
    > {
        // Try to select a stream.
        self.select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: err,
                    parties: ()
                }
            })?
            .map_retry(|retry| SelectorBatchSelectError::Select {
                select: retry,
                parties: ()
            })
            .flat_map_ok(|(mut stream, selected)| {
                Ok(stream
                    .push_offer(ctx, hash, frags)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            })
    }

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushOfferRetry>,
        Self::PushOfferError
    > {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => {
                self.push_offer(ctx, hash, frags)
            }
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .retry_push_offer(ctx, hash, frags, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as BatchError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::PushOfferRetry>,
        Self::PushOfferError
    > {
        match err {
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .complete_push_offer(ctx, hash, frags, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }
}

impl<Msg, Epochs, StreamID, Stream, Reporter, Ctx>
    PushStreamPrivateSingle<Msg, Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + PushStreamPrivateSingle<Msg, Ctx> + Send
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<StreamID>,
            (),
            <Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushRetry,
        Epochs::Item
    >;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: err,
                    parties: ()
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => match stream
                .push(ctx, msg)
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })? {
                // We succeeded, so we can package it up and return it.
                RetryResult::Success(batch_id) => {
                    Ok(RetryResult::Success(StreamSelectorBatch {
                        stream: id,
                        batch_id: batch_id
                    }))
                }
                // We got a retry when creating the batch on the stream.
                RetryResult::Retry(retry) => {
                    Ok(RetryResult::Retry(SelectorBatchSelectError::Stream {
                        selected: id,
                        stream: retry
                    }))
                }
            },
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    select: retry,
                    parties: ()
                }))
            }
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => self.push(ctx, msg),
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.retry_push(ctx, msg, retry).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
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
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.complete_push(ctx, msg, err).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            // The stream couldn't be obtained, so we can't cancel.
            SelectorBatchError::Stream { .. } => Ok(RetryResult::Success(())),
            // The batch was never created.
            SelectorBatchError::Batch { batch } => match batch {
                // The batch was never created.
                SelectorBatchSelectError::Select { .. } => {
                    Ok(RetryResult::Success(()))
                }
                // This is the one case where we need to cancel.
                SelectorBatchSelectError::Stream {
                    selected,
                    stream: err
                } => {
                    let mut stream =
                        self.dense_id_stream(&selected).map_err(|err| {
                            SelectorBatchError::Stream { err: err }
                        })?;

                    Ok(stream
                        .cancel_push(ctx, err)
                        .map_err(|err| SelectorBatchError::Batch {
                            batch: SelectorBatchSelectError::Stream {
                                selected: selected.clone(),
                                stream: err
                            }
                        })?
                        .map_retry(|retry| SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }))
                }
            }
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match retry {
            // The batch was never created.
            SelectorBatchSelectError::Select { .. } => {
                Ok(RetryResult::Success(()))
            }
            // This is the one case where we need to cancel.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .retry_cancel_push(ctx, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            SelectorBatchSelectError::Select { .. } => {
                panic!("Impossible case!")
            }
            // This is the one case where we need to cancel.
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .complete_cancel_push(ctx, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }
}

impl<Msg, Epochs, StreamID, Stream, Reporter, Ctx>
    PushStreamSharedSingle<Msg, Ctx>
    for DispatchSelector<Epochs, StreamID, Stream, Reporter, Ctx>
where
    Epochs: IDGen + Iterator,
    Epochs::Item: Clone + Display + Default + Eq,
    StreamID: Clone + Display + Eq + Hash,
    Reporter: StreamReporter<Src = StreamID, Stream = Stream>,
    Stream: Clone + PushStream<Ctx> + PushStreamSharedSingle<Msg, Ctx> + Send
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                DispatchSelectorSelectError<StreamID>
            >,
            (),
            <Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                DispatchSelectorSelectError<StreamID>
            >,
            (),
            <Stream as PushStreamSharedSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Stream as PushStreamSharedSingle<Msg, Ctx>>::PushRetry,
        Epochs::Item
    >;

    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        // ISSUE #13: find a better way to do this that doesn't
        // involve creating arrays
        let parties: Vec<Self::PartyID> = parties.cloned().collect();

        // Try to select a stream.
        match self
            .select_stream()
            .map_err(|err| SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: PartiesBatchError::new(parties.clone(), err),
                    parties: ()
                }
            })? {
            // We succeeded, now create a batch on that stream.
            RetryResult::Success((mut stream, id)) => match stream
                .push(ctx, parties.iter(), msg)
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })? {
                // We succeeded, so we can package it up and return it.
                RetryResult::Success(batch_id) => {
                    Ok(RetryResult::Success(StreamSelectorBatch {
                        stream: id,
                        batch_id: batch_id
                    }))
                }
                // We got a retry when creating the batch on the stream.
                RetryResult::Retry(retry) => {
                    Ok(RetryResult::Retry(SelectorBatchSelectError::Stream {
                        selected: id,
                        stream: retry
                    }))
                }
            },
            // We got a retry for selecting the stream.
            RetryResult::Retry(retry) => {
                Ok(RetryResult::Retry(SelectorBatchSelectError::Select {
                    select: SelectorStartRetry {
                        when: retry,
                        parties: parties
                    },
                    parties: ()
                }))
            }
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select {
                select: SelectorStartRetry { parties, .. },
                ..
            } => self.push(ctx, parties.iter(), msg),
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.retry_push(ctx, msg, retry).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
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
            SelectorBatchSelectError::Select { select, .. } => {
                error!(target: "dispatch-selector",
                       concat!("should never call complete_push ",
                               "with select error"));

                self.push(ctx, select.take_parties().iter(), msg)
            }
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.complete_push(ctx, msg, err).map_err(|err| {
                    SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    }
                })? {
                    // We created the batch, wrap it up and return it.
                    RetryResult::Success(batch_id) => {
                        Ok(RetryResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryResult::Retry(retry) => Ok(RetryResult::Retry(
                        SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }
                    ))
                }
            }
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            // The stream couldn't be obtained, so we can't cancel.
            SelectorBatchError::Stream { .. } => Ok(RetryResult::Success(())),
            // The batch was never created.
            SelectorBatchError::Batch { batch } => match batch {
                // The batch was never created.
                SelectorBatchSelectError::Select { .. } => {
                    Ok(RetryResult::Success(()))
                }
                // This is the one case where we need to cancel.
                SelectorBatchSelectError::Stream {
                    selected,
                    stream: err
                } => {
                    let mut stream =
                        self.dense_id_stream(&selected).map_err(|err| {
                            SelectorBatchError::Stream { err: err }
                        })?;

                    Ok(stream
                        .cancel_push(ctx, err)
                        .map_err(|err| SelectorBatchError::Batch {
                            batch: SelectorBatchSelectError::Stream {
                                selected: selected.clone(),
                                stream: err
                            }
                        })?
                        .map_retry(|retry| SelectorBatchSelectError::Stream {
                            selected: selected,
                            stream: retry
                        }))
                }
            }
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match retry {
            // The batch was never created.
            SelectorBatchSelectError::Select { .. } => {
                Ok(RetryResult::Success(()))
            }
            // This is the one case where we need to cancel.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .retry_cancel_push(ctx, retry)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match err {
            // The batch was never created.
            SelectorBatchSelectError::Select { .. } => {
                Ok(RetryResult::Success(()))
            }
            // This is the one case where we need to cancel.
            SelectorBatchSelectError::Stream {
                selected,
                stream: err
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                Ok(stream
                    .complete_cancel_push(ctx, err)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: selected.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: selected,
                        stream: retry
                    }))
            }
        }
    }
}

impl<Item> Display for DispatchSelectorSelectError<Item>
where
    Item: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            DispatchSelectorSelectError::Select { err } => err.fmt(f),
            DispatchSelectorSelectError::Report { err } => err.fmt(f),
            DispatchSelectorSelectError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<Report, StreamID> Display for DispatchSelectorReportError<Report, StreamID>
where
    Report: Display,
    StreamID: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            DispatchSelectorReportError::Report { err } => err.fmt(f),
            DispatchSelectorReportError::Refresh { err } => err.fmt(f),
            DispatchSelectorReportError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<StreamID> Display for DispatchSelectorRefreshError<StreamID>
where
    StreamID: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            DispatchSelectorRefreshError::Refresh { err } => err.fmt(f),
            DispatchSelectorRefreshError::BadID { id } => {
                write!(f, "bad ID {} in new assignment", id)
            }
        }
    }
}
