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

//! Adaptive ranking and selection from a set of streams.
//!
//! This module implements [StreamSelector], a scheduling-based
//! mechanism for selecting from a set of stream options.  The set of
//! options is obtained from a set of combinations of [Addrs] and
//! [Channels] instances.  This is then ranked by a [Scheduler]
//! instance, which selects from one of the options on demand.
//!
//! The instances for [PushStream] and its child traits use this to
//! allow the entire set of stream options to be treated as if they
//! were a single stream.
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::net::IPEndpointAddr;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
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
use log::warn;

use crate::addrs::Addrs;
use crate::addrs::AddrsCreate;
use crate::channels::ChannelParam;
use crate::channels::Channels;
use crate::channels::ChannelsCreate;
use crate::config::ConnectionConfig;
use crate::config::FarSchedulerConfig;
use crate::config::PartyConfig;
use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::error::PartiesBatchError;
use crate::error::SelectionsError;
use crate::select::sched::FarHistory;
use crate::select::sched::FarHistoryConfig;
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
use crate::stream::StreamID;
use crate::stream::StreamReporter;
use crate::stream::ThreadedStreamError;

mod sched;

/// Newtype for the index for a set of connections.
#[derive(Clone, Eq, Hash, PartialEq)]
struct ConnectionsIdx(usize);

/// Newtype for the index for a set of streams.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StreamsIdx(usize);

/// Newtype used to identify a specific channel from an element of the
/// set of connection options.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ConnChannelID<ChannelID> {
    /// The specific channel.
    channel: ChannelID,
    /// The index of the connection option.
    conn_idx: ConnectionsIdx
}

/// Threaded version of [StreamSelectorConnections]
struct ThreadedStreamSelectorConnections<
    Src: Channels<Ctx>,
    Resolve: Addrs<Addr = Src::Addr>,
    Ctx
> {
    ctx: PhantomData<Ctx>,
    // ISSUE #12: try to make these into RwLocks
    /// Source of counterparty addresses to use to get raw streams.
    addrs: Mutex<Resolve>,
    /// Sources of channels from which to get raw streams.
    channels: Mutex<Src>
}

/// Type of batch ID's produce by [StreamSelector].
#[derive(Clone)]
pub struct StreamSelectorBatch<Epoch, BatchID> {
    /// The stream on which the batch exists.
    stream: DenseItemID<Epoch>,
    /// The batch ID on the target stream.
    batch_id: BatchID
}

/// Stream-like abstraction for constructing and selecting among
/// different streams for a given party.
///
/// In its most basic functioning, this type maintains a set of
/// connection options, each of which contains an [Addrs] instance
/// that provides possible endpoint addresses, together with a
/// [Channels] instance that provides a set of low-level channels for
/// communicating with the endpoint addresses.  Multiple such pairings
/// can be maintained to avoid spurious pairings (for example, an IP
/// address endpoint and a Unix socket channel).
///
/// # Maintaining and Scheduling Streams
///
/// These connection options are periodically refreshed and recomputed
/// to produce a total set of connection options.  This process
/// proceeds as follows:
///
///  1. Each connection option's [Addrs] instance is periodically refreshed to
///     get a new set of endpoint addresses.  In the case of DNS names, this
///     will resolve the name to obtain a set of IP addresses.
///
///  2. When a connection option's set of endpoint addresses changes, the set of
///     channels is obtained from its [Channels] instance. All possible pairings
///     of channels and endpoint addresses are checked for compatibility.  The
///     full set of compatible channel-endpoint pairings then becomes the set of
///     streams for this connection option.
///
///  3. Whenever any connection option's set of streams changes, all streams for
///     all connection options are collected, and given dense integer indexes.
///     An *epoch* identifier is then generated, which identifies the
///     stream-to-ID mapping.
///
///  4. The epoch continues until the set of streams changes, at which point a
///     new epoch is generated.  When the epoch changes, all currently-existing
///     streams are preserved (though any pending batches are automatically
///     cancelled).  Any streams that are no longer valid (for example, because
///     a DNS mapping changed) will be shut down, however.
///
/// A [Scheduler] structure is maintained for the set of possible
/// streams, and is used to select amongst them for the sending of any
/// single message or batch.  An actual stream and its corresponding
/// low-level channel will not be established until the possible
/// stream is selected for use (meaning that any low-level connection
/// or negotiation protocols will not take place until that happens).
///
/// # Stream Abstractions
///
/// This type also implements the [PushStream], [PushStreamAdd],
/// [PushStreamSingle], and [PushStreamSharedSingle] traits, allowing
/// it to function as an abstract stream in and of itself.  When used
/// in this manner, the scheduler is used to select from one of the
/// possible streams to create a batch.  All subsequent batch
/// operations are performed on that batch, which is ultimately sent
/// along that stream.
///
/// If the epoch changes before such a batch is sent, the batch will
/// be cancelled, and all subsequent operations will report an error.
/// Note that this is allowed by the overall stream abstraction, which
/// is based on unreliable datagram protocols.
pub struct StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Src: Channels<Ctx>,
    Resolve: Addrs<Addr = Src::Addr>,
    Src::Stream: Clone + PushStream<Ctx> + Send {
    /// The set of connection options.
    ///
    /// This represents the sources of possible streams.
    connections: Arc<Vec<ThreadedStreamSelectorConnections<Src, Resolve, Ctx>>>,
    /// Mutable state.
    state: Arc<RwLock<StreamSelectorState<Epochs, Src, Resolve, Ctx>>>,
    /// When to next refresh the set of possible streams.
    refresh_when: Arc<RwLock<Option<Instant>>>
}

/// [StreamReporter] instance for allowing [StreamSelector] to receive
/// streams from other sources.
///
/// This is primarily used to allow the pull side to install incoming
/// sessions into a [StreamSelector].
pub struct StreamSelectorReporter<Reporter, Epochs, Src, Resolve, Ctx>
where
    Reporter: StreamReporter<
        Src = StreamID<Src::Addr, Src::ChannelID, Src::Param>,
        Stream = Src::Stream
    >,
    Epochs: Iterator,
    Resolve: Addrs<Addr = Src::Addr>,
    Src: Channels<Ctx>,
    Src::Stream: Clone + PushStream<Ctx> + Send {
    reporter: Reporter,
    state: Arc<RwLock<StreamSelectorState<Epochs, Src, Resolve, Ctx>>>
}

/// Container for core mutable state.
struct StreamSelectorState<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Src: Channels<Ctx>,
    Resolve: Addrs<Addr = Src::Addr>,
    Src::Stream: Clone + PushStream<Ctx> + Send {
    /// Scheduler to use for selecting a raw stream.
    sched: Scheduler<
        Epochs,
        FarHistory,
        PassthruPolicy<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >,
        Resolve::Origin
    >,
    /// A mapping from the endpoint address, channel, and parameter
    /// set to dense IDs for this epoch.
    ///
    /// This is regenerated at the start of each epoch.
    stream_ids:
        HashMap<StreamID<Src::Addr, Src::ChannelID, Src::Param>, StreamsIdx>,
    /// The current set of possible streams, and any active stream objects.
    ///
    /// This is regenerated at the start of each epoch.
    streams:
        Vec<StreamEntry<Src::Addr, Src::ChannelID, Src::Param, Src::Stream>>
}

/// Entry in the streams array, representing a possible stream.
struct StreamEntry<Addr, ChannelID, Param, Stream> {
    /// Identifier of the stream.
    id: StreamID<Addr, ChannelID, Param>,
    /// Stream, if it has been created.
    stream: Option<Stream>
}

/// Type of [Selections](PushStreamShared::Selections) used by
/// [StreamSelector].
#[derive(Clone)]
pub struct SelectorSelections<ID, Inner> {
    id: Option<ID>,
    inner: Inner
}

/// Errors that can occur when refreshing the connections.
#[derive(Debug)]
pub enum StreamSelectorError<Addrs, Param> {
    /// Error occurred while refreshing one or more connection options.
    Refresh { err: RefreshError },
    /// Error occurred while obtaining addresses.
    Addrs { err: Addrs },
    /// Error occurred while obtaining low-level channels.
    Param { err: Param }
}

/// Errors that can occur when refreshing the connections.
#[derive(Debug)]
pub enum ThreadedStreamSelectorError<Addrs, Param> {
    /// Error occurred while refreshing one or more connection options.
    Refresh { err: RefreshError },
    /// Error occurred while obtaining addresses.
    Addrs { err: Addrs },
    /// Error occurred while obtaining low-level channels.
    Param { err: Param },
    /// Mutex was poisoned.
    MutexPoison
}

/// Errors that can occur when setting up a connection.
#[derive(Debug)]
pub enum StreamSelectorConnectionCreateError<Channels, Addrs> {
    /// Error occurred while obtaining a low-level channel.
    Channels { err: Channels },
    /// Error occurred while obtaining addresses.
    Addrs { err: Addrs }
}

/// Errors that can occur when creating a [StreamSelector].
#[derive(Debug)]
pub enum StreamSelectorCreateError<Src, Addrs> {
    /// Error occurred creating the connection options.
    Connection {
        err: StreamSelectorConnectionCreateError<Src, Addrs>
    },
    /// Error occurred during the initial refresh.
    Refresh { err: RefreshError }
}

/// Errors that can occur when reporting a success or failure.
#[derive(Debug)]
pub enum StreamSelectorReportError<Report> {
    /// Error occurred while reporting success or failure.
    Report {
        /// Error while reporting success or failure.
        err: Report
    },
    /// The specified stream was not found.
    NotFound,
    /// Mutex poisoned.
    MutexPoison
}

/// Errors that can occur when selecting a stream on a [StreamSelector].
#[derive(Debug)]
pub enum StreamSelectorSelectError<Addrs, Param, Item> {
    /// Error occurred while refreshing the connections.
    Selector {
        /// Error while refreshing the connections.
        err: ThreadedStreamSelectorError<Addrs, Param>
    },
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

/// Errors that can occur getting existing streams on a
/// [StreamSelector].
///
/// This is a template used by many concrete error types.
#[derive(Debug)]
pub enum SelectorStreamError<Epoch> {
    /// The epochs do not match.
    ///
    /// This is not recoverable, but does not represent a "hard"
    /// error.  It can occur sporadically due to timing issues.
    EpochMismatch {
        /// The current epoch.
        curr: Epoch,
        /// The batch epoch.
        batch: Epoch
    },
    /// The stream was closed, likely representing a stream error.
    ///
    /// This is not recoverable, but does not represent a "hard"
    /// error.  It can occur sporadically due to connection issues.
    StreamClosed,
    /// Mutex poisoned.
    MutexPoison
}

/// Errors that can occur with batches on a [StreamSelector].
///
/// This is a template used by many concrete error types.
#[derive(Debug)]
pub enum SelectorBatchError<Epoch, Err> {
    /// Error occurred at the batch level.
    Batch {
        /// Error at the batch level.
        batch: Err
    },
    /// Error occurred getting the stream.
    Stream {
        /// Error getting the stream.
        err: SelectorStreamError<Epoch>
    }
}

/// Errors that can occur with batches on a [StreamSelector].
///
/// This is a template used by many concrete error types.
#[derive(Debug)]
pub enum SelectorReportFailureError<Epoch, Item, Err> {
    /// Error occurred at the batch level.
    Inner {
        /// Error at the batch level.
        err: Err
    },
    /// Error occurred during reporting.
    Report {
        /// Error during reporting.
        err: StreamSelectorReportError<ReportError<Item>>
    },
    /// Error occurred getting the stream.
    Stream {
        /// Error getting the stream.
        err: SelectorStreamError<Epoch>
    }
}

/// Errors that can occur when creating batches.
#[derive(Clone)]
pub enum SelectorBatchSelectError<Select, Parties, Stream, Epoch> {
    /// Error occurred while selecting a stream.
    Select {
        /// Error while selecting a stream.
        select: Select,
        /// Parties for the batch, if applicable.
        parties: Parties
    },
    /// Error occurred on the underlying stream.
    Stream {
        /// Dense ID of the selected stream.
        selected: DenseItemID<Epoch>,
        /// Error on the underlying stream.
        stream: Stream
    }
}

/// Retry information for starting batches for [StreamSelector].
#[derive(Clone)]
pub struct SelectorStartRetry<PartyID> {
    when: Instant,
    parties: Vec<PartyID>
}

impl<ID, Inner> Default for SelectorSelections<ID, Inner>
where
    Inner: Default
{
    #[inline]
    fn default() -> Self {
        SelectorSelections {
            inner: Inner::default(),
            id: None
        }
    }
}

impl<Src, Resolve, Ctx> ThreadedStreamSelectorConnections<Src, Resolve, Ctx>
where
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Into<Option<IPEndpointAddr>>
{
    /// Create a single connections from its configuration objects.
    fn create<EndpointConfig>(
        ctx: &mut Ctx,
        reporter: Src::Reporter,
        addrs_config: &Resolve::Config,
        config: ConnectionConfig<Src::Config, String, EndpointConfig>
    ) -> Result<
        Self,
        StreamSelectorConnectionCreateError<
            Src::CreateError,
            Resolve::CreateError
        >
    >
    where
        Resolve: AddrsCreate<Ctx, Vec<EndpointConfig>>,
        Resolve::Config: Clone {
        let (channels, srcs, endpoints) = config.take();
        let channels =
            Src::create(ctx, reporter, channels, srcs).map_err(|err| {
                StreamSelectorConnectionCreateError::Channels { err: err }
            })?;
        let addrs = Resolve::create(ctx, addrs_config.clone(), endpoints)
            .map_err(|err| StreamSelectorConnectionCreateError::Addrs {
                err: err
            })?;

        Ok(ThreadedStreamSelectorConnections {
            ctx: PhantomData,
            channels: Mutex::new(channels),
            addrs: Mutex::new(addrs)
        })
    }

    fn get_refresh(
        &self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<(
            Vec<(Src::Addr, Resolve::Origin)>,
            Vec<(Src::ChannelID, Src::Param)>,
            Option<Instant>
        )>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Src::ParamError>
    > {
        let (addrs, refresh_addrs_when) = match self
            .addrs
            .lock()
            .map_err(|_| ThreadedStreamSelectorError::MutexPoison)?
            .addrs()
            .map_err(|err| ThreadedStreamSelectorError::Addrs { err: err })?
        {
            // Pass through retries.
            RetryResult::Retry(when) => return Ok(RetryResult::Retry(when)),
            RetryResult::Success(addrs) => addrs
        };
        let (params, refresh_channels_when) = match self
            .channels
            .lock()
            .map_err(|_| ThreadedStreamSelectorError::MutexPoison)?
            .params(ctx)
            .map_err(|err| ThreadedStreamSelectorError::Param { err: err })?
        {
            // Pass through retries.
            RetryResult::Retry(when) => return Ok(RetryResult::Retry(when)),
            RetryResult::Success(res) => res
        };
        let addrs: Vec<(Resolve::Addr, Resolve::Origin)> =
            addrs.map(|(addr, endpoint, _)| (addr, endpoint)).collect();
        let params: Vec<(Src::ChannelID, Src::Param)> = params.collect();

        let refresh_when = match (refresh_channels_when, refresh_addrs_when) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, out) => out,
            (out, None) => out
        };

        Ok(RetryResult::Success((addrs, params, refresh_when)))
    }

    #[inline]
    fn stream(
        &self,
        ctx: &mut Ctx,
        channel: &Src::ChannelID,
        addr: &Src::Addr,
        param: &Src::Param,
        origin: Resolve::Origin
    ) -> Result<RetryResult<Src::Stream>, ThreadedStreamError<Src::StreamError>>
    {
        self.channels
            .lock()
            .map_err(|_| ThreadedStreamError::MutexPoison)?
            .stream(ctx, channel, param, addr, origin.into().as_ref())
            .map_err(|err| ThreadedStreamError::Inner { error: err })
    }
}

impl<Epochs, Src, Resolve, Ctx> StreamSelectorState<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    fn create(
        config: FarSchedulerConfig,
        retry: Retry,
        epochs: Epochs
    ) -> Result<Self, RefreshError>
where {
        let config = FarHistoryConfig::from(&config);
        let sched =
            Scheduler::new(config, retry, PassthruPolicy::default(), epochs)?;

        Ok(StreamSelectorState {
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
    ) -> Result<Self, RefreshError>
where {
        let config = FarHistoryConfig::from(&config);
        let sched =
            Scheduler::new(config, retry, PassthruPolicy::default(), epochs)?;

        Ok(StreamSelectorState {
            sched: sched,
            stream_ids: HashMap::with_capacity(size),
            streams: Vec::new()
        })
    }

    #[inline]
    fn epoch(&self) -> Epochs::Item {
        self.sched.epoch().clone()
    }

    fn epoch_change(
        &mut self,
        epoch: EpochChange<
            Epochs::Item,
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>,
            Resolve::Origin
        >
    ) {
        let (_, dense_ids, _, _) = epoch.take();
        let mut new_stream_ids = HashMap::with_capacity(dense_ids.len());
        let mut new_streams = Vec::with_capacity(dense_ids.len());

        // Build the new stream IDs map.
        for (i, stream) in dense_ids.iter().enumerate() {
            let stream_id = StreamID::new(
                stream.0.party_addr().clone(),
                stream.0.channel().channel.clone(),
                stream.0.param().clone()
            );

            new_stream_ids.insert(stream_id.clone(), StreamsIdx(i));
            new_streams.push(StreamEntry {
                id: stream_id,
                stream: None
            })
        }

        for ent in self.streams.drain(..) {
            let StreamEntry {
                id: item,
                mut stream,
                ..
            } = ent;

            // Check if the existing stream is to be retained.
            match new_stream_ids.get(&item) {
                Some(idx) => {
                    trace!(target: "stream-selector",
                           "retaining stream for {}",
                           item);

                    // Cancel all pending batches.
                    if let Some(stream) = &mut stream {
                        trace!(target: "stream-selector",
                               "canceling pending batches");

                        stream.cancel_batches();
                    }

                    new_streams[idx.0].stream = stream
                }
                _ => {
                    debug!(target: "stream-selector",
                           "deleting stream for {}",
                           item);
                }
            }
        }

        self.streams = new_streams;
        self.stream_ids = new_stream_ids;
    }

    fn refresh_update(
        &mut self,
        mut pairs: Vec<(
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>,
            Resolve::Origin
        )>,
        refresh_when: Option<Instant>,
        now: Instant
    ) -> Result<RetryResult<Option<Instant>>, RefreshError> {
        // Update the scheduler, possibly get a new epoch
        match self.sched.refresh(now, pairs.drain(..))? {
            // The epoch changed.
            Some(epoch) => {
                self.epoch_change(epoch);

                Ok(RetryResult::Success(refresh_when))
            }
            // No epoch change.
            None => Ok(RetryResult::Success(None))
        }
    }

    /// Report a success for a given stream.
    fn success(
        &mut self,
        channel: Src::ChannelID,
        param: Src::Param,
        party_addr: Src::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
        let stream_id = StreamID::new(party_addr, channel, param);

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
    ) -> Result<
        (),
        ReportError<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    > {
        self.sched.success_id(id)
    }

    /// Report a success for a given stream.
    fn failure(
        &mut self,
        channel: Src::ChannelID,
        param: Src::Param,
        party_addr: Src::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
        let stream_id = StreamID::new(party_addr, channel, param);

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
    ) -> Result<
        (),
        ReportError<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    > {
        self.sched.failure_id(id)
    }

    fn handle_selected(
        &mut self,
        ctx: &mut Ctx,
        connections: &[ThreadedStreamSelectorConnections<Src, Resolve, Ctx>],
        stream_id: StreamID<
            Src::Addr,
            ConnChannelID<Src::ChannelID>,
            Src::Param
        >,
        origin: Resolve::Origin,
        dense_id: DenseItemID<Epochs::Item>
    ) -> Result<
        RetryResult<(Src::Stream, DenseItemID<Epochs::Item>)>,
        ReportError<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    > {
        let (party_addr, ConnChannelID { conn_idx, channel }, param) =
            stream_id.take();

        match &mut self.streams[dense_id.idx()] {
            // If the stream already exists, then just return it.
            StreamEntry {
                stream: Some(stream),
                ..
            } => {
                trace!(target: "stream-selector",
                   "stream for {} over channel {} ({}) already exists",
                   party_addr, channel, param);

                Ok(RetryResult::Success((stream.clone(), dense_id)))
            }
            // If the stream does not exist, create it.
            StreamEntry { stream, .. } => {
                match connections[conn_idx.0].stream(
                    ctx,
                    &channel,
                    &party_addr,
                    &param,
                    origin
                ) {
                    Ok(val) => Ok(val.flat_map(move |newstream| {
                        *stream = Some(newstream.clone());

                        RetryResult::Success((newstream, dense_id))
                    })),
                    // Errors here indicate stream negotiation
                    // errors; they are logged and reported to the
                    // scheduler, but do not result in hard
                    // errors.
                    Err(err) => {
                        warn!(target: "stream-selector",
                          "failed to establish stream: {}",
                          err);

                        // Report the failure
                        self.sched.failure_id(&dense_id)?;

                        // It's ok to try again immediately here;
                        // the scheduler will handle the retry for
                        // this item, and will end up generating a
                        // later retry if we run through all the
                        // options.
                        Ok(RetryResult::Retry(Instant::now()))
                    }
                }
            }
        }
    }

    fn do_select(
        &mut self,
        ctx: &mut Ctx,
        connections: &[ThreadedStreamSelectorConnections<Src, Resolve, Ctx>]
    ) -> Result<
        RetryResult<(Src::Stream, DenseItemID<Epochs::Item>)>,
        StreamSelectorSelectError<
            Resolve::AddrsError,
            Src::ParamError,
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    > {
        self.sched
            .select()
            .map_err(|err| StreamSelectorSelectError::Select { err: err })?
            .flat_map_ok(move |(stream_id, origin, dense_id)| {
                self.handle_selected(
                    ctx,
                    connections,
                    stream_id,
                    origin,
                    dense_id
                )
                .map_err(|err| StreamSelectorSelectError::Report { err: err })
            })
    }

    fn report<Reporter>(
        &mut self,
        reporter: &mut Reporter,
        stream_id: StreamID<Src::Addr, Src::ChannelID, Src::Param>,
        prin: Reporter::Prin,
        stream: Src::Stream
    ) -> Result<
        Option<Src::Stream>,
        StreamSelectorReportError<Reporter::ReportError>
    >
    where
        Reporter: StreamReporter<
            Src = StreamID<Src::Addr, Src::ChannelID, Src::Param>,
            Stream = Src::Stream
        > {
        match self.stream_ids.get(&stream_id) {
            Some(idx) => match &self.streams[idx.0].stream {
                Some(stream) => {
                    trace!(target: "stream-selector",
                           "stream {} already existed",
                           stream_id);

                    Ok(Some(stream.clone()))
                }
                None => {
                    trace!(target: "stream-selector",
                           "reporting stream {} to inner reporter",
                           stream_id);

                    match reporter
                        .report(stream_id.clone(), prin, stream.clone())
                        .map_err(|err| StreamSelectorReportError::Report {
                            err: err
                        })? {
                        Some(stream) => {
                            trace!(target: "stream-selector",
                                   "inner reporter already had stream for {}",
                                   stream_id);

                            Ok(Some(stream))
                        }
                        None => {
                            trace!(target: "stream-selector",
                                   "adding stream {}",
                                   stream_id);

                            self.streams[idx.0].stream = Some(stream);

                            Ok(None)
                        }
                    }
                }
            },
            None => Err(StreamSelectorReportError::NotFound)
        }
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Src::Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Src::Stream, SelectorStreamError<Epochs::Item>> {
        // Check that the epochs match.
        let curr_epoch = self.epoch();

        if batch.stream.epoch() == &curr_epoch {
            // The epoch matches; check that the stream is still open.

            match &self.streams[batch.stream.idx()].stream {
                // The stream is still open; try to cancel the batch.
                Some(stream) => Ok(stream.clone()),
                None => Err(SelectorStreamError::StreamClosed)
            }
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
    ) -> Result<Src::Stream, SelectorStreamError<Epochs::Item>> {
        // Check that the epochs match.
        let curr_epoch = self.epoch();

        if id.epoch() == &curr_epoch {
            // The epoch matches; check that the stream is still open.

            match &self.streams[id.idx()].stream {
                // The stream is still open; try to cancel the batch.
                Some(stream) => Ok(stream.clone()),
                None => Err(SelectorStreamError::StreamClosed)
            }
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
            .filter_map(|StreamEntry { stream, .. }| stream.as_mut())
        {
            stream.cancel_batches()
        }
    }
}

impl<Epochs, Src, Resolve, Ctx> Clone
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Src: Channels<Ctx>,
    Resolve: Addrs<Addr = Src::Addr>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    fn clone(&self) -> Self {
        StreamSelector {
            refresh_when: self.refresh_when.clone(),
            connections: self.connections.clone(),
            state: self.state.clone()
        }
    }
}

impl<Reporter, Epochs, Src, Resolve, Ctx> StreamReporter
    for StreamSelectorReporter<Reporter, Epochs, Src, Resolve, Ctx>
where
    Reporter: StreamReporter<
        Src = StreamID<Src::Addr, Src::ChannelID, Src::Param>,
        Stream = Src::Stream
    >,
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    type Prin = Reporter::Prin;
    type ReportError = StreamSelectorReportError<Reporter::ReportError>;
    type Src = StreamID<Src::Addr, Src::ChannelID, Src::Param>;
    type Stream = Src::Stream;

    fn report(
        &mut self,
        src: StreamID<Src::Addr, Src::ChannelID, Src::Param>,
        prin: Reporter::Prin,
        stream: Self::Stream
    ) -> Result<Option<Self::Stream>, Self::ReportError> {
        debug!(target: "stream-selector",
               "reporting stream for {}",
               src);

        match self.state.write() {
            Ok(mut guard) => {
                guard.report(&mut self.reporter, src, prin, stream)
            }
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }
}

impl<Epochs, Src, Resolve, Reporter, Ctx> PushStreamReporter<Reporter>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send,
    Reporter: StreamReporter<
        Src = StreamID<Src::Addr, Src::ChannelID, Src::Param>,
        Stream = Src::Stream
    >
{
    type Reporter = StreamSelectorReporter<Reporter, Epochs, Src, Resolve, Ctx>;

    #[inline]
    fn reporter(
        &self,
        reporter: Reporter
    ) -> StreamSelectorReporter<Reporter, Epochs, Src, Resolve, Ctx> {
        StreamSelectorReporter {
            reporter: reporter,
            state: self.state.clone()
        }
    }
}

impl<Epochs, Src, Resolve, Ctx> StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    /// Create a new [StreamSelector] from a configuration and other
    /// necessary objects.
    ///
    /// The `reporter` parameter is a [StreamReporter] instance that
    /// will be used to report *both* newly-created streams as well as
    /// incoming streams reported to *this* `StreamSelector` by a
    /// [StreamSelectorReporter].  (This is necessary to avoid deadlocks.)
    pub fn create<EndpointConfig>(
        ctx: &mut Ctx,
        reporter: Src::Reporter,
        config: PartyConfig<
            Resolve::Config,
            Src::Config,
            String,
            EndpointConfig
        >,
        epochs: Epochs
    ) -> Result<
        Self,
        StreamSelectorCreateError<Src::CreateError, Resolve::CreateError>
    >
    where
        Resolve: AddrsCreate<Ctx, Vec<EndpointConfig>>,
        Resolve::Config: Clone + Default {
        let (scheduler, resolver, retry, size_hint, connections) =
            config.take();
        let state = match size_hint {
            Some(size) => StreamSelectorState::with_capacity(
                scheduler, retry, epochs, size
            ),
            None => StreamSelectorState::create(scheduler, retry, epochs)
        }
        .map_err(|err| StreamSelectorCreateError::Refresh { err: err })?;
        let now = Instant::now();
        let mut conns = Vec::with_capacity(connections.len());

        for connection in connections {
            conns.push(
                ThreadedStreamSelectorConnections::create(
                    ctx,
                    reporter.clone(),
                    &resolver,
                    connection
                )
                .map_err(|err| {
                    StreamSelectorCreateError::Connection { err: err }
                })?
            );
        }

        Ok(StreamSelector {
            state: Arc::new(RwLock::new(state)),
            connections: Arc::new(conns),
            refresh_when: Arc::new(RwLock::new(Some(now)))
        })
    }

    fn get_refreshes(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        (
            Vec<(
                ConnectionsIdx,
                Vec<(Src::Addr, Resolve::Origin)>,
                Vec<(Src::ChannelID, Src::Param)>
            )>,
            Option<Instant>,
            Option<Instant>,
            usize
        ),
        ThreadedStreamSelectorError<Resolve::AddrsError, Src::ParamError>
    > {
        let mut refreshes = Vec::with_capacity(self.connections.len());
        let mut min_retry: Option<Instant> = None;
        let mut min_refresh: Option<Instant> = None;
        let mut size_hint = 0;

        // Gather up all the refresh results.
        for i in 0..self.connections.len() {
            match self.connections[i].get_refresh(ctx)? {
                // Split retry results and
                RetryResult::Retry(when) => {
                    min_retry =
                        Some(min_retry.map_or(when, |curr| curr.min(when)));
                }
                RetryResult::Success((addrs, params, refresh_when)) => {
                    size_hint += addrs.len() * params.len();
                    refreshes.push((ConnectionsIdx(i), addrs, params));
                    min_refresh = match (min_refresh, refresh_when) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (None, out) => out,
                        (out, None) => out
                    }
                }
            }
        }

        Ok((refreshes, min_retry, min_refresh, size_hint))
    }

    fn refresh_pairs(
        refreshes: Vec<(
            ConnectionsIdx,
            Vec<(Src::Addr, Resolve::Origin)>,
            Vec<(Src::ChannelID, Src::Param)>
        )>,
        size_hint: usize
    ) -> Vec<(
        StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>,
        Resolve::Origin
    )> {
        let mut pairs = Vec::with_capacity(size_hint);
        let mut dedup = HashSet::with_capacity(size_hint);

        // Go through each of the refresh results, and pair up all
        // the addresses and params.
        for (conn_idx, addrs, params) in refreshes {
            for (addr, endpoint) in addrs {
                for (channel, param) in &params {
                    trace!(target: "stream-selector",
                           "trying to pair {} over {}",
                           addr, param);

                    if param.accepts_addr(&addr) {
                        trace!(target: "stream-selector",
                               "channel {} accepts address {}",
                               param, addr);

                        let stream_id = StreamID::new(
                            addr.clone(),
                            channel.clone(),
                            param.clone()
                        );

                        // Make sure we don't already have an
                        // equivalent stream.
                        if !dedup.contains(&stream_id) {
                            dedup.insert(stream_id);

                            let conn_channel = ConnChannelID {
                                conn_idx: conn_idx.clone(),
                                channel: channel.clone()
                            };
                            let sched_stream_id = StreamID::new(
                                addr.clone(),
                                conn_channel,
                                param.clone()
                            );

                            pairs.push((sched_stream_id, endpoint.clone()));
                        } else {
                            warn!(target: "stream-selector",
                                  concat!("duplicate stream from ",
                                          "configuration: to {} ",
                                          "over channel {} ({})"),
                                  addr, channel, param);
                        }
                    } else {
                        debug!(target: "stream-selector",
                               "channel {} does not accept address {}",
                               param, addr);
                    }
                }
            }
        }

        pairs
    }

    fn do_refresh(
        &mut self,
        ctx: &mut Ctx,
        now: Instant
    ) -> Result<
        RetryResult<Option<Instant>>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Src::ParamError>
    > {
        let (refreshes, min_retry, min_refresh, size_hint) =
            self.get_refreshes(ctx)?;

        if !refreshes.is_empty() {
            // We got at least one valid refresh.

            // If we got a retry time, use that; otherwise use the
            // refresh time.
            let refresh_when = match (min_retry, min_refresh) {
                (Some(a), _) => Some(a),
                (None, out) => out
            };

            let pairs = Self::refresh_pairs(refreshes, size_hint);

            let out = match self.state.write() {
                Ok(mut guard) => {
                    guard.refresh_update(pairs, refresh_when, now).map_err(
                        |err| ThreadedStreamSelectorError::Refresh { err: err }
                    )
                }
                Err(_) => Err(ThreadedStreamSelectorError::MutexPoison)
            };

            match self.refresh_when.write() {
                Ok(mut guard) => {
                    *guard = refresh_when;

                    out
                }
                // Technically, we could return, but throw the error
                // just to be careful.
                Err(_) => Err(ThreadedStreamSelectorError::MutexPoison)
            }
        } else {
            // There should have been a retry time set if we get here.
            let min_retry = match min_retry {
                Some(time) => time,
                None => {
                    // This should never happen, but we can recover.
                    error!(target: "stream-selector",
                           "min retry time should have been set");

                    Instant::now()
                }
            };

            // Every refresh indicated to retry later.
            Ok(RetryResult::Retry(min_retry))
        }
    }

    /// Refresh all eligible connection options, and possibly move to
    /// a new epoch.
    ///
    /// This will scan all connection options that are ready for
    /// refresh, obtain a fresh set of addresses, and then compile a
    /// new set of possible streams by comparing them against the
    /// available channels.  If the total set of possible streams has
    /// not changed, then the current epoch will continue.  However,
    /// if there is any change, then the possible streams will be
    /// re-indexed, and a new epoch will begin.
    pub fn refresh(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Src::ParamError>
    > {
        let now = Instant::now();

        let when = match self.refresh_when.read() {
            Ok(guard) => {
                if let Some(when) = *guard &&
                    when <= now
                {
                    Ok(None)
                } else {
                    Ok(Some(*guard))
                }
            }
            Err(_) => Err(ThreadedStreamSelectorError::MutexPoison)
        }?;

        match when {
            Some(when) => Ok(RetryResult::Success(when)),
            None => self.do_refresh(ctx, now)
        }
    }

    /// Report a success for a given stream.
    #[inline]
    pub fn success(
        &mut self,
        channel: Src::ChannelID,
        param: Src::Param,
        party_addr: Src::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
        match self.state.write() {
            Ok(mut guard) => guard.success(channel, param, party_addr),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Report a success for the stream identified by `id`.
    #[inline]
    pub fn success_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
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
        channel: Src::ChannelID,
        param: Src::Param,
        party_addr: Src::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
        match self.state.write() {
            Ok(mut guard) => guard.failure(channel, param, party_addr),
            Err(_) => Err(StreamSelectorReportError::MutexPoison)
        }
    }

    /// Report a failure for the stream identified by `id`.
    #[inline]
    pub fn failure_id(
        &mut self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >
        >
    > {
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
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<(Src::Stream, DenseItemID<Epochs::Item>)>,
        StreamSelectorSelectError<
            Resolve::AddrsError,
            Src::ParamError,
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    > {
        // First try to refresh, if needed.
        self
            .refresh(ctx)
            .map_err(|err| StreamSelectorSelectError::Selector { err: err })?
            // If the refresh succeeds, call the scheduler to get the
            // selected item.
            .flat_map_ok(move |_| match self.state.write() {
                Ok(mut guard) => guard.do_select(ctx, &self.connections),
                Err(_) => Err(StreamSelectorSelectError::MutexPoison)
            })
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Src::Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Src::Stream, SelectorStreamError<Epochs::Item>> {
        match self.state.read() {
            Ok(guard) => guard.batch_stream(batch),
            Err(_) => Err(SelectorStreamError::MutexPoison)
        }
    }

    fn dense_id_stream(
        &self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<Src::Stream, SelectorStreamError<Epochs::Item>> {
        match self.state.read() {
            Ok(guard) => guard.dense_id_stream(id),
            Err(_) => Err(SelectorStreamError::MutexPoison)
        }
    }
}

impl<Report> ScopedError for StreamSelectorReportError<Report>
where
    Report: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            StreamSelectorReportError::Report { err } => err.scope(),
            StreamSelectorReportError::NotFound => ErrorScope::Unrecoverable,
            StreamSelectorReportError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Epoch> ScopedError for SelectorStreamError<Epoch> {
    fn scope(&self) -> ErrorScope {
        match self {
            SelectorStreamError::EpochMismatch { .. } => ErrorScope::Batch,
            SelectorStreamError::StreamClosed => ErrorScope::Session,
            SelectorStreamError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Epoch, Err> ScopedError for SelectorBatchError<Epoch, Err>
where
    Err: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            SelectorBatchError::Batch { batch } => batch.scope(),
            SelectorBatchError::Stream { err } => err.scope()
        }
    }
}

impl<Epoch, Item, Err> ScopedError
    for SelectorReportFailureError<Epoch, Item, Err>
where
    Err: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            SelectorReportFailureError::Inner { err } => err.scope(),
            SelectorReportFailureError::Report { err } => err.scope(),
            SelectorReportFailureError::Stream { err } => err.scope()
        }
    }
}

impl<Epoch> BatchError for SelectorStreamError<Epoch>
where
    Epoch: Display
{
    type Completable = Infallible;
    type Permanent = SelectorStreamError<Epoch>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SelectorStreamError::EpochMismatch { curr, batch } => (
                None,
                Some(SelectorStreamError::EpochMismatch { curr, batch })
            ),
            SelectorStreamError::StreamClosed => {
                (None, Some(SelectorStreamError::StreamClosed))
            }
            SelectorStreamError::MutexPoison => {
                (None, Some(SelectorStreamError::MutexPoison))
            }
        }
    }
}

impl<Epoch, Err, T> ErrorReportInfo<T> for SelectorBatchError<Epoch, Err>
where
    Err: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let SelectorBatchError::Batch { batch } = self {
            batch.report_info()
        } else {
            None
        }
    }
}

impl<Epoch, Item, Err, T> ErrorReportInfo<T>
    for SelectorReportFailureError<Epoch, Item, Err>
where
    Err: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let SelectorReportFailureError::Inner { err } = self {
            err.report_info()
        } else {
            None
        }
    }
}

impl<Epoch, Err> BatchError for SelectorBatchError<Epoch, Err>
where
    Err: BatchError,
    Epoch: Display
{
    type Completable = Err::Completable;
    type Permanent = SelectorBatchError<Epoch, Err::Permanent>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SelectorBatchError::Batch { batch } => {
                let (completable, permanent) = batch.split();

                (
                    completable,
                    permanent
                        .map(|err| SelectorBatchError::Batch { batch: err })
                )
            }
            SelectorBatchError::Stream { err } => {
                let (_, permanent) = err.split();

                (
                    None,
                    permanent
                        .map(|err| SelectorBatchError::Stream { err: err })
                )
            }
        }
    }
}

impl<Select, Parties, Stream, Epoch> ScopedError
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: ScopedError,
    Stream: ScopedError,
    Epoch: Clone
{
    fn scope(&self) -> ErrorScope {
        match self {
            SelectorBatchSelectError::Select { select, .. } => select.scope(),
            SelectorBatchSelectError::Stream { stream, .. } => stream.scope()
        }
    }
}

impl<Select, Parties, Stream, Epoch> BatchError
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: BatchError,
    Stream: BatchError,
    Parties: Clone,
    Epoch: Clone
{
    type Completable = SelectorBatchSelectError<
        Select::Completable,
        Parties,
        Stream::Completable,
        Epoch
    >;
    type Permanent = SelectorBatchSelectError<
        Select::Permanent,
        Parties,
        Stream::Permanent,
        Epoch
    >;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SelectorBatchSelectError::Select { select, parties } => {
                let (completable, permanent) = select.split();

                (
                    completable.map(|err| SelectorBatchSelectError::Select {
                        select: err,
                        parties: parties.clone()
                    }),
                    permanent.map(|err| SelectorBatchSelectError::Select {
                        select: err,
                        parties: parties
                    })
                )
            }
            SelectorBatchSelectError::Stream { stream, selected } => {
                let (completable, permanent) = stream.split();

                (
                    completable.map(|err| SelectorBatchSelectError::Stream {
                        stream: err,
                        selected: selected.clone()
                    }),
                    permanent.map(|err| SelectorBatchSelectError::Stream {
                        stream: err,
                        selected: selected
                    })
                )
            }
        }
    }
}

impl<PartyID> RetryWhen for SelectorStartRetry<PartyID> {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

impl<Select, Parties, Stream, Epoch> ErrorReportInfo<DenseItemID<Epoch>>
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Epoch: Clone
{
    #[inline]
    fn report_info(&self) -> Option<DenseItemID<Epoch>> {
        if let SelectorBatchSelectError::Stream { selected, .. } = self {
            Some(selected.clone())
        } else {
            None
        }
    }
}

impl<Select, Parties, Stream, Epoch> RetryWhen
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: RetryWhen,
    Stream: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            SelectorBatchSelectError::Select { select, .. } => select.when(),
            SelectorBatchSelectError::Stream { stream, .. } => stream.when()
        }
    }
}

impl<Addrs, Param> ScopedError for StreamSelectorError<Addrs, Param>
where
    Param: ScopedError,
    Addrs: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            StreamSelectorError::Addrs { err } => err.scope(),
            StreamSelectorError::Param { err } => err.scope(),
            StreamSelectorError::Refresh { err } => err.scope()
        }
    }
}

impl<Addrs, Param> ScopedError for ThreadedStreamSelectorError<Addrs, Param>
where
    Param: ScopedError,
    Addrs: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            ThreadedStreamSelectorError::Addrs { err } => err.scope(),
            ThreadedStreamSelectorError::Param { err } => err.scope(),
            ThreadedStreamSelectorError::Refresh { err } => err.scope(),
            ThreadedStreamSelectorError::MutexPoison => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<Addrs, Param, StreamID> ScopedError
    for StreamSelectorSelectError<Addrs, Param, StreamID>
where
    Param: ScopedError,
    Addrs: ScopedError,
    StreamID: Display
{
    fn scope(&self) -> ErrorScope {
        match self {
            StreamSelectorSelectError::Selector { err } => err.scope(),
            StreamSelectorSelectError::Select { err } => err.scope(),
            StreamSelectorSelectError::Report { err } => err.scope(),
            StreamSelectorSelectError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Addrs, Param, StreamID> BatchError
    for StreamSelectorSelectError<Addrs, Param, StreamID>
where
    Param: Display + ScopedError,
    Addrs: Display + ScopedError,
    StreamID: Display
{
    type Completable = Infallible;
    type Permanent = Self;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<Epochs, Src, Resolve, Ctx> PushStream<Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    type BatchID = StreamSelectorBatch<
        Epochs::Item,
        <Src::Stream as PushStream<Ctx>>::BatchID
    >;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Src::Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type CancelBatchRetry = <Src::Stream as PushStream<Ctx>>::CancelBatchRetry;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Src::Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type FinishBatchRetry = <Src::Stream as PushStream<Ctx>>::FinishBatchRetry;
    type ReportError = SelectorReportFailureError<
        Epochs::Item,
        StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>,
        <Src::Stream as PushStream<Ctx>>::ReportError
    >;
    type StreamFlags = <Src::Stream as PushStream<Ctx>>::StreamFlags;

    #[inline]
    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
        Src::Stream::empty_flags_with_capacity(size)
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
                error!(target: "stream-selector",
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

impl<Epochs, Src, Resolve, Ctx> PushStreamReportError<DenseItemID<Epochs::Item>>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    type ReportError = StreamSelectorReportError<
        ReportError<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    >;

    fn report_error(
        &mut self,
        selected: &DenseItemID<Epochs::Item>
    ) -> Result<(), Self::ReportError> {
        trace!(target: "stream-selector",
               "reporting error to {}",
               selected);

        self.failure_id(selected)
    }
}

impl<Epochs, Src, Resolve, Ctx, Error> PushStreamReportError<Error>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send,
    Error: ErrorReportInfo<DenseItemID<Epochs::Item>>
{
    type ReportError = StreamSelectorReportError<
        ReportError<
            StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
        >
    >;

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

impl<Epochs, Src, Resolve, Ctx, Error>
    PushStreamReportBatchError<Error, <Self as PushStream<Ctx>>::BatchID>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + Send
{
    type ReportBatchError = <Self as PushStream<Ctx>>::ReportError;

    fn report_error_with_batch(
        &mut self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Src::Stream as PushStream<Ctx>>::BatchID
        >,
        _error: &Error
    ) -> Result<(), Self::ReportBatchError> {
        self.report_failure(batch)
    }
}

impl<Msg, Epochs, Src, Resolve, Ctx> PushStreamAdd<Msg, Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + PushStreamAdd<Msg, Ctx> + Send
{
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Src::Stream as PushStreamAdd<Msg, Ctx>>::AddError
    >;
    type AddRetry = <Src::Stream as PushStreamAdd<Msg, Ctx>>::AddRetry;

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

impl<Epochs, Src, Resolve, Ctx> PushStreamPartyID
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + PushStreamPartyID + Send
{
    type PartyID = <Src::Stream as PushStreamPartyID>::PartyID;
}

impl<Epochs, Src, Resolve, Ctx> PushStreamShared<Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + PushStreamShared<Ctx> + Send
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Src::Stream as PushStreamShared<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry =
        <Src::Stream as PushStreamShared<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            Vec<<Src::Stream as PushStreamPartyID>::PartyID>,
            <Src::Stream as PushStreamShared<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Src::Stream as PushStreamPartyID>::PartyID>,
        <Src::Stream as PushStreamShared<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Src::Stream as PushStreamShared<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            Vec<<Src::Stream as PushStreamPartyID>::PartyID>,
            <Src::Stream as PushStreamShared<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Src::Stream as PushStreamPartyID>::PartyID>,
        <Src::Stream as PushStreamShared<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Src::Stream as PushStreamShared<Ctx>>::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Src::Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Src::Stream::empty_batches_with_capacity(size)
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
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.clone(),
                    select: err
                }
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
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.clone(),
                    select: err
                }
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
        error!(target: "stream-selector",
               "should never call retry_abort_start_batch on this stream");

        RetryResult::Success(())
    }
}

impl<Epochs, Src, Resolve, Ctx> PushStreamPrivate<Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStream<Ctx> + PushStreamPrivate<Ctx> + Send
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Src::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry =
        <Src::Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            (),
            <Src::Stream as PushStreamPrivate<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Src::Stream as PushStreamPrivate<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Src::Stream as PushStreamPrivate<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            (),
            <Src::Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Src::Stream as PushStreamPrivate<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Src::Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Src::Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Src::Stream::empty_batches_with_capacity(size)
    }

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryResult<(), Self::SelectRetry>, Self::SelectError> {
        // Try to select a stream.
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: (),
                    select: err
                }
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
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: (),
                    select: err
                }
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
        error!(target: "stream-selector",
               "should never call retry_abort_start_batch on this stream");

        RetryResult::Success(())
    }
}

impl<Msg, Epochs, Src, Resolve, Ctx> PushStreamPrivateSingle<Msg, Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream: Clone + PushStreamPrivateSingle<Msg, Ctx> + Send
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            (),
            <Src::Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Src::Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Src::ParamError,
                StreamID<Src::Addr, ConnChannelID<Src::ChannelID>, Src::Param>
            >,
            (),
            <Src::Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Src::Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushRetry,
        Epochs::Item
    >;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        // Try to select a stream.
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: err,
                    parties: ()
                }
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

impl<Msg, Epochs, Src, Resolve, Ctx> PushStreamSharedSingle<Msg, Ctx>
    for StreamSelector<Epochs, Src, Resolve, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Src: ChannelsCreate<Ctx, Vec<String>>,
    Src::Config: Default,
    Src::Reporter: Clone,
    Resolve: Addrs<Addr = Src::Addr>,
    Resolve::Origin: Clone + Eq + Hash + Into<Option<IPEndpointAddr>>,
    Src::Stream:
        Clone + PushStreamSharedSingle<Msg, Ctx> + PushStreamPartyID + Send
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                StreamSelectorSelectError<
                    Resolve::AddrsError,
                    Src::ParamError,
                    StreamID<
                        Src::Addr,
                        ConnChannelID<Src::ChannelID>,
                        Src::Param
                    >
                >
            >,
            (),
            <Src::Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Src::Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                StreamSelectorSelectError<
                    Resolve::AddrsError,
                    Src::ParamError,
                    StreamID<
                        Src::Addr,
                        ConnChannelID<Src::ChannelID>,
                        Src::Param
                    >
                >
            >,
            (),
            <Src::Stream as PushStreamSharedSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Src::Stream as PushStreamSharedSingle<Msg, Ctx>>::PushRetry,
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
        match self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: PartiesBatchError::new(parties.clone(), err),
                    parties: ()
                }
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
                error!(target: "stream-selector",
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

impl Display for ConnectionsIdx {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "connections #{}", self.0)
    }
}

impl<ChannelID> Display for ConnChannelID<ChannelID>
where
    ChannelID: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "channel {} in {}", self.channel, self.conn_idx)
    }
}

impl<Src, Addrs> Display for StreamSelectorConnectionCreateError<Src, Addrs>
where
    Src: Display,
    Addrs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorConnectionCreateError::Channels { err } => err.fmt(f),
            StreamSelectorConnectionCreateError::Addrs { err } => err.fmt(f)
        }
    }
}

impl<Src, Addrs> Display for StreamSelectorCreateError<Src, Addrs>
where
    Src: Display,
    Addrs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorCreateError::Connection { err } => err.fmt(f),
            StreamSelectorCreateError::Refresh { err } => err.fmt(f)
        }
    }
}

impl<Addrs, Param> Display for StreamSelectorError<Addrs, Param>
where
    Param: Display,
    Addrs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorError::Addrs { err } => err.fmt(f),
            StreamSelectorError::Param { err } => err.fmt(f),
            StreamSelectorError::Refresh { err } => err.fmt(f)
        }
    }
}

impl<Addrs, Param> Display for ThreadedStreamSelectorError<Addrs, Param>
where
    Param: Display,
    Addrs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ThreadedStreamSelectorError::Addrs { err } => err.fmt(f),
            ThreadedStreamSelectorError::Param { err } => err.fmt(f),
            ThreadedStreamSelectorError::Refresh { err } => err.fmt(f),
            ThreadedStreamSelectorError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<Item> Display for StreamSelectorReportError<Item>
where
    Item: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorReportError::Report { err } => err.fmt(f),
            StreamSelectorReportError::NotFound => {
                write!(f, "stream not found")
            }
            StreamSelectorReportError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<Addrs, Param, Item> Display
    for StreamSelectorSelectError<Addrs, Param, Item>
where
    Addrs: Display,
    Param: Display,
    Item: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorSelectError::Selector { err } => err.fmt(f),
            StreamSelectorSelectError::Select { err } => err.fmt(f),
            StreamSelectorSelectError::Report { err } => err.fmt(f),
            StreamSelectorSelectError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl Display for StreamsIdx {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        self.0.fmt(f)
    }
}

impl<Select, Parties, Stream, Epoch> Display
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: Display,
    Stream: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SelectorBatchSelectError::Select { select, .. } => select.fmt(f),
            SelectorBatchSelectError::Stream { stream, .. } => stream.fmt(f)
        }
    }
}

impl<Epoch> Display for SelectorStreamError<Epoch>
where
    Epoch: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SelectorStreamError::EpochMismatch { curr, batch } => write!(
                f,
                "epoch mismatch (current: {}, batch: {})",
                curr, batch
            ),
            SelectorStreamError::StreamClosed => write!(f, "stream was closed"),
            SelectorStreamError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}

impl<Epoch, Err> Display for SelectorBatchError<Epoch, Err>
where
    Epoch: Display,
    Err: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SelectorBatchError::Batch { batch } => batch.fmt(f),
            SelectorBatchError::Stream { err } => err.fmt(f)
        }
    }
}

impl<Epoch, Item, Err> Display for SelectorReportFailureError<Epoch, Item, Err>
where
    Epoch: Display,
    Item: Display,
    Err: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SelectorReportFailureError::Inner { err } => err.fmt(f),
            SelectorReportFailureError::Report { err } => err.fmt(f),
            SelectorReportFailureError::Stream { err } => err.fmt(f)
        }
    }
}
