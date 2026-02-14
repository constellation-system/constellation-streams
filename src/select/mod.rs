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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;

use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
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
use crate::config::ConnectionConfig;
use crate::config::FarSchedulerConfig;
use crate::config::PartyConfig;
use crate::error::ErrorReportInfo;
use crate::error::PartiesBatchError;
use crate::error::SelectionsError;
use crate::large_obj::LargeObjID;
use crate::select::sched::FarHistory;
use crate::select::sched::FarHistoryConfig;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;

pub mod dispatch;
mod sched;

pub trait OutboundEndpointConfig<Endpoint, OutboundNego> {
    fn take(self) -> (Endpoint, OutboundNego);
}

/// Newtype for the index for a set of connections.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConnectionsIdx(usize);

/// Newtype for the index for a set of streams.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamsIdx(usize);

/// Newtype used to identify a specific channel from an element of the
/// set of connection options.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnChannelID<ChannelID> {
    /// The specific channel.
    channel: ChannelID,
    /// The index of the connection option.
    conn_idx: ConnectionsIdx
}

/// Threaded version of [StreamSelectorConnections]
struct ThreadedStreamSelectorConnections<
    Resolve: Addrs<Addr = Ctx::Addr>,
    Ctx: Channels<()>
> {
    ctx: PhantomData<Ctx>,
    /// Channel IDs to use for getting new parameters.
    channels: Vec<Ctx::ChannelID>,
    // ISSUE #12: try to make these into RwLocks
    /// Source of counterparty addresses to use to get raw streams.
    addrs: Mutex<Resolve>,
    /// Outbound negotiator parameters.
    params: HashMap<Resolve::Origin, Ctx::OutNegoParam>
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
/// [PushStreamPrivate], and [PushStreamShared] traits, allowing
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
pub struct StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr> {
    /// The set of connection options.
    ///
    /// This represents the sources of possible streams.
    connections: Arc<Vec<ThreadedStreamSelectorConnections<Resolve, Ctx>>>,
    /// Mutable state.
    state: Arc<RwLock<StreamSelectorState<Epochs, Ctx>>>,
    /// When to next refresh the set of possible streams.
    refresh_when: Arc<RwLock<Option<Instant>>>,
}

/// Container for core mutable state.
struct StreamSelectorState<Epochs, Ctx>
where
    Epochs: Iterator,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send {
    /// Scheduler to use for selecting a raw stream.
    sched: Scheduler<
        Epochs,
        FarHistory,
        PassthruPolicy<
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >,
        Ctx::OutNegoParam
    >,
    /// A mapping from the endpoint address, channel, and parameter
    /// set to dense IDs for this epoch.
    ///
    /// This is regenerated at the start of each epoch.
    stream_ids:
        HashMap<StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>, StreamsIdx>,
    /// The current set of possible streams, and any active stream objects.
    ///
    /// This is regenerated at the start of each epoch.
    streams:
        Vec<StreamEntry<Ctx::Addr, Ctx::ChannelID, Ctx::Param, Ctx::Stream>>
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
pub enum StreamSelectorConnectionCreateError<Addrs> {
    /// Error occurred while obtaining addresses.
    Addrs { err: Addrs },
    BadName { name: String }
}

/// Errors that can occur when creating a [StreamSelector].
#[derive(Debug)]
pub enum StreamSelectorCreateError<Addrs, Epochs> {
    /// Error occurred creating the connection options.
    Connection {
        err: StreamSelectorConnectionCreateError<Addrs>
    },
    /// Error occurred during the initial refresh.
    Refresh { err: RefreshError },
    /// Error occurred creating the [Epochs] instance.
    Epochs { err: Epochs }
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
#[derive(Clone, Debug)]
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

impl<Resolve, Ctx> ThreadedStreamSelectorConnections<Resolve, Ctx>
where
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    /// Create a single connections from its configuration objects.
    fn create<EndpointConfig>(
        ctx: &mut Ctx,
        addrs_config: &Resolve::Config,
        config: ConnectionConfig<String, EndpointConfig>
    ) -> Result<
        Self,
        StreamSelectorConnectionCreateError<Resolve::CreateError>
    >
    where
        EndpointConfig: OutboundEndpointConfig<Resolve::Origin,
                                               Ctx::OutNegoParam>,
        Resolve: AddrsCreate<Ctx>,
        Resolve::Config: Clone {
        let (srcs, endpoints) = config.take();
        let params: HashMap<Resolve::Origin, Ctx::OutNegoParam> = endpoints
            .into_iter()
            .map(|endpoint| endpoint.take())
            .collect();
        let addrs = Resolve::create(ctx, addrs_config.clone(),
                                    params.keys().cloned())
            .map_err(|err| StreamSelectorConnectionCreateError::Addrs {
                err: err
            })?;
        let mut channels = Vec::with_capacity(srcs.len());

        for src in srcs {
            let id = ctx.channel_id(&src)
                .ok_or(StreamSelectorConnectionCreateError::BadName {
                    name: src
                })?;

            channels.push(id);
        }

        Ok(ThreadedStreamSelectorConnections {
            ctx: PhantomData,
            channels: channels,
            addrs: Mutex::new(addrs),
            params: params
        })
    }

    fn handle_refresh_params(
        &self,
        params: Vec<(Ctx::ChannelID, Ctx::Param)>,
        refresh_channels_when: Option<Instant>
    ) -> Result<
        RetryResult<(
            Vec<(Ctx::Addr, Ctx::OutNegoParam)>,
            Vec<(Ctx::ChannelID, Ctx::Param)>,
            Option<Instant>
        )>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>
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
        let addrs: Vec<(Resolve::Addr, Ctx::OutNegoParam)> = addrs
            .flat_map(|(addr, endpoint, _)| {
                if let Some(param) = self.params.get(&endpoint) {
                    Some((addr, param.clone()))
                } else {
                    error!(target: "stream-selector-connections",
                           "no parameter entry for {}",
                           endpoint);

                    None
                }
            })
            .collect();

        let refresh_when = match (refresh_channels_when, refresh_addrs_when) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, out) => out,
            (out, None) => out
        };

        Ok(RetryResult::Success((addrs, params, refresh_when)))
    }

    fn get_refresh(
        &self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<(
            Vec<(Ctx::Addr, Ctx::OutNegoParam)>,
            Vec<(Ctx::ChannelID, Ctx::Param)>,
            Option<Instant>
        )>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>
    > {
        let (params, refresh_channels_when) = match ctx
            .params(&mut (), self.channels.iter().cloned())
            .map_err(|err| ThreadedStreamSelectorError::Param { err: err })?
        {
            // Pass through retries.
            RetryResult::Retry(when) => return Ok(RetryResult::Retry(when)),
            RetryResult::Success(res) => res
        };
        let params: Vec<(Ctx::ChannelID, Ctx::Param)> = params.collect();

        self.handle_refresh_params(params, refresh_channels_when)
    }

    #[inline]
    fn stream(
        &self,
        ctx: &mut Ctx,
        channel: &Ctx::ChannelID,
        addr: &Ctx::Addr,
        param: &Ctx::Param,
        origin: &Ctx::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Ctx::Stream>,
            bool,
            Option<Instant>
        )>,
        Ctx::ReqStreamError
    >
    {
        ctx.req_stream(&mut (), channel, param, addr, origin)
    }
}

impl<Epochs, Ctx> StreamSelectorState<Epochs, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send
{
    fn create(
        config: FarSchedulerConfig,
        retry: Retry,
        epochs: Epochs
    ) -> Result<Self, RefreshError> {
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
    ) -> Result<Self, RefreshError> {
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
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>,
            Ctx::OutNegoParam,
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
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>,
            Ctx::OutNegoParam,
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
        channel: Ctx::ChannelID,
        param: Ctx::Param,
        party_addr: Ctx::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    > {
        self.sched.success_id(id)
    }

    // XXX Need to clear out streams that have failed with an error
    // indicating the stream is no longer viable.

    /// Report a success for a given stream.
    fn failure(
        &mut self,
        channel: Ctx::ChannelID,
        param: Ctx::Param,
        party_addr: Ctx::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    > {
        self.sched.failure_id(id)
    }

    fn handle_selected<Resolve>(
        &mut self,
        ctx: &mut Ctx,
        connections: &[ThreadedStreamSelectorConnections<Resolve, Ctx>],
        stream_id: StreamID<
            Ctx::Addr,
            ConnChannelID<Ctx::ChannelID>,
            Ctx::Param
        >,
        origin: Ctx::OutNegoParam,
        dense_id: DenseItemID<Epochs::Item>
    ) -> Result<
        RetryResult<(
            DenseItemID<Epochs::Item>,
            bool,
            Option<Ctx::Stream>,
        )>,
        ReportError<
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    >
    where
        Resolve: Addrs<Addr = Ctx::Addr>,
        Resolve::Origin: Clone + Display + Eq + Hash
    {
        let (party_addr, ConnChannelID { conn_idx, channel }, param) =
            stream_id.take();

        let out = match &mut self.streams[dense_id.idx()] {
            // If the stream already exists, then just return it.
            StreamEntry {
                stream: Some(stream),
                ..
            } => {
                trace!(target: "stream-selector",
                       "stream for {} over channel {} ({}) already exists",
                       party_addr, channel, param);

                Ok(RetryResult::Success((Some(stream.clone()), false, None)))
            }
            // If the stream does not exist, create it.
            StreamEntry { stream, .. } => match connections[conn_idx.0].stream(
                ctx,
                &channel,
                &party_addr,
                &param,
                &origin
            ) {
                Ok(val) => Ok(val.flat_map(move |(newstream, refresh, when)| {
                    *stream = newstream.clone();

                    RetryResult::Success((newstream, refresh, when))
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
        }?;

        if matches!(out, RetryResult::Success((None, _, _))) {
            self.sched.set_active_id(&dense_id, false)?
        }

        Ok(out.map(|(stream, refresh, _)| (dense_id, refresh, stream)))
    }

    fn do_select<Resolve>(
        &mut self,
        ctx: &mut Ctx,
        connections: &[ThreadedStreamSelectorConnections<Resolve, Ctx>]
    ) -> Result<
        RetryIndefResult<(
            DenseItemID<Epochs::Item>,
            bool,
            Option<Ctx::Stream>,
        )>,
        StreamSelectorSelectError<
            Resolve::AddrsError,
            Ctx::ParamError,
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    >
    where
        Resolve: Addrs<Addr = Ctx::Addr>,
        Resolve::Origin: Clone + Display + Eq + Hash
    {
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
                    .map_err(|err| StreamSelectorSelectError::Report {
                        err: err
                    })
                    .map(RetryIndefResult::from)
            })
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Ctx::Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Ctx::Stream, SelectorStreamError<Epochs::Item>> {
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
    ) -> Result<Ctx::Stream, SelectorStreamError<Epochs::Item>> {
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

impl<Epochs, Party, Ctx>
    StreamReporter<Party, StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
                   Ctx::Stream, Ctx>
    for StreamSelectorState<Epochs, Ctx>
where
    Epochs: Iterator,
    Epochs::Item: Clone + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Ctx: StreamReporter<Party, StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
                        Ctx::Stream, ()>
{
    type ReportStreamError = StreamSelectorReportError<Ctx::ReportStreamError>;

    fn report_stream(
        &mut self,
        ctx: &mut Ctx,
        party: &Party,
        stream_id: StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
        stream: Ctx::Stream
    ) -> Result<Option<Ctx::Stream>, Self::ReportStreamError> {
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

                    match ctx
                        .report_stream(&mut (), party, stream_id.clone(),
                                       stream.clone())
                        .map_err(|err| StreamSelectorReportError::Report {
                            err: err
                        })? {
                        Some(stream) => {
                            trace!(target: "stream-selector",
                                   "inner reporter already had stream for {}",
                                   stream_id);

                            self.streams[idx.0].stream = Some(stream.clone());

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
}

impl<Epochs, Resolve, Party, Ctx>
    StreamReporter<Party, StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
                   Ctx::Stream, Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash,
    Ctx: StreamReporter<Party, StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
                        Ctx::Stream, ()>
{
    type ReportStreamError =
        WithMutexPoison<StreamSelectorReportError<Ctx::ReportStreamError>>;

    fn report_stream(
        &mut self,
        ctx: &mut Ctx,
        party: &Party,
        id: StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
        stream: Ctx::Stream
    ) -> Result<Option<Ctx::Stream>, Self::ReportStreamError> {
        self.state
            .write()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .report_stream(ctx, party, id, stream)
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }
}

impl<Epochs, Resolve, Ctx> Clone
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Default,
    Ctx: Channels<()>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send
{
    fn clone(&self) -> Self {
        StreamSelector {
            refresh_when: self.refresh_when.clone(),
            connections: self.connections.clone(),
            state: self.state.clone()
        }
    }
}

impl<Epochs, Resolve, Ctx> StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
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
        config: PartyConfig<
            Resolve::Config,
            Epochs::Config,
            String,
            EndpointConfig
        >
    ) -> Result<
        Self,
        StreamSelectorCreateError<
            Resolve::CreateError,
            Epochs::CreateError
        >
    >
    where
        EndpointConfig: OutboundEndpointConfig<Resolve::Origin,
                                               Ctx::OutNegoParam>,
        Resolve: AddrsCreate<Ctx>,
        Resolve::Config: Clone + Default {
        let (scheduler, resolver, epochs, retry, size_hint, connections) =
            config.take();
        let epochs = Epochs::create(epochs)
            .map_err(|err| StreamSelectorCreateError::Epochs { err: err })?;
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
            refresh_when: Arc::new(RwLock::new(Some(now))),
        })
    }

    fn get_refreshes(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<
        (
            Vec<(
                ConnectionsIdx,
                Vec<(Ctx::Addr, Ctx::OutNegoParam)>,
                Vec<(Ctx::ChannelID, Ctx::Param)>
            )>,
            Option<Instant>,
            Option<Instant>,
            usize
        ),
        ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>
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
            Vec<(Ctx::Addr, Ctx::OutNegoParam)>,
            Vec<(Ctx::ChannelID, Ctx::Param)>
        )>,
        size_hint: usize
    ) -> Vec<(
        StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>,
        Ctx::OutNegoParam
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
        ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>
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

    /// Report a success for a given stream.
    #[inline]
    pub fn success(
        &mut self,
        channel: Ctx::ChannelID,
        param: Ctx::Param,
        party_addr: Ctx::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
        channel: Ctx::ChannelID,
        param: Ctx::Param,
        party_addr: Ctx::Addr
    ) -> Result<
        (),
        StreamSelectorReportError<
            ReportError<
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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
        ctx: &mut Ctx,
    ) -> Result<
        RetryIndefResult<(Ctx::Stream, DenseItemID<Epochs::Item>)>,
        StreamSelectorSelectError<
            Resolve::AddrsError,
            Ctx::ParamError,
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    > {
        let res = self.state.write()
            .map_err(|_| StreamSelectorSelectError::MutexPoison)?
            .do_select(ctx, &self.connections)?;

        res.flat_map_ok(|(id, refresh, stream)| {
            // XXX what should we do with this time?
            let _ = if refresh {
                let now = Instant::now();

                match self.do_refresh(ctx, now)
                    .map_err(|err| StreamSelectorSelectError::Selector {
                        err: err
                    })? {
                    RetryResult::Success(when) => when,
                    RetryResult::Retry(when) => Some(when)
                }
            } else {
                None
            };

            match stream {
                Some(stream) => Ok(RetryIndefResult::Success((stream, id))),
                None => Ok(RetryIndefResult::Indef(()))
            }
        })
    }

    fn batch_stream(
        &self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Ctx::Stream as PushStream<Ctx>>::BatchID
        >
    ) -> Result<Ctx::Stream, SelectorStreamError<Epochs::Item>> {
        match self.state.read() {
            Ok(guard) => guard.batch_stream(batch),
            Err(_) => Err(SelectorStreamError::MutexPoison)
        }
    }

    fn dense_id_stream(
        &self,
        id: &DenseItemID<Epochs::Item>
    ) -> Result<Ctx::Stream, SelectorStreamError<Epochs::Item>> {
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

impl<Addrs, Param> RecoverableError
    for ThreadedStreamSelectorError<Addrs, Param>
where Addrs: Debug + Display + ScopedError,
      Param: Debug + Display + ScopedError
{
    type Completable = Infallible;
    type Permanent = ThreadedStreamSelectorError<Addrs, Param>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<Epoch> RecoverableError for SelectorStreamError<Epoch>
where
    Epoch: Debug + Display
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

impl<Epoch, Err> RecoverableError for SelectorBatchError<Epoch, Err>
where
    Err: RecoverableError,
    Epoch: Debug + Display
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

impl<Select, Parties, Stream, Epoch> RecoverableError
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: RecoverableError,
    Stream: RecoverableError,
    Parties: Clone,
    Epoch: Clone + Debug
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

impl<Addrs, Param, StreamID> RecoverableError
    for StreamSelectorSelectError<Addrs, Param, StreamID>
where
    Param: Debug + Display + ScopedError,
    Addrs: Debug + Display + ScopedError,
    StreamID: Debug + Display
{
    type Completable = Infallible;
    type Permanent = Self;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<Epochs, Resolve, Ctx> PushStream<Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type BatchID = StreamSelectorBatch<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::BatchID
    >;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type CancelBatchRetry = <Ctx::Stream as PushStream<Ctx>>::CancelBatchRetry;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type FinishBatchRetry = <Ctx::Stream as PushStream<Ctx>>::FinishBatchRetry;
    type ReportError = SelectorReportFailureError<
        Epochs::Item,
        StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>,
        <Ctx::Stream as PushStream<Ctx>>::ReportError
    >;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;

    #[inline]
    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
        Ctx::Stream::empty_flags_with_capacity(size)
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
        err: <Self::FinishBatchError as RecoverableError>::Completable
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
        err: <Self::CancelBatchError as RecoverableError>::Completable
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

impl<Epochs, Resolve, Ctx> PushStreamReportError<DenseItemID<Epochs::Item>>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type ReportError = StreamSelectorReportError<
        ReportError<
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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

impl<Epochs, Resolve, Ctx, Error> PushStreamReportError<Error>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash,
    Error: ErrorReportInfo<DenseItemID<Epochs::Item>>
{
    type ReportError = StreamSelectorReportError<
        ReportError<
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
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

impl<Epochs, Resolve, Ctx, Error>
    PushStreamReportBatchError<
        Error,
        StreamSelectorBatch<
            Epochs::Item,
            <Ctx::Stream as PushStream<Ctx>>::BatchID
        >
    > for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash,
{
    type ReportBatchError = StreamSelectorReportError<
        ReportError<
            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
        >
    >;

    fn report_error_with_batch(
        &mut self,
        batch: &StreamSelectorBatch<
            Epochs::Item,
            <Ctx::Stream as PushStream<Ctx>>::BatchID
        >,
        _error: &Error
    ) -> Result<(), Self::ReportBatchError> {
        self.report_error(&batch.stream)
    }
}

impl<Epochs, Resolve, Ctx> StreamRefresh<Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type RefreshRetry = Instant;
    type RefreshError = ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>;

    fn refresh(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Ctx::ParamError>
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

    fn retry_refresh(
        &mut self,
        ctx: &mut Ctx,
        when: Self::RefreshRetry
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError> {
        if when < Instant::now() {
            self.refresh(ctx)
        } else {
            Ok(RetryResult::Retry(when))
        }
    }

    fn complete_refresh(
        &mut self,
        ctx: &mut Ctx,
        _errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<RetryResult<Option<Instant>, Self::RefreshRetry>,
                Self::RefreshError> {
        error!(target: "stream-selector",
               "should never call complete_refresh");

        self.refresh(ctx)
    }
}

impl<Msg, Epochs, Resolve, Ctx> PushStreamAdd<Msg, Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + PushStreamAdd<Msg, Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStreamAdd<Msg, Ctx>>::AddError
    >;
    type AddRetry = <Ctx::Stream as PushStreamAdd<Msg, Ctx>>::AddRetry;

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
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.batch_stream(batch)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .complete_add(ctx, flags, msg, &batch.batch_id, err)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }
}

impl<Epochs, Resolve, Ctx> PushStreamPartyID
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + PushStreamPartyID + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type PartyID = <Ctx::Stream as PushStreamPartyID>::PartyID;
}

impl<Epochs, Resolve, Ctx> PushStreamShared<Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + PushStreamShared<Ctx> + Send,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type IndefParties = <Ctx::Stream as PushStreamShared<Ctx>>::IndefParties;
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Ctx::Stream as PushStreamShared<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry =
        <Ctx::Stream as PushStreamShared<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            Vec<<Ctx::Stream as PushStreamPartyID>::PartyID>,
            <Ctx::Stream as PushStreamShared<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Ctx::Stream as PushStreamPartyID>::PartyID>,
        <Ctx::Stream as PushStreamShared<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Ctx::Stream as PushStreamShared<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            Vec<<Ctx::Stream as PushStreamPartyID>::PartyID>,
            <Ctx::Stream as PushStreamShared<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        Vec<<Ctx::Stream as PushStreamPartyID>::PartyID>,
        <Ctx::Stream as PushStreamShared<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Ctx::Stream as PushStreamShared<Ctx>>::StartBatchStreamBatches;
    type BatchPartiesIter =
        <Ctx::Stream as PushStreamShared<Ctx>>::BatchPartiesIter;
    type BatchPartiesError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStreamShared<Ctx>>::BatchPartiesError
    >;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Ctx::Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Ctx::Stream::empty_batches_with_capacity(size)
    }

    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError> {
        self.batch_stream(batch_id)
            .map_err(|err| SelectorBatchError::Stream { err: err })?
            .batch_parties(&batch_id.batch_id)
            .map_err(|err| SelectorBatchError::Batch { batch: err })
    }

    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Option<Self::IndefParties>>,
                Self::SelectError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        // Try to select a stream.
        match self.select_stream(ctx) {
            // We succeeded, now create a batch on that stream.
            Ok(RetryIndefResult::Success((mut stream, id))) => {
                selections.id = Some(id.clone());

                Ok(stream
                    .select(ctx, &mut selections.inner, parties)
                    .map_err(|err| SelectorBatchError::Batch {
                        batch: SelectorBatchSelectError::Stream {
                            selected: id.clone(),
                            stream: err
                        }
                    })?
                    .map_retry(|retry| SelectorBatchSelectError::Stream {
                        selected: id,
                        stream: retry
                    }))
            }
            // We got a retry for selecting the stream.
            Ok(RetryIndefResult::Retry(retry)) => {
                Ok(RetryIndefResult::Retry(SelectorBatchSelectError::Select {
                    parties: parties.cloned().collect(),
                    select: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) =>
                Ok(RetryIndefResult::Indef(None)),
            Err(err) => Err(SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.cloned().collect(),
                    select: err
                }
            })
        }
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Option<Self::IndefParties>>,
                Self::SelectError> {
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

                Ok(stream
                   .retry_select(ctx, &mut selections.inner, retry)
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

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Self::IndefParties>,
                Self::SelectError> {
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

                Ok(stream
                    .complete_select(ctx, &mut selections.inner, err)
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
        err: <Self::CreateBatchError as RecoverableError>::Completable
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
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Option<Self::IndefParties>>,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        // Try to select a stream.
        match self.select_stream(ctx) {
            // We succeeded, now create a batch on that stream.
            Ok(RetryIndefResult::Success((mut stream, id))) => Ok(stream
                .start_batch(ctx, parties)
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })?
                .map_retry(|retry| SelectorBatchSelectError::Stream {
                    selected: id.clone(),
                    stream: retry
                })
                .map(|batch_id| StreamSelectorBatch {
                    stream: id,
                    batch_id: batch_id
                })),
            // We got a retry for selecting the stream.
            Ok(RetryIndefResult::Retry(retry)) => {
                Ok(RetryIndefResult::Retry(SelectorBatchSelectError::Select {
                    parties: parties.cloned().collect(),
                    select: retry
                }))
            }
            Ok(RetryIndefResult::Indef(())) =>
                Ok(RetryIndefResult::Indef(None)),
            Err(err) => Err(SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    parties: parties.cloned().collect(),
                    select: err
                }
            })
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Option<Self::IndefParties>>,
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

                match stream.retry_start_batch(ctx, retry)
                    .map_err(|err| {
                        SelectorBatchError::Batch {
                            batch: SelectorBatchSelectError::Stream {
                                selected: selected.clone(),
                                stream: err
                            }
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(parties) =>
                        Ok(RetryIndefResult::Indef(parties))
                }
            }
        }
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Self::IndefParties>,
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(parties) =>
                        Ok(RetryIndefResult::Indef(parties))
                }
            }
        }
    }

    #[inline]
    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _err: <Self::StartBatchError as RecoverableError>::Permanent
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

impl<Epochs, Resolve, Ctx> PushStreamPrivate<Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStream<Ctx> + PushStreamPrivate<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = SelectionsError<
        SelectorBatchError<
            Epochs::Item,
            <Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
        >,
        ()
    >;
    type CreateBatchRetry =
        <Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry;
    type SelectError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError,
            Epochs::Item
        >
    >;
    type SelectRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as PushStreamPrivate<Ctx>>::SelectRetry,
        Epochs::Item
    >;
    type Selections = SelectorSelections<
        DenseItemID<Epochs::Item>,
        <Ctx::Stream as PushStreamPrivate<Ctx>>::Selections
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type StartBatchRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchRetry,
        Epochs::Item
    >;
    type StartBatchStreamBatches =
        <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches;

    #[inline]
    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SelectorSelections {
            inner: Ctx::Stream::empty_selections_with_capacity(size),
            id: None
        }
    }

    #[inline]
    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        Ctx::Stream::empty_batches_with_capacity(size)
    }

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        // Try to select a stream.
        self.select_stream(ctx)
            .map_err(|err| {
                SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Select {
                        parties: (),
                        select: err
                    }
                }
            })?
            .map_retry(|retry| SelectorBatchSelectError::Select {
                parties: (),
                select: retry
            })
            // Record the selection and descend.
            .flat_map_ok(|(mut stream, id)| {
                selections.id = Some(id.clone());

                Ok(stream.select(ctx, &mut selections.inner)
                   .map_err(|err| SelectorBatchError::Batch {
                       batch: SelectorBatchSelectError::Stream {
                           selected: id.clone(),
                           stream: err
                       }
                   })?
                   .map_retry(|retry| SelectorBatchSelectError::Stream {
                       selected: id,
                       stream: retry
                   }))
            })
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
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

                Ok(stream
                   .retry_select(ctx, &mut selections.inner, retry)
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

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
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

                Ok(stream
                   .complete_select(ctx, &mut selections.inner, err)
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
        err: <Self::CreateBatchError as RecoverableError>::Completable
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
        ctx: &mut Ctx,
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        // Try to select a stream.
        self.select_stream(ctx)
            .map_err(|err| {
                SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Select {
                        parties: (),
                        select: err
                    }
                }
            })?
            .map_retry(|retry| SelectorBatchSelectError::Select {
                parties: (),
                select: retry
            })
            .flat_map_ok(|(mut stream, id)| Ok(stream
                         .start_batch(ctx)
                         .map_err(|err| SelectorBatchError::Batch {
                             batch: SelectorBatchSelectError::Stream {
                                 selected: id.clone(),
                                 stream: err
                             }
                         })?
                         .map_retry(|retry| SelectorBatchSelectError::Stream {
                             selected: id.clone(),
                             stream: retry
                         })
                         .map(|batch_id| StreamSelectorBatch {
                             stream: id,
                             batch_id: batch_id
                         })))
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match retry {
            // We got a retry in the select phase; just restart the whole thing.
            SelectorBatchSelectError::Select { .. } => self
                .start_batch(ctx),
            // We got a retry once the stream was selected.
            SelectorBatchSelectError::Stream {
                selected,
                stream: retry
            } => {
                let mut stream = self
                    .dense_id_stream(&selected)
                    .map_err(|err| SelectorBatchError::Stream { err: err })?;

                match stream.retry_start_batch(ctx, retry)
                    .map_err(|err| {
                        SelectorBatchError::Batch {
                            batch: SelectorBatchSelectError::Stream {
                                selected: selected.clone(),
                                stream: err
                            }
                        }
                    })? {
                    // We created the batch, wrap it up and return it.
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(()) => Ok(RetryIndefResult::Indef(()))
                }
            }
        }
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(()) => Ok(RetryIndefResult::Indef(()))
                }
            }
        }
    }

    #[inline]
    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _err: <Self::StartBatchError as RecoverableError>::Permanent
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

impl<Epochs, Resolve, Ctx> LargeObjStream<Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx> + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type Frags = <Ctx::Stream as LargeObjStream<Ctx>>::Frags;
    type PushFragError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjStream<Ctx>>::PushFragError,
            Epochs::Item
        >
    >;
    type PushFragRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as LargeObjStream<Ctx>>::PushFragRetry,
        Epochs::Item
    >;
    type Parties = <Ctx::Stream as LargeObjStream<Ctx>>::Parties;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry>,
        Self::PushFragError
    > {
        // Try to select a stream.
        self.select_stream(ctx)
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
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry>,
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
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry>,
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

impl<H, Epochs, Resolve, Ctx> LargeObjOfferStream<H, Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    H: HashID,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjOfferStream<H, Ctx>
        + PushStream<Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type PushOfferError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjOfferStream<H, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type PushOfferRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as LargeObjOfferStream<H, Ctx>>::PushOfferRetry,
        Epochs::Item
    >;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry>,
        Self::PushOfferError
    > {
        // Try to select a stream.
        self.select_stream(ctx)
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
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry>,
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
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry>,
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

impl<Msg, Epochs, Resolve, Ctx> PushStreamPrivateSingle<Msg, Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + PushStreamPrivateSingle<Msg, Ctx> + Send,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as PushStreamPrivateSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectError<
                Resolve::AddrsError,
                Ctx::ParamError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        Instant,
        (),
        <Ctx::Stream as PushStreamPrivateSingle<Msg, Ctx>>::PushRetry,
        Epochs::Item
    >;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
    {
        // Try to select a stream.
        self.select_stream(ctx).map_err(|err| {
            SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: err,
                    parties: ()
                }
            }
        })?
        .map_retry(|retry| SelectorBatchSelectError::Select {
            select: retry,
            parties: ()
        })
        .flat_map_ok(|(mut stream, id)| Ok(stream
                     .push(ctx, msg)
                     .map_err(|err| SelectorBatchError::Batch {
                         batch: SelectorBatchSelectError::Stream {
                             selected: id.clone(),
                             stream: err
                         }
                     })?
                     .map_retry(|retry| SelectorBatchSelectError::Stream {
                         selected: id.clone(),
                         stream: retry
                     })
                     .map(|batch_id| StreamSelectorBatch {
                         stream: id,
                         batch_id: batch_id
                     })))
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(()) => Ok(RetryIndefResult::Indef(())),
                }
            }
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(()) => Ok(RetryIndefResult::Indef(())),
                }
            }
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
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
        err: <Self::CancelPushError as RecoverableError>::Completable
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

impl<Msg, Epochs, Resolve, Ctx> PushStreamSharedSingle<Msg, Ctx>
    for StreamSelector<Epochs, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream:
        Clone + PushStreamSharedSingle<Msg, Ctx> + PushStreamPartyID + Send,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type CancelPushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                StreamSelectorSelectError<
                    Resolve::AddrsError,
                    Ctx::ParamError,
                    StreamID<
                        Ctx::Addr,
                        ConnChannelID<Ctx::ChannelID>,
                        Ctx::Param
                    >
                >
            >,
            (),
            <Ctx::Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushError,
            Epochs::Item
        >
    >;
    type CancelPushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Ctx::Stream as PushStreamSharedSingle<Msg, Ctx>>::CancelPushRetry,
        Epochs::Item
    >;
    type PushError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            PartiesBatchError<
                Vec<Self::PartyID>,
                StreamSelectorSelectError<
                    Resolve::AddrsError,
                    Ctx::ParamError,
                    StreamID<
                        Ctx::Addr,
                        ConnChannelID<Ctx::ChannelID>,
                        Ctx::Param
                    >
                >
            >,
            (),
            <Ctx::Stream as PushStreamSharedSingle<Msg, Ctx>>::PushError,
            Epochs::Item
        >
    >;
    type PushRetry = SelectorBatchSelectError<
        SelectorStartRetry<Self::PartyID>,
        (),
        <Ctx::Stream as PushStreamSharedSingle<Msg, Ctx>>::PushRetry,
        Epochs::Item
    >;

    fn push<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I,
        msg: &Msg
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Option<Self::IndefParties>>,
                Self::PushError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        // Try to select a stream.
        match self.select_stream(ctx) {
            // We succeeded, now create a batch on that stream.
            Ok(RetryIndefResult::Success((mut stream, id))) => Ok(stream
                .push(ctx, parties, msg)
                .map_err(|err| SelectorBatchError::Batch {
                    batch: SelectorBatchSelectError::Stream {
                        selected: id.clone(),
                        stream: err
                    }
                })?
                .map_retry(|retry| SelectorBatchSelectError::Stream {
                    selected: id.clone(),
                    stream: retry
                })
                .map(|batch_id| StreamSelectorBatch {
                    stream: id,
                    batch_id: batch_id
                })),
            // We got a retry for selecting the stream.
            Ok(RetryIndefResult::Retry(retry)) => {
                Ok(RetryIndefResult::Retry(SelectorBatchSelectError::Select {
                    select: SelectorStartRetry {
                        when: retry,
                        parties: parties.cloned().collect()
                    },
                    parties: ()
                }))
            }
            Ok(RetryIndefResult::Indef(())) =>
                Ok(RetryIndefResult::Indef(None)),
            Err(err) => Err(SelectorBatchError::Batch {
                batch: SelectorBatchSelectError::Select {
                    select: PartiesBatchError::new(parties.cloned().collect(),
                                                   err),
                    parties: ()
                }
            })
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Option<Self::IndefParties>>,
                Self::PushError>
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(parties) =>
                        Ok(RetryIndefResult::Indef(parties)),
                }
            }
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::PushRetry,
                                 Option<Self::IndefParties>>,
                Self::PushError>
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
                    RetryIndefResult::Success(batch_id) => {
                        Ok(RetryIndefResult::Success(StreamSelectorBatch {
                            stream: selected,
                            batch_id: batch_id
                        }))
                    }
                    // We have to retry again.
                    RetryIndefResult::Retry(retry) =>
                        Ok(RetryIndefResult::Retry(
                            SelectorBatchSelectError::Stream {
                                selected: selected,
                                stream: retry
                            }
                        )),
                    RetryIndefResult::Indef(parties) =>
                        Ok(RetryIndefResult::Indef(parties)),
                }
            }
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
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
        err: <Self::CancelPushError as RecoverableError>::Completable
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

impl<Addrs> Display for StreamSelectorConnectionCreateError<Addrs>
where
    Addrs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorConnectionCreateError::Addrs { err } => err.fmt(f),
            StreamSelectorConnectionCreateError::BadName { name } =>
                write!(f, "no such channel {}", name)
        }
    }
}

impl<Addrs, Epochs> Display
    for StreamSelectorCreateError<Addrs, Epochs>
where
    Addrs: Display,
    Epochs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StreamSelectorCreateError::Connection { err } => err.fmt(f),
            StreamSelectorCreateError::Refresh { err } => write!(f, "{}", err),
            StreamSelectorCreateError::Epochs { err } => err.fmt(f)
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
            StreamSelectorError::Refresh { err } => write!(f, "{}", err)
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
            ThreadedStreamSelectorError::Refresh { err } => {
                write!(f, "{}", err)
            }
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
            StreamSelectorSelectError::Select { err } => write!(f, "{}", err),
            StreamSelectorSelectError::Report { err } => err.fmt(f),
            StreamSelectorSelectError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl From<&'_ StreamsIdx> for usize {
    #[inline]
    fn from(val: &StreamsIdx) -> usize {
        val.0
    }
}

impl From<StreamsIdx> for usize {
    #[inline]
    fn from(val: StreamsIdx) -> usize {
        val.0
    }
}

impl From<usize> for StreamsIdx {
    #[inline]
    fn from(val: usize) -> StreamsIdx {
        StreamsIdx(val)
    }
}

impl Display for StreamsIdx {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "{}", self.0)
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

impl<Select, Parties, Stream, Epoch> Debug
    for SelectorBatchSelectError<Select, Parties, Stream, Epoch>
where
    Select: Debug,
    Stream: Debug,
    Epoch: Debug
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SelectorBatchSelectError::Select { select, .. } => {
                write!(f, "Select {{ select: {:?} }}", select)
            }
            SelectorBatchSelectError::Stream {
                stream, selected, ..
            } => write!(
                f,
                "Stream {{ stream: {:?}, selected: {:?} }}",
                stream, selected
            )
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
