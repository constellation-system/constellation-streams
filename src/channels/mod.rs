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

//! Functionality for obtaining raw streams from a set of channels.
//!
//! This module provides traits that define the abstractions for
//! creating raw streams from a set of channels.  This provides the
//! means for obtaining the foundational streams from which all other
//! stream abstractions are then built.
//!
//! This module also provides [SharedPrivateChannels] and related types,
//! which can represent a combination of both shared (true multicast)
//! and private (unicast) streams in a single type.
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::Empty;
use std::iter::FusedIterator;
use std::iter::empty;
use std::iter::once;
use std::net::SocketAddr;
use std::time::Instant;
use std::vec::IntoIter;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashID;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
use constellation_common::unix::UnixSocketAddr;
use log::error;
use mio::Token;

use crate::error::ErrorReportInfo;
use crate::large_obj::LargeObjID;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::Parties;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;

pub mod test;

/// Trait for determining whether a given channel parameter can pair
/// with a given endpoint address.
///
/// This is used in multiplexing to determine whether to pair off
/// parameters and counterparty addresses.
pub trait ChannelParam<Addr>: Clone + Debug + Display + Eq + Hash {
    /// Check whether this parameter set can accept the address `addr`.
    fn accepts_addr(
        &self,
        addr: &Addr
    ) -> bool;
}

/// Trait for sets of channels that can produce private streams.
///
/// Typically, this trait will be implemented by a structure that acts
/// as a channel registry, binding individual channel configurations
/// to names, and then using the configurations to create private
/// streams.
///
/// The process for obtaining streams is designed to support the needs
/// of a variety of types of unreliable datagram channels.  The
/// process goes as follows:
///
///  1. A channel undergoes a setup step, which produces a "parameter set". This
///     is often a binding address.  Depending on the nature of the channel,
///     multiple concrete parameter sets may result from the setup process.
///
///  2. Parameter sets are checked for compatibility with counterparty addresses
///     using the [ChannelParam] trait.
///
///  3. A parameter set and a compatible counterparty address are used to obtain
///     an individual stream from the channel, which may involve some degree of
///     protocol negotiation.
///
///  4. An individual stream is returned, providing a connection over the given
///     channel to the counterparty.  The exact nature of this stream and its
///     API depend on the details of the underlying channels.
pub trait Channels<Ctx> {
    /// Type of channel IDs.
    type ChannelID: Clone + Debug + Display + Eq + Hash;
    /// Type of parameters for obtaining streams.
    ///
    /// Channel parameters are used to obtain individual streams from
    /// a channel.  These can be checked for compatibility with an
    /// address using [accepts_addr](ChannelParam::accepts_addr).
    type Param: ChannelParam<Self::Addr>;
    /// Type of iterator on parameters.
    ///
    /// This provides both the ID of the originating channel, and the
    /// channel parameter.
    type ParamsIter<I>: Iterator<
        Item = (
            Self::ChannelID,
            RetryResult<(Vec<Self::Param>, Option<Instant>)>
        )
    >
    where
        I: Iterator<Item = Self::ChannelID>;
    /// Type of errors that can occur when obtaining parameters.
    type ParamsError: Debug + Display + ScopedError;
    /// Outbound negotiator parameter.
    ///
    /// This usually corresponds to verify endpoints for TLS streams.
    type OutNegoParam;
    /// Type of counterparty addresses to which to connect.
    type Addr: Clone + Debug + Display + Eq + Hash;
    /// Type of raw streams obtained from parameters.
    type Stream;
    /// Type of errors that can occur when obtaining flows from a parameter.
    type ReqStreamError: Debug + Display + ScopedError;

    /// Request a stream for a given endpoint.
    ///
    /// This will attempt any negotiations necessary to establish the
    /// stream.  If negotiations conclude immediately, then the stream
    /// will be returned.  Otherwise, it will be returned by a
    /// subsequent call to [listen](Channels::listen).  Subsequent
    /// calls to this function with the same `endpoint` will return an
    /// error.
    ///
    /// This may also attempt to refresh the set of addresses, and
    /// will return a new set if this happens.
    ///
    /// Streams obtained from this function must eventually be shut
    /// down with [shutdown_stream](Channels::shutdown_stream).
    ///
    /// # Parameter
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `channel`: The channel ID on which to create the stream.
    ///
    /// - `param`: The channel parameter to use.  These are obtained from this
    ///   function, or from [params](Channels::params).
    ///
    /// - `endpoint`: The counterparty address.
    ///
    /// - `nego_param`: The outbound negotiation parameter to use.
    ///
    /// # Return Value
    ///
    /// A triple containing three values in order:
    ///
    /// 1. The authenticated session, if there is one.
    ///
    /// 1. If a refresh occurred, a [Vec] containing
    ///    [ChannelID](Channels::ChannelID)s that have been refreshed.
    ///
    /// 1. When the next refresh occurs.
    fn req_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        endpoint: &Self::Addr,
        nego_param: &Self::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Self::Stream>,
            Option<Vec<Self::Param>>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    >;

    /// Obtain the current set of all channel parameters, and the time
    /// at which they will need to be refreshed.
    ///
    /// If parameters never need to be refreshed again, `None` will be
    /// returned.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `channels`: [Iterator] for channel IDs for which to get parameters.
    fn params<I>(
        &mut self,
        ctx: &mut Ctx,
        channels: I
    ) -> Result<Self::ParamsIter<I>, Self::ParamsError>
    where
        I: Iterator<Item = Self::ChannelID>;

    /// Get a channel's ID from its name.
    ///
    /// # Parameters
    ///
    /// - `name`: The channel's text name.
    ///
    /// # Return Value
    ///
    /// The channel's ID, or `None` if no such channel exists.
    fn channel_id(
        &self,
        name: &str
    ) -> Option<Self::ChannelID>;
}

pub trait ChannelsListen<Ctx>: Channels<Ctx> {
    /// Type of iterators indicating which streams have available messoges.
    ///
    /// This produces a triple containing the following items:
    ///
    /// 1. The endpoint address of the incoming messages.
    ///
    /// 1. The ID of the channel on which messages were received.
    ///
    /// 1. The channel parameter for which messages were received.
    type EndpointIter: Iterator<
        Item = (Self::Addr, Self::ChannelID, Self::Param)
    >;
    /// Type of iterators over new incoming streams (sessions).
    ///
    /// This produces a 4-tuple containing the following items:
    ///
    /// 1. The endpoint address of the incoming session.
    ///
    /// 1. The ID of the channel on which this session was received.
    ///
    /// 1. The channel parameter for which this session was received.
    ///
    /// 1. The [Stream](Channels::Stream) representing the session.
    type StreamIter: Iterator<
        Item = (Self::Addr, Self::ChannelID, Self::Param, Self::Stream)
    >;
    /// Type of errors that can occur when
    /// [listen](ChannelsListen::listen)ing.
    type ListenError: Debug + Display + ScopedError;

    /// Listen for new messages and/or incoming sessions.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `tokens`: The set of live [Token]s.
    ///
    /// # Return Value
    ///
    /// A tuple containing four elements;
    ///
    /// 1. An [Iterator](ChannelsListen::StreamIter) of new incoming sessions.
    ///
    /// 1. An [Iterator](ChannelsListen::EndpointsIter) containing endpoints for
    ///    existing sessions that have received new messages.
    ///
    /// 1. If a refresh occurred, a [Vec] containing
    ///    [ChannelID](Channels::ChannelID)s that have been refreshed.
    ///
    /// 1. If `Some`, then he earliest next time at which a `listen` should take
    ///    place, regardless of polling; if `None`, then the next listen should
    ///    take place as indicated by polling.
    fn listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Option<Vec<(Self::ChannelID, Option<Vec<Self::Param>>)>>,
            Option<Instant>
        )>,
        Self::ListenError
    >;
}

pub trait ChannelsShutdown<Ctx>: Channels<Ctx> + Sized {
    type ShutdownStreamError: Debug + Display + ScopedError;
    type ShutdownStreamRetry: RetryWhen;
    type ShutdownListenError: Debug + Display + ScopedError;

    /// Shut down a given stream.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `channel`: The channel ID on which to create the stream.
    ///
    /// - `param`: The channel parameter to use.  These are obtained from this
    ///   function, or from [params](Channels::params).
    ///
    /// - `session`: The session to shut down.
    ///
    /// # Return Value
    ///
    /// A pair containing the following:
    ///
    /// 1. A set of new [Param](Channels::Param) if a refresh was done.
    ///
    /// 1. When the next refresh occurs.
    fn shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        session: Self::Stream
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    >;

    fn retry_shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        retry: Self::ShutdownStreamRetry
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    >;

    /// Variant of [listen](ChannelsListen::listen) for shutting down.
    ///
    /// This is used to listen for input in the shutdown process.
    /// This will not generate new sessions, but will result in
    /// shutting down existing ones.  This will consume the `Channels`
    /// instance, returning it if the shutdown process needs to
    /// continue.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `tokens`: The set of live [Token]s.
    ///
    /// # Return Value
    ///
    /// - `None`: The shutdown process is complete.
    ///
    /// - `Some((self, None))`: The shutdown process is continuing, and
    ///   `shutdown_listen` should be called after polling returns more tokens.
    ///
    /// - `Some((self, Some(when)))`: The shutdwon process is continuing, and
    ///   `shutdown_listen` should be called at `when` at the latest, or after
    ///   polling returns more tokens.
    fn shutdown_listen(
        self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<Option<(Self, Option<Instant>)>, Self::ShutdownListenError>;
}

/// An implementation of [Channels] that is always empty.
///
/// This is primarily used for testing purposes.
pub struct NullChannels;

/// Channel ID type for [NullChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NullChannelsID;

/// Channel param type for [NullChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NullChannelsParam;

/// Address type for [NullChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NullChannelsAddr;

/// Implementation of [Channels] that combines private and shared channels.
///
/// This allows for shared and private channels to be handled through
/// the same [Channels] instance, and ultimately to create
/// [SharedPrivateChannelStream]s to handle both cases through a
/// single common interface.  This stream type contains destination
/// addresses for all shared streams, which it transparently adds into
/// all batches.  This allows both shared and private streams to be
/// treated as if they were private streams
///
/// This type can be used as the lowest level of a set of stream
/// combinators, allowing higher levels to select amongst various
/// channels (including shared ones), with this level handling the
/// creation of a batch and the addition of recipients as needed.
pub struct SharedPrivateChannels<Private, Shared> {
    /// The private channels source.
    private: Option<Private>,
    /// The shared channels source.
    shared: Option<Shared>
}

#[cfg(test)]
pub struct TestChannel {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SharedPrivateValue<Private, Shared> {
    /// ID for the private channels.
    Private {
        /// Private ID.
        private: Private
    },
    /// ID for the shared channels.
    Shared {
        /// Shared ID.
        shared: Shared
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SharedPrivateStreamIter<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateStream,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedStream,
    SharedIter
> where
    SharedAddr: Clone,
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam, PrivateStream)>,
    SharedIter:
        Iterator<Item = (SharedAddr, SharedID, SharedParam, SharedStream)> {
    /// Param iterator for the private channels.
    private: Option<PrivateIter>,
    /// Param iterator for the shared channels.
    shared: Option<SharedIter>
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SharedPrivateEndpointIter<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedIter
> where
    PrivateIter:
        FusedIterator + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam)>,
    SharedIter: Iterator<Item = (SharedAddr, SharedID, SharedParam)> {
    /// Param iterator for the private channels.
    private: Option<PrivateIter>,
    /// Param iterator for the shared channels.
    shared: Option<SharedIter>
}

/// Param iterator for [SharedPrivateChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SharedPrivateParamIter<
    PrivateID,
    PrivateParam,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedIter
> where
    PrivateIter: FusedIterator + Iterator<Item = (PrivateID, PrivateParam)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam)> {
    /// Param iterator for the private channels.
    private: Option<PrivateIter>,
    /// Param iterator for the shared channels.
    shared: Option<SharedIter>
}

/// Param error for [SharedPrivateChannels].
#[derive(Debug)]
pub enum SharedPrivateError<Private, Shared> {
    /// Param error for the private channels.
    Private {
        /// Private channel param error.
        err: Private
    },
    /// Param error for the shared channels.
    Shared {
        /// Shared channel param iterator.
        err: Shared
    },
    /// One of the options is shut down.
    Shutdown
}

/// Type of streams for [SharedPrivateChannels].
///
/// This implements the basic [PushStream], [PushStreamAdd], and
/// [PushStreamPrivate] traits that would be expected for *private*
/// channels.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SharedPrivateChannelStream<Private, Shared, Party> {
    /// Stream for the private channels.
    Private {
        /// Private channel stream.
        stream: Private
    },
    /// Stream for the shared channels.
    Shared {
        /// Shared channel stream.
        stream: Shared,
        /// Party to which messages are sent on the shared stream,
        party: Party
    }
}

/// Type of parties for [SharedPrivateChannelStream].
#[derive(Clone, Debug)]
pub enum SharedPrivateStreamParties<Private, Shared> {
    /// Parties for private channels.
    Private {
        /// Parties for private channels.
        parties: Private
    },
    /// Parties for shared channels.
    Shared {
        /// Parties for shared channels.
        parties: Shared
    }
}

/// Type of retry information for [SharedPrivateChannelStream].
#[derive(Clone, Debug)]
pub enum SharedPrivateStreamRetry<Private, Shared> {
    /// Private channels need to be retried.
    Private {
        /// Retry info for the private channels.
        retry: Private
    },
    /// Shared channels need to be retried.
    Shared {
        /// Retry info for the shared channels.
        retry: Shared
    }
}

/// Error for [SharedPrivateChannels].
#[derive(Debug)]
pub enum SharedPrivateMatchError<Private, Shared> {
    /// Stream error for the private channels.
    Private {
        /// Private channel stream error.
        err: Private
    },
    /// Stream error for the shared channels.
    Shared {
        /// Shared channel stream iterator.
        err: Shared
    },
    /// Mismatch between shared and private types.
    ///
    /// This should never happen.
    Mismatch,
    /// One of the options is shut down.
    Shutdown
}

/// Common structure for
/// [StartBatchStreamBatches](PushStreamPrivate::StartBatchStreamBatches),
/// [Selections](PushStreamPrivate::Selections) and
/// [StreamFlags](PushStream::StreamFlags) for
/// [SharedPrivateChannelStream].
#[derive(Clone)]
pub struct SharedPrivateStreamCaches<Private, Shared> {
    shared: Shared,
    private: Private
}

impl<Private, Shared> Default for SharedPrivateStreamCaches<Private, Shared>
where
    Private: Default,
    Shared: Default
{
    #[inline]
    fn default() -> Self {
        SharedPrivateStreamCaches {
            shared: Shared::default(),
            private: Private::default()
        }
    }
}

impl<Private, Shared> ScopedError for SharedPrivateMatchError<Private, Shared>
where
    Private: ScopedError,
    Shared: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            SharedPrivateMatchError::Private { err } => err.scope(),
            SharedPrivateMatchError::Shared { err } => err.scope(),
            SharedPrivateMatchError::Mismatch |
            SharedPrivateMatchError::Shutdown => ErrorScope::Unrecoverable
        }
    }
}

impl<Private, Shared> ScopedError for SharedPrivateError<Private, Shared>
where
    Private: ScopedError,
    Shared: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            SharedPrivateError::Private { err } => err.scope(),
            SharedPrivateError::Shared { err } => err.scope(),
            SharedPrivateError::Shutdown => ErrorScope::Unrecoverable
        }
    }
}

impl<Private, Shared> RetryWhen for SharedPrivateStreamRetry<Private, Shared>
where
    Private: RetryWhen,
    Shared: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            SharedPrivateStreamRetry::Private { retry } => retry.when(),
            SharedPrivateStreamRetry::Shared { retry } => retry.when()
        }
    }
}

impl<Private, Shared, T> ErrorReportInfo<T>
    for SharedPrivateError<Private, Shared>
where
    Private: ErrorReportInfo<T>,
    Shared: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        match self {
            SharedPrivateError::Private { err } => err.report_info(),
            SharedPrivateError::Shared { err } => err.report_info(),
            SharedPrivateError::Shutdown => None
        }
    }
}

impl<Private, Shared, T> ErrorReportInfo<T>
    for SharedPrivateMatchError<Private, Shared>
where
    Private: ErrorReportInfo<T>,
    Shared: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        match self {
            SharedPrivateMatchError::Private { err } => err.report_info(),
            SharedPrivateMatchError::Shared { err } => err.report_info(),
            SharedPrivateMatchError::Mismatch |
            SharedPrivateMatchError::Shutdown => None
        }
    }
}

impl ChannelParam<String> for String {
    fn accepts_addr(
        &self,
        addr: &String
    ) -> bool {
        self == addr
    }
}

impl<Ctx> Channels<Ctx> for NullChannels {
    type Addr = NullChannelsAddr;
    type ChannelID = NullChannelsID;
    type OutNegoParam = ();
    type Param = NullChannelsParam;
    type ParamsError = Infallible;
    type ParamsIter<I>
        = Empty<(
        NullChannelsID,
        RetryResult<(Vec<Self::Param>, Option<Instant>)>
    )>
    where
        I: Iterator<Item = Self::ChannelID>;
    type ReqStreamError = Infallible;
    type Stream = ();

    #[inline]
    fn params<I>(
        &mut self,
        _ctx: &mut Ctx,
        _channels: I
    ) -> Result<Self::ParamsIter<I>, Self::ParamsError>
    where
        I: Iterator<Item = Self::ChannelID> {
        Ok(empty())
    }

    #[inline]
    fn req_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        _endpoint: &Self::Addr,
        _nego_param: &Self::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Self::Stream>,
            Option<Vec<NullChannelsParam>>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        Ok(RetryResult::Success((Some(()), None, None)))
    }

    #[inline]
    fn channel_id(
        &self,
        _name: &str
    ) -> Option<Self::ChannelID> {
        None
    }
}

impl<Ctx> ChannelsListen<Ctx> for NullChannels {
    type EndpointIter = Empty<(Self::Addr, Self::ChannelID, Self::Param)>;
    type ListenError = Infallible;
    type StreamIter =
        Empty<(Self::Addr, Self::ChannelID, Self::Param, Self::Stream)>;

    #[inline]
    fn listen(
        &mut self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Option<Vec<(Self::ChannelID, Option<Vec<Self::Param>>)>>,
            Option<Instant>
        )>,
        Self::ListenError
    > {
        Ok(RetryResult::Success((empty(), empty(), None, None)))
    }
}

impl<Ctx> ChannelsShutdown<Ctx> for NullChannels {
    type ShutdownListenError = Infallible;
    type ShutdownStreamError = Infallible;
    type ShutdownStreamRetry = Infallible;

    #[inline]
    fn shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        _session: Self::Stream
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    > {
        Ok(RetryResult::Success((None, None)))
    }

    #[inline]
    fn retry_shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        _retry: Self::ShutdownStreamRetry
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    > {
        error!(target: "",
               "should never call retry_shutdown_stream");

        Ok(RetryResult::Success((None, None)))
    }

    #[inline]
    fn shutdown_listen(
        self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<Option<(Self, Option<Instant>)>, Self::ShutdownListenError>
    {
        Ok(None)
    }
}

impl<Private, Shared> SharedPrivateChannels<Private, Shared> {
    #[inline]
    pub fn new(
        private: Private,
        shared: Shared
    ) -> Self {
        SharedPrivateChannels {
            private: Some(private),
            shared: Some(shared)
        }
    }
}

impl<Private, Shared, Ctx> Channels<Ctx>
    for SharedPrivateChannels<Private, Shared>
where
    Private: Channels<Ctx>,
    Shared: Channels<Ctx>
{
    type Addr = SharedPrivateValue<Private::Addr, Shared::Addr>;
    type ChannelID = SharedPrivateValue<Private::ChannelID, Shared::ChannelID>;
    type OutNegoParam =
        SharedPrivateValue<Private::OutNegoParam, Shared::OutNegoParam>;
    type Param = SharedPrivateValue<Private::Param, Shared::Param>;
    type ParamsError =
        SharedPrivateError<Private::ParamsError, Shared::ParamsError>;
    type ParamsIter<I>
        = IntoIter<(
        Self::ChannelID,
        RetryResult<(Vec<Self::Param>, Option<Instant>)>
    )>
    where
        I: Iterator<Item = Self::ChannelID>;
    type ReqStreamError = SharedPrivateMatchError<
        Private::ReqStreamError,
        Shared::ReqStreamError
    >;
    type Stream = SharedPrivateChannelStream<
        Private::Stream,
        Shared::Stream,
        Shared::Addr
    >;

    fn params<I>(
        &mut self,
        ctx: &mut Ctx,
        channels: I
    ) -> Result<Self::ParamsIter<I>, Self::ParamsError>
    where
        I: Iterator<Item = Self::ChannelID> {
        let (_, hint) = channels.size_hint();
        let mut private_channels = match hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };
        let mut shared_channels = match hint {
            Some(hint) => Vec::with_capacity(hint),
            None => Vec::new()
        };

        for channel in channels {
            match channel {
                SharedPrivateValue::Shared { shared } => {
                    shared_channels.push(shared)
                }
                SharedPrivateValue::Private { private } => {
                    private_channels.push(private)
                }
            }
        }

        let private = self
            .private
            .as_mut()
            .ok_or(SharedPrivateError::Shutdown)?
            .params(ctx, private_channels.into_iter())
            .map_err(|err| SharedPrivateError::Private { err: err })?;
        let private: Vec<(
            Self::ChannelID,
            RetryResult<(Vec<Self::Param>, Option<Instant>)>
        )> = private
            .map(|(id, res)| {
                let res = res.map(|(vec, when)| {
                    let vec = vec
                        .into_iter()
                        .map(|param| SharedPrivateValue::Private {
                            private: param
                        })
                        .collect();

                    (vec, when)
                });

                (SharedPrivateValue::Private { private: id }, res)
            })
            .collect();
        let shared = self
            .shared
            .as_mut()
            .ok_or(SharedPrivateError::Shutdown)?
            .params(ctx, shared_channels.into_iter())
            .map_err(|err| SharedPrivateError::Shared { err: err })?;
        let out: Vec<(
            Self::ChannelID,
            RetryResult<(Vec<Self::Param>, Option<Instant>)>
        )> = private
            .into_iter()
            .chain(shared.map(|(id, res)| {
                let res = res.map(|(vec, when)| {
                    let vec = vec
                        .into_iter()
                        .map(|param| SharedPrivateValue::Shared {
                            shared: param
                        })
                        .collect();

                    (vec, when)
                });

                (SharedPrivateValue::Shared { shared: id }, res)
            }))
            .collect();

        Ok(out.into_iter())
    }

    fn req_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        endpoint: &Self::Addr,
        nego_param: &Self::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Self::Stream>,
            Option<Vec<Self::Param>>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        match (channel, param, endpoint, nego_param) {
            (
                SharedPrivateValue::Private { private: id },
                SharedPrivateValue::Private { private: param },
                SharedPrivateValue::Private { private: addr },
                SharedPrivateValue::Private {
                    private: nego_param
                }
            ) => Ok(self
                .private
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .req_stream(ctx, id, param, addr, nego_param)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(stream, refresh, when)| {
                    let stream = stream.map(|stream| {
                        SharedPrivateChannelStream::Private { stream: stream }
                    });
                    let refresh = refresh.map(|refresh| {
                        refresh
                            .into_iter()
                            .map(|param| SharedPrivateValue::Private {
                                private: param
                            })
                            .collect()
                    });

                    (stream, refresh, when)
                })),
            (
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateValue::Shared { shared: addr },
                SharedPrivateValue::Shared { shared: nego_param }
            ) => Ok(self
                .shared
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .req_stream(ctx, id, param, addr, nego_param)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(stream, refresh, when)| {
                    let stream = stream.map(|stream| {
                        SharedPrivateChannelStream::Shared {
                            stream: stream,
                            party: addr.clone()
                        }
                    });
                    let refresh = refresh.map(|refresh| {
                        refresh
                            .into_iter()
                            .map(|param| SharedPrivateValue::Shared {
                                shared: param
                            })
                            .collect()
                    });

                    (stream, refresh, when)
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    #[inline]
    fn channel_id(
        &self,
        name: &str
    ) -> Option<Self::ChannelID> {
        self.private
            .as_ref()
            .and_then(|private| {
                private
                    .channel_id(name)
                    .map(|id| SharedPrivateValue::Private { private: id })
            })
            .or_else(|| {
                self.shared.as_ref().and_then(|private| {
                    private
                        .channel_id(name)
                        .map(|id| SharedPrivateValue::Shared { shared: id })
                })
            })
    }
}

impl<Private, Shared, Ctx> ChannelsListen<Ctx>
    for SharedPrivateChannels<Private, Shared>
where
    Private: ChannelsListen<Ctx>,
    Shared: ChannelsListen<Ctx>,
    Private::EndpointIter: FusedIterator,
    Private::StreamIter: FusedIterator
{
    type EndpointIter = SharedPrivateEndpointIter<
        Private::ChannelID,
        Private::Param,
        Private::Addr,
        Private::EndpointIter,
        Shared::ChannelID,
        Shared::Param,
        Shared::Addr,
        Shared::EndpointIter
    >;
    type ListenError =
        SharedPrivateError<Private::ListenError, Shared::ListenError>;
    type StreamIter = SharedPrivateStreamIter<
        Private::ChannelID,
        Private::Param,
        Private::Addr,
        Private::Stream,
        Private::StreamIter,
        Shared::ChannelID,
        Shared::Param,
        Shared::Addr,
        Shared::Stream,
        Shared::StreamIter
    >;

    fn listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Option<Vec<(Self::ChannelID, Option<Vec<Self::Param>>)>>,
            Option<Instant>
        )>,
        Self::ListenError
    > {
        match (
            self.private
                .as_mut()
                .ok_or(SharedPrivateError::Shutdown)?
                .listen(ctx, tokens)
                .map_err(|err| SharedPrivateError::Private { err: err })?,
            self.shared
                .as_mut()
                .ok_or(SharedPrivateError::Shutdown)?
                .listen(ctx, tokens)
                .map_err(|err| SharedPrivateError::Shared { err: err })?
        ) {
            (
                RetryResult::Success((
                    private_streams,
                    private_addrs,
                    private_refresh,
                    private_when
                )),
                RetryResult::Success((
                    shared_streams,
                    shared_addrs,
                    shared_refresh,
                    shared_when
                ))
            ) => {
                let streams = SharedPrivateStreamIter {
                    private: Some(private_streams),
                    shared: Some(shared_streams)
                };
                let addrs = SharedPrivateEndpointIter {
                    private: Some(private_addrs),
                    shared: Some(shared_addrs)
                };
                let refresh = match (private_refresh, shared_refresh) {
                    (Some(private_refresh), Some(shared_refresh)) => {
                        let private_refresh =
                            private_refresh.into_iter().map(|(id, params)| {
                                let id =
                                    SharedPrivateValue::Private { private: id };
                                let params = params.map(|params| {
                                    params
                                        .into_iter()
                                        .map(|param| {
                                            SharedPrivateValue::Private {
                                                private: param
                                            }
                                        })
                                        .collect()
                                });

                                (id, params)
                            });
                        let shared_refresh =
                            shared_refresh.into_iter().map(|(id, params)| {
                                let id =
                                    SharedPrivateValue::Shared { shared: id };
                                let params = params.map(|params| {
                                    params
                                        .into_iter()
                                        .map(|param| {
                                            SharedPrivateValue::Shared {
                                                shared: param
                                            }
                                        })
                                        .collect()
                                });

                                (id, params)
                            });

                        Some(private_refresh.chain(shared_refresh).collect())
                    }
                    (Some(private_refresh), None) => Some(
                        private_refresh
                            .into_iter()
                            .map(|(id, params)| {
                                let id =
                                    SharedPrivateValue::Private { private: id };
                                let params = params.map(|params| {
                                    params
                                        .into_iter()
                                        .map(|param| {
                                            SharedPrivateValue::Private {
                                                private: param
                                            }
                                        })
                                        .collect()
                                });

                                (id, params)
                            })
                            .collect()
                    ),
                    (None, Some(shared_refresh)) => Some(
                        shared_refresh
                            .into_iter()
                            .map(|(id, params)| {
                                let id =
                                    SharedPrivateValue::Shared { shared: id };
                                let params = params.map(|params| {
                                    params
                                        .into_iter()
                                        .map(|param| {
                                            SharedPrivateValue::Shared {
                                                shared: param
                                            }
                                        })
                                        .collect()
                                });

                                (id, params)
                            })
                            .collect()
                    ),
                    _ => None
                };
                let when = next_retry(&private_when, &shared_when);

                Ok(RetryResult::Success((streams, addrs, refresh, when)))
            }
            // XXX these cases will break, because downstream will
            // only see half the addresses.  Solution is probably to
            // cache addresses on the channels.
            (
                RetryResult::Success((
                    private_streams,
                    private_addrs,
                    refresh,
                    private_when
                )),
                RetryResult::Retry(shared_when)
            ) => {
                let streams = SharedPrivateStreamIter {
                    private: Some(private_streams),
                    shared: None
                };
                let addrs = SharedPrivateEndpointIter {
                    private: Some(private_addrs),
                    shared: None
                };
                let refresh = refresh.map(|refresh| {
                    refresh
                        .into_iter()
                        .map(|(id, params)| {
                            let id =
                                SharedPrivateValue::Private { private: id };
                            let params = params.map(|params| {
                                params
                                    .into_iter()
                                    .map(|param| SharedPrivateValue::Private {
                                        private: param
                                    })
                                    .collect()
                            });

                            (id, params)
                        })
                        .collect()
                });
                let when =
                    Some(next_retry_definite(&private_when, &shared_when));

                Ok(RetryResult::Success((streams, addrs, refresh, when)))
            }
            (
                RetryResult::Retry(private_when),
                RetryResult::Success((
                    shared_streams,
                    shared_addrs,
                    refresh,
                    shared_when
                ))
            ) => {
                let streams = SharedPrivateStreamIter {
                    private: None,
                    shared: Some(shared_streams)
                };
                let addrs = SharedPrivateEndpointIter {
                    private: None,
                    shared: Some(shared_addrs)
                };
                let refresh = refresh.map(|refresh| {
                    refresh
                        .into_iter()
                        .map(|(id, params)| {
                            let id = SharedPrivateValue::Shared { shared: id };
                            let params = params.map(|params| {
                                params
                                    .into_iter()
                                    .map(|param| SharedPrivateValue::Shared {
                                        shared: param
                                    })
                                    .collect()
                            });

                            (id, params)
                        })
                        .collect()
                });
                let when =
                    Some(next_retry_definite(&shared_when, &private_when));

                Ok(RetryResult::Success((streams, addrs, refresh, when)))
            }
            (
                RetryResult::Retry(private_when),
                RetryResult::Retry(shared_when)
            ) => Ok(RetryResult::Retry(private_when.max(shared_when)))
        }
    }
}

impl<Private, Shared, Ctx> ChannelsShutdown<Ctx>
    for SharedPrivateChannels<Private, Shared>
where
    Private: ChannelsShutdown<Ctx>,
    Shared: ChannelsShutdown<Ctx>
{
    type ShutdownListenError = SharedPrivateError<
        Private::ShutdownListenError,
        Shared::ShutdownListenError
    >;
    type ShutdownStreamError = SharedPrivateMatchError<
        Private::ShutdownStreamError,
        Shared::ShutdownStreamError
    >;
    type ShutdownStreamRetry = SharedPrivateStreamRetry<
        Private::ShutdownStreamRetry,
        Shared::ShutdownStreamRetry
    >;

    fn shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        session: Self::Stream
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    > {
        match (channel, param, session) {
            (
                SharedPrivateValue::Private { private: id },
                SharedPrivateValue::Private { private: param },
                SharedPrivateChannelStream::Private { stream }
            ) => Ok(self
                .private
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .shutdown_stream(ctx, id, param, stream)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|(params, when)| {
                    let params = params.map(|params| {
                        params
                            .into_iter()
                            .map(|param| SharedPrivateValue::Private {
                                private: param
                            })
                            .collect()
                    });

                    (params, when)
                })),
            (
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateChannelStream::Shared { stream, .. }
            ) => Ok(self
                .shared
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .shutdown_stream(ctx, id, param, stream)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|(params, when)| {
                    let params = params.map(|params| {
                        params
                            .into_iter()
                            .map(|param| SharedPrivateValue::Shared {
                                shared: param
                            })
                            .collect()
                    });

                    (params, when)
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn retry_shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        retry: Self::ShutdownStreamRetry
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    > {
        match (channel, param, retry) {
            (
                SharedPrivateValue::Private { private: id },
                SharedPrivateValue::Private { private: param },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(self
                .private
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .retry_shutdown_stream(ctx, id, param, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|(params, when)| {
                    let params = params.map(|params| {
                        params
                            .into_iter()
                            .map(|param| SharedPrivateValue::Private {
                                private: param
                            })
                            .collect()
                    });

                    (params, when)
                })),
            (
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(self
                .shared
                .as_mut()
                .ok_or(SharedPrivateMatchError::Shutdown)?
                .retry_shutdown_stream(ctx, id, param, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|(params, when)| {
                    let params = params.map(|params| {
                        params
                            .into_iter()
                            .map(|param| SharedPrivateValue::Shared {
                                shared: param
                            })
                            .collect()
                    });

                    (params, when)
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn shutdown_listen(
        mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<Option<(Self, Option<Instant>)>, Self::ShutdownListenError>
    {
        let private_when = if let Some(private) = self.private.take() {
            if let Some((private, when)) = private
                .shutdown_listen(ctx, tokens)
                .map_err(|err| SharedPrivateError::Private { err: err })?
            {
                self.private = Some(private);

                when
            } else {
                None
            }
        } else {
            None
        };
        let shared_when = if let Some(private) = self.private.take() {
            if let Some((private, when)) = private
                .shutdown_listen(ctx, tokens)
                .map_err(|err| SharedPrivateError::Private { err: err })?
            {
                self.private = Some(private);

                when
            } else {
                None
            }
        } else {
            None
        };

        if self.private.is_some() || self.shared.is_some() {
            Ok(Some((self, next_retry(&private_when, &shared_when))))
        } else {
            Ok(None)
        }
    }
}

impl ChannelParam<UnixSocketAddr> for UnixSocketAddr {
    #[inline]
    fn accepts_addr(
        &self,
        _addr: &UnixSocketAddr
    ) -> bool {
        true
    }
}

impl ChannelParam<SocketAddr> for SocketAddr {
    #[inline]
    fn accepts_addr(
        &self,
        _addr: &SocketAddr
    ) -> bool {
        true
    }
}

impl<T> ChannelParam<T> for NullChannelsParam {
    #[inline]
    fn accepts_addr(
        &self,
        _addr: &T
    ) -> bool {
        false
    }
}

impl<PrivateAddr, PrivateParam, SharedAddr, SharedParam>
    ChannelParam<SharedPrivateValue<PrivateAddr, SharedAddr>>
    for SharedPrivateValue<PrivateParam, SharedParam>
where
    PrivateParam: ChannelParam<PrivateAddr>,
    SharedParam: ChannelParam<SharedAddr>
{
    #[inline]
    fn accepts_addr(
        &self,
        addr: &SharedPrivateValue<PrivateAddr, SharedAddr>
    ) -> bool {
        match (self, addr) {
            (
                SharedPrivateValue::Private { private: param },
                SharedPrivateValue::Private { private: addr }
            ) => param.accepts_addr(addr),
            (
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateValue::Shared { shared: addr }
            ) => param.accepts_addr(addr),
            _ => false
        }
    }
}

impl<PrivateID, PrivateParam, PrivateIter, SharedID, SharedParam, SharedIter>
    Iterator
    for SharedPrivateParamIter<
        PrivateID,
        PrivateParam,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    >
where
    PrivateIter: FusedIterator + Iterator<Item = (PrivateID, PrivateParam)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam)>
{
    type Item = (
        SharedPrivateValue<PrivateID, SharedID>,
        SharedPrivateValue<PrivateParam, SharedParam>
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.private {
            Some(private) => match private.next() {
                None => {
                    self.private = None;

                    self.next()
                }
                param => param.map(|(id, param)| {
                    (
                        SharedPrivateValue::Private { private: id },
                        SharedPrivateValue::Private { private: param }
                    )
                })
            },
            None => self.shared.as_mut().and_then(|shared| {
                shared.next().map(|(id, param)| {
                    (
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param }
                    )
                })
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => {
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
                }
            }
            (None, Some(shared)) => shared.size_hint(),
            (Some(private), None) => private.size_hint(),
            (None, None) => (0, Some(0))
        }
    }

    #[inline]
    fn count(self) -> usize {
        match (self.private, self.shared) {
            (Some(private), Some(shared)) => private.count() + shared.count(),
            (None, Some(shared)) => shared.count(),
            (Some(private), None) => private.count(),
            (None, None) => 0
        }
    }
}

impl<PrivateID, PrivateParam, PrivateIter, SharedID, SharedParam, SharedIter>
    ExactSizeIterator
    for SharedPrivateParamIter<
        PrivateID,
        PrivateParam,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    >
where
    PrivateIter: FusedIterator
        + ExactSizeIterator
        + Iterator<Item = (PrivateID, PrivateParam)>,
    SharedIter: ExactSizeIterator + Iterator<Item = (SharedID, SharedParam)>
{
    #[inline]
    fn len(&self) -> usize {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => private.len() + shared.len(),
            (None, Some(shared)) => shared.len(),
            (Some(private), None) => private.len(),
            (None, None) => 0
        }
    }
}

impl<PrivateID, PrivateParam, PrivateIter, SharedID, SharedParam, SharedIter>
    FusedIterator
    for SharedPrivateParamIter<
        PrivateID,
        PrivateParam,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    >
where
    PrivateIter: FusedIterator + Iterator<Item = (PrivateID, PrivateParam)>,
    SharedIter: FusedIterator + Iterator<Item = (SharedID, SharedParam)>
{
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedIter
> Iterator
    for SharedPrivateEndpointIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedIter
    >
where
    PrivateIter:
        FusedIterator + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam)>,
    SharedIter: Iterator<Item = (SharedAddr, SharedID, SharedParam)>
{
    type Item = (
        SharedPrivateValue<PrivateAddr, SharedAddr>,
        SharedPrivateValue<PrivateID, SharedID>,
        SharedPrivateValue<PrivateParam, SharedParam>
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.private {
            Some(private) => match private.next() {
                None => {
                    self.private = None;

                    self.next()
                }
                param => param.map(|(addr, id, param)| {
                    (
                        SharedPrivateValue::Private { private: addr },
                        SharedPrivateValue::Private { private: id },
                        SharedPrivateValue::Private { private: param }
                    )
                })
            },
            None => self.shared.as_mut().and_then(|shared| {
                shared.next().map(|(addr, id, param)| {
                    (
                        SharedPrivateValue::Shared { shared: addr },
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param }
                    )
                })
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => {
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
                }
            }
            (None, Some(shared)) => shared.size_hint(),
            (Some(private), None) => private.size_hint(),
            (None, None) => (0, Some(0))
        }
    }

    #[inline]
    fn count(self) -> usize {
        match (self.private, self.shared) {
            (Some(private), Some(shared)) => private.count() + shared.count(),
            (None, Some(shared)) => shared.count(),
            (Some(private), None) => private.count(),
            (None, None) => 0
        }
    }
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedIter
> ExactSizeIterator
    for SharedPrivateEndpointIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedIter
    >
where
    PrivateIter: FusedIterator
        + ExactSizeIterator
        + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam)>,
    SharedIter: ExactSizeIterator
        + Iterator<Item = (SharedAddr, SharedID, SharedParam)>
{
    #[inline]
    fn len(&self) -> usize {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => private.len() + shared.len(),
            (None, Some(shared)) => shared.len(),
            (Some(private), None) => private.len(),
            (None, None) => 0
        }
    }
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedIter
> FusedIterator
    for SharedPrivateEndpointIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedIter
    >
where
    PrivateIter:
        FusedIterator + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam)>,
    SharedIter:
        FusedIterator + Iterator<Item = (SharedAddr, SharedID, SharedParam)>
{
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateStream,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedStream,
    SharedIter
> Iterator
    for SharedPrivateStreamIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateStream,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedStream,
        SharedIter
    >
where
    SharedAddr: Clone,
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam, PrivateStream)>,
    SharedIter:
        Iterator<Item = (SharedAddr, SharedID, SharedParam, SharedStream)>
{
    type Item = (
        SharedPrivateValue<PrivateAddr, SharedAddr>,
        SharedPrivateValue<PrivateID, SharedID>,
        SharedPrivateValue<PrivateParam, SharedParam>,
        SharedPrivateChannelStream<PrivateStream, SharedStream, SharedAddr>
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.private {
            Some(private) => match private.next() {
                None => {
                    self.private = None;

                    self.next()
                }
                param => param.map(|(addr, id, param, stream)| {
                    (
                        SharedPrivateValue::Private { private: addr },
                        SharedPrivateValue::Private { private: id },
                        SharedPrivateValue::Private { private: param },
                        SharedPrivateChannelStream::Private { stream: stream }
                    )
                })
            },
            None => self.shared.as_mut().and_then(|shared| {
                shared.next().map(|(addr, id, param, stream)| {
                    (
                        SharedPrivateValue::Shared {
                            shared: addr.clone()
                        },
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param },
                        SharedPrivateChannelStream::Shared {
                            stream: stream,
                            party: addr
                        }
                    )
                })
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => {
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
                }
            }
            (None, Some(shared)) => shared.size_hint(),
            (Some(private), None) => private.size_hint(),
            (None, None) => (0, Some(0))
        }
    }

    #[inline]
    fn count(self) -> usize {
        match (self.private, self.shared) {
            (Some(private), Some(shared)) => private.count() + shared.count(),
            (None, Some(shared)) => shared.count(),
            (Some(private), None) => private.count(),
            (None, None) => 0
        }
    }
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateStream,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedStream,
    SharedIter
> ExactSizeIterator
    for SharedPrivateStreamIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateStream,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedStream,
        SharedIter
    >
where
    SharedAddr: Clone,
    PrivateIter: FusedIterator
        + ExactSizeIterator
        + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam, PrivateStream)>,
    SharedIter: ExactSizeIterator
        + Iterator<Item = (SharedAddr, SharedID, SharedParam, SharedStream)>
{
    #[inline]
    fn len(&self) -> usize {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) => private.len() + shared.len(),
            (None, Some(shared)) => shared.len(),
            (Some(private), None) => private.len(),
            (None, None) => 0
        }
    }
}

impl<
    PrivateID,
    PrivateParam,
    PrivateAddr,
    PrivateStream,
    PrivateIter,
    SharedID,
    SharedParam,
    SharedAddr,
    SharedStream,
    SharedIter
> FusedIterator
    for SharedPrivateStreamIter<
        PrivateID,
        PrivateParam,
        PrivateAddr,
        PrivateStream,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedAddr,
        SharedStream,
        SharedIter
    >
where
    SharedAddr: Clone,
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateAddr, PrivateID, PrivateParam, PrivateStream)>,
    SharedIter: FusedIterator
        + Iterator<Item = (SharedAddr, SharedID, SharedParam, SharedStream)>
{
}

impl<Private, Shared> RecoverableError
    for SharedPrivateMatchError<Private, Shared>
where
    Private: RecoverableError,
    Shared: RecoverableError
{
    type Completable =
        SharedPrivateMatchError<Private::Completable, Shared::Completable>;
    type Permanent =
        SharedPrivateMatchError<Private::Permanent, Shared::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SharedPrivateMatchError::Private { err } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| SharedPrivateMatchError::Private {
                        err: err
                    }),
                    permanent.map(|err| SharedPrivateMatchError::Private {
                        err: err
                    })
                )
            }
            SharedPrivateMatchError::Shared { err } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| SharedPrivateMatchError::Shared {
                        err: err
                    }),
                    permanent.map(|err| SharedPrivateMatchError::Shared {
                        err: err
                    })
                )
            }
            SharedPrivateMatchError::Mismatch => {
                (None, Some(SharedPrivateMatchError::Mismatch))
            }
            SharedPrivateMatchError::Shutdown => {
                (None, Some(SharedPrivateMatchError::Shutdown))
            }
        }
    }
}

impl<Ctx, Shared, Private> PushStream<Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStream<Ctx> + PushStreamPartyID,
    Private: PushStream<Ctx>
{
    type BatchID = SharedPrivateValue<Private::BatchID, Shared::BatchID>;
    type CancelBatchError = SharedPrivateMatchError<
        Private::CancelBatchError,
        Shared::CancelBatchError
    >;
    type CancelBatchRetry = SharedPrivateStreamRetry<
        Private::CancelBatchRetry,
        Shared::CancelBatchRetry
    >;
    type FinishBatchError = SharedPrivateMatchError<
        Private::FinishBatchError,
        Shared::FinishBatchError
    >;
    type FinishBatchRetry = SharedPrivateStreamRetry<
        Private::FinishBatchRetry,
        Shared::FinishBatchRetry
    >;
    type ReportError =
        SharedPrivateMatchError<Private::ReportError, Shared::ReportError>;
    type StreamFlags =
        SharedPrivateStreamCaches<Private::StreamFlags, Shared::StreamFlags>;

    fn empty_flags(&self) -> Self::StreamFlags {
        match self {
            SharedPrivateChannelStream::Private { stream } => {
                SharedPrivateStreamCaches {
                    shared: Shared::StreamFlags::default(),
                    private: stream.empty_flags()
                }
            }
            SharedPrivateChannelStream::Shared { stream, .. } => {
                SharedPrivateStreamCaches {
                    private: Private::StreamFlags::default(),
                    shared: stream.empty_flags()
                }
            }
        }
    }

    fn empty_flags_with_capacity(size: usize) -> Self::StreamFlags {
        SharedPrivateStreamCaches {
            private: Private::empty_flags_with_capacity(size),
            shared: Shared::empty_flags_with_capacity(size)
        }
    }

    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match (self, batch) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id }
            ) => Ok(stream
                .finish_batch(ctx, &mut flags.private, id)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id }
            ) => Ok(stream
                .finish_batch(ctx, &mut flags.shared, id)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match (self, batch, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_finish_batch(ctx, &mut flags.private, id, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_finish_batch(ctx, &mut flags.shared, id, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_finish_batch(ctx, &mut flags.private, id, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_finish_batch(ctx, &mut flags.shared, id, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match (self, batch) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id }
            ) => Ok(stream
                .cancel_batch(ctx, &mut flags.private, id)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id }
            ) => Ok(stream
                .cancel_batch(ctx, &mut flags.shared, id)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match (self, batch, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_cancel_batch(ctx, &mut flags.private, id, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_cancel_batch(ctx, &mut flags.shared, id, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_cancel_batch(ctx, &mut flags.private, id, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_cancel_batch(ctx, &mut flags.shared, id, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn cancel_batches(&mut self) {
        match self {
            SharedPrivateChannelStream::Private { stream } => {
                stream.cancel_batches()
            }
            SharedPrivateChannelStream::Shared { stream, .. } => {
                stream.cancel_batches()
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        match (self, batch) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id }
            ) => stream
                .report_failure(id)
                .map_err(|err| SharedPrivateMatchError::Private { err: err }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id }
            ) => stream
                .report_failure(id)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err }),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<
    Shared,
    Private,
    SharedError,
    PrivateError,
    SharedBatch,
    PrivateBatch,
    PartyID
>
    PushStreamReportBatchError<
        SharedPrivateMatchError<PrivateError, SharedError>,
        SharedPrivateValue<PrivateBatch, SharedBatch>
    > for SharedPrivateChannelStream<Private, Shared, PartyID>
where
    Private: PushStreamReportBatchError<PrivateError, PrivateBatch>,
    Shared: PushStreamReportBatchError<SharedError, SharedBatch>
{
    type ReportBatchError = SharedPrivateMatchError<
        <Private as PushStreamReportBatchError<PrivateError, PrivateBatch>>::ReportBatchError,
        <Shared as PushStreamReportBatchError<SharedError, SharedBatch>>::ReportBatchError
    >;

    fn report_error_with_batch(
        &mut self,
        batch: &SharedPrivateValue<PrivateBatch, SharedBatch>,
        error: &SharedPrivateMatchError<PrivateError, SharedError>
    ) -> Result<(), Self::ReportBatchError> {
        match (self, batch, error) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateMatchError::Private { err }
            ) => stream
                .report_error_with_batch(id, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateMatchError::Shared { err }
            ) => stream
                .report_error_with_batch(id, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err }),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<Shared, Private, SharedError, PrivateError, PartyID>
    PushStreamReportError<SharedPrivateMatchError<PrivateError, SharedError>>
    for SharedPrivateChannelStream<Private, Shared, PartyID>
where
    Private: PushStreamReportError<PrivateError>,
    Shared: PushStreamReportError<SharedError>
{
    type ReportError = SharedPrivateMatchError<
        <Private as PushStreamReportError<PrivateError>>::ReportError,
        <Shared as PushStreamReportError<SharedError>>::ReportError
    >;

    fn report_error(
        &mut self,
        error: &SharedPrivateMatchError<PrivateError, SharedError>
    ) -> Result<(), Self::ReportError> {
        match (self, error) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => stream
                .report_error(err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => stream
                .report_error(err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err }),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<Shared, Private, T, Ctx> PushStreamAdd<T, Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStreamAdd<T, Ctx> + PushStreamPartyID,
    Private: PushStreamAdd<T, Ctx>
{
    type AddError =
        SharedPrivateMatchError<Private::AddError, Shared::AddError>;
    type AddRetry =
        SharedPrivateStreamRetry<Private::AddRetry, Shared::AddRetry>;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match (self, batch) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id }
            ) => Ok(stream
                .add(ctx, &mut flags.private, msg, id)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id }
            ) => Ok(stream
                .add(ctx, &mut flags.shared, msg, id)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match (self, batch, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_add(ctx, &mut flags.private, msg, id, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_add(ctx, &mut flags.shared, msg, id, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateValue::Private { private: id },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_add(ctx, &mut flags.private, msg, id, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_add(ctx, &mut flags.shared, msg, id, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<Private, Shared, Ctx> LargeObjStream<Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: LargeObjStream<Ctx> + PushStreamPartyID,
    Private: LargeObjStream<Ctx, Frags = Shared::Frags>
{
    type Frags = Shared::Frags;
    type Parties =
        SharedPrivateStreamParties<Private::Parties, Shared::Parties>;
    type PushFragError =
        SharedPrivateMatchError<Private::PushFragError, Shared::PushFragError>;
    type PushFragRetry =
        SharedPrivateStreamRetry<Private::PushFragRetry, Shared::PushFragRetry>;

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
        match self {
            SharedPrivateChannelStream::Shared { stream, .. } => Ok(stream
                .push_frags(ctx, id, frags)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .push_frags(ctx, id, frags)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                }))
        }
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
        match (self, retry) {
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_push_frags(ctx, id, frags, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_push_frags(ctx, id, frags, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
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
        match (self, err) {
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_push_frags(ctx, id, frags, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_push_frags(ctx, id, frags, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<Private, Shared, H, Ctx> LargeObjOfferStream<H, Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared:
        LargeObjStream<Ctx> + LargeObjOfferStream<H, Ctx> + PushStreamPartyID,
    Private: LargeObjStream<Ctx, Frags = Shared::Frags>
        + LargeObjOfferStream<H, Ctx>,
    H: HashID
{
    type PushOfferError = SharedPrivateMatchError<
        Private::PushOfferError,
        Shared::PushOfferError
    >;
    type PushOfferRetry = SharedPrivateStreamRetry<
        Private::PushOfferRetry,
        Shared::PushOfferRetry
    >;

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
        match self {
            SharedPrivateChannelStream::Shared { stream, .. } => Ok(stream
                .push_offer(ctx, hash, frags)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .push_offer(ctx, hash, frags)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                }))
        }
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
        match (self, retry) {
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_push_offer(ctx, hash, frags, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_push_offer(ctx, hash, frags, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
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
        match (self, err) {
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_push_offer(ctx, hash, frags, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Shared { parties: parties }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Shared {
                        parties: parties
                    })
                })),
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_push_offer(ctx, hash, frags, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(when, parties)| {
                    (
                        when,
                        SharedPrivateStreamParties::Private {
                            parties: parties
                        }
                    )
                })
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map_indef(|parties| {
                    parties.map(|parties| SharedPrivateStreamParties::Private {
                        parties: parties
                    })
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl<Shared, Private> PushStreamPartyID
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStreamPartyID
{
    type PartyID = ();
}

impl<Ctx, Shared, Private> PushStreamPrivate<Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStreamShared<Ctx> + PushStreamPartyID,
    Private: PushStreamPrivate<Ctx>
{
    type AbortBatchRetry = SharedPrivateStreamRetry<
        Private::AbortBatchRetry,
        Shared::AbortBatchRetry
    >;
    type CreateBatchError = SharedPrivateMatchError<
        Private::CreateBatchError,
        Shared::CreateBatchError
    >;
    type CreateBatchRetry = SharedPrivateStreamRetry<
        Private::CreateBatchRetry,
        Shared::CreateBatchRetry
    >;
    type SelectError =
        SharedPrivateMatchError<Private::SelectError, Shared::SelectError>;
    type SelectRetry =
        SharedPrivateStreamRetry<Private::SelectRetry, Shared::SelectRetry>;
    type Selections =
        SharedPrivateStreamCaches<Private::Selections, Shared::Selections>;
    type StartBatchError = SharedPrivateMatchError<
        Private::StartBatchError,
        Shared::StartBatchError
    >;
    type StartBatchRetry = SharedPrivateStreamRetry<
        Private::StartBatchRetry,
        Shared::StartBatchRetry
    >;
    type StartBatchStreamBatches = SharedPrivateStreamCaches<
        Private::StartBatchStreamBatches,
        Shared::StartBatchStreamBatches
    >;

    fn empty_selections(&self) -> Self::Selections {
        match self {
            SharedPrivateChannelStream::Private { stream } => {
                SharedPrivateStreamCaches {
                    shared: Shared::Selections::default(),
                    private: stream.empty_selections()
                }
            }
            SharedPrivateChannelStream::Shared { stream, .. } => {
                SharedPrivateStreamCaches {
                    private: Private::Selections::default(),
                    shared: stream.empty_selections()
                }
            }
        }
    }

    fn empty_selections_with_capacity(size: usize) -> Self::Selections {
        SharedPrivateStreamCaches {
            private: Private::empty_selections_with_capacity(size),
            shared: Shared::empty_selections_with_capacity(size)
        }
    }

    fn empty_batches(&self) -> Self::StartBatchStreamBatches {
        match self {
            SharedPrivateChannelStream::Private { stream } => {
                SharedPrivateStreamCaches {
                    shared: Shared::StartBatchStreamBatches::default(),
                    private: stream.empty_batches()
                }
            }
            SharedPrivateChannelStream::Shared { stream, .. } => {
                SharedPrivateStreamCaches {
                    private: Private::StartBatchStreamBatches::default(),
                    shared: stream.empty_batches()
                }
            }
        }
    }

    fn empty_batches_with_capacity(
        size: usize
    ) -> Self::StartBatchStreamBatches {
        SharedPrivateStreamCaches {
            private: Private::empty_batches_with_capacity(size),
            shared: Shared::empty_batches_with_capacity(size)
        }
    }

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .select(ctx, &mut selections.private)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            SharedPrivateChannelStream::Shared { stream, party } => Ok(stream
                .select(ctx, &mut selections.shared, once(&*party))
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|_| ()))
        }
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_select(ctx, &mut selections.private, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_select(ctx, &mut selections.shared, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|_| ())),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_select(ctx, &mut selections.private, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_select(ctx, &mut selections.shared, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|_| ())),
            _ => Err(SharedPrivateMatchError::Mismatch)
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
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .create_batch(ctx, &mut batches.private, &selections.private)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            SharedPrivateChannelStream::Shared { stream, .. } => Ok(stream
                .create_batch(ctx, &mut batches.shared, &selections.shared)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Shared { shared: id }))
        }
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
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_create_batch(
                    ctx,
                    &mut batches.private,
                    &selections.private,
                    retry
                )
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_create_batch(
                    ctx,
                    &mut batches.shared,
                    &selections.shared,
                    retry
                )
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
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
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_create_batch(
                    ctx,
                    &mut batches.private,
                    &selections.private,
                    err
                )
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_create_batch(
                    ctx,
                    &mut batches.shared,
                    &selections.shared,
                    err
                )
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .start_batch(ctx)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            SharedPrivateChannelStream::Shared { stream, party } => Ok(stream
                .start_batch(ctx, once(&*party))
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id }))
        }
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_start_batch(ctx, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_start_batch(ctx, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
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
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_start_batch(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_start_batch(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => stream
                .abort_start_batch(ctx, &mut flags.private, err)
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => stream
                .abort_start_batch(ctx, &mut flags.shared, err)
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                }),
            _ => {
                error!(target: "shared-private-channels-stream",
                       "mismatch between stream and error subtypes");

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
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => stream
                .retry_abort_start_batch(ctx, &mut flags.private, retry)
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => stream
                .retry_abort_start_batch(ctx, &mut flags.shared, retry)
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                }),
            _ => {
                error!(target: "shared-private-channels-stream",
                       "mismatch between stream and error subtypes");

                RetryResult::Success(())
            }
        }
    }
}

impl<Shared, Private, T, Ctx> PushStreamPrivateSingle<T, Ctx>
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStreamSharedSingle<T, Ctx> + PushStreamPartyID,
    Private: PushStreamPrivateSingle<T, Ctx>
{
    type CancelPushError = SharedPrivateMatchError<
        Private::CancelPushError,
        Shared::CancelPushError
    >;
    type CancelPushRetry = SharedPrivateStreamRetry<
        Private::CancelPushRetry,
        Shared::CancelPushRetry
    >;
    type PushError =
        SharedPrivateMatchError<Private::PushError, Shared::PushError>;
    type PushRetry =
        SharedPrivateStreamRetry<Private::PushRetry, Shared::PushRetry>;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .push(ctx, msg)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            SharedPrivateChannelStream::Shared { stream, party } => Ok(stream
                .push(ctx, once(&*party), msg)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id }))
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_push(ctx, msg, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_push(ctx, msg, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_push(ctx, msg, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateValue::Private { private: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_push(ctx, msg, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map_indef(|_| ())
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as RecoverableError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .cancel_push(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .cancel_push(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn retry_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_cancel_push(ctx, retry)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_cancel_push(ctx, retry)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateMatchError::Private { err }
            ) => Ok(stream
                .complete_cancel_push(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateMatchError::Shared { err }
            ) => Ok(stream
                .complete_cancel_push(ctx, err)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }
}

impl Display for NullChannelsID {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "null channel ID")
    }
}

impl Display for NullChannelsParam {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "null channel param")
    }
}

impl Display for NullChannelsAddr {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "null channel addr")
    }
}

impl<Private, Shared> Display for SharedPrivateError<Private, Shared>
where
    Private: Display,
    Shared: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SharedPrivateError::Private { err } => err.fmt(f),
            SharedPrivateError::Shared { err } => err.fmt(f),
            SharedPrivateError::Shutdown => {
                write!(f, "stream is partially shut down")
            }
        }
    }
}

impl<Private, Shared> Display for SharedPrivateMatchError<Private, Shared>
where
    Private: Display,
    Shared: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SharedPrivateMatchError::Private { err } => err.fmt(f),
            SharedPrivateMatchError::Shared { err } => err.fmt(f),
            SharedPrivateMatchError::Mismatch => {
                write!(f, "mismatched param and addr")
            }
            SharedPrivateMatchError::Shutdown => {
                write!(f, "stream is partially shut down")
            }
        }
    }
}

impl<Private, Shared> Display for SharedPrivateValue<Private, Shared>
where
    Private: Display,
    Shared: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            SharedPrivateValue::Private { private } => private.fmt(f),
            SharedPrivateValue::Shared { shared } => shared.fmt(f)
        }
    }
}
