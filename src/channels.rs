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
use std::convert::Infallible;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::empty;
use std::iter::once;
use std::iter::Empty;
use std::iter::FusedIterator;
use std::net::SocketAddr;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::unix::UnixSocketAddr;
use log::error;
use mio::Token;

use crate::error::ErrorReportInfo;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::StreamReporter;

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
    type ParamIter: Iterator<Item = (Self::ChannelID, Self::Param)>;
    type EndpointIter: Iterator<Item = (Self::Addr, Self::ChannelID, Self::Param)>;
    type StreamIter: Iterator<Item = (Self::Addr, Self::ChannelID, Self::Param, Self::Stream)>;
    /// Type of errors that can occur when obtaining parameters.
    type ParamError: Debug + Display + ScopedError;
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
    type ListenError: Debug + Display + ScopedError;
    type ShutdownStreamError: Debug + Display + ScopedError;
    type ShutdownListenError: Debug + Display + ScopedError;
    type ShutdownError: Debug + Display + ScopedError;

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
    /// # Return Value
    ///
    /// - `(params, Some(when))`: The current set of parameters is
    ///   `params`, and will be refresh again at `when`.
    ///
    /// - `(params, None)`: The current set of parameters is
    ///   `params`, and does not need to be refreshed.
    fn params(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>,
                Self::ParamError>;

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
    /// - `ctx`: The context.
    ///
    /// - `channel`: The channel ID on which to create the stream.
    ///
    /// - `param`: The channel parameter to use.  These are obtained
    ///   from this fungtion, or from [params](Channels::params).
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
    /// 1. If a refresh occurred, the new set of channel parameters.
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
            Option<Self::ParamIter>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    >;

    fn listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Self::ParamIter,
            Option<Instant>
        )>,
        Self::ListenError
    >;

    fn shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        session: Self::Stream
    ) -> Result<
        RetryResult<(Option<Self::ParamIter>, Option<Instant>)>,
        Self::ShutdownStreamError
    >;

    fn shutdown(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<bool, Self::ShutdownError>;

    fn shutdown_listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<RetryResult<bool>, Self::ShutdownListenError>;
}

/// Trait for instances of `Channels` that can be created from a
/// configuration object.
pub trait ChannelsCreate<Ctx, Srcs>: Sized + Channels<Ctx> {
    /// Type of configuration from which this instance can be created.
    ///
    /// This will be supplemented by `Srcs`, which is assumed to
    /// provide a set of channels.  This configuration type is assumed
    /// to carry any additional information.
    type Config;
    /// Type of errors that can occur during creation.
    type CreateError: Debug + Display;

    /// Create an instance of this `Channels`.
    fn create(
        ctx: &mut Ctx,
        config: Self::Config,
        srcs: Srcs
    ) -> Result<Self, Self::CreateError>;
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
    private: Private,
    /// The shared channels source.
    shared: Shared
}

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
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr, PrivateStream)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam, SharedAddr, SharedStream)> {
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
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam, SharedAddr)> {
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
    }
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

/// Type of [Frags] instance for [SharedPrivateChannelStream].
#[derive(Clone)]
pub enum SharedPrivateStreamFrags<Private, Shared> {
    /// [Frags] for private channels.
    Private {
        /// `Frags` instance for private channels.
        frags: Private
    },
    /// [Frags] for shared channels.
    Shared {
        /// `Frags` instance for shared channels.
        frags: Shared
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
    Mismatch
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
            SharedPrivateMatchError::Mismatch => ErrorScope::Unrecoverable
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
            SharedPrivateError::Shared { err } => err.scope()
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
            SharedPrivateError::Shared { err } => err.report_info()
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
            SharedPrivateMatchError::Mismatch => None
        }
    }
}

impl<Ctx> Channels<Ctx> for NullChannels {
    type ChannelID = NullChannelsID;
    type Param = NullChannelsParam;
    type ParamIter = Empty<(NullChannelsID, NullChannelsParam)>;
    type EndpointIter = Empty<(Self::ChannelID, Self::Param, Self::Addr)>;
    type StreamIter =
        Empty<(Self::ChannelID, Self::Param, Self::Addr, Self::Stream)>;
    type ParamError = Infallible;
    type OutNegoParam = ();
    type Addr = NullChannelsAddr;
    type Stream = ();
    type ReqStreamError = Infallible;
    type ListenError = Infallible;
    type ShutdownStreamError = Infallible;
    type ShutdownListenError = Infallible;
    type ShutdownError = Infallible;

    #[inline]
    fn params(
        &mut self,
        _ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>,
                Self::ParamError> {
        Ok(RetryResult::Success((empty(), None)))
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
            Option<Self::ParamIter>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        Ok(RetryResult::Success((Some(()), None, None)))
    }

    #[inline]
    fn listen(
        &mut self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Self::ParamIter,
            Option<Instant>
        )>,
        Self::ListenError
    > {
        Ok(RetryResult::Success((empty(), empty(), empty(), None)))
    }

    #[inline]
    fn shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        _session: Self::Stream
    ) -> Result<
        RetryResult<(Option<Self::ParamIter>, Option<Instant>)>,
        Self::ShutdownStreamError
    > {
        Ok(RetryResult::Success((None, None)))
    }

    #[inline]
    fn shutdown(
        &mut self,
        _ctx: &mut Ctx,
    ) -> Result<bool, Self::ShutdownError> {
        Ok(true)
    }

    fn shutdown_listen(
        &mut self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<RetryResult<bool>, Self::ShutdownListenError> {
        Ok(RetryResult::Success(true))
    }
}

impl<Private, Shared> SharedPrivateChannels<Private, Shared> {
    #[inline]
    pub fn new(
        private: Private,
        shared: Shared
    ) -> Self {
        SharedPrivateChannels {
            private: private,
            shared: shared
        }
    }
}

impl<Private, Shared, Ctx> Channels<Ctx>
    for SharedPrivateChannels<Private, Shared>
where
    Private: Channels<Ctx>,
    Shared: Channels<Ctx>,
    Private::ParamIter: FusedIterator,
    Private::EndpointIter: FusedIterator,
    Private::StreamIter: FusedIterator
{
    type ChannelID = SharedPrivateValue<Private::ChannelID, Shared::ChannelID>;
    type Param = SharedPrivateValue<Private::Param, Shared::Param>;
    type ParamIter = SharedPrivateParamIter<
        Private::ChannelID,
        Private::Param,
        Private::ParamIter,
        Shared::ChannelID,
        Shared::Param,
        Shared::ParamIter
    >;
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
    type ParamError =
        SharedPrivateError<Private::ParamError, Shared::ParamError>;
    type OutNegoParam = SharedPrivateValue<Private::OutNegoParam,
                                           Shared::OutNegoParam>;
    type Addr = SharedPrivateValue<Private::Addr, Shared::Addr>;
    type Stream = SharedPrivateChannelStream<
        Private::Stream,
        Shared::Stream,
        Shared::Addr
    >;
    type ReqStreamError = SharedPrivateMatchError<Private::ReqStreamError,
                                                   Shared::ReqStreamError>;
    type ListenError = SharedPrivateError<Private::ListenError,
                                          Shared::ListenError>;
    type ShutdownStreamError =
        SharedPrivateMatchError<Private::ShutdownStreamError,
                                Shared::ShutdownStreamError>;
    type ShutdownListenError = SharedPrivateError<Private::ShutdownListenError,
                                                  Shared::ShutdownListenError>;
    type ShutdownError = SharedPrivateError<Private::ShutdownError,
                                            Shared::ShutdownError>;

    fn params(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>,
                Self::ParamError> {
        let (private, private_when) = match self
            .private
            .params(ctx)
            .map_err(|err| SharedPrivateError::Private { err: err })?
        {
            RetryResult::Retry(when) => return Ok(RetryResult::Retry(when)),
            RetryResult::Success(private) => private
        };
        let (shared, shared_when) = match self
            .shared
            .params(ctx)
            .map_err(|err| SharedPrivateError::Shared { err: err })?
        {
            RetryResult::Retry(when) => return Ok(RetryResult::Retry(when)),
            RetryResult::Success(shared) => shared
        };
        let refresh_when = match (private_when, shared_when) {
            (Some(a), Some(b)) => {
                if a < b {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (None, when) => when,
            (when, None) => when
        };
        let iter = SharedPrivateParamIter {
            private: Some(private),
            shared: Some(shared)
        };

        Ok(RetryResult::Success((iter, refresh_when)))
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
            Option<Self::ParamIter>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        match (channel, param, endpoint, nego_param) {
            (
                SharedPrivateValue::Private { private: id },
                SharedPrivateValue::Private { private: param },
                SharedPrivateValue::Private { private: addr },
                SharedPrivateValue::Private { private: nego_param }
            ) => Ok(self
                .private
                .req_stream(ctx, id, param, addr, nego_param)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(stream, params, when)| {
                    let stream = stream
                        .map(|stream| SharedPrivateChannelStream::Private {
                            stream: stream
                        });
                    let params = params
                        .map(|params| SharedPrivateParamIter {
                            private: Some(params),
                            shared: None
                        });

                    (stream, params, when)
                })),
            (
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateValue::Shared { shared: addr },
                SharedPrivateValue::Shared { shared: nego_param }
            ) => Ok(self
                .shared
                .req_stream(ctx, id, param, addr, nego_param)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(stream, params, when)| {
                    let stream = stream
                        .map(|stream| SharedPrivateChannelStream::Shared {
                            stream: stream,
                            party: addr.clone()
                        });
                    let params = params
                        .map(|params| SharedPrivateParamIter {
                            private: None,
                            shared: Some(params)
                        });

                    (stream, params, when)
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Self::ParamIter,
            Option<Instant>
        )>,
        Self::ListenError
    > {
        match (self.private.listen(ctx, tokens)
               .map_err(|err| SharedPrivateError::Private { err: err })?,
               self.shared.listen(ctx, tokens)
               .map_err(|err| SharedPrivateError::Shared { err: err })?) {
            (RetryResult::Success((private_streams, private_addrs,
                                   private_params, private_when)),
             RetryResult::Success((shared_streams, shared_addrs,
                                   shared_params, shared_when))) => {
                let streams = SharedPrivateStreamIter {
                    private: Some(private_streams),
                    shared: Some(shared_streams)
                };
                let addrs = SharedPrivateEndpointIter {
                    private: Some(private_addrs),
                    shared: Some(shared_addrs)
                };
                let params = SharedPrivateParamIter {
                    private: Some(private_params),
                    shared: Some(shared_params)
                };
                let when = private_when
                    .map_or(shared_when,
                            |private_when| Some(shared_when
                                .map_or(private_when,
                                        |shared_when|
                                        shared_when.max(private_when))
                            ));

                Ok(RetryResult::Success((streams, addrs, params, when)))
            }
            // XXX these cases will break, because downstream will
            // only see half the addresses.  Solution is probably to
            // cache addresses on the channels.
            (RetryResult::Success((private_streams, private_addrs,
                                   private_params, private_when)),
             RetryResult::Retry(shared_when)) => {
                let streams = SharedPrivateStreamIter {
                    private: Some(private_streams),
                    shared: None
                };
                let addrs = SharedPrivateEndpointIter {
                    private: Some(private_addrs),
                    shared: None
                };
                let params = SharedPrivateParamIter {
                    private: Some(private_params),
                    shared: None
                };
                let when = Some(private_when
                    .map_or(shared_when,
                            |private_when| private_when.max(shared_when))
                );

                Ok(RetryResult::Success((streams, addrs, params, when)))
            }
            (RetryResult::Retry(private_when),
             RetryResult::Success((shared_streams, shared_addrs,
                                   shared_params, shared_when))) => {
                let streams = SharedPrivateStreamIter {
                    private: None,
                    shared: Some(shared_streams)
                };
                let addrs = SharedPrivateEndpointIter {
                    private: None,
                    shared: Some(shared_addrs)
                };
                let params = SharedPrivateParamIter {
                    private: None,
                    shared: Some(shared_params)
                };
                let when = Some(shared_when
                    .map_or(private_when, |shared_when|
                            shared_when.max(private_when))
                );

                Ok(RetryResult::Success((streams, addrs, params, when)))
            }
            (RetryResult::Retry(private_when),
             RetryResult::Retry(shared_when)) =>
                Ok(RetryResult::Retry(private_when.max(shared_when)))
        }
    }

    fn shutdown_stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        session: Self::Stream
    ) -> Result<
        RetryResult<(Option<Self::ParamIter>, Option<Instant>)>,
        Self::ShutdownStreamError
    > {
        match (channel, param, session) {
            (
                SharedPrivateValue::Private { private: id },
                SharedPrivateValue::Private { private: param },
                SharedPrivateChannelStream::Private { stream }
            ) => Ok(self
                .private
                .shutdown_stream(ctx, id, param, stream)
                .map_err(|err| SharedPrivateMatchError::Private { err: err })?
                .map(|(params, when)| {
                    let params = params
                        .map(|params| SharedPrivateParamIter {
                            private: Some(params),
                            shared: None
                        });

                    (params, when)
                })),
            (
                SharedPrivateValue::Shared { shared: id },
                SharedPrivateValue::Shared { shared: param },
                SharedPrivateChannelStream::Shared { stream, .. }
            ) => Ok(self
                .shared
                .shutdown_stream(ctx, id, param, stream)
                .map_err(|err| SharedPrivateMatchError::Shared { err: err })?
                .map(|(params, when)| {
                    let params = params
                        .map(|params| SharedPrivateParamIter {
                            private: None,
                            shared: Some(params)
                        });

                    (params, when)
                })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn shutdown(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<bool, Self::ShutdownError> {
        let private = self.private.shutdown(ctx)
            .map_err(|err| SharedPrivateError::Private { err: err })?;
        let shared = self.shared.shutdown(ctx)
            .map_err(|err| SharedPrivateError::Shared { err: err })?;

        Ok(shared && private)
    }

    fn shutdown_listen(
        &mut self,
        ctx: &mut Ctx,
        tokens: &HashSet<Token>
    ) -> Result<RetryResult<bool>, Self::ShutdownListenError> {
        self.private.shutdown_listen(ctx, tokens)
            .map_err(|err| SharedPrivateError::Private { err: err })?
            .flat_map_ok(|private| Ok(self.shared
                             .shutdown_listen(ctx, tokens)
                             .map_err(|err| SharedPrivateError::Shared {
                                 err: err
                             })?
                             .map(|shared| shared && private)
                         )

            )
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

impl<
        PrivateID,
        PrivateParam,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    > Iterator
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
            None => self.shared.as_mut()
                .and_then(|shared| shared.next().map(|(id, param)| {
                    (
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param }
                    )
                }))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) =>
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
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
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    > ExactSizeIterator
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

impl<
        PrivateID,
        PrivateParam,
        PrivateIter,
        SharedID,
        SharedParam,
        SharedIter
    > FusedIterator
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
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam, SharedAddr)>
{
    type Item = (
        SharedPrivateValue<PrivateID, SharedID>,
        SharedPrivateValue<PrivateParam, SharedParam>,
        SharedPrivateValue<PrivateAddr, SharedAddr>
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.private {
            Some(private) => match private.next() {
                None => {
                    self.private = None;

                    self.next()
                }
                param => param.map(|(id, param, addr)| {
                    (
                        SharedPrivateValue::Private { private: id },
                        SharedPrivateValue::Private { private: param },
                        SharedPrivateValue::Private { private: addr }
                    )
                })
            },
            None => self.shared.as_mut()
                .and_then(|shared| shared.next().map(|(id, param, addr)| {
                    (
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param },
                        SharedPrivateValue::Shared { shared: addr }
                    )
                }))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) =>
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
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
    PrivateIter: FusedIterator + ExactSizeIterator
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr)>,
    SharedIter: ExactSizeIterator
        + Iterator<Item = (SharedID, SharedParam, SharedAddr)>
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
    PrivateIter: FusedIterator
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr)>,
    SharedIter: FusedIterator
        + Iterator<Item = (SharedID, SharedParam, SharedAddr)>
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
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr, PrivateStream)>,
    SharedIter: Iterator<Item = (SharedID, SharedParam, SharedAddr, SharedStream)>
{
    type Item = (
        SharedPrivateValue<PrivateID, SharedID>,
        SharedPrivateValue<PrivateParam, SharedParam>,
        SharedPrivateValue<PrivateAddr, SharedAddr>,
        SharedPrivateChannelStream<PrivateStream, SharedStream, SharedAddr>
    );

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.private {
            Some(private) => match private.next() {
                None => {
                    self.private = None;

                    self.next()
                }
                param => param.map(|(id, param, addr, stream)| {
                    (
                        SharedPrivateValue::Private { private: id },
                        SharedPrivateValue::Private { private: param },
                        SharedPrivateValue::Private { private: addr },
                        SharedPrivateChannelStream::Private { stream: stream }
                    )
                })
            },
            None => self.shared.as_mut()
                .and_then(|shared| shared.next().map(|(id, param, addr, stream)| {
                    (
                        SharedPrivateValue::Shared { shared: id },
                        SharedPrivateValue::Shared { shared: param },
                        SharedPrivateValue::Shared { shared: addr.clone() },
                        SharedPrivateChannelStream::Shared {
                            stream: stream,
                            party: addr
                        }
                    )
                }))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match (&self.private, &self.shared) {
            (Some(private), Some(shared)) =>
                match (private.size_hint(), shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
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
    PrivateIter: FusedIterator + ExactSizeIterator
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr, PrivateStream)>,
    SharedIter: ExactSizeIterator
        + Iterator<Item = (SharedID, SharedParam, SharedAddr, SharedStream)>
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
        + Iterator<Item = (PrivateID, PrivateParam, PrivateAddr, PrivateStream)>,
    SharedIter: FusedIterator
        + Iterator<Item = (SharedID, SharedParam, SharedAddr, SharedStream)>
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
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
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
                .map(|_| ()))
        }
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
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
                .map(|_| ())),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
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
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
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
                .map(|id| SharedPrivateValue::Shared { shared: id }))
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
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
                .map(|id| SharedPrivateValue::Shared { shared: id })),
            _ => Err(SharedPrivateMatchError::Mismatch)
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID, Self::PushRetry>,
                Self::PushError>
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
            SharedPrivateError::Shared { err } => err.fmt(f)
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
