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
use constellation_common::error::ScopedError;
use constellation_common::net::IPEndpointAddr;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::error;

use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::StreamBatches;
use crate::stream::StreamFlags;
use crate::stream::StreamReporter;

/// Trait for determining whether a given channel parameter can pair
/// with a given endpoint address.
///
/// This is used in multiplexing to determine whether to pair off
/// parameters and counterparty addresses.
pub trait ChannelParam<Addr>: Clone + Display + Eq + Hash {
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
    type ChannelID: Clone + Display + Eq + Hash;
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
    /// Type of errors that can occur when obtaining parameters.
    type ParamError: Display + ScopedError;
    /// Type of counterparty addresses to which to connect.
    type Addr: Clone + Display + Eq + Hash;
    /// Type of raw streams obtained from parameters.
    type Stream;
    /// Type of errors that can occur when obtaining flows from a parameter.
    type StreamError: Display + ScopedError;

    /// Obtain the current set of all channel parameters, and the time
    /// at which they will need to be refreshed.
    ///
    /// If parameters never need to be refreshed again, `None` will be
    /// returned.
    fn params(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>, Self::ParamError>;

    /// Get a raw stream from a channel.
    ///
    /// This will obtain a `Stream` instance from `channel`, using
    /// `param`, and with `party_addr` as the counterparty's address.
    ///
    /// The `verify_endpoint` parameter exists to provide a name for
    /// the purpose of TLS-like negatiations.  It should specify the
    /// "name" of the counterparty for the purpose of any certificate
    /// verification.
    fn stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        party_addr: &Self::Addr,
        verify_endpoint: Option<&IPEndpointAddr>
    ) -> Result<RetryResult<Self::Stream>, Self::StreamError>;
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
    /// [StreamReporter] to use for reporting new streams.
    type Reporter: StreamReporter;
    /// Type of errors that can occur during creation.
    type CreateError: Display;

    /// Create an instance of this `Channels`.
    fn create(
        ctx: &mut Ctx,
        reporter: Self::Reporter,
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

/// ID for [SharedPrivateChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SharedPrivateID<Private, Shared> {
    /// ID for the private channels.
    Private {
        /// Private ID.
        id: Private
    },
    /// ID for the shared channels.
    Shared {
        /// Shared ID.
        id: Shared
    }
}

/// Channel param for [SharedPrivateChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SharedPrivateChannelParam<Private, Shared> {
    /// Channel param for the private channels.
    Private {
        /// Private channel param.
        param: Private
    },
    /// Channel param for the shared channels.
    Shared {
        /// Shared channel param.
        param: Shared
    }
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
    shared: SharedIter
}

/// Param error for [SharedPrivateChannels].
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

/// Channel address for [SharedPrivateChannels].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SharedPrivateChannelAddr<Private, Shared> {
    /// Channel address for the private channels.
    Private {
        /// Private channel address.
        addr: Private
    },
    /// Channel address for the shared channels.
    Shared {
        /// Shared channel address.
        addr: Shared
    }
}

/// Type of streams for [SharedPrivateChannels].
///
/// This implements the basic [PushStream], [PushStreamAdd], and
/// [PushStreamSingle] traits that would be expected for *private*
/// channels.  It keeps the intended recipient for any shared channel,
/// which it transparently adds to any batch using the stream's
/// [PushStreamAddParty] trait.
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

/// Type of retry information for [SharedPrivateChannelStream].
#[derive(Clone)]
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

/// Stream error for [SharedPrivateChannels].
pub enum SharedPrivateStreamError<Private, Shared> {
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

/// [StreamBatches] and [StreamFlags] instance for [SharedPrivateChannelStream].
pub struct SharedPrivateStreamCaches<Private, Shared> {
    shared: Shared,
    private: Private
}

impl<Private, Shared> ScopedError for SharedPrivateStreamError<Private, Shared>
where
    Private: ScopedError,
    Shared: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            SharedPrivateStreamError::Private { err } => err.scope(),
            SharedPrivateStreamError::Shared { err } => err.scope(),
            SharedPrivateStreamError::Mismatch => ErrorScope::Unrecoverable
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
    for SharedPrivateStreamError<Private, Shared>
where
    Private: ErrorReportInfo<T>,
    Shared: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        match self {
            SharedPrivateStreamError::Private { err } => err.report_info(),
            SharedPrivateStreamError::Shared { err } => err.report_info(),
            SharedPrivateStreamError::Mismatch => None
        }
    }
}

impl<Private, Shared> StreamBatches
    for SharedPrivateStreamCaches<Private, Shared>
where
    Private: StreamBatches,
    Shared: StreamBatches
{
    type BatchID = SharedPrivateID<Private::BatchID, Shared::BatchID>;
    type StreamID = SharedPrivateID<Private::StreamID, Shared::StreamID>;

    #[inline]
    fn new() -> Self {
        SharedPrivateStreamCaches {
            private: Private::new(),
            shared: Shared::new()
        }
    }

    #[inline]
    fn with_capacity(size: usize) -> Self {
        SharedPrivateStreamCaches {
            private: Private::with_capacity(size),
            shared: Shared::with_capacity(size)
        }
    }

    #[inline]
    fn batch(
        &self,
        stream: &Self::StreamID
    ) -> Option<Self::BatchID> {
        match stream {
            SharedPrivateID::Private { id } => self
                .private
                .batch(id)
                .map(|id| SharedPrivateID::Private { id: id }),
            SharedPrivateID::Shared { id } => self
                .shared
                .batch(id)
                .map(|id| SharedPrivateID::Shared { id: id })
        }
    }

    #[inline]
    fn add_batch(
        &mut self,
        stream: Self::StreamID,
        batch: Self::BatchID
    ) {
        match (stream, batch) {
            (
                SharedPrivateID::Private { id: stream },
                SharedPrivateID::Private { id: batch }
            ) => self.private.add_batch(stream, batch),
            (
                SharedPrivateID::Shared { id: stream },
                SharedPrivateID::Shared { id: batch }
            ) => self.shared.add_batch(stream, batch),
            _ => {
                error!(target: "shared-private-channels",
                       "mismatched shared/private stream and batch IDs")
            }
        }
    }
}

impl<Private, Shared> StreamFlags for SharedPrivateStreamCaches<Private, Shared>
where
    Private: StreamFlags,
    Shared: StreamFlags
{
    type StreamID = SharedPrivateID<Private::StreamID, Shared::StreamID>;

    #[inline]
    fn new() -> Self {
        SharedPrivateStreamCaches {
            private: Private::new(),
            shared: Shared::new()
        }
    }

    #[inline]
    fn with_capacity(size: usize) -> Self {
        SharedPrivateStreamCaches {
            private: Private::with_capacity(size),
            shared: Shared::with_capacity(size)
        }
    }

    #[inline]
    fn is_set(
        &self,
        stream: &Self::StreamID
    ) -> bool {
        match stream {
            SharedPrivateID::Private { id } => self.private.is_set(id),
            SharedPrivateID::Shared { id } => self.shared.is_set(id)
        }
    }

    #[inline]
    fn set(
        &mut self,
        stream: Self::StreamID
    ) {
        match stream {
            SharedPrivateID::Private { id: stream } => self.private.set(stream),
            SharedPrivateID::Shared { id: stream } => self.shared.set(stream)
        }
    }
}

impl<Ctx> Channels<Ctx> for NullChannels {
    type Addr = NullChannelsAddr;
    type ChannelID = NullChannelsID;
    type Param = NullChannelsParam;
    type ParamError = Infallible;
    type ParamIter = Empty<(NullChannelsID, NullChannelsParam)>;
    type Stream = ();
    type StreamError = Infallible;

    #[inline]
    fn params(
        &mut self,
        _ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>, Self::ParamError>
    {
        Ok(RetryResult::Success((empty(), None)))
    }

    #[inline]
    fn stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        _party_addr: &Self::Addr,
        _endpoint: Option<&IPEndpointAddr>
    ) -> Result<RetryResult<Self::Stream>, Self::StreamError> {
        Ok(RetryResult::Success(()))
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
    Private::ParamIter: FusedIterator
{
    type Addr = SharedPrivateChannelAddr<Private::Addr, Shared::Addr>;
    type ChannelID = SharedPrivateID<Private::ChannelID, Shared::ChannelID>;
    type Param = SharedPrivateChannelParam<Private::Param, Shared::Param>;
    type ParamError =
        SharedPrivateError<Private::ParamError, Shared::ParamError>;
    type ParamIter = SharedPrivateParamIter<
        Private::ChannelID,
        Private::Param,
        Private::ParamIter,
        Shared::ChannelID,
        Shared::Param,
        Shared::ParamIter
    >;
    type Stream = SharedPrivateChannelStream<
        Private::Stream,
        Shared::Stream,
        Shared::Addr
    >;
    type StreamError =
        SharedPrivateStreamError<Private::StreamError, Shared::StreamError>;

    #[inline]
    fn params(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>, Self::ParamError>
    {
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
            shared: shared
        };

        Ok(RetryResult::Success((iter, refresh_when)))
    }

    #[inline]
    fn stream(
        &mut self,
        ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        party_addr: &Self::Addr,
        endpoint: Option<&IPEndpointAddr>
    ) -> Result<RetryResult<Self::Stream>, Self::StreamError> {
        match (channel, param, party_addr) {
            (
                SharedPrivateID::Private { id },
                SharedPrivateChannelParam::Private { param },
                SharedPrivateChannelAddr::Private { addr }
            ) => Ok(self
                .private
                .stream(ctx, id, param, addr, endpoint)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map(|stream| SharedPrivateChannelStream::Private {
                    stream: stream
                })),
            (
                SharedPrivateID::Shared { id },
                SharedPrivateChannelParam::Shared { param },
                SharedPrivateChannelAddr::Shared { addr }
            ) => Ok(self
                .shared
                .stream(ctx, id, param, addr, endpoint)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map(|stream| SharedPrivateChannelStream::Shared {
                    stream: stream,
                    party: addr.clone()
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
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
    ChannelParam<SharedPrivateChannelAddr<PrivateAddr, SharedAddr>>
    for SharedPrivateChannelParam<PrivateParam, SharedParam>
where
    PrivateParam: ChannelParam<PrivateAddr>,
    SharedParam: ChannelParam<SharedAddr>
{
    #[inline]
    fn accepts_addr(
        &self,
        addr: &SharedPrivateChannelAddr<PrivateAddr, SharedAddr>
    ) -> bool {
        match (self, addr) {
            (
                SharedPrivateChannelParam::Private { param },
                SharedPrivateChannelAddr::Private { addr }
            ) => param.accepts_addr(addr),
            (
                SharedPrivateChannelParam::Shared { param },
                SharedPrivateChannelAddr::Shared { addr }
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
        SharedPrivateID<PrivateID, SharedID>,
        SharedPrivateChannelParam<PrivateParam, SharedParam>
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
                        SharedPrivateID::Private { id: id },
                        SharedPrivateChannelParam::Private { param: param }
                    )
                })
            },
            None => self.shared.next().map(|(id, param)| {
                (
                    SharedPrivateID::Shared { id: id },
                    SharedPrivateChannelParam::Shared { param: param }
                )
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.private {
            Some(private) => {
                match (private.size_hint(), self.shared.size_hint()) {
                    ((lo_a, Some(hi_a)), (lo_b, Some(hi_b))) => {
                        (lo_a + lo_b, Some(hi_a + hi_b))
                    }
                    ((a, _), (b, _)) => (a + b, None)
                }
            }
            None => self.shared.size_hint()
        }
    }

    #[inline]
    fn count(self) -> usize {
        match self.private {
            Some(private) => private.count() + self.shared.count(),
            None => self.shared.count()
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
        match &self.private {
            Some(private) => private.len() + self.shared.len(),
            None => self.shared.len()
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

impl<Private, Shared> BatchError for SharedPrivateStreamError<Private, Shared>
where
    Private: BatchError,
    Shared: BatchError
{
    type Completable =
        SharedPrivateStreamError<Private::Completable, Shared::Completable>;
    type Permanent =
        SharedPrivateStreamError<Private::Permanent, Shared::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SharedPrivateStreamError::Private { err } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| SharedPrivateStreamError::Private {
                        err: err
                    }),
                    permanent.map(|err| SharedPrivateStreamError::Private {
                        err: err
                    })
                )
            }
            SharedPrivateStreamError::Shared { err } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| SharedPrivateStreamError::Shared {
                        err: err
                    }),
                    permanent.map(|err| SharedPrivateStreamError::Shared {
                        err: err
                    })
                )
            }
            SharedPrivateStreamError::Mismatch => {
                (None, Some(SharedPrivateStreamError::Mismatch))
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
    type BatchID = SharedPrivateID<Private::BatchID, Shared::BatchID>;
    type CancelBatchError = SharedPrivateStreamError<
        Private::CancelBatchError,
        Shared::CancelBatchError
    >;
    type CancelBatchRetry = SharedPrivateStreamRetry<
        Private::CancelBatchRetry,
        Shared::CancelBatchRetry
    >;
    type FinishBatchError = SharedPrivateStreamError<
        Private::FinishBatchError,
        Shared::FinishBatchError
    >;
    type FinishBatchRetry = SharedPrivateStreamRetry<
        Private::FinishBatchRetry,
        Shared::FinishBatchRetry
    >;
    type ReportError =
        SharedPrivateStreamError<Private::ReportError, Shared::ReportError>;
    type StartBatchStreamBatches = SharedPrivateStreamCaches<
        Private::StartBatchStreamBatches,
        Shared::StartBatchStreamBatches
    >;
    type StreamFlags =
        SharedPrivateStreamCaches<Private::StreamFlags, Shared::StreamFlags>;

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
                SharedPrivateID::Private { id }
            ) => Ok(stream
                .finish_batch(ctx, &mut flags.private, id)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id }
            ) => Ok(stream
                .finish_batch(ctx, &mut flags.shared, id)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                SharedPrivateID::Private { id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_finish_batch(ctx, &mut flags.private, id, retry)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_finish_batch(ctx, &mut flags.shared, id, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateID::Private { id },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_finish_batch(ctx, &mut flags.private, id, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_finish_batch(ctx, &mut flags.shared, id, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                SharedPrivateID::Private { id }
            ) => Ok(stream
                .cancel_batch(ctx, &mut flags.private, id)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id }
            ) => Ok(stream
                .cancel_batch(ctx, &mut flags.shared, id)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                SharedPrivateID::Private { id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_cancel_batch(ctx, &mut flags.private, id, retry)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_cancel_batch(ctx, &mut flags.shared, id, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateID::Private { id },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_cancel_batch(ctx, &mut flags.private, id, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_cancel_batch(ctx, &mut flags.shared, id, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                SharedPrivateID::Private { id }
            ) => stream
                .report_failure(id)
                .map_err(|err| SharedPrivateStreamError::Private { err: err }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id }
            ) => stream
                .report_failure(id)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err }),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }
}

impl<Shared, Private, SharedError, PrivateError, PartyID>
    PushStreamReportError<SharedPrivateStreamError<PrivateError, SharedError>>
    for SharedPrivateChannelStream<Private, Shared, PartyID>
where
    Private: PushStreamReportError<PrivateError>,
    Shared: PushStreamReportError<SharedError>
{
    type ReportError = SharedPrivateStreamError<
        <Private as PushStreamReportError<PrivateError>>::ReportError,
        <Shared as PushStreamReportError<SharedError>>::ReportError
    >;

    fn report_error(
        &mut self,
        error: &SharedPrivateStreamError<PrivateError, SharedError>
    ) -> Result<(), Self::ReportError> {
        match (self, error) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => stream
                .report_error(err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
            ) => stream
                .report_error(err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err }),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
        SharedPrivateStreamError<Private::AddError, Shared::AddError>;
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
                SharedPrivateID::Private { id }
            ) => Ok(stream
                .add(ctx, &mut flags.private, msg, id)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id }
            ) => Ok(stream
                .add(ctx, &mut flags.shared, msg, id)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                SharedPrivateID::Private { id },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_add(ctx, &mut flags.private, msg, id, retry)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_add(ctx, &mut flags.shared, msg, id, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match (self, batch, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateID::Private { id },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_add(ctx, &mut flags.private, msg, id, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateID::Shared { id },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_add(ctx, &mut flags.shared, msg, id, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }
}

impl<Shared, Private> PushStreamPartyID
    for SharedPrivateChannelStream<Private, Shared, Shared::PartyID>
where
    Shared: PushStreamPartyID,
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
    type StartBatchError = SharedPrivateStreamError<
        Private::StartBatchError,
        Shared::StartBatchError
    >;
    type StartBatchRetry = SharedPrivateStreamRetry<
        Private::StartBatchRetry,
        Shared::StartBatchRetry
    >;

    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .start_batch(ctx, &mut batches.private)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            SharedPrivateChannelStream::Shared { stream, .. } => Ok(stream
                .start_batch(ctx, &mut batches.shared)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id }))
        }
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
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_start_batch(ctx, &mut batches.private, retry)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_start_batch(ctx, &mut batches.shared, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
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
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_start_batch(ctx, &mut batches.private, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_start_batch(ctx, &mut batches.shared, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as BatchError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => stream
                .abort_start_batch(ctx, &mut flags.private, err)
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                }),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
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
    type CancelPushError = SharedPrivateStreamError<
        Private::CancelPushError,
        Shared::CancelPushError
    >;
    type CancelPushRetry = SharedPrivateStreamRetry<
        Private::CancelPushRetry,
        Shared::CancelPushRetry
    >;
    type PushError =
        SharedPrivateStreamError<Private::PushError, Shared::PushError>;
    type PushRetry =
        SharedPrivateStreamRetry<Private::PushRetry, Shared::PushRetry>;

    fn push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match self {
            SharedPrivateChannelStream::Private { stream } => Ok(stream
                .push(ctx, msg)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            SharedPrivateChannelStream::Shared { stream, party } => Ok(stream
                .push(ctx, once(party.clone()), msg)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id }))
        }
    }

    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        retry: Self::PushRetry
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match (self, retry) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamRetry::Private { retry }
            ) => Ok(stream
                .retry_push(ctx, msg, retry)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_push(ctx, msg, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &T,
        err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_push(ctx, msg, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Private { id: id })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_push(ctx, msg, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })
                .map(|id| SharedPrivateID::Shared { id: id })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .cancel_push(ctx, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .cancel_push(ctx, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamRetry::Shared { retry }
            ) => Ok(stream
                .retry_cancel_push(ctx, retry)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
        }
    }

    fn complete_cancel_push(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        match (self, err) {
            (
                SharedPrivateChannelStream::Private { stream },
                SharedPrivateStreamError::Private { err }
            ) => Ok(stream
                .complete_cancel_push(ctx, err)
                .map_err(|err| SharedPrivateStreamError::Private { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Private {
                    retry: retry
                })),
            (
                SharedPrivateChannelStream::Shared { stream, .. },
                SharedPrivateStreamError::Shared { err }
            ) => Ok(stream
                .complete_cancel_push(ctx, err)
                .map_err(|err| SharedPrivateStreamError::Shared { err: err })?
                .map_retry(|retry| SharedPrivateStreamRetry::Shared {
                    retry: retry
                })),
            _ => Err(SharedPrivateStreamError::Mismatch)
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

impl<Private, Shared> Display for SharedPrivateID<Private, Shared>
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
            SharedPrivateID::Private { id } => id.fmt(f),
            SharedPrivateID::Shared { id } => id.fmt(f)
        }
    }
}

impl<Private, Shared> Display for SharedPrivateChannelParam<Private, Shared>
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
            SharedPrivateChannelParam::Private { param } => param.fmt(f),
            SharedPrivateChannelParam::Shared { param } => param.fmt(f)
        }
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

impl<Private, Shared> Display for SharedPrivateStreamError<Private, Shared>
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
            SharedPrivateStreamError::Private { err } => err.fmt(f),
            SharedPrivateStreamError::Shared { err } => err.fmt(f),
            SharedPrivateStreamError::Mismatch => {
                write!(f, "mismatched param and addr")
            }
        }
    }
}

impl<Private, Shared> Display for SharedPrivateChannelAddr<Private, Shared>
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
            SharedPrivateChannelAddr::Private { addr } => addr.fmt(f),
            SharedPrivateChannelAddr::Shared { addr } => addr.fmt(f)
        }
    }
}
