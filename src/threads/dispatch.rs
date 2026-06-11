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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::net::PrivateMsgs;
use constellation_common::retry::next_retry_definite;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use mio::Events;
use mio::Poll;
use mio::Registry;
use mio::Token;
use mio::Waker;

use crate::channels::ChannelParam;
use crate::channels::Channels;
use crate::channels::ChannelsCreate;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::stream::PullStream;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::PushModeResult;
use crate::threads::RegistryCtx;
use crate::threads::Tokens;
use crate::threads::TokensCtx;

pub trait DispatchInboundTypes {
    type InMsg;
    type Wrapper;
    type OutMsg: Send;
    type SessionPrin: Clone + Display + Eq + Hash + Send;
    type MsgPrin: Clone + Display + Eq + Hash;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type MsgAuthError: Debug + Display + ScopedError;
    type MsgAuth: Clone
        + MsgAuthN<
            Self::InMsg,
            Self::Wrapper,
            Prin = Self::MsgPrin,
            SessionPrin = Self::SessionPrin,
            AuthNMsg = Self::AuthNMsg,
            Error = Self::MsgAuthError
        > + Send;
}

pub trait DispatchEntryTypes<Ctx>: DispatchInboundTypes {
    type Addr: Clone + Debug + Display + Eq + Hash + Send;
    type ChannelParam: Clone
        + Debug
        + Display
        + Eq
        + Hash
        + ChannelParam<Self::Addr>
        + Send;
    type ChannelID: Clone + Debug + Display + Eq + Hash + Send;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen + Send;
    type RefreshCompletableError: ScopedError + Send;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<
            Completable = Self::RefreshCompletableError,
            Permanent = Self::RefreshPermanentError
        >;
    type ReportStreamError: Debug + Display + ScopedError;
    type Stream: StreamRefresh<
            DispatchThreadCtx<Self::Chans, Ctx>,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        > + StreamReporter<
            Self::SessionPrin,
            StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
            Self::AuthNChan,
            ReportStreamError = Self::ReportStreamError
        > + Send;
    type Msgs: PrivateMsgs<Self::OutMsg> + Send;
    type RecvError: Debug + Display + ScopedError;
    type Recv: AuthNMsgRecv<
            Self::MsgPrin,
            Self::InMsg,
            Self::AuthNMsg,
            RecvError = Self::RecvError
        > + Send;
    type Chan: Credentials
        + PullStream<Self::Wrapper, PullError = Self::PullError>;
    type AuthNChan: Clone + AuthNed<Self::SessionPrin, Self::Chan> + Send;
    type ModeConfig: Clone + Send;
    type ModeCreateError: Debug + Display;
    type Mode: PushMode<Self::Stream, Self::Msgs, DispatchThreadCtx<Self::Chans, Ctx>>
        + for<'a> CreateWithParam<
            &'a Self::Stream,
            Config = Self::ModeConfig,
            CreateError = Self::ModeCreateError
        > + Send;
    type ChansSrcs;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type ChanShutdownError: Debug + Display;
    type Chans: ChannelsCreate<
            Ctx,
            Self::ChansSrcs,
            Config = Self::ChansConfig,
            CreateError = Self::ChansCreateError
        > + Channels<
            Ctx,
            Addr = Self::Addr,
            Param = Self::ChannelParam,
            Stream = Self::AuthNChan,
            ChannelID = Self::ChannelID
        > + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx, ShutdownStreamError = Self::ChanShutdownError>
        + Send;
}

pub trait DispatchTypes<Ctx>: DispatchEntryTypes<Ctx> + Sized {
    type DispatchError: Debug + Display + ScopedError;
    type Disp: Dispatch<
            Self,
            DispatchThreadCtx<Self::Chans, Ctx>,
            Msgs = Self::Msgs,
            Recv = Self::Recv,
            PushStream = Self::Stream,
            DispatchError = Self::DispatchError
        > + Send;
}

/// Trait for session dispatchers.
///
/// Instances of this respond to new incoming session principals by
/// setting up whatever application-layer handling is needed, then
/// returning information needed to complete the setup of stream-level
/// communications.
///
/// # Type Parameters
///
/// - `Ctx`: Type of context objects.
///
/// - `Types`: [DispatchInboundTypes] type trait defining the message and
///   authentication types.
pub trait Dispatch<Types, Ctx>
where
    Types: DispatchInboundTypes {
    /// Type of top-level push-side streams to be returned from
    /// dispatch.
    type PushStream;
    /// Type of outbound message structures.
    ///
    /// This will be used by the created [PushStreamPrivateThread] to
    /// obtain messages to be sent using the
    /// [PushStream](Dispatch::PushStream) instance.
    type Msgs: PrivateMsgs<Types::OutMsg>;
    /// Type of authenticated message receivers.
    ///
    /// This will be used to deliver incoming messages.
    type Recv: AuthNMsgRecv<Types::MsgPrin, Types::InMsg, Types::AuthNMsg>;
    /// Type of errors that can occur during dispatch.
    type DispatchError: Debug + Display + ScopedError;

    /// Obtain the components of a new private session.
    ///
    /// This is called when an inbound channel with a new session
    /// principal is authenticated, to start the session with that
    /// principal.  It will typically set up data structures and a
    /// manager thread for that session.  The necessary components for
    /// setting up the stream-level communications are returned as a
    /// [Dispatched].
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context object to use.
    ///
    /// - `prin`: The new session principal.
    ///
    /// - `notify`: Notifier used to alert the dispatch thread to
    /// changes in outbound messages.
    fn dispatch(
        &mut self,
        ctx: &mut Ctx,
        prin: &Types::SessionPrin,
        notify: Arc<Waker>
    ) -> Result<
        Dispatched<
            Types,
            Types::OutMsg,
            Self::PushStream,
            Self::Msgs,
            Self::Recv
        >,
        Self::DispatchError
    >;
}

/// Session handler for a specific principal.
///
/// This is returned by implementations of [Dispatch] from
/// [dispatch](Dispatch::dispatch) in response to a session with a new
/// session principal.  It contains the information needed to complete
/// setup of new sessions with this principal.
///
/// # Type Parameters
///
/// - `Types`: [DisptachInboundTypes] instance that defines most of the types.
///
/// - `OutMsg`: Type of outbound messages.
///
/// - `Stream`: Type of [PushStream]s used to send messages.
///
/// - `Msgs`: Type of [PrivateMsgs] outbound message box used to generate
///   outbound messages.
///
/// - `Recv`: Type of [AuthNMsgRecv] used to send messages.
pub struct Dispatched<Types, OutMsg, Stream, Msgs, Recv>
where
    Types: DispatchInboundTypes,
    Msgs: PrivateMsgs<OutMsg>,
    Recv: AuthNMsgRecv<Types::MsgPrin, Types::InMsg, Types::AuthNMsg> {
    outmsg: PhantomData<OutMsg>,
    /// Flag used to signal shutdown to the connected thread.
    shutdown: ShutdownFlag,
    /// Message authenticator to use for inbound messages.
    authn: Types::MsgAuth,
    /// Inbound message receiver.
    recv: Recv,
    /// Outbound message box.
    msgs: Msgs,
    /// [PushStream] used to send messages.
    stream: Stream
}

pub struct DispatchedEntry<Types, Ctx>
where
    Types: DispatchEntryTypes<Ctx> {
    ctx: PhantomData<Ctx>,
    pull_streams: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        Types::AuthNChan
    >,
    dispatched: Dispatched<
        Types,
        Types::OutMsg,
        Types::Stream,
        Types::Msgs,
        Types::Recv
    >,
    mode: Types::Mode,
    refresh_complete: Option<Types::RefreshCompletableError>,
    refresh_retry: Option<Types::RefreshRetry>,
    next_refresh: Option<Instant>,
    pending: PushModeResult
}

pub struct DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx> {
    channels: Chans,
    ctx: Ctx,
    poll: Poll,
    tokens: Tokens
}

pub struct DispatchThread<Types, Ctx>
where
    Types: DispatchTypes<Ctx> {
    ctx: DispatchThreadCtx<Types::Chans, Ctx>,
    /// [DispatchedEntry]s, indexed by the [Token]s corresponding to
    /// their [Waker]s.
    dispatched: HashMap<DispatchedID, DispatchedEntry<Types, Ctx>>,
    /// Map from principals to the [DispatchedID]s that index
    /// [DispatchedEntry]s.
    parties: HashMap<Types::SessionPrin, DispatchedID>,
    stream_ids: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        DispatchedID
    >,
    dispatcher: Types::Disp,
    mode_config: Types::ModeConfig,
    shutdown: ShutdownFlag,
    notify: Arc<Waker>,
    nevents: usize
}

/// Newtype to distinguish tokens associated with [DispatchEntry]s
/// from regular tokens.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchedID(Token);

#[derive(Debug)]
pub enum DispatchThreadCreateError<Channels> {
    Channels { err: Channels },
    IO { err: Error }
}

#[derive(Debug)]
pub enum DispatchThreadDispatchError<Mode, Dispatch> {
    Mode { err: Mode },
    Dispatch { err: Dispatch }
}

#[derive(Debug)]
pub enum DispatchThreadHandleMsgError<AuthN, Recv> {
    AuthN { err: AuthN },
    Recv { err: Recv }
}

#[derive(Debug)]
pub enum DispatchEntryRecvError<Pull, AuthN, Recv> {
    Pull {
        err: Pull
    },
    Msg {
        err: DispatchThreadHandleMsgError<AuthN, Recv>
    }
}

#[derive(Debug)]
pub enum DispatchThreadRecvError<ID, Pull, AuthN, Recv> {
    Ent {
        err: DispatchEntryRecvError<Pull, AuthN, Recv>
    },
    NoEnt {
        id: ID,
        token: DispatchedID
    },
    NoToken {
        id: ID
    }
}

impl<Types, OutMsg, Stream, Msgs, Recv>
    Dispatched<Types, OutMsg, Stream, Msgs, Recv>
where
    Types: DispatchInboundTypes,
    Msgs: PrivateMsgs<OutMsg>,
    Recv: AuthNMsgRecv<Types::MsgPrin, Types::InMsg, Types::AuthNMsg>
{
    /// Create a new `Dispatched` from its components.
    ///
    /// # Parameters
    ///
    /// - `shutdown`: A [ShutdownFlag] used to signal any connected thread to
    ///   shut down.  The dispatch thread will set this when it shuts down.
    ///
    /// - `stream`: The [PushStream] used to send messages.
    ///
    /// - `msgs`: The [PrivateMsgs] message outbox used to generate messages to
    ///   send.
    ///
    /// - `authn`: The [MsgAuthN] used to authenticate incoming messages.
    ///
    /// - `recv`: The [AuthNMsgRecv] used to receive incoming messages.
    #[inline]
    pub fn new(
        shutdown: ShutdownFlag,
        stream: Stream,
        msgs: Msgs,
        authn: Types::MsgAuth,
        recv: Recv
    ) -> Self {
        Dispatched {
            outmsg: PhantomData,
            shutdown: shutdown,
            stream: stream,
            authn: authn,
            msgs: msgs,
            recv: recv
        }
    }

    /// Process one incoming message.
    ///
    /// This will perform message authentication, and if successful,
    /// will deliver the message up to the [AuthNMsgRecv].
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier for the session in which `msg` was received.  This is
    ///   used solely for logging.
    ///
    /// - `session_prin`: Session principal for the session.
    ///
    /// - `msg`: The wrapped incoming message.
    fn handle_msg<ID>(
        &mut self,
        id: &ID,
        session_prin: &Types::SessionPrin,
        msg: Types::Wrapper
    ) -> Result<
        (),
        DispatchThreadHandleMsgError<Types::MsgAuthError, Recv::RecvError>
    >
    where
        ID: Display {
        trace!(target: "dispatched",
               "handling incoming message from {} ({})",
               session_prin, id);

        // ISSUE #10: future: unwrap XCIAP here and
        // report successes.

        match self
            .authn
            .msg_authn(session_prin, msg)
            .map_err(|err| DispatchThreadHandleMsgError::AuthN { err: err })?
        {
            AuthNResult::Accept(msg) => {
                trace!(target: "dispatched",
                       "authenticated message from {} ({}) as {}",
                       session_prin, id, msg.prin());

                self.recv.recv_auth_msg(msg).map_err(|err| {
                    DispatchThreadHandleMsgError::Recv { err: err }
                })
            }
            AuthNResult::Reject(_) => {
                warn!(target: "dispatched",
                      "authentication rejected message from {} ({})",
                      session_prin, id);

                Ok(())
            }
        }
    }

    fn complete_refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>,
        err: <Stream::RefreshError as RecoverableError>::Completable
    ) -> RetryResult<Option<Instant>, Stream::RefreshRetry>
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>> {
        self.stream
            .complete_refresh(ctx, err)
            .unwrap_or_else(|err| match err.split() {
                (_, Some(err)) => {
                    error!(target: "dispatched",
                       "unrecoverable error refreshing stream: {}",
                       err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(ctx, err),
                (None, None) => {
                    error!(target: "dispatched",
                       "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            })
    }

    fn retry_refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>,
        retry: Stream::RefreshRetry
    ) -> RetryResult<Option<Instant>, Stream::RefreshRetry>
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>> {
        self.stream.retry_refresh(ctx, retry).unwrap_or_else(|err| {
            match err.split() {
                (_, Some(err)) => {
                    error!(target: "dispatched",
                       "unrecoverable error refreshing stream: {}",
                       err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(ctx, err),
                (None, None) => {
                    error!(target: "dispatched",
                       "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            }
        })
    }

    fn refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>
    ) -> RetryResult<Option<Instant>, Stream::RefreshRetry>
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>> {
        self.stream
            .refresh(ctx)
            .unwrap_or_else(|err| match err.split() {
                (_, Some(err)) => {
                    error!(target: "dispatched",
                       "unrecoverable error refreshing stream: {}",
                       err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(ctx, err),
                (None, None) => {
                    error!(target: "dispatched",
                       "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            })
    }

    /// Shut down this `Dispatched`.
    ///
    /// This will trigger the [ShutdownFlag] associated with this
    /// `Dispatched`.
    #[inline]
    fn shutdown(&mut self) {
        self.shutdown.set();
    }
}

impl<Chans, Ctx> Channels<()> for DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    type Addr = Chans::Addr;
    type ChannelID = Chans::ChannelID;
    type OutNegoParam = Chans::OutNegoParam;
    type Param = Chans::Param;
    type ParamError = Chans::ParamError;
    type ParamsIter<'a>
        = Chans::ParamsIter<'a>
    where
        Self: 'a;
    type ReqStreamError = Chans::ReqStreamError;
    type Stream = Chans::Stream;

    #[inline]
    fn req_stream<'a>(
        &'a mut self,
        _ctx: &'a mut (),
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
        self.channels.req_stream(
            &mut self.ctx,
            channel,
            param,
            endpoint,
            nego_param
        )
    }

    #[inline]
    fn params<'a, I>(
        &'a mut self,
        _ctx: &'a mut (),
        channels: I
    ) -> Result<RetryResult<Self::ParamsIter<'a>>, Self::ParamError>
    where
        I: 'a + Iterator<Item = Self::ChannelID> {
        self.channels.params(&mut self.ctx, channels)
    }

    #[inline]
    fn channel_id(
        &self,
        name: &str
    ) -> Option<Self::ChannelID> {
        self.channels.channel_id(name)
    }
}

impl<Chans, Ctx> TokensCtx for DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    #[inline]
    fn token(&mut self) -> Token {
        self.tokens.token()
    }

    #[inline]
    fn free_token(
        &mut self,
        token: Token
    ) {
        self.tokens.free_token(token)
    }
}

impl<Chans, Ctx> RegistryCtx for DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    #[inline]
    fn registry(&self) -> &Registry {
        self.poll.registry()
    }
}

impl<Chans, Ctx> DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    fn new(
        ctx: Ctx,
        poll: Poll,
        channels: Chans,
        tokens_hint: Option<usize>
    ) -> Self {
        let tokens = match tokens_hint {
            Some(hint) => Tokens::with_capacity(hint),
            None => Tokens::new()
        };

        DispatchThreadCtx {
            channels: channels,
            ctx: ctx,
            poll: poll,
            tokens: tokens
        }
    }
}

impl<Types, Ctx> DispatchedEntry<Types, Ctx>
where
    Types: DispatchEntryTypes<Ctx>
{
    /// Report a new stream for a given principal.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `id`: The ID of the new stream.
    ///
    /// - `stream`: The stream being reported.
    fn recv_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) -> Result<(), Types::ReportStreamError> {
        debug!(target: "dispatched-entry",
               "receiving stream from {} for {}",
               id, stream.prin());

        // Report up to the stream.
        let res = self.dispatched.stream.report_stream(
            stream.prin(),
            id.clone(),
            stream.clone()
        )?;
        let stream = match res {
            Some(existing) => {
                warn!(target: "dispatch-entry",
                      "stream {} with {} was already present",
                      id, stream.prin());

                if let Err(err) = ctx.channels.shutdown_stream(
                    &mut ctx.ctx,
                    id.channel(),
                    id.param(),
                    stream
                ) {
                    error!(target: "dispatch-thread",
                           "error shutting down stream {}: {}",
                           id, err);
                }

                existing
            }
            None => stream
        };

        // Insert into the pull streams.
        if self
            .pull_streams
            .insert(id.clone(), stream.clone())
            .is_some()
        {
            error!(target: "dispatch-entry",
                   "stream {} was already present for {}",
                   id, stream.prin());
        }

        Ok(())
    }

    /// Pull messages from a given stream.
    ///
    /// # Parameters
    ///
    /// - `id`: Stream from which to pull messages.
    fn pull_msgs(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>
    ) -> Result<
        (),
        DispatchEntryRecvError<
            Types::PullError,
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        debug!(target: "dispatch-entry",
               "pulling messages from {}",
               id);

        if let Some(stream) = self.pull_streams.get_mut(&id) {
            let mut valid = true;

            while self.dispatched.shutdown.is_live() && valid {
                trace!(target: "dispatch-entry",
                       "listening for message on {}",
                       id);

                match stream.get_mut().pull() {
                    Ok(msg) => self
                        .dispatched
                        .handle_msg(&id, stream.prin(), msg)
                        .map_err(|err| DispatchEntryRecvError::Msg {
                            err: err
                        })?,
                    Err(err) => match err.scope() {
                        ErrorScope::Retryable => {
                            error!(target: "dispatch-entry",
                                   "shouldn't see a retryable error here")
                        }
                        ErrorScope::WouldBlock => {
                            trace!(target: "dispatch-entry",
                                   "exhausted messages on {}",
                                   id);

                            valid = false;
                        }
                        ErrorScope::Unrecoverable |
                        ErrorScope::Session |
                        ErrorScope::System |
                        ErrorScope::Shutdown => {
                            return Err(DispatchEntryRecvError::Pull {
                                err: err
                            })
                        }
                        _ => {
                            error!(target: "dispatch-entry",
                                   "error receiving message: {}",
                                   err);
                        }
                    }
                }
            }
        } else {
            error!(target: "dispatch-entry",
                   "stream not found for {}",
                   id);
        }

        Ok(())
    }

    fn handle_refresh_stream_error(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        err: Types::RefreshError
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        match err.split() {
            (_, Some(err)) => {
                error!(target: "dispatched-entry",
                       "unrecoverable error refreshing stream: {}",
                       err);

                RetryResult::Success(None)
            }
            (Some(err), _) => {
                if err.scope() == ErrorScope::WouldBlock {
                    self.refresh_complete = Some(err);

                    RetryResult::Success(None)
                } else {
                    self.complete_refresh_stream(ctx, err)
                }
            }
            (None, None) => {
                error!(target: "dispatched-entry",
                       "refresh error split produced no results");

                RetryResult::Success(None)
            }
        }
    }

    fn complete_refresh_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        err: Types::RefreshCompletableError
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.dispatched
            .stream
            .complete_refresh(ctx, err)
            .unwrap_or_else(|err| self.handle_refresh_stream_error(ctx, err))
    }

    /// Do necessary state updates for a refresh result.
    fn handle_refresh_result(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        res: RetryResult<Option<Instant>, Types::RefreshRetry>
    ) {
        match res {
            RetryResult::Success(when) => {
                self.next_refresh = when;

                // We succeeded; retry indefinites.
                if let Err(err) = self.mode.retry_indefs(
                    ctx,
                    &mut self.dispatched.msgs,
                    &mut self.dispatched.stream
                ) {
                    error!(target: "dispatch-entry",
                           "error retrying refresh: {}",
                           err)
                }
            }
            RetryResult::Retry(retry) => self.refresh_retry = Some(retry)
        }
    }

    fn needs_refresh(
        &self,
        now: Instant
    ) -> bool {
        self.refresh_complete.is_some() ||
            self.next_refresh.map_or(false, |when| when <= now) ||
            self.refresh_retry
                .as_ref()
                .map_or(false, |retry| retry.when() <= now)
    }

    fn refresh_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        need_refresh: bool,
        now: Instant
    ) {
        // Check if there's a pending completion.
        if let Some(refresh_complete) = self.refresh_complete.take() {
            // There's a pending completion; run it.
            let res = self.complete_refresh_stream(ctx, refresh_complete);

            self.handle_refresh_result(ctx, res)
        // Check if there's a pending retry.
        } else if let Some(retry) = self.refresh_retry.take() {
            // There's a pending retry; see if it's time yet.
            if retry.when() < now {
                trace!(target: "dispatch-entry",
                       "retrying stream refresh");

                let res = self.dispatched.retry_refresh_stream(ctx, retry);

                self.handle_refresh_result(ctx, res)
            } else {
                self.refresh_retry = Some(retry)
            }
        // Check if we need to start a new retry.
        } else if self.next_refresh.map_or(false, |when| when <= now) ||
            need_refresh
        {
            trace!(target: "dispatch-entry",
                   "refreshing stream");

            self.next_refresh = None;

            let res = self.dispatched.refresh_stream(ctx);

            self.handle_refresh_result(ctx, res);
        }
    }

    /// Complete pending push operations if necessary.
    fn complete_pending(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>
    ) {
        if self.pending.take_has_completes() {
            match self.mode.complete_pending(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                &live
            ) {
                Ok(res) => self.pending.merge(&res),
                Err(err) => {
                    error!(target: "dispatched-entry",
                           "error completing stalled sends: {}",
                           err);
                }
            }
        }
    }

    /// Retry pending push operations if necessary.
    fn retry_pending(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) {
        if self
            .pending
            .retry_pending()
            .map_or(false, |when| when <= now)
        {
            let _ = self.pending.take_retry_pending();

            trace!(target: "dispatch-entry",
                   "retrying pending messages");

            match self.mode.retry_pending(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                &live,
                now
            ) {
                Ok(res) => {
                    self.pending.merge(&res);
                }
                Err(err) => {
                    error!(target: "dispatch-entry",
                           "error retrying pending messages: {}",
                           err);
                }
            }
        }
    }

    /// Push messages if it's time to do so.
    fn push_outbound_msgs(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) {
        if self
            .pending
            .next_outbound()
            .map_or(false, |when| when <= now)
        {
            trace!(target: "dispatch-entry",
                   "pushing messages");

            let _ = self.pending.take_next_outbound();

            match self.mode.send_from_outbound(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                live
            ) {
                Ok(res) => {
                    self.pending.merge(&res);
                }
                Err(err) => {
                    error!(target: "dispatch-entry",
                           "error sending messages: {}",
                           err);
                }
            }
        }
    }

    fn shutdown(
        mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>
    ) {
        // Shut down all streams.
        for (id, stream) in self.pull_streams.into_iter() {
            debug!(target: "poll-thread",
                   "shutting down stream {} with {}",
                   id, stream.prin());

            if let Err(err) = ctx.channels.shutdown_stream(
                &mut ctx.ctx,
                id.channel(),
                id.param(),
                stream
            ) {
                error!(target: "poll-thread",
                       "error shutting down stream {}: {}",
                       id, err);
            }
        }

        self.dispatched.shutdown()
    }
}

impl<Types, Ctx> DispatchThread<Types, Ctx>
where
    Types: 'static + DispatchTypes<Ctx>,
    Ctx: 'static + Send
{
    pub fn create(
        mode_config: Types::ModeConfig,
        chans_config: Types::ChansConfig,
        srcs: Types::ChansSrcs,
        dispatcher: Types::Disp,
        mut ctx: Ctx,
        shutdown: ShutdownFlag,
        nevents: usize,
        nsessions: Option<usize>,
        ndispatched: Option<usize>,
        tokens_hint: Option<usize>
    ) -> Result<Self, DispatchThreadCreateError<Types::ChansCreateError>> {
        let channels = Types::Chans::create(&mut ctx, chans_config, srcs)
            .map_err(|err| DispatchThreadCreateError::Channels { err: err })?;
        let poll = Poll::new()
            .map_err(|err| DispatchThreadCreateError::IO { err: err })?;
        let tokens_hint =
            tokens_hint.or_else(|| match (nsessions, ndispatched) {
                (Some(nsessions), Some(ndispatched)) => {
                    Some((nsessions * ndispatched) + (2 * ndispatched) + 1)
                }
                _ => None
            });
        let mut ctx = DispatchThreadCtx::new(ctx, poll, channels, tokens_hint);
        let (dispatched, parties, stream_ids) = match ndispatched {
            Some(ndispatched) => (
                HashMap::with_capacity(ndispatched),
                HashMap::with_capacity(ndispatched),
                HashMap::with_capacity(ndispatched)
            ),
            None => (HashMap::new(), HashMap::new(), HashMap::new())
        };
        let token = ctx.token();
        let notify = Waker::new(ctx.registry(), token.clone())
            .map_err(|err| DispatchThreadCreateError::IO { err: err })?;
        let notify = Arc::new(notify);

        Ok(DispatchThread {
            mode_config: mode_config,
            dispatcher: dispatcher,
            shutdown: shutdown,
            dispatched: dispatched,
            stream_ids: stream_ids,
            parties: parties,
            ctx: ctx,
            nevents: nevents,
            notify: notify
        })
    }

    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
    }

    /// Install a newly dispatched session into the tables.
    fn setup_dispatched(
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        ents: &mut HashMap<DispatchedID, DispatchedEntry<Types, Ctx>>,
        stream_ids: &mut HashMap<
            StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
            DispatchedID
        >,
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        dispatched: Dispatched<
            Types,
            Types::OutMsg,
            Types::Stream,
            Types::Msgs,
            Types::Recv
        >,
        mode: Types::Mode,
        stream: Types::AuthNChan,
        token: DispatchedID
    ) {
        // XXX use a size hint here.
        let pull_streams = HashMap::new();
        let now = Instant::now();
        let pending = PushModeResult::new(Some(now), None, false);
        let mut dispatched = DispatchedEntry {
            ctx: PhantomData,
            dispatched: dispatched,
            pull_streams: pull_streams,
            mode: mode,
            pending: pending,
            next_refresh: Some(now),
            refresh_retry: None,
            refresh_complete: None
        };

        match dispatched.recv_stream(ctx, id.clone(), stream) {
            // Receive went through; add the stream ID entry.
            Ok(()) => {
                if let Some(token) =
                    stream_ids.insert(id.clone(), token.clone())
                {
                    error!(target: "dispatch-thread",
                       "existing stream ID entry for {}: {}",
                       id, token);
                }
            }
            Err(err) => {
                error!(target: "dispatch-thread",
                       "error reporting stream {}: {}",
                       id, err);
            }
        }

        if ents.insert(token.clone(), dispatched).is_some() {
            error!(target: "dispatch-thread",
                   "existing entry for token {}",
                   token);
        }
    }

    /// Take a new session and install it into the system.
    ///
    /// If the session corresponds to an existing principal, no
    /// dispatch will be performed; rather, the session will be
    /// installed into that principal's [DispatchedEntry].
    ///
    /// If the session corresponds to a new principal, then a dispatch
    /// will be performed to set up the application layer processing.
    ///
    /// # Parameters
    ///
    /// - `id`: The ID of the stream.
    ///
    /// - `session`: The authenticated session.
    fn recv_session(
        &mut self,
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        session: Types::AuthNChan
    ) -> Option<DispatchedID> {
        match self.parties.entry(session.prin().clone()) {
            Entry::Occupied(token) => {
                match self.dispatched.get_mut(token.get()) {
                    Some(ent) => match ent.recv_stream(
                        &mut self.ctx,
                        id.clone(),
                        session
                    ) {
                        // Receive went through; add the stream ID entry.
                        Ok(()) => {
                            if let Some(token) = self
                                .stream_ids
                                .insert(id.clone(), token.get().clone())
                            {
                                error!(target: "dispatch-thread",
                                   "existing stream ID entry for {}: {}",
                                   id, token);
                            }

                            Some(token.get().clone())
                        }
                        Err(err) => {
                            error!(target: "dispatch-thread",
                               "error reporting stream {}: {}",
                               id, err);

                            None
                        }
                    },
                    None => {
                        error!(target: "dispatch-thread",
                           "missing dispatch entry for {} ({})",
                           session.prin(), token.get());

                        if let Err(err) = self.ctx.channels.shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            session
                        ) {
                            error!(target: "dispatch-thread",
                               "error shutting down stream {}: {}",
                               id, err);
                        }

                        Some(token.get().clone())
                    }
                }
            }
            Entry::Vacant(ent) => {
                debug!(target: "dispatch-thread",
                       "dispatching for {}",
                       session.prin());

                let token = self.ctx.tokens.token();

                match Waker::new(self.ctx.poll.registry(), token.clone()) {
                    Ok(notify) => match self.dispatcher.dispatch(
                        &mut self.ctx,
                        session.prin(),
                        Arc::new(notify)
                    ) {
                        Ok(dispatched) => match Types::Mode::create(
                            self.mode_config.clone(),
                            &dispatched.stream
                        ) {
                            Ok(mode) => {
                                let token = DispatchedID(token);

                                Self::setup_dispatched(
                                    &mut self.ctx,
                                    &mut self.dispatched,
                                    &mut self.stream_ids,
                                    id,
                                    dispatched,
                                    mode,
                                    session,
                                    token.clone()
                                );
                                ent.insert(token.clone());

                                Some(token)
                            }
                            Err(err) => {
                                error!(target: "dispatch-thread",
                                       "error creating push mode for {}: {}",
                                       session.prin(), err);

                                self.ctx.tokens.free_token(token);

                                None
                            }
                        },
                        Err(err) => {
                            error!(target: "dispatch-thread",
                                   "error dispatching for {}: {}",
                                   session.prin(), err);

                            if let Err(err) = self.ctx.channels.shutdown_stream(
                                &mut self.ctx.ctx,
                                id.channel(),
                                id.param(),
                                session
                            ) {
                                error!(target: "dispatch-thread",
                                       "error shutting down stream {}: {}",
                                       id, err);
                            }

                            self.ctx.tokens.free_token(token);

                            None
                        }
                    },
                    Err(err) => {
                        error!(target: "dispatch-thread",
                               "error creating notifier for {}: {}",
                               session.prin(), err);

                        if let Err(err) = self.ctx.channels.shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            session
                        ) {
                            error!(target: "dispatch-thread",
                                   "error shutting down stream {}: {}",
                                   id, err);
                        }

                        self.ctx.tokens.free_token(token);

                        None
                    }
                }
            }
        }
    }

    /// Pull messages from the given stream.
    fn pull_msgs(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>
    ) -> Result<
        (),
        DispatchThreadRecvError<
            StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
            Types::PullError,
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        let token = self
            .stream_ids
            .get(id)
            .ok_or(DispatchThreadRecvError::NoToken { id: id.clone() })?;
        let ent = self.dispatched.get_mut(token).ok_or(
            DispatchThreadRecvError::NoEnt {
                id: id.clone(),
                token: token.clone()
            }
        )?;

        ent.pull_msgs(id)
            .map_err(|err| DispatchThreadRecvError::Ent { err: err })
    }

    fn handle_events(
        &mut self,
        next_listen: &mut Option<Instant>,
        live: HashSet<Token>,
        refreshes: Option<Vec<DispatchedID>>,
        outbounds: Option<Vec<DispatchedID>>,
        retries: Option<Vec<DispatchedID>>,
        completes: Option<Vec<DispatchedID>>,
        now: Instant
    ) -> bool {
        let mut valid = true;

        // XXX We ought to be able to filter the dispatched sessions
        // by the tokens in live.  Note that this is not just
        // filtering them by the keys for self.dispatched.

        // If we have pending completes, run them.
        if let Some(completes) = completes {
            for token in completes.into_iter() {
                if let Some(ent) = self.dispatched.get_mut(&token) {
                    ent.complete_pending(&mut self.ctx, &live)
                } else {
                    // This shouldn't happen.
                    error!(target: "dispatch-thread",
                           "entry for complete for {} not found",
                           token);
                }
            }
        }

        // If we have pending retries, run them.
        if let Some(retries) = retries {
            for token in retries.into_iter() {
                // Look up the dispatched entry.
                if let Some(ent) = self.dispatched.get_mut(&token) {
                    ent.retry_pending(&mut self.ctx, &live, now)
                } else {
                    // This shouldn't happen.
                    error!(target: "dispatch-thread",
                           "entry for retry for {} not found",
                           token);
                }
            }
        }

        // Do pulls before pushing new messages.
        let need_refreshes = if next_listen.map_or(false, |when| when <= now) {
            let mut need_refreshes =
                HashSet::with_capacity(self.dispatched.len());

            trace!(target: "dispatch-thread",
                   "listening");

            match self.ctx.channels.listen(&mut self.ctx.ctx, &live) {
                Ok(RetryResult::Success((
                    streams,
                    endpoints,
                    // XXX Uncertain relationship here with the refresh field.
                    //
                    // This appears to signal the need to do a refresh
                    // for a single stream, but this doesn't
                    // generalize to multiple sessions.  We would need
                    // to indicate stream IDs here or something.
                    refresh,
                    when
                ))) => {
                    *next_listen = when;

                    // Report new streams.
                    for (addr, channel_id, param, stream) in streams {
                        let id = StreamID::new(addr, channel_id, param);

                        if let Some(token) = self.recv_session(id, stream) {
                            if need_refreshes.insert(token.clone()) {
                                error!(target: "dispatch-thread",
                                       "{} already in needed refreshes",
                                       token);
                            }
                        }
                    }

                    // Pull in messages from all active streams.
                    for (addr, channel_id, param) in endpoints {
                        let id = StreamID::new(addr, channel_id, param);

                        if let Err(err) = self.pull_msgs(&id) {
                            error!(target: "dispatch-thread",
                                   "error receiving messages from {}: {}",
                                   id, err);

                            valid = false;
                        }
                    }
                }
                Ok(RetryResult::Retry(when)) => {
                    *next_listen = Some(when);
                }
                Err(err) => {
                    error!(target: "dispatch-thread",
                           "error listening: {}",
                           err);
                }
            }

            Some(need_refreshes)
        } else {
            None
        };

        // If we have pending refreshes, run them.
        let refreshes: HashSet<DispatchedID> = if let Some(refreshes) =
            refreshes
        {
            // Deduplicate the refreshed tokens from both the incoming
            // refreshes, as well as the new tokens in need_refreshes.
            if let Some(need_refreshes) = &need_refreshes {
                refreshes
                    .into_iter()
                    .chain(need_refreshes.iter().cloned())
                    .collect()
            } else {
                refreshes.into_iter().collect()
            }
        } else {
            need_refreshes
                .as_ref()
                .map_or(HashSet::new(), |need_refreshes| need_refreshes.clone())
        };

        for token in refreshes.into_iter() {
            if let Some(ent) = self.dispatched.get_mut(&token) {
                // Complete the pending operation; record a new
                // pending operation if it returns a time.
                let need_refresh =
                    need_refreshes.as_ref().map_or(false, |need_refreshes| {
                        need_refreshes.contains(&token)
                    });

                ent.refresh_stream(&mut self.ctx, need_refresh, now)
            } else {
                // This shouldn't happen.
                error!(target: "dispatch-thread",
                       "entry for pending for {} not found",
                       token);
            }
        }

        // Finally, if there are outbound messages pending, send them.
        let outbounds: Vec<DispatchedID> = if let Some(outbounds) = outbounds {
            outbounds
                .into_iter()
                .chain(
                    live.iter()
                        .cloned()
                        .map(DispatchedID)
                        .filter(|id| self.dispatched.contains_key(&id))
                )
                .collect()
        } else {
            live.iter()
                .cloned()
                .map(DispatchedID)
                .filter(|id| self.dispatched.contains_key(&id))
                .collect()
        };

        for token in outbounds.into_iter() {
            if let Some(ent) = self.dispatched.get_mut(&token) {
                ent.push_outbound_msgs(&mut self.ctx, &live, now)
            } else {
                // This shouldn't happen.
                error!(target: "dispatch-thread",
                       "entry for send for {} not found",
                       token);
            }
        }

        valid
    }

    fn run(mut self) {
        let mut events = Events::with_capacity(self.nevents);
        let mut next_listen = None;
        let mut outbounds: Option<Vec<DispatchedID>> = None;
        let mut retries: Option<Vec<DispatchedID>> = None;
        let mut completes: Option<Vec<DispatchedID>> = None;
        let mut refreshes: Option<Vec<DispatchedID>> = None;
        let mut now;

        info!(target: "dispatch-thread",
              "mio polling thread starting");

        while {
            let mut next = next_listen;
            let nents = self.dispatched.len();

            now = Instant::now();

            for (id, ent) in self.dispatched.iter() {
                if ent.needs_refresh(now) {
                    match &mut refreshes {
                        Some(completes) => completes.push(id.clone()),
                        None => {
                            let mut vec = Vec::with_capacity(nents);

                            vec.push(id.clone());
                            refreshes = Some(vec);
                        }
                    }
                }

                if ent.pending.has_completes() {
                    match &mut completes {
                        Some(completes) => completes.push(id.clone()),
                        None => {
                            let mut vec = Vec::with_capacity(nents);

                            vec.push(id.clone());
                            completes = Some(vec);
                        }
                    }
                }

                if let Some(when) = ent.pending.next_outbound() {
                    if when < now {
                        match &mut outbounds {
                            Some(outbounds) => {
                                next = Some(next_retry_definite(&next, &when));
                                outbounds.push(id.clone())
                            }
                            None => {
                                let mut vec = Vec::with_capacity(nents);

                                next = Some(next_retry_definite(&next, &when));
                                vec.push(id.clone());
                                outbounds = Some(vec);
                            }
                        }
                    }
                }

                if let Some(when) = ent.pending.retry_pending() {
                    if when < now {
                        match &mut retries {
                            Some(retries) => {
                                next = Some(next_retry_definite(&next, &when));
                                retries.push(id.clone())
                            }
                            None => {
                                let mut vec = Vec::with_capacity(nents);

                                next = Some(next_retry_definite(&next, &when));
                                vec.push(id.clone());
                                retries = Some(vec);
                            }
                        }
                    }
                }
            }

            self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                (completes.is_some() ||
                 next.is_some_and(|next: Instant| next < now) ||
                 {
                     let duration = next.map(|next| next - now);

                     if let Some(duration) = &duration {
                         trace!(target: "dispatch-thread",
                                "waiting for poll for {}.{:03}",
                                duration.as_secs(), duration.subsec_millis());
                     } else {
                         trace!(target: "dispatch-thread",
                                "waiting for poll indefinitely");
                     }

                     self.ctx
                         .poll
                         .poll(&mut events, duration)
                         .inspect_err(|err| {
                             error!(target: "dispatch-thread",
                                    "error polling: {}",
                                    err)
                         })
                         .is_ok()
                 })
        } {
            // Gather up all the events.
            let live: HashSet<Token> =
                events.iter().map(|event| event.token()).collect();

            if !self.handle_events(
                &mut next_listen,
                live,
                refreshes.take(),
                outbounds.take(),
                retries.take(),
                completes.take(),
                now
            ) {
                break;
            }
        }

        self.shutdown(events)
    }

    fn shutdown(
        self,
        mut events: Events
    ) {
        let DispatchThread {
            mut ctx,
            mut dispatched,
            parties,
            ..
        } = self;
        info!(target: "dispatch-thread",
              "mio dispatch thread shutting down");

        // Shutdown all dispatched entries.
        for (party, token) in parties.into_iter() {
            if let Some(ent) = dispatched.remove(&token) {
                info!(target: "dispatch-thread",
                      "shutting down dispatched entry for {}",
                      party);

                ent.shutdown(&mut ctx)
            } else {
                trace!(target: "dispatch-thread",
                       "entry missing for {}, {}",
                       party, token);
            }
        }

        // Empty out the remaining tokens.
        for (token, ent) in dispatched.into_iter() {
            warn!(target: "dispatch-thread",
                  "shutting down dispatched entry for {} with no party",
                  token);

            ent.shutdown(&mut ctx)
        }

        let mut live = true;
        let mut next = None;

        while {
            let now = Instant::now();

            live && (next.is_some_and(|next: Instant| next < now) || {
                let duration = next.map(|next| next - now);

                if let Some(duration) = &duration {
                    trace!(target: "dispatch-thread",
                                "waiting for poll for {}.{:03}",
                                duration.as_secs(), duration.subsec_millis());
                } else {
                    trace!(target: "dispatch-thread",
                                "waiting for poll indefinitely");
                }

                ctx.poll
                    .poll(&mut events, duration)
                    .inspect_err(|err| {
                        error!(target: "dispatch-thread",
                                    "error polling: {}",
                                    err)
                    })
                    .is_ok()
            })
        } {
            // Gather up all the events.
            let tokens: HashSet<Token> =
                events.iter().map(|event| event.token()).collect();

            next = None;

            match ctx.channels.shutdown_listen(&mut ctx.ctx, &tokens) {
                Ok(RetryResult::Success(res)) => {
                    live = res;
                }
                Ok(RetryResult::Retry(when)) => next = Some(when),
                Err(err) => {
                    error!(target: "dispatch-thread",
                           "error listening during shutdown: {}",
                           err);

                    live = false;
                }
            }
        }

        if let Err(err) = ctx.channels.shutdown(&mut ctx.ctx) {
            error!(target: "dispatch-thread",
                   "error shutting down channels: {}",
                   err);
        }

        info!(target: "dispatch-thread",
              "mio dispatch thread exiting");
    }

    pub fn start(self) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("poll-thread"))
            .spawn(move || self.run())
    }
}

impl Display for DispatchedID {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        write!(f, "dispatched {}", self.0 .0)
    }
}

impl<Channels> Display for DispatchThreadCreateError<Channels>
where
    Channels: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadCreateError::Channels { err } => err.fmt(f),
            DispatchThreadCreateError::IO { err } => write!(f, "{}", err)
        }
    }
}

impl<Mode, Dispatch> Display for DispatchThreadDispatchError<Mode, Dispatch>
where
    Dispatch: Display,
    Mode: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadDispatchError::Dispatch { err } => err.fmt(f),
            DispatchThreadDispatchError::Mode { err } => err.fmt(f)
        }
    }
}

impl<AuthN, Recv> Display for DispatchThreadHandleMsgError<AuthN, Recv>
where
    AuthN: Display,
    Recv: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadHandleMsgError::AuthN { err } => err.fmt(f),
            DispatchThreadHandleMsgError::Recv { err } => err.fmt(f)
        }
    }
}

impl<Pull, AuthN, Recv> Display for DispatchEntryRecvError<Pull, AuthN, Recv>
where
    Pull: Display,
    AuthN: Display,
    Recv: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchEntryRecvError::Pull { err } => err.fmt(f),
            DispatchEntryRecvError::Msg { err } => err.fmt(f)
        }
    }
}

impl<ID, Pull, AuthN, Recv> Display
    for DispatchThreadRecvError<ID, Pull, AuthN, Recv>
where
    ID: Display,
    Pull: Display,
    AuthN: Display,
    Recv: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadRecvError::Ent { err } => err.fmt(f),
            DispatchThreadRecvError::NoEnt { id, token } => {
                write!(f, "no dispatched entry for {} ({})", id, token)
            }
            DispatchThreadRecvError::NoToken { id } => {
                write!(f, "no token entry for {}", id)
            }
        }
    }
}
