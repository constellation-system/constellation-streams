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

use std::convert::Infallible;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::config::Create;
use constellation_common::error::ScopedError;
use constellation_common::net::PrivateMsgs;
use constellation_common::error::RecoverableError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::warn;
use log::info;
use log::trace;
use mio::Events;
use mio::Poll;
use mio::Registry;
use mio::Token;
use mio::Waker;

use crate::channels::Channels;
use crate::channels::ChannelParam;
use crate::channels::ChannelsCreate;
use crate::stream::PullStream;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::RegistryCtx;
use crate::threads::Tokens;
use crate::threads::TokensCtx;

pub trait DispatchInboundTypes {
    type InMsg;
    type Wrapper;
    type OutMsg;
    type SessionPrin: Clone + Display + Eq + Hash;
    type MsgPrin: Clone + Display + Eq + Hash;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type MsgAuthError: Debug + Display + ScopedError;
    type MsgAuth: Clone + MsgAuthN<Self::InMsg, Self::Wrapper,
                                   Prin = Self::MsgPrin,
                                   SessionPrin = Self::SessionPrin,
                                   AuthNMsg = Self::AuthNMsg,
                                   Error = Self::MsgAuthError>;
}

pub trait DispatchEntryTypes<Ctx>: DispatchInboundTypes {
    type Addr: Clone + Debug + Display + Eq + Hash;
    type ChannelParam: Clone + Debug + Display + Eq + Hash
        + ChannelParam<Self::Addr>;
    type ChannelID: Clone + Debug + Display + Eq + Hash;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<Completable = Self::RefreshCompletableError,
                           Permanent = Self::RefreshPermanentError>;
    type Stream: StreamRefresh<
        DispatchThreadCtx<Self::Chans, Ctx>,
        RefreshRetry = Self::RefreshRetry,
        RefreshError = Self::RefreshError
    > + StreamReporter<
        Self::SessionPrin,
        StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
        Self::AuthNChan
    >;
    type Msgs: PrivateMsgs<Self::OutMsg>;
    type RecvError: Debug + Display + ScopedError;
    type Recv: AuthNMsgRecv<Self::MsgPrin, Self::InMsg, Self::AuthNMsg,
                            RecvError = Self::RecvError>;
    type Chan: Credentials + PullStream<Self::Wrapper>;
    type AuthNChan: Clone + AuthNed<Self::SessionPrin, Self::Chan>;
    type ModeConfig: Clone;
    type ModeCreateError: Debug + Display;
    type Mode: PushMode<
        Self::Stream,
        Self::Msgs,
        DispatchThreadCtx<Self::Chans, Ctx>,
        Config = Self::ModeConfig,
        CreateError = Self::ModeCreateError
    >;
    type ChansSrcs;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type Chans:
        ChannelsCreate<Ctx, Self::ChansSrcs,
                       Config = Self::ChansConfig,
                       CreateError = Self::ChansCreateError>
        + Channels<Ctx,
                   Addr = Self::Addr,
                   Param = Self::ChannelParam,
                   Stream = Self::AuthNChan,
                   ChannelID = Self::ChannelID>;
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
    >;
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
/// - `Types`: [DispatchInboundTypes] type trait defining the message
///   and authentication types.
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
        notify: Arc<Waker>,
    ) -> Result<
        Dispatched<Types, Types::OutMsg, Self::PushStream,
                   Self::Msgs, Self::Recv>,
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
/// - `Msgs`: Type of [PrivateMsgs] outbound message box used to
///   generate outbound messages.
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
    stream: Stream,
}

pub struct DispatchedEntry<Types, Ctx>
where
    Types: DispatchEntryTypes<Ctx>,
{
    ctx: PhantomData<Ctx>,
    dispatched: Dispatched<Types, Types::OutMsg, Types::Stream,
                           Types::Msgs, Types::Recv>,
    pull_streams: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        Types::AuthNChan
    >,
    mode: Types::Mode,
    retry_refresh: Option<Types::RefreshRetry>,
    next_refresh: Option<Instant>,
    next_outbound: Option<Instant>,
}

pub struct DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    channels: Chans,
    ctx: Ctx,
    poll: Poll,
    tokens: Tokens
}


pub struct DispatchThread<Types, Ctx>
where
    Types: DispatchTypes<Ctx>,
{
    ctx: DispatchThreadCtx<Types::Chans, Ctx>,
    dispatched: HashMap<Token, DispatchedEntry<Types, Ctx>>,
    tokens: HashMap<Types::SessionPrin, Token>,
    dispatcher: Types::Disp,
    mode_config: Types::ModeConfig,
    shutdown: ShutdownFlag,
    notify: Arc<Waker>,
    wake_token: Token,
    nevents: usize,
}

#[derive(Debug)]
pub enum DispatchThreadCreateError<Channels> {
    Channels {
        err: Channels
    },
    IO {
        err: Error
    }
}

#[derive(Debug)]
pub enum DispatchThreadDispatchError<Mode, Dispatch> {
    Mode {
        err: Mode
    },
    Dispatch {
        err: Dispatch
    }
}

#[derive(Debug)]
pub enum DispatchThreadHandleMsgError<AuthN, Recv> {
    AuthN {
        err: AuthN
    },
    Recv {
        err: Recv
    }
}

impl<Types, OutMsg, Stream, Msgs, Recv>
    Dispatched<Types, OutMsg, Stream, Msgs, Recv>
where
    Types: DispatchInboundTypes,
    Msgs: PrivateMsgs<OutMsg>,
    Recv: AuthNMsgRecv<Types::MsgPrin, Types::InMsg, Types::AuthNMsg> {
    /// Create a new `Dispatched` from its components.
    ///
    /// # Parameters
    ///
    /// - `shutdown`: A [ShutdownFlag] used to signal any connected
    ///   thread to shut down.  The dispatch thread will set this when
    ///   it shuts down.
    ///
    /// - `stream`: The [PushStream] used to send messages.
    ///
    /// - `msgs`: The [PrivateMsgs] message outbox used to generate
    ///   messages to send.
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
            recv: recv,
        }
    }

    fn handle_msg<ID>(
        &mut self,
        id: &ID,
        session_prin: &Types::SessionPrin,
        msg: Types::Wrapper
    ) -> Result<
        (),
        DispatchThreadHandleMsgError<
            Types::MsgAuthError,
            Recv::RecvError
        >
    >
    where ID: Display {
        trace!(target: "poll-thread",
               "handling incoming message from {} ({})",
               session_prin, id);

        // ISSUE #10: future: unwrap XCIAP here and
        // report successes.

        match self.authn.msg_authn(session_prin, msg)
            .map_err(|err| DispatchThreadHandleMsgError::AuthN {
                err: err
            })? {
            AuthNResult::Accept(msg) => {
                trace!(target: "poll-thread",
                       "authenticated message from {} ({}) as {}",
                       session_prin, id, msg.prin());

                self.recv.recv_auth_msg(msg)
                    .map_err(|err| DispatchThreadHandleMsgError::Recv {
                        err: err
                    })
            },
            AuthNResult::Reject(_) => {
                warn!(target: "poll-thread",
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
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>
    {
        self.stream.complete_refresh(ctx, err)
            .unwrap_or_else(|err| match err.split() {
            (_, Some(err)) => {
                error!(target: "poll-thread",
                       "unrecoverable error refreshing stream: {}",
                       err);

                RetryResult::Success(None)
            }
            (Some(err), _) => self.complete_refresh_stream(ctx, err),
            (None, None) => {
                error!(target: "poll-thread",
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
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>
    {
        self.stream.retry_refresh(ctx, retry)
            .unwrap_or_else(|err| match err.split() {
            (_, Some(err)) => {
                error!(target: "poll-thread",
                       "unrecoverable error refreshing stream: {}",
                       err);

                RetryResult::Success(None)
            }
            (Some(err), _) => self.complete_refresh_stream(ctx, err),
            (None, None) => {
                error!(target: "poll-thread",
                       "refresh error split produced no results");

                RetryResult::Success(None)
            }
        })
    }

    fn refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>,
    ) -> RetryResult<Option<Instant>, Stream::RefreshRetry>
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>
    {
        self.stream.refresh(ctx).unwrap_or_else(|err| match err.split() {
            (_, Some(err)) => {
                error!(target: "poll-thread",
                       "unrecoverable error refreshing stream: {}",
                       err);

                RetryResult::Success(None)
            }
            (Some(err), _) => self.complete_refresh_stream(ctx, err),
            (None, None) => {
                error!(target: "poll-thread",
                       "refresh error split produced no results");

                RetryResult::Success(None)
            }
        })
    }
}

impl<Chans, Ctx> Channels<()> for DispatchThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    type ChannelID = Chans::ChannelID;
    type Param = Chans::Param;
    type ParamIter = Chans::ParamIter;
    type ParamError = Chans::ParamError;
    type OutNegoParam = Chans::OutNegoParam;
    type Addr = Chans::Addr;
    type Stream = Chans::Stream;
    type ReqStreamError = Chans::ReqStreamError;

    #[inline]
    fn req_stream(
        &mut self,
        _ctx: &mut (),
        channel: &Self::ChannelID,
        param: &Self::Param,
        endpoint: &Self::Addr,
        nego_param: &Self::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Self::Stream>,
            bool,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        self.channels.req_stream(&mut self.ctx, channel, param,
                                 endpoint, nego_param)
    }

    #[inline]
    fn params<I>(
        &mut self,
        _ctx: &mut (),
        channels: I
    ) -> Result<RetryResult<(Self::ParamIter, Option<Instant>)>,
                Self::ParamError>
    where I: Iterator<Item = Self::ChannelID> {
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
        tokens_hint: Option<usize>,
    ) -> Self {
        let tokens = match tokens_hint {
            Some(hint) => Tokens::with_capacity(hint),
            None => Tokens::new()
        };

        DispatchThreadCtx {
            channels: channels,
            ctx: ctx,
            poll: poll,
            tokens: tokens,
        }
    }
}

impl<Types, Ctx> DispatchedEntry<Types, Ctx>
where
    Types: DispatchEntryTypes<Ctx>,
{
    fn recv_stream(
        &mut self,
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) {
        debug!(target: "dispatched-entry",
               "receiving stream from {} for {}",
               id, stream.prin());

        // Report up to the stream.
        match self
            .dispatched
            .stream
            .report_stream(stream.prin(), id.clone(), stream.clone()) {
            Ok(res) => {
                let stream = match res {
                    Some(stream) => {
                        warn!(target: "dispatch-entry",
                              "stream {} with {} was already present",
                              id, stream.prin());

                        stream
                    }
                    None => stream
                };

                if self
                    .pull_streams
                    .insert(id.clone(), stream.clone())
                    .is_some() {
                    error!(target: "dispatch-entry",
                           "stream {} was already present for {}",
                           id, stream.prin());
                }
            }
            Err(err) => {
                error!(target: "dispatch-entry",
                       "error reporting stream {} with {}: {}",
                       id, stream.prin(), err);
            }
        }
    }

    #[inline]
    fn handle_msg(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        session_prin: &Types::SessionPrin,
        msg: Types::Wrapper
    ) -> Result<
        (),
        DispatchThreadHandleMsgError<
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        self.dispatched.handle_msg(id, session_prin, msg)
    }

    fn next_refresh(&self) -> Option<Instant> {
        self.retry_refresh.as_ref().map(|retry| retry.when())
            .or(self.next_refresh)
    }

    fn refresh_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        need_refresh: bool,
        now: Instant
    ) -> Option<Instant> {
        // Refresh the stream if needed.
        if let Some(retry) = self.retry_refresh.take() {
            if retry.when() < now {
                trace!(target: "dispatch-entry",
                       "retrying stream refresh");

                match self.dispatched.retry_refresh_stream(ctx, retry) {
                    RetryResult::Success(when) => {
                        self.next_refresh = when;

                        if let Err(err) = self.mode.retry_indefs(
                            ctx,
                            &mut self.dispatched.msgs,
                            &mut self.dispatched.stream,
                        ) {
                            error!(target: "dispatch-entry",
                                   "error retrying indefinite delays: {}",
                                   err)
                        }
                    }
                    RetryResult::Retry(retry) => {
                        self.retry_refresh = Some(retry)
                    }
                }
            } else {
                self.retry_refresh = Some(retry)
            }
        } else if self.next_refresh.map_or(false, |when| when <= now) ||
            need_refresh {
            trace!(target: "dispatch-entry",
                   "refreshing stream");

            match self.dispatched.refresh_stream(ctx) {
                RetryResult::Success(when) => {
                    self.next_refresh = when;

                    if let Err(err) = self.mode.retry_indefs(
                        ctx,
                        &mut self.dispatched.msgs,
                        &mut self.dispatched.stream,
                    ) {
                        error!(target: "dispatch-entry",
                               "error retrying indefinite delays: {}",
                               err)
                    }
                }
                RetryResult::Retry(retry) => {
                    self.retry_refresh = Some(retry)
                }
            }
        }

        self.next_refresh()
    }

    fn retry_pending(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) -> Option<Instant> {
        if self.next_pending.map_or(false, |when| when <= now) {
            trace!(target: "poll-thread",
                   "retrying pending messages");

            match self.mode.retry_pending(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                &live,
                now,
            ) {
                Ok(next) => {
                    self.next_pending = next;
                }
                Err(err) => {
                    error!(target: "dispatch-entry",
                           "error retrying pending messages: {}",
                           err);
                }
            }
        }

        self.next_outbound
    }

    fn push_msgs(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) -> Option<Instant> {
        if self.next_outbound.map_or(false, |when| when <= now) {
            trace!(target: "dispatch-entry",
                   "pushing messages");

            match self.mode.send_from_outbound(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                live
            ) {
                Ok(next) => self.next_outbound = next,
                Err(err) => {
                    error!(target: "dispatch-entry",
                           "error sending messages: {}",
                           err);
                }
            }
        }

        self.next_outbound
    }
}

impl<Types, Ctx> DispatchThread<Types, Ctx>
where
    Types: DispatchTypes<Ctx>,
{
    fn create(
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
        let tokens_hint = tokens_hint
            .or_else(|| match (nsessions, ndispatched) {
                (Some(nsessions), Some(ndispatched)) =>
                    Some((nsessions * ndispatched) + (2 * ndispatched) + 1),
                _ => None
            });
        let mut ctx = DispatchThreadCtx::new(ctx, poll, channels, tokens_hint);
        let (dispatched, tokens) = match ndispatched {
            Some(ndispatched) => (HashMap::with_capacity(ndispatched),
                                  HashMap::with_capacity(ndispatched)),
            None => (HashMap::new(), HashMap::new())
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
            tokens: tokens,
            ctx: ctx,
            nevents: nevents,
            notify: notify,
            wake_token: token
        })
    }

    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
    }

    fn recv_stream(
        &mut self,
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) {
        match self.tokens.entry(stream.prin().clone()) {
            Entry::Occupied(token) => match self.dispatched
                .get_mut(token.get()) {
                Some(ent) => ent.recv_stream(id, stream),
                None => {
                    error!(target: "dispatch-thread",
                           "missing dispatch entry for {} (token {})",
                           stream.prin(), token.get().0);
                }
            }
            Entry::Vacant(ent) => {
                debug!(target: "dispatch-thread",
                       "dispatching for {}",
                       stream.prin());

                let token = self.ctx.tokens.token();

                match Waker::new(self.ctx.poll.registry(), token.clone()) {
                    Ok(notify) => match self.dispatcher
                        .dispatch(&mut self.ctx, stream.prin(),
                                  Arc::new(notify)) {
                        Ok(dispatched) => match Types::Mode
                            ::create(&dispatched.stream,
                                     self.mode_config.clone()) {
                            Ok(mode) => {
                                // XXX use a size hint here.
                                let pull_streams = HashMap::new();
                                let now = Instant::now();
                                let mut dispatched = DispatchedEntry {
                                    ctx: PhantomData,
                                    dispatched: dispatched,
                                    pull_streams: pull_streams,
                                    mode: mode,
                                    next_outbound: Some(now),
                                    next_refresh: Some(now),
                                    retry_refresh: None
                                };

                                dispatched.recv_stream(id, stream);
                                ent.insert(token);

                                // XXX shut down the stream.
                                if self.dispatched
                                    .insert(token, dispatched)
                                    .is_some() {
                                    warn!(target: "dispatch-thread",
                                           "existing entry for token {}",
                                           token.0);
                                }
                            }
                            Err(err) => {
                                error!(target: "dispatch-thread",
                                       "error creating push mode for {}: {}",
                                       stream.prin(), err);
                            }
                        }
                        Err(err) => {
                            error!(target: "dispatch-thread",
                                   "error dispatching for {}: {}",
                                   stream.prin(), err);
                        }
                    },
                    Err(err) => {
                        error!(target: "dispatch-thread",
                               "error creating notifier for {}: {}",
                               stream.prin(), err);
                    }
                }
            }
        }
    }

}

/*

pub struct PullStreamsDispatchThread<
    Msg,
    AuthN,
    Dispatcher,
    Listener,
    Mode,
    Ctx
> where
    Msg: 'static + Clone + Send,
    Mode: PushMode<Dispatcher::PushStream, Dispatcher::Msgs, Ctx> + Send,
    Listener: PullStreamListener<Msg>,
    Listener::Stream: 'static + ConcurrentStream + Credentials,
    Listener::Addr: 'static + Send,
    Dispatcher: Dispatch<Msg, Listener::Addr, Listener::Stream, AuthN, Ctx>,
    Dispatcher::PushStream: PushStreamReporter,
    Dispatcher::Recv: Clone,
    AuthN: 'static
        + Clone
        + MsgAuthN<Msg, Msg, SessionPrin = Listener::Prin>
        + Send,
    AuthN::SessionPrin: Send,
    Ctx: Clone {
    msg: PhantomData<Msg>,
    mode: Mode::Config,
    dispatcher: Dispatcher,
    listener: Listener,
    shutdown: ShutdownFlag,
    recvs: Arc<
        Mutex<
            HashMap<
                Listener::Prin,
                DispatchEntry<
                    Msg,
                    Listener::Addr,
                    Listener::Stream,
                    AuthN,
                    Dispatcher::Recv,
                    <Dispatcher::PushStream as PushStreamReporter>::Reporter
                >
            >
        >
    >,
    ctx: Ctx
}

#[derive(Debug)]
pub enum DispatchHandlerError<Dispatch> {
    Dispatch { err: Dispatch },
    IO { err: Error },
    MutexPoison
}

impl<Msg, AuthN, Dispatcher, Listener, Mode, Ctx>
    PullStreamsDispatchThread<Msg, AuthN, Dispatcher, Listener, Mode, Ctx>
where
    Msg: 'static + Clone + Send,
    Mode: 'static
        + PushMode<Dispatcher::PushStream, Dispatcher::Msgs, Ctx>
        + Send,
    Mode::Config: Send,
    Listener: 'static + PullStreamListener<Msg> + Send,
    Listener::Stream: ConcurrentStream + Credentials,
    Listener::Addr: Send,
    Listener::Prin: Clone + Eq + Hash + Send,
    Dispatcher: 'static
        + Dispatch<Msg, Listener::Addr, Listener::Stream, AuthN, Ctx>
        + Send,
    Dispatcher::PushStream: PushStreamReporter,
    <Dispatcher::PushStream as PushStreamReporter>::Reporter: StreamReporter<
            Stream = ThreadedStream<Listener::Stream>,
            Prin = Listener::Prin,
            Src = Listener::Addr
        > + Send,
    Dispatcher::Recv: Clone,
    AuthN: 'static
        + Clone
        + MsgAuthN<Msg, Msg, SessionPrin = Listener::Prin>
        + Send,
    AuthN::SessionPrin: Send,
    Ctx: 'static + Clone + Send + Sync
{
    fn create(
        mode: Mode::Config,
        dispatcher: Dispatcher,
        listener: Listener,
        shutdown: ShutdownFlag,
        ctx: Ctx,
        recvs: Arc<
        Mutex<
            HashMap<
                Listener::Prin,
                DispatchEntry<
                    Msg,
                    Listener::Addr,
                    Listener::Stream,
                    AuthN,
                    Dispatcher::Recv,
                    <Dispatcher::PushStream as PushStreamReporter>::Reporter
                >
            >
        >
    >
    ) -> Self {
        PullStreamsDispatchThread {
            msg: PhantomData,
            mode: mode,
            dispatcher: dispatcher,
            listener: listener,
            shutdown: shutdown,
            recvs: recvs,
            ctx: ctx
        }
    }

    pub fn new(
        mode: Mode::Config,
        dispatcher: Dispatcher,
        listener: Listener,
        shutdown: ShutdownFlag,
        ctx: Ctx
    ) -> Self {
        let recvs = Arc::new(Mutex::new(HashMap::new()));

        Self::create(mode, dispatcher, listener, shutdown, ctx, recvs)
    }

    pub fn with_capacity(
        mode: Mode::Config,
        dispatcher: Dispatcher,
        listener: Listener,
        shutdown: ShutdownFlag,
        ctx: Ctx,
        size: usize
    ) -> Self {
        let recvs = Arc::new(Mutex::new(HashMap::with_capacity(size)));

        Self::create(mode, dispatcher, listener, shutdown, ctx, recvs)
    }

    fn report(
        ent: &mut DispatchEntry<
            Msg,
            Listener::Addr,
            Listener::Stream,
            AuthN,
            Dispatcher::Recv,
            <Dispatcher::PushStream as PushStreamReporter>::Reporter
        >,
        stream: Listener::Stream,
        addr: Listener::Addr,
        prin: Listener::Prin
    ) {
        let stream = ThreadedStream::new(ent.inner.shutdown.clone(), stream);

        match ent.reporter.report(addr.clone(), prin, stream) {
            Ok(None) => {
                debug!(target: "pull-streams-dispatch-thread",
                       "incoming stream registered for {}",
                       addr);
            }
            Ok(Some(_)) => {
                debug!(target: "pull-streams-dispatch-thread",
                       "stream already exists for {}, aborting",
                       addr);
            }
            Err(err) => {
                error!(target: "pull-streams-dispatch-thread",
                       "error reporting new stream: {}",
                       err)
            }
        }
    }

    fn handle(
        &mut self,
        stream: Listener::Stream,
        addr: Listener::Addr,
        prin: Listener::Prin
    ) -> Result<(), DispatchHandlerError<Dispatcher::DispatchError>> {
        match self
            .recvs
            .lock()
            .map_err(|_| DispatchHandlerError::MutexPoison)?
            .entry(prin.clone())
        {
            Entry::Occupied(mut ent) => {
                let ent = ent.get_mut();

                Self::report(ent, stream, addr, prin);

                Ok(())
            }
            Entry::Vacant(ent) => {
                debug!(target: "pull-streams-dispatch-thread",
                       "no dispatcher entry for {}",
                       prin);

                let (push_stream, msgs, notify, dispatched) = self
                    .dispatcher
                    .dispatch(&mut self.ctx, prin.clone())
                    .map_err(|err| DispatchHandlerError::Dispatch {
                        err: err
                    })?;
                let reporter = push_stream.reporter();
                let push_thread: PushStreamThread<_, _, Mode, _> =
                    PushStreamThread::create(
                        self.mode.clone(),
                        self.ctx.clone(),
                        msgs,
                        notify,
                        push_stream,
                        dispatched.shutdown.clone()
                    );
                let join = push_thread
                    .start()
                    .map_err(|err| DispatchHandlerError::IO { err: err })?;
                let ent = ent.insert(DispatchEntry {
                    inner: dispatched,
                    reporter: reporter,
                    push_thread: join
                });

                Self::report(ent, stream, addr, prin);

                Ok(())
            }
        }
    }

    fn run(&mut self) {
        let mut valid = true;

        debug!(target: "pull-streams-dispatch-thread",
               "listen thread starting");

        while self.shutdown.is_live() && valid {
            trace!(target: "pull-streams-dispatch-thread",
                   "listening for connection");

            match self.listener.listen() {
                Ok(RetryResult::Success((stream, addr, prin))) => {
                    info!(target: "pull-streams-dispatch-thread",
                          "received new incoming stream from {}",
                          addr);

                    if let Err(err) = self.handle(stream, addr, prin) {
                        error!(target: "pull-streams-dispatch-thread",
                               "error handling new stream: {}",
                               err);

                        valid = false
                    }
                }
                Ok(RetryResult::Retry(until)) => {
                    let now = Instant::now();

                    if now < until {
                        let delay = until - now;

                        debug!(
                            "retrying listen in {}.{:03}s",
                            delay.as_secs(),
                            delay.subsec_millis()
                        );

                        sleep(delay)
                    }
                }
                Err(err) => {
                    error!(target: "pull-streams-dispatch-thread",
                           "error listening for new sessions: {}",
                           err);

                    valid = false;
                }
            }
        }

        info!(target: "pull-streams-dispatch-thread",
              "listener thread exiting");
    }

    #[inline]
    pub fn start(mut self) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("pull-streams-dispatch-thread"))
            .spawn(move || self.run())
    }
}

impl<Dispatch> Display for DispatchHandlerError<Dispatch>
where
    Dispatch: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchHandlerError::Dispatch { err } => err.fmt(f),
            DispatchHandlerError::IO { err } => write!(f, "{}", err),
            DispatchHandlerError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}
*/

impl<Channels> Display for DispatchThreadCreateError<Channels>
where
    Channels: Display {
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
    Mode: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadDispatchError::Dispatch { err } => err.fmt(f),
            DispatchThreadDispatchError::Mode { err } => err.fmt(f),
        }
    }
}

impl<AuthN, Recv> Display for DispatchThreadHandleMsgError<AuthN, Recv>
where
    AuthN: Display,
    Recv: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DispatchThreadHandleMsgError::AuthN { err } => err.fmt(f),
            DispatchThreadHandleMsgError::Recv { err } => err.fmt(f),
        }
    }
}
