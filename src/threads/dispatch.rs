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

use std::collections::BinaryHeap;
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
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
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

use crate::channels::Channels;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::config::DispatchThreadConfig;
use crate::stream::PullStream;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::PushModeResult;
use crate::threads::RegistryCtx;
use crate::threads::RetryHeapEntry;
use crate::threads::Tokens;
use crate::threads::TokensCtx;
use crate::threads::types::DispatchEntryTypes;
use crate::threads::types::DispatchInboundTypes;
use crate::threads::types::DispatchTypes;

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
    type Msgs;
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
    /// - `shutdown`: A [ShutdownFlag] that will be used to signal a shutdown.
    ///
    /// - `notify`: Notifier used to alert the dispatch thread to changes in
    ///   outbound messages.
    fn dispatch(
        &mut self,
        ctx: &mut Ctx,
        prin: &Types::SessionPrin,
        shutdown: ShutdownFlag,
        notify: Notify
    ) -> Result<
        Dispatched<Types, Self::PushStream, Self::Msgs, Self::Recv>,
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
/// - `Msgs`: Type of outbound message box used to generate outbound messages.
///
/// - `Recv`: Type of [AuthNMsgRecv] used to send messages.
pub struct Dispatched<Types, Stream, Msgs, Recv>
where
    Types: DispatchInboundTypes,
    Recv: AuthNMsgRecv<Types::MsgPrin, Types::InMsg, Types::AuthNMsg> {
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
    dispatched: Dispatched<Types, Types::Stream, Types::Msgs, Types::Recv>,
    notify: Notify,
    mode: Types::Mode,
    shutdown_retries: Option<
        BinaryHeap<
            RetryHeapEntry<
                StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
                Types::ChanShutdownRetry
            >
        >
    >,
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
    // XXX Replace this hash table + counter with a better dense map
    // data structure.
    /// Current ID for creating DispatchedIDs
    curr_id: usize,
    /// [DispatchedEntry]s, indexed by [DispatchID]s.
    dispatched: HashMap<DispatchedID, DispatchedEntry<Types, Ctx>>,
    /// Map from principals to the [DispatchedID]s that index
    /// [DispatchedEntry]s.
    parties: HashMap<Types::SessionPrin, DispatchedID>,
    stream_ids: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        DispatchedID
    >,
    orphan_shutdown_retries: Option<
        BinaryHeap<
            RetryHeapEntry<
                StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
                Types::ChanShutdownRetry
            >
        >
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
pub struct DispatchedID(usize);

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
enum RecvStreamError<Report, Shutdown> {
    Report { err: Report },
    Shutdown { err: Shutdown }
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

impl<Types, Stream, Msgs, Recv> Dispatched<Types, Stream, Msgs, Recv>
where
    Types: DispatchInboundTypes,
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
    /// - `msgs`: The message outbox used to generate messages to send.
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
    ) -> Result<
        RetryResult<Option<Instant>, Stream::RefreshRetry>,
        Stream::RefreshError
    >
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>,
        <Stream::RefreshError as RecoverableError>::Completable: ScopedError
    {
        self.stream.complete_refresh(ctx, err)
    }

    fn retry_refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>,
        retry: Stream::RefreshRetry
    ) -> Result<
        RetryResult<Option<Instant>, Stream::RefreshRetry>,
        Stream::RefreshError
    >
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>,
        <Stream::RefreshError as RecoverableError>::Completable: ScopedError
    {
        self.stream.retry_refresh(ctx, retry)
    }

    fn refresh_stream<Ctx, Chans>(
        &mut self,
        ctx: &mut DispatchThreadCtx<Chans, Ctx>
    ) -> Result<
        RetryResult<Option<Instant>, Stream::RefreshRetry>,
        Stream::RefreshError
    >
    where
        Chans: Channels<Ctx>,
        Stream: StreamRefresh<DispatchThreadCtx<Chans, Ctx>>,
        <Stream::RefreshError as RecoverableError>::Completable: ScopedError
    {
        self.stream.refresh(ctx)
    }

    /// Shut down this `Dispatched`.
    ///
    /// This will trigger the [ShutdownFlag] associated with this
    /// `Dispatched`.
    #[inline]
    fn shutdown(&mut self) -> Result<(), Error> {
        self.shutdown.set()
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
    type ParamsError = Chans::ParamsError;
    type ParamsIter<I>
        = Chans::ParamsIter<I>
    where
        I: Iterator<Item = Self::ChannelID>;
    type ReqStreamError = Chans::ReqStreamError;
    type Stream = Chans::Stream;

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
    fn params<I>(
        &mut self,
        _ctx: &mut (),
        channels: I
    ) -> Result<Self::ParamsIter<I>, Self::ParamsError>
    where
        I: Iterator<Item = Self::ChannelID> {
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
    ) -> Result<
        (Option<Vec<Types::ChannelParam>>, Option<Instant>),
        RecvStreamError<Types::ReportStreamError, Types::ChanShutdownError>
    > {
        debug!(target: "dispatch-entry",
               "receiving stream from {} for {}",
               id, stream.prin());

        // Report up to the stream.
        match self
            .dispatched
            .stream
            .report_stream(stream.prin(), id.clone(), stream.clone())
            .map_err(|err| RecvStreamError::Report { err: err })?
        {
            Some(stream) => {
                warn!(target: "dispatch-entry",
                      "stream {} with {} was already present",
                      id, stream.prin());

                match ctx
                    .channels
                    .shutdown_stream(
                        &mut ctx.ctx,
                        id.channel(),
                        id.param(),
                        stream
                    )
                    .map_err(|err| RecvStreamError::Shutdown { err: err })?
                {
                    RetryResult::Success(out) => Ok(out),
                    RetryResult::Retry(retry) => {
                        trace!(target: "dispatch-entry",
                               "retrying shutdown of stream {} later",
                               id);

                        let id = id.clone();
                        let ent = RetryHeapEntry::new(id, retry);

                        match &mut self.shutdown_retries {
                            Some(shutdown_retries) => {
                                shutdown_retries.push(ent);
                            }
                            None => {
                                let mut heap = BinaryHeap::with_capacity(
                                    self.pull_streams.len()
                                );

                                heap.push(ent);
                                self.shutdown_retries = Some(heap);
                            }
                        }

                        Ok((None, None))
                    }
                }
            }
            // Insert into the pull streams.
            None => {
                if self
                    .pull_streams
                    .insert(id.clone(), stream.clone())
                    .is_some()
                {
                    error!(target: "dispatch-entry",
                           "stream {} was already present for {}",
                           id, stream.prin());
                }

                Ok((None, None))
            }
        }
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

        if let Some(stream) = self.pull_streams.get_mut(id) {
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
                            });
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

    fn complete_refresh_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        err: Types::RefreshCompletableError
    ) -> Result<
        RetryResult<Option<Instant>, Types::RefreshRetry>,
        Types::RefreshError
    > {
        self.dispatched.complete_refresh_stream(ctx, err)
    }

    fn update_next_refresh(
        &mut self,
        when: &Option<Instant>
    ) {
        self.next_refresh = next_retry(&self.next_refresh, when)
    }

    /// Do necessary state updates for a refresh result.
    fn handle_refresh_result(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        res: Result<
            RetryResult<Option<Instant>, Types::RefreshRetry>,
            Types::RefreshError
        >
    ) -> bool {
        match res {
            Ok(RetryResult::Success(when)) => {
                self.next_refresh = when;

                trace!(target: "dispatched-entry",
                       "refresh succeeded, retrying indefinites");

                // We succeeded; retry indefinites.
                match self.mode.retry_indefs(
                    ctx,
                    &mut self.dispatched.msgs,
                    &mut self.dispatched.stream
                ) {
                    Ok(res) => {
                        self.pending.merge(&res);

                        true
                    }
                    Err(err) => match err.scope() {
                        ErrorScope::Unrecoverable | ErrorScope::System => {
                            error!(target: "dispatched-entry",
                                   "fatal error completing refresh: {}",
                                   err);

                            false
                        }
                        ErrorScope::Shutdown => false,
                        ErrorScope::WouldBlock => {
                            error!(target: "dispatched-entry",
                                   "error of scope WouldBlock \
                                    shouldn't be seen here");

                            true
                        }
                        _ => {
                            error!(target: "dispatched-entry",
                                   "error completing refresh: {}",
                                   err);

                            true
                        }
                    }
                }
            }
            Ok(RetryResult::Retry(retry)) => {
                trace!(target: "dispatched-entry",
                       "retrying refresh stream later");

                self.refresh_retry = Some(retry);

                true
            }
            Err(err) => match err.split() {
                (_, Some(err)) => {
                    error!(target: "dispatched",
                           "unrecoverable error refreshing stream: {}",
                           err);

                    false
                }
                (Some(err), _) => match err.scope() {
                    ErrorScope::WouldBlock => {
                        trace!(target: "dispatched-entry",
                               "deferring refreshing stream");

                        self.refresh_complete = Some(err);

                        true
                    }
                    _ => {
                        trace!(target: "dispatch-entry",
                               "retrying after recoverable refresh error");

                        let res =
                            self.dispatched.complete_refresh_stream(ctx, err);

                        self.handle_refresh_result(ctx, res)
                    }
                },
                (None, None) => {
                    error!(target: "dispatched",
                           "refresh error split produced no results");

                    false
                }
            }
        }
    }

    fn next_shutdown_retry(&self) -> Option<Instant> {
        self.shutdown_retries
            .as_ref()
            .and_then(|heap| heap.peek().map(|ent| ent.when()))
    }

    fn needs_refresh(
        &self,
        now: Instant
    ) -> bool {
        self.refresh_complete.is_some() ||
            self.next_refresh.is_some_and(|when| when <= now) ||
            self.refresh_retry
                .as_ref()
                .is_some_and(|retry| retry.when() <= now)
    }

    fn refresh_stream(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        need_refresh: bool,
        now: Instant
    ) -> bool {
        // Check if there's a pending completion.
        if let Some(refresh_complete) = self.refresh_complete.take() {
            trace!(target: "dispatch-entry",
                   "completing stream refresh");

            // There's a pending completion; run it.
            let res = self.complete_refresh_stream(ctx, refresh_complete);

            self.handle_refresh_result(ctx, res)
        // Check if there's a pending retry.
        } else if let Some(retry) = self.refresh_retry.take() {
            // There's a pending retry; see if it's time yet.
            if retry.when() <= now {
                trace!(target: "dispatch-entry",
                       "retrying stream refresh");

                let res = self.dispatched.retry_refresh_stream(ctx, retry);

                self.handle_refresh_result(ctx, res)
            } else {
                self.refresh_retry = Some(retry);

                true
            }
        // Check if we need to start a new retry.
        } else if self.next_refresh.is_some_and(|when| when <= now) ||
            need_refresh
        {
            trace!(target: "dispatch-entry",
                   "refreshing stream");

            self.next_refresh = None;

            let res = self.dispatched.refresh_stream(ctx);

            self.handle_refresh_result(ctx, res)
        } else {
            true
        }
    }

    /// Complete pending push operations if necessary.
    fn complete_pending(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>
    ) -> bool {
        let mut valid = true;

        if self.pending.take_has_completes() {
            match self.mode.complete_pending(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                live
            ) {
                Ok(res) => self.pending.merge(&res),
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "dispatched-entry",
                               "fatal error completing stalled sends: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "dispatched-entry",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "dispatched-entry",
                               "error completing stalled sends: {}",
                               err);
                    }
                }
            }
        }

        valid
    }

    /// Retry pending shutdown operations if necessary.
    fn retry_shutdown(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        now: Instant
    ) {
        if let Some(mut retries) = self.shutdown_retries.take() {
            let mut newents: Option<Vec<_>> = None;

            while retries.peek().is_some_and(|ent| ent.when() <= now) {
                if let Some(ent) = retries.pop() {
                    let (id, retry) = ent.take();

                    match ctx.channels.retry_shutdown_stream(
                        &mut ctx.ctx,
                        id.channel(),
                        id.param(),
                        retry
                    ) {
                        Ok(res) => {
                            if let RetryResult::Retry(retry) = res {
                                let ent = RetryHeapEntry::new(id, retry);

                                match &mut newents {
                                    Some(newents) => {
                                        newents.push(ent);
                                    }
                                    None => {
                                        let mut heap = Vec::with_capacity(
                                            self.pull_streams.len()
                                        );

                                        heap.push(ent);
                                        newents = Some(heap);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!(target: "poll-thread",
                                   "error shutting down stream {}: {}",
                                   id, err);
                        }
                    }
                } else {
                    error!(target: "poll-thread",
                           "shutdown_retries.pop() should not be None")
                }
            }

            if let Some(newents) = newents {
                newents.into_iter().for_each(|ent| retries.push(ent));
            }

            if !retries.is_empty() {
                self.shutdown_retries = Some(retries)
            }
        }
    }

    /// Retry pending push operations if necessary.
    fn retry_pending(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) -> bool {
        let mut valid = true;

        if self.pending.retry_pending().is_some_and(|when| when <= now) {
            let _ = self.pending.take_retry_pending();

            trace!(target: "dispatch-entry",
                   "retrying pending messages");

            match self.mode.retry_pending(
                ctx,
                &mut self.dispatched.msgs,
                &mut self.dispatched.stream,
                live,
                now
            ) {
                Ok(res) => {
                    self.pending.merge(&res);
                }
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "dispatch-entry",
                               "fatal error sending messages: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "dispatch-entry",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "dispatch-entry",
                               "error sending messages: {}",
                               err);
                    }
                }
            }
        }

        valid
    }

    /// Push messages if it's time to do so.
    fn push_outbound_msgs(
        &mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        live: &HashSet<Token>,
        now: Instant
    ) -> bool {
        let mut valid = true;

        if self.pending.next_outbound().is_some_and(|when| when <= now) {
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
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "dispatch-entry",
                               "fatal error sending messages: {}",
                               err);

                        valid = false
                    }
                    ErrorScope::Shutdown => valid = false,
                    ErrorScope::WouldBlock => {
                        error!(target: "dispatch-entry",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "dispatch-entry",
                               "error sending messages: {}",
                               err);
                    }
                }
            }
        }

        valid
    }

    fn shutdown(
        mut self,
        ctx: &mut DispatchThreadCtx<Types::Chans, Ctx>,
        shutdown_retries: &mut Option<
            BinaryHeap<
                RetryHeapEntry<
                    StreamID<
                        Types::Addr,
                        Types::ChannelID,
                        Types::ChannelParam
                    >,
                    Types::ChanShutdownRetry
                >
            >
        >
    ) -> Result<(), Error> {
        let nsessions = self.pull_streams.len();

        // Shut down all streams.
        for (id, stream) in self.pull_streams.into_iter() {
            debug!(target: "poll-thread",
                   "shutting down stream {} with {}",
                   id, stream.prin());

            match ctx.channels.shutdown_stream(
                &mut ctx.ctx,
                id.channel(),
                id.param(),
                stream
            ) {
                Ok(res) => {
                    if let RetryResult::Retry(retry) = res {
                        let id = id.clone();
                        let ent = RetryHeapEntry::new(id, retry);

                        match &mut self.shutdown_retries {
                            Some(shutdown_retries) => {
                                shutdown_retries.push(ent);
                            }
                            None => {
                                let mut heap =
                                    BinaryHeap::with_capacity(nsessions);

                                heap.push(ent);
                                self.shutdown_retries = Some(heap);
                            }
                        }
                    }
                }
                Err(err) => {
                    error!(target: "poll-thread",
                           "error shutting down stream {}: {}",
                           id, err);
                }
            }
        }

        if let Some(shutdown_retries) = shutdown_retries {
            if let Some(new_retries) = &mut self.shutdown_retries {
                shutdown_retries.append(new_retries);
            }
        } else {
            *shutdown_retries = self.shutdown_retries
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
        config: DispatchThreadConfig<Types::ChansConfig, Types::ModeConfig>,
        dispatcher: Types::Disp,
        mut ctx: Ctx
    ) -> Result<Self, DispatchThreadCreateError<Types::ChansCreateError>> {
        let (
            chans_config,
            mode_config,
            nevents,
            nsessions,
            ndispatched,
            tokens_hint
        ) = config.take();
        let channels = Types::Chans::create(chans_config, &mut ctx)
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
        let notify = Waker::new(ctx.registry(), token)
            .map_err(|err| DispatchThreadCreateError::IO { err: err })?;
        let notify = Arc::new(notify);
        let shutdown = ShutdownFlag::new(notify.clone());

        Ok(DispatchThread {
            mode_config: mode_config,
            curr_id: 0,
            dispatcher: dispatcher,
            shutdown: shutdown,
            dispatched: dispatched,
            orphan_shutdown_retries: None,
            stream_ids: stream_ids,
            parties: parties,
            ctx: ctx,
            nevents: nevents,
            notify: notify
        })
    }

    #[inline]
    pub fn shutdown_flag(&self) -> ShutdownFlag {
        self.shutdown.clone()
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
        dispatched: Dispatched<Types, Types::Stream, Types::Msgs, Types::Recv>,
        mode: Types::Mode,
        stream: Types::AuthNChan,
        notify: Notify,
        token: DispatchedID,
        now: Instant
    ) -> Result<
        (Option<Vec<Types::ChannelParam>>, Option<Instant>),
        RecvStreamError<Types::ReportStreamError, Types::ChanShutdownError>
    > {
        // XXX use a size hint here.
        let pull_streams = HashMap::new();
        let pending = PushModeResult::new(Some(now), None, false);
        let mut dispatched = DispatchedEntry {
            ctx: PhantomData,
            shutdown_retries: None,
            dispatched: dispatched,
            pull_streams: pull_streams,
            notify: notify,
            mode: mode,
            pending: pending,
            next_refresh: Some(now),
            refresh_retry: None,
            refresh_complete: None
        };

        let out = dispatched.recv_stream(ctx, id.clone(), stream)?;

        // Receive went through; add the stream ID entry.
        if let Some(token) = stream_ids.insert(id.clone(), token.clone()) {
            error!(target: "dispatch-thread",
                   "existing stream ID entry for {}: {}",
                   id, token);
        }

        if ents.insert(token.clone(), dispatched).is_some() {
            error!(target: "dispatch-thread",
                   "existing entry for token {}",
                   token);
        }

        Ok(out)
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
        session: Types::AuthNChan,
        now: Instant
    ) -> (
        Option<DispatchedID>,
        Option<Vec<Types::ChannelParam>>,
        Option<Instant>
    ) {
        match self.parties.entry(session.prin().clone()) {
            Entry::Occupied(token) => {
                match self.dispatched.get_mut(token.get()) {
                    Some(ent) => match ent.recv_stream(
                        &mut self.ctx,
                        id.clone(),
                        session
                    ) {
                        Ok((refreshes, next_refresh)) => {
                            if let Some(token) = self
                                .stream_ids
                                .insert(id.clone(), token.get().clone())
                            {
                                error!(target: "dispatch-thread",
                                       "existing stream ID entry for {}: {}",
                                       id, token);
                            }

                            (Some(token.get().clone()), refreshes, next_refresh)
                        }
                        Err(err) => {
                            error!(target: "dispatch-thread",
                                   "error receiving session: {}",
                                   err);

                            (None, None, None)
                        }
                    },
                    None => {
                        error!(target: "dispatch-thread",
                           "missing dispatch entry for {} ({})",
                           session.prin(), token.get());

                        match self.ctx.channels.shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            session
                        ) {
                            Ok(RetryResult::Success((
                                refreshes,
                                next_refresh
                            ))) => (
                                Some(token.get().clone()),
                                refreshes,
                                next_refresh
                            ),
                            Ok(RetryResult::Retry(retry)) => {
                                trace!(target: "dispatch-entry",
                                       "retrying shutdown of stream {} later",
                                       id);

                                let id = id.clone();
                                let ent = RetryHeapEntry::new(id, retry);

                                match &mut self.orphan_shutdown_retries {
                                    Some(shutdown_retries) => {
                                        shutdown_retries.push(ent);
                                    }
                                    None => {
                                        let mut heap = BinaryHeap::new();

                                        heap.push(ent);
                                        self.orphan_shutdown_retries =
                                            Some(heap);
                                    }
                                }

                                (None, None, None)
                            }
                            Err(err) => {
                                error!(target: "dispatch-thread",
                                           "error receiving session: {}",
                                       err);

                                (None, None, None)
                            }
                        }
                    }
                }
            }
            Entry::Vacant(ent) => {
                debug!(target: "dispatch-thread",
                       "dispatching for {}",
                       session.prin());

                let notify = Notify::new(self.notify.clone());

                match self.dispatcher.dispatch(
                    &mut self.ctx,
                    session.prin(),
                    self.shutdown.clone(),
                    notify.clone()
                ) {
                    Ok(dispatched) => match Types::Mode::create(
                        self.mode_config.clone(),
                        &dispatched.stream
                    ) {
                        Ok(mode) => {
                            let idx = DispatchedID(self.curr_id);

                            self.curr_id += 1;
                            ent.insert(idx.clone());

                            match Self::setup_dispatched(
                                &mut self.ctx,
                                &mut self.dispatched,
                                &mut self.stream_ids,
                                id,
                                dispatched,
                                mode,
                                session,
                                notify,
                                idx.clone(),
                                now
                            ) {
                                Ok((refreshes, next_refresh)) => {
                                    (Some(idx), refreshes, next_refresh)
                                }
                                Err(err) => {
                                    error!(target: "dispatch-thread",
                                           "error receiving session: {}",
                                           err);

                                    (None, None, None)
                                }
                            }
                        }
                        Err(err) => {
                            error!(target: "dispatch-thread",
                                   "error creating push mode for {}: {}",
                                   session.prin(), err);

                            (None, None, None)
                        }
                    },
                    Err(err) => {
                        error!(target: "dispatch-thread",
                               "error dispatching for {}: {}",
                               session.prin(), err);

                        match self.ctx.channels.shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            session
                        ) {
                            Ok(RetryResult::Success((
                                refreshes,
                                next_refresh
                            ))) => (None, refreshes, next_refresh),
                            Ok(RetryResult::Retry(retry)) => {
                                trace!(target: "dispatch-entry",
                                       "retrying shutdown of stream {} later",
                                       id);

                                let id = id.clone();
                                let ent = RetryHeapEntry::new(id, retry);

                                match &mut self.orphan_shutdown_retries {
                                    Some(shutdown_retries) => {
                                        shutdown_retries.push(ent);
                                    }
                                    None => {
                                        let mut heap = BinaryHeap::new();

                                        heap.push(ent);
                                        self.orphan_shutdown_retries =
                                            Some(heap);
                                    }
                                }

                                (None, None, None)
                            }
                            Err(err) => {
                                error!(target: "dispatch-thread",
                                       "error receiving session: {}",
                                       err);

                                (None, None, None)
                            }
                        }
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
        let idx = self
            .stream_ids
            .get(id)
            .ok_or(DispatchThreadRecvError::NoToken { id: id.clone() })?;
        let ent = self.dispatched.get_mut(idx).ok_or(
            DispatchThreadRecvError::NoEnt {
                id: id.clone(),
                token: idx.clone()
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
        mut outbounds: Option<Vec<DispatchedID>>,
        retries: Option<Vec<DispatchedID>>,
        shutdown_retries: Option<Vec<DispatchedID>>,
        completes: Option<Vec<DispatchedID>>,
        now: Instant
    ) -> bool {
        let mut valid = true;

        // If we have pending completes, run them.
        if let Some(completes) = completes {
            for token in completes.into_iter() {
                if let Some(ent) = self.dispatched.get_mut(&token) {
                    valid &= ent.complete_pending(&mut self.ctx, &live)
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
                    valid &= ent.retry_pending(&mut self.ctx, &live, now)
                } else {
                    // This shouldn't happen.
                    error!(target: "dispatch-thread",
                           "entry for retry for {} not found",
                           token);
                }
            }
        }

        // If we have pending shutdown retries, run them.
        if let Some(shutdown_retries) = shutdown_retries {
            for token in shutdown_retries.into_iter() {
                // Look up the dispatched entry.
                if let Some(ent) = self.dispatched.get_mut(&token) {
                    ent.retry_shutdown(&mut self.ctx, now)
                } else {
                    // This shouldn't happen.
                    error!(target: "dispatch-thread",
                           "entry for shutdown retry for {} not found",
                           token);
                }
            }

            if let Some(mut retries) = self.orphan_shutdown_retries.take() {
                trace!(target: "poll-thread",
                       "retrying shutdowns");

                let nents = retries.len();
                let mut newents: Option<Vec<_>> = None;

                while retries.peek().is_some_and(|ent| ent.when() <= now) {
                    if let Some(ent) = retries.pop() {
                        let (id, retry) = ent.take();

                        trace!(target: "poll-thread",
                               "retrying shutdown of {}",
                               id);

                        match self.ctx.channels.retry_shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            retry
                        ) {
                            Ok(res) => {
                                if let RetryResult::Retry(retry) = res {
                                    let ent = RetryHeapEntry::new(id, retry);

                                    match &mut newents {
                                        Some(newents) => {
                                            newents.push(ent);
                                        }
                                        None => {
                                            let mut heap =
                                                Vec::with_capacity(nents);

                                            heap.push(ent);
                                            newents = Some(heap);
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                error!(target: "dispatch-thread",
                                       "error shutting down stream {}: {}",
                                       id, err);
                            }
                        }
                    } else {
                        error!(target: "dispatch-thread",
                               "shutdown_retries.pop() should not be None")
                    }
                }

                if let Some(newents) = newents {
                    newents.into_iter().for_each(|ent| retries.push(ent));
                }

                if !retries.is_empty() {
                    self.orphan_shutdown_retries = Some(retries)
                }
            }
        }

        // Do pulls before pushing new messages.
        let need_refreshes = if next_listen.is_some_and(|when| when <= now) {
            let mut need_refreshes =
                HashSet::with_capacity(self.dispatched.len());

            let _ = next_listen.take();

            trace!(target: "dispatch-thread",
                   "listening");

            match self.ctx.channels.listen(&mut self.ctx.ctx, &live) {
                Ok(RetryResult::Success((
                    streams,
                    endpoints,
                    refreshed,
                    when
                ))) => {
                    *next_listen = when;

                    trace!(target: "dispatch-thread",
                           "recording necessary refreshes");

                    // Pull in any streams for channels that were refreshed.
                    if let Some(refreshed) = refreshed {
                        let refreshed: HashSet<Types::ChannelID> =
                            refreshed.into_iter().map(|(id, _)| id).collect();

                        for (stream_id, disp) in self.stream_ids.iter() {
                            if refreshed.contains(stream_id.channel()) {
                                trace!(target: "dispatch-thread",
                                       "adding {} because {} was refreshed",
                                       disp, stream_id);

                                need_refreshes.insert(disp.clone());
                            }
                        }
                    }

                    trace!(target: "dispatch-thread",
                           "reporting new streams");

                    // Report new streams.
                    for (addr, channel_id, param, stream) in streams {
                        let id = StreamID::new(addr, channel_id.clone(), param);
                        let (disp, refreshes, next_refresh) =
                            self.recv_session(id.clone(), stream, now);

                        if refreshes.is_some() {
                            for (stream_id, disp) in self.stream_ids.iter() {
                                if stream_id.channel() == &channel_id {
                                    trace!(target: "dispatch-thread",
                                           "adding {} because {} was refreshed",
                                           disp, stream_id);

                                    need_refreshes.insert(disp.clone());

                                    if let Some(ent) =
                                        self.dispatched.get_mut(disp)
                                    {
                                        ent.update_next_refresh(&next_refresh)
                                    } else {
                                        error!(target: "dispatch-thread",
                                               "missing dispatch entry {} ({})",
                                               disp, stream_id);
                                    }
                                }
                            }
                        }

                        if let Some(disp) = disp {
                            trace!(target: "dispatch-thread",
                                   "adding new {}",
                                   disp);

                            need_refreshes.insert(disp.clone());

                            match &mut outbounds {
                                Some(outbounds) => {
                                    outbounds.push(disp.clone());
                                }
                                None => {
                                    // XXX use size hint here
                                    let vec = vec![disp.clone()];

                                    outbounds = Some(vec);
                                }
                            }
                        }

                        trace!(target: "dispatch-thread",
                               "reporting messages for {}",
                               id);

                        if let Err(err) = self.pull_msgs(&id) {
                            error!(target: "dispatch-thread",
                                   "error receiving messages from {}: {}",
                                   id, err);

                            valid = false;
                        }
                    }

                    trace!(target: "dispatch-thread",
                           "reporting messages");

                    // Pull in messages from all active streams.
                    for (addr, channel_id, param) in endpoints {
                        let id = StreamID::new(addr, channel_id, param);

                        trace!(target: "dispatch-thread",
                               "reporting messages for {}",
                               id);

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
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "dispatch-thread",
                               "fatal error listening: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "dispatch-thread",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "dispatch-thread",
                               "error listening: {}",
                               err);
                    }
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
            trace!(target: "dispatch-thread",
                   "refreshing");

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

        for disp in refreshes.into_iter() {
            if let Some(ent) = self.dispatched.get_mut(&disp) {
                trace!(target: "dispatch-thread",
                       "refreshing {}",
                       disp);

                // Complete the pending operation; record a new
                // pending operation if it returns a time.
                let need_refresh =
                    need_refreshes.as_ref().is_some_and(|need_refreshes| {
                        need_refreshes.contains(&disp)
                    });

                valid &= ent.refresh_stream(&mut self.ctx, need_refresh, now)
            } else {
                // This shouldn't happen.
                error!(target: "dispatch-thread",
                       "entry for pending for {} not found",
                       disp);
            }
        }

        // Finally, if there are outbound messages pending, send them.
        let outbounds: Vec<DispatchedID> = if let Some(outbounds) = outbounds {
            outbounds
                .into_iter()
                .chain(self.dispatched.iter().flat_map(|(id, ent)| {
                    if ent.notify.collect() {
                        Some(id.clone())
                    } else {
                        None
                    }
                }))
                .collect()
        } else {
            self.dispatched
                .iter()
                .flat_map(|(id, ent)| {
                    if ent.notify.collect() {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        for token in outbounds.into_iter() {
            if let Some(ent) = self.dispatched.get_mut(&token) {
                valid &= ent.push_outbound_msgs(&mut self.ctx, &live, now)
            } else {
                // This shouldn't happen.
                error!(target: "dispatch-thread",
                       "entry for send for {} not found",
                       token);
            }
        }

        valid
    }

    fn collect_actions(
        &mut self,
        next: &mut Option<Instant>,
        outbounds: &mut Option<Vec<DispatchedID>>,
        shutdown_retries: &mut Option<Vec<DispatchedID>>,
        retries: &mut Option<Vec<DispatchedID>>,
        completes: &mut Option<Vec<DispatchedID>>,
        refreshes: &mut Option<Vec<DispatchedID>>,
        now: Instant
    ) {
        let nents = self.dispatched.len();

        trace!(target: "dispatch-thread",
               "collecting pending events");

        for (id, ent) in self.dispatched.iter() {
            if ent.needs_refresh(now) {
                trace!(target: "dispatch-thread",
                       "{} needs refresh",
                       id);

                match refreshes {
                    Some(refreshes) => refreshes.push(id.clone()),
                    None => {
                        let mut vec = Vec::with_capacity(nents);

                        vec.push(id.clone());
                        *refreshes = Some(vec);
                    }
                }
            }

            if ent.pending.has_completes() {
                trace!(target: "dispatch-thread",
                       "{} has completes",
                       id);

                match completes {
                    Some(completes) => completes.push(id.clone()),
                    None => {
                        let mut vec = Vec::with_capacity(nents);

                        vec.push(id.clone());
                        *completes = Some(vec);
                    }
                }
            }

            if let Some(when) = ent.pending.next_outbound() &&
                when <= now
            {
                trace!(target: "dispatch-thread",
                       "{} needs to check outbounds",
                       id);

                match outbounds {
                    Some(outbounds) => {
                        *next = Some(next_retry_definite(next, &when));
                        outbounds.push(id.clone())
                    }
                    None => {
                        let mut vec = Vec::with_capacity(nents);

                        *next = Some(next_retry_definite(next, &when));
                        vec.push(id.clone());
                        *outbounds = Some(vec);
                    }
                }
            }

            if let Some(when) = ent.next_shutdown_retry() &&
                when <= now
            {
                trace!(target: "dispatch-thread",
                       "{} needs to resend shutdown messages",
                       id);

                match shutdown_retries {
                    Some(shutdown_retries) => {
                        *next = Some(next_retry_definite(next, &when));
                        shutdown_retries.push(id.clone())
                    }
                    None => {
                        let mut vec = Vec::with_capacity(nents);

                        *next = Some(next_retry_definite(next, &when));
                        vec.push(id.clone());
                        *shutdown_retries = Some(vec);
                    }
                }
            }

            if let Some(when) = ent.pending.retry_pending() &&
                when <= now
            {
                trace!(target: "dispatch-thread",
                       "{} needs to resend messages",
                       id);

                match retries {
                    Some(retries) => {
                        *next = Some(next_retry_definite(next, &when));
                        retries.push(id.clone())
                    }
                    None => {
                        let mut vec = Vec::with_capacity(nents);

                        *next = Some(next_retry_definite(next, &when));
                        vec.push(id.clone());
                        *retries = Some(vec);
                    }
                }
            }
        }
    }

    fn run(mut self) {
        let mut events = Events::with_capacity(self.nevents);
        let mut next_listen = None;
        let mut outbounds: Option<Vec<DispatchedID>> = None;
        let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
        let mut retries: Option<Vec<DispatchedID>> = None;
        let mut completes: Option<Vec<DispatchedID>> = None;
        let mut refreshes: Option<Vec<DispatchedID>> = None;
        let mut now;

        info!(target: "dispatch-thread",
              "mio polling thread starting");

        while {
            let mut next = next_listen;

            now = Instant::now();

            self.collect_actions(
                &mut next,
                &mut outbounds,
                &mut shutdown_retries,
                &mut retries,
                &mut completes,
                &mut refreshes,
                now
            );

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
                shutdown_retries.take(),
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
        let mut shutdown_retries: Option<BinaryHeap<_>> = None;

        info!(target: "dispatch-thread",
              "mio dispatch thread shutting down");

        // Shutdown all dispatched entries.
        for (party, token) in parties.into_iter() {
            if let Some(ent) = dispatched.remove(&token) {
                info!(target: "dispatch-thread",
                      "shutting down dispatched entry for {}",
                      party);

                if let Err(err) = ent.shutdown(&mut ctx, &mut shutdown_retries)
                {
                    error!(target: "dispatch-thread",
                           "error setting shutdown flag for {} ({}): {}",
                           token, party, err);
                }
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

            if let Err(err) = ent.shutdown(&mut ctx, &mut shutdown_retries) {
                error!(target: "dispatch-thread",
                       "error setting shutdown flag for {}: {}",
                       token, err);
            }
        }

        let DispatchThreadCtx {
            mut ctx,
            mut poll,
            channels,
            ..
        } = ctx;
        let mut channels = Some(channels);
        let mut next = None;

        while {
            let now = Instant::now();

            channels.is_some() &&
                (next.is_some_and(|next: Instant| next < now) || {
                    let duration = next.map(|next| next - now);

                    if let Some(duration) = &duration {
                        trace!(target: "dispatch-thread",
                           "waiting for poll for {}.{:03}",
                           duration.as_secs(), duration.subsec_millis());
                    } else {
                        trace!(target: "dispatch-thread",
                           "waiting for poll indefinitely");
                    }

                    poll.poll(&mut events, duration)
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
            let now = Instant::now();

            next = None;

            channels = if let Some(mut channels) = channels.take() {
                if let Some(mut retries) = shutdown_retries.take() {
                    let mut newents: Option<Vec<_>> = None;
                    let nsessions = retries.len();

                    while retries.peek().is_some_and(|ent| ent.when() <= now) {
                        if let Some(ent) = retries.pop() {
                            let (id, retry) = ent.take();

                            match channels.retry_shutdown_stream(
                                &mut ctx,
                                id.channel(),
                                id.param(),
                                retry
                            ) {
                                Ok(res) => {
                                    if let RetryResult::Retry(retry) = res {
                                        let ent =
                                            RetryHeapEntry::new(id, retry);

                                        match &mut newents {
                                            Some(newents) => {
                                                newents.push(ent);
                                            }
                                            None => {
                                                let mut heap =
                                                    Vec::with_capacity(
                                                        nsessions
                                                    );

                                                heap.push(ent);
                                                newents = Some(heap);
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!(target: "poll-thread",
                                           "error shutting down stream {}: {}",
                                           id, err);
                                }
                            }
                        } else {
                            error!(target: "poll-thread",
                                   "shutdown_retries.pop() should not be None")
                        }
                    }

                    if let Some(newents) = newents {
                        newents.into_iter().for_each(|ent| retries.push(ent));
                    }

                    if !retries.is_empty() {
                        shutdown_retries = Some(retries)
                    }
                }

                match channels.shutdown_listen(&mut ctx, &tokens) {
                    Ok(res) => res.map(|(channels, when)| {
                        next = when;

                        channels
                    }),
                    Err(err) => {
                        error!(target: "poll-thread",
                               "error listening during shutdown: {}",
                               err);

                        None
                    }
                }
            } else {
                error!(target: "poll-thread",
                       "channels should not be empty here");

                None
            }
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
        write!(f, "dispatched {}", self.0)
    }
}

impl<Report, Shutdown> Display for RecvStreamError<Report, Shutdown>
where
    Report: Display,
    Shutdown: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            RecvStreamError::Report { err } => err.fmt(f),
            RecvStreamError::Shutdown { err } => err.fmt(f)
        }
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

#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use constellation_auth::cred::NullCred;
#[cfg(test)]
use constellation_common::config::Create;

#[cfg(test)]
use crate::addrs::test::TestEndpoint;
#[cfg(test)]
use crate::channels::test::TestChannel;
#[cfg(test)]
use crate::channels::test::TestChannelParam;
#[cfg(test)]
use crate::channels::test::TestChannelsError;
#[cfg(test)]
use crate::channels::test::TestChannelsScript;
#[cfg(test)]
use crate::channels::test::TestStreamID;
#[cfg(test)]
use crate::init;
#[cfg(test)]
use crate::threads::test::TestChannelCore;
#[cfg(test)]
use crate::threads::test::TestCompletableError;
#[cfg(test)]
use crate::threads::test::TestDispatch;
#[cfg(test)]
use crate::threads::test::TestDispatchScriptEntry;
#[cfg(test)]
use crate::threads::test::TestError;
#[cfg(test)]
use crate::threads::test::TestPushModeScriptElem;
#[cfg(test)]
use crate::threads::test::TestRefreshError;
#[cfg(test)]
use crate::threads::test::TestRefreshRetry;
#[cfg(test)]
use crate::threads::test::ThreadTestTypes;

#[test]
fn test_recv_session_send() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: Some((Some(when), vec![String::from("hello")])),
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_retry_send() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: Some((Some(when), vec![String::from("hello")])),
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Retry(now)),
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(when)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert!(thread.stream_ids.is_empty());
    assert!(thread.dispatched.is_empty());

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: Some((Some(when), vec![String::from("hello")])),
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Err(TestChannelsError {
            scope: ErrorScope::Unrecoverable
        })],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );

    assert!(!res);
    assert!(thread.stream_ids.is_empty());
    assert!(thread.dispatched.is_empty());
    assert_eq!(next_listen, None);
}

#[test]
fn test_send_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Err(TestError {
        scope: ErrorScope::Unrecoverable
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            now,
            Ok(TestPushModeScriptElem {
                sends: Some((Some(later), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })
        ))),
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_retry_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            now,
            Err(TestError {
                scope: ErrorScope::Unrecoverable
            })
        ))),
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_send_retry_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            now,
            Ok(TestPushModeScriptElem {
                sends: None,
                retries: Some(Box::new((
                    when,
                    Ok(TestPushModeScriptElem {
                        sends: Some((Some(after), vec![String::from("hello")])),
                        retries: None,
                        indefs: None,
                        completes: None
                    })
                ))),
                indefs: None,
                completes: None
            })
        ))),
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(after)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_retry_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            now,
            Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: Some(Box::new(Ok(TestPushModeScriptElem {
                    sends: Some((Some(after), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })))
            })
        ))),
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(after)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        })))
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_complete_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Err(TestError {
                scope: ErrorScope::Unrecoverable
            })))
        })))
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_send_complete_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(after), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        })))
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(after)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_complete_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                when,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(after), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        })))
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(after)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_recv_one() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("hello")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_recv_two() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("hello")),
                        Ok(String::from("goodbye")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(
        recved,
        vec![
            (NullCred, String::from("hello")),
            (NullCred, String::from("goodbye"))
        ]
    );
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_recv_collide() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("goodbye")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![Ok(RetryResult::Success((None, None)))]
                )],
                vec![],
                None,
                Some(later)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(later))),
            Ok(RetryResult::Success(Some(later))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_recv_collide_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("goodbye")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![Err(TestChannelsError {
                        scope: ErrorScope::Unrecoverable
                    })]
                )],
                vec![],
                None,
                Some(later)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(later))),
            Ok(RetryResult::Success(Some(later))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);
}

#[test]
fn test_recv_session_recv_collide_retry_shutdown() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("goodbye")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![
                        Ok(RetryResult::Retry(when)),
                        Ok(RetryResult::Success((None, None))),
                    ]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(after))),
            Ok(RetryResult::Success(Some(after))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, Some(vec![DispatchedID(0)]));
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_recv_collide_retry_shutdown_error() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![
                        Ok(String::from("goodbye")),
                        Err(TestError {
                            scope: ErrorScope::WouldBlock
                        }),
                    ])
                    .expect("Expected success"),
                    vec![
                        Ok(RetryResult::Retry(when)),
                        Err(TestChannelsError {
                            scope: ErrorScope::Unrecoverable
                        }),
                    ]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(after))),
            Ok(RetryResult::Success(Some(after))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, Some(vec![DispatchedID(0)]));
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> = reports
        .lock()
        .expect("lock failed")
        .iter()
        .cloned()
        .collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);
}

#[test]
fn test_recv_session_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Success(Some(later))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_imm_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Err(TestRefreshError::Completable {
                        result: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                later
                            ))))
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_imm_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Err(TestRefreshError::Completable {
                        result: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                after
                            ))))
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Err(TestRefreshError::Completable {
                        result: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                after
                            ))))
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(post)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Err(TestRefreshError::Completable {
                        result: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                post
                            ))))
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        after
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_permanent() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Permanent {
                err: TestError {
                    scope: ErrorScope::Unrecoverable
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_refresh_complete_imm_permanent() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Err(TestRefreshError::Permanent {
                        err: TestError {
                            scope: ErrorScope::Unrecoverable
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_refresh_complete_permanent() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Err(TestRefreshError::Permanent {
                        err: TestError {
                            scope: ErrorScope::Unrecoverable
                        }
                    }))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_refresh_complete_imm_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Retry(
                        TestRefreshRetry {
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                after
                            )))),
                            when: when
                        }
                    )))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_complete_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(post)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Retry(
                        TestRefreshRetry {
                            result: Arc::new(Ok(RetryResult::Success(Some(
                                post
                            )))),
                            when: when
                        }
                    )))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        after
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(after)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_retry_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(post)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Retry(TestRefreshRetry {
                    result: Arc::new(Ok(RetryResult::Success(Some(post)))),
                    when: later
                }))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        after
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_retry_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Err(TestRefreshError::Completable {
                    result: TestCompletableError {
                        scope: ErrorScope::Retryable,
                        result: Arc::new(Ok(RetryResult::Success(Some(after))))
                    }
                })),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_retry_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(post)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Err(TestRefreshError::Completable {
                    result: TestCompletableError {
                        scope: ErrorScope::WouldBlock,
                        result: Arc::new(Ok(RetryResult::Success(Some(post))))
                    }
                })),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        after
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_refresh_retry_permanent() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Err(TestRefreshError::Permanent {
                    err: TestError {
                        scope: ErrorScope::Unrecoverable
                    }
                })),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(!res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);
}

#[test]
fn test_recv_session_send_indef() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![Ok(RetryResult::Success(Some(later)))]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Success(Some(later))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_retry_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                when,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(after), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Success(Some(after))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_complete_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(after), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Success(Some(after))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_indef_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: None
            }))),
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Success(Some(after))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(after)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_retry_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                later,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(post), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(post)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(post)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(post)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_complete_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(post), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(post)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(post)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(post)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_indef_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: None
            }))),
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(after)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![TestChannel::new(
                stream_id.clone(),
                TestChannelCore::create(vec![Err(TestError {
                    scope: ErrorScope::WouldBlock
                })])
                .expect("Expected success"),
                vec![]
            )],
            vec![],
            None,
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(when));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_retry_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                when,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(after), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(when));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_complete_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(after), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_indef_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: None
            }))),
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(later)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(later)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_retry_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                later,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(post), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(post)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(post)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(post))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, Some(vec![DispatchedID(0)]));
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_complete_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let post = after + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(post), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(post)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(post)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(post))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, Some(vec![DispatchedID(0)]));
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        later
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(post));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(post));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_indef_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: None,
            indefs: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: None
            }))),
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id =
        StreamID::new(endpoint, String::from("test-channel"), channel_param);
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(after)
            ))),
            Ok(RetryResult::Success((vec![], vec![], None, Some(after)))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_listen_refresh() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id = StreamID::new(
        endpoint,
        String::from("test-channel"),
        channel_param.clone()
    );
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![],
                vec![],
                Some(vec![(
                    String::from("test-channel"),
                    Some(vec![channel_param])
                )]),
                Some(later)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(later))),
            Ok(RetryResult::Success(Some(later))),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_listen_refresh_retry() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id = StreamID::new(
        endpoint,
        String::from("test-channel"),
        channel_param.clone()
    );
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![],
                vec![],
                Some(vec![(
                    String::from("test-channel"),
                    Some(vec![channel_param])
                )]),
                Some(after)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(after)))),
                when: when
            })),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_listen_refresh_complete_imm() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id = StreamID::new(
        endpoint,
        String::from("test-channel"),
        channel_param.clone()
    );
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![],
                vec![],
                Some(vec![(
                    String::from("test-channel"),
                    Some(vec![channel_param])
                )]),
                Some(later)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(later))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(later));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(later));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}

#[test]
fn test_recv_session_send_indef_listen_refresh_complete() {
    init();

    let pre = Instant::now();
    let now = pre + Duration::from_secs(1);
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("hello")])),
            retries: None,
            indefs: None,
            completes: None
        }))),
        completes: None
    })];
    let endpoint = TestEndpoint::from("test-addr");
    let channel_param = TestChannelParam {
        accepts: HashSet::from([endpoint.clone()])
    };
    let stream_id = StreamID::new(
        endpoint,
        String::from("test-channel"),
        channel_param.clone()
    );
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![
            Ok(RetryResult::Success((
                vec![TestChannel::new(
                    stream_id.clone(),
                    TestChannelCore::create(vec![Err(TestError {
                        scope: ErrorScope::WouldBlock
                    })])
                    .expect("Expected success"),
                    vec![]
                )],
                vec![],
                None,
                Some(now)
            ))),
            Ok(RetryResult::Success((
                vec![],
                vec![],
                Some(vec![(
                    String::from("test-channel"),
                    Some(vec![channel_param])
                )]),
                Some(after)
            ))),
        ],
        shutdown_listen: vec![]
    };
    let recvbuf = Arc::new(Mutex::new(vec![]));
    let dispatch = TestDispatchScriptEntry {
        msgs: recvbuf.clone(),
        stream_script: vec![
            Ok(RetryResult::Success(Some(now))),
            Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }),
        ]
    };
    let dispatcher =
        TestDispatch::create(vec![dispatch]).expect("Expected success");
    let config = DispatchThreadConfig::new(
        chans_config,
        mode_config,
        16,
        None,
        None,
        None
    );
    let mut thread: DispatchThread<ThreadTestTypes, _> =
        DispatchThread::create(config, dispatcher, ())
            .expect("Expected success");

    let mut next_listen = Some(pre);
    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        pre
    );
    assert_eq!(next_listen, Some(pre));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        pre
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(now));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![stream_id.clone()]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        now
    );
    assert_eq!(next_listen, Some(now));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        now
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        when
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, Some(vec![DispatchedID(0)]));

    let res = thread.handle_events(
        &mut next_listen,
        HashSet::new(),
        refreshes,
        outbounds,
        retries,
        shutdown_retries,
        completes,
        when
    );
    let dispatched = thread.stream_ids.get(&stream_id).expect("Expected some");
    let dispatched = thread.dispatched.get(&dispatched).expect("Expected some");
    let sendbuf = dispatched.dispatched.stream.sends.clone();
    let reports = dispatched.dispatched.stream.reports.clone();
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();
    let reported: Vec<TestStreamID> =
        reports.lock().expect("lock failed").drain().collect();

    assert!(res);
    assert_eq!(next_listen, Some(after));
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert_eq!(reported, vec![]);

    let mut outbounds: Option<Vec<DispatchedID>> = None;
    let mut shutdown_retries: Option<Vec<DispatchedID>> = None;
    let mut retries: Option<Vec<DispatchedID>> = None;
    let mut completes: Option<Vec<DispatchedID>> = None;
    let mut refreshes: Option<Vec<DispatchedID>> = None;

    thread.collect_actions(
        &mut next_listen,
        &mut outbounds,
        &mut shutdown_retries,
        &mut retries,
        &mut completes,
        &mut refreshes,
        later
    );
    assert_eq!(next_listen, Some(after));
    assert_eq!(outbounds, None);
    assert_eq!(shutdown_retries, None);
    assert_eq!(retries, None);
    assert_eq!(completes, None);
    assert_eq!(refreshes, None);
}
