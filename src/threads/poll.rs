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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::sync::Arc;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use mio::Events;
use mio::Registry;
use mio::Token;
use mio::Waker;

use crate::channels::Channels;
use crate::channels::ChannelsID;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::config::PollThreadConfig;
use crate::stream::PullStream;
use crate::stream::ShutdownStream;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::stream::StreamRetry;
use crate::threads::PushMode;
use crate::threads::PushModeResult;
use crate::threads::RegistryCtx;
use crate::threads::SelfPartyCtx;
use crate::threads::ThreadInnerCtx;
use crate::threads::TokensCtx;
use crate::threads::types::PollThreadTypes;

pub struct PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>> {
    self_party: Option<Party>,
    inner: ThreadInnerCtx<Ctx>,
    channels: Chans
}

pub struct PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>,
    Ctx: 'static + Send {
    pull_streams: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        Types::AuthNChan
    >,
    ctx: PollThreadCtx<Types::SessionPrin, Types::Chans, Ctx>,
    refresh_complete: Option<Types::RefreshCompletableError>,
    shutdown_retries: Option<
        BinaryHeap<
            StreamRetry<
                StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
                Types::ChanShutdownRetry
            >
        >
    >,
    authn: Types::MsgAuth,
    recv: Types::Recv,
    mode: Types::Mode,
    /// Source of outbound messages.
    msgs: Types::Msgs,
    notify: Arc<Waker>,
    notify_token: Token,
    /// Flag to use to shut the stream down.
    shutdown: ShutdownFlag,
    /// Stream to use to send.
    stream: Types::Stream,
    nevents: usize
}

pub trait MsgsWaker {
    fn set_waker(
        &mut self,
        waker: Arc<Waker>
    );
}

impl MsgsWaker for () {
    #[inline]
    fn set_waker(
        &mut self,
        _waker: Arc<Waker>
    ) {
    }
}

impl<Party, Chans, Ctx> ChannelsID for PollThreadCtx<Party, Chans, Ctx>
where
    Chans: ChannelsID + Channels<ThreadInnerCtx<Ctx>>
{
    type ChannelID = Chans::ChannelID;

    #[inline]
    fn channel_id(
        &self,
        name: &str
    ) -> Option<Self::ChannelID> {
        self.channels.channel_id(name)
    }
}

impl<Party, Chans, Ctx> Channels<()> for PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>>
{
    type Addr = Chans::Addr;
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
            &mut self.inner,
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
        self.channels.params(&mut self.inner, channels)
    }
}

impl<Party, Chans, Ctx> SelfPartyCtx<Party> for PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>>
{
    #[inline]
    fn self_party(&self) -> Option<&Party> {
        self.self_party.as_ref()
    }
}

impl<Party, Chans, Ctx> TokensCtx for PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>>
{
    #[inline]
    fn token(&mut self) -> Token {
        self.inner.token()
    }

    #[inline]
    fn free_token(
        &mut self,
        token: Token
    ) {
        self.inner.free_token(token)
    }
}

impl<Party, Chans, Ctx> RegistryCtx for PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>>
{
    #[inline]
    fn registry(&self) -> &Registry {
        self.inner.registry()
    }
}

#[derive(Debug)]
pub enum PollThreadCreateError<Mode, Channels, Stream, AuthN> {
    IO { err: Error },
    Mode { err: Mode },
    Channels { err: Channels },
    Stream { err: Stream },
    AuthN { err: AuthN }
}

#[derive(Debug)]
pub enum PollThreadRecvError<Pull, AuthN, Recv> {
    Pull { err: Pull },
    AuthN { err: AuthN },
    Recv { err: Recv }
}

impl<Party, Chans, Ctx> PollThreadCtx<Party, Chans, Ctx>
where
    Chans: Channels<ThreadInnerCtx<Ctx>>
{
    fn new(
        inner: ThreadInnerCtx<Ctx>,
        channels: Chans,
        self_party: Option<Party>
    ) -> Self {
        PollThreadCtx {
            self_party: self_party,
            channels: channels,
            inner: inner
        }
    }

    #[inline]
    pub fn inner(&self) -> &Ctx {
        self.inner.inner()
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut Ctx {
        self.inner.inner_mut()
    }
}

impl<Ctx, Types> PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>,
    Ctx: Send
{
    pub fn create(
        config: PollThreadConfig<
            Types::ChansConfig,
            Types::ModeConfig,
            Types::StreamConfig,
            Types::MsgAuthConfig
        >,
        self_party: Option<Types::SessionPrin>,
        ctx: Ctx,
        recv: Types::Recv,
        mut msgs: Types::Msgs
    ) -> Result<
        Self,
        PollThreadCreateError<
            Types::ModeCreateError,
            Types::ChansCreateError,
            Types::StreamCreateError,
            Types::MsgAuthCreateError
        >
    > {
        let (
            chans_config,
            mode_config,
            stream_config,
            authn_config,
            nevents,
            nsessions
        ) = config.take();
        let authn = Types::MsgAuth::create(authn_config)
            .map_err(|err| PollThreadCreateError::AuthN { err: err })?;
        let pull_streams = match nsessions {
            Some(nsessions) => HashMap::with_capacity(nsessions),
            None => HashMap::new()
        };
        let mut ctx = ThreadInnerCtx::new(ctx)
            .map_err(|err| PollThreadCreateError::IO { err: err })?;
        let channels = Types::Chans::create(chans_config, &mut ctx)
            .map_err(|err| PollThreadCreateError::Channels { err: err })?;
        let mut ctx = PollThreadCtx::new(ctx, channels, self_party);
        let stream = Types::Stream::create(stream_config, &mut ctx)
            .map_err(|err| PollThreadCreateError::Stream { err: err })?;
        let mode = Types::Mode::create(mode_config, &stream)
            .map_err(|err| PollThreadCreateError::Mode { err: err })?;
        let notify_token = ctx.token();
        let notify = Waker::new(ctx.inner.registry(), notify_token)
            .map_err(|err| PollThreadCreateError::IO { err: err })?;
        let notify = Arc::new(notify);
        let shutdown = ShutdownFlag::new(notify.clone());

        msgs.set_waker(notify.clone());

        Ok(PollThread {
            pull_streams: pull_streams,
            shutdown_retries: None,
            refresh_complete: None,
            authn: authn,
            mode: mode,
            msgs: msgs,
            notify: notify,
            notify_token: notify_token,
            shutdown: shutdown,
            stream: stream,
            nevents: nevents,
            recv: recv,
            ctx: ctx
        })
    }
}

impl<Ctx, Types> PollThread<Ctx, Types>
where
    Types: 'static + PollThreadTypes<Ctx>,
    Ctx: 'static + Send
{
    pub fn start(
        config: PollThreadConfig<
            Types::ChansConfig,
            Types::ModeConfig,
            Types::StreamConfig,
            Types::MsgAuthConfig
        >,
        self_party: Option<Types::SessionPrin>,
        ctx: Ctx,
        recv: Types::Recv,
        msgs: Types::Msgs
    ) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("poll-thread"))
            .spawn(move || {
                match Self::create(config, self_party, ctx, recv, msgs) {
                    Ok(poll) => poll.run(),
                    Err(err) => error!(target: "poll-thread",
                                       "error creating poll thread: {}",
                                       err)
                }
            })
    }

    #[inline]
    pub fn stream(&self) -> &Types::Stream {
        &self.stream
    }

    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
    }

    /// Shut down this `Dispatched`.
    ///
    /// This will trigger the [ShutdownFlag] associated with this
    /// `Dispatched`.
    #[inline]
    pub fn shutdown_flag(&mut self) -> Result<(), Error> {
        self.shutdown.set()
    }

    fn handle_msg(
        _stream: &mut Types::Stream,
        authn: &mut Types::MsgAuth,
        recv: &mut Types::Recv,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        session_prin: &Types::SessionPrin,
        msg: Types::Wrapper
    ) -> Result<
        (),
        PollThreadRecvError<
            Types::PullError,
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        trace!(target: "poll-thread",
               "handling incoming message from {} ({})",
               session_prin, id);

        // ISSUE #10: future: unwrap XCIAP here and
        // report successes.

        match authn
            .msg_authn(session_prin, msg)
            .map_err(|err| PollThreadRecvError::AuthN { err: err })?
        {
            AuthNResult::Accept(msg) => {
                trace!(target: "poll-thread",
                       "authenticated message from {} ({}) as {}",
                       session_prin, id, msg.prin());

                recv.recv_auth_msg(msg)
                    .map_err(|err| PollThreadRecvError::Recv { err: err })
            }
            AuthNResult::Reject(_) => {
                warn!(target: "poll-thread",
                      "authentication rejected message from {} ({})",
                      session_prin, id);

                Ok(())
            }
        }
    }

    fn pull_msgs(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>
    ) -> Result<
        (),
        PollThreadRecvError<
            Types::PullError,
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        debug!(target: "poll-thread",
               "pulling messages from {}",
               id);

        if let Some(stream) = self.pull_streams.get_mut(id) {
            let mut valid = true;

            while self.shutdown.is_live() && valid {
                trace!(target: "poll-thread",
                       "listening for message on {}",
                       id);

                match stream.pull() {
                    Ok(msg) => Self::handle_msg(
                        &mut self.stream,
                        &mut self.authn,
                        &mut self.recv,
                        id,
                        stream.prin(),
                        msg
                    )?,
                    Err(err) => match err.scope() {
                        ErrorScope::Retryable => {
                            error!(target: "poll-thread",
                                   "shouldn't see a retryable error here")
                        }
                        ErrorScope::WouldBlock => {
                            trace!(target: "poll-thread",
                                   "exhausted messages on {}",
                                   id);

                            valid = false;
                        }
                        ErrorScope::Unrecoverable |
                        ErrorScope::Session |
                        ErrorScope::System |
                        ErrorScope::Shutdown => {
                            return Err(PollThreadRecvError::Pull { err: err });
                        }
                        _ => {
                            error!(target: "poll-thread",
                                   "error receiving message: {}",
                                   err);
                        }
                    }
                }
            }
        } else {
            error!(target: "poll-thread",
                   "stream not found for {}",
                   id);
        }

        Ok(())
    }

    fn recv_stream(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) -> Result<
        (),
        PollThreadRecvError<
            Types::PullError,
            Types::MsgAuthError,
            Types::RecvError
        >
    > {
        debug!(target: "poll-thread",
               "receiving stream from {} for {}",
               id, stream.prin());

        // Report up to the stream.
        match self.stream.report_stream(
            stream.prin(),
            id.clone(),
            stream.clone()
        ) {
            Ok(Some(stream)) => {
                warn!(target: "poll-thread",
                      "stream {} with {} was already present",
                      id, stream.prin());

                match self.ctx.channels.shutdown_stream(
                    &mut self.ctx.inner,
                    id.channel(),
                    id.param(),
                    stream
                ) {
                    Ok(res) => {
                        if let RetryResult::Retry(retry) = res {
                            trace!(target: "poll-thread",
                               "retrying shutdown of stream {} later",
                               id);

                            let id = id.clone();
                            let ent = StreamRetry::new(id, retry);

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
                        }
                    }
                    Err(err) => {
                        error!(target: "poll-thread",
                               "error shutting down stream {}: {}",
                               id, err);
                    }
                }

                Ok(())
            }
            Ok(None) => {
                if self
                    .pull_streams
                    .insert(id.clone(), stream.clone())
                    .is_some()
                {
                    error!(target: "poll-thread",
                       "stream {} was already present for {}",
                       id, stream.prin());

                    Ok(())
                } else {
                    self.pull_msgs(id)
                }
            }
            Err(err) => {
                error!(target: "poll-thread",
                       "error reporting stream {} with {}: {}",
                       id, stream.prin(), err);

                // Shut down the incoming stream.
                if let Err(err) = self.ctx.channels.shutdown_stream(
                    &mut self.ctx.inner,
                    id.channel(),
                    id.param(),
                    stream
                ) {
                    error!(target: "poll-thread",
                           "error shutting down stream {}: {}",
                           id, err);
                }

                Ok(())
            }
        }
    }

    fn handle_refresh_stream_error(
        &mut self,
        err: Types::RefreshError
    ) -> RetryResult<(Option<Instant>, bool, bool), Types::RefreshRetry> {
        match err.split() {
            (_, Some(err)) => match err.scope() {
                ErrorScope::Unrecoverable | ErrorScope::System => {
                    error!(target: "poll-thread",
                           "fatal error refreshing stream: {}",
                           err);

                    RetryResult::Success((None, false, false))
                }
                ErrorScope::Shutdown => {
                    RetryResult::Success((None, false, false))
                }
                ErrorScope::WouldBlock => {
                    error!(target: "poll-thread",
                           "error of scope WouldBlock \
                            shouldn't be seen here");

                    RetryResult::Success((None, true, false))
                }
                _ => {
                    error!(target: "poll-thread",
                           "error refreshing stream: {}",
                           err);

                    RetryResult::Success((None, true, false))
                }
            },
            (Some(err), _) => {
                if err.scope() == ErrorScope::WouldBlock {
                    trace!(target: "poll-thread",
                           "deferring stream refresh");

                    self.refresh_complete = Some(err);

                    RetryResult::Success((None, true, false))
                } else {
                    self.complete_refresh_stream(err)
                }
            }
            (None, None) => {
                error!(target: "poll-thread",
                       "refresh error split produced no results");

                RetryResult::Success((None, true, false))
            }
        }
    }

    fn complete_refresh_stream(
        &mut self,
        err: Types::RefreshCompletableError
    ) -> RetryResult<(Option<Instant>, bool, bool), Types::RefreshRetry> {
        self.stream
            .complete_refresh(&mut self.ctx, err)
            .map(|res| res.map(|res| (res, true, true)))
            .unwrap_or_else(|err| self.handle_refresh_stream_error(err))
    }

    fn retry_refresh_stream(
        &mut self,
        retry: Types::RefreshRetry
    ) -> RetryResult<(Option<Instant>, bool, bool), Types::RefreshRetry> {
        self.stream
            .retry_refresh(&mut self.ctx, retry)
            .map(|res| res.map(|res| (res, true, true)))
            .unwrap_or_else(|err| self.handle_refresh_stream_error(err))
    }

    fn refresh_stream(
        &mut self
    ) -> RetryResult<(Option<Instant>, bool, bool), Types::RefreshRetry> {
        self.stream
            .refresh(&mut self.ctx)
            .map(|res| res.map(|res| (res, true, true)))
            .unwrap_or_else(|err| self.handle_refresh_stream_error(err))
    }

    /// Do one round of event handling.
    ///
    /// # Parameters
    ///
    /// - `events`: The [Events] structure.
    ///
    /// - `retry_refresh`: Mutable reference to the retry value for refreshing
    ///   streams, if there is one.  This should be updated if necessary.
    ///
    /// - `next_listen`: The next time to listen for messages.  This should be
    ///   updated if necessary.
    ///
    /// - `next_refresh`: The next time to refresh the streams.  This should be
    ///   updated if necessary.
    ///
    /// - `pending`: The status of pending messages.
    ///
    /// # Return Value
    ///
    /// Whether the event loop should continue.
    fn handle_events(
        &mut self,
        events: &mut Events,
        retry_refresh: &mut Option<Types::RefreshRetry>,
        next_refresh: &mut Option<Instant>,
        next_listen: &mut Option<Instant>,
        pending: &mut PushModeResult,
        now: Instant
    ) -> bool {
        // Gather up all the events.
        let live: HashSet<Token> =
            events.iter().map(|event| event.token()).collect();
        let mut valid = true;

        // First deal with stalled and pending sends.
        let outbound_ready =
            if pending.next_outbound().is_some_and(|when| when <= now) {
                let _ = pending.take_next_outbound();

                true
            } else {
                false
            };

        // Complete any stalled sends first.
        if pending.take_has_completes() {
            trace!(target: "poll-thread",
                   "processing stalled sends");

            match self.mode.complete_pending(
                &mut self.ctx,
                &mut self.msgs,
                &mut self.stream,
                &live
            ) {
                Ok(res) => {
                    pending.merge(&res);
                }
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "poll-thread",
                               "fatal error completing stalled sends: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "poll-thread",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "poll-thread",
                               "error completing stalled sends: {}",
                               err);
                    }
                }
            }
        }

        if let Some(mut retries) = self.shutdown_retries.take() {
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
                        &mut self.ctx.inner,
                        id.channel(),
                        id.param(),
                        retry
                    ) {
                        Ok(res) => {
                            if let RetryResult::Retry(retry) = res {
                                let ent = StreamRetry::new(id, retry);

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

        // Push all pending messages.
        if pending.retry_pending().is_some_and(|when| when <= now) {
            trace!(target: "poll-thread",
                   "retrying pending messages");

            let _ = pending.take_retry_pending();

            match self.mode.retry_pending(
                &mut self.ctx,
                &mut self.msgs,
                &mut self.stream,
                &live,
                now
            ) {
                Ok(res) => {
                    pending.merge(&res);
                }
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "poll-thread",
                               "fatal error sending messages: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "poll-thread",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "poll-thread",
                               "error sending messages: {}",
                               err);
                    }
                }
            }
        }

        // Do pulls before pushing new messages.
        let need_refresh = if !live.is_empty() ||
            next_listen.is_some_and(|when| when <= now)
        {
            trace!(target: "poll-thread",
                   "listening");

            match self.ctx.channels.listen(&mut self.ctx.inner, &live) {
                Ok(RetryResult::Success((
                    streams,
                    endpoints,
                    refresh,
                    when
                ))) => {
                    *next_listen = when;

                    trace!(target: "poll-thread",
                           "reporting new streams");

                    // Report new streams.
                    for (addr, channel_id, param, stream) in streams {
                        let id = StreamID::new(addr, channel_id, param);

                        if let Err(err) = self.recv_stream(&id, stream) {
                            error!(target: "poll-thread",
                                   "error receiving messages from {}: {}",
                                   id, err);
                        }
                    }

                    trace!(target: "poll-thread",
                           "listening for message");

                    // Pull in messages from all active streams.
                    for (addr, channel_id, param) in endpoints {
                        let id = StreamID::new(addr, channel_id, param);

                        if let Err(err) = self.pull_msgs(&id) {
                            error!(target: "poll-thread",
                                   "error receiving messages from {}: {}",
                                   id, err);

                            valid = false;
                        }
                    }

                    refresh.is_some()
                }
                Ok(RetryResult::Retry(when)) => {
                    *next_listen = Some(when);

                    false
                }
                Err(err) => {
                    error!(target: "poll-thread",
                           "error listening: {}",
                           err);

                    false
                }
            }
        } else {
            false
        };

        // Refresh the stream if needed.
        if let Some(refresh_complete) = self.refresh_complete.take() {
            trace!(target: "poll-thread",
                   "completing refresh");

            match self.complete_refresh_stream(refresh_complete) {
                RetryResult::Success((when, cont, refreshed)) => {
                    *next_refresh = when;
                    valid &= cont;

                    if refreshed {
                        trace!(target: "poll-thread",
                               "retrying indefinitely-delayed sends");

                        match self.mode.retry_indefs(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream
                        ) {
                            Ok(res) => {
                                pending.merge(&res);
                            }
                            Err(err) => match err.scope() {
                                ErrorScope::Unrecoverable |
                                ErrorScope::System => {
                                    error!(target: "poll-thread",
                                           "fatal error completing refresh: {}",
                                           err);

                                    valid = false;
                                }
                                ErrorScope::Shutdown => {
                                    valid = false;
                                }
                                ErrorScope::WouldBlock => {
                                    error!(target: "poll-thread",
                                           "error of scope WouldBlock \
                                            shouldn't be seen here");
                                }
                                _ => {
                                    error!(target: "poll-thread",
                                           "error completing refresh: {}",
                                           err);
                                }
                            }
                        }
                    }
                }
                RetryResult::Retry(retry) => *retry_refresh = Some(retry)
            }
        } else if let Some(retry) = retry_refresh.take() {
            if retry.when() <= now {
                trace!(target: "poll-thread",
                       "retrying stream refresh");

                match self.retry_refresh_stream(retry) {
                    RetryResult::Success((when, cont, refreshed)) => {
                        *next_refresh = when;
                        valid &= cont;

                        if refreshed {
                            trace!(target: "poll-thread",
                                   "retrying indefinitely-delayed sends");

                            match self.mode.retry_indefs(
                                &mut self.ctx,
                                &mut self.msgs,
                                &mut self.stream
                            ) {
                                Ok(res) => {
                                    pending.merge(&res);
                                }
                                Err(err) => match err.scope() {
                                    ErrorScope::Unrecoverable |
                                    ErrorScope::System => {
                                        error!(target: "poll-thread",
                                               "fatal error completing \
                                                refresh: {}",
                                               err);

                                        valid = false;
                                    }
                                    ErrorScope::Shutdown => {
                                        valid = false;
                                    }
                                    ErrorScope::WouldBlock => {
                                        error!(target: "poll-thread",
                                               "error of scope WouldBlock \
                                                shouldn't be seen here");
                                    }
                                    _ => {
                                        error!(target: "poll-thread",
                                               "error completing refresh: {}",
                                               err);
                                    }
                                }
                            }
                        }
                    }
                    RetryResult::Retry(retry) => *retry_refresh = Some(retry)
                }
            } else {
                *retry_refresh = Some(retry)
            }
        } else if next_refresh.is_some_and(|when| when <= now) || need_refresh {
            trace!(target: "poll-thread",
                   "refreshing stream");

            *next_refresh = None;

            match self.refresh_stream() {
                RetryResult::Success((when, cont, refreshed)) => {
                    *next_refresh = when;
                    valid &= cont;

                    trace!(target: "poll-thread",
                           "retrying indefinitely-delayed sends");

                    if refreshed {
                        trace!(target: "poll-thread",
                               "retrying indefinitely-delayed sends");

                        match self.mode.retry_indefs(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream
                        ) {
                            Ok(res) => {
                                pending.merge(&res);
                            }
                            Err(err) => match err.scope() {
                                ErrorScope::Unrecoverable |
                                ErrorScope::System => {
                                    error!(target: "poll-thread",
                                           "fatal error completing refresh: {}",
                                           err);

                                    valid = false;
                                }
                                ErrorScope::Shutdown => {
                                    valid = false;
                                }
                                ErrorScope::WouldBlock => {
                                    error!(target: "poll-thread",
                                           "error of scope WouldBlock \
                                            shouldn't be seen here");
                                }
                                _ => {
                                    error!(target: "poll-thread",
                                           "error completing refresh: {}",
                                           err);
                                }
                            }
                        }
                    }
                }
                RetryResult::Retry(retry) => *retry_refresh = Some(retry)
            }
        }

        // Push new messages.
        if outbound_ready || live.contains(&self.notify_token) {
            trace!(target: "poll-thread",
                   "pushing messages");

            match self.mode.send_from_outbound(
                &mut self.ctx,
                &mut self.msgs,
                &mut self.stream,
                &live
            ) {
                Ok(res) => {
                    pending.merge(&res);
                }
                Err(err) => match err.scope() {
                    ErrorScope::Unrecoverable | ErrorScope::System => {
                        error!(target: "poll-thread",
                               "fatal error sending messages: {}",
                               err);

                        valid = false;
                    }
                    ErrorScope::Shutdown => {
                        valid = false;
                    }
                    ErrorScope::WouldBlock => {
                        error!(target: "poll-thread",
                               "error of scope WouldBlock \
                                shouldn't be seen here");
                    }
                    _ => {
                        error!(target: "poll-thread",
                               "error sending messages: {}",
                               err);
                    }
                }
            }
        }

        let shutdown_retry = self
            .shutdown_retries
            .as_ref()
            .and_then(|retries| retries.peek().map(|ent| ent.when()));

        pending.merge_next_retry(&shutdown_retry);

        valid
    }

    fn run(mut self) {
        let mut events = Events::with_capacity(self.nevents);
        let mut next_listen = None;
        let mut next_refresh = None;
        let mut retry_refresh: Option<Types::RefreshRetry> = None;
        let mut pending =
            PushModeResult::new(Some(Instant::now()), None, false);
        let mut now;

        info!(target: "poll-thread",
              "mio polling thread starting");

        debug!(target: "poll-thread",
              "initial stream refresh");

        match self.refresh_stream() {
            RetryResult::Success((when, _, _)) => {
                next_refresh = when;
            }
            RetryResult::Retry(retry) => {
                retry_refresh = Some(retry);
            }
        }

        debug!(target: "poll-thread",
              "entering polling loop");

        // Loop until told to shut down.
        while {
            let next = self
                .shutdown_retries
                .as_ref()
                .and_then(|heap| heap.peek().map(|ent| ent.when()));
            let next = next_retry(&next, &pending.next_outbound());
            let next = next_retry(&next, &pending.retry_pending());
            let next = next_retry(&next, &next_listen);
            let next = next_retry(&next, &next_refresh);

            now = Instant::now();

            self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                (next.is_some_and(|next: Instant| next < now) ||
                 self.refresh_complete.is_some() ||
                 {
                     let duration = next.map(|next| next - now);

                     if let Some(duration) = &duration {
                         trace!(target: "poll-thread",
                                "waiting for poll for {}.{:03}",
                                duration.as_secs(), duration.subsec_millis());
                     } else {
                         trace!(target: "poll-thread",
                                "waiting for poll indefinitely");
                     }

                     self.ctx
                         .inner
                         .poll()
                         .poll(&mut events, duration)
                         .inspect_err(|err| {
                             error!(target: "poll-thread",
                                    "error polling: {}",
                                    err)
                         })
                         .is_ok()
                 }) &&
                self.handle_events(&mut events, &mut retry_refresh,
                                   &mut next_refresh, &mut next_listen,
                                   &mut pending, now)
        } {}

        self.shutdown(events)
    }

    fn shutdown(
        self,
        mut events: Events
    ) {
        let PollThread {
            mut shutdown_retries,
            stream,
            pull_streams,
            ctx,
            ..
        } = self;
        let PollThreadCtx {
            inner: mut ctx,
            mut channels,
            ..
        } = ctx;
        let nsessions = pull_streams.len();

        info!(target: "poll-thread-shutdown",
              "mio polling thread shutting down");

        debug!(target: "poll-thread-shutdown",
               "shutting down pull streams");

        // Shut down all streams.
        for (id, stream) in pull_streams.into_iter() {
            debug!(target: "poll-thread-shutdown",
                   "shutting down stream {} with {}",
                   id, stream.prin());

            match channels.shutdown_stream(
                &mut ctx,
                id.channel(),
                id.param(),
                stream
            ) {
                Ok(res) => {
                    if let RetryResult::Retry(retry) = res {
                        let id = id.clone();
                        let ent = StreamRetry::new(id, retry);

                        match &mut shutdown_retries {
                            Some(shutdown_retries) => {
                                shutdown_retries.push(ent);
                            }
                            None => {
                                let mut heap =
                                    BinaryHeap::with_capacity(nsessions);

                                heap.push(ent);
                                shutdown_retries = Some(heap);
                            }
                        }
                    }
                }
                Err(err) => {
                    error!(target: "poll-thread-shutdown",
                           "error shutting down stream {}: {}",
                           id, err);
                }
            }
        }

        debug!(target: "poll-thread-shutdown",
               "shutting down push streams");

        // Shut down the push stream.
        match stream.shutdown_stream(&mut ctx, &mut channels) {
            Ok(res) => {
                if let RetryResult::Retry(retries) = res {
                    match &mut shutdown_retries {
                        Some(shutdown_retries) => {
                            shutdown_retries.extend(retries);
                        }
                        None => {
                            let mut heap = BinaryHeap::with_capacity(nsessions);

                            heap.extend(retries);
                            shutdown_retries = Some(heap);
                        }
                    }
                }
            }
            Err(err) => {
                error!(target: "poll-thread-shutdown",
                       "error shutting down push stream: {}",
                       err);
            }
        }

        debug!(target: "poll-thread-shutdown",
               "finishing all shutdown negotiations");

        let mut channels = Some(channels);
        let mut next = Some(Instant::now());

        while {
            next = next_retry(
                &next,
                &shutdown_retries
                    .as_ref()
                    .and_then(|heap| heap.peek().map(|ent| ent.when()))
            );
            let now = Instant::now();

            channels.is_some() &&
                (next.is_some_and(|next: Instant| next < now) || {
                    let duration = next.map(|next| next - now);

                    if let Some(duration) = &duration {
                        trace!(target: "poll-thread-shutdown",
                           "waiting for poll for {}.{:03}",
                           duration.as_secs(), duration.subsec_millis());
                    } else {
                        trace!(target: "poll-thread-shutdown",
                           "waiting for poll indefinitely");
                    }

                    ctx.poll()
                        .poll(&mut events, duration)
                        .inspect_err(|err| {
                            error!(target: "poll-thread-shutdown",
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
                                        let ent = StreamRetry::new(id, retry);

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
                                    error!(target: "poll-thread-shutdown",
                                           "error shutting down stream {}: {}",
                                           id, err);
                                }
                            }
                        } else {
                            error!(target: "poll-thread-shutdown",
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
                        error!(target: "poll-thread-shutdown",
                               "error listening during shutdown: {}",
                               err);

                        None
                    }
                }
            } else {
                error!(target: "poll-thread-shutdown",
                       "channels should not be empty here");

                None
            };
        }

        info!(target: "poll-thread-shutdown",
              "mio polling thread exiting");
    }
}

impl<Mode, Channels, Stream, AuthN> Display
    for PollThreadCreateError<Mode, Channels, Stream, AuthN>
where
    Channels: Display,
    Mode: Display,
    Stream: Display,
    AuthN: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            PollThreadCreateError::IO { err } => write!(f, "{}", err),
            PollThreadCreateError::Channels { err } => err.fmt(f),
            PollThreadCreateError::Stream { err } => err.fmt(f),
            PollThreadCreateError::AuthN { err } => err.fmt(f),
            PollThreadCreateError::Mode { err } => err.fmt(f)
        }
    }
}

impl<Pull, AuthN, Recv> Display for PollThreadRecvError<Pull, AuthN, Recv>
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
            PollThreadRecvError::Pull { err } => err.fmt(f),
            PollThreadRecvError::AuthN { err } => err.fmt(f),
            PollThreadRecvError::Recv { err } => err.fmt(f)
        }
    }
}

#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use constellation_auth::authn::AuthNedDestruct;
#[cfg(test)]
use constellation_auth::cred::NullCred;

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
use crate::init;
#[cfg(test)]
use crate::threads::test::TestChannelCore;
#[cfg(test)]
use crate::threads::test::TestCompletableError;
#[cfg(test)]
use crate::threads::test::TestError;
#[cfg(test)]
use crate::threads::test::TestPushModeScriptElem;
#[cfg(test)]
use crate::threads::test::TestRecv;
#[cfg(test)]
use crate::threads::test::TestRefreshError;
#[cfg(test)]
use crate::threads::test::TestRefreshRetry;
#[cfg(test)]
use crate::threads::test::ThreadTestTypes;

#[test]
fn test_send() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: Some((Some(when), vec![String::from("hello")])),
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_later() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: Some((Some(later), vec![String::from("hello")])),
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(when), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_error() {
    init();

    let now = Instant::now();
    let mode_config = vec![Err(TestError {
        scope: ErrorScope::Unrecoverable
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            when,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_after_and_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![
        Ok(TestPushModeScriptElem {
            sends: Some((Some(when), vec![])),
            retries: Some(Box::new((
                when,
                Ok(TestPushModeScriptElem {
                    sends: Some((Some(later), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })
            ))),
            indefs: None,
            completes: None
        }),
        Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("goodbye")])),
            retries: None,
            indefs: None,
            completes: None
        }),
    ];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello"), String::from("goodbye")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_and_retry_after() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![
        Ok(TestPushModeScriptElem {
            sends: Some((Some(when), vec![])),
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
        }),
        Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("goodbye")])),
            retries: None,
            indefs: None,
            completes: None
        }),
    ];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello"), String::from("goodbye")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_retry_error() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            when,
            Err(TestError {
                scope: ErrorScope::Unrecoverable
            })
        ))),
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_retry_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            when,
            Ok(TestPushModeScriptElem {
                sends: None,
                retries: Some(Box::new((
                    later,
                    Ok(TestPushModeScriptElem {
                        sends: Some((Some(after), vec![])),
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_retry_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: Some(Box::new((
            when,
            Ok(TestPushModeScriptElem {
                sends: None,
                retries: None,
                indefs: None,
                completes: Some(Box::new(Ok(TestPushModeScriptElem {
                    sends: Some((Some(later), vec![String::from("hello")])),
                    retries: None,
                    indefs: None,
                    completes: None
                })))
            })
        ))),
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(when));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_after_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![
        Ok(TestPushModeScriptElem {
            sends: Some((Some(when), vec![])),
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(later), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }),
        Ok(TestPushModeScriptElem {
            sends: Some((Some(after), vec![String::from("goodbye")])),
            retries: None,
            indefs: None,
            completes: None
        }),
    ];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec!["hello", "goodbye"]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_complete_after() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![
        Ok(TestPushModeScriptElem {
            sends: Some((Some(when), vec![])),
            retries: None,
            indefs: None,
            completes: Some(Box::new(Ok(TestPushModeScriptElem {
                sends: Some((Some(after), vec![String::from("hello")])),
                retries: None,
                indefs: None,
                completes: None
            })))
        }),
        Ok(TestPushModeScriptElem {
            sends: Some((Some(later), vec![String::from("goodbye")])),
            retries: None,
            indefs: None,
            completes: None
        }),
    ];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(when));
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec!["hello", "goodbye"]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_complete_error() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: Some(Box::new(Err(TestError {
            scope: ErrorScope::Unrecoverable
        })))
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_complete_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec!["hello"]);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_complete_retry() {
    init();

    let now = Instant::now();
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
                later,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_listen_new_stream_recv_none() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
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
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_one() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
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
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_two() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
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
            Some(when)
        )))],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(
        recved,
        vec![
            (NullCred, String::from("hello")),
            (NullCred, String::from("goodbye"))
        ]
    );
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_collide() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
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
                Some(when)
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
                None
            ))),
        ],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_collide_error() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
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
                Some(when)
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
                None
            ))),
        ],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_collide_retry_shutdown() {
    init();

    let now = Instant::now();
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
                Some(when)
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
                        Ok(RetryResult::Retry(later)),
                        Ok(RetryResult::Success((None, None))),
                    ]
                )],
                vec![],
                None,
                None
            ))),
        ],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_listen_new_stream_recv_collide_retry_shutdown_error() {
    init();

    let now = Instant::now();
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
                Some(when)
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
                        Ok(RetryResult::Retry(later)),
                        Err(TestChannelsError {
                            scope: ErrorScope::Unrecoverable
                        }),
                    ]
                )],
                vec![],
                None,
                None
            ))),
        ],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(now);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![(NullCred, String::from("hello"))]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id.clone()])
    );

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert_eq!(
        *reports.lock().expect("lock failed"),
        HashSet::from([stream_id])
    );
}

#[test]
fn test_refresh() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(when)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_imm_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(later))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(later))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_imm_complete_imm_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_imm_complete_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_complete_imm_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    result: Arc::new(Ok(RetryResult::Success(Some(later))))
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_complete_success() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Err(TestRefreshError::Completable {
                result: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    result: Arc::new(Ok(RetryResult::Success(Some(after))))
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_permanent() {
    init();

    let now = Instant::now();
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Permanent {
        err: TestError {
            scope: ErrorScope::Unrecoverable
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_imm_permanent() {
    init();

    let now = Instant::now();
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Err(TestRefreshError::Permanent {
                err: TestError {
                    scope: ErrorScope::Unrecoverable
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Err(TestRefreshError::Permanent {
                err: TestError {
                    scope: ErrorScope::Unrecoverable
                }
            }))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_imm_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(later)))),
                when: when
            })))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_complete_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Retry(TestRefreshRetry {
                result: Arc::new(Ok(RetryResult::Success(Some(after)))),
                when: later
            })))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(later)))),
        when: when
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_retry_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Retry(TestRefreshRetry {
            result: Arc::new(Ok(RetryResult::Success(Some(after)))),
            when: later
        }))),
        when: when
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_retry_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Err(TestRefreshError::Completable {
            result: TestCompletableError {
                scope: ErrorScope::Retryable,
                result: Arc::new(Ok(RetryResult::Success(Some(later))))
            }
        })),
        when: when
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_retry_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Err(TestRefreshError::Completable {
            result: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                result: Arc::new(Ok(RetryResult::Success(Some(after))))
            }
        })),
        when: when
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_refresh_retry_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: None,
        completes: None
    })];
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Err(TestRefreshError::Permanent {
            err: TestError {
                scope: ErrorScope::Unrecoverable
            }
        })),
        when: when
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(now);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(when));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(!res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let stream_script = vec![];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        stream_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_refresh() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(later)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_retry_refresh() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                later,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(after)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_complete_refresh() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(after)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_indef_refresh() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(after)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_refresh_retry() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(after)))),
        when: later
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_retry_refresh_retry() {
    init();

    let now = Instant::now();
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
                after,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(post)))),
        when: later
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(after));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        after
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(post));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_complete_refresh_retry() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(post)))),
        when: later
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        after
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(post));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_indef_refresh_retry() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(after)))),
        when: later
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_refresh_complete_imm() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(later))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_retry_refresh_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let after = later + Duration::from_secs(1);
    let mode_config = vec![Ok(TestPushModeScriptElem {
        sends: None,
        retries: None,
        indefs: Some(Box::new(Ok(TestPushModeScriptElem {
            sends: None,
            retries: Some(Box::new((
                later,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(later));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_complete_refresh_complete_imm() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_indef_refresh_complete_imm() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_refresh_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_retry_refresh_complete() {
    init();

    let now = Instant::now();
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
                after,
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(post))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), Some(after));
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        after
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(post));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_complete_refresh_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(post))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        after
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(post));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), Some(post));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_indef_refresh_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = Some(when);
    let mut next_listen = None;
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(when));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, None);
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_listen_refresh() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![],
            vec![],
            Some(vec![(
                String::from("test-channel"),
                Some(vec![channel_param])
            )]),
            Some(later)
        )))],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Success(Some(later)))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(when);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, Some(later));
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_listen_refresh_retry() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![],
            vec![],
            Some(vec![(
                String::from("test-channel"),
                Some(vec![channel_param])
            )]),
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Ok(RetryResult::Retry(TestRefreshRetry {
        result: Arc::new(Ok(RetryResult::Success(Some(after)))),
        when: later
    }))];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(when);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert_eq!(retry_refresh.as_ref().map(|retry| retry.when), Some(later));
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(after));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, Some(after));
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_listen_refresh_complete_imm() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![],
            vec![],
            Some(vec![(
                String::from("test-channel"),
                Some(vec![channel_param])
            )]),
            Some(later)
        )))],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::Retryable,
            result: Arc::new(Ok(RetryResult::Success(Some(later))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(when);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(later));
    assert_eq!(next_listen, Some(later));
    assert_eq!(pending.next_outbound(), Some(later));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}

#[test]
fn test_send_indef_listen_refresh_complete() {
    init();

    let now = Instant::now();
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
    let chans_config = TestChannelsScript {
        req_streams: vec![],
        listen: vec![Ok(RetryResult::Success((
            vec![],
            vec![],
            Some(vec![(
                String::from("test-channel"),
                Some(vec![channel_param])
            )]),
            Some(after)
        )))],
        shutdown_listen: vec![]
    };
    let refresh_script = vec![Err(TestRefreshError::Completable {
        result: TestCompletableError {
            scope: ErrorScope::WouldBlock,
            result: Arc::new(Ok(RetryResult::Success(Some(after))))
        }
    })];
    let recv = TestRecv::default();
    let recvbuf = recv.msgs.clone();
    let config = PollThreadConfig::new(
        chans_config,
        mode_config,
        refresh_script,
        (),
        16,
        None
    );
    let mut poll: PollThread<_, ThreadTestTypes> =
        PollThread::create(config, None, (), recv, ())
            .expect("Expected success");
    let reports = poll.stream().reports.clone();
    let sendbuf = poll.stream().sends.clone();

    let mut events = Events::with_capacity(16);
    let mut retry_refresh = None;
    let mut next_refresh = None;
    let mut next_listen = Some(when);
    let mut pending = PushModeResult::new(Some(now), None, false);
    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        now
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(when));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        when
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, None);
    assert_eq!(next_listen, Some(after));
    assert_eq!(pending.next_outbound(), None);
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(*sendbuf.lock().expect("lock failed"), vec![] as Vec<String>);
    assert!(reports.lock().expect("lock failed").is_empty());

    let res = poll.handle_events(
        &mut events,
        &mut retry_refresh,
        &mut next_refresh,
        &mut next_listen,
        &mut pending,
        later
    );
    let recved: Vec<(NullCred, String)> = recvbuf
        .lock()
        .expect("lock failed")
        .drain(..)
        .map(|authned| authned.take())
        .collect();

    assert!(res);
    assert!(retry_refresh.is_none());
    assert_eq!(next_refresh, Some(after));
    assert_eq!(next_listen, Some(after));
    assert_eq!(pending.next_outbound(), Some(after));
    assert_eq!(pending.retry_pending(), None);
    assert!(!pending.has_completes());
    assert_eq!(recved, vec![]);
    assert_eq!(
        *sendbuf.lock().expect("lock failed"),
        vec![String::from("hello")]
    );
    assert!(reports.lock().expect("lock failed").is_empty());
}
