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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
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
use constellation_common::retry::next_retry;
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

pub trait PollThreadTypes<Ctx>
where
    Ctx: 'static + Send {
    type Addr: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type ChannelParam: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type ChannelID: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: Display;
    type AuthNChan: 'static
        + Clone
        + AuthNed<Self::SessionPrin, Self::Chan>
        + Send;
    type Chan: Clone + PullStream<Self::Wrapper, PullError = Self::PullError>;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError: ScopedError + Send;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<
            Completable = Self::RefreshCompletableError,
            Permanent = Self::RefreshPermanentError
        >;
    type Stream: 'static
        + StreamRefresh<
            PollThreadCtx<Self::Chans, Ctx>,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        >
        + StreamReporter<
            Self::SessionPrin,
            StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
            Self::AuthNChan
        >
        + Send;
    type InMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type Wrapper;
    type Msgs: 'static + Send;
    type ChansSrcs;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type ChanShutdownError: Debug + Display;
    type Chans: 'static
        + ChannelsCreate<
            Ctx,
            Self::ChansSrcs,
            Config = Self::ChansConfig,
            CreateError = Self::ChansCreateError
        >
        + Channels<
            Ctx,
            Addr = Self::Addr,
            Param = Self::ChannelParam,
            Stream = Self::AuthNChan,
            ChannelID = Self::ChannelID
        >
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx, ShutdownStreamError = Self::ChanShutdownError>
        + Send;
    type MsgAuthConfig;
    type MsgAuth: 'static
        + Create<
            Config = Self::MsgAuthConfig,
            CreateError = Self::MsgAuthCreateError
        >
        + MsgAuthN<
            Self::InMsg,
            Self::Wrapper,
            Prin = Self::MsgPrin,
            AuthNMsg = Self::AuthNMsg,
            SessionPrin = Self::SessionPrin,
            Error = Self::MsgAuthError
        >
        + Send;
    type MsgAuthCreateError: Debug + Display;
    type MsgAuthError: Debug + Display + ScopedError;
    type RecvError: Debug + Display + ScopedError;
    type Recv: 'static
        + AuthNMsgRecv<
            Self::MsgPrin,
            Self::InMsg,
            Self::AuthNMsg,
            RecvError = Self::RecvError
        >
        + Send;
    type ModeConfig;
    type ModeCreateError: Debug + Display;
    type Mode: 'static
        + PushMode<Self::Stream, Self::Msgs, PollThreadCtx<Self::Chans, Ctx>>
        + Send;
}

pub struct PollThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx> {
    channels: Chans,
    ctx: Ctx,
    poll: Poll
}

pub struct PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>,
    Ctx: 'static + Send {
    pull_streams: HashMap<
        StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        Types::AuthNChan
    >,
    ctx: PollThreadCtx<Types::Chans, Ctx>,
    refresh_complete: Option<Types::RefreshCompletableError>,
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

impl<Chans, Ctx> Channels<()> for PollThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    type Addr = Chans::Addr;
    type ChannelID = Chans::ChannelID;
    type OutNegoParam = Chans::OutNegoParam;
    type Param = Chans::Param;
    type ParamError = Chans::ParamError;
    type ReqStreamError = Chans::ReqStreamError;
    type SelectParamIter<'a>
        = Chans::SelectParamIter<'a>
    where
        Self: 'a;
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
        RetryResult<(Option<Self::Stream>, bool, Option<Instant>)>,
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
    ) -> Result<
        RetryResult<(Self::SelectParamIter<'a>, Option<Instant>)>,
        Self::ParamError
    >
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

impl<Chans, Ctx> RegistryCtx for PollThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    #[inline]
    fn registry(&self) -> &Registry {
        self.poll.registry()
    }
}

#[derive(Debug)]
pub enum PollThreadCreateError<Mode, Channels, AuthN> {
    Mode { err: Mode },
    Channels { err: Channels },
    AuthN { err: AuthN }
}

#[derive(Debug)]
pub enum PollThreadRecvError<Pull, AuthN, Recv> {
    Pull { err: Pull },
    AuthN { err: AuthN },
    Recv { err: Recv }
}

impl<Chans, Ctx> PollThreadCtx<Chans, Ctx>
where
    Chans: Channels<Ctx>
{
    fn new(
        ctx: Ctx,
        channels: Chans,
        poll: Poll
    ) -> Self {
        PollThreadCtx {
            channels: channels,
            ctx: ctx,
            poll: poll
        }
    }
}

impl<Ctx, Types> PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>,
    Ctx: 'static + Send
{
    pub fn create(
        mode_config: Types::ModeConfig,
        chans_config: Types::ChansConfig,
        authn_config: Types::MsgAuthConfig,
        srcs: Types::ChansSrcs,
        mut ctx: Ctx,
        poll: Poll,
        recv: Types::Recv,
        msgs: Types::Msgs,
        notify: Arc<Waker>,
        notify_token: Token,
        stream: Types::Stream,
        shutdown: ShutdownFlag,
        nevents: usize,
        nsessions: Option<usize>
    ) -> Result<
        Self,
        PollThreadCreateError<
            Types::ModeCreateError,
            Types::ChansCreateError,
            Types::MsgAuthCreateError
        >
    >
    where
        Types::Mode: for<'a> CreateWithParam<
            &'a Types::Stream,
            Config = Types::ModeConfig,
            CreateError = Types::ModeCreateError
        > {
        let channels = Types::Chans::create(&mut ctx, chans_config, srcs)
            .map_err(|err| PollThreadCreateError::Channels { err: err })?;
        let mode = Types::Mode::create(mode_config, &stream)
            .map_err(|err| PollThreadCreateError::Mode { err: err })?;
        let authn = Types::MsgAuth::create(authn_config)
            .map_err(|err| PollThreadCreateError::AuthN { err: err })?;
        let pull_streams = match nsessions {
            Some(nsessions) => HashMap::with_capacity(nsessions),
            None => HashMap::new()
        };
        let ctx = PollThreadCtx::new(ctx, channels, poll);

        Ok(PollThread {
            pull_streams: pull_streams,
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
    Ctx: 'static + Send,
    Types: 'static + PollThreadTypes<Ctx>
{
    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
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

        if let Some(stream) = self.pull_streams.get_mut(&id) {
            let mut valid = true;

            while self.shutdown.is_live() && valid {
                trace!(target: "pull-streams-recv-thread",
                       "listening for message on {}",
                       id);

                match stream.get_mut().pull() {
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
                            return Err(PollThreadRecvError::Pull { err: err })
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
        id: StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) {
        debug!(target: "poll-thread",
               "receiving stream from {} for {}",
               id, stream.prin());

        // Report up to the stream.
        match self.stream.report_stream(
            stream.prin(),
            id.clone(),
            stream.clone()
        ) {
            Ok(res) => {
                let stream = match res {
                    Some(curr) => {
                        warn!(target: "poll-thread",
                              "stream {} with {} was already present",
                              id, curr.prin());

                        // Shut down the incoming stream.
                        if let Err(err) = self.ctx.channels.shutdown_stream(
                            &mut self.ctx.ctx,
                            id.channel(),
                            id.param(),
                            stream
                        ) {
                            error!(target: "poll-thread",
                                   "error shutting down stream {}: {}",
                                   id, err);
                        }

                        curr
                    }
                    None => stream
                };

                if self
                    .pull_streams
                    .insert(id.clone(), stream.clone())
                    .is_some()
                {
                    error!(target: "poll-thread",
                           "stream {} was already present for {}",
                           id, stream.prin());
                }
            }
            Err(err) => {
                error!(target: "poll-thread",
                       "error reporting stream {} with {}: {}",
                       id, stream.prin(), err);

                // Shut down the incoming stream.
                if let Err(err) = self.ctx.channels.shutdown_stream(
                    &mut self.ctx.ctx,
                    id.channel(),
                    id.param(),
                    stream
                ) {
                    error!(target: "poll-thread",
                           "error shutting down stream {}: {}",
                           id, err);
                }
            }
        }
    }

    fn handle_refresh_stream_error(
        &mut self,
        err: Types::RefreshError
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        match err.split() {
            (_, Some(err)) => {
                error!(target: "poll-thread",
                       "unrecoverable error refreshing stream: {}",
                       err);

                RetryResult::Success(None)
            }
            (Some(err), _) => {
                if err.scope() == ErrorScope::WouldBlock {
                    self.refresh_complete = Some(err);

                    RetryResult::Success(None)
                } else {
                    self.complete_refresh_stream(err)
                }
            }
            (None, None) => {
                error!(target: "poll-thread",
                       "refresh error split produced no results");

                RetryResult::Success(None)
            }
        }
    }

    fn complete_refresh_stream(
        &mut self,
        err: Types::RefreshCompletableError
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream
            .complete_refresh(&mut self.ctx, err)
            .unwrap_or_else(|err| self.handle_refresh_stream_error(err))
    }

    fn retry_refresh_stream(
        &mut self,
        retry: Types::RefreshRetry
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream
            .retry_refresh(&mut self.ctx, retry)
            .unwrap_or_else(|err| self.handle_refresh_stream_error(err))
    }

    fn refresh_stream(
        &mut self
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream
            .refresh(&mut self.ctx)
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
        let this_outbound = pending.take_next_outbound();

        // Complete any stalled sends first.
        if pending.take_has_completes() {
            match self.mode.complete_pending(
                &mut self.ctx,
                &mut self.msgs,
                &mut self.stream,
                &live
            ) {
                Ok(res) => {
                    pending.merge(&res);
                }
                Err(err) => {
                    error!(target: "poll-thread",
                           "error completing stalled sends: {}",
                           err)
                }
            }
        }

        // XXX Uncertain when next_retry is actually going to get set.

        // Push all pending messages.
        if pending.retry_pending().map_or(false, |when| when <= now) {
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
                Err(err) => {
                    error!(target: "poll-thread",
                           "error retrying pending messages: {}",
                           err);
                }
            }
        }

        // Do pulls before pushing new messages.
        let need_refresh = if next_listen.map_or(false, |when| when <= now) {
            trace!(target: "poll-thread",
                   "listening");

            match self.ctx.channels.listen(&mut self.ctx.ctx, &live) {
                Ok(RetryResult::Success((
                    streams,
                    endpoints,
                    refresh,
                    when
                ))) => {
                    *next_listen = when;

                    // Report new streams.
                    for (addr, channel_id, param, stream) in streams {
                        let id = StreamID::new(addr, channel_id, param);

                        self.recv_stream(id, stream)
                    }

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

                    refresh
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
            match self.complete_refresh_stream(refresh_complete) {
                RetryResult::Success(when) => {
                    *next_refresh = when;

                    if let Err(err) = self.mode.retry_indefs(
                        &mut self.ctx,
                        &mut self.msgs,
                        &mut self.stream
                    ) {
                        error!(target: "poll-thread",
                               "error completing refresh: {}",
                               err)
                    }
                }
                RetryResult::Retry(retry) => *retry_refresh = Some(retry)
            }
        } else if let Some(retry) = retry_refresh.take() {
            if retry.when() < now {
                trace!(target: "poll-thread",
                       "retrying stream refresh");

                match self.retry_refresh_stream(retry) {
                    RetryResult::Success(when) => {
                        *next_refresh = when;

                        if let Err(err) = self.mode.retry_indefs(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream
                        ) {
                            error!(target: "poll-thread",
                                   "error retrying refresh: {}",
                                   err)
                        }
                    }
                    RetryResult::Retry(retry) => *retry_refresh = Some(retry)
                }
            } else {
                *retry_refresh = Some(retry)
            }
        } else if next_refresh.map_or(false, |when| when <= now) || need_refresh
        {
            trace!(target: "poll-thread",
                   "refreshing stream");

            *next_refresh = None;

            match self.refresh_stream() {
                RetryResult::Success(when) => {
                    *next_refresh = when;

                    if let Err(err) = self.mode.retry_indefs(
                        &mut self.ctx,
                        &mut self.msgs,
                        &mut self.stream
                    ) {
                        error!(target: "poll-thread",
                               "error retrying refresh: {}",
                               err)
                    }
                }
                RetryResult::Retry(retry) => *retry_refresh = Some(retry)
            }
        }

        // XXX will need to have a mailbox to allow application layer
        // to signal that outbound messages are ready.

        // Push new messages.
        if this_outbound.map_or(false, |when| when <= now) ||
            live.contains(&self.notify_token)
        {
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
                Err(err) => {
                    error!(target: "poll-thread",
                           "error sending messages: {}",
                           err);
                }
            }
        }

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

        // Loop until told to shut down.
        while {
            let next = next_retry(&pending.next_outbound(),
                                  &pending.retry_pending());
            let next = next_retry(&next, &next_listen);
            let next = next_retry(&next, &next_refresh);

            now = Instant::now();

            self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                (next.is_some_and(|next: Instant| next < now) ||
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
                         .poll
                         .poll(&mut events, duration)
                         .inspect_err(|err| {
                             error!(target: "poll-thread",
                                    "error polling: {}",
                                    err)
                         })
                         .is_ok()
                 } ||
                 self.refresh_complete.is_some()) &&
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
            pull_streams,
            mut ctx,
            ..
        } = self;

        info!(target: "poll-thread",
              "mio polling thread shutting down");

        // Shut down all streams.
        for (id, stream) in pull_streams.into_iter() {
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

        let mut live = true;
        let mut next = None;

        while {
            let now = Instant::now();

            live && (next.is_some_and(|next: Instant| next < now) || {
                let duration = next.map(|next| next - now);

                if let Some(duration) = &duration {
                    trace!(target: "poll-thread",
                                "waiting for poll for {}.{:03}",
                                duration.as_secs(), duration.subsec_millis());
                } else {
                    trace!(target: "poll-thread",
                                "waiting for poll indefinitely");
                }

                ctx.poll
                    .poll(&mut events, duration)
                    .inspect_err(|err| {
                        error!(target: "poll-thread",
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
                    error!(target: "poll-thread",
                           "error listening during shutdown: {}",
                           err);

                    live = false;
                }
            }
        }

        if let Err(err) = ctx.channels.shutdown(&mut ctx.ctx) {
            error!(target: "poll-thread",
                   "error shutting down channels: {}",
                   err);
        }

        info!(target: "poll-thread",
              "mio polling thread exiting");
    }

    pub fn start(self) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("poll-thread"))
            .spawn(move || self.run())
    }
}

impl<Mode, Channels, AuthN> Display
    for PollThreadCreateError<Mode, Channels, AuthN>
where
    Channels: Display,
    Mode: Display,
    AuthN: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            PollThreadCreateError::Channels { err } => err.fmt(f),
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
