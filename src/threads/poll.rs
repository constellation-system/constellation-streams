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
use std::convert::Infallible;
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
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::RecoverableError;
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
use crate::channels::ChannelsListen;
use crate::channels::ChannelsCreate;
use crate::stream::PullStream;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::RegistryCtx;

pub trait PollThreadTypes<Ctx>
where Ctx: 'static + Send
{
    type Addr: 'static + Clone + Display + Eq + Hash + Send;
    type ChannelParam: 'static + Clone + Display + Eq + Hash + Send;
    type ChannelID: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: Display;
    type AuthNChan: 'static
        + Clone + AuthNed<Self::SessionPrin, Self::Chan> + Send;
    type Chan: Clone
        + PullStream<Self::Wrapper,
                     PullError = Self::PullError>;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<Completable = Self::RefreshCompletableError,
                           Permanent = Self::RefreshPermanentError>;
    type Stream: 'static
        + StreamRefresh<
            PollThreadCtx<
                Self::Chans,
                Ctx
            >,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        > + Send;
    type InMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type Wrapper;
    type Msgs: 'static + Send;
    type ChansSrcs;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type Chans: 'static
        + ChannelsCreate<Ctx, Self::ChansSrcs,
                         Config = Self::ChansConfig,
                         CreateError = Self::ChansCreateError>
        + Channels<Ctx,
                   Addr = Self::Addr,
                   Param = Self::ChannelParam,
                   Stream = Self::AuthNChan,
                   ChannelID = Self::ChannelID>
        + ChannelsListen<Ctx> + Send;
    type MsgAuthConfig;
    type MsgAuth: 'static
        + Create<Config = Self::MsgAuthConfig,
                 CreateError = Self::MsgAuthCreateError>
        + MsgAuthN<Self::InMsg, Self::Wrapper,
                   Prin = Self::MsgPrin,
                   AuthNMsg = Self::AuthNMsg,
                   SessionPrin = Self::SessionPrin,
                   Error = Self::MsgAuthError> + Send;
    type MsgAuthCreateError: Debug + Display;
    type MsgAuthError: Debug + Display + ScopedError;
    type RecvError: Debug + Display + ScopedError;
    type Recv: 'static
        + AuthNMsgRecv<Self::MsgPrin, Self::InMsg, Self::AuthNMsg,
                       RecvError = Self::RecvError> + Send;
    type ModeConfig;
    type ModeCreateError: Debug + Display;
    type Mode: 'static +
        PushMode<
            Self::Stream,
            Self::Msgs,
            PollThreadCtx<
                Self::Chans,
                Ctx
            >,
            Config = Self::ModeConfig,
            CreateError = Self::ModeCreateError
        > + Send;
}

pub struct PollThreadCtx<Chans, Ctx>
where Chans: Channels<Ctx>
{
    pull_streams: HashMap<
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream
    >,
    channels: Chans,
    ctx: Ctx,
    poll: Poll,
    nevents: usize
}

pub struct PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>,
    Ctx: 'static + Send
{
    ctx: PollThreadCtx<Types::Chans, Ctx>,
    authn: Types::MsgAuth,
    recv: Types::Recv,
    mode: Types::Mode,
    /// Source of outbound messages.
    msgs: Types::Msgs,
    notify: Arc<Waker>,
    /// Flag to use to shut the stream down.
    shutdown: ShutdownFlag,
    /// Stream to use to send.
    stream: Types::Stream,
}

impl<Chans, Ctx> Channels<()> for PollThreadCtx<Chans, Ctx>
where Chans: Channels<Ctx>
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

impl<Chans, Ctx> RegistryCtx for PollThreadCtx<Chans, Ctx>
where Chans: Channels<Ctx>
{
    #[inline]
    fn registry(&self) -> &Registry {
        self.poll.registry()
    }
}

impl <Party, Chans, Ctx>
    StreamReporter<
        Party,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
        ()
    >
    for PollThreadCtx<Chans, Ctx>
where Chans: Channels<Ctx>,
      Chans::Stream: Clone {
    type ReportStreamError = Infallible;

    fn report_stream(
        &mut self,
        _ctx: &mut (),
        _party: &Party,
        stream_id: StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        stream: Chans::Stream
    ) -> Result<Option<Chans::Stream>, Self::ReportStreamError> {
        match self.pull_streams.get(&stream_id) {
            Some(out) => Ok(Some(out.clone())),
            None => {
                if self.pull_streams.insert(stream_id, stream).is_some() {
                    error!(target: "poll-thread-context",
                           "insert should not return Some");
                }

                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
pub enum PollThreadCreateError<Mode, Channels, AuthN> {
    Mode {
        err: Mode
    },
    Channels {
        err: Channels
    },
    AuthN {
        err: AuthN
    },
    IO {
        err: Error
    }
}

#[derive(Debug)]
pub enum PollThreadRecvError<Pull, AuthN, Recv> {
    Pull {
        err: Pull
    },
    AuthN {
        err: AuthN
    },
    Recv {
        err: Recv
    }
}

impl<Chans, Ctx> PollThreadCtx<Chans, Ctx>
where Chans: Channels<Ctx>
{
    pub fn new(
        ctx: Ctx,
        channels: Chans,
        nevents: usize
    ) -> Result<Self, Error> {
        let poll = Poll::new()?;

        Ok(PollThreadCtx {
            pull_streams: HashMap::new(),
            channels: channels,
            ctx: ctx,
            poll: poll,
            nevents: nevents
        })
    }

    pub fn with_capacity(
        ctx: Ctx,
        channels: Chans,
        nevents: usize,
        nsessions: usize
    ) -> Result<Self, Error> {
        let poll = Poll::new()?;

        Ok(PollThreadCtx {
            pull_streams: HashMap::with_capacity(nsessions),
            channels: channels,
            ctx: ctx,
            poll: poll,
            nevents: nevents
        })
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
        recv: Types::Recv,
        msgs: Types::Msgs,
        notify: Arc<Waker>,
        stream: Types::Stream,
        shutdown: ShutdownFlag,
        nevents: usize,
        nsessions: Option<usize>
    ) -> Result<Self, PollThreadCreateError<Types::ModeCreateError,
                                            Types::ChansCreateError,
                                            Types::MsgAuthCreateError>>
    {
        let channels = Types::Chans::create(&mut ctx, chans_config, srcs)
            .map_err(|err| PollThreadCreateError::Channels { err: err })?;
        let mode = Types::Mode::create(&stream, mode_config)
            .map_err(|err| PollThreadCreateError::Mode { err: err })?;
        let authn = Types::MsgAuth::create(authn_config)
            .map_err(|err| PollThreadCreateError::AuthN { err: err })?;
        let ctx = match nsessions {
            Some(nsessions) =>
                PollThreadCtx::with_capacity(ctx, channels, nevents, nsessions),
            None => PollThreadCtx::new(ctx, channels, nevents),
        }.map_err(|err| PollThreadCreateError::IO { err: err })?;

        Ok(PollThread {
            authn: authn,
            mode: mode,
            msgs: msgs,
            notify: notify,
            shutdown: shutdown,
            stream: stream,
            recv: recv,
            ctx: ctx
        })
    }
}

impl<Ctx, Types> PollThread<Ctx, Types>
where
    Ctx: 'static + Send,
    Types: 'static + PollThreadTypes<Ctx>,
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

        match authn.msg_authn(session_prin, msg)
            .map_err(|err| PollThreadRecvError::AuthN {
                err: err
            })? {
            AuthNResult::Accept(msg) => {
                trace!(target: "poll-thread",
                       "authenticated message from {} ({}) as {}",
                       session_prin, id, msg.prin());

                recv.recv_auth_msg(msg)
                    .map_err(|err| PollThreadRecvError::Recv {
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

        if let Some(stream) = self.ctx.pull_streams.get_mut(&id) {
            let mut valid = true;

            while self.shutdown.is_live() && valid {
                trace!(target: "pull-streams-recv-thread",
                       "listening for message on {}",
                       id);

                match stream.get_mut().pull() {
                    Ok(msg) => Self::handle_msg(&mut self.authn, &mut self.recv,
                                                id, stream.prin(), msg)?,
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
        id: &StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
        stream: Types::AuthNChan
    ) {
        debug!(target: "poll-thread",
               "receiving stream from {}",
               id);

        if self.ctx.pull_streams.insert(id.clone(), stream.clone()).is_some() {
            error!(target: "poll-thread",
                   "stream was already present for {}",
                   id);
        }

        // Report up to the stream.
    }

    fn complete_refresh_stream(
        &mut self,
        err: Types::RefreshCompletableError
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream.complete_refresh(&mut self.ctx, err)
            .unwrap_or_else(|err| match err.split() {
                (_, Some(err)) => {
                    error!(target: "poll-thread",
                           "unrecoverable error refreshing stream: {}",
                           err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(err),
                (None, None) => {
                    error!(target: "poll-thread",
                           "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            })
    }

    fn retry_refresh_stream(
        &mut self,
        retry: Types::RefreshRetry
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream.retry_refresh(&mut self.ctx, retry)
            .unwrap_or_else(|err| match err.split() {
                (_, Some(err)) => {
                    error!(target: "poll-thread",
                           "unrecoverable error refreshing stream: {}",
                           err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(err),
                (None, None) => {
                    error!(target: "poll-thread",
                           "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            })
    }

    fn refresh_stream(
        &mut self
    ) -> RetryResult<Option<Instant>, Types::RefreshRetry> {
        self.stream.refresh(&mut self.ctx)
            .unwrap_or_else(|err| match err.split() {
                (_, Some(err)) => {
                    error!(target: "poll-thread",
                           "unrecoverable error refreshing stream: {}",
                           err);

                    RetryResult::Success(None)
                }
                (Some(err), _) => self.complete_refresh_stream(err),
                (None, None) => {
                    error!(target: "poll-thread",
                           "refresh error split produced no results");

                    RetryResult::Success(None)
                }
            })
    }

    fn run(mut self) {
        let mut events = Events::with_capacity(self.ctx.nevents);
        let mut next_pending = None;
        let mut next_outbound = None;
        let mut next_listen = None;
        let mut next_refresh = None;
        let mut retry_refresh: Option<Types::RefreshRetry> = None;
        let mut valid = true;
        let mut now;

        info!(target: "poll-thread",
              "mio polling thread starting");

        // Loop until told to shut down.
        while {
            let next = next_pending.map_or(next_outbound, |next| {
                Some(next_outbound.map_or(next, |when: Instant| when.min(next)))
            });
            let next = next.map_or(next_listen, |next| {
                Some(next_listen.map_or(next, |when: Instant| when.min(next)))
            });
            let next = next.map_or(next_refresh, |next| {
                Some(next_refresh.map_or(next, |when: Instant| when.min(next)))
            });

            now = Instant::now();

            valid && self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                next.is_some_and(|next: Instant| next < now) ||
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
                }
        } {
            // Gather up all the events.
            let live: HashSet<Token> = events
                .iter()
                .map(|event| event.token())
                .collect();

            // First push all pending messages.
            if next_pending.map_or(false, |when| when <= now) {
                trace!(target: "poll-thread",
                       "retrying pending messages");

                match self.mode.retry_pending(
                    &mut self.ctx,
                    &mut self.msgs,
                    &mut self.stream,
                    &live,
                    now,
                ) {
                    Ok(next) => {
                        next_pending = next;
                    }
                    Err(err) => {
                        error!(target: "poll-thread",
                               "error retrying pending messages: {}",
                               err);
                    }
                }
            }

            // Do pulls before pushing new messages.
            let need_refresh = if next_listen
                .map_or(false, |when| when <= now) {
                trace!(target: "poll-thread",
                       "listening");

                match self.ctx.channels.listen(&mut self.ctx.ctx, &live) {
                    Ok(RetryResult::Success((streams, endpoints,
                                             refresh, when))) => {
                        next_listen = when;

                        // Report new streams.
                        for (addr, channel_id, param, stream) in streams {
                            let id = StreamID::new(addr, channel_id, param);

                            self.recv_stream(&id, stream)
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
                    },
                    Ok(RetryResult::Retry(when)) => {
                        next_listen = Some(when);

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
            if let Some(retry) = retry_refresh.take() {
                if retry.when() < now {
                    trace!(target: "poll-thread",
                           "retrying stream refresh");

                    match self.retry_refresh_stream(retry) {
                        RetryResult::Success(when) => {
                            next_refresh = when;

                            if let Err(err) = self.mode.retry_indefs(
                                &mut self.ctx,
                                &mut self.msgs,
                                &mut self.stream,
                            ) {
                                error!(target: "poll-thread",
                                       "error retrying indefinite delays: {}",
                                       err)
                            }
                        }
                        RetryResult::Retry(retry) => {
                            retry_refresh = Some(retry)
                        }
                    }
                } else {
                    retry_refresh = Some(retry)
                }
            } else if next_refresh.map_or(false, |when| when <= now) ||
                need_refresh {
                trace!(target: "poll-thread",
                       "refreshing stream");

                match self.refresh_stream() {
                    RetryResult::Success(when) => {
                        next_refresh = when;

                        if let Err(err) = self.mode.retry_indefs(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream,
                        ) {
                            error!(target: "poll-thread",
                                   "error retrying indefinite delays: {}",
                                   err)
                        }
                    }
                    RetryResult::Retry(retry) => {
                        retry_refresh = Some(retry)
                    }
                }
            }

            // Push new messages.
            if next_outbound.map_or(false, |when| when <= now) {
                trace!(target: "poll-thread",
                       "pushing messages");

                match self.mode.send_from_outbound(
                    &mut self.ctx,
                    &mut self.msgs,
                    &mut self.stream,
                    &live
                ) {
                    Ok(next) => next_outbound = next,
                    Err(err) => {
                        error!(target: "poll-thread",
                               "error sending messages: {}",
                               err);
                    }
                }
            }
        }
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
    AuthN: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            PollThreadCreateError::Channels { err } => err.fmt(f),
            PollThreadCreateError::AuthN { err } => err.fmt(f),
            PollThreadCreateError::Mode { err } => err.fmt(f),
            PollThreadCreateError::IO { err } => write!(f, "{}", err)
        }
    }
}

impl<Pull, AuthN, Recv> Display for PollThreadRecvError<Pull, AuthN, Recv>
where
    Pull: Display,
    AuthN: Display,
    Recv: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            PollThreadRecvError::Pull { err } => err.fmt(f),
            PollThreadRecvError::AuthN { err } => err.fmt(f),
            PollThreadRecvError::Recv { err } => err.fmt(f),
        }
    }
}
