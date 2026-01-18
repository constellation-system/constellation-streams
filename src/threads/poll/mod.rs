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
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
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
use crate::stream::PullStream;
use crate::stream::StreamID;
use crate::threads::PushMode;
use crate::threads::RegistryCtx;

pub trait PollThreadTypes<Ctx> {
    type Addr: Clone + Display + Eq + Hash;
    type Param: Clone + Display + Eq + Hash;
    type ChannelID: Clone + Display + Eq + Hash;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: Display;
    type AuthNChan: Clone + AuthNed<Self::SessionPrin, Self::Chan>;
    type Chan: PullStream<Self::Wrapper,
                          PullError = Self::PullError>;
    type PullError: Debug + Display + ScopedError;
    type Stream;
    type InMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type Wrapper;
    type Msgs;
    type ChansSrcs;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type Chans: ChannelsCreate<Ctx, Self::ChansSrcs,
                               Config = Self::ChansConfig,
                               CreateError = Self::ChansCreateError>
        + Channels<PollThreadCtx<Ctx>,
                   Addr = Self::Addr,
                   Param = Self::Param,
                   Stream = Self::AuthNChan,
                   ChannelID = Self::ChannelID>;
    type MsgAuthConfig;
    type MsgAuth: Create<Config = Self::MsgAuthConfig,
                         CreateError = Self::MsgAuthCreateError>
        + MsgAuthN<Self::InMsg, Self::Wrapper,
                   Prin = Self::MsgPrin,
                   AuthNMsg = Self::AuthNMsg,
                   SessionPrin = Self::SessionPrin,
                   Error = Self::MsgAuthError>;
    type MsgAuthCreateError: Debug + Display;
    type MsgAuthError: Debug + Display + ScopedError;
    type RecvError: Debug + Display + ScopedError;
    type Recv: AuthNMsgRecv<Self::MsgPrin, Self::InMsg, Self::AuthNMsg,
                            RecvError = Self::RecvError>;
    type ModeConfig;
    type ModeCreateError: Debug + Display;
    type Mode: Create<Config = Self::ModeConfig,
                      CreateError = Self::ModeCreateError>
        + PushMode<Self::Stream, Self::Msgs, PollThreadCtx<Ctx>>;
}

pub struct PollThreadCtx<Ctx> {
    ctx: Ctx,
    poll: Poll,
    nevents: usize
}

pub struct PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>
{
    ctx: PollThreadCtx<Ctx>,
    channels: Types::Chans,
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
    pull_streams: HashMap<StreamID<Types::Addr, Types::ChannelID, Types::Param>,
                          Types::AuthNChan>
}

impl<Ctx> RegistryCtx for PollThreadCtx<Ctx> {
    #[inline]
    fn registry(&self) -> &Registry {
        self.poll.registry()
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

impl<Ctx, Types> PollThread<Ctx, Types>
where
    Types: PollThreadTypes<Ctx>
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
        nstreams: Option<usize>
    ) -> Result<Self, PollThreadCreateError<Types::ModeCreateError,
                                            Types::ChansCreateError,
                                            Types::MsgAuthCreateError>>
    {
        let channels = Types::Chans::create(&mut ctx, chans_config, srcs)
            .map_err(|err| PollThreadCreateError::Channels { err: err })?;
        let mode = Types::Mode::create(mode_config)
            .map_err(|err| PollThreadCreateError::Mode { err: err })?;
        let authn = Types::MsgAuth::create(authn_config)
            .map_err(|err| PollThreadCreateError::AuthN { err: err })?;
        let poll = Poll::new()
            .map_err(|err| PollThreadCreateError::IO { err: err })?;
        let ctx = PollThreadCtx {
            poll: poll,
            ctx: ctx,
            nevents: nevents
        };
        let pull_streams = match nstreams {
            Some(size) => HashMap::with_capacity(size),
            None => HashMap::new()
        };

        Ok(PollThread {
            pull_streams: pull_streams,
            channels: channels,
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
    Types::Addr: 'static + Send,
    Types::AuthNChan: 'static + Send,
    Types::Chans: 'static + Send,
    Types::Mode: 'static + Send,
    Types::Msgs: 'static + Send,
    Types::MsgAuth: 'static + Send,
    Types::Param: 'static + Send,
    Types::Recv: 'static + Send,
    Types::Stream: 'static + Send,
    Types::ChannelID: 'static + Send
{
    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
    }

    fn handle_msg(
        authn: &mut Types::MsgAuth,
        recv: &mut Types::Recv,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::Param>,
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

    fn recv_stream(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::Param>,
        stream: Types::AuthNChan
    ) {
        debug!(target: "poll-thread",
               "receiving stream from {}",
               id);

        if self.pull_streams.insert(id.clone(), stream.clone()).is_some() {
            error!(target: "poll-thread",
                   "stream was already present for {}",
                   id);
        }

        // Report up to the stream.
    }

    fn recv_param(
        &mut self,
        channel_id: Types::ChannelID,
        param: Types::Param,
    ) {
        debug!(target: "poll-thread",
               "receiving parameter {} for {}",
               param, channel_id);
    }

    fn pull_msgs(
        &mut self,
        id: &StreamID<Types::Addr, Types::ChannelID, Types::Param>
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

    fn run(mut self) {
        let mut events = Events::with_capacity(self.ctx.nevents);
        let mut next_pending = None;
        let mut next_outbound = None;
        let mut next_listen = None;
        let mut valid = true;
        let mut now;

        info!(target: "poll-thread",
              "mio polling thread starting");

        // Loop until told to shut down.
        while {
            let next = next_pending.map_or(next_outbound, |next| {
                next_outbound.map(|when: Instant| when.max(next))
            });
            let next = next.map_or(next_listen, |next| {
                next_listen.map(|when: Instant| when.max(next))
            });

            now = Instant::now();

            valid && self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                next.is_some_and(|next: Instant| next < now) ||
                {
                    let duration = next.map(|next| next - now);

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
            if next_listen.map_or(false, |when| when <= now) {
                match self.channels.listen(&mut self.ctx, &live) {
                    Ok(RetryResult::Success((streams, endpoints,
                                             params, when))) => {
                        next_listen = when;

                        for (addr, channel_id, param, stream) in streams {
                            let id = StreamID::new(addr, channel_id, param);

                            self.recv_stream(&id, stream)
                        }

                        for (channel_id, param) in params {
                            self.recv_param(channel_id, param)
                        }

                        for (addr, channel_id, param) in endpoints {
                            let id = StreamID::new(addr, channel_id, param);

                            if let Err(err) = self.pull_msgs(&id) {
                                error!(target: "poll-thread",
                                       "error receiving messages from {}: {}",
                                       id, err);

                                valid = false;
                            }
                        }
                    },
                    Ok(RetryResult::Retry(when)) => {
                        next_listen = Some(when);
                    }
                    Err(err) => {
                        error!(target: "poll-thread",
                               "error listening: {}",
                               err);
                    }
                }
            }

            // Push new messages.
            if next_outbound.map_or(false, |when| when <= now) {
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
