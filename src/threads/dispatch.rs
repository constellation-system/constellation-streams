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
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::config::Create;
use constellation_common::error::ScopedError;
use constellation_common::net::PrivateMsgs;
use constellation_common::retry::RetryResult;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
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
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::RegistryCtx;

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
    type Stream;
    type Msgs: PrivateMsgs<Self::OutMsg>;
    type Recv: AuthNMsgRecv<Self::MsgPrin, Self::InMsg, Self::AuthNMsg>;
    type Chan: Credentials + PullStream<Self::Wrapper>;
    type AuthNChan: Clone + AuthNed<Self::SessionPrin, Self::Chan>;
    type ModeConfig;
    type ModeCreateError: Debug + Display;
    type Mode: PushMode<
        Self::Stream,
        Self::Msgs,
        Ctx,
        Config = Self::ModeConfig,
        CreateError = Self::ModeCreateError
    >;
}

pub trait DispatchCtxTypes<Ctx>: DispatchEntryTypes<Ctx> + Sized {
    type Addr: Clone + Debug + Display + Eq + Hash;
    type ChannelParam: Clone + Debug + Display + Eq + Hash
        + ChannelParam<Self::Addr>;
    type ChannelID: Clone + Debug + Display + Eq + Hash;
    type DispatchError: Debug + Display + ScopedError;
    type Disp: Dispatch<
        Self, Ctx,
        Msgs = Self::Msgs,
        Recv = Self::Recv,
        PushStream = Self::Stream,
        DispatchError = Self::DispatchError
    >;
}

pub trait DispatchTypes<Ctx>: DispatchCtxTypes<Ctx> {
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

pub struct DispatchedEntry<ID, Types, Ctx>
where
    Types: DispatchEntryTypes<Ctx>,
{
    ctx: PhantomData<Ctx>,
    dispatched: Dispatched<Types, Types::OutMsg, Types::Stream,
                           Types::Msgs, Types::Recv>,
    pull_streams: HashMap<ID, Types::AuthNChan>,
    mode: Types::Mode
}

pub struct DispatchThreadCtx<Types, Chans, Ctx>
where
    Types: DispatchCtxTypes<Ctx>,
    Chans: Channels<Ctx,
                    Addr = Types::Addr,
                    Param = Types::ChannelParam,
                    Stream = Types::AuthNChan,
                    ChannelID = Types::ChannelID>
{
    dispatched: HashMap<
        Types::SessionPrin,
        DispatchedEntry<
            StreamID<Types::Addr, Types::ChannelID, Types::ChannelParam>,
            Types,
            Ctx
        >
    >,
    dispatcher: Types::Disp,
    channels: Chans,
    notify: Arc<Waker>,
    ctx: Ctx,
    poll: Poll,
    nevents: usize
}


pub struct DispatchThread<Types, Ctx>
where
    Types: DispatchTypes<Ctx>,
{
    ctx: DispatchThreadCtx<Types, Types::Chans, Ctx>,
    shutdown: ShutdownFlag,
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
}

impl<ID, Types, Ctx>
    StreamReporter<
        Types::SessionPrin,
        ID,
        Types::AuthNChan,
        ()
    >
    for DispatchedEntry<ID, Types, Ctx>
where
    ID: Clone + Debug + Display + Debug + Eq + Hash,
    Types: DispatchEntryTypes<Ctx>
{
    type ReportStreamError = Infallible;

    fn report_stream(
        &mut self,
        _ctx: &mut (),
        _party: &Types::SessionPrin,
        stream_id: ID,
        stream: Types::AuthNChan
    ) -> Result<Option<Types::AuthNChan>, Self::ReportStreamError> {
        match self.pull_streams.get(&stream_id) {
            Some(out) => Ok(Some(out.clone())),
            None => {
                if self.pull_streams.insert(stream_id, stream).is_some() {
                    error!(target: "dispatch-thread-context",
                           "insert should not return Some");
                }

                Ok(None)
            }
        }
    }
}

impl<Types, Chans, Ctx> Channels<()> for DispatchThreadCtx<Types, Chans, Ctx>
where
    Types: DispatchCtxTypes<Ctx>,
    Chans: Channels<Ctx,
                    Addr = Types::Addr,
                    Param = Types::ChannelParam,
                    Stream = Types::AuthNChan,
                    ChannelID = Types::ChannelID>
{
    type ChannelID = Types::ChannelID;
    type Param = Types::ChannelParam;
    type ParamIter = Chans::ParamIter;
    type ParamError = Chans::ParamError;
    type OutNegoParam = Chans::OutNegoParam;
    type Addr = Types::Addr;
    type Stream = Types::AuthNChan;
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

impl<Types, Chans, Ctx> RegistryCtx for DispatchThreadCtx<Types, Chans, Ctx>
where
    Types: DispatchCtxTypes<Ctx>,
    Chans: Channels<Ctx,
                    Addr = Types::Addr,
                    Param = Types::ChannelParam,
                    Stream = Types::AuthNChan,
                    ChannelID = Types::ChannelID>
{
    #[inline]
    fn registry(&self) -> &Registry {
        self.poll.registry()
    }
}

impl<Types, Chans, Ctx>
    StreamReporter<
        Types::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
        ()
    >
    for DispatchThreadCtx<Types, Chans, Ctx>
where
    Types: DispatchCtxTypes<Ctx>,
    Chans: Channels<Ctx,
                    Addr = Types::Addr,
                    Param = Types::ChannelParam,
                    Stream = Types::AuthNChan,
                    ChannelID = Types::ChannelID>
{
    type ReportStreamError = Types::DispatchError;

    fn report_stream(
        &mut self,
        ctx: &mut (),
        party: &Types::SessionPrin,
        stream_id: StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        stream: Chans::Stream
    ) -> Result<Option<Chans::Stream>, Self::ReportStreamError> {
        match self.dispatched.entry(party.clone()) {
            Entry::Occupied(mut ent) => {
                let Ok(out) = ent.get_mut()
                    .report_stream(ctx, party, stream_id, stream);

                Ok(out)
            },
            Entry::Vacant(ent) => {
                debug!(target: "dispatch-thread-context",
                       "adding stream {} for {}",
                       stream_id, party);

                let dispatched = self.dispatcher
                    .dispatch(&mut self.ctx, party, self.notify.clone())?;
                // XXX use a size hint here.
                let pull_streams = HashMap::new();
                let mut dispatched = DispatchedEntry {
                    dispatched: dispatched,
                    pull_streams: pull_streams
                };
                let Ok(out) = dispatched
                    .report_stream(ctx, party, stream_id, stream);

                if out.is_some() {
                    error!(target: "dispatch-thread-context",
                           "result of report_stream should not be Some");
                }

                ent.insert(dispatched);

                Ok(out)
            }
        }
    }
}

impl<Types, Chans, Ctx> DispatchThreadCtx<Types, Chans, Ctx>
where
    Types: DispatchCtxTypes<Ctx>,
    Chans: Channels<Ctx,
                    Addr = Types::Addr,
                    Param = Types::ChannelParam,
                    Stream = Types::AuthNChan,
                    ChannelID = Types::ChannelID>
{
    #[inline]
    fn new(
        ctx: Ctx,
        poll: Poll,
        notify: Arc<Waker>,
        dispatcher: Types::Disp,
        channels: Chans,
        nevents: usize
    ) -> Self {
        DispatchThreadCtx {
            dispatched: HashMap::new(),
            dispatcher: dispatcher,
            channels: channels,
            notify: notify,
            ctx: ctx,
            poll: poll,
            nevents: nevents
        }
    }

    #[inline]
    fn with_capacity(
        ctx: Ctx,
        poll: Poll,
        notify: Arc<Waker>,
        dispatcher: Types::Disp,
        channels: Chans,
        nevents: usize,
        nsessions: usize
    ) -> Self {
        DispatchThreadCtx {
            dispatched: HashMap::with_capacity(nsessions),
            dispatcher: dispatcher,
            channels: channels,
            notify: notify,
            ctx: ctx,
            poll: poll,
            nevents: nevents
        }
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
        nsessions: Option<usize>
    ) -> Result<Self, DispatchThreadCreateError<Types::ChansCreateError>>
    {
        let channels = Types::Chans::create(&mut ctx, chans_config, srcs)
            .map_err(|err| DispatchThreadCreateError::Channels { err: err })?;
        let mode = Types::Mode::create(&stream, mode_config)
            .map_err(|err| DispatchThreadCreateError::Mode { err: err })?;
    }
}

/*
pub struct Dispatched<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>,
    Addr: Clone + Eq + Hash {
    msg: PhantomData<Msg>,
    shutdown: ShutdownFlag,
    authn: AuthN,
    recv: Recv,
    streams: HashMap<Addr, RecvThreadEntry<Msg, Stream>>
}

struct DispatchEntry<Msg, Addr, Stream, AuthN, Recv, Reporter>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>,
    Reporter: StreamReporter,
    Addr: Clone + Eq + Hash {
    inner: Dispatched<Msg, Addr, Stream, AuthN, Recv>,
    reporter: Reporter,
    push_thread: JoinHandle<()>
}

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
