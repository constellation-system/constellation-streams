// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::sleep;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::error::MutexPoison;
use constellation_common::error::WithMutexPoison;
use constellation_common::net::PrivateMsgs;
use constellation_common::retry::RetryResult;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::stream::ConcurrentStream;
use crate::stream::PullStream;
use crate::stream::PullStreamListener;
use crate::stream::PushStreamReporter;
use crate::stream::StreamReporter;
use crate::stream::ThreadedStream;
use crate::threads::push::PushMode;
use crate::threads::push::PushStreamThread;
use crate::threads::RecvThread;
use crate::threads::RecvThreadEntry;

pub trait Dispatch<Msg, Addr, Stream, AuthN, Ctx>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Msg: 'static + Clone + Send,
    Addr: Clone + Eq + Hash {
    /// Type of top-level push-side streams to be returned from
    /// dispatch.
    type PushStream: Send;
    /// Type of outbound message structures.
    ///
    /// This will be used by the created [PushStreamPrivateThread] to
    /// obtain messages to be sent using the
    /// [PushStream](Dispatch::PushStream) instance.
    type Msgs: 'static + PrivateMsgs<Msg> + Send;
    /// Type of authenticated message receivers.
    ///
    /// This will be used to deliver incoming messages.
    type Recv: 'static + AuthNMsgRecv<AuthN::Prin, Msg> + Send;
    /// Type of errors that can occur during dispatch.
    type DispatchError: Display;

    /// Obtain the components of a new private session.
    fn dispatch(
        &mut self,
        ctx: &mut Ctx,
        prin: AuthN::SessionPrin
    ) -> Result<
        (
            Self::PushStream,
            Self::Msgs,
            Notify,
            Dispatched<Msg, Addr, Stream, AuthN, Self::Recv>
        ),
        Self::DispatchError
    >;
}

pub struct Dispatched<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash {
    msg: PhantomData<Msg>,
    shutdown: ShutdownFlag,
    authn: AuthN,
    recv: Recv,
    streams: Arc<Mutex<HashMap<Addr, RecvThreadEntry<Msg, Stream>>>>
}

struct DispatchEntry<Msg, Addr, Stream, AuthN, Recv, Reporter>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
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

/// [StreamReporter] instance derived from an entry in a
/// [PullStreamsDispatchThread].
///
/// This is typically created to serve much the same purpose as a
/// [PullStreamsReporter], but for a single principal.
pub struct DispatchEntryReporter<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash {
    inner: Dispatched<Msg, Addr, Stream, AuthN, Recv>
}

unsafe impl<Msg, Addr, Stream, AuthN, Recv> Sync
    for Dispatched<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
}

impl<Msg, Addr, Stream, AuthN, Recv> Clone
    for Dispatched<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: Clone + AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
    fn clone(&self) -> Self {
        Dispatched {
            msg: self.msg,
            authn: self.authn.clone(),
            recv: self.recv.clone(),
            streams: self.streams.clone(),
            shutdown: self.shutdown.clone()
        }
    }
}

impl<Msg, Addr, Stream, AuthN, Recv> Clone
    for DispatchEntryReporter<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: Clone + AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        DispatchEntryReporter {
            inner: self.inner.clone()
        }
    }
}

impl<Msg, Addr, Stream, AuthN, Recv> Dispatched<Msg, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: Clone + MsgAuthN<Msg, Msg> + Send,
    Recv: Clone + AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
    pub fn new(
        shutdown: ShutdownFlag,
        authn: AuthN,
        recv: Recv
    ) -> Self {
        let streams = HashMap::new();
        let streams = Arc::new(Mutex::new(streams));

        Dispatched {
            msg: PhantomData,
            shutdown: shutdown,
            authn: authn,
            recv: recv,
            streams: streams
        }
    }

    #[inline]
    pub fn reporter(
        &self
    ) -> DispatchEntryReporter<Msg, Addr, Stream, AuthN, Recv> {
        DispatchEntryReporter {
            inner: self.clone()
        }
    }
}

impl<Msg, Addr, Stream, AuthN, Recv> StreamReporter
    for DispatchEntryReporter<Msg, Addr, Stream, AuthN, Recv>
where
    Msg: 'static + Send,
    Stream: 'static + ConcurrentStream + Credentials + PullStream<Msg> + Send,
    AuthN: 'static + Clone + MsgAuthN<Msg, Msg> + Send,
    AuthN::SessionPrin: Send,
    Recv: 'static + Clone + AuthNMsgRecv<AuthN::Prin, Msg> + Send,
    Addr: 'static + Clone + Display + Eq + Hash + Send
{
    type Prin = AuthN::SessionPrin;
    type ReportError = MutexPoison;
    type Src = Addr;
    type Stream = ThreadedStream<Stream>;

    fn report(
        &mut self,
        src: Self::Src,
        prin: Self::Prin,
        stream: ThreadedStream<Stream>
    ) -> Result<Option<ThreadedStream<Stream>>, Self::ReportError> {
        debug!(target: "dispatch-entry-reporter",
               "reporting stream for {} to pull side",
               src);

        match self.inner.streams.lock() {
            Ok(mut guard) => match guard.entry(src.clone()) {
                // We've already got one.
                Entry::Occupied(ent) => Ok(Some(ent.get().stream.clone())),
                Entry::Vacant(ent) => {
                    debug!(target: "dispatch-entry-reporter",
                           "adding stream for {} to listeners",
                           src);

                    let mut thread = RecvThread {
                        msg: PhantomData,
                        authn: self.inner.authn.clone(),
                        recv: self.inner.recv.clone(),
                        shutdown: self.inner.shutdown.clone(),
                        stream: stream.clone(),
                        addr: src.clone(),
                        session_prin: prin,
                        recvs: self.inner.streams.clone()
                    };

                    debug!(target: "dispatch-entry-reporter",
                           "launching receiver for {}",
                           src);

                    let join = spawn(move || thread.run());
                    let entry = RecvThreadEntry {
                        msg: PhantomData,
                        join: join,
                        stream: stream
                    };

                    ent.insert(entry);

                    Ok(None)
                }
            },
            Err(_) => {
                error!(target: "dispatch-entry-reporter",
                       "mutex poisoned");

                Err(MutexPoison)
            }
        }
    }
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
    ) -> Result<(), WithMutexPoison<Dispatcher::DispatchError>> {
        match self
            .recvs
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
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
                    .map_err(|err| WithMutexPoison::Inner { error: err })?;
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
                let join = push_thread.start();
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
    pub fn start(mut self) -> JoinHandle<()> {
        spawn(move || self.run())
    }
}
