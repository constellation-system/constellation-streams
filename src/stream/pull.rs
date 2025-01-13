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

//! Pull stream management.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::spawn;
use std::thread::JoinHandle;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;

use crate::stream::ConcurrentStream;
use crate::stream::PullStream;
use crate::stream::PullStreamListener;
use crate::stream::StreamReporter;
use crate::stream::ThreadedStream;

struct RecvThreadEntry<Msg, Stream>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send {
    msg: PhantomData<Msg>,
    join: JoinHandle<()>,
    stream: ThreadedStream<Stream>
}

struct RecvThread<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    Addr: Display + Eq + Hash,
    AuthN: Clone + MsgAuthN<Msg, Wrapper>,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg> {
    msg: PhantomData<Msg>,
    authn: AuthN,
    shutdown: ShutdownFlag,
    stream: ThreadedStream<Stream>,
    recv: Recv,
    addr: Addr,
    session_prin: AuthN::SessionPrin,
    recvs: Arc<Mutex<HashMap<Addr, RecvThreadEntry<Wrapper, Stream>>>>
}

/// Listener thread for the entire "pull" side.
///
/// When [start](PullStreamsListenThread::start)ed, this will listen
/// for incoming sessions, convert them to streams, and then report
/// them to the push side.  This will eventually result in the push
/// side reporting them back to the pull side.
pub struct PullStreamsListenThread<Msg, Wrapper, Listener>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: 'static + ConcurrentStream + Credentials + Send,
    Listener::Addr: 'static + Send,
    Listener::Prin: 'static + Send,
    Wrapper: 'static + Send,
    Msg: 'static + Send {
    msg: PhantomData<Msg>,
    listener: Listener,
    shutdown: ShutdownFlag,
    recvs: Arc<
        Mutex<
            HashMap<Listener::Addr, RecvThreadEntry<Wrapper, Listener::Stream>>
        >
    >
}

/// Representation of the entire "pull side" in the streams API.
pub struct PullStreams<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg> {
    msg: PhantomData<Msg>,
    authn: AuthN,
    shutdown: ShutdownFlag,
    recv: Recv,
    streams: Arc<
        Mutex<
            HashMap<Listener::Addr, RecvThreadEntry<Wrapper, Listener::Stream>>
        >
    >
}

/// [StreamReporter] instance derived from a [PullStreams].
///
/// This is typically created once initialization is finished, and
/// given to the push side as a reporter.
pub struct PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg> {
    inner: Arc<PullStreams<Msg, Wrapper, Listener, AuthN, Recv>>
}

/// Errors that can occur reporting a stream through
/// [PullStreamsReportError].
#[derive(Debug)]
pub enum PullStreamsReportError {
    MutexPoison
}

#[derive(Debug)]
enum RecvSendError<AuthN> {
    AuthN { err: AuthN },
    Shutdown
}

impl<AuthN> ScopedError for RecvSendError<AuthN>
where
    AuthN: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            RecvSendError::AuthN { err } => err.scope(),
            RecvSendError::Shutdown => ErrorScope::Shutdown
        }
    }
}

unsafe impl<Msg, Wrapper, Listener, AuthN, Recv> Send
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>
{
}

unsafe impl<Msg, Wrapper, Listener, AuthN, Recv> Sync
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>
{
}

impl<Msg, Wrapper, Listener, AuthN, Recv> Clone
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>
{
    fn clone(&self) -> Self {
        PullStreamsReporter {
            inner: self.inner.clone()
        }
    }
}

impl<Msg, Wrapper, Listener> Drop
    for PullStreamsListenThread<Msg, Wrapper, Listener>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials + Send,
    Listener::Addr: Send,
    Listener::Prin: Send,
    Wrapper: Send,
    Msg: Send
{
    fn drop(&mut self) {
        debug!(target: "pull-streams-listen-thread",
               "listen thread dropped, terminating receiver threads");

        let ents = match self.recvs.lock() {
            Ok(mut recvs) => recvs.drain().collect(),
            Err(_) => {
                error!(target: "pull-streams-listen-thread",
                       "mutex poisoned");
                vec![]
            }
        };

        debug!(target: "pull-streams-listen-thread",
               "joining receiver threads");

        // Join all the remaining receiver threads.
        for (addr, entry) in ents {
            debug!(target: "pull-streams-listen-thread",
                   "waiting on receiver for {} to shut down",
                   addr);

            if entry.join.join().is_err() {
                error!(target: "pull-streams-listen-thread",
                       "could not join receiver thread for {}",
                       addr);
            }
        }
    }
}

impl<Msg, Wrapper, Addr, Stream, AuthN, Recv> Drop
    for RecvThread<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    Addr: Display + Eq + Hash,
    AuthN: Clone + MsgAuthN<Msg, Wrapper>,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>
{
    fn drop(&mut self) {
        if self.shutdown.is_live() {
            // Remove ourselves from the receiver threads.
            match self.recvs.lock() {
                Ok(mut guard) => {
                    trace!(target: "recv-thread",
                           "removing receiver for {}",
                           self.addr);

                    guard.remove(&self.addr);
                }
                Err(_) => {
                    error!(target: "recv-thread",
                           "mutex poisoned");
                }
            }
        }
    }
}

impl<Msg, Wrapper, Addr, Stream, AuthN, Recv>
    RecvThread<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    Addr: Display + Eq + Hash,
    AuthN: Clone + MsgAuthN<Msg, Wrapper>,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>
{
    fn handle_msg(
        &mut self,
        msg: Wrapper
    ) -> Result<(), RecvSendError<AuthN::Error>> {
        trace!(target: "pull-streams-recv-thread",
               "handling incoming message from {}",
               self.addr);
        // ISSUE #10: future: unwrap XCIAP here and report successes.

        match self.authn.msg_authn(&self.session_prin, msg) {
            Ok(AuthNResult::Accept((prin, msg))) => self
                .recv
                .recv_auth_msg(&prin, msg)
                .map_err(|_| RecvSendError::Shutdown),
            Ok(AuthNResult::Reject) => {
                warn!(target: "pull-streams-recv-thread",
                      "message from {} failed authentication, discarding",
                      self.addr);

                Ok(())
            }
            Err(err) => Err(RecvSendError::AuthN { err: err })
        }
    }

    fn run(&mut self) {
        let mut valid = true;

        debug!(target: "pull-streams-recv-thread",
               "starting receiver thread for {}",
               self.addr);

        while self.shutdown.is_live() && valid {
            trace!(target: "pull-streams-recv-thread",
                   "listening for message on {}",
                   self.addr);

            match self.stream.pull() {
                Ok(msg) => {
                    if let Err(err) = self.handle_msg(msg) {
                        if !err.is_shutdown() {
                            error!(target: "pull-streams-recv-thread",
                               "error handling message: {}",
                               err);
                        } else {
                            debug!(target: "pull-streams-recv-thread",
                               "receiver thread saw shutdown condition: {}",
                                   err);
                        }

                        match err.scope() {
                            ErrorScope::Retryable => {
                                error!(target: "pull-streams-recv-thread",
                                   "shouldn't see a retryable error here")
                            }
                            ErrorScope::Unrecoverable |
                            ErrorScope::Session |
                            ErrorScope::System |
                            ErrorScope::Shutdown => {
                                debug!(target: "pull-streams-recv-thread",
                                       "stopping thread");

                                valid = false;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => {
                    error!(target: "pull-streams-recv-thread",
                           "error receiving message: {}",
                           err);

                    match err.scope() {
                        ErrorScope::Retryable => {
                            error!(target: "pull-streams-recv-thread",
                                   "shouldn't see a retryable error here")
                        }
                        ErrorScope::Unrecoverable |
                        ErrorScope::Session |
                        ErrorScope::System |
                        ErrorScope::Shutdown => {
                            valid = false;
                        }
                        _ => {}
                    }
                }
            }
        }

        info!(target: "pull-streams-recv-thread",
              "receiver thread for {} exiting",
              self.addr);
    }
}

impl<Msg, Wrapper, Listener> PullStreamsListenThread<Msg, Wrapper, Listener>
where
    Listener: 'static + PullStreamListener<Wrapper> + Send,
    Listener::Stream: 'static + ConcurrentStream + Credentials + Send,
    Listener::Addr: 'static + Send + Sync,
    Listener::Prin: 'static + Send,
    Wrapper: 'static + Send,
    Msg: 'static + Send
{
    fn run<S>(
        &mut self,
        mut stream_reporter: S
    ) where
        S: StreamReporter<
            Stream = ThreadedStream<Listener::Stream>,
            Prin = Listener::Prin,
            Src = Listener::Addr
        > {
        let mut valid = true;

        debug!(target: "pull-streams-listen-thread",
               "listen thread starting");

        while self.shutdown.is_live() && valid {
            trace!(target: "pull-streams-listen-thread",
                   "listening for connection");

            match self.listener.listen() {
                Ok((addr, prin, stream)) => {
                    info!(target: "pull-streams-listen-thread",
                          "received new incoming stream from {}",
                          addr);

                    // ISSUE #11: handle session-level credentials and
                    // authentication here.

                    let stream =
                        ThreadedStream::new(self.shutdown.clone(), stream);

                    match stream_reporter.report(addr.clone(), prin, stream) {
                        Ok(None) => {
                            debug!(target: "pull-streams-listen-thread",
                                   "incoming stream registered for {}",
                                   addr);
                        }
                        Ok(Some(_)) => {
                            debug!(target: "pull-streams-listen-thread",
                                   "stream already exists for {}, aborting",
                                   addr);
                        }
                        Err(err) => {
                            error!(target: "pull-streams-listen-thread",
                                   "error reporting new stream: {}",
                                   err)
                        }
                    }
                }
                Err(err) => {
                    error!(target: "pull-streams-listen-thread",
                           "error listening for new sessions: {}",
                           err);

                    valid = false;
                }
            }
        }

        info!(target: "pull-streams-listen-thread",
              "listener thread exiting");
    }

    #[inline]
    pub fn start<S>(
        mut self,
        stream_reporter: S
    ) -> JoinHandle<()>
    where
        S: 'static
            + StreamReporter<
                Stream = ThreadedStream<Listener::Stream>,
                Prin = Listener::Prin,
                Src = Listener::Addr
            >
            + Send {
        spawn(move || self.run(stream_reporter))
    }
}

impl<Msg, Wrapper, Listener, AuthN, Recv> StreamReporter
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: 'static + ConcurrentStream + Credentials + Send,
    Listener::Addr: 'static + Send + Sync,
    Listener::Prin: 'static + Send,
    Wrapper: 'static + Send,
    Msg: 'static + Send,
    AuthN: 'static
        + Clone
        + MsgAuthN<Msg, Wrapper, SessionPrin = Listener::Prin>
        + Send,
    Recv: 'static + AuthNMsgRecv<AuthN::Prin, Msg> + Clone + Send,
    AuthN::Prin: 'static + Send
{
    type Prin = AuthN::SessionPrin;
    type ReportError = PullStreamsReportError;
    type Src = Listener::Addr;
    type Stream = ThreadedStream<Listener::Stream>;

    fn report(
        &mut self,
        src: Self::Src,
        prin: Self::Prin,
        stream: ThreadedStream<Listener::Stream>
    ) -> Result<Option<ThreadedStream<Listener::Stream>>, Self::ReportError>
    {
        debug!(target: "pull-streams-reporter",
               "reporting stream for {} to pull side",
               src);

        match self.inner.streams.lock() {
            Ok(mut guard) => match guard.entry(src.clone()) {
                // We've already got one.
                Entry::Occupied(ent) => Ok(Some(ent.get().stream.clone())),
                Entry::Vacant(ent) => {
                    debug!(target: "pull-streams-reporter",
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

                    debug!(target: "pull-streams-reporter",
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
                error!(target: "pull-streams-reporter",
                       "mutex poisoned");

                Err(PullStreamsReportError::MutexPoison)
            }
        }
    }
}

impl<Msg, Wrapper, Listener, AuthN, Recv>
    PullStreams<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: 'static + PullStreamListener<Wrapper> + Send,
    Listener::Stream: 'static + ConcurrentStream + Credentials + Send,
    Listener::Addr: 'static + Send + Sync,
    Listener::Prin: 'static + Send,
    Wrapper: 'static + Send,
    Msg: 'static + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send
{
    fn create(
        listener: Listener,
        recv: Recv,
        shutdown: ShutdownFlag,
        streams: Arc<
            Mutex<
                HashMap<
                    Listener::Addr,
                    RecvThreadEntry<Wrapper, Listener::Stream>
                >
            >
        >,
        authn: AuthN
    ) -> (Self, PullStreamsListenThread<Msg, Wrapper, Listener>) {
        let thread = PullStreamsListenThread {
            msg: PhantomData,
            listener: listener,
            shutdown: shutdown.clone(),
            recvs: streams.clone()
        };

        debug!(target: "pull-streams",
               "launching listener thread");

        let streams = PullStreams {
            msg: PhantomData,
            authn: authn,
            recv: recv,
            shutdown: shutdown,
            streams: streams
        };

        (streams, thread)
    }

    /// Create a new `PullStreams` from its essential components.
    ///
    /// The `listener` parameter is a [PullStreamListener] that will
    /// be used to obtain incoming sessions.  The `shutdown` parameter
    /// is a [ShutdownFlag] that will be used to shut down the pull
    /// side.  The `authn` parameter is the authenticator.
    ///
    /// This will also create a [PullStreamsListenThread].
    pub fn new(
        listener: Listener,
        recv: Recv,
        shutdown: ShutdownFlag,
        authn: AuthN
    ) -> (Self, PullStreamsListenThread<Msg, Wrapper, Listener>) {
        let streams = Arc::new(Mutex::new(HashMap::new()));

        Self::create(listener, recv, shutdown, streams, authn)
    }

    /// Create a new `PullStreams` from its essential components with
    /// a size hint for the total number of live streams.
    ///
    /// The `listener` parameter is a [PullStreamListener] that will
    /// be used to obtain incoming sessions.  The `shutdown` parameter
    /// is a [ShutdownFlag] that will be used to shut down the pull
    /// side.  The `authn` parameter is the authenticator.
    ///
    /// This will also create a [PullStreamsListenThread].
    pub fn with_capacity(
        listener: Listener,
        recv: Recv,
        shutdown: ShutdownFlag,
        authn: AuthN,
        size: usize
    ) -> (Self, PullStreamsListenThread<Msg, Wrapper, Listener>) {
        let streams = Arc::new(Mutex::new(HashMap::with_capacity(size)));

        Self::create(listener, recv, shutdown, streams, authn)
    }

    /// Convert this into a [PullStreamsReporter].
    #[inline]
    pub fn reporter(
        self
    ) -> PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv> {
        PullStreamsReporter {
            inner: Arc::new(self)
        }
    }
}

impl ScopedError for PullStreamsReportError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            PullStreamsReportError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<AuthN> Display for RecvSendError<AuthN>
where
    AuthN: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            RecvSendError::AuthN { err } => err.fmt(f),
            RecvSendError::Shutdown => write!(f, "upstream channel shut down")
        }
    }
}

impl Display for PullStreamsReportError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PullStreamsReportError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}
