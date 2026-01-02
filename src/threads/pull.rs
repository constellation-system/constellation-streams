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

//! Pull stream management.
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::sleep;
use std::thread::spawn;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::error::MutexPoison;
use constellation_common::retry::RetryResult;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::stream::ConcurrentStream;
use crate::stream::PullStreamListener;
use crate::stream::StreamReporter;
use crate::stream::ThreadedStream;
use crate::threads::RecvThread;
use crate::threads::RecvThreadEntry;

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
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg> {
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
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg> {
    inner: Arc<PullStreams<Msg, Wrapper, Listener, AuthN, Recv>>
}

unsafe impl<Msg, Wrapper, Listener, AuthN, Recv> Send
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>
{
}

unsafe impl<Msg, Wrapper, Listener, AuthN, Recv> Sync
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>
{
}

impl<Msg, Wrapper, Listener, AuthN, Recv> Clone
    for PullStreamsReporter<Msg, Wrapper, Listener, AuthN, Recv>
where
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>
{
    #[inline]
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
                Ok(RetryResult::Success((stream, addr, prin))) => {
                    info!(target: "pull-streams-listen-thread",
                          "received new incoming stream from {}",
                          addr);

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
    ) -> Result<JoinHandle<()>, Error>
    where
        S: 'static
            + StreamReporter<
                Stream = ThreadedStream<Listener::Stream>,
                Prin = Listener::Prin,
                Src = Listener::Addr
            >
            + Send {
        Builder::new()
            .name(String::from("pull-streams-recv-thread"))
            .spawn(move || self.run(stream_reporter))
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
    Recv: 'static
        + AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>
        + Clone
        + Send,
    AuthN::Prin: 'static + Send
{
    type Prin = AuthN::SessionPrin;
    type ReportError = MutexPoison;
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

                Err(MutexPoison)
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
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg>,
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
