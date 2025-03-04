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

use crate::error::BatchError;
use crate::stream::ConcurrentStream;
use crate::stream::PullStream;
use crate::stream::PullStreamListener;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamReporter;
use crate::stream::StreamReporter;
use crate::stream::ThreadedStream;
use crate::threads::push::private::PushStreamPrivateThread;
use crate::threads::RecvThread;
use crate::threads::RecvThreadEntry;

pub trait Dispatch<Msg, Prin, Ctx>
where
    Msg: 'static + Clone + Send {
    /// Type of top-level push-side streams to be returned from
    /// dispatch.
    type Stream: 'static
        + PushStreamReportBatchError<
            <<Self::Stream as PushStream<Ctx>>::FinishBatchError as BatchError>::Permanent,
            <Self::Stream as PushStream<Ctx>>::BatchID
        >
        + PushStreamReportError<
            <<Self::Stream as PushStreamPrivate<Ctx>>::StartBatchError as BatchError>::Permanent
        >
        + PushStreamReportBatchError<
            <<Self::Stream as PushStreamAdd<Msg, Ctx>>::AddError as BatchError>::Permanent,
            <Self::Stream as PushStream<Ctx>>::BatchID
        >
        + PushStreamPrivateSingle<Msg, Ctx>
        + PushStreamPrivate<Ctx>
        + Send;
    /// Type of outbound message structures.
    ///
    /// This will be used by the created [PushStreamPrivateThread] to
    /// obtain messages to be sent using the
    /// [PushStream](Dispatch::PushStream) instance.
    type Msgs: 'static + PrivateMsgs<Msg> + Send;
    /// Type of authenticated message receivers.
    ///
    /// This will be used to deliver incoming messages.
    type Recv: 'static + AuthNMsgRecv<Prin, Msg> + Send;
    /// Type of errors that can occur during dispatch.
    type DispatchError: Display;

    /// Obtain the components of a new private session.
    fn dispatch(
        &mut self
    ) -> Result<
        (Self::Stream, Notify, Self::Msgs, Self::Recv),
        Self::DispatchError
    >;
}
struct DispatchEntryInner<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash {
    msg: PhantomData<Msg>,
    shutdown: ShutdownFlag,
    authn: AuthN,
    recv: Recv,
    streams: Arc<Mutex<HashMap<Addr, RecvThreadEntry<Wrapper, Stream>>>>
}

struct DispatchEntry<Msg, Wrapper, Addr, Stream, AuthN, Recv, Reporter>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Reporter: StreamReporter,
    Addr: Clone + Eq + Hash {
    inner: Arc<DispatchEntryInner<Msg, Wrapper, Addr, Stream, AuthN, Recv>>,
    reporter: Reporter,
    push_thread: JoinHandle<()>
}

pub struct DispatchDropHandle<
    Msg,
    Wrapper,
    AuthN,
    Dispatcher,
    Listener,
    Reporter,
    Ctx
> where
    Msg: 'static + Clone + Send,
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    Dispatcher: Dispatch<Msg, AuthN::Prin, Ctx>,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Reporter: StreamReporter<
        Stream = ThreadedStream<Listener::Stream>,
        Prin = Listener::Prin,
        Src = Listener::Addr
    >,
    Ctx: Clone {
    prin: Listener::Prin,
    recvs: Arc<
        Mutex<
            HashMap<
                Listener::Prin,
                DispatchEntry<
                    Msg,
                    Wrapper,
                    Listener::Addr,
                    Listener::Stream,
                    AuthN,
                    Dispatcher::Recv,
                    Reporter
                >
            >
        >
    >
}

pub struct PullStreamsDispatchThread<
    Msg,
    Wrapper,
    AuthN,
    Dispatcher,
    Listener,
    Ctx
> where
    Msg: 'static + Clone + Send,
    Wrapper: 'static + Send,
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: 'static + ConcurrentStream + Credentials,
    Listener::Addr: 'static + Send,
    Dispatcher: Dispatch<Msg, AuthN::Prin, Ctx>,
    Dispatcher::Stream: PushStreamReporter<
        DispatchEntryReporter<
            Msg,
            Wrapper,
            Listener::Addr,
            Listener::Stream,
            AuthN,
            Dispatcher::Recv
        >
    >,
    Dispatcher::Recv: Clone,
    AuthN: 'static + Clone + MsgAuthN<Msg, Wrapper> + Send,
    AuthN::SessionPrin: Send,
    Ctx: Clone {
    msg: PhantomData<Msg>,
    dispatcher: Dispatcher,
    listener: Listener,
    shutdown: ShutdownFlag,
    recvs: Arc<
        Mutex<
            HashMap<
                Listener::Prin,
                DispatchEntry<
                    Msg,
                    Wrapper,
                    Listener::Addr,
                    Listener::Stream,
                    AuthN,
                    Dispatcher::Recv,
                    <Dispatcher::Stream as PushStreamReporter<
                        DispatchEntryReporter<
                            Msg,
                            Wrapper,
                            Listener::Addr,
                            Listener::Stream,
                            AuthN,
                            Dispatcher::Recv
                        >
                    >>::Reporter
                >
            >
        >
    >,
    authn: AuthN,
    ctx: Ctx
}

/// [StreamReporter] instance derived from an entry in a
/// [PullStreamsDispatchThread].
///
/// This is typically created to serve much the same purpose as a
/// [PullStreamsReporter], but for a single principal.
pub struct DispatchEntryReporter<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash {
    inner: Arc<DispatchEntryInner<Msg, Wrapper, Addr, Stream, AuthN, Recv>>
}

unsafe impl<Msg, Wrapper, Addr, Stream, AuthN, Recv> Sync
    for DispatchEntryInner<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
}

impl<Msg, Wrapper, Addr, Stream, AuthN, Recv> Clone
    for DispatchEntryReporter<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Stream: ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Recv: AuthNMsgRecv<AuthN::Prin, Msg>,
    Addr: Clone + Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        DispatchEntryReporter {
            inner: self.inner.clone()
        }
    }
}

impl<Msg, Wrapper, AuthN, Dispatcher, Listener, Reporter, Ctx> Drop
    for DispatchDropHandle<
        Msg,
        Wrapper,
        AuthN,
        Dispatcher,
        Listener,
        Reporter,
        Ctx
    >
where
    Msg: 'static + Clone + Send,
    Listener: PullStreamListener<Wrapper>,
    Listener::Stream: ConcurrentStream + Credentials,
    Dispatcher: Dispatch<Msg, AuthN::Prin, Ctx>,
    AuthN: Clone + MsgAuthN<Msg, Wrapper> + Send,
    Reporter: StreamReporter<
        Stream = ThreadedStream<Listener::Stream>,
        Prin = Listener::Prin,
        Src = Listener::Addr
    >,
    Ctx: Clone
{
    fn drop(&mut self) {
        trace!(target: "dispatch-drop-handle",
               "deleting dispatch entry for principal {}",
               self.prin);

        if let Ok(mut guard) = self.recvs.lock() {
            if let Some(ent) = guard.remove(&self.prin) {
                debug!(target: "dispatch-drop-handle",
                       "shutting down push thread for principal {}",
                       self.prin);

                ent.inner.shutdown.clone().set();

                if ent.push_thread.join().is_err() {
                    error!(target: "pull-streams-dispatch-entry",
                           "failed to join push thread")
                }
            } else {
                debug!(target: "dispatch-drop-handle",
                       "dispatch entry was not present for principal {}",
                       self.prin);
            }
        } else {
            error!(target: "dispatch-drop-handle",
                   "mutex poisoned")
        }
    }
}

impl<Msg, Wrapper, Addr, Stream, AuthN, Recv> StreamReporter
    for DispatchEntryReporter<Msg, Wrapper, Addr, Stream, AuthN, Recv>
where
    Msg: 'static + Send,
    Wrapper: 'static + Send,
    Stream:
        'static + ConcurrentStream + Credentials + PullStream<Wrapper> + Send,
    AuthN: 'static + Clone + MsgAuthN<Msg, Wrapper> + Send,
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

impl<Msg, Wrapper, AuthN, Dispatcher, Listener, Ctx>
    PullStreamsDispatchThread<Msg, Wrapper, AuthN, Dispatcher, Listener, Ctx>
where
    Msg: 'static + Clone + Send,
    Wrapper: 'static + Send,
    Listener: 'static + PullStreamListener<Wrapper> + Send,
    Listener::Stream: ConcurrentStream + Credentials,
    Listener::Addr: Send,
    Listener::Prin: Clone + Eq + Hash + Send,
    Dispatcher: 'static + Dispatch<Msg, AuthN::Prin, Ctx> + Send,
    Dispatcher::Stream: PushStreamReporter<
        DispatchEntryReporter<
            Msg,
            Wrapper,
            Listener::Addr,
            Listener::Stream,
            AuthN,
            Dispatcher::Recv
        >
    >,
    <Dispatcher::Stream as PushStreamReporter<
        DispatchEntryReporter<
            Msg,
            Wrapper,
            Listener::Addr,
            Listener::Stream,
            AuthN,
            Dispatcher::Recv
        >
    >>::Reporter: StreamReporter<
            Stream = ThreadedStream<Listener::Stream>,
            Prin = Listener::Prin,
            Src = Listener::Addr
        > + Send,
    <Dispatcher::Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches:
        Send,
    <Dispatcher::Stream as PushStreamPrivate<Ctx>>::StartBatchRetry: Send,
    <Dispatcher::Stream as PushStreamPrivate<Ctx>>::AbortBatchRetry: Send,
    <Dispatcher::Stream as PushStreamAdd<Msg, Ctx>>::AddRetry: Send,
    <Dispatcher::Stream as PushStream<Ctx>>::FinishBatchRetry: Send,
    <Dispatcher::Stream as PushStream<Ctx>>::CancelBatchRetry: Send,
    <Dispatcher::Stream as PushStream<Ctx>>::StreamFlags: Send,
    <Dispatcher::Stream as PushStream<Ctx>>::BatchID: Send,
    Dispatcher::Recv: Clone,
    AuthN: 'static + Clone + MsgAuthN<Msg, Wrapper> + Send,
    AuthN::SessionPrin: Send,
    Ctx: 'static + Clone + Send + Sync
{
    fn report(
        ent: &mut DispatchEntry<
            Msg,
            Wrapper,
            Listener::Addr,
            Listener::Stream,
            AuthN,
            Dispatcher::Recv,
            <Dispatcher::Stream as PushStreamReporter<
                DispatchEntryReporter<
                    Msg,
                    Wrapper,
                    Listener::Addr,
                    Listener::Stream,
                    AuthN,
                    Dispatcher::Recv
                >
            >>::Reporter
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
                let (push_stream, notify, msgs, recv) = self
                    .dispatcher
                    .dispatch()
                    .map_err(|err| WithMutexPoison::Inner { error: err })?;
                let shutdown = ShutdownFlag::new();
                // XXX need a size hint here.
                let streams = HashMap::new();
                let streams = Arc::new(Mutex::new(streams));
                let inner = DispatchEntryInner {
                    msg: PhantomData,
                    shutdown: shutdown.clone(),
                    authn: self.authn.clone(),
                    recv: recv,
                    streams: streams
                };
                let inner = Arc::new(inner);
                let reporter = DispatchEntryReporter {
                    inner: inner.clone()
                };
                let reporter = push_stream.reporter(reporter);
                // XXX need a size hint here.
                let push_thread = PushStreamPrivateThread::create(
                    self.ctx.clone(),
                    msgs,
                    notify,
                    push_stream,
                    shutdown
                );
                let join = push_thread.start();
                let ent = ent.insert(DispatchEntry {
                    inner: inner,
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
