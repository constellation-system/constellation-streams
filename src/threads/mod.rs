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

//! Manager threads for various kinds of push and pull streams.

use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
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
use crate::stream::ThreadedStream;

pub mod dispatch;
pub mod pull;
pub mod push;

pub(crate) struct RecvThreadEntry<Msg, Stream>
where
    Stream: ConcurrentStream + Credentials + PullStream<Msg> + Send {
    msg: PhantomData<Msg>,
    join: JoinHandle<()>,
    stream: ThreadedStream<Stream>
}

pub(crate) struct RecvThread<Msg, Wrapper, Addr, Stream, AuthN, Recv>
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

#[derive(Debug)]
enum RecvSendError<AuthN> {
    AuthN { err: AuthN },
    Shutdown
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
            Ok(AuthNResult::Accept(msg)) => {
                let (prin, msg) = msg.take();

                self.recv
                    .recv_auth_msg(&prin, msg)
                    .map_err(|_| RecvSendError::Shutdown)
            }
            Ok(AuthNResult::Reject(_)) => {
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
