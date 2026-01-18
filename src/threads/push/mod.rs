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

//! Manager threads for various kinds of push streams.

use std::fmt::Display;
use std::io::Error;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjPushRetry;
use crate::stream::LargeObjOfferStream;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamReportError;

pub mod private;
pub mod shared;


pub trait PushMode<Stream, Msgs, Ctx>: PushModeCreate {
    type SendError: Display + ScopedError;
    type RetryError: Display + ScopedError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::SendError>;

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError>;
}


pub struct PushStreamThread<Msgs, Stream, Mode, Ctx>
where
    Mode: PushMode<Stream, Msgs, Ctx> {
    ctx: Ctx,
    mode: Mode,
    /// Source of outbound messages.
    msgs: Msgs,
    /// `Notify` instance used to indicate that new messages are
    /// available to be sent.
    notify: Notify,
    /// Flag to use to shut the stream down.
    shutdown: ShutdownFlag,
    /// Stream to use to send.
    stream: Stream
}


impl<Msgs, Stream, Mode, Ctx> PushStreamThread<Msgs, Stream, Mode, Ctx>
where
    Mode: 'static + PushMode<Stream, Msgs, Ctx>,
    Stream: 'static + Send,
    Mode: Send,
    Msgs: 'static + Send,
    Ctx: 'static + Send
{
    pub fn create(
        mode: Mode::Config,
        ctx: Ctx,
        msgs: Msgs,
        notify: Notify,
        stream: Stream,
        shutdown: ShutdownFlag
    ) -> Self {
        let mode = Mode::create(mode);

        PushStreamThread {
            mode: mode,
            msgs: msgs,
            notify: notify,
            shutdown: shutdown,
            stream: stream,
            ctx: ctx
        }
    }

    /// Get the `Notify` used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Notify {
        self.notify.clone()
    }

    fn run(mut self) {
        match self.notify.register() {
            Ok(idx) => {
                let mut next_outbound = Some(Instant::now());
                let mut next_pending = None;
                let mut valid = true;

                info!(target: "push-stream-thread",
                      "push stream send thread starting");

                // Loop until told to shut down.
                while valid && self.shutdown.is_live() {
                    let now = Instant::now();

                    if let Some(when) = next_outbound &&
                        when <= now
                    {
                        match self.mode.send_from_outbound(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream
                        ) {
                            Ok(next) => next_outbound = next,
                            Err(err) => {
                                error!(target: "push-stream-thread",
                                       "error obtaining messages: {}",
                                       err);

                                if err.scope() >= ErrorScope::Shutdown {
                                    valid = false
                                }
                            }
                        }
                    }

                    if let Some(next) = next_pending &&
                        next <= now
                    {
                        match self.mode.retry_pending(
                            &mut self.ctx,
                            &mut self.msgs,
                            &mut self.stream,
                            now
                        ) {
                            Ok(next) => {
                                next_pending = next;
                            }
                            Err(err) => {
                                error!(target: "push-stream-thread",
                                       "error retrying pending: {}",
                                       err);
                            }
                        }
                    }

                    match next_pending.map_or(next_outbound, |next| {
                        next_outbound.map(|when| when.max(next))
                    }) {
                        Some(when) => {
                            let now = Instant::now();

                            if now < when {
                                let duration = when - now;

                                trace!(target: "push-stream-thread",
                                       "waiting, next activity in {}.{:03}s",
                                       duration.as_secs(),
                                       duration.subsec_millis());

                                match self.notify.wait_timeout(&idx, duration) {
                                    Ok(notify) => {
                                        if notify {
                                            next_outbound = Some(now)
                                        }
                                    }
                                    Err(err) => {
                                        error!(target: "push-stream-thread",
                                               "error waiting: {}",
                                               err);

                                        valid = false
                                    }
                                }
                            }
                        }
                        None => {
                            trace!(target: "push-stream-thread",
                                   "waiting for notification indefinitely");

                            match self.notify.wait(&idx) {
                                Ok(_) => next_outbound = Some(now),
                                Err(err) => {
                                    error!(target: "push-stream-thread",
                                           "error waiting for notification: {}",
                                           err);

                                    valid = false
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!(target: "push-stream-thread",
                       "error registering to notify: {}",
                       err);
            }
        }

        debug!(target: "push-stream-thread",
               "push stream send thread exiting");
    }

    pub fn start(self) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("push-stream-thread"))
            .spawn(move || self.run())
    }
}

impl<Msgs, Stream, Mode, Ctx> PushStreamThread<Msgs, Stream, Mode, Ctx>
where
    Mode: 'static + PushMode<Stream, Msgs, Ctx>,
    Stream: 'static + PushStreamParties + Send,
    Mode: Send,
    Msgs: 'static + Send,
    Ctx: 'static + Send
{
    #[inline]
    pub fn parties(&self) -> Result<Stream::PartiesIter, Stream::PartiesError> {
        self.stream.parties()
    }
}
