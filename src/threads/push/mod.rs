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

//! Manager threads for various kinds of push streams.

use std::fmt::Display;
use std::io::Error;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::ErrorScope;
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

use crate::error::BatchError;
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

pub trait PushModeCreate {
    type Config: Clone;

    fn create(config: Self::Config) -> Self;
}

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

pub(crate) enum LargeObjEntry<Stream, H, Ctx>
where
    Stream: LargeObjOfferStream<H::HashID, Ctx>,
    H: Clone + HashAlgo {
    PushFrags {
        id: LargeObjID,
        retry: Stream::PushFragRetry
    },
    PushOffer {
        hash: H::HashID,
        retry: Stream::PushOfferRetry
    }
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

impl<Stream, H, Ctx> RetryWhen for LargeObjEntry<Stream, H, Ctx>
where
    Stream: LargeObjOfferStream<H::HashID, Ctx>,
    H: Clone + HashAlgo
{
    fn when(&self) -> Instant {
        match self {
            LargeObjEntry::PushFrags { retry, .. } => retry.when(),
            LargeObjEntry::PushOffer { retry, .. } => retry.when()
        }
    }
}

impl<Stream, H, Ctx> LargeObjEntry<Stream, H, Ctx>
where
    Stream: LargeObjOfferStream<H::HashID, Ctx>
        + PushStreamReportError<<Stream::PushFragError as BatchError>::Permanent>
        + PushStreamReportError<<Stream::PushOfferError as BatchError>::Permanent>,
    H: Clone + HashAlgo,
    H::HashID: Clone
{
    pub(crate) fn exec<InMsg, OutMsg, PartyID, Types>(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        proto: &mut LargeObjProto<InMsg, OutMsg, PartyID, Stream::Frags, Types>
    ) -> Result<
        RetryResult<Option<Instant>, Self>,
        LargeObjPushError<
            H::HashID,
            <Stream::PushFragError as BatchError>::Permanent,
            <Stream::PushOfferError as BatchError>::Permanent
        >
    >
    where
        Types: LargeObjProtoTypes<InMsg, OutMsg, Hash = H, HashID = H::HashID>,
        PartyID: Clone {
        match self {
            LargeObjEntry::PushFrags { id, retry } => proto
                .retry_push_frags(ctx, stream, id.clone(), retry)
                .map(|out| {
                    out.map_retry(|retry| LargeObjEntry::PushFrags {
                        retry: retry,
                        id: id.clone()
                    })
                }),
            LargeObjEntry::PushOffer { hash, retry } => proto
                .retry_push_offer(ctx, stream, hash.clone(), retry)
                .map(|out| {
                    out.map_retry(|retry| LargeObjEntry::PushOffer {
                        retry: retry,
                        hash: hash.clone()
                    })
                })
        }
    }

    pub(crate) fn from_try_send<InMsg, OutMsg, PartyID, Types>(
        ctx: &mut Ctx,
        stream: &mut Stream,
        proto: &mut LargeObjProto<InMsg, OutMsg, PartyID, Stream::Frags, Types>
    ) -> Result<
        RetryResult<Option<Instant>, Self>,
        LargeObjPushError<
            H::HashID,
            <Stream::PushFragError as BatchError>::Permanent,
            <Stream::PushOfferError as BatchError>::Permanent
        >
    >
    where
        Types: LargeObjProtoTypes<InMsg, OutMsg, Hash = H, HashID = H::HashID>,
        PartyID: Clone {
        Ok(proto
            .try_push(ctx, stream)?
            .flat_map_retry(|retry| match retry {
                LargeObjPushRetry::Frags { retry, id } => {
                    RetryResult::Retry(LargeObjEntry::PushFrags {
                        retry: retry,
                        id: id
                    })
                }
                LargeObjPushRetry::Offer { retry, hash } => {
                    RetryResult::Retry(LargeObjEntry::PushOffer {
                        retry: retry,
                        hash: hash
                    })
                }
                LargeObjPushRetry::Retry { when } => {
                    RetryResult::Success(Some(when))
                }
            }))
    }
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
