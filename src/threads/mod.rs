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

//! Manager threads for various kinds of push and pull streams.

use std::collections::HashSet;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::cred::Credentials;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use mio::Registry;
use mio::Token;

use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjPushRetry;
use crate::stream::ConcurrentStream;
use crate::stream::PullStream;
use crate::stream::LargeObjOfferStream;
use crate::stream::PushStreamReportError;
use crate::stream::ThreadedStream;


//pub mod dispatch;
pub mod poll;
pub mod private;
pub mod shared;

pub trait PushMode<Stream, Msgs, Ctx> {
    type SendError: Display + ScopedError;
    type RetryError: Display + ScopedError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>
    ) -> Result<Option<Instant>, Self::SendError>;

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>,
        now: Instant,
    ) -> Result<Option<Instant>, Self::RetryError>;
}

pub trait RegistryCtx {
    fn registry(&self) -> &Registry;
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
    Recv: AuthNMsgRecv<AuthN::Prin, Msg, AuthN::AuthNMsg> {
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
        + PushStreamReportError<
            <Stream::PushFragError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Stream::PushOfferError as RecoverableError>::Permanent
        >,
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
            <Stream::PushFragError as RecoverableError>::Permanent,
            <Stream::PushOfferError as RecoverableError>::Permanent
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
            <Stream::PushFragError as RecoverableError>::Permanent,
            <Stream::PushOfferError as RecoverableError>::Permanent
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
