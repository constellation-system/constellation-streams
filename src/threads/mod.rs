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
use std::fmt::Debug;
use std::fmt::Display;
use std::time::Instant;

use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryWhen;
use mio::Registry;
use mio::Token;

use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjPushRetry;
use crate::stream::LargeObjOfferStream;
use crate::stream::PushStreamReportError;

//pub mod dispatch;
pub mod poll;
pub mod private;
pub mod shared;

pub trait PushMode<Stream, Msgs, Ctx> {
    type SendError: Debug + Display + ScopedError;
    type RetryError: Debug + Display + ScopedError;
    type RetryIndefError: Debug + Display + ScopedError;

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

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::RetryIndefError>;
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
        RetryIndefResult<(Option<Instant>, Stream::Parties), Self>,
        LargeObjPushError<
            H::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
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
        RetryIndefResult<(Option<Instant>, Option<Stream::Parties>), Self>,
        LargeObjPushError<
            H::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Types: LargeObjProtoTypes<InMsg, OutMsg, Hash = H, HashID = H::HashID>,
        PartyID: Clone {
        Ok(proto
           .try_push(ctx, stream)?
           .map(|(when, parties)| (when, Some(parties)))
           .flat_map_retry(|retry| match retry {
               LargeObjPushRetry::Frags { retry, id } => {
                   RetryIndefResult::Retry(LargeObjEntry::PushFrags {
                       retry: retry,
                       id: id
                   })
               }
               LargeObjPushRetry::Offer { retry, hash } => {
                   RetryIndefResult::Retry(LargeObjEntry::PushOffer {
                       retry: retry,
                       hash: hash
                   })
               }
               LargeObjPushRetry::Retry { when } => {
                   RetryIndefResult::Success((Some(when), None))
               }
           }))
    }
}
