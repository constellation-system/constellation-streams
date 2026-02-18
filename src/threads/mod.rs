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
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::fmt::Display;
use std::time::Instant;

use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryWhen;
use log::error;
use mio::Registry;
use mio::Token;

use crate::large_obj::FragsOrOffer;
use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::large_obj::LargeObjPushError;
use crate::large_obj::LargeObjPushRetry;
use crate::stream::LargeObjOfferStream;
use crate::stream::Parties;
use crate::stream::PushStreamReportError;

pub mod dispatch;
pub mod poll;
pub mod private;
pub mod shared;

pub trait PushMode<Stream, Msgs, Ctx>: Sized {
    /// Type of configuration objects used in [create](PushMode::create).
    type Config;
    /// Type of errors that can happen in [create](PushMode::create).
    type CreateError: Debug + Display + ScopedError;
    type SendError: Debug + Display + ScopedError;
    type RetryError: Debug + Display + ScopedError;
    type RetryIndefError: Debug + Display + ScopedError;

    /// Create an instance from a stream and a configuration object.
    ///
    /// # Parameters
    ///
    /// - `stream`: Reference to the [PushStream] that will be used to
    ///   send messages.  This allows the `PushMode` to gather
    ///   information from the stream.
    ///
    /// - `config`: The configuratiot object.
    fn create(
        stream: &Stream,
        config: Self::Config,
    ) -> Result<Self, Self::CreateError>;

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

    fn complete_pending(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>,
    ) -> Result<Option<Instant>, Self::RetryError>;

    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<Option<Instant>, Self::RetryIndefError>;
}

pub trait RegistryCtx {
    fn registry(&self) -> &Registry;
}

pub trait TokensCtx {
    /// Allocate a [Token].
    fn token(&mut self) -> Token;

    /// Release a [Token] from use.
    ///
    /// # Parameters
    ///
    /// - `token`: Token to release.
    fn free_token(
        &mut self,
        token: Token
    );
}

pub struct Tokens {
    /// Current count for generating new tokens.
    curr: usize,
    /// Binary heap of freed tokens.
    freed: BinaryHeap<Token>
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

impl Tokens {
    #[inline]
    pub fn new() -> Tokens {
        Tokens {
            freed: BinaryHeap::new(),
            curr: 0,
        }
    }

    #[inline]
    pub fn with_capacity(hint: usize) -> Tokens {
        Tokens {
            freed: BinaryHeap::with_capacity(hint),
            curr: 0,
        }
    }
}

impl TokensCtx for Tokens {
    fn token(&mut self) -> Token {
        self.freed.pop().unwrap_or_else(|| {
            let out = self.curr;

            // Clear out any tokens that can be merged back into curr.
            while self.freed.peek()
                .map_or(false, |head| head.0 + 1 == self.curr) {
                // Smoke-check
                match self.freed.pop() {
                    Some(head) => if head.0 + 1 != self.curr {
                        error!(target: "tokens",
                               "wrong result for pop from freed")
                    }
                    None => {
                        error!(target: "tokens",
                               "pop from freed should not be None")
                    }
                }
            }

            self.curr += 1;

            Token(out)
        })
    }

    #[inline]
    fn free_token(
        &mut self,
        token: Token
    ) {
        if token.0 + 1 == self.curr {
            self.curr -= 1;
        } else {
            self.freed.push(token)
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
        RetryIndefResult<(Option<Instant>, Stream::Parties),
                         Self,
                         Parties<Stream::Parties>>,
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

    pub(crate) fn complete_send<InMsg, OutMsg, PartyID, Types>(
        ctx: &mut Ctx,
        stream: &mut Stream,
        proto: &mut LargeObjProto<InMsg, OutMsg, PartyID, Stream::Frags, Types>,
        err: FragsOrOffer<
            H::HashID,
            <Stream::PushFragError as RecoverableError>::Completable,
            <Stream::PushOfferError as RecoverableError>::Completable
        >
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Option<Stream::Parties>),
                         Self,
                         Parties<Stream::Parties>>,
        LargeObjPushError<
            H::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Types: LargeObjProtoTypes<InMsg, OutMsg, Hash = H, HashID = H::HashID>,
        PartyID: Clone {
        match err {
            FragsOrOffer::Frags { err, id } => Ok(proto
               .complete_push_frags(ctx, stream, id.clone(), err)?
               .map(|(when, parties)| (when, Some(parties)))
               .map_retry(|retry| LargeObjEntry::PushFrags {
                   retry: retry,
                   id: id
               })),
            FragsOrOffer::Offer { err, hash } => Ok(proto
               .complete_push_offer(ctx, stream, hash.clone(), err)?
               .map(|(when, parties)| (when, Some(parties)))
               .map_retry(|retry| LargeObjEntry::PushOffer {
                   retry: retry,
                   hash: hash
               }))
        }
    }

    pub(crate) fn try_send<InMsg, OutMsg, PartyID, Types>(
        ctx: &mut Ctx,
        stream: &mut Stream,
        proto: &mut LargeObjProto<InMsg, OutMsg, PartyID, Stream::Frags, Types>
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Option<Stream::Parties>),
                         Self,
                         Parties<Stream::Parties>>,
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

#[test]
fn test_tokens_alloc_free_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let expected = tokens.token();

    tokens.free_token(expected.clone());

    let actual = tokens.token();

    assert_eq!(expected, actual)
}

#[test]
fn test_tokens_unique() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();

    assert_ne!(first, second)
}

#[test]
fn test_tokens_alloc_free_first_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();

    tokens.free_token(first);

    let third = tokens.token();

    assert_ne!(second, third)
}

#[test]
fn test_tokens_alloc_free_second_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let _ = tokens.token();
    let second = tokens.token();

    tokens.free_token(second.clone());

    let third = tokens.token();

    assert_eq!(second, third)
}

#[test]
fn test_tokens_alloc_free_all_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();

    tokens.free_token(first.clone());
    tokens.free_token(second);
    tokens.free_token(third);

    let fourth = tokens.token();

    assert_eq!(first, fourth)
}

#[test]
fn test_tokens_alloc_free_all_rev_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();

    tokens.free_token(third);
    tokens.free_token(second);
    tokens.free_token(first.clone());

    let fourth = tokens.token();

    assert_eq!(first, fourth)
}

#[test]
fn test_tokens_alloc_free_gap_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let _ = tokens.token();
    let fourth = tokens.token();

    tokens.free_token(first);
    tokens.free_token(second);
    tokens.free_token(fourth.clone());

    let fifth = tokens.token();

    let fourth = tokens.token();

    assert_eq!(fifth, fourth)
}

#[test]
fn test_tokens_alloc_free_close_gap_alloc() {
    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();
    let fourth = tokens.token();

    tokens.free_token(first.clone());
    tokens.free_token(second);
    tokens.free_token(fourth);
    tokens.free_token(third);

    let fifth = tokens.token();

    assert_eq!(fifth, first)
}
