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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::time::Instant;

use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
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
pub mod test;

pub trait PushMode<Stream, Msgs, Ctx>: Sized {
    type SendError: Debug + Display + ScopedError;
    type RetryError: Debug + Display + ScopedError;
    type RetryIndefError: Debug + Display + ScopedError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError>;

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>,
        now: Instant
    ) -> Result<PushModeResult, Self::RetryError>;

    fn complete_pending(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
        live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::RetryError>;

    /// Retry all stored indefinite retries.
    ///
    /// # Parameters
    ///
    /// - `ctx`: The context to use.
    ///
    /// - `msgs`: The outbound message buffer to use to get messages.
    ///
    /// - `stream`: The stream to use to send.
    ///
    /// # Return Value
    ///
    /// If retries are generated, the time at which to call
    /// [retry_pending](PushMode::retry_pending), or `None`.
    fn retry_indefs(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream
    ) -> Result<PushModeResult, Self::RetryIndefError>;
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

pub(crate) struct RetryHeapEntry<I, T>
where
    T: RetryWhen {
    when: T,
    id: I
}

/// Result from [PushMode] operations.
///
/// This records times for the next operations, and whether there are
/// any operations that need to be completed.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct PushModeResult {
    next_outbound: Option<Instant>,
    next_retry: Option<Instant>,
    has_completes: bool
}

pub struct Tokens {
    /// Current count for generating new tokens.
    curr: usize,
    /// Binary heap of freed tokens.
    freed: BinaryHeap<Token>
}

pub struct WithTokens<T> {
    tokens: Tokens,
    inner: T
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

impl<I, T> PartialEq for RetryHeapEntry<I, T>
where
    T: RetryWhen
{
    #[inline]
    fn eq(
        &self,
        other: &Self
    ) -> bool {
        self.when().eq(&other.when())
    }
}

impl<I, T> Eq for RetryHeapEntry<I, T> where T: RetryWhen {}

impl<I, T> Ord for RetryHeapEntry<I, T>
where
    T: RetryWhen
{
    #[inline]
    fn cmp(
        &self,
        other: &Self
    ) -> Ordering {
        self.when().cmp(&other.when())
    }
}

impl<I, T> PartialOrd for RetryHeapEntry<I, T>
where
    T: RetryWhen
{
    #[inline]
    fn partial_cmp(
        &self,
        other: &Self
    ) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I, T> RetryWhen for RetryHeapEntry<I, T>
where
    T: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        self.when.when()
    }
}

impl<I, T> RetryHeapEntry<I, T>
where
    T: RetryWhen
{
    #[inline]
    pub(crate) fn new(
        id: I,
        when: T
    ) -> Self {
        RetryHeapEntry { when: when, id: id }
    }

    #[inline]
    pub(crate) fn take(self) -> (I, T) {
        (self.id, self.when)
    }
}

impl Default for PushModeResult {
    #[inline]
    fn default() -> Self {
        PushModeResult {
            next_outbound: None,
            next_retry: None,
            has_completes: false
        }
    }
}

impl PushModeResult {
    #[inline]
    pub(crate) fn new(
        next_outbound: Option<Instant>,
        next_retry: Option<Instant>,
        has_completes: bool
    ) -> Self {
        PushModeResult {
            next_outbound: next_outbound,
            next_retry: next_retry,
            has_completes: has_completes
        }
    }

    #[inline]
    pub(crate) fn from_next_retry(next_retry: Instant) -> Self {
        PushModeResult {
            next_outbound: None,
            next_retry: Some(next_retry),
            has_completes: false
        }
    }

    /// Get the next time to send outbound messages.
    ///
    /// This indicates when next to call
    /// (send_from_outbound)[PushMode::send_from_outbound].
    #[inline]
    pub fn next_outbound(&self) -> Option<Instant> {
        self.next_outbound
    }

    /// Get the next time to retry sending messages.
    ///
    /// This indicates when next to call
    /// (retry_pending)[PushMode::retry_pending].
    #[inline]
    pub fn retry_pending(&self) -> Option<Instant> {
        self.next_retry
    }

    /// Indicate whether there are sends that need to be completed.
    ///
    /// This indicates whether
    /// (complete_pending)[PushMode::complete_pending] needs to be
    /// called after the next wait.
    #[inline]
    pub fn has_completes(&self) -> bool {
        self.has_completes
    }

    #[inline]
    pub fn take_next_outbound(&mut self) -> Option<Instant> {
        self.next_outbound.take()
    }

    #[inline]
    pub fn take_retry_pending(&mut self) -> Option<Instant> {
        self.next_retry.take()
    }

    #[inline]
    pub fn take_has_completes(&mut self) -> bool {
        let out = self.has_completes;

        self.has_completes = false;

        out
    }

    #[inline]
    pub fn merge_next_outbound(
        &mut self,
        next: &Option<Instant>
    ) {
        self.next_outbound = next_retry(&self.next_outbound, next)
    }

    #[inline]
    pub fn merge_next_outbound_definite(
        &mut self,
        next: &Instant
    ) {
        self.next_outbound =
            Some(next_retry_definite(&self.next_outbound, next))
    }

    #[inline]
    pub fn merge_next_retry(
        &mut self,
        next: &Option<Instant>
    ) {
        self.next_retry = next_retry(&self.next_retry, next)
    }

    #[inline]
    pub fn merge_next_retry_definite(
        &mut self,
        next: &Instant
    ) {
        self.next_retry = Some(next_retry_definite(&self.next_retry, next))
    }

    #[inline]
    pub fn merge_has_completes(
        &mut self,
        has_completes: bool
    ) {
        self.has_completes |= has_completes
    }

    #[inline]
    pub fn set_has_completes(&mut self) {
        self.has_completes = true
    }

    #[inline]
    pub fn merge(
        &mut self,
        other: &Self
    ) {
        self.merge_next_outbound(&other.next_outbound);
        self.merge_next_retry(&other.next_retry);
        self.merge_has_completes(other.has_completes);
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

impl<T> WithTokens<T> {
    #[inline]
    pub fn new(inner: T) -> Self {
        WithTokens {
            tokens: Tokens::default(),
            inner: inner
        }
    }

    #[inline]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> TokensCtx for WithTokens<T> {
    #[inline]
    fn token(&mut self) -> Token {
        self.tokens.token()
    }

    #[inline]
    fn free_token(
        &mut self,
        token: Token
    ) {
        self.tokens.free_token(token)
    }
}

impl Default for Tokens {
    #[inline]
    fn default() -> Tokens {
        Tokens {
            freed: BinaryHeap::new(),
            curr: 0
        }
    }
}

impl Tokens {
    #[inline]
    pub fn new() -> Tokens {
        Tokens::default()
    }

    #[inline]
    pub fn with_capacity(hint: usize) -> Tokens {
        Tokens {
            freed: BinaryHeap::with_capacity(hint),
            curr: 0
        }
    }
}

impl TokensCtx for Tokens {
    fn token(&mut self) -> Token {
        if let Some(out) = self.freed.pop() {
            out
        } else {
            let out = self.curr;

            self.curr += 1;

            Token(out)
        }
    }

    #[inline]
    fn free_token(
        &mut self,
        token: Token
    ) {
        if token.0 + 1 == self.curr {
            self.curr -= 1;

            // Clear out any tokens that can be merged back into curr.
            while self
                .freed
                .peek()
                .is_some_and(|head| head.0 + 1 == self.curr)
            {
                self.curr -= 1;

                // Smoke-check
                match self.freed.pop() {
                    Some(head) => {
                        if head.0 + 1 != self.curr {
                            error!(target: "tokens",
                               "wrong result for pop from freed")
                        }
                    }
                    None => {
                        error!(target: "tokens",
                               "pop from freed should not be None")
                    }
                }
            }
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
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            Self,
            Parties<Stream::Parties>
        >,
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
        RetryIndefResult<
            (Option<Instant>, Option<Stream::Parties>),
            Self,
            Parties<Stream::Parties>
        >,
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
        RetryIndefResult<
            (Option<Instant>, Option<Stream::Parties>),
            Self,
            Parties<Stream::Parties>
        >,
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
            .try_push(ctx, stream, Instant::now())?
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
