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

//! Large-object transfer protocol.

use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::iter::once;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_auth::authn::MsgAuthNTypes;
use constellation_common::codec::per::PERCodec;
use constellation_common::codec::DatagramCodec;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::MutexPoison;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::net::PrivateMsgs;
use constellation_common::net::SharedMsgs;
use constellation_common::retry::next_retry;
use constellation_common::retry::next_retry_definite;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::sync::Notify;
use log::debug;
use log::error;
use log::trace;

use crate::config::LargeObjProtoConfig;
use crate::error::ErrorReportInfo;
use crate::frags::Frags;
use crate::frags::InboundFrags;
use crate::frags::InboundRecvError;
use crate::frags::OutboundDataError;
use crate::frags::OutboundFrags;
use crate::generated::large_obj::LargeObjAccept;
use crate::generated::large_obj::LargeObjFinish;
use crate::generated::large_obj::LargeObjFragHeader;
use crate::generated::large_obj::LargeObjFragRef;
use crate::generated::large_obj::LargeObjFragReq;
use crate::generated::large_obj::LargeObjFrags;
use crate::generated::large_obj::LargeObjMetadata;
use crate::generated::large_obj::LargeObjOffer;
use crate::generated::large_obj::LargeObjReq;
use crate::generated::large_obj::LargeObjReqObj;
use crate::stream::LargeObjOfferStream;
use crate::stream::Parties;
use crate::stream::PushStreamReportError;

pub mod test;

pub trait LargeObjMsgs<H, Msg>: Sized
where
    H: HashAlgo + Clone,
    H::HashID: Clone + Display + Hash + HashID + Eq {
    type AddMsgsError<Encode>: Debug + Display + ScopedError
    where
        Encode: Debug + Display + ScopedError;

    /// Use `sender` to add outbound large object messages.
    fn add_msgs<Enc, F>(
        &mut self,
        sender: &mut LargeObjSender<H, Msg, Enc, F>
    ) -> Result<Option<Instant>, Self::AddMsgsError<Enc::EncodeError>>
    where
        Enc: Clone + Create + Encoder<Msg>,
        Enc::Config: Default,
        F: Frags;
}

pub trait LargeObjProtoTypes<InMsg, OutMsg> {
    /// Type of principals assigned to messages.
    type Prin: Debug + Display + Clone;
    /// Type of session principals.
    type SessionPrin: Clone + Debug + Display + Eq + Hash;
    type IDsConfig: Default;
    type IDs: Create<Config = Self::IDsConfig> + Iterator<Item = LargeObjID>;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq;
    /// Hash algorithm to use.
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    /// Type of wrapper messages.
    type Wrapper;
    type DecoderConfig: Default;
    type DecodeError: Debug + Display;
    type Decoder: Create<Config = Self::DecoderConfig>
        + Decoder<Self::Wrapper, DecodeError = Self::DecodeError>;
    type EncoderConfig: Default;
    type EncodeError: Debug + Display + ScopedError;
    type Encoder: Clone
        + Create<Config = Self::EncoderConfig>
        + Encoder<OutMsg, EncodeError = Self::EncodeError>;
    /// Type of message source.
    type Msgs: LargeObjMsgs<Self::Hash, OutMsg>;
    /// Type of message receiver.
    type Recv: AuthNMsgRecv<Self::Prin, InMsg, Self::AuthNMsg> + Clone;
    type AuthNMsg: AuthNed<Self::Prin, InMsg>;
    type AuthNError: Debug + Display;
    type MsgAuthN: MsgAuthN<
        InMsg,
        Self::Wrapper,
        SessionPrin = Self::SessionPrin,
        Prin = Self::Prin,
        AuthNMsg = Self::AuthNMsg,
        Error = Self::AuthNError
    >;
    /// Message authentication types.
    type AuthNTypes: MsgAuthNTypes<
        InMsg,
        Wrapper = Self::Wrapper,
        Prin = Self::Prin,
        SessionPrin = Self::SessionPrin,
        Decoder = Self::Decoder,
        DecoderConfig = Self::DecoderConfig,
        DecodeError = Self::DecodeError,
        AuthNError = Self::AuthNError,
        MsgAuthN = Self::MsgAuthN
    >;
}

const LARGE_OBJ_METADATA_SIZE: usize = 1171;
const LARGE_OBJ_METADATA_BITS: usize = LARGE_OBJ_METADATA_SIZE * 8;

const LARGE_OBJ_FRAG_HEADER_SIZE: usize = 18;
const LARGE_OBJ_FRAG_HEADER_BITS: usize = LARGE_OBJ_FRAG_HEADER_SIZE * 8;

pub type LargeObjFragHeaderPERCodec =
    PERCodec<LargeObjFragHeader, LARGE_OBJ_FRAG_HEADER_BITS>;

pub type LargeObjMetadataPERCodec =
    PERCodec<LargeObjMetadata, LARGE_OBJ_METADATA_BITS>;

/// Type of IDs for large-object transfers.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LargeObjID(u64);

/// Message codec for large-object transfer protocol messages.
#[derive(Clone)]
pub struct LargeObjMsgCodec<H>
where
    H: HashAlgo {
    frag_header: LargeObjFragHeaderPERCodec,
    metadata: LargeObjMetadataPERCodec,
    hash: H
}

/// Representation of a fragment of a large object.
#[derive(Clone, Debug, Hash, PartialEq)]
pub struct LargeObjFrag {
    /// Offset of the fragment into the large object.
    offset: u64,
    /// The data fragment.
    data: Vec<u8>
}

/// Abstracted representaton of large-object protocol messages.
#[derive(Clone, Debug, Hash, PartialEq)]
pub enum LargeObjMsg<H>
where
    H: HashID {
    /// Large object offer message.
    ///
    /// This initiates a large object transfer for an object.  This
    /// doubles as an initial `Frags` message, and delivers one
    /// fragment.  The counterparty should reply with a `ReqObj` to
    /// initiate the transfer.
    ///
    /// If the total object is small enough, then the entire object
    /// can be delivered by the `Offer` message.  If this happens, the
    /// counterparty should reply with an `Accept` message instead.
    Offer {
        /// The hash of the entire encoded large object.
        hash: H,
        /// The size of the entire encoded large object.
        size: u64,
        /// An initial fragment.
        frag: LargeObjFrag
    },
    /// Accept an `Offer` message and complete the transfer.
    ///
    /// This responds to one or more `Offer` messages, accepting the
    /// transfer, but also indicates the transfer to be complete.  In
    /// essence, this message functions as both a `ReqObj` and
    /// `Finished` message.
    Accept {
        /// The hash corresponding to the `Offer` messages to which
        /// this responds.
        hash: H,
        /// The size of the entire encoded large object.
        size: u64,
        /// An ID given to this transfer.
        ///
        /// This ID is valid only between the two counterparties in
        /// this transfer.
        id: LargeObjID
    },
    /// Accept an `Offer` message and initiate a large-object transfer.
    ///
    /// This responds to one or more `Offer` messages, accepting the
    /// transfer and assigning an ID to it.  Following this, the
    /// exchange will consist solely of `Frags`, `Req`, and `Finished`
    /// messages.
    ReqObj {
        /// The hash corresponding to the `Offer` messages to which
        /// this responds.
        hash: H,
        /// The size of the entire encoded large object.
        size: u64,
        /// An ID given to this transfer.
        ///
        /// This ID is valid only between the two counterparties in
        /// this transfer.
        id: LargeObjID
    },
    /// Fragment-transfer message.
    ///
    /// This delivers one or more data fragments in a large-object
    /// transfer.  This message should be sent exclusively by the
    /// senderr in the transfer.
    Frags {
        /// The ID of the transfer.
        id: LargeObjID,
        /// The transferred data fragments.
        frags: Vec<LargeObjFrag>
    },
    /// Fragment-request message.
    ///
    /// This updates the sender on the state of the transfer,
    /// acknowledging delivery of fragments, or requesting delivery of
    /// them.  This is sent periodically by the receiver of the
    /// transfer to indicate which fragments do or do not need to be
    /// sent.
    Req {
        /// The ID of the transfer.
        id: LargeObjID,
        /// The fragment status updates.
        reqs: Vec<LargeObjFragReq>
    },
    /// Transfer complete message.
    ///
    /// This is sent by the receiver of a transfer to indicate that
    /// the transfer is complete.
    Finish {
        /// The hash of the transferred object.
        hash: H,
        /// The ID of the transfer.
        id: LargeObjID
    }
}

#[derive(Debug)]
pub enum LargeObjProtoAddOutboundError<Encode> {
    Encode { err: Encode },
    NoIDs,
    MutexPoison
}

struct ReqState {
    when: Instant,
    nretries: usize
}

enum InboundFragsState {
    Active {
        frags: InboundFrags,
        req: Option<ReqState>
    },
    Finished {
        expire: Option<Instant>,
        size: usize,
        accept: bool,
        send: bool
    }
}

struct RecvEntry<H> {
    frags: InboundFragsState,
    hash: H
}

struct SendEntry<F>
where
    F: Frags {
    id: Option<LargeObjID>,
    when: Option<Instant>,
    frags: F
}

struct LargeObjInbound<H, Prin> {
    objs: HashMap<LargeObjID, RecvEntry<H>>,
    hashes: HashMap<(Prin, H), LargeObjID>
}

struct LargeObjOutbound<H, F>
where
    F: Frags {
    objs: HashMap<H, SendEntry<F>>,
    hashes: HashMap<LargeObjID, H>
}

pub struct LargeObjSender<H, Msg, Enc, F>
where
    Enc: Create + Encoder<Msg>,
    Enc::Config: Default,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Hash + HashID + Eq,
    F: Frags {
    wrapper: PhantomData<Msg>,
    outbound: Arc<Mutex<LargeObjOutbound<H::HashID, F>>>,
    param: Arc<RwLock<F::Param>>,
    encoder: Enc,
    hash: H
}

pub struct LargeObjProto<InMsg, OutMsg, PartyID, F, Types>
where
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    PartyID: Clone,
    F: Frags {
    in_msg: PhantomData<InMsg>,
    out_msg: PhantomData<OutMsg>,
    outbound: Arc<Mutex<LargeObjOutbound<Types::HashID, F>>>,
    parties: Arc<RwLock<HashMap<Types::SessionPrin, PartyID>>>,
    inbound: Arc<Mutex<LargeObjInbound<Types::HashID, Types::SessionPrin>>>,
    param: Arc<RwLock<F::Param>>,
    ids: Arc<Mutex<Types::IDs>>,
    tombstone_duration: Duration,
    notify: Notify,
    upstream: Types::Recv,
    msgs: Types::Msgs,
    retry: Retry,
    encoder: Types::Encoder,
    decoder: Types::Decoder,
    auth: Types::MsgAuthN,
    hash: Types::Hash
}

pub enum LargeObjPushRetry<H, Frags, Offer> {
    Frags { retry: Frags, id: LargeObjID },
    Offer { retry: Offer, hash: H },
    Retry { when: Instant }
}

#[derive(Debug)]
pub enum LargeObjPushError<H, Frags, Offer> {
    Frags { err: Frags, id: LargeObjID },
    Offer { err: Offer, hash: H },
    NoObjID { hash: H, id: LargeObjID },
    NoID { id: LargeObjID },
    NoObj { hash: H },
    MutexPoison
}

#[derive(Debug)]
pub enum LargeObjSendError<H, Prin, Msgs> {
    Msgs { err: Msgs },
    NoObj { hash: H, id: LargeObjID },
    NoPrin { prin: Prin },
    MutexPoison
}

#[derive(Debug)]
pub enum LargeObjRecvError<H, Auth, Decode, Upstream, Frags> {
    Upstream {
        err: Upstream
    },
    Decode {
        err: Decode
    },
    Auth {
        err: Auth
    },
    OutboundRecv {
        hash: H,
        id: LargeObjID,
        err: Frags
    },
    InboundRecv {
        hash: H,
        id: LargeObjID,
        err: InboundRecvError
    },
    FinishedPending {
        hash: H,
        id: LargeObjID
    },
    NotFound {
        hash: H,
        id: LargeObjID
    },
    NoHash {
        hash: H,
        id: LargeObjID
    },
    NoObj {
        hash: H,
        id: LargeObjID
    },
    Collision,
    AuthNFail,
    NoID,
    MutexPoison
}

#[derive(Debug)]
pub enum LargeObjProtoCreateError<Encoder, Decoder, IDs> {
    Encoder { err: Encoder },
    Decoder { err: Decoder },
    IDs { err: IDs }
}

#[derive(Debug)]
pub enum LargeObjMsgEncodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec
              as Encoder<LargeObjMetadata>>::EncodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec
              as Encoder<LargeObjFragHeader>>::EncodeError
    },
    TooShort
}

#[derive(Debug)]
pub enum LargeObjMsgDecodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec
              as Decoder<LargeObjMetadata>>::DecodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec
              as Decoder<LargeObjFragHeader>>::DecodeError
    },
    Hash {
        err: TryFromSliceError
    },
    TooShort
}

#[derive(Debug)]
pub enum LargeObjDataError {
    Frags { err: OutboundDataError },
    OutOfBounds
}

pub enum FragsOrOffer<HashID, Frags, Offer> {
    Frags { id: LargeObjID, err: Frags },
    Offer { hash: HashID, err: Offer }
}

impl From<usize> for LargeObjID {
    #[inline]
    fn from(val: usize) -> LargeObjID {
        LargeObjID(val as u64)
    }
}

impl From<u128> for LargeObjID {
    #[inline]
    fn from(val: u128) -> LargeObjID {
        LargeObjID(val as u64)
    }
}

impl From<u64> for LargeObjID {
    #[inline]
    fn from(val: u64) -> LargeObjID {
        LargeObjID(val)
    }
}

impl From<LargeObjID> for usize {
    #[inline]
    fn from(val: LargeObjID) -> usize {
        val.0 as usize
    }
}

impl From<LargeObjID> for u64 {
    #[inline]
    fn from(val: LargeObjID) -> u64 {
        val.0
    }
}

impl From<&'_ LargeObjID> for usize {
    #[inline]
    fn from(val: &LargeObjID) -> usize {
        val.0 as usize
    }
}

impl From<&'_ LargeObjID> for u64 {
    #[inline]
    fn from(val: &LargeObjID) -> u64 {
        val.0
    }
}

impl<H, Frags, Offer> RetryWhen for LargeObjPushRetry<H, Frags, Offer>
where
    Frags: RetryWhen,
    Offer: RetryWhen
{
    fn when(&self) -> Instant {
        match self {
            LargeObjPushRetry::Frags { retry, .. } => retry.when(),
            LargeObjPushRetry::Offer { retry, .. } => retry.when(),
            LargeObjPushRetry::Retry { when } => *when
        }
    }
}

impl LargeObjFrag {
    #[inline]
    pub fn new(
        offset: u64,
        data: Vec<u8>
    ) -> Self {
        LargeObjFrag {
            offset: offset,
            data: data
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn take(self) -> (u64, Vec<u8>) {
        (self.offset, self.data)
    }
}

impl InboundFragsState {
    fn size(&self) -> usize {
        match self {
            InboundFragsState::Active { frags, .. } => frags.len(),
            InboundFragsState::Finished { size, .. } => *size
        }
    }
}

impl<H> LargeObjMsg<H>
where
    H: HashID
{
    /// Maximum number of bytes that can be sent with a datagram.
    pub const LARGE_OBJ_DATAGRAM_MAX_DATA: usize = 1024;

    #[inline]
    pub fn offer(
        frags: &mut OutboundFrags,
        hash: H,
        max_bytes: usize
    ) -> Result<RetryResult<(Self, Instant)>, LargeObjDataError> {
        frags
            .offer_frag(max_bytes)
            .map_err(|err| LargeObjDataError::Frags { err: err })?
            .map_ok(|(offset, len, when)| match frags.data(offset, len) {
                Ok(data) => Ok((
                    LargeObjMsg::Offer {
                        hash: hash,
                        size: frags.len() as u64,
                        frag: LargeObjFrag {
                            offset: offset as u64,
                            data: data.to_vec()
                        }
                    },
                    when
                )),
                Err(_) => Err(LargeObjDataError::OutOfBounds)
            })
    }

    #[inline]
    pub fn accept(
        hash: H,
        size: usize,
        id: LargeObjID
    ) -> Self {
        LargeObjMsg::Accept {
            hash: hash,
            size: size as u64,
            id: id
        }
    }

    #[inline]
    pub fn req_obj(
        hash: H,
        size: usize,
        id: LargeObjID
    ) -> Self {
        LargeObjMsg::ReqObj {
            hash: hash,
            size: size as u64,
            id: id
        }
    }

    pub fn frags(
        frags: &mut OutboundFrags,
        id: LargeObjID,
        max_bytes: usize
    ) -> Result<RetryResult<Option<(Self, Instant)>>, LargeObjDataError> {
        let mut buf = [(0, 0); 16];

        frags
            .data_frags(&mut buf, max_bytes)
            .map_err(|err| LargeObjDataError::Frags { err: err })?
            .map_ok(|res| match res {
                Some((nmsgs, when)) => {
                    let mut fragbuf = Vec::with_capacity(nmsgs);

                    for (offset, len) in buf.iter().take(nmsgs) {
                        match frags.data(*offset, *len) {
                            Ok(data) => fragbuf.push(LargeObjFrag {
                                offset: *offset as u64,
                                data: data.to_vec()
                            }),
                            Err(_) => {
                                return Err(LargeObjDataError::OutOfBounds)
                            }
                        }
                    }

                    Ok(Some((
                        LargeObjMsg::Frags {
                            id: id,
                            frags: fragbuf
                        },
                        when
                    )))
                }
                None => Ok(None)
            })
    }

    #[inline]
    pub fn reqs<I>(
        id: LargeObjID,
        reqs: I
    ) -> Self
    where
        I: Iterator<Item = (bool, usize, usize)> {
        let reqs = reqs
            .map(|(need, offset, len)| {
                if need {
                    LargeObjFragReq::Need(LargeObjFragRef {
                        offset: offset as u64,
                        len: len as u64
                    })
                } else {
                    LargeObjFragReq::Ack(LargeObjFragRef {
                        offset: offset as u64,
                        len: len as u64
                    })
                }
            })
            .collect();

        LargeObjMsg::Req { id: id, reqs: reqs }
    }

    #[inline]
    pub fn finish(
        hash: H,
        id: LargeObjID
    ) -> Self {
        LargeObjMsg::Finish { hash: hash, id: id }
    }
}

impl<InMsg, OutMsg, PartyID, F, Types>
    SharedMsgs<PartyID, LargeObjMsg<Types::HashID>>
    for LargeObjProto<InMsg, OutMsg, PartyID, F, Types>
where
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    PartyID: Clone + Eq + Hash,
    F: Frags
{
    type MsgsError = LargeObjSendError<
        Types::HashID,
        Types::SessionPrin,
        <Types::Msgs as LargeObjMsgs<Types::Hash, OutMsg>>::AddMsgsError<
            Types::EncodeError
        >
    >;

    fn msgs(
        &mut self,
        parties: &HashSet<PartyID>,
        now: Instant
    ) -> Result<
        (
            Option<Vec<(Vec<PartyID>, Vec<LargeObjMsg<Types::HashID>>)>>,
            Option<Instant>
        ),
        Self::MsgsError
    > {
        debug!(target: "large-obj-proto",
               "collecting shared outbound messages");

        let mut next = self
            .msgs
            .add_msgs(&mut self.sender())
            .map_err(|err| LargeObjSendError::Msgs { err: err })?;
        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjSendError::MutexPoison)?;
        let size = inbound.hashes.len();
        let when = now + self.tombstone_duration;
        let mut msgs = Vec::with_capacity(size);
        let mut deletes = Vec::with_capacity(size);
        let hashes: Vec<((Types::SessionPrin, Types::HashID), LargeObjID)> =
            inbound
                .hashes
                .iter()
                .map(|((prin, hash), id)| {
                    ((prin.clone(), hash.clone()), id.clone())
                })
                .collect();

        // Scan the inbound objects for protocol replies that need to
        // be sent out.
        for ((prin, hash), id) in hashes {
            let party_id = self
                .parties
                .read()
                .map_err(|_| LargeObjSendError::MutexPoison)?
                .get(&prin)
                .ok_or(LargeObjSendError::NoPrin { prin: prin.clone() })?
                .clone();
            let ent =
                inbound.objs.get_mut(&id).ok_or(LargeObjSendError::NoObj {
                    hash: hash.clone(),
                    id: id.clone()
                })?;

            if parties.contains(&party_id) {
                match &mut ent.frags {
                    // Still sending accepts.
                    InboundFragsState::Active {
                        req: Some(ReqState { nretries, when }),
                        frags
                    } => {
                        let size = frags.len();
                        let delay = self.retry.retry_delay(*nretries);
                        let retry = now + delay;
                        let msg = LargeObjMsg::req_obj(hash, size, id);

                        trace!(target: "large-obj-proto",
                               "generating object request message");

                        *nretries += 1;
                        *when = retry;
                        next = Some(next_retry_definite(&next, &retry));
                        msgs.push((vec![party_id], vec![msg]));
                    }
                    InboundFragsState::Active { frags, .. } => {
                        let mut buf = [(false, 0, 0); 16];

                        trace!(target: "large-obj-proto",
                               "generating requests message");

                        match frags.reqs_acks(&mut buf[..], &self.retry) {
                            RetryResult::Success((n, retry)) => {
                                let iter = buf[..n].iter().cloned();
                                let msg = LargeObjMsg::reqs(id, iter);

                                msgs.push((vec![party_id], vec![msg]));
                                next = next_retry(&next, &retry);
                            }
                            RetryResult::Retry(retry) => {
                                next = Some(next_retry_definite(&next, &retry));
                            }
                        }
                    }
                    InboundFragsState::Finished {
                        expire,
                        accept,
                        size,
                        send
                    } => {
                        trace!(target: "large-obj-proto",
                               "generating finished or accept messages");

                        if let Some(expire) = expire {
                            if *send {
                                trace!(target: "large-obj-proto",
                                       concat!("extending expiration for ",
                                               "tombstone for {} ({}) to {:?}"),
                                       id, hash, self.tombstone_duration);

                                *expire = when;
                            } else if *expire <= now {
                                trace!(target: "large-obj-proto",
                                       "expiring tombstone for {} ({})",
                                       id, hash);

                                deletes.push((prin.clone(), hash.clone()));
                            }
                        } else {
                            trace!(target: "large-obj-proto",
                                   concat!("setting expiration for tombstone ",
                                           "for {} ({}) in {:?}"),
                                   id, hash, self.tombstone_duration);

                            *expire = Some(when);
                        }

                        if *send {
                            let msg = if *accept {
                                trace!(target: "large-obj-proto",
                                       "pushing accept for {}",
                                       hash);

                                LargeObjMsg::accept(ent.hash.clone(), *size, id)
                            } else {
                                trace!(target: "large-obj-proto",
                                       "pushing finish for ID {} ({})",
                                       id, hash);

                                LargeObjMsg::finish(ent.hash.clone(), id)
                            };

                            msgs.push((vec![party_id], vec![msg]));
                        }

                        *send = false;
                    }
                }
            }
        }

        // Get rid of all the finished entries.
        for key in deletes {
            if inbound.hashes.remove(&key).is_none() {
                error!(target: "large-obj-proto",
                       "remove should not return None")
            }
        }

        let msgs = if !msgs.is_empty() { Some(msgs) } else { None };

        Ok((msgs, next))
    }
}

impl<InMsg, OutMsg, PartyID, F, Types> PrivateMsgs<LargeObjMsg<Types::HashID>>
    for LargeObjProto<InMsg, OutMsg, PartyID, F, Types>
where
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    PartyID: Clone,
    F: Frags
{
    type MsgsError = LargeObjSendError<
        Types::HashID,
        Types::SessionPrin,
        <Types::Msgs as LargeObjMsgs<Types::Hash, OutMsg>>::AddMsgsError<
            Types::EncodeError
        >
    >;

    fn msgs(
        &mut self,
        now: Instant
    ) -> Result<
        (Option<Vec<LargeObjMsg<Types::HashID>>>, Option<Instant>),
        Self::MsgsError
    > {
        debug!(target: "large-obj-proto",
               "collecting private outbound messages");

        let mut next = self
            .msgs
            .add_msgs(&mut self.sender())
            .map_err(|err| LargeObjSendError::Msgs { err: err })?;
        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjSendError::MutexPoison)?;
        let size = inbound.hashes.len();
        let when = now + self.tombstone_duration;
        let mut msgs = Vec::with_capacity(size);
        let mut deletes = Vec::with_capacity(size);
        let hashes: Vec<((Types::SessionPrin, Types::HashID), LargeObjID)> =
            inbound
                .hashes
                .iter()
                .map(|((prin, hash), id)| {
                    ((prin.clone(), hash.clone()), id.clone())
                })
                .collect();

        // Scan the inbound objects for protocol replies that need to
        // be sent out.
        for ((prin, hash), id) in hashes {
            let ent =
                inbound.objs.get_mut(&id).ok_or(LargeObjSendError::NoObj {
                    hash: hash.clone(),
                    id: id.clone()
                })?;

            match &mut ent.frags {
                // Still sending accepts.
                InboundFragsState::Active {
                    req: Some(ReqState { nretries, when }),
                    frags
                } => {
                    let size = frags.len();
                    let delay = self.retry.retry_delay(*nretries);
                    let retry = now + delay;
                    let msg = LargeObjMsg::req_obj(hash, size, id);

                    *nretries += 1;
                    *when = retry;
                    next = Some(next_retry_definite(&next, &retry));
                    msgs.push(msg);
                }
                InboundFragsState::Active { frags, .. } => {
                    let mut buf = [(false, 0, 0); 16];

                    match frags.reqs_acks(&mut buf[..], &self.retry) {
                        RetryResult::Success((n, retry)) => {
                            let iter = buf[..n].iter().cloned();
                            let msg = LargeObjMsg::reqs(id, iter);

                            msgs.push(msg);
                            next = next_retry(&next, &retry);
                        }
                        RetryResult::Retry(retry) => {
                            next = Some(next_retry_definite(&next, &retry));
                        }
                    }
                }
                InboundFragsState::Finished {
                    accept,
                    expire,
                    size,
                    send
                } => {
                    if let Some(expire) = expire {
                        if *send {
                            trace!(target: "large-obj-proto",
                                   concat!("extending expiration for ",
                                           "tombstone for {} ({}) to {:?}"),
                                   id, hash, self.tombstone_duration);

                            *expire = when;
                        } else if *expire <= now {
                            trace!(target: "large-obj-proto",
                                   "expiring tombstone for {} ({})",
                                   id, hash);

                            deletes.push((prin.clone(), hash.clone()));
                        }
                    } else {
                        trace!(target: "large-obj-proto",
                               concat!("setting expiration for tombstone for ",
                                       "{} ({}) in {:?}"),
                               id, hash, self.tombstone_duration);

                        *expire = Some(when);
                    }

                    if *send {
                        let msg = if *accept {
                            trace!(target: "large-obj-proto",
                                   "pushing accept for {}",
                                   hash);

                            LargeObjMsg::accept(ent.hash.clone(), *size, id)
                        } else {
                            trace!(target: "large-obj-proto",
                                   "pushing finish for ID {} ({})",
                                   id, hash);

                            LargeObjMsg::finish(ent.hash.clone(), id)
                        };

                        msgs.push(msg);
                    }

                    *send = false;
                }
            }
        }

        // Get rid of all the finished entries.
        for key in deletes {
            if inbound.hashes.remove(&key).is_none() {
                error!(target: "large-obj-proto",
                       "remove should not return None")
            }
        }

        let msgs = if !msgs.is_empty() { Some(msgs) } else { None };

        Ok((msgs, next))
    }
}

impl<H> Create for LargeObjMsgCodec<H>
where
    H: HashAlgo + Default
{
    type Config = ();
    type CreateError = Infallible;

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(Self::default())
    }
}

impl<H> Decoder<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where
    H: HashAlgo + Default
{
    type DecodeError = LargeObjMsgDecodeError;

    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(LargeObjMsg<H::HashID>, usize), Self::DecodeError> {
        let (metadata, mut curr) = self
            .metadata
            .decode(buf)
            .map_err(|err| LargeObjMsgDecodeError::Metadata { err: err })?;

        match metadata {
            LargeObjMetadata::Offer(LargeObjOffer { hash, size, frag }) => {
                let datalen = frag.len as usize;
                let mut data = vec![0; datalen];
                let hash = self
                    .hash
                    .wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash { err: err })?;

                curr += if curr + datalen <= buf.len() {
                    data.copy_from_slice(&buf[curr..curr + datalen]);

                    Ok(datalen)
                } else {
                    Err(LargeObjMsgDecodeError::TooShort)
                }?;

                Ok((
                    LargeObjMsg::Offer {
                        hash: hash,
                        size: size,
                        frag: LargeObjFrag {
                            offset: frag.offset,
                            data: data
                        }
                    },
                    curr
                ))
            }
            LargeObjMetadata::ReqObj(LargeObjReqObj { hash, size, id }) => {
                let hash = self
                    .hash
                    .wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash { err: err })?;

                Ok((
                    LargeObjMsg::ReqObj {
                        id: id.into(),
                        hash: hash,
                        size: size
                    },
                    curr
                ))
            }
            LargeObjMetadata::Accept(LargeObjAccept { hash, size, id }) => {
                let hash = self
                    .hash
                    .wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash { err: err })?;

                Ok((
                    LargeObjMsg::Accept {
                        id: id.into(),
                        hash: hash,
                        size: size
                    },
                    curr
                ))
            }
            LargeObjMetadata::Frags(LargeObjFrags { id, nfrags }) => {
                let mut frags = Vec::with_capacity(nfrags as usize);

                for _ in 0..nfrags {
                    let (header, nbytes) = self
                        .frag_header
                        .decode(&buf[curr..])
                        .map_err(|err| LargeObjMsgDecodeError::FragHeader {
                            err: err
                        })?;

                    curr += nbytes;

                    let datalen = header.len as usize;
                    let mut data = vec![0; datalen];

                    curr += if curr + datalen <= buf.len() {
                        data.copy_from_slice(&buf[curr..curr + datalen]);
                        frags.push(LargeObjFrag {
                            offset: header.offset,
                            data: data
                        });

                        Ok(datalen)
                    } else {
                        Err(LargeObjMsgDecodeError::TooShort)
                    }?;
                }

                Ok((
                    LargeObjMsg::Frags {
                        id: id.into(),
                        frags: frags
                    },
                    curr
                ))
            }
            LargeObjMetadata::Req(LargeObjReq { id, reqs }) => Ok((
                LargeObjMsg::Req {
                    id: id.into(),
                    reqs: reqs
                },
                curr
            )),
            LargeObjMetadata::Finish(LargeObjFinish { id, hash }) => {
                let hash = self
                    .hash
                    .wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash { err: err })?;

                Ok((
                    LargeObjMsg::Finish {
                        hash: hash,
                        id: id.into()
                    },
                    curr
                ))
            }
        }
    }
}

impl<H> Encoder<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where
    H: HashAlgo + Default
{
    type EncodeError = LargeObjMsgEncodeError;

    #[inline]
    fn buf_size(
        &self,
        val: &LargeObjMsg<H::HashID>
    ) -> usize {
        match val {
            LargeObjMsg::Offer { hash, frag, .. } => {
                let hash = hash.hash_len() + 9;
                let frag = frag.data.len() + 9;
                let size = 9;

                hash + frag + size
            }
            LargeObjMsg::Accept { hash, .. } => {
                let hash = hash.hash_len();
                let size = 9;
                let id = 9;

                hash + size + id
            }
            LargeObjMsg::ReqObj { hash, .. } => {
                let hash = hash.hash_len();
                let size = 9;
                let id = 9;

                hash + size + id
            }
            LargeObjMsg::Frags { frags, .. } => {
                let frags: usize =
                    frags.iter().map(|frags| 18 + frags.data.len()).sum();
                let frags = frags + 9;
                let id = 9;

                frags + id
            }
            LargeObjMsg::Req { reqs, .. } => {
                let req_tag = 1;
                let req_offset = 9;
                let req_len = 9;
                let req = req_tag + req_offset + req_len;
                let reqs = (reqs.len() * req) + 9;
                let id = 9;

                reqs + id
            }
            LargeObjMsg::Finish { hash, .. } => {
                let hash = hash.hash_len();
                let id = 9;

                hash + id
            }
        }
    }

    #[inline]
    fn encode_to_vec(
        &mut self,
        val: &LargeObjMsg<H::HashID>
    ) -> Result<Vec<u8>, Self::EncodeError> {
        let mut buf = vec![0; Self::MAX_BYTES];

        self.encode(val, &mut buf)?;

        Ok(buf)
    }

    fn encode(
        &mut self,
        val: &LargeObjMsg<H::HashID>,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        match val {
            LargeObjMsg::Offer { hash, size, frag } => {
                let data_len = frag.data.len();
                let header = LargeObjFragHeader {
                    offset: frag.offset,
                    len: data_len as u64
                };
                let metadata = LargeObjMetadata::Offer(LargeObjOffer {
                    hash: hash.bytes().to_vec(),
                    size: *size,
                    frag: header
                });
                let mut curr =
                    self.metadata.encode(&metadata, buf).map_err(|err| {
                        LargeObjMsgEncodeError::Metadata { err: err }
                    })?;

                curr += if curr + data_len <= buf.len() {
                    buf[curr..curr + data_len].copy_from_slice(&frag.data);

                    Ok(data_len)
                } else {
                    Err(LargeObjMsgEncodeError::TooShort)
                }?;

                Ok(curr)
            }
            LargeObjMsg::Accept { hash, size, id } => {
                let msg = LargeObjMetadata::Accept(LargeObjAccept {
                    hash: hash.bytes().to_vec(),
                    size: *size,
                    id: id.clone().into()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::ReqObj { hash, size, id } => {
                let msg = LargeObjMetadata::ReqObj(LargeObjReqObj {
                    hash: hash.bytes().to_vec(),
                    size: *size,
                    id: id.clone().into()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::Frags { id, frags } => {
                let metadata = LargeObjMetadata::Frags(LargeObjFrags {
                    id: id.clone().into(),
                    nfrags: frags.len() as u8
                });
                let mut curr =
                    self.metadata.encode(&metadata, buf).map_err(|err| {
                        LargeObjMsgEncodeError::Metadata { err: err }
                    })?;

                for frag in frags {
                    let datalen = frag.data.len();
                    let header = LargeObjFragHeader {
                        offset: frag.offset,
                        len: datalen as u64
                    };

                    curr += self
                        .frag_header
                        .encode(&header, &mut buf[curr..])
                        .map_err(|err| LargeObjMsgEncodeError::FragHeader {
                            err: err
                        })?;

                    curr += if curr + datalen <= buf.len() {
                        buf[curr..curr + datalen].copy_from_slice(&frag.data);

                        Ok(datalen)
                    } else {
                        Err(LargeObjMsgEncodeError::TooShort)
                    }?;
                }

                Ok(curr)
            }
            LargeObjMsg::Req { id, reqs } => {
                let msg = LargeObjMetadata::Req(LargeObjReq {
                    id: id.clone().into(),
                    reqs: reqs.clone()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::Finish { id, hash } => {
                let msg = LargeObjMetadata::Finish(LargeObjFinish {
                    hash: hash.bytes().to_vec(),
                    id: id.clone().into()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
        }
    }
}

impl<H, Prin> LargeObjInbound<H, Prin> {
    #[inline]
    fn new() -> Self {
        LargeObjInbound {
            hashes: HashMap::new(),
            objs: HashMap::new()
        }
    }

    #[inline]
    fn with_capacity(size: usize) -> Self {
        LargeObjInbound {
            hashes: HashMap::with_capacity(size),
            objs: HashMap::with_capacity(size)
        }
    }
}

impl<H, F> LargeObjOutbound<H, F>
where
    F: Frags
{
    #[inline]
    fn new() -> Self {
        LargeObjOutbound {
            hashes: HashMap::new(),
            objs: HashMap::new()
        }
    }

    #[inline]
    fn with_capacity(size: usize) -> Self {
        LargeObjOutbound {
            hashes: HashMap::with_capacity(size),
            objs: HashMap::with_capacity(size)
        }
    }
}

impl<H, Msg, Enc, F> LargeObjSender<H, Msg, Enc, F>
where
    Enc: Create + Encoder<Msg>,
    Enc::Config: Default,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Hash + HashID + Eq,
    F: Frags
{
    pub fn add_outbound(
        &mut self,
        msg: &Msg
    ) -> Result<
        Option<H::HashID>,
        LargeObjProtoAddOutboundError<Enc::EncodeError>
    > {
        trace!(target: "large-obj-proto",
               "adding new outbound message if not already present");

        let data = self.encoder.encode_to_vec(msg).map_err(|err| {
            LargeObjProtoAddOutboundError::Encode { err: err }
        })?;
        let hash = self.hash.hash_bytes(once(&data[..]));

        trace!(target: "large-obj-proto",
               "hash for message is {}",
               hash);

        let mut guard = self
            .outbound
            .lock()
            .map_err(|_| LargeObjProtoAddOutboundError::MutexPoison)?;

        if !guard.objs.contains_key(&hash) {
            trace!(target: "large-obj-proto",
                   "message {} was not present",
                   hash);

            let param = self
                .param
                .read()
                .map_err(|_| LargeObjProtoAddOutboundError::MutexPoison)?
                .clone();
            let frags = F::from_data(param, data);
            let ent = SendEntry {
                when: Some(Instant::now()),
                frags: frags,
                id: None
            };

            // Insert into objs *only*; hashes is for the
            // *counterparty's* IDs.
            let _ = guard.objs.insert(hash.clone(), ent);

            debug!(target: "large-obj-proto",
                   "added message {} to outbound",
                   hash);

            Ok(Some(hash))
        } else {
            trace!(target: "large-obj-proto",
                   "message {} was present",
                   hash);

            Ok(None)
        }
    }
}

impl<InMsg, OutMsg, PartyID, F, Types>
    LargeObjProto<InMsg, OutMsg, PartyID, F, Types>
where
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    PartyID: Clone,
    F: Frags
{
    /// Create a protocol instance.
    pub fn create(
        config: LargeObjProtoConfig<
            Types::EncoderConfig,
            Types::DecoderConfig,
            Types::IDsConfig
        >,
        notify: Notify,
        upstream: Types::Recv,
        msgs: Types::Msgs,
        auth: Types::MsgAuthN,
        hash: Types::Hash
    ) -> Result<
        Self,
        LargeObjProtoCreateError<
            <Types::Encoder as Create>::CreateError,
            <Types::Decoder as Create>::CreateError,
            <Types::IDs as Create>::CreateError
        >
    > {
        let (
            retry,
            encoder,
            decoder,
            ids,
            tombstone_duration,
            inbound_size,
            outbound_size
        ) = config.take();
        let inbound = match inbound_size {
            Some(size) => LargeObjInbound::with_capacity(size),
            None => LargeObjInbound::new()
        };
        let inbound = Arc::new(Mutex::new(inbound));
        let outbound = match outbound_size {
            Some(size) => LargeObjOutbound::with_capacity(size),
            None => LargeObjOutbound::new()
        };
        let outbound = Arc::new(Mutex::new(outbound));
        let parties = Arc::new(RwLock::new(HashMap::new()));
        let encoder = <Types::Encoder as Create>::create(encoder)
            .map_err(|err| LargeObjProtoCreateError::Encoder { err })?;
        let decoder = <Types::Decoder as Create>::create(decoder)
            .map_err(|err| LargeObjProtoCreateError::Decoder { err })?;
        let ids = Types::IDs::create(ids)
            .map_err(|err| LargeObjProtoCreateError::IDs { err })?;
        let ids = Arc::new(Mutex::new(ids));
        let param = Arc::new(RwLock::new(F::param(retry.clone())));

        Ok(LargeObjProto {
            out_msg: PhantomData,
            in_msg: PhantomData,
            tombstone_duration: tombstone_duration,
            inbound: inbound,
            outbound: outbound,
            parties: parties,
            upstream: upstream,
            notify: notify,
            param: param,
            retry: retry,
            encoder: encoder,
            decoder: decoder,
            auth: auth,
            hash: hash,
            msgs: msgs,
            ids: ids
        })
    }

    #[inline]
    pub fn sender(
        &self
    ) -> LargeObjSender<Types::Hash, OutMsg, Types::Encoder, F> {
        LargeObjSender {
            wrapper: PhantomData,
            outbound: self.outbound.clone(),
            encoder: self.encoder.clone(),
            param: self.param.clone(),
            hash: self.hash.clone()
        }
    }

    /// Set the allowed parties for this protocol instance.
    pub fn set_parties<I>(
        &mut self,
        param: F::Param,
        parties: I
    ) -> Result<(), MutexPoison>
    where
        I: Iterator<Item = (PartyID, Types::SessionPrin)> {
        let mut guard = self.parties.write().map_err(|_| MutexPoison)?;

        *guard = parties.map(|(a, b)| (b, a)).collect();

        let mut guard = self.param.write().map_err(|_| MutexPoison)?;

        *guard = param;

        Ok(())
    }

    pub(crate) fn try_push<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        now: Instant
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            LargeObjPushRetry<
                Types::HashID,
                Stream::PushFragRetry,
                Stream::PushOfferRetry
            >,
            Parties<Stream::Parties>
        >,
        LargeObjPushError<
            Types::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Stream: LargeObjOfferStream<Types::HashID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as RecoverableError>::Permanent
            > + PushStreamReportError<
                <Stream::PushOfferError as RecoverableError>::Permanent
            > {
        trace!(target: "large-obj-proto",
               "trying to push fragments");

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjPushError::MutexPoison)?;
        // XXX use a better data structure here.
        let mut ents: Vec<(&Types::HashID, &mut SendEntry<F>)> =
            outbound.objs.iter_mut().collect();

        if !ents.is_empty() {
            ents.sort_unstable_by(|(_, a), (_, b)| match (a.when, b.when) {
                (Some(a), Some(b)) => a.cmp(&b),
                (None, None) => Ordering::Equal,
                (None, _) => Ordering::Greater,
                (_, None) => Ordering::Less
            });

            let hash = ents[0].0;
            let ents_len = ents.len();

            match ents[0].1.when {
                Some(when) if when <= now => {
                    if let Some(id) = ents[0].1.id.clone() {
                        trace!(target: "large-obj-proto",
                               "pushing fragments for {}",
                               id);

                        stream
                            .push_frags(ctx, id.clone(), &mut ents[0].1.frags)
                            .map_err(|err| LargeObjPushError::Frags {
                                err: err,
                                id: id.clone()
                            })
                            .map(|res| {
                                res.map_retry(|retry| {
                                    LargeObjPushRetry::Frags {
                                        retry: retry,
                                        id: id.clone()
                                    }
                                })
                                .map(
                                    |(retry, parties)| {
                                        ents[0].1.when = retry;

                                        if ents_len < 2 {
                                            (retry, parties)
                                        } else {
                                            let when = next_retry(
                                                &ents[1].1.when,
                                                &retry
                                            );

                                            (when, parties)
                                        }
                                    }
                                )
                            })
                    } else {
                        trace!(target: "large-obj-proto",
                               "pushing offer for {}",
                               hash);

                        stream
                            .push_offer(ctx, hash.clone(), &mut ents[0].1.frags)
                            .map_err(|err| LargeObjPushError::Offer {
                                hash: hash.clone(),
                                err: err
                            })
                            .map(|res| {
                                res.map_retry(|retry| {
                                    LargeObjPushRetry::Offer {
                                        retry: retry,
                                        hash: hash.clone()
                                    }
                                })
                                .map(
                                    |(retry, parties)| {
                                        ents[0].1.when = retry;

                                        if ents_len < 2 {
                                            (retry, parties)
                                        } else {
                                            let when = next_retry(
                                                &ents[1].1.when,
                                                &retry
                                            );

                                            (when, parties)
                                        }
                                    }
                                )
                            })
                    }
                }
                Some(when) => {
                    Ok(RetryIndefResult::Retry(LargeObjPushRetry::Retry {
                        when: when
                    }))
                }
                None => Ok(RetryIndefResult::Indef(Parties::All))
            }
        } else {
            trace!(target: "large-obj-proto",
                   "no active entries");

            Ok(RetryIndefResult::Indef(Parties::All))
        }
    }

    pub(crate) fn retry_push_frags<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        id: LargeObjID,
        retry: Stream::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            Stream::PushFragRetry,
            Parties<Stream::Parties>
        >,
        LargeObjPushError<
            Types::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Stream: LargeObjOfferStream<Types::HashID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as RecoverableError>::Permanent
            > {
        trace!(target: "large-obj-proto",
               "retrying pushing fragments for {}",
               id);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjPushError::MutexPoison)?;
        let hash = outbound
            .hashes
            .get(&id)
            .cloned()
            .ok_or(LargeObjPushError::NoID { id: id.clone() })?;
        let ent =
            outbound
                .objs
                .get_mut(&hash)
                .ok_or(LargeObjPushError::NoObjID {
                    hash: hash,
                    id: id.clone()
                })?;

        stream
            .retry_push_frags(ctx, id.clone(), &mut ent.frags, retry)
            .map_err(|err| LargeObjPushError::Frags { id: id, err: err })
    }

    pub(crate) fn retry_push_offer<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        hash: Types::HashID,
        retry: Stream::PushOfferRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            Stream::PushOfferRetry,
            Parties<Stream::Parties>
        >,
        LargeObjPushError<
            Types::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Stream: LargeObjOfferStream<Types::HashID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushOfferError as RecoverableError>::Permanent
            > {
        trace!(target: "large-obj-proto",
               "retrying pushing offer for {}",
               hash);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjPushError::MutexPoison)?;
        let ent = outbound
            .objs
            .get_mut(&hash)
            .ok_or(LargeObjPushError::NoObj { hash: hash.clone() })?;

        stream
            .retry_push_offer(ctx, hash.clone(), &mut ent.frags, retry)
            .map_err(|err| LargeObjPushError::Offer {
                hash: hash,
                err: err
            })
    }

    pub(crate) fn complete_push_frags<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        id: LargeObjID,
        err: <Stream::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            Stream::PushFragRetry,
            Parties<Stream::Parties>
        >,
        LargeObjPushError<
            Types::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Stream: LargeObjOfferStream<Types::HashID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as RecoverableError>::Permanent
            > {
        trace!(target: "large-obj-proto",
               "completing pushing fragments for {}",
               id);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjPushError::MutexPoison)?;
        let hash = outbound
            .hashes
            .get(&id)
            .cloned()
            .ok_or(LargeObjPushError::NoID { id: id.clone() })?;
        let ent =
            outbound
                .objs
                .get_mut(&hash)
                .ok_or(LargeObjPushError::NoObjID {
                    hash: hash,
                    id: id.clone()
                })?;

        stream
            .complete_push_frags(ctx, id.clone(), &mut ent.frags, err)
            .map_err(|err| LargeObjPushError::Frags { err: err, id: id })
    }

    pub(crate) fn complete_push_offer<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        hash: Types::HashID,
        err: <Stream::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Stream::Parties),
            Stream::PushOfferRetry,
            Parties<Stream::Parties>
        >,
        LargeObjPushError<
            Types::HashID,
            Stream::PushFragError,
            Stream::PushOfferError
        >
    >
    where
        Stream: LargeObjOfferStream<Types::HashID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushOfferError as RecoverableError>::Permanent
            > {
        trace!(target: "large-obj-proto",
               "completing pushing offer for {}",
               hash);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjPushError::MutexPoison)?;
        let ent = outbound
            .objs
            .get_mut(&hash)
            .ok_or(LargeObjPushError::NoObj { hash: hash.clone() })?;

        stream
            .complete_push_offer(ctx, hash.clone(), &mut ent.frags, err)
            .map_err(|err| LargeObjPushError::Offer {
                hash: hash,
                err: err
            })
    }

    fn recv_msg(
        &mut self,
        prin: Types::SessionPrin,
        msg: LargeObjMsg<Types::HashID>
    ) -> Result<
        (),
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        let data = match msg {
            // Inbound messages.
            LargeObjMsg::Offer { hash, size, frag } => {
                self.recv_offer_msg(&prin, hash, size, frag)
            }
            LargeObjMsg::Frags { id, frags } => self.recv_frags_msg(id, frags),
            // Outbound messages.
            LargeObjMsg::Accept { hash, id, .. } => {
                self.recv_accept_msg(hash, id)
            }
            LargeObjMsg::ReqObj { hash, id, .. } => {
                self.recv_req_obj_msg(hash, id)
            }
            LargeObjMsg::Req { id, reqs } => self.recv_reqs_msg(id, reqs),
            LargeObjMsg::Finish { id, hash } => {
                self.recv_finish_msg(hash, id)?;

                Ok(None)
            }
        }?;

        // Complete the message and send it upstream.
        if let Some(data) = data {
            debug!(target: "large-obj-proto",
                   "processing complete message");

            trace!(target: "large-obj-proto",
                   "decoding message of length {}",
                   data.len());

            // Decode the complete message.
            let (wrapper, _) = self.decoder.decode(&data).map_err(|err| {
                error!(target: "large-obj-proto",
                           "error decoding message: {}",
                           err);

                LargeObjRecvError::Decode { err: err }
            })?;

            trace!(target: "large-obj-proto",
                   "authenticating message");

            // Authenticate the complete message.
            match self
                .auth
                .msg_authn(&prin, wrapper)
                .map_err(|err| LargeObjRecvError::Auth { err: err })?
            {
                AuthNResult::Accept(msg) => {
                    trace!(target: "large-obj-proto",
                           "message authenticated");

                    // Send it upstream.
                    self.upstream.recv_auth_msg(msg).map_err(|err| {
                        LargeObjRecvError::Upstream { err: err }
                    })?;

                    Ok(())
                }
                AuthNResult::Reject(_) => {
                    trace!(target: "large-obj-proto",
                           "message authentication failed");

                    Err(LargeObjRecvError::AuthNFail)
                }
            }
        } else {
            Ok(())
        }
    }

    pub fn recv_offer_msg(
        &mut self,
        prin: &<Types::AuthNTypes as MsgAuthNTypes<InMsg>>::SessionPrin,
        hash: Types::HashID,
        size: u64,
        frag: LargeObjFrag
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received offer for {}",
               hash);

        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        let out = match inbound.hashes.entry((prin.clone(), hash.clone())) {
            Entry::Occupied(ent) => {
                // ID already exists, get the entry.
                let id = ent.get().clone();

                trace!(target: "large-obj-proto",
                       "entry exists for {} (ID {})",
                       hash, id);

                let RecvEntry { frags, hash, .. } =
                    inbound.objs.get_mut(&id).ok_or(
                        LargeObjRecvError::NotFound {
                            hash: hash,
                            id: id.clone()
                        }
                    )?;
                // Check if we're still receiving fragments.
                let closeout = match frags {
                    InboundFragsState::Active { frags, .. } => {
                        // Receive the fragment.
                        frags
                            .recv(frag.offset() as usize, frag.data())
                            .map_err(|err| LargeObjRecvError::InboundRecv {
                                hash: hash.clone(),
                                id: id.clone(),
                                err: err
                            })?;

                        frags.is_finished()
                    }
                    InboundFragsState::Finished { send, .. } => {
                        // This is ok, it can happen due to delayed
                        // messages.
                        trace!(target: "large-obj-proto",
                               "redundant offer message for ID {} ({})",
                               id, hash);

                        *send = true;

                        false
                    }
                };

                // Check if the entry is finished and report if it is.
                if closeout {
                    debug!(target: "large-obj-proto",
                           "finished transfer for ID {}",
                           id);

                    let finished = InboundFragsState::Finished {
                        size: frags.size(),
                        accept: false,
                        expire: None,
                        send: true
                    };
                    let data = match std::mem::replace(frags, finished) {
                        InboundFragsState::Active { frags, .. } => {
                            match frags.finish() {
                                Ok(data) => Some(data),
                                Err(_) => {
                                    error!(target: "large-obj-proto",
                                       "finish for ID {} should not fail",
                                       id);

                                    None
                                }
                            }
                        }
                        InboundFragsState::Finished { .. } => {
                            error!(target: "large-obj-proto",
                                   concat!("frags for ID {} should ",
                                           "not be finished"),
                                   id);

                            None
                        }
                    };

                    Ok(data)
                } else {
                    Ok(None)
                }
            }
            Entry::Vacant(hashes) => {
                trace!(target: "large-obj-proto",
                       "no entry exists for {}",
                       hash);

                // No entry for this hash exists, set one up.
                let id = self
                    .ids
                    .lock()
                    .map_err(|_| LargeObjRecvError::MutexPoison)?
                    .next()
                    .ok_or(LargeObjRecvError::NoID)?;
                let size = size as usize;

                hashes.insert(id.clone());

                debug!(target: "large-obj-proto",
                       "creating new transfer for {} with ID {}",
                       hash, id);

                let (offset, data) = frag.take();
                // See if the offer provides all the data.
                let (ent, data) = if offset == 0 && data.len() == size {
                    trace!(target: "large-obj-proto",
                           "offer message provides entire object");

                    // Complete the message and report it upstream.

                    (
                        RecvEntry {
                            frags: InboundFragsState::Finished {
                                accept: true,
                                size: size,
                                expire: None,
                                send: true
                            },
                            hash: hash
                        },
                        Some(data)
                    )
                } else {
                    trace!(target: "large-obj-proto",
                           "offer message provides partial object");

                    let mut frags = InboundFrags::new(size);

                    frags.recv(offset as usize, &data).map_err(|err| {
                        LargeObjRecvError::InboundRecv {
                            hash: hash.clone(),
                            id: id.clone(),
                            err: err
                        }
                    })?;

                    (
                        RecvEntry {
                            frags: InboundFragsState::Active {
                                req: Some(ReqState {
                                    when: Instant::now(),
                                    nretries: 0
                                }),
                                frags: frags
                            },
                            hash: hash
                        },
                        None
                    )
                };

                // Error if an entry already exists under this ID.
                if inbound.objs.insert(id, ent).is_none() {
                    Ok(data)
                } else {
                    Err(LargeObjRecvError::Collision)
                }
            }
        }?;

        // Notify, as all cases generate messages.
        self.notify
            .notify()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        Ok(out)
    }

    pub fn recv_frags_msg(
        &mut self,
        id: LargeObjID,
        recv: Vec<LargeObjFrag>
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received fragments for ID {}",
               id);

        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        // Look up the entry.
        match inbound.objs.get_mut(&id) {
            Some(RecvEntry { frags, hash, .. }) => {
                // Receive the fragments and see if it completes the message.
                let closeout = if let InboundFragsState::Active { frags, req } =
                    frags
                {
                    // If we get a frags message, that means our req
                    // has been acknowledged.
                    *req = None;

                    // Receive all of the fragments
                    for frag in recv {
                        frags
                            .recv(frag.offset() as usize, frag.data())
                            .map_err(|err| LargeObjRecvError::InboundRecv {
                                hash: hash.clone(),
                                id: id.clone(),
                                err: err
                            })?;
                    }

                    frags.is_finished()
                } else {
                    trace!(target: "large-obj-proto",
                           "redundant fragments message for ID {} ({})",
                           id, hash);

                    false
                };

                // Check if the entry is finished and report if it is.
                if closeout {
                    debug!(target: "large-obj-proto",
                           "finished transfer for ID {}",
                           id);

                    let finished = InboundFragsState::Finished {
                        size: frags.size(),
                        accept: false,
                        expire: None,
                        send: true
                    };
                    let data = match std::mem::replace(frags, finished) {
                        InboundFragsState::Active { frags, .. } => {
                            match frags.finish() {
                                Ok(data) => Some(data),
                                Err(_) => {
                                    error!(target: "large-obj-proto",
                                       "finish for ID {} should not fail",
                                       id);

                                    None
                                }
                            }
                        }
                        InboundFragsState::Finished { .. } => {
                            error!(target: "large-obj-proto",
                                   concat!("frags for ID {} should ",
                                           "not be finished"),
                                   id);

                            None
                        }
                    };

                    // Notify, as this will generate a finished message.
                    self.notify
                        .notify()
                        .map_err(|_| LargeObjRecvError::MutexPoison)?;

                    Ok(data)
                } else {
                    Ok(None)
                }
            }
            None => {
                trace!(target: "large-obj-proto",
                       "fragments message for non-existent ID {}",
                       id);

                Ok(None)
            }
        }
    }

    pub fn recv_accept_msg(
        &mut self,
        hash: Types::HashID,
        id: LargeObjID
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received accept for ID {} ({})",
               id, hash);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        // Remove the entry; we're done.
        //
        // We don't need to notify, as no more messages are required.
        if outbound.objs.remove(&hash).is_some() {
            debug!(target: "large-obj-proto",
                   "received acceptance for {} (ID {})",
                   hash, id);

            if outbound.hashes.insert(id, hash).is_none() {
                Ok(None)
            } else {
                Err(LargeObjRecvError::Collision)
            }
        } else {
            trace!(target: "large-obj-proto",
                   "redundant accept for {}",
                   hash);

            Ok(None)
        }
    }

    pub fn recv_req_obj_msg(
        &mut self,
        hash: Types::HashID,
        id: LargeObjID
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received object request for {} (ID {})",
               hash, id);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        // Look up the object entry and add the ID.
        let valid = match outbound.objs.get_mut(&hash) {
            Some(ent) => {
                ent.id = Some(id.clone());
                ent.when = Some(Instant::now());

                true
            }
            None => false
        };

        if valid {
            // If the entry exists, add an entry to the hash-id map.
            match outbound.hashes.entry(id.clone()) {
                Entry::Occupied(_) => {
                    trace!(target: "large-obj-proto",
                           "redundant object request for {}",
                           hash);
                }
                Entry::Vacant(ent) => {
                    debug!(target: "large-obj-proto",
                           "new request for {} (ID {})",
                           hash, id);

                    ent.insert(hash);
                }
            }

            // Notify, as this could potentially generate new
            // messages.
            self.notify
                .notify()
                .map_err(|_| LargeObjRecvError::MutexPoison)?;

            Ok(None)
        } else {
            trace!(target: "large-obj-proto",
                   "stray object request for {}",
                   hash);

            Ok(None)
        }
    }

    pub fn recv_reqs_msg(
        &mut self,
        id: LargeObjID,
        reqs: Vec<LargeObjFragReq>
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received fragments for ID {}",
               id);

        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        // Look up the entries.
        match outbound.hashes.get(&id).cloned() {
            Some(hash) => match outbound.objs.get_mut(&hash) {
                Some(ent) => {
                    // Update the outbound fragment structure.
                    debug!(target: "large-obj-proto",
                           "received fragments for {} (ID {})",
                           hash, id);

                    for req in reqs {
                        ent.frags.recv_req(&req).map_err(|err| {
                            LargeObjRecvError::OutboundRecv {
                                hash: hash.clone(),
                                id: id.clone(),
                                err: err
                            }
                        })?;
                    }

                    // Notify, as this could potentially generate new
                    // messages.
                    self.notify
                        .notify()
                        .map_err(|_| LargeObjRecvError::MutexPoison)?;

                    Ok(None)
                }
                None => Err(LargeObjRecvError::NoObj {
                    hash: hash,
                    id: id.clone()
                })
            },
            None => {
                trace!(target: "large-obj-proto",
                       "stray frags message for {}",
                       id);

                Ok(None)
            }
        }
    }

    pub fn recv_finish_msg(
        &mut self,
        hash: Types::HashID,
        id: LargeObjID
    ) -> Result<
        (),
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >
    > {
        trace!(target: "large-obj-proto",
               "received finish for ID {} ({})",
               id, hash);

        // The transfer is finished; remove the entry.
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        if outbound.hashes.remove(&id).is_none() {
            trace!(target: "large-obj-proto",
                   "redundant finish for ID {}",
                   id);
        }

        if outbound.objs.remove(&hash).is_none() {
            trace!(target: "large-obj-proto",
                   "redundant finish for ID {}",
                   id);
        }

        // We don't need to notify, as no more messages are required.

        Ok(())
    }
}

impl<InMsg, OutMsg, PartyID, F, AuthNMsg, Types>
    AuthNMsgRecv<Types::SessionPrin, LargeObjMsg<Types::HashID>, AuthNMsg>
    for LargeObjProto<InMsg, OutMsg, PartyID, F, Types>
where
    AuthNMsg: AuthNed<Types::SessionPrin, LargeObjMsg<Types::HashID>>,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    PartyID: Clone,
    F: Frags
{
    type RecvError =
        LargeObjRecvError<
            Types::HashID,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::AuthNError,
            <Types::AuthNTypes as MsgAuthNTypes<InMsg>>::DecodeError,
            <Types::Recv as AuthNMsgRecv<
                Types::Prin,
                InMsg,
                Types::AuthNMsg
            >>::RecvError,
            F::RecvReqError
        >;

    fn recv_auth_msg(
        &mut self,
        msg: AuthNMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();

        self.recv_msg(prin, msg)
    }
}

impl RecoverableError for LargeObjMsgEncodeError {
    type Completable = Infallible;
    type Permanent = Self;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<H, Frags, Offer> RecoverableError for LargeObjPushError<H, Frags, Offer>
where
    Frags: RecoverableError,
    Offer: RecoverableError,
    H: Clone + Debug + Display + Eq + Hash + HashID
{
    type Completable = FragsOrOffer<H, Frags::Completable, Offer::Completable>;
    type Permanent = LargeObjPushError<H, Frags::Permanent, Offer::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            LargeObjPushError::Frags { err, id } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| FragsOrOffer::Frags {
                        err: err,
                        id: id.clone()
                    }),
                    permanent.map(|err| LargeObjPushError::Frags {
                        err: err,
                        id: id
                    })
                )
            }
            LargeObjPushError::Offer { err, hash } => {
                let (completable, permanent) = err.split();

                (
                    completable.map(|err| FragsOrOffer::Offer {
                        hash: hash.clone(),
                        err: err
                    }),
                    permanent.map(|err| LargeObjPushError::Offer {
                        hash: hash,
                        err: err
                    })
                )
            }
            LargeObjPushError::NoObjID { hash, id } => (
                None,
                Some(LargeObjPushError::NoObjID { hash: hash, id: id })
            ),
            LargeObjPushError::NoID { id } => {
                (None, Some(LargeObjPushError::NoID { id: id }))
            }
            LargeObjPushError::NoObj { hash } => {
                (None, Some(LargeObjPushError::NoObj { hash: hash }))
            }
            LargeObjPushError::MutexPoison => {
                (None, Some(LargeObjPushError::MutexPoison))
            }
        }
    }
}

impl<H> DatagramCodec<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where
    H: Default + HashAlgo
{
    const MAX_BYTES: usize = 1286;
}

impl<Encode> ScopedError for LargeObjProtoAddOutboundError<Encode>
where
    Encode: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjProtoAddOutboundError::Encode { err } => err.scope(),
            LargeObjProtoAddOutboundError::NoIDs |
            LargeObjProtoAddOutboundError::MutexPoison => {
                ErrorScope::Unrecoverable
            }
        }
    }
}

impl<H, Frags, Offer> ScopedError for FragsOrOffer<H, Frags, Offer>
where
    Frags: ScopedError,
    Offer: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            FragsOrOffer::Frags { err, .. } => err.scope(),
            FragsOrOffer::Offer { err, .. } => err.scope()
        }
    }
}

impl<H, Auth, Decode, Upstream, Frags> ScopedError
    for LargeObjRecvError<H, Auth, Decode, Upstream, Frags>
where
    H: Clone + Display + Hash + HashID + Eq,
    Upstream: ScopedError,
    Frags: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjRecvError::NotFound { .. } |
            LargeObjRecvError::NoHash { .. } |
            LargeObjRecvError::NoObj { .. } |
            LargeObjRecvError::Collision |
            LargeObjRecvError::MutexPoison |
            LargeObjRecvError::NoID => ErrorScope::Unrecoverable,
            LargeObjRecvError::FinishedPending { .. } |
            LargeObjRecvError::InboundRecv { .. } |
            LargeObjRecvError::Decode { .. } |
            LargeObjRecvError::Auth { .. } |
            LargeObjRecvError::AuthNFail => ErrorScope::Msg,
            LargeObjRecvError::OutboundRecv { err, .. } => err.scope(),
            LargeObjRecvError::Upstream { err } => err.scope()
        }
    }
}

impl<H, Frags, Offer> ScopedError for LargeObjPushError<H, Frags, Offer>
where
    H: Clone + Display + Hash + HashID + Eq,
    Frags: ScopedError,
    Offer: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjPushError::Offer { err, .. } => err.scope(),
            LargeObjPushError::Frags { err, .. } => err.scope(),
            LargeObjPushError::NoObjID { .. } |
            LargeObjPushError::NoID { .. } |
            LargeObjPushError::NoObj { .. } |
            LargeObjPushError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<H, Prin, Msgs> ScopedError for LargeObjSendError<H, Prin, Msgs>
where
    Msgs: ScopedError,
    H: Clone + Display + Hash + HashID + Eq
{
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjSendError::Msgs { err } => err.scope(),
            LargeObjSendError::NoObj { .. } |
            LargeObjSendError::NoPrin { .. } |
            LargeObjSendError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<H> Default for LargeObjMsgCodec<H>
where
    H: HashAlgo + Default
{
    #[inline]
    fn default() -> Self {
        LargeObjMsgCodec {
            frag_header: LargeObjFragHeaderPERCodec::default(),
            metadata: LargeObjMetadataPERCodec::default(),
            hash: H::default()
        }
    }
}

impl<Info> ErrorReportInfo<Info> for LargeObjDataError {
    #[inline]
    fn report_info(&self) -> Option<Info> {
        match self {
            LargeObjDataError::Frags { err } => err.report_info(),
            LargeObjDataError::OutOfBounds => None
        }
    }
}

impl ScopedError for LargeObjDataError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjDataError::Frags { .. } => ErrorScope::Unrecoverable,
            LargeObjDataError::OutOfBounds => ErrorScope::Unrecoverable
        }
    }
}

impl ScopedError for LargeObjMsgEncodeError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        ErrorScope::Msg
    }
}

impl ScopedError for LargeObjMsgDecodeError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        ErrorScope::Msg
    }
}

impl Display for LargeObjID {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "obj #{:x}", self.0)
    }
}

impl Display for LargeObjDataError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjDataError::Frags { err } => write!(f, "{}", err),
            LargeObjDataError::OutOfBounds => {
                write!(f, "offered data is outside available data range")
            }
        }
    }
}

impl<Encoder, Decoder, IDs> Display
    for LargeObjProtoCreateError<Encoder, Decoder, IDs>
where
    Encoder: Display,
    Decoder: Display,
    IDs: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjProtoCreateError::Encoder { err } => err.fmt(f),
            LargeObjProtoCreateError::Decoder { err } => err.fmt(f),
            LargeObjProtoCreateError::IDs { err } => err.fmt(f)
        }
    }
}

impl Display for LargeObjMsgEncodeError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjMsgEncodeError::Metadata { err } => write!(f, "{}", err),
            LargeObjMsgEncodeError::FragHeader { err } => write!(f, "{}", err),
            LargeObjMsgEncodeError::TooShort => {
                write!(f, "output buffer is too short")
            }
        }
    }
}

impl Display for LargeObjMsgDecodeError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjMsgDecodeError::Metadata { err } => write!(f, "{}", err),
            LargeObjMsgDecodeError::FragHeader { err } => write!(f, "{}", err),
            LargeObjMsgDecodeError::Hash { err } => write!(f, "{}", err),
            LargeObjMsgDecodeError::TooShort => {
                write!(f, "input buffer is too short")
            }
        }
    }
}

impl<H, Frags, Offer> Display for LargeObjPushError<H, Frags, Offer>
where
    H: Clone + Display + Hash + HashID + Eq,
    Frags: Display,
    Offer: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjPushError::Offer { err, .. } => err.fmt(f),
            LargeObjPushError::Frags { err, .. } => err.fmt(f),
            LargeObjPushError::NoObjID { hash, id } => write!(
                f,
                "ID {} exists for {}, but no object entry found",
                id, hash
            ),
            LargeObjPushError::NoObj { hash } => {
                write!(f, "no object entry found for {}", hash)
            }
            LargeObjPushError::NoID { id } => {
                write!(f, "no object id {} found", id)
            }
            LargeObjPushError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}

impl<H, Prin, Msgs> Display for LargeObjSendError<H, Prin, Msgs>
where
    H: Clone + Display + Hash + HashID + Eq,
    Msgs: Display,
    Prin: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjSendError::Msgs { err } => err.fmt(f),
            LargeObjSendError::NoObj { hash, id } => write!(
                f,
                "ID {} exists for {}, but no object entry found",
                id, hash
            ),
            LargeObjSendError::NoPrin { prin } => {
                write!(f, "no party ID for principal {}", prin)
            }
            LargeObjSendError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}

impl<H, Auth, Decode, Upstream, Frags> Display
    for LargeObjRecvError<H, Auth, Decode, Upstream, Frags>
where
    H: Clone + Display + Hash + HashID + Eq,
    Upstream: Display,
    Decode: Display,
    Frags: Display,
    Auth: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjRecvError::Upstream { err } => err.fmt(f),
            LargeObjRecvError::Decode { err } => err.fmt(f),
            LargeObjRecvError::Auth { err } => err.fmt(f),
            LargeObjRecvError::FinishedPending { hash, id } => write!(
                f,
                concat!(
                    "received finished for ID {} ({}), ",
                    "but transfer is still pending"
                ),
                id, hash
            ),
            LargeObjRecvError::OutboundRecv { hash, id, err } => {
                write!(f, "error receiving for ID {} ({}): {}", id, hash, err)
            }
            LargeObjRecvError::InboundRecv { hash, id, err } => {
                write!(f, "error receiving for ID {} ({}): {}", id, hash, err)
            }
            LargeObjRecvError::NotFound { hash, id } => write!(
                f,
                "ID {} exists for {}, but no object entry found",
                id, hash
            ),
            LargeObjRecvError::NoHash { hash, id } => write!(
                f,
                "ID {} exists for {}, but no hash entry found",
                id, hash
            ),
            LargeObjRecvError::NoObj { id, hash } => write!(
                f,
                "ID {} exists for {}, but no object entry found",
                id, hash
            ),
            LargeObjRecvError::Collision => {
                write!(f, "id generator produced collision")
            }
            LargeObjRecvError::NoID => write!(f, "id generator exhausted"),
            LargeObjRecvError::AuthNFail => {
                write!(f, "message authentication failed")
            }
            LargeObjRecvError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}

impl<Encode> Display for LargeObjProtoAddOutboundError<Encode>
where
    Encode: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjProtoAddOutboundError::Encode { err } => err.fmt(f),
            LargeObjProtoAddOutboundError::NoIDs => write!(f, "IDs exhausted"),
            LargeObjProtoAddOutboundError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

#[cfg(test)]
use constellation_common::hashid::SHA3Algo;
#[cfg(test)]
use constellation_common::hashid::SHA3ID;

#[test]
fn test_encode_decode_metadata_offer() {
    let msg = LargeObjMetadata::Offer(LargeObjOffer {
        hash: vec![0xaa; 64],
        size: 0x31337,
        frag: LargeObjFragHeader {
            offset: 0x1337feeddeadbeef,
            len: 0x1234567890abcdef
        }
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_metadata_req_obj() {
    let msg = LargeObjMetadata::ReqObj(LargeObjReqObj {
        hash: vec![0xaa; 64],
        size: 0x31337,
        id: 0x1337feeddeadbeef
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_metadata_accept() {
    let msg = LargeObjMetadata::Accept(LargeObjAccept {
        hash: vec![0xaa; 64],
        size: 0x31337,
        id: 0x1337feeddeadbeef
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_metadata_frags() {
    let msg = LargeObjMetadata::Frags(LargeObjFrags {
        id: 0x1337feeddeadbeef,
        nfrags: 131
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_metadata_req() {
    let msg = LargeObjMetadata::Req(LargeObjReq {
        id: 0x1337feeddeadbeef,
        reqs: vec![
            LargeObjFragReq::Need(LargeObjFragRef {
                offset: 0x1337feeddeadbeef,
                len: 0x1234567890abcdef
            });
            64
        ]
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_metadata_finish() {
    let msg = LargeObjMetadata::Finish(LargeObjFinish {
        hash: vec![0xaa; 64],
        id: 0x1337feeddeadbeef
    });
    let mut codec = LargeObjMetadataPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_frag_header() {
    let msg = LargeObjFragHeader {
        offset: 0x1337feeddeadbeef,
        len: 0x1234567890abcdef
    };
    let mut codec = LargeObjFragHeaderPERCodec::default();
    let mut buf = [0; LARGE_OBJ_METADATA_SIZE];

    codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_offer_frag() {
    let algo = SHA3Algo::default();
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Offer {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        frag: LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req_obj() {
    let algo = SHA3Algo::default();
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::ReqObj {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        id: LargeObjID(0x1234567890abcdef)
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_accept() {
    let algo = SHA3Algo::default();
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Accept {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        id: LargeObjID(0x1234567890abcdef)
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_1_frag() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: LargeObjID(0x1234567890abcdef),
        frags: vec![LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_4_frags() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: LargeObjID(0x1234567890abcdef),
        frags: vec![
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 256]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 256]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 256]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 256]
            },
        ]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_16_frags() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: LargeObjID(0x1234567890abcdef),
        frags: vec![
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
        ]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Req {
        id: LargeObjID(0x1337feeddeadbeef),
        reqs: vec![
            LargeObjFragReq::Need(LargeObjFragRef {
                offset: 0x1337feeddeadbeef,
                len: 0x1234567890abcdef
            });
            64
        ]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_finish() {
    let algo = SHA3Algo::default();
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Finish {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        id: LargeObjID(0x1234567890abcdef)
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; <LargeObjMsgCodec<SHA3Algo> as DatagramCodec<
        LargeObjMsg<SHA3ID>
    >>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[cfg(test)]
use std::ops::Deref;

#[cfg(test)]
use constellation_auth::authn::test::TestAuthNMsgRecv;
#[cfg(test)]
use constellation_auth::authn::PassthruMsgAuthN;
#[cfg(test)]
use constellation_auth::cred::NullCred;
#[cfg(test)]
use constellation_common::codec::test::TestBytesCodec;

#[cfg(test)]
use crate::init;
#[cfg(test)]
use crate::large_obj::test::TestLargeObjMsgs;
#[cfg(test)]
use crate::large_obj::test::TestLargeObjProtoTypes;
#[cfg(test)]
use crate::stream::LargeObjStream;

#[cfg(test)]
struct TestStream {
    msgs: Vec<LargeObjMsg<SHA3ID>>
}

#[cfg(test)]
impl PushStreamReportError<Infallible> for TestStream {
    type ReportError = Infallible;

    fn report_error(
        &mut self,
        _error: &Infallible
    ) -> Result<(), Self::ReportError> {
        panic!("Should not call this")
    }
}

#[cfg(test)]
impl LargeObjStream<()> for TestStream {
    type Frags = OutboundFrags;
    type Parties = ();
    type PushFragError = Infallible;
    type PushFragRetry = Instant;

    fn push_frags(
        &mut self,
        _ctx: &mut (),
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        LargeObjMsg::frags(frags, id, 1024)
            .expect("Expected success")
            .map_ok(|res| match res {
                Some((msg, when)) => {
                    self.msgs.push(msg);

                    Ok((Some(when), ()))
                }
                None => Ok((None, ()))
            })
            .map(RetryIndefResult::from)
    }

    fn retry_push_frags(
        &mut self,
        _ctx: &mut (),
        _id: LargeObjID,
        _frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        panic!("Should not call this")
    }

    fn complete_push_frags(
        &mut self,
        _ctx: &mut (),
        _id: LargeObjID,
        _frags: &mut Self::Frags,
        _err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        panic!("Should not call this")
    }
}

#[cfg(test)]
impl LargeObjOfferStream<SHA3ID, ()> for TestStream {
    type PushOfferError = Infallible;
    type PushOfferRetry = Instant;

    fn push_offer(
        &mut self,
        _ctx: &mut (),
        id: SHA3ID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        LargeObjMsg::offer(frags, id, 1024)
            .expect("Expected success")
            .map_ok(|(msg, when)| {
                self.msgs.push(msg);

                Ok((Some(when), ()))
            })
            .map(RetryIndefResult::from)
    }

    fn retry_push_offer(
        &mut self,
        _ctx: &mut (),
        _id: SHA3ID,
        _frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        panic!("Should not call this")
    }

    fn complete_push_offer(
        &mut self,
        _ctx: &mut (),
        _id: SHA3ID,
        _frags: &mut Self::Frags,
        _err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        panic!("Should not call this")
    }
}

#[test]
fn test_offer_complete() {
    init();

    let msg = vec![0xaa; 512];
    let mut codec = TestBytesCodec;
    let msg = codec.encode_to_vec(&msg).expect("Expected success");
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg.clone()), None)];
    let sender_msgs = TestLargeObjMsgs::new(script);
    let sender_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut sender: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        sender_recv.clone(),
        sender_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");
    let mut sender_stream = TestStream { msgs: Vec::new() };
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let receiver_msgs = TestLargeObjMsgs::new(script);
    let receiver_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut receiver: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        receiver_recv.clone(),
        receiver_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");

    let (msgs, when) = PrivateMsgs::msgs(&mut sender, Instant::now())
        .expect("Expected success");

    assert!(when.is_none());
    assert!(msgs.is_none());
    assert!(sender_recv.msgs().is_empty());
    assert!(receiver_recv.msgs().is_empty());

    // Have sender generate offer.
    if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, Instant::now())
        .expect("Expected success")
    {
        let (when, ()) = res;

        assert!(when.is_some());
    } else {
        panic!("Expected success")
    };

    let recved = sender_stream.msgs.pop().expect("Expect some");

    assert!(sender_stream.msgs.is_empty());

    // Deliver offer to receiver.
    receiver
        .recv_msg(NullCred, recved)
        .expect("Expected success");

    // Check for complete message.
    assert_eq!(receiver_recv.msgs().deref(), &[msg]);

    // Have receiver generate accept.
    let (msgs, when) = PrivateMsgs::msgs(&mut receiver, Instant::now())
        .expect("Expected success");
    let mut msgs = msgs.expect("Expected some");
    let recved = msgs.pop().expect("Expected some");

    assert!(when.is_none());
    assert!(msgs.is_empty());
    assert!(sender_recv.msgs().is_empty());

    // Deliver to sender.
    sender.recv_msg(NullCred, recved).expect("Expected success");
}

#[test]
fn test_offer_complete_repeat() {
    init();

    let msg = vec![0xaa; 512];
    let mut codec = TestBytesCodec;
    let msg = codec.encode_to_vec(&msg).expect("Expected success");
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg.clone()), None)];
    let sender_msgs = TestLargeObjMsgs::new(script);
    let sender_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut sender: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        sender_recv.clone(),
        sender_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");
    let mut sender_stream = TestStream { msgs: Vec::new() };
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let receiver_msgs = TestLargeObjMsgs::new(script);
    let receiver_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut receiver: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        receiver_recv.clone(),
        receiver_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");

    let (msgs, when) = PrivateMsgs::msgs(&mut sender, Instant::now())
        .expect("Expected success");

    assert!(when.is_none());
    assert!(msgs.is_none());
    assert!(sender_recv.msgs().is_empty());
    assert!(receiver_recv.msgs().is_empty());

    // Have sender generate offer.
    let when = if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, Instant::now())
        .expect("Expected success")
    {
        let (when, ()) = res;

        when.expect("Expected some")
    } else {
        panic!("Expected success")
    };

    let recved_1 = sender_stream.msgs.pop().expect("Expect some");

    // Deliver to receiver.
    receiver
        .recv_msg(NullCred, recved_1)
        .expect("Expected success");

    // Have sender generate second offer.
    if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, when)
        .expect("Expected success")
    {
        let (when, ()) = res;

        assert!(when.is_some());
    } else {
        panic!("Expected success")
    };

    let recved_2 = sender_stream.msgs.pop().expect("Expect some");

    assert!(sender_stream.msgs.is_empty());

    // Deliver to receiver.
    receiver
        .recv_msg(NullCred, recved_2)
        .expect("Expected success");

    // Check for completed message.
    assert_eq!(receiver_recv.msgs().deref(), &[msg]);

    // Have receiver generate accept.
    let (msgs, when) = PrivateMsgs::msgs(&mut receiver, Instant::now())
        .expect("Expected success");
    let mut msgs = msgs.expect("Expected some");
    let recved = msgs.pop().expect("Expected some");

    assert!(when.is_none());
    assert!(msgs.is_empty());
    assert!(sender_recv.msgs().is_empty());

    // Deliver to sender.
    sender.recv_msg(NullCred, recved).expect("Expected success");
}

#[test]
fn test_offer_complete_repeat_multi_finish() {
    init();

    let msg = vec![0xaa; 512];
    let mut codec = TestBytesCodec;
    let msg = codec.encode_to_vec(&msg).expect("Expected success");
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg.clone()), None)];
    let sender_msgs = TestLargeObjMsgs::new(script);
    let sender_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut sender: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        sender_recv.clone(),
        sender_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");
    let mut sender_stream = TestStream { msgs: Vec::new() };
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(None, None), (None, None)];
    let receiver_msgs = TestLargeObjMsgs::new(script);
    let receiver_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut receiver: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        receiver_recv.clone(),
        receiver_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");

    let (msgs, when) = PrivateMsgs::msgs(&mut sender, Instant::now())
        .expect("Expected success");

    assert!(when.is_none());
    assert!(msgs.is_none());
    assert!(sender_recv.msgs().is_empty());
    assert!(receiver_recv.msgs().is_empty());

    // Have sender generate offer.
    let when = if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, Instant::now())
        .expect("Expected success")
    {
        let (when, ()) = res;

        when.expect("Expected some")
    } else {
        panic!("Expected success")
    };

    let recved_1 = sender_stream.msgs.pop().expect("Expect some");

    // Have sender generate second offer.
    if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, when)
        .expect("Expected success")
    {
        let (when, ()) = res;

        assert!(when.is_some());
    } else {
        panic!("Expected success")
    };

    let recved_2 = sender_stream.msgs.pop().expect("Expect some");

    assert!(sender_stream.msgs.is_empty());

    // Deliver offer to receiver.
    receiver
        .recv_msg(NullCred, recved_2)
        .expect("Expected success");

    // Check for complete message.
    assert_eq!(receiver_recv.msgs().deref(), &[msg]);

    // Have receiver generate accept.
    let (msgs, when) = PrivateMsgs::msgs(&mut receiver, Instant::now())
        .expect("Expected success");
    let mut msgs = msgs.expect("Expected some");
    let recved = msgs.pop().expect("Expected some");

    assert!(when.is_none());
    assert!(msgs.is_empty());
    assert!(sender_recv.msgs().is_empty());

    // Deliver accept to sender.
    sender.recv_msg(NullCred, recved).expect("Expected success");

    // Deliver second offer to receiver.
    receiver
        .recv_msg(NullCred, recved_1)
        .expect("Expected success");

    // Have receiver generate second accept.
    let (msgs, when) = PrivateMsgs::msgs(&mut receiver, Instant::now())
        .expect("Expected success");
    let mut msgs = msgs.expect("Expected some");
    let recved = msgs.pop().expect("Expected some");

    assert!(when.is_none());
    assert!(msgs.is_empty());
    assert!(sender_recv.msgs().is_empty());

    // Deliver second accept to sender.
    sender.recv_msg(NullCred, recved).expect("Expected success");
}

#[test]
fn test_long_offer_complete_repeat() {
    init();

    let msg = vec![0xaa; 1536];
    let mut codec = TestBytesCodec;
    let msg = codec.encode_to_vec(&msg).expect("Expected success");
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg.clone()), None)];
    let sender_msgs = TestLargeObjMsgs::new(script);
    let sender_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut sender: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        sender_recv.clone(),
        sender_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");
    let mut sender_stream = TestStream { msgs: Vec::new() };
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let receiver_msgs = TestLargeObjMsgs::new(script);
    let receiver_recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let mut receiver: LargeObjProto<
        _,
        _,
        (),
        OutboundFrags,
        TestLargeObjProtoTypes<_>
    > = LargeObjProto::create(
        LargeObjProtoConfig::default(),
        Notify::new(),
        receiver_recv.clone(),
        receiver_msgs,
        PassthruMsgAuthN::default(),
        SHA3Algo::default()
    )
    .expect("Expected success");

    let (msgs, when) = PrivateMsgs::msgs(&mut sender, Instant::now())
        .expect("Expected success");

    assert!(when.is_none());
    assert!(msgs.is_none());
    assert!(sender_recv.msgs().is_empty());
    assert!(receiver_recv.msgs().is_empty());

    // Have sender generate offer.
    let when = if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, Instant::now())
        .expect("Expected success")
    {
        let (when, ()) = res;

        when.expect("Expected some")
    } else {
        panic!("Expected success")
    };

    let recved_1 = sender_stream.msgs.pop().expect("Expect some");

    // Deliver to receiver.
    receiver
        .recv_msg(NullCred, recved_1)
        .expect("Expected success");

    // Have sender generate second offer.
    if let RetryIndefResult::Success(res) = sender
        .try_push(&mut (), &mut sender_stream, when)
        .expect("Expected success")
    {
        let (when, ()) = res;

        assert!(when.is_some());
    } else {
        panic!("Expected success")
    };

    let recved_2 = sender_stream.msgs.pop().expect("Expect some");

    assert!(sender_stream.msgs.is_empty());

    // Deliver to receiver.
    receiver
        .recv_msg(NullCred, recved_2)
        .expect("Expected success");

    // Check for complete message.
    assert_eq!(receiver_recv.msgs().deref(), &[msg]);

    // Have receiver generate accept.
    let (msgs, when) = PrivateMsgs::msgs(&mut receiver, Instant::now())
        .expect("Expected success");
    let mut msgs = msgs.expect("Expected some");
    let recved = msgs.pop().expect("Expected some");

    assert!(when.is_none());
    assert!(msgs.is_empty());
    assert!(sender_recv.msgs().is_empty());

    // Deliver accept to sender.
    sender.recv_msg(NullCred, recved).expect("Expected success");
}
