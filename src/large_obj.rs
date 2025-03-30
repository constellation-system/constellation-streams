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

//! Large-object transfer protocol.

use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNResult;
use constellation_auth::authn::MsgAuthN;
use constellation_common::codec::per::PERCodec;
use constellation_common::codec::Codec;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::net::PrivateMsgs;
use constellation_common::net::SharedMsgs;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::debug;
use log::error;
use log::trace;

use crate::error::BatchError;
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
use crate::stream::LargeObjStream;
use crate::stream::PushStreamReportError;

const LARGE_OBJ_METADATA_SIZE: usize = 1171;
const LARGE_OBJ_METADATA_BITS: usize = LARGE_OBJ_METADATA_SIZE * 8;

const LARGE_OBJ_FRAG_HEADER_SIZE: usize = 18;
const LARGE_OBJ_FRAG_HEADER_BITS: usize = LARGE_OBJ_FRAG_HEADER_SIZE * 8;

pub type LargeObjFragHeaderPERCodec =
    PERCodec<LargeObjFragHeader, LARGE_OBJ_FRAG_HEADER_BITS>;

pub type LargeObjMetadataPERCodec =
    PERCodec<LargeObjMetadata, LARGE_OBJ_METADATA_BITS>;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LargeObjID(u64);

#[derive(Clone)]
pub struct LargeObjMsgCodec<H>
where
    H: HashAlgo {
    frag_header: LargeObjFragHeaderPERCodec,
    metadata: LargeObjMetadataPERCodec,
    hash: H
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct LargeObjFrag {
    offset: u64,
    data: Vec<u8>
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum LargeObjMsg<H>
where
    H: HashID {
    Offer {
        hash: H,
        size: u64,
        frag: LargeObjFrag
    },
    Accept {
        hash: H,
        size: u64,
        id: LargeObjID
    },
    ReqObj {
        hash: H,
        size: u64,
        id: LargeObjID
    },
    Frags {
        id: LargeObjID,
        frags: Vec<LargeObjFrag>
    },
    Req {
        id: LargeObjID,
        reqs: Vec<LargeObjFragReq>
    },
    Finish {
        id: LargeObjID
    }
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
        size: usize,
        accept: bool
    }
}

struct RecvEntry<H> {
    frags: InboundFragsState,
    hash: H
}

struct SendEntry<F>
where
    F: Frags {
    id: LargeObjID,
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

pub struct LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    Auth: MsgAuthN<Msg, Wrapper>,
    Codec: DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags {
    wrapper: PhantomData<Wrapper>,
    msg: PhantomData<Msg>,
    inbound: Arc<Mutex<LargeObjInbound<H, Auth::SessionPrin>>>,
    outbound: Arc<Mutex<LargeObjOutbound<H, F>>>,
    parties: Arc<RwLock<HashMap<Auth::SessionPrin, PartyID>>>,
    upstream: Recv,
    retry: Retry,
    codec: Codec,
    auth: Auth,
    ids: IDs
}

impl<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F> Clone
    for LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: Clone + AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: Clone + IDGen + Iterator<Item = LargeObjID>,
    Auth: Clone + MsgAuthN<Msg, Wrapper>,
    Codec: Clone + DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags
{
    fn clone(&self) -> Self {
        LargeObjProto {
            wrapper: self.wrapper,
            msg: self.msg,
            inbound: self.inbound.clone(),
            outbound: self.outbound.clone(),
            upstream: self.upstream.clone(),
            parties: self.parties.clone(),
            retry: self.retry.clone(),
            codec: self.codec.clone(),
            auth: self.auth.clone(),
            ids: self.ids.clone()
        }
    }
}

pub struct LargeObjPushFragsRetry<Retry> {
    retry: Retry,
    id: LargeObjID
}

pub enum LargeObjPushFragsError<H, Frags> {
    PushFrags { err: Frags },
    NoObj { hash: H, id: LargeObjID },
    MutexPoison
}

pub enum LargeObjSendError<H, Prin> {
    NoObj { hash: H, id: LargeObjID },
    NoPrin { prin: Prin },
    MutexPoison
}

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
pub enum LargeObjMsgEncodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec as Codec<LargeObjMetadata>>::EncodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec as Codec<LargeObjFragHeader>>::EncodeError
    },
    TooShort
}

#[derive(Debug)]
pub enum LargeObjMsgDecodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec as Codec<LargeObjMetadata>>::EncodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec as Codec<LargeObjFragHeader>>::EncodeError
    },
    Hash {
        err: TryFromSliceError
    },
    TooShort
}

pub enum LargeObjDataError {
    Frags { err: OutboundDataError },
    OutOfBounds
}

impl From<usize> for LargeObjID {
    #[inline]
    fn from(val: usize) -> LargeObjID {
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

impl<Retry> LargeObjPushFragsRetry<Retry> {
    #[inline]
    pub(crate) fn take(self) -> (Retry, LargeObjID) {
        (self.retry, self.id)
    }
}

impl<Retry> RetryWhen for LargeObjPushFragsRetry<Retry>
where
    Retry: RetryWhen
{
    #[inline]
    fn when(&self) -> Instant {
        self.retry.when()
    }
}

impl LargeObjFrag {
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
    pub fn finish(id: LargeObjID) -> Self {
        LargeObjMsg::Finish { id: id }
    }
}

impl<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
    SharedMsgs<PartyID, LargeObjMsg<H>>
    for LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    Auth: MsgAuthN<Msg, Wrapper>,
    Codec: DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags
{
    type MsgsError = LargeObjSendError<H, Auth::SessionPrin>;

    fn msgs(
        &mut self
    ) -> Result<
        (
            Option<Vec<(Vec<PartyID>, Vec<LargeObjMsg<H>>)>>,
            Option<Instant>
        ),
        Self::MsgsError
    > {
        debug!(target: "large-obj-proto",
               "collecting outbound messages");

        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjSendError::MutexPoison)?;
        let size = inbound.hashes.len();
        let now = Instant::now();
        let mut msgs = Vec::with_capacity(size);
        let mut deletes = Vec::with_capacity(size);
        let mut next = None;
        let hashes: Vec<((Auth::SessionPrin, H), IDs::Item)> = inbound
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
                    next = Some(
                        next.map_or(retry, |next: Instant| next.min(retry))
                    );
                    msgs.push((vec![party_id], vec![msg]));
                }
                InboundFragsState::Active { frags, .. } => {
                    let mut buf = [(false, 0, 0); 16];

                    match frags.reqs_acks(&mut buf[..], &self.retry) {
                        RetryResult::Success((n, retry)) => {
                            let iter = buf[..n].iter().cloned();
                            let msg = LargeObjMsg::reqs(id, iter);

                            msgs.push((vec![party_id], vec![msg]));
                            next = next.map_or(retry, |next| {
                                retry.map(|retry| next.min(retry))
                            });
                        }
                        RetryResult::Retry(retry) => {
                            next = Some(
                                next.map_or(retry, |next| next.min(retry))
                            );
                        }
                    }
                }
                InboundFragsState::Finished { size, accept } => {
                    let msg = if *accept {
                        LargeObjMsg::accept(ent.hash.clone(), *size, id)
                    } else {
                        LargeObjMsg::finish(id)
                    };

                    msgs.push((vec![party_id], vec![msg]));
                    // XXX keep these around for a configurable amount
                    // of time as "tombstones".
                    deletes.push((prin.clone(), hash.clone()));
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

impl<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
    PrivateMsgs<LargeObjMsg<H>>
    for LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    IDs::Item: Clone + Default + Display + Eq + Hash + Into<u64>,
    Auth: MsgAuthN<Msg, Wrapper>,
    Codec: DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags
{
    type MsgsError = LargeObjSendError<H, Auth::SessionPrin>;

    fn msgs(
        &mut self
    ) -> Result<(Option<Vec<LargeObjMsg<H>>>, Option<Instant>), Self::MsgsError>
    {
        debug!(target: "large-obj-proto",
               "collecting outbound messages");

        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjSendError::MutexPoison)?;
        let size = inbound.hashes.len();
        let now = Instant::now();
        let mut msgs = Vec::with_capacity(size);
        let mut deletes = Vec::with_capacity(size);
        let mut next = None;
        let hashes: Vec<((Auth::SessionPrin, H), IDs::Item)> = inbound
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
                    next = Some(
                        next.map_or(retry, |next: Instant| next.min(retry))
                    );
                    msgs.push(msg);
                }
                InboundFragsState::Active { frags, .. } => {
                    let mut buf = [(false, 0, 0); 16];

                    match frags.reqs_acks(&mut buf[..], &self.retry) {
                        RetryResult::Success((n, retry)) => {
                            let iter = buf[..n].iter().cloned();
                            let msg = LargeObjMsg::reqs(id, iter);

                            msgs.push(msg);
                            next = next.map_or(retry, |next| {
                                retry.map(|retry| next.min(retry))
                            });
                        }
                        RetryResult::Retry(retry) => {
                            next = Some(
                                next.map_or(retry, |next| next.min(retry))
                            );
                        }
                    }
                }
                InboundFragsState::Finished { size, accept } => {
                    let msg = if *accept {
                        LargeObjMsg::accept(ent.hash.clone(), *size, id)
                    } else {
                        LargeObjMsg::finish(id)
                    };

                    msgs.push(msg);
                    // XXX keep these around for a configurable amount
                    // of time as "tombstones".
                    deletes.push((prin.clone(), hash.clone()));
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

impl<H> Codec<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where
    H: HashAlgo + Default
{
    type CreateError = Infallible;
    type DecodeError = LargeObjMsgDecodeError;
    type EncodeError = LargeObjMsgEncodeError;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(Self::default())
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
            LargeObjMsg::Finish { id } => {
                let msg = LargeObjMetadata::Finish(LargeObjFinish {
                    id: id.clone().into()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
        }
    }

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
            LargeObjMetadata::Finish(LargeObjFinish { id }) => {
                Ok((LargeObjMsg::Finish { id: id.into() }, curr))
            }
        }
    }
}

impl<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
    LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    IDs::Item: Clone + Default + Display + Eq + Hash + Into<u64>,
    Auth: MsgAuthN<Msg, Wrapper>,
    Codec: DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags
{
    pub(crate) fn try_push_frags<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> Result<
        RetryResult<
            Option<Instant>,
            LargeObjPushFragsRetry<Stream::PushFragRetry>
        >,
        LargeObjPushFragsError<
            H,
            <Stream::PushFragError as BatchError>::Permanent
        >
    >
    where
        Stream: LargeObjStream<LargeObjID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as BatchError>::Permanent
            > {
        let res = {
            let mut outbound = self
                .outbound
                .lock()
                .map_err(|_| LargeObjPushFragsError::MutexPoison)?;

            if !outbound.objs.is_empty() {
                // XXX use a better data structure here.
                let mut ents: Vec<&mut SendEntry<F>> =
                    outbound.objs.values_mut().collect();

                ents.sort_unstable_by(|a, b| match (a.when, b.when) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Greater,
                    (_, None) => Ordering::Less
                });

                let id = ents[0].id.clone();

                match stream.push_frags(ctx, id.clone(), &mut ents[0].frags) {
                    Ok(RetryResult::Success(retry)) => {
                        ents[0].when = retry;

                        if ents.len() < 2 {
                            Ok(RetryResult::Success(retry))
                        } else {
                            let when = ents[1].when.map_or(retry, |when| {
                                retry.map(|retry| when.min(retry))
                            });

                            Ok(RetryResult::Success(when))
                        }
                    }
                    Ok(RetryResult::Retry(retry)) => {
                        Ok(RetryResult::Retry(LargeObjPushFragsRetry {
                            retry: retry,
                            id: id.clone()
                        }))
                    }
                    Err(err) => Err((id, err))
                }
            } else {
                Ok(RetryResult::Success(None))
            }
        };

        match res {
            // It succeeded.
            Ok(out) => Ok(out),
            Err((id, err)) => self
                .complete_push_frags(ctx, stream, id.clone(), err)
                .map(|res| {
                    res.map_retry(|retry| LargeObjPushFragsRetry {
                        retry: retry,
                        id: id
                    })
                })
        }
    }

    pub(crate) fn retry_push_frags<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        id: IDs::Item,
        retry: Stream::PushFragRetry
    ) -> Result<
        RetryResult<Option<Instant>, Stream::PushFragRetry>,
        LargeObjPushFragsError<
            H,
            <Stream::PushFragError as BatchError>::Permanent
        >
    >
    where
        Stream: LargeObjStream<LargeObjID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as BatchError>::Permanent
            >,
        IDs::Item: Into<usize> {
        let res = {
            let mut outbound = self
                .outbound
                .lock()
                .map_err(|_| LargeObjPushFragsError::MutexPoison)?;

            match outbound.hashes.get(&id).cloned() {
                Some(hash) => match outbound.objs.get_mut(&hash) {
                    Some(ent) => stream.retry_push_frags(
                        ctx,
                        id.clone(),
                        &mut ent.frags,
                        retry
                    ),
                    None => {
                        return Err(LargeObjPushFragsError::NoObj {
                            hash: hash,
                            id: id.clone()
                        })
                    }
                },
                None => {
                    trace!(target: "large-obj-proto",
                           "stray frags for {}",
                           id);

                    Ok(RetryResult::Success(None))
                }
            }
        };

        match res {
            // It succeeded.
            Ok(out) => Ok(out),
            Err(err) => self.complete_push_frags(ctx, stream, id, err)
        }
    }

    pub(crate) fn complete_push_frags<Stream, Ctx>(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        id: IDs::Item,
        err: Stream::PushFragError
    ) -> Result<
        RetryResult<Option<Instant>, Stream::PushFragRetry>,
        LargeObjPushFragsError<
            H,
            <Stream::PushFragError as BatchError>::Permanent
        >
    >
    where
        Stream: LargeObjStream<LargeObjID, Ctx, Frags = F>
            + PushStreamReportError<
                <Stream::PushFragError as BatchError>::Permanent
            >,
        IDs::Item: Into<usize> {
        let res = match err.split() {
            (Some(completable), None) => {
                let mut outbound = self
                    .outbound
                    .lock()
                    .map_err(|_| LargeObjPushFragsError::MutexPoison)?;

                match outbound.hashes.get(&id).cloned() {
                    Some(hash) => match outbound.objs.get_mut(&hash) {
                        Some(ent) => stream.complete_push_frags(
                            ctx,
                            id.clone(),
                            &mut ent.frags,
                            completable
                        ),
                        None => {
                            return Err(LargeObjPushFragsError::NoObj {
                                hash: hash,
                                id: id.clone()
                            })
                        }
                    },
                    None => {
                        trace!(target: "large-obj-proto",
                               "stray frags for {}",
                               id);

                        Ok(RetryResult::Success(None))
                    }
                }
            }
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred.
                error!(target: "large-obj-entry",
                       "unrecoverable error pushing fragments: {}",
                       permanent);

                // Report the failure
                if let Err(err) = stream.report_error(&permanent) {
                    error!(target: "large-obj-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Ok(RetryResult::Success(None))
            }
            (None, None) => {
                error!(target: "large-obj-entry",
                       "neither completable nor permanent errors reported");

                Ok(RetryResult::Success(None))
            }
        };

        match res {
            // It succeeded.
            Ok(out) => Ok(out),
            Err(err) => self.complete_push_frags(ctx, stream, id, err)
        }
    }

    fn recv_offer_msg(
        &mut self,
        prin: &Auth::SessionPrin,
        hash: H,
        size: u64,
        frag: LargeObjFrag
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        match inbound.hashes.entry((prin.clone(), hash.clone())) {
            Entry::Occupied(ent) => {
                // ID already exists, get the entry.
                let id = ent.get().clone();
                let RecvEntry { frags, hash, .. } =
                    inbound.objs.get_mut(&id).ok_or(
                        LargeObjRecvError::NotFound {
                            hash: hash,
                            id: id.clone()
                        }
                    )?;
                // Check if we're still receiving fragments.
                let closeout =
                    if let InboundFragsState::Active { frags, .. } = frags {
                        // Receive the fragment.
                        frags
                            .recv(frag.offset() as usize, frag.data())
                            .map_err(|err| LargeObjRecvError::InboundRecv {
                                hash: hash.clone(),
                                id: id.clone(),
                                err: err
                            })?;

                        frags.is_finished()
                    } else {
                        // This is ok, it can happen due to delayed
                        // messages.
                        trace!(target: "large-obj-proto",
                           "redundant offer message for ID {} ({})",
                           id, hash);

                        false
                    };

                // Check if the entry is finished and
                // report if it is.
                if closeout {
                    debug!(target: "large-obj-proto",
                           "finished transfer for ID {}",
                           id);

                    let finished = InboundFragsState::Finished {
                        size: frags.size(),
                        accept: false
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
            Entry::Vacant(ent) => {
                // No entry for this hash exists, set one up.
                let id = self.ids.next().ok_or(LargeObjRecvError::NoID)?;
                let size = size as usize;

                ent.insert(id.clone());

                debug!(target: "large-obj-proto",
                       "creating new transfer for {} with ID {}",
                       hash, id);

                // See if the offer provides all the data.
                let ent = if frag.offset() == 0 && frag.len() == size {
                    trace!(target: "large-obj-proto",
                           "offer message provides entire object");

                    // Complete the message and report it upstream.

                    RecvEntry {
                        frags: InboundFragsState::Finished {
                            accept: true,
                            size: size
                        },
                        hash: hash
                    }
                } else {
                    trace!(target: "large-obj-proto",
                           "offer message provides partial object");

                    let frags = InboundFrags::new(size);

                    RecvEntry {
                        frags: InboundFragsState::Active {
                            req: Some(ReqState {
                                when: Instant::now(),
                                nretries: 0
                            }),
                            frags: frags
                        },
                        hash: hash
                    }
                };

                // Error if an entry already exists under this ID.
                if inbound.objs.insert(id, ent).is_none() {
                    Ok(None)
                } else {
                    Err(LargeObjRecvError::Collision)
                }
            }
        }
    }

    fn recv_frags_msg(
        &mut self,
        id: IDs::Item,
        recv: Vec<LargeObjFrag>
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut inbound = self
            .inbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        match inbound.objs.get_mut(&id) {
            Some(RecvEntry { frags, hash, .. }) => {
                let closeout = if let InboundFragsState::Active { frags, req } =
                    frags
                {
                    debug!(target: "large-obj-proto",
                           "received finished acknowledgement for ID {}",
                           id);

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
                        accept: false
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
            None => {
                trace!(target: "large-obj-proto",
                       "fragments message for non-existent ID {}",
                       id);

                Ok(None)
            }
        }
    }

    fn recv_accept_msg(
        &mut self,
        hash: H,
        id: IDs::Item
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

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

    fn recv_req_obj_msg(
        &mut self,
        hash: H,
        id: IDs::Item
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        if outbound.objs.contains_key(&hash) {
            match outbound.hashes.entry(id.clone()) {
                Entry::Occupied(_) => {
                    trace!(target: "large-obj-proto",
                           "redundant object request for {}",
                           hash);

                    Ok(None)
                }
                Entry::Vacant(ent) => {
                    debug!(target: "large-obj-proto",
                           "received object request for {} (ID {})",
                           hash, id);

                    ent.insert(hash);

                    Ok(None)
                }
            }
        } else {
            trace!(target: "large-obj-proto",
                   "stray object request for {}",
                   hash);

            Ok(None)
        }
    }

    fn recv_reqs_msg(
        &mut self,
        id: IDs::Item,
        reqs: Vec<LargeObjFragReq>
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        match outbound.hashes.get(&id).cloned() {
            Some(hash) => match outbound.objs.get_mut(&hash) {
                Some(ent) => {
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

                    Ok(None)
                }
                None => Err(LargeObjRecvError::NoObj {
                    hash: hash,
                    id: id.clone()
                })
            },
            None => {
                trace!(target: "large-obj-proto",
                       "stray frags for {}",
                       id);

                Ok(None)
            }
        }
    }

    fn recv_finish_msg(
        &mut self,
        id: IDs::Item
    ) -> Result<
        Option<Vec<u8>>,
        LargeObjRecvError<
            H,
            Auth::Error,
            Codec::DecodeError,
            Recv::RecvError,
            F::RecvReqError
        >
    > {
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| LargeObjRecvError::MutexPoison)?;

        match outbound.hashes.remove(&id) {
            Some(hash) => match outbound.objs.remove(&hash) {
                Some(SendEntry { .. }) => {
                    trace!(target: "large-obj-proto",
                           "removed transfer entries for {} ({})",
                           id, hash);

                    Ok(None)
                }
                None => Err(LargeObjRecvError::NotFound { hash: hash, id: id })
            },
            None => {
                trace!(target: "large-obj-proto",
                       "redundant finish for ID {}",
                       id);

                Ok(None)
            }
        }
    }
}

impl<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
    AuthNMsgRecv<Auth::SessionPrin, LargeObjMsg<H>>
    for LargeObjProto<H, Msg, Wrapper, Auth, PartyID, Codec, IDs, Recv, F>
where
    Recv: AuthNMsgRecv<Auth::Prin, Msg>,
    IDs: IDGen + Iterator<Item = LargeObjID>,
    IDs::Item: Clone + Default + Display + Eq + Hash + Into<u64>,
    Auth: MsgAuthN<Msg, Wrapper>,
    Codec: DatagramCodec<Wrapper>,
    H: Clone + Display + Hash + HashID + Eq,
    PartyID: Clone,
    F: Frags
{
    type RecvError = LargeObjRecvError<
        H,
        Auth::Error,
        Codec::DecodeError,
        Recv::RecvError,
        F::RecvReqError
    >;

    fn recv_auth_msg(
        &mut self,
        prin: &Auth::SessionPrin,
        msg: LargeObjMsg<H>
    ) -> Result<(), Self::RecvError> {
        let data = match msg {
            // Inbound messages.
            LargeObjMsg::Offer { hash, size, frag } => {
                self.recv_offer_msg(prin, hash, size, frag)
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
            LargeObjMsg::Finish { id } => self.recv_finish_msg(id)
        }?;

        // Complete the message and send it upstream.
        if let Some(data) = data {
            debug!(target: "large-obj-proto",
                   "processsing complete message");

            // Decode the complete message.
            let (wrapper, _) = self
                .codec
                .decode(&data)
                .map_err(|err| LargeObjRecvError::Decode { err: err })?;

            // Authenticate the complete message.
            match self
                .auth
                .msg_authn(prin, wrapper)
                .map_err(|err| LargeObjRecvError::Auth { err: err })?
            {
                AuthNResult::Accept((prin, msg)) => {
                    // Send it upstream.
                    self.upstream.recv_auth_msg(&prin, msg).map_err(|err| {
                        LargeObjRecvError::Upstream { err: err }
                    })?;
                }
                AuthNResult::Reject => return Err(LargeObjRecvError::AuthNFail)
            }
        }

        Ok(())
    }
}

impl BatchError for LargeObjMsgEncodeError {
    type Completable = Infallible;
    type Permanent = Self;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        (None, Some(self))
    }
}

impl<H> DatagramCodec<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where H: Default + HashAlgo {
    const MAX_BYTES: usize = 1286;
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

impl<H, Frags> ScopedError for LargeObjPushFragsError<H, Frags>
where
    H: Clone + Display + Hash + HashID + Eq,
    Frags: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjPushFragsError::PushFrags { err } => err.scope(),
            LargeObjPushFragsError::NoObj { .. } |
            LargeObjPushFragsError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<H, Prin> ScopedError for LargeObjSendError<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq
{
    fn scope(&self) -> ErrorScope {
        match self {
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
            LargeObjDataError::Frags { err } => err.fmt(f),
            LargeObjDataError::OutOfBounds => {
                write!(f, "offered data is outside available data range")
            }
        }
    }
}

impl Display for LargeObjMsgEncodeError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjMsgEncodeError::Metadata { err } => err.fmt(f),
            LargeObjMsgEncodeError::FragHeader { err } => err.fmt(f),
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
            LargeObjMsgDecodeError::Metadata { err } => err.fmt(f),
            LargeObjMsgDecodeError::FragHeader { err } => err.fmt(f),
            LargeObjMsgDecodeError::Hash { err } => err.fmt(f),
            LargeObjMsgDecodeError::TooShort => {
                write!(f, "input buffer is too short")
            }
        }
    }
}

impl<H, Frags> Display for LargeObjPushFragsError<H, Frags>
where
    H: Clone + Display + Hash + HashID + Eq,
    Frags: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjPushFragsError::PushFrags { err } => err.fmt(f),
            LargeObjPushFragsError::NoObj { hash, id } => write!(
                f,
                "ID {} exists for {}, but no object entry found",
                id, hash
            ),
            LargeObjPushFragsError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}

impl<H, Prin> Display for LargeObjSendError<H, Prin>
where
    H: Clone + Display + Hash + HashID + Eq,
    Prin: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
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
fn test_encode_decode_msg_accopt() {
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
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Finish {
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
