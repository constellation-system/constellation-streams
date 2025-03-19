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

//! User-facing types for the large-object transfer protocol.

use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
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
use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;
use log::debug;
use log::error;
use log::trace;

use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::frags::InboundFrags;
use crate::frags::InboundRecvError;
use crate::frags::OutboundDataError;
use crate::frags::OutboundFrags;
use crate::frags::OutboundRecvError;
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

const LARGE_OBJ_METADATA_SIZE: usize = 1171;
const LARGE_OBJ_METADATA_BITS: usize = LARGE_OBJ_METADATA_SIZE * 8;

const LARGE_OBJ_FRAG_HEADER_SIZE: usize = 18;
const LARGE_OBJ_FRAG_HEADER_BITS: usize = LARGE_OBJ_FRAG_HEADER_SIZE * 8;

pub type LargeObjFragHeaderPERCodec =
    PERCodec<LargeObjFragHeader, LARGE_OBJ_FRAG_HEADER_BITS>;

pub type LargeObjMetadataPERCodec =
    PERCodec<LargeObjMetadata, LARGE_OBJ_METADATA_BITS>;

#[derive(Clone)]
pub struct LargeObjMsgCodec<H>
where H: HashAlgo {
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
pub enum LargeObjMsg<ID>
where ID: HashID {
    Offer {
        hash: ID,
        size: u64,
        frag: LargeObjFrag
    },
    Accept {
        hash: ID,
        size: u64,
        id: u64
    },
    ReqObj {
        hash: ID,
        size: u64,
        id: u64
    },
    Frags {
        id: u64,
        frags: Vec<LargeObjFrag>
    },
    Req {
        id: u64,
        reqs: Vec<LargeObjFragReq>
    },
    Finish {
        id: u64
    }
}

struct RecvEntry<H> {
    frags: Option<InboundFrags>,
    when: Option<Instant>,
    req: bool,
    hash: H,
}

struct SendEntry {
    frags: OutboundFrags,
    when: Option<Instant>,
}

pub struct LargeObjProto<H, Msg, Wrapper, Auth, Codec, IDs, Recv>
where Recv: AuthNMsgRecv<Auth::Prin, Msg>,
      IDs: IDGen + Iterator<Item = u64>,
      Auth: MsgAuthN<Msg, Wrapper>,
      Codec: DatagramCodec<Wrapper>,
      H: Clone + Display + Hash + HashID + Eq {
    wrapper: PhantomData<Wrapper>,
    msg: PhantomData<Msg>,
    inbound_objs: HashMap<u64, RecvEntry<H>>,
    inbound_hashes: HashMap<(Auth::SessionPrin, H), u64>,
    outbound_objs: HashMap<H, SendEntry>,
    outbound_hashes: HashMap<u64, H>,
    upstream: Recv,
    retry: Retry,
    codec: Codec,
    auth: Auth,
    ids: IDs
}

pub enum LargeObjRecvError<H, Auth, Decode, Upstream> {
    Upstream {
        err: Upstream,
    },
    Decode {
        err: Decode,
    },
    Auth {
        err: Auth,
    },
    OutboundRecv {
        hash: H,
        id: u64,
        err: OutboundRecvError
    },
    InboundRecv {
        hash: H,
        id: u64,
        err: InboundRecvError
    },
    FinishedPending {
        hash: H,
        id: u64
    },
    NotFound {
        hash: H,
        id: u64
    },
    NoHash {
        hash: H,
        id: u64
    },
    NoObj {
        hash: H,
        id: u64
    },
    Collision,
    AuthNFail,
    NoID
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

impl<ID> LargeObjMsg<ID>
where ID: HashID {
    /// Maximum number of bytes that can be sent with a datagram.
    pub const LARGE_OBJ_DATAGRAM_MAX_DATA: usize = 1024;

    #[inline]
    pub fn offer(
        frags: &mut OutboundFrags,
        hash: ID,
        max_bytes: usize
    ) -> Result<RetryResult<Self>, LargeObjDataError> {
        frags
            .offer_frag(max_bytes)
            .map_err(|err| LargeObjDataError::Frags { err: err })?
            .map_ok(|(offset, len)| match frags.data(offset, len) {
                Ok(data) => Ok(LargeObjMsg::Offer {
                    hash: hash,
                    size: frags.len() as u64,
                    frag: LargeObjFrag {
                        offset: offset as u64,
                        data: data.to_vec()
                    }
                }),
                Err(_) => Err(LargeObjDataError::OutOfBounds)
            })
    }

    #[inline]
    pub fn accept(
        hash: ID,
        size: usize,
        id: usize
    ) -> Self {
        LargeObjMsg::Accept {
            hash: hash,
            size: size as u64,
            id: id as u64
        }
    }

    #[inline]
    pub fn req_obj(
        hash: ID,
        size: usize,
        id: usize
    ) -> Self {
        LargeObjMsg::ReqObj {
            hash: hash,
            size: size as u64,
            id: id as u64
        }
    }

    pub fn frags(
        frags: &mut OutboundFrags,
        id: usize,
        max_bytes: usize
    ) -> Result<RetryResult<Self>, LargeObjDataError> {
        let mut buf = [(0, 0); 16];

        frags
            .data_frags(&mut buf, max_bytes)
            .map_err(|err| LargeObjDataError::Frags { err: err })?
            .map_ok(|nmsgs| {
                let mut fragbuf = Vec::with_capacity(nmsgs);

                for (offset, len) in buf.iter().take(nmsgs) {
                    match frags.data(*offset, *len) {
                        Ok(data) => fragbuf.push(LargeObjFrag {
                            offset: *offset as u64,
                            data: data.to_vec()
                        }),
                        Err(_) => return Err(LargeObjDataError::OutOfBounds)
                    }
                }

                Ok(LargeObjMsg::Frags {
                    id: id as u64,
                    frags: fragbuf
                })
            })
    }

    #[inline]
    pub fn reqs<I>(
        id: usize,
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

        LargeObjMsg::Req {
            id: id as u64,
            reqs: reqs
        }
    }

    #[inline]
    pub fn finish(id: usize) -> Self {
        LargeObjMsg::Finish { id: id as u64 }
    }
}

impl<H> Codec<LargeObjMsg<H::HashID>> for LargeObjMsgCodec<H>
where H: HashAlgo + Default {
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
                    id: *id
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::ReqObj { hash, size, id } => {
                let msg = LargeObjMetadata::ReqObj(LargeObjReqObj {
                    hash: hash.bytes().to_vec(),
                    size: *size,
                    id: *id
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::Frags { id, frags } => {
                let metadata = LargeObjMetadata::Frags(LargeObjFrags {
                    id: *id,
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
                    id: *id,
                    reqs: reqs.clone()
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::Finish { id } => {
                let msg = LargeObjMetadata::Finish(LargeObjFinish { id: *id });

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
                let hash = self.hash.wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash {
                        err: err
                    })?;

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
                let hash = self.hash.wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash {
                        err: err
                    })?;

                Ok((
                    LargeObjMsg::ReqObj {
                        id: id,
                        hash: hash,
                        size: size
                    },
                    curr
                ))
            }
            LargeObjMetadata::Accept(LargeObjAccept { hash, size, id }) => {
                let hash = self.hash.wrap_hashed_bytes(&hash)
                    .map_err(|err| LargeObjMsgDecodeError::Hash {
                        err: err
                    })?;

                Ok((
                    LargeObjMsg::Accept {
                        id: id,
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
                        id: id,
                        frags: frags
                    },
                    curr
                ))
            }
            LargeObjMetadata::Req(LargeObjReq { id, reqs }) => {
                Ok((LargeObjMsg::Req { id: id, reqs: reqs }, curr))
            }
            LargeObjMetadata::Finish(LargeObjFinish { id }) => {
                Ok((LargeObjMsg::Finish { id: id }, curr))
            }
        }
    }
}


impl<H, Msg, Wrapper, Auth, Codec, IDs, Recv>
    AuthNMsgRecv<Auth::SessionPrin, LargeObjMsg<H>>
    for LargeObjProto<H, Msg, Wrapper, Auth, Codec, IDs, Recv>
where Recv: AuthNMsgRecv<Auth::Prin, Msg>,
      IDs: IDGen + Iterator<Item = u64>,
      Auth: MsgAuthN<Msg, Wrapper>,
      Codec: DatagramCodec<Wrapper>,
      H: Clone + Display + Hash + HashID + Eq {
    type RecvError = LargeObjRecvError<
        H,
        Auth::Error,
        Codec::DecodeError,
        Recv::RecvError
    >;

    fn recv_auth_msg(
        &mut self,
        prin: &Auth::SessionPrin,
        msg: LargeObjMsg<H>
    ) -> Result<(), Self::RecvError> {
        let data = match msg {
            // Inbound messages.
            LargeObjMsg::Offer {
                hash, size, frag
            } => match self.inbound_hashes.entry((prin.clone(), hash.clone())) {
                Entry::Occupied(ent) => {
                    // ID already exists, get the entry.
                    let id = *ent.get();
                    let RecvEntry { frags, hash, when, .. } =
                        self.inbound_objs.get_mut(&id)
                        .ok_or(LargeObjRecvError::NotFound {
                            hash: hash,
                            id: id
                        })?;
                    // Check if we're still receiving fragments.
                    let finished = if let Some(frags) = frags {
                        // Receive the fragment.
                        frags.recv(frag.offset() as usize, frag.data())
                            .map_err(|err| LargeObjRecvError::InboundRecv {
                                hash: hash.clone(),
                                id: id,
                                err: err
                            })?;

                        frags.is_finished()
                    } else {
                        // This is ok, it can happen due to delayed
                        // messages.
                        trace!(target: "large-obj-proto",
                               "redundant offer message for ID {:x} ({})",
                               id, hash);

                        false
                    };

                    // Check if the entry is finished and
                    // report if it is.
                    if finished {
                        debug!(target: "large-obj-proto",
                               "finished transfer for ID {:x}",
                               id);

                        // Send a finished message immediately.
                        *when = Some(Instant::now());

                        let data = match frags.take() {
                            Some(frags) => match frags.finish() {
                                Ok(data) => Some(data),
                                Err(_) => {
                                    error!(target: "large-obj-proto",
                                           "finish for ID {:x} should not fail",
                                           id);

                                    None
                                }
                            },
                            None => {
                                error!(target: "large-obj-proto",
                                       "frags for ID {:x} should not be None",
                                       id);

                                None
                            }
                        };

                        Ok(data)
                    } else {
                        Ok(None)
                    }
                },
                Entry::Vacant(ent) => {
                    // No entry for this hash exists, set one up.
                    let id = self.ids.next().ok_or(LargeObjRecvError::NoID)?;
                    let size = size as usize;

                    ent.insert(id);

                    debug!(target: "large-obj-proto",
                           "creating new transfer for {} with ID {:x}",
                           hash, id);

                    // See if the offer provides all the data.
                    let ent = if frag.offset() == 0 && frag.len() == size {
                        trace!(target: "large-obj-proto",
                               "offer message provides entire object");

                        // Complete the message and report it upstream.

                        RecvEntry {
                            when: Some(Instant::now()),
                            frags: None,
                            hash: hash,
                            req: true
                        }
                    } else {
                        trace!(target: "large-obj-proto",
                               "offer message provides partial object");

                        RecvEntry {
                            frags: Some(InboundFrags::new(size)),
                            when: Some(Instant::now()),
                            hash: hash,
                            req: true,
                        }
                    };

                    // Error if an entry already exists under this ID.
                    if self.inbound_objs.insert(id, ent).is_none() {
                        Ok(None)
                    } else {
                        Err(LargeObjRecvError::Collision)
                    }
                }
            }
            LargeObjMsg::Frags { id, frags: recv } => match self.inbound_objs
                .get_mut(&id) {
                Some(RecvEntry { frags, req, hash, when }) => {
                    let finished = if let Some(frags) = frags {
                        debug!(target: "large-obj-proto",
                               "received finished acknowledgement for ID {:x}",
                               id);

                        // If we get a frags message, that means our req
                        // has been acknowledged.
                        *req = false;

                        // Receive all of the fragments
                        for frag in recv {
                            frags.recv(frag.offset() as usize, frag.data())
                                .map_err(|err| LargeObjRecvError::InboundRecv {
                                    hash: hash.clone(),
                                    id: id,
                                    err: err
                                })?;
                        }

                        frags.is_finished()
                    } else {
                        trace!(target: "large-obj-proto",
                               "redundant fragments message for ID {:x} ({})",
                               id, hash);

                        false
                    };

                    // Check if the entry is finished and report if it is.
                    if finished {
                        debug!(target: "large-obj-proto",
                               "finished transfer for ID {:x}",
                               id);

                        // Send a finished message immediately.
                        *when = Some(Instant::now());

                        let data = match frags.take() {
                            Some(frags) => match frags.finish() {
                                Ok(data) => Some(data),
                                Err(_) => {
                                    error!(target: "large-obj-proto",
                                           "finish for ID {:x} should not fail",
                                           id);

                                    None
                                }
                            },
                            None => {
                                error!(target: "large-obj-proto",
                                       "frags for ID {:x} should not be None",
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
                           "fragments message for non-existent ID {:x}",
                           id);

                    Ok(None)
                }
            }
            // Outbound messages.
            LargeObjMsg::Accept { hash, id, .. } => if self
                .outbound_objs
                .remove(&hash).is_some() {
                debug!(target: "large-obj-proto",
                       "received acceptance for {} (ID {:x})",
                       hash, id);

                if self.outbound_hashes.insert(id, hash).is_none() {
                    Ok(None)
                } else {
                    Err(LargeObjRecvError::Collision)
                }
            } else {
                trace!(target: "large-obj-proto",
                       "redundant accept for {}",
                       hash);

                Ok(None)
            },
            LargeObjMsg::ReqObj { hash, id, .. } => if self
                .outbound_objs
                .get(&hash).is_none() {
                match self.outbound_hashes.entry(id) {
                    Entry::Occupied(_) => {
                        trace!(target: "large-obj-proto",
                               "redundant object request for {}",
                               hash);

                        Ok(None)
                    }
                    Entry::Vacant(ent) => {
                        debug!(target: "large-obj-proto",
                               "received object request for {} (ID {:x})",
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
            },
            LargeObjMsg::Req { id, reqs } => match self.outbound_hashes
                .get(&id) {
                Some(hash) => match self.outbound_objs.get_mut(&hash) {
                    Some(ent) => {
                        debug!(target: "large-obj-proto",
                               "received fragments for {} (ID {:x})",
                               hash, id);

                        for req in reqs {
                            ent.frags.recv_req(&req)
                                .map_err(|err| LargeObjRecvError::OutboundRecv {
                                    hash: hash.clone(),
                                    id: id,
                                    err: err
                                })?;
                        }

                        Ok(None)
                    }
                    None => return Err(LargeObjRecvError::NoObj {
                        hash: hash.clone(),
                        id: id
                    })
                }
                None => {
                    trace!(target: "large-obj-proto",
                           "stray frags for {:0x}",
                           id);

                    Ok(None)
                }
            },
            LargeObjMsg::Finish { id } => match self
                .outbound_hashes.remove(&id) {
                Some(hash) => match self.outbound_objs.remove(&hash) {
                    Some(SendEntry { .. }) => {
                        trace!(target: "large-obj-proto",
                               "removed transfer entries for {:x} ({})",
                               id, hash);

                        Ok(None)
                    },
                    None => return Err(LargeObjRecvError::NotFound {
                        hash: hash,
                        id: id
                    })
                },
                None => {
                    trace!(target: "large-obj-proto",
                           "redundant finish for ID {:x}",
                           id);

                    Ok(None)
                }
            }
        }?;

        // Complete the message and send it upstream.
        if let Some(data) = data {
            debug!(target: "large-obj-proto",
                   "processsing complete message");

            // Decode the complete message.
            let (wrapper, _) = self.codec.decode(&data)
                .map_err(|err| LargeObjRecvError::Decode {
                    err: err
                })?;

            // Authenticate the complete message.
            match self.auth.msg_authn(prin, wrapper)
                .map_err(|err| LargeObjRecvError::Auth {
                    err: err
                })? {
                AuthNResult::Accept((prin, msg)) => {
                    // Send it upstream.
                    self.upstream.recv_auth_msg(&prin, msg)
                        .map_err(|err| LargeObjRecvError::Upstream {
                            err: err
                        })?;
                },
                AuthNResult::Reject => {
                    return Err(LargeObjRecvError::AuthNFail)
                }
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

impl<H, Auth, Decode, Upstream> ScopedError
    for LargeObjRecvError<H, Auth, Decode, Upstream>
where
    H: Clone + Display + Hash + HashID + Eq,
    Upstream: ScopedError {
    fn scope(&self) -> ErrorScope {
        match self {
            LargeObjRecvError::NotFound { .. } |
            LargeObjRecvError::NoHash { .. } |
            LargeObjRecvError::NoObj { .. } |
            LargeObjRecvError::Collision |
            LargeObjRecvError::NoID => ErrorScope::Unrecoverable,
            LargeObjRecvError::FinishedPending { .. } |
            LargeObjRecvError::OutboundRecv { .. } |
            LargeObjRecvError::InboundRecv { .. } |
            LargeObjRecvError::Decode { .. } |
            LargeObjRecvError::Auth { .. } |
            LargeObjRecvError::AuthNFail => ErrorScope::Msg,
            LargeObjRecvError::Upstream { err } => err.scope(),
        }
    }
}

impl<H> Default for LargeObjMsgCodec<H>
where H: HashAlgo + Default {
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

impl<H, Auth, Decode, Upstream> Display
    for LargeObjRecvError<H, Auth, Decode, Upstream>
where
    H: Clone + Display + Hash + HashID + Eq,
    Upstream: Display,
    Decode: Display,
    Auth: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            LargeObjRecvError::Upstream { err } => err.fmt(f),
            LargeObjRecvError::Decode { err } => err.fmt(f),
            LargeObjRecvError::Auth { err } => err.fmt(f),
            LargeObjRecvError::FinishedPending { hash, id } =>
                write!(f, concat!("received finished for ID {:x} ({}), ",
                                  "but transfer is still pending"),
                       id, hash),
            LargeObjRecvError::OutboundRecv { hash, id, err } =>
                write!(f, "error receiving for ID {:x} ({}): {}",
                       id, hash, err),
            LargeObjRecvError::InboundRecv { hash, id, err } =>
                write!(f, "error receiving for ID {:x} ({}): {}",
                       id, hash, err),
            LargeObjRecvError::NotFound { hash, id } =>
                write!(f, "ID {:x} exists for {}, but no object entry found",
                       id, hash),
            LargeObjRecvError::NoHash { hash, id } =>
                write!(f, "ID {:x} exists for {}, but no hash entry found",
                       id, hash),
            LargeObjRecvError::NoObj { id, hash } =>
                write!(f, "ID {:x} exists for {}, but no object entry found",
                       id, hash),
            LargeObjRecvError::Collision =>
                write!(f, "id generator produced collision"),
            LargeObjRecvError::NoID => write!(f, "id generator exhausted"),
            LargeObjRecvError::AuthNFail =>
                write!(f, "message authentication failed")
        }
    }
}

#[cfg(test)]
use constellation_common::hashid::SHA3ID;
#[cfg(test)]
use constellation_common::hashid::SHA3Algo;

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
    let msg = LargeObjMsg::Offer {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        frag: LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req_obj() {
    let algo = SHA3Algo::default();
    let msg = LargeObjMsg::ReqObj {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_accopt() {
    let algo = SHA3Algo::default();
    let msg = LargeObjMsg::Accept {
        hash: algo.wrap_hashed_bytes(&[0xaa; 64]).unwrap(),
        size: 0x31337,
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_1_frag() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: 0x1234567890abcdef,
        frags: vec![LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_4_frags() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: 0x1234567890abcdef,
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
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_16_frags() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Frags {
        id: 0x1234567890abcdef,
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
            LargeObjFrag {
                offset: 0x1111111111111111,
                data: vec![0x5a; 64]
            },
        ]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Req {
        id: 0x1337feeddeadbeef,
        reqs: vec![
            LargeObjFragReq::Need(LargeObjFragRef {
                offset: 0x1337feeddeadbeef,
                len: 0x1234567890abcdef
            });
            64
        ]
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_finish() {
    let msg: LargeObjMsg<SHA3ID> = LargeObjMsg::Finish {
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::<SHA3Algo>::default();
    let mut buf = [0; LargeObjMsgCodec::<SHA3Algo>::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}
