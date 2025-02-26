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

use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;

use constellation_common::codec::per::PERCodec;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;

use crate::error::ErrorReportInfo;
use crate::frags::OutboundDataError;
use crate::frags::OutboundFrags;
use crate::generated::large_obj::LargeObjAccept;
use crate::generated::large_obj::LargeObjFinish;
use crate::generated::large_obj::LargeObjFinished;
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

pub struct LargeObjMsgCodec {
    frag_header: LargeObjFragHeaderPERCodec,
    metadata: LargeObjMetadataPERCodec
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct LargeObjFrag {
    offset: u64,
    data: Vec<u8>
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum LargeObjMsg {
    Offer {
        hash: Vec<u8>,
        size: u64,
        frag: LargeObjFrag
    },
    Accept {
        hash: Vec<u8>,
        size: u64,
        id: u64
    },
    ReqObj {
        hash: Vec<u8>,
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
    },
    Finished {
        id: u64
    }
}

#[derive(Debug)]
pub enum LargeObjMsgEncodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec as DatagramCodec<LargeObjMetadata>>::EncodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec as DatagramCodec<LargeObjFragHeader>>::EncodeError
    },
    TooShort
}

#[derive(Debug)]
pub enum LargeObjMsgDecodeError {
    Metadata {
        err: <LargeObjMetadataPERCodec as DatagramCodec<LargeObjMetadata>>::EncodeError
    },
    FragHeader {
        err: <LargeObjFragHeaderPERCodec as DatagramCodec<LargeObjFragHeader>>::EncodeError
    },
    TooShort
}

pub enum LargeObjDataError {
    Frags { err: OutboundDataError },
    OutOfBounds
}

impl LargeObjMsg {
    /// Maximum number of bytes that can be sent with a datagram.
    pub const LARGE_OBJ_DATAGRAM_MAX_DATA: usize = 1024;

    #[inline]
    pub fn offer(
        frags: &mut OutboundFrags,
        hash: Vec<u8>,
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
        hash: Vec<u8>,
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
        hash: Vec<u8>,
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

    #[inline]
    pub fn finished(id: usize) -> Self {
        LargeObjMsg::Finish { id: id as u64 }
    }
}

impl DatagramCodec<LargeObjMsg> for LargeObjMsgCodec {
    type CreateError = Infallible;
    type DecodeError = LargeObjMsgDecodeError;
    type EncodeError = LargeObjMsgEncodeError;
    type Param = ();

    const MAX_BYTES: usize = 1286;

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(Self::default())
    }

    #[inline]
    fn encode(
        &mut self,
        val: &LargeObjMsg,
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
                    hash: hash.clone(),
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
                    hash: hash.clone(),
                    size: *size,
                    id: *id
                });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
            LargeObjMsg::ReqObj { hash, size, id } => {
                let msg = LargeObjMetadata::ReqObj(LargeObjReqObj {
                    hash: hash.clone(),
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
            LargeObjMsg::Finished { id } => {
                let msg =
                    LargeObjMetadata::Finished(LargeObjFinished { id: *id });

                self.metadata.encode(&msg, buf).map_err(|err| {
                    LargeObjMsgEncodeError::Metadata { err: err }
                })
            }
        }
    }

    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(LargeObjMsg, usize), Self::DecodeError> {
        let (metadata, mut curr) = self
            .metadata
            .decode(buf)
            .map_err(|err| LargeObjMsgDecodeError::Metadata { err: err })?;

        match metadata {
            LargeObjMetadata::Offer(LargeObjOffer { hash, size, frag }) => {
                let datalen = frag.len as usize;
                let mut data = vec![0; datalen];

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
            LargeObjMetadata::Finished(LargeObjFinished { id }) => {
                Ok((LargeObjMsg::Finished { id: id }, curr))
            }
        }
    }
}

impl Default for LargeObjMsgCodec {
    #[inline]
    fn default() -> Self {
        LargeObjMsgCodec {
            frag_header: LargeObjFragHeaderPERCodec::default(),
            metadata: LargeObjMetadataPERCodec::default()
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
            LargeObjMsgDecodeError::TooShort => {
                write!(f, "input buffer is too short")
            }
        }
    }
}

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
fn test_encode_decode_metadata_finished() {
    let msg = LargeObjMetadata::Finished(LargeObjFinished {
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
    let msg = LargeObjMsg::Offer {
        hash: vec![0xaa; 64],
        size: 0x31337,
        frag: LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req_obj() {
    let msg = LargeObjMsg::ReqObj {
        hash: vec![0xaa; 64],
        size: 0x31337,
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_accopt() {
    let msg = LargeObjMsg::Accept {
        hash: vec![0xaa; 64],
        size: 0x31337,
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_1_frag() {
    let msg = LargeObjMsg::Frags {
        id: 0x1234567890abcdef,
        frags: vec![LargeObjFrag {
            offset: 0x1111111111111111,
            data: vec![0x5a; 1024]
        }]
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_4_frags() {
    let msg = LargeObjMsg::Frags {
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
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_frags_16_frags() {
    let msg = LargeObjMsg::Frags {
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
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_req() {
    let msg = LargeObjMsg::Req {
        id: 0x1337feeddeadbeef,
        reqs: vec![
            LargeObjFragReq::Need(LargeObjFragRef {
                offset: 0x1337feeddeadbeef,
                len: 0x1234567890abcdef
            });
            64
        ]
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_finish() {
    let msg = LargeObjMsg::Finish {
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}

#[test]
fn test_encode_decode_msg_finished() {
    let msg = LargeObjMsg::Finished {
        id: 0x1234567890abcdef
    };
    let mut codec = LargeObjMsgCodec::default();
    let mut buf = [0; LargeObjMsgCodec::MAX_BYTES];

    let _ = codec.encode(&msg, &mut buf).expect("Expected success");

    let (decoded, _) = codec.decode(&buf).expect("Expected success");

    assert_eq!(msg, decoded);
}
