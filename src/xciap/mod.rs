// Copyright © 2024 The Johns Hopkins Applied Physics Laboratory LLC.
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

//! Cross-Channel Identification/Acknowledgement Protocol (XCIAP)
//!
//! XCIAP is a simple session protocol adaptation for tracking the use
//! of multiple unreliable datagram channels in a single session, and
//! acknowledging the receipt of past messages.

use constellation_common::codec::per::PERCodec;

pub use crate::generated::xciap::Xciap16Header;
pub use crate::generated::xciap::Xciap256Header;
pub use crate::generated::xciap::Xciap4Header;
pub use crate::generated::xciap::Xciap64Header;
pub use crate::generated::xciap::XciapHeader;

pub type XCIAP4HeaderPERCodec = PERCodec<Xciap4Header, 38>;
pub type XCIAP16HeaderPERCodec = PERCodec<Xciap16Header, 52>;
pub type XCIAP64HeaderPERCodec = PERCodec<Xciap64Header, 102>;
pub type XCIAP256HeaderPERCodec = PERCodec<Xciap256Header, 296>;
pub type XCIAPHeaderPERCodec = PERCodec<XciapHeader, 298>;

impl Xciap4Header {
    #[inline]
    pub fn create(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        Xciap4Header {
            epoch: epoch,
            channel: channel,
            ack_epoch: ack_epoch,
            acks: asn1rs::prelude::BitVec::from_bytes(acks, 4)
        }
    }

    /// Get the sender's epoch.
    ///
    /// This will be sent as the acknowledgement epoch when the
    /// messages are acknowledged.
    #[inline]
    pub fn epoch(&self) -> u16 {
        self.epoch
    }

    /// Get the acknowledgement epoch.
    ///
    /// This is the epoch on our side for which the acknowledgements
    /// should be applied.
    #[inline]
    pub fn ack_epoch(&self) -> u16 {
        self.ack_epoch
    }

    /// Get the sender's channel.
    ///
    /// This is the channel used by the sender to deliver this
    /// message.  Note that the meaning of this number depends on the
    /// value of [epoch](Xciap4Header::epoch).
    #[inline]
    pub fn channel(&self) -> u8 {
        self.channel
    }
}

impl Xciap16Header {
    #[inline]
    pub fn create(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        Xciap16Header {
            epoch: epoch,
            channel: channel,
            ack_epoch: ack_epoch,
            acks: asn1rs::prelude::BitVec::from_bytes(acks, 16)
        }
    }
}

impl Xciap64Header {
    #[inline]
    pub fn create(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        Xciap64Header {
            epoch: epoch,
            channel: channel,
            ack_epoch: ack_epoch,
            acks: asn1rs::prelude::BitVec::from_bytes(acks, 64)
        }
    }
}

impl Xciap256Header {
    #[inline]
    pub fn create(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        Xciap256Header {
            epoch: epoch,
            channel: channel,
            ack_epoch: ack_epoch,
            acks: asn1rs::prelude::BitVec::from_bytes(acks, 256)
        }
    }
}

impl XciapHeader {
    #[inline]
    pub fn create_4(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        XciapHeader::Xciap4(Xciap4Header::create(
            epoch, ack_epoch, channel, acks
        ))
    }

    #[inline]
    pub fn create_16(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        XciapHeader::Xciap16(Xciap16Header::create(
            epoch, ack_epoch, channel, acks
        ))
    }

    #[inline]
    pub fn create_64(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        XciapHeader::Xciap64(Xciap64Header::create(
            epoch, ack_epoch, channel, acks
        ))
    }

    #[inline]
    pub fn create_256(
        epoch: u16,
        ack_epoch: u16,
        channel: u8,
        acks: Vec<u8>
    ) -> Self {
        XciapHeader::Xciap256(Xciap256Header::create(
            epoch, ack_epoch, channel, acks
        ))
    }
}

#[cfg(test)]
use constellation_common::codec::DatagramCodec;

#[test]
fn test_xciap4header_codec() {
    let version = Xciap4Header::create(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAP4HeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAP4HeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap16header_codec() {
    let version = Xciap16Header::create(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAP16HeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAP16HeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap64header_codec() {
    let version = Xciap64Header::create(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAP64HeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAP64HeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap256header_codec() {
    let version = Xciap256Header::create(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAP256HeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAP256HeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap_header_codec_4() {
    let version = XciapHeader::create_4(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAPHeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAPHeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap_header_codec_16() {
    let version = XciapHeader::create_16(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAPHeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAPHeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap_header_codec_64() {
    let version = XciapHeader::create_64(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAPHeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAPHeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}

#[test]
fn test_xciap_header_codec_256() {
    let version = XciapHeader::create_256(0xff00, 0xaaaa, 2, vec![0x0c]);
    let mut codec = XCIAPHeaderPERCodec::create(()).unwrap();
    let mut buf = [0; XCIAPHeaderPERCodec::MAX_BYTES];
    let nencoded = codec.encode(&version, &mut buf[..]).unwrap();
    let (actual, nbytes) = codec.decode(&buf[..]).unwrap();

    assert_eq!(version, actual);
    assert_eq!(nencoded, nbytes);
}
