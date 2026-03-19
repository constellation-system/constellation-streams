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

use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;
use constellation_streams::frags::InboundFrags;
use constellation_streams::frags::OutboundFrags;

#[test]
fn test_offer_frag_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert!(matches![first, RetryResult::Success((0, 16, _))]);
}

#[test]
fn test_offer_frag_short() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 8]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert!(matches![first, RetryResult::Success((0, 8, _))]);
}

#[test]
fn test_offer_frag_multi() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 40]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert!(matches![first, RetryResult::Success((0, 16, _))]);

    let second = frags.offer_frag(16).expect("Expected success");

    assert!(matches![second, RetryResult::Success((16, 16, _))]);

    let third = frags.offer_frag(16).expect("Expected success");

    assert!(matches![third, RetryResult::Success((32, 8, _))]);
}

#[test]
fn test_data_frags_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);
    let mut buf = [(0, 0); 1];

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((1, _)))]);
    assert_eq!(&buf[0], &(0, 16));
}

#[test]
fn test_data_frags_short() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 8]);
    let mut buf = [(0, 0); 1];

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((1, _)))]);
    assert_eq!(&buf[0], &(0, 8));
}

#[test]
fn test_data_frags_ack() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);
    let mut buf = [(0, 0); 1];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((1, _)))]);
    assert_eq!(&buf[0], &(0, 8));
}

#[test]
fn test_data_frags_ack_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_long() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 24]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_exact_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![second, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_gap_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 24]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    frags.recv_ack(20, 4).expect("Expected success");

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![second, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_req_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![first, RetryResult::Success(Some((2, _)))]);
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    frags.recv_need(8, 4).expect("Expected success");

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert!(matches![second, RetryResult::Success(Some((1, _)))]);
    assert_eq!(&buf[0], &(0, 16));
}

#[test]
fn test_reqs_exact() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 1];

    let first = frags.reqs_acks(&mut buf, &retry);

    assert!(matches![first, RetryResult::Success((1, _))]);
    assert_eq!(&buf[0], &(true, 0, 16));
}

#[test]
fn test_reqs_exact_recv_first() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 2];

    frags.recv(0, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert!(matches![first, RetryResult::Success((2, _))]);
    assert_eq!(&buf[0], &(false, 0, 8));
    assert_eq!(&buf[1], &(true, 8, 8));
}

#[test]
fn test_reqs_exact_recv_second() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 2];

    frags.recv(8, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert!(matches![first, RetryResult::Success((2, _))]);
    assert_eq!(&buf[0], &(true, 0, 8));
    assert_eq!(&buf[1], &(false, 8, 8));
}

#[test]
fn test_reqs_exact_recv_cont() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 1];

    frags.recv(8, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert!(matches![first, RetryResult::Success((1, _))]);
    assert_eq!(&buf[0], &(true, 0, 8));

    let second = frags.reqs_acks(&mut buf, &retry);

    assert!(matches![second, RetryResult::Success((1, _))]);
    assert_eq!(&buf[0], &(false, 8, 8));
}
