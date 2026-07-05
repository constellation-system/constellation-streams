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

use std::collections::HashSet;
use std::iter::once;
use std::ops::Deref;
use std::time::Duration;
use std::time::Instant;

use constellation_auth::authn::test::TestAuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::cred::NullCred;
use constellation_common::codec::test::TestBytesCodec;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::sync::Notify;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::config::PrivateLargeObjModeConfig;
use constellation_streams::large_obj::test::TestLargeObjMsgs;
use constellation_streams::large_obj::test::TestLargeObjProtoTypes;
use constellation_streams::large_obj::LargeObjFrag;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestLargeObjPushModeTypes;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::Parties;
use constellation_streams::threads::private::PrivateLargeObjPushMode;
use constellation_streams::threads::PushMode;

use crate::init;

#[test]
fn test_send_from_outbound_offer_succeed() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(when) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: Some(when) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success { val: Some(when) }
                }
            }),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success { val: Some(when) }
                }
            }),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_retry_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session
                }
            }),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success { val: Some(when) }
                }
            }),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success { val: Some(when) }
                }
            }),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_indef_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session
                }
            }),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Indef
                }
            }),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Indef
                }
            }),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_imm_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_offer_complete_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(when) }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: Some(when) }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(later), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());

    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success { val: Some(later) }
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success { val: Some(later) }
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_retry_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later)),
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success { val: Some(when) }
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success { val: Some(later) }
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_indef_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session
                }
            }),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), Some(now));
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Indef
                }
            }),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_indef() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Indef
                }
            }),
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(when), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(when));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success {
                                val: Some(later)
                            }
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            action: TestIndefAction::Success {
                                val: Some(later)
                            }
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success {
                                val: Some(later)
                            }
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::WouldBlock,
                            action: TestIndefAction::Success {
                                val: Some(later)
                            }
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(later));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(0 as u64)]
    );
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_imm_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_frags_complete_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> =
        vec![(Some(msg), Some(when)), (None, None), (None, Some(later))];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), Some(now));
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto
        .recv_req_obj_msg(hash.clone(), id)
        .expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next.next_outbound(), None);
    assert_eq!(next.retry_pending(), None);
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_create_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_retry_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_create_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_create_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_add_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_add_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_finish_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_finish_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_add_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_add_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_finish_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_retry_finish_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_retry_finish_complete_imm() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_retry_finish_complete() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_retry_create_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![RetryResult::Success(())],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_retry_add_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_retry_finish_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: when })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_retry_add_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_retry_finish_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_retry_finish_permanent() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_indef() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_create_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_create_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_create_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        }]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_indef_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(())),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_create_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_create_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_add_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_imm_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_finish_retry() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, later)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_create_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_create_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_create_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_create_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(!next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_add_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_add_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_complete_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_imm_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_finish_complete_imm() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_imm_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_add_complete_finish_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_create_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![RetryResult::Success(())],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_complete_create_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![RetryResult::Success(())],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_complete_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_complete_imm_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_complete_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_complete_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_complete_imm_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_complete_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_complete_imm_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_complete_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_select_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert!(stream
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert_eq!(
        stream
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPermanentError {
            scope: ErrorScope::Session
        }]
    );
    assert!(stream
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_send_from_outbound_msg_create_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![RetryResult::Success(())],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_create_permanent_retry_abort() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![
            RetryResult::Retry(TestAbortRetry {
                batch: 0,
                when: when
            }),
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_permanent_retry_cancel() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_permanent_complete_imm_cancel() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_permanent_complete_cancel() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_add_permanent_cancel_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_finish_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_finish_permanent_retry_cancel() {
    init();

    let now = Instant::now();
    let when = now + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: when })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_some());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_finish_permanent_complete_imm_cancel() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_finish_permanent_complete_cancel() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::WouldBlock,
                action: TestAction::Success { val: () }
            }
        })],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_none());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());
    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}

#[test]
fn test_send_from_outbound_msg_finish_permanent_cancel_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let size = 4096;
    let msg = vec![0x11; size];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(
        TestBytesCodec
            .encode_to_vec(&msg)
            .expect("Expected success")
            .as_slice()
    ));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![(None, None)];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>,
        ()
    > = PrivateLargeObjPushMode::create(config).expect("Expected success");
    let recv: TestAuthNMsgRecv<Vec<u8>> = TestAuthNMsgRecv::default();
    let tokens = HashSet::new();
    let mut proto: LargeObjProto<_, _, _, _, TestLargeObjProtoTypes<_>> =
        LargeObjProto::create(
            LargeObjProtoConfig::default(),
            Notify::new(),
            recv,
            msgs,
            PassthruMsgAuthN::default(),
            SHA3Algo::default()
        )
        .expect("Expected success");
    let frag = LargeObjFrag::new(0, vec![0x11; 1024]);
    let res = proto
        .recv_offer_msg(&NullCred, hash.clone(), size as u64, frag)
        .expect("Expected success");

    assert!(res.is_none());

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert!(next.next_outbound().is_some());
    assert!(next.retry_pending().is_none());
    assert!(!next.has_completes());

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec![LargeObjMsg::ReqObj {
                id: LargeObjID::from(0 as u64),
                size: size as u64,
                hash: hash.clone()
            }]
        },]
    );
    assert!(stream
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![(
            0,
            TestPermanentError {
                scope: ErrorScope::Session
            }
        )]
    );
}
