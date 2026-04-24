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
use std::time::Duration;
use std::time::Instant;

use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::test::TestAuthNMsgRecv;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::net::test::TestPrivateMsgs;
use constellation_common::net::test::TestMsgsError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryIndefResult;
use constellation_common::sync::Notify;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::config::PrivateLargeObjModeConfig;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsg;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::test::TestLargeObjMsgs;
use constellation_streams::large_obj::test::TestLargeObjProtoTypes;
use constellation_streams::stream::Parties;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestBatchError;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestLargeObjPushModeTypes;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestStartBatchError;
use constellation_streams::threads::PushMode;
use constellation_streams::threads::private::PrivateLargeObjPushModeTypes;
use constellation_streams::threads::private::PrivateLargeObjPushMode;

use crate::init;

#[test]
fn test_private_send_from_outbound_succeed() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_offer_imm() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(when)
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success {
                        val: Some(when)
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_permanent() {
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
        push_offers: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_retry_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_complete_offer_imm() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(when)
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_complete_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success {
                        val: Some(when)
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_indef_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_retry_permanent_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_retry_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Success((Some(when), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_indef_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_complete_offer_imm() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(when)
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_complete_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Success {
                        val: Some(when)
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_indef_permanent_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<Vec<u8>, LargeObjMsg<SHA3ID>, SHA3ID> =
        TestPrivateStream::new(script);
    let msg = vec![0x11; 4096];
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_imm_retry_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry {
                            when: now
                        }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_retry_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Retry {
                        retry: TestRetry {
                            when: now
                        }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(now));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_pending(&mut (), &mut proto, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_imm_indef_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_indef_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut proto, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_imm_complete_imm_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestIndefAction::Success {
                                    val: Some(now)
                                }
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_complete_imm_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::WouldBlock,
                                action: TestIndefAction::Success {
                                    val: Some(now)
                                }
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(now));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_imm_complete_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestIndefAction::Success {
                                    val: Some(now)
                                }
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(now));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_complete_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::WouldBlock,
                                action: TestIndefAction::Success {
                                    val: Some(now)
                                }
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let next = mode
        .complete_pending(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(now));
    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash.clone()
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash
               ]);
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_imm_permanent_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}

#[test]
fn test_private_send_from_outbound_complete_permanent_offer() {
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
            Ok(RetryIndefResult::Success((Some(later), ()))),
        ],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
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
    let hash = hasher.hash_bytes(once(msg.as_slice()));
    let script: Vec<(Option<Vec<u8>>, Option<Instant>)> = vec![
        (Some(msg), Some(when)),
        (None, None),
        (None, Some(later))
    ];
    let msgs = TestLargeObjMsgs::new(script);
    let config = PrivateLargeObjModeConfig::default();
    let mut mode: PrivateLargeObjPushMode<
        TestLargeObjPushModeTypes<Vec<u8>, SHA3Algo>, ()
    > = PrivateLargeObjPushMode::create(config)
        .expect("Expected success");
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
        ).expect("Expected success");

    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());

    let id = LargeObjID::from(0 as u64);
    let _ = proto.recv_req_obj_msg(hash.clone(), id).expect("Expected success");
    let next = mode
        .send_from_outbound(&mut (), &mut proto, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.is_empty());
    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(0 as u64)
               ]);
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.is_empty());
    assert!(stream.batch_reports.is_empty());
}
