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

use std::iter::once;
use std::ops::Deref;
use std::time::Instant;

use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_streams::config::BatchSlotsConfig;
use constellation_streams::config::MulticastPartyConfig;
use constellation_streams::config::StreamMulticasterConfig;
use constellation_streams::frags::Frags;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::multicast::StreamMulticaster;
use constellation_streams::multicast::StreamMulticasterFrags;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::LargeObjOfferStream;
use constellation_streams::stream::LargeObjStream;
use constellation_streams::stream::Parties;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamShared;

use crate::init;

#[test]
fn test_select_all_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_multi_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_all_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Indef(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1, 2]));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_subset_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Indef(parties) = stream
        .select(&mut (), &mut selections, [0, 1].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1]));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_one_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Indef(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1, 2]));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_one_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_all_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let parties = if let RetryIndefResult::Success(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let parties = if let RetryIndefResult::Success(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_one_indef_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let parties = if let RetryIndefResult::Success(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_complete_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let parties = if let RetryIndefResult::Success(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_complete_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let parties = if let RetryIndefResult::Indef(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1, 2]));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: () }
                        }
                    })
                }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_complete_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Indef
                        }
                    })
                }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![1, 2]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_indef_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: () }
                        }
                    })
                }
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
    let script_1 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(())),
        ],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    let err = stream.select(&mut (), &mut selections, vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let retry = if let RetryIndefResult::Retry(parties) = stream
        .complete_select(&mut (), &mut selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    let parties = if let RetryIndefResult::Success(parties) = stream
        .retry_select(&mut (), &mut selections, retry)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, vec![0, 1]);

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_all_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_succeed_subset_selected() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_multi_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let retry = if let RetryResult::Retry(retry) = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let retry = if let RetryResult::Retry(retry) = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let retry = if let RetryResult::Retry(retry) = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryResult::Retry(retry) = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_one_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());
    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_all_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_create_batch(&mut (), &mut flags, &selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_create_batch(&mut (), &mut flags, &selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_create_batch(&mut (), &mut flags, &selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_create_batch(&mut (), &mut flags, &selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_create_retry_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let err = stream.create_batch(&mut (), &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut (),
        &mut flags,
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_create_batch(&mut (), &mut flags, &selections, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_create_batch(&mut (), &mut flags, &selections, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    stream
        .start_batch(&mut (), [0, 1, 2].iter())
        .expect("Expected success");

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    stream
        .start_batch(&mut (), [0, 1].iter())
        .expect("Expected success");

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let retry = stream
        .start_batch(&mut (), vec![0, 1, 2].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let retry = stream
        .start_batch(&mut (), vec![0, 1, 2].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_both_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let retry = stream
        .start_batch(&mut (), vec![0, 1, 2].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_all_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let indef = stream
        .start_batch(&mut (), vec![0, 1, 2].iter())
        .expect("Expected success");

    assert!(indef.is_indef());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_one_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    stream
        .start_batch(&mut (), [0, 1, 2].iter())
        .expect("Expected success");

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(permanent.is_some());
    assert!(completable.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(permanent.is_some());
    assert!(completable.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Success { val: () }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_both_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_complete_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_complete_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_both_complete_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_complete_indef_all() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let indef = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(indef.is_indef());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_succeed_complete_indef_one() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_complete_indef_one() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_both_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_select_complete_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
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
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_create_complete_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_start_batch_both_complete_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: () }
            }
        })],
        create_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
                        }
                    })
                }
            }
        })],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_all_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.cancel_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.cancel_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_multi_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.cancel_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .cancel_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .cancel_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .cancel_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryResult::Retry(retry) = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_one_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());
    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_all_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_cancel_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_succeed_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_cancel_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Ok(RetryResult::Success(()))],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_cancel_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_cancel_batch_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_cancel_batch(&mut (), &mut flags, &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_cancel_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_cancel_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_all_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.finish_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.finish_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_multi_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.finish_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .finish_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .finish_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .finish_batch(&mut (), &mut flags, &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryResult::Retry(retry) = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_one_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Permanent {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        })],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());
    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_all_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_finish_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_succeed_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_finish_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Ok(RetryResult::Success(()))],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_finish_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_finish_batch_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_finish_batch(&mut (), &mut flags, &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_finish_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_finish_batch(&mut (), &mut flags, &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_all_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.add(&mut (), &mut flags, &"hello", &batch),
        Ok(RetryResult::Success(()))
    ));

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(()))],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.add(&mut (), &mut flags, &"hello", &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_multi_subset_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    assert!(matches!(
        stream.add(&mut (), &mut flags, &"hello", &batch),
        Ok(RetryResult::Success(()))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .add(&mut (), &mut flags, &"hello", &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .add(&mut (), &mut flags, &"hello", &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let retry = if let RetryResult::Retry(retry) = stream
        .add(&mut (), &mut flags, &"hello", &batch)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryResult::Retry(retry) = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let batch = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_one_permanent() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());
    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_all_complete_succeed() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
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
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_succeed_complete() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    if let RetryResult::Success(parties) = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Ok(RetryResult::Success(()))],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_add_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestAction::Success { val: () }
                        }
                    })
                }
            }
        })],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
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
    let script_2 = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry { when: now })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let mut flags = stream.empty_flags();
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) = stream
        .select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    let batch = stream
        .create_batch(&mut (), &mut flags, &selections)
        .expect("Expected success");

    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = stream.empty_flags();

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_add(&mut (), &mut flags, &"hello", &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = if let RetryResult::Retry(parties) = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success")
    {
        parties
    } else {
        panic!("Expected success")
    };

    if let RetryResult::Success(batch) = stream
        .retry_add(&mut (), &mut flags, &"hello", &batch, retry)
        .expect("Expected success")
    {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        stream
            .stream(0)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert_eq!(
        stream
            .stream(1)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert_eq!(
        stream
            .stream(2)
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_all_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    if let RetryIndefResult::Success(res) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_subset_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    if let RetryIndefResult::Success(res) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_all_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_subset_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    if let RetryIndefResult::Success(res) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_one_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    if let RetryIndefResult::Success(res) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_indef_retry_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_indef_retry_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_indef_retry_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_one_permanent() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_all_complete_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_succeed_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_one_indef_complete_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_one_succeed_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_one_indef_complete_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let indef = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(indef.is_indef());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_frags(
        &mut (),
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_complete_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
                            action: TestIndefAction::Indef
                        }
                    })
                }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_frags(
        &mut (),
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_frags_retry_indef_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
                            action: TestIndefAction::Success { val: Some(now) }
                        }
                    })
                }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
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
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_frags(
        &mut (),
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_all_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Success((Some(now), ()))),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));

    if let RetryIndefResult::Success(res) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .push_offer(&mut (), hash_1.clone(), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(), hash_1.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(), hash_1.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(), hash_1.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_subset_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    if let RetryIndefResult::Success(res) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_all_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    assert!(matches!(
        stream.push_offer(&mut (), hash_0.clone(), &mut frags),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_subset_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    if let RetryIndefResult::Success(res) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_one_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    if let RetryIndefResult::Success(res) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_succeed_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_indef_retry_indef_retry_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_indef_retry_indef_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Indef(_))
    ));

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_indef_retry_succeed_retry_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_one_permanent() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
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
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_all_complete_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_succeed_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_one_indef_complete_succeed() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_one_succeed_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected success")
    }

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_one_indef_complete_indef() {
    init();

    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::Some(())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags: StreamMulticasterFrags<usize, OutboundFrags> =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let indef = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success");

    assert!(indef.is_indef());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_complete_retry() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Indef
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_offer(
        &mut (),
        hash_0.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_complete_retry_complete_indef() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefAction::Indef
                        }
                    })
                }
            }
        })],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Success((Some(now), ())))],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_offer(
        &mut (),
        hash_0.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![1, 2]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(2)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_push_offer_retry_indef_complete_retry_complete() {
    init();

    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
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
    let script_1 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Retry {
                        retry: TestRetry { when: now }
                    }
                }
            }),
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let script_2 = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Indef(Parties::Some(()))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let streams = vec![
        MulticastPartyConfig::new("stream-0", script_0, Retry::default()),
        MulticastPartyConfig::new("stream-1", script_1, Retry::default()),
        MulticastPartyConfig::new("stream-2", script_2, Retry::default()),
    ];
    let config =
        StreamMulticasterConfig::new(streams, BatchSlotsConfig::default());
    let mut stream: StreamMulticaster<
        _,
        _,
        TestPrivateStream<&str, &str, SHA3ID>,
        _,
        _
    > = StreamMulticaster::create(config, (&mut (), None))
        .expect("Expected success");
    let frags_params = vec![Retry::default(); 3];
    let mut frags =
        StreamMulticasterFrags::from_data(frags_params, vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash_0.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let err = stream.complete_push_offer(
        &mut (),
        hash_0.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("Expected success")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());

    if let RetryIndefResult::Success(res) = stream
        .retry_push_offer(&mut (), hash_0.clone(), &mut frags, retry)
        .expect("Expected success")
    {
        assert_eq!(res, (None, vec![0, 1]))
    } else {
        panic!("Expected indef")
    };

    assert!(stream
        .stream(0)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(0)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(0)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream
        .stream(1)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(1)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        stream
            .stream(1)
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream
        .stream(2)
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream
        .stream(2)
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(stream.stream(2).failures.is_empty());
}
