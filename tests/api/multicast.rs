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

use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::hashid::SHA3ID;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_streams::config::BatchSlotsConfig;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::multicast::StreamMulticaster;
use constellation_streams::stream::Parties;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;

#[test]
fn test_select_all_succeed() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_multi_subset_succeed() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) =
        stream.select(&mut (), &mut selections, [0, 1].iter())
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_all_indef() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Indef(parties) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1, 2]));

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_subset_indef() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Indef(parties) =
        stream.select(&mut (), &mut selections, [0, 1].iter())
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1]));

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_one_indef() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let parties = if let RetryIndefResult::Success(parties) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_succeed() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_succeed() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_retry_retry_succeed() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0, 1, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_succeed() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1, 2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_indef() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Indef(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected indef")
    };

    assert_eq!(parties, Parties::Some(vec![0, 1, 2]));

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_indef() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_indef_retry_succeed() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![0]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_succeed_retry_indef_retry_indef() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![2]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_indef_retry_succeed_retry_indef() {
    let now = Instant::now();
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
    let mut selections = stream.empty_selections();

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.select(&mut (), &mut selections, [0, 1, 2].iter())
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let retry = if let RetryIndefResult::Retry(retry) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        retry
    } else {
        panic!("Expected retry")
    };

    let parties = if let RetryIndefResult::Success(parties) =
        stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success") {
        parties
    } else {
        panic!("Expected success")
    };

    assert_eq!(parties, vec![1]);

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}

#[test]
fn test_select_one_permanent() {
    let script_0 = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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
    let stream_0: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_0);
    let stream_1: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_1);
    let stream_2: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script_2);
    let streams = vec![
        ("stream-0", Retry::default(), stream_0),
        ("stream-1", Retry::default(), stream_1),
        ("stream-2", Retry::default(), stream_2),
    ];
    let mut stream = StreamMulticaster::create(streams.into_iter(),
                                               BatchSlotsConfig::default());
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

    assert!(stream.stream(0).batches.is_empty());
    assert!(stream.stream(0).frags.is_empty());
    assert!(stream.stream(0).offers.is_empty());
    assert!(stream.stream(0).failures.is_empty());
    assert!(stream.stream(1).batches.is_empty());
    assert!(stream.stream(1).frags.is_empty());
    assert!(stream.stream(1).offers.is_empty());
    assert!(stream.stream(1).failures.is_empty());
    assert!(stream.stream(2).batches.is_empty());
    assert!(stream.stream(2).frags.is_empty());
    assert!(stream.stream(2).offers.is_empty());
    assert!(stream.stream(2).failures.is_empty());
}
