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
use std::time::Instant;

use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_streams::addrs::test::TestAddrs;
use constellation_streams::addrs::test::TestAddrsScript;
use constellation_streams::addrs::test::TestEndpoint;
use constellation_streams::channels::test::TestChannel;
use constellation_streams::channels::test::TestChannelParam;
use constellation_streams::channels::test::TestChannels;
use constellation_streams::channels::test::TestChannelsError;
use constellation_streams::channels::test::TestChannelsScript;
use constellation_streams::config::ConnectionConfig;
use constellation_streams::config::FarSchedulerConfig;
use constellation_streams::config::PartyConfig;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::select::StreamSelector;
use constellation_streams::stream::LargeObjOfferStream;
use constellation_streams::stream::LargeObjStream;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::StreamID;
use constellation_streams::stream::StreamRefresh;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestSharedBatchState;
use constellation_streams::stream::test::TestSharedStream;
use constellation_streams::stream::test::TestSharedStreamScript;

use crate::init;

mod dispatch;

const TEST_CHANNEL_ID: &str = "test-channel";
const TEST_ENDPOINT: &str = "test-endpoint";

fn make_selector<Inner>(
    resolve: TestAddrsScript,
    channels: TestChannelsScript<Inner>
) -> (
    StreamSelector<AscendingCount<u128>, TestAddrs, TestChannels<Inner>>,
    TestChannels<Inner>
)
where
    Inner: Clone + PushStream<TestChannels<Inner>> {
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let connections = ConnectionConfig::new(
        vec![TEST_CHANNEL_ID.to_string()],
        vec![test_endpoint]
    );
    let mut channels =
        TestChannels::create(channels, &mut ()).expect("Expected success");
    let config = PartyConfig::new(
        FarSchedulerConfig::default(),
        resolve,
        <AscendingCount<u128> as Create>::Config::default(),
        Retry::default(),
        vec![connections],
        None
    );
    let stream: StreamSelector<
        AscendingCount<u128>,
        TestAddrs,
        TestChannels<Inner>
    > = StreamSelector::create(config, &mut channels)
        .expect("Expected success");

    (stream, channels)
}

#[test]
fn test_private_select_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .select(&mut channels, &mut selections)
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_select_req_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestPrivateStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![
                (test_stream_id.clone(), Ok(RetryResult::Retry(now))),
                (
                    test_stream_id,
                    Ok(RetryResult::Success((Some(inner.clone()), None, None)))
                ),
            ],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut channels, &mut selections)
        .expect("Expected success")
    {
        retry
    } else {
        panic!("expected retry")
    };

    let res = stream
        .retry_select(&mut channels, &mut selections, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_select_req_indef() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestPrivateStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![(
                test_stream_id,
                Ok(RetryResult::Success((None, None, None)))
            )],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let res = stream
        .select(&mut channels, &mut selections)
        .expect("Expected success");

    assert!(res.is_indef());

    assert!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.failures.is_empty());
    assert!(
        inner
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_select_req_error() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestPrivateStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![
                (
                    test_stream_id.clone(),
                    Err(TestChannelsError {
                        scope: ErrorScope::Session
                    })
                ),
                (
                    test_stream_id,
                    Ok(RetryResult::Success((Some(inner.clone()), None, None)))
                ),
            ],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut channels, &mut selections)
        .expect("Expected success")
    {
        assert!(retry.when() <= Instant::now());

        retry
    } else {
        panic!("expected retry")
    };

    let res = stream
        .retry_select(&mut channels, &mut selections, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    let res = stream
        .select(&mut channels, &mut selections)
        .expect("Expected success");

    assert!(res.is_success());

    let batch = stream
        .create_batch(&mut channels, &mut (), &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let retry = stream
        .create_batch(&mut channels, &mut (), &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_create_batch(&mut channels, &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_complete_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_create_batch(&mut channels, &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut channels,
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_create_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(stream.select(&mut channels, &mut selections).is_ok());

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut channels,
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream.start_batch(&mut channels).expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_start_batch(&mut channels, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_complete_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_start_batch(&mut channels, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut channels, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_start_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut channels, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .cancel_batch(&mut channels, &mut (), &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .cancel_batch(&mut channels, &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_cancel_batch(&mut channels, &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.cancel_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_cancel_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_cancel_batch(&mut channels, &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut channels,
        &mut (),
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_cancel_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_cancel_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut channels,
        &mut (),
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .finish_batch(&mut channels, &mut (), &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .finish_batch(&mut channels, &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_finish_batch(&mut channels, &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
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
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.finish_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_finish_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_finish_batch(&mut channels, &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut channels,
        &mut (),
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_finish_batch(&mut channels, &mut (), &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_finish_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut channels, &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut channels,
        &mut (),
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_abort_start_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    assert!(
        stream
            .abort_start_batch(&mut channels, &mut (), permanent)
            .is_success()
    );

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_abort_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
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
                when: now
            }),
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    let retry = stream.abort_start_batch(&mut channels, &mut (), permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    assert!(
        stream
            .retry_abort_start_batch(&mut channels, &mut (), retry)
            .is_success()
    );

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .add(&mut channels, &mut (), &"hello", &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .add(&mut channels, &mut (), &"hello", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_add(&mut channels, &mut (), &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut channels, &mut (), &"nothing", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let err = stream.add(&mut channels, &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_add(&mut channels, &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut channels, &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(&mut channels, &mut (), &"nothing", &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_add(&mut channels, &mut (), &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut channels, &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut channels,
        &mut (),
        &"hello",
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut channels, &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_add_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
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
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream.start_batch(&mut channels).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut channels, &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut channels,
        &mut (),
        &"nothing",
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .push_frags(&mut channels, LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );

    let res = stream
        .push_frags(&mut channels, LargeObjID::from(2 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream
        .push_frags(&mut channels, LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(2 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_complete_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_complete_complete() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_frags_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .push_offer(&mut channels, hash_0.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );

    let res = stream
        .push_offer(&mut channels, hash_1.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream
        .push_offer(&mut channels, hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut channels, hash.clone(), &mut frags, retry)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut channels, hash.clone(), &mut frags, retry)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_complete_complete() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut channels,
        hash.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_private_offer_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestPrivateStreamScript {
        select: vec![Ok(RetryIndefResult::Success(()))],
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut channels,
        hash.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_select_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_select_req_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestSharedStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![
                (test_stream_id.clone(), Ok(RetryResult::Retry(now))),
                (
                    test_stream_id,
                    Ok(RetryResult::Success((Some(inner.clone()), None, None)))
                ),
            ],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success")
    {
        retry
    } else {
        panic!("expected retry")
    };

    let res = stream
        .retry_select(&mut channels, &mut selections, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_select_req_indef() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestSharedStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![(
                test_stream_id,
                Ok(RetryResult::Success((None, None, None)))
            )],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let res = stream
        .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success");

    assert!(res.is_indef());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_select_req_error() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels: TestChannelsScript<TestSharedStream<&str, &str, SHA3ID>> =
        TestChannelsScript {
            req_streams: vec![
                (
                    test_stream_id.clone(),
                    Err(TestChannelsError {
                        scope: ErrorScope::Session
                    })
                ),
                (
                    test_stream_id,
                    Ok(RetryResult::Success((Some(inner.clone()), None, None)))
                ),
            ],
            listen: vec![],
            shutdown_listen: vec![]
        };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut selections = stream.empty_selections();

    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none())
    } else {
        panic!("Expected success");
    }

    let retry = if let RetryIndefResult::Retry(retry) = stream
        .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success")
    {
        assert!(retry.when() <= Instant::now());

        retry
    } else {
        panic!("expected retry")
    };

    let res = stream
        .retry_select(&mut channels, &mut selections, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    let res = stream
        .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success");

    assert!(res.is_success());

    let batch = stream
        .create_batch(&mut channels, &mut (), &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let retry = stream
        .create_batch(&mut channels, &mut (), &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_create_batch(&mut channels, &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_complete_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_create_batch(&mut channels, &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut channels,
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_create_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }
    let mut selections = stream.empty_selections();

    assert!(
        stream
            .select(&mut channels, &mut selections, vec![1, 2, 3].iter())
            .is_ok()
    );

    let err = stream.create_batch(&mut channels, &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut channels,
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut channels, &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_start_batch(&mut channels, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_complete_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let batch = stream
        .retry_start_batch(&mut channels, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut channels, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_start_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut channels, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut channels, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .cancel_batch(&mut channels, &mut false, &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .cancel_batch(&mut channels, &mut false, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_cancel_batch(&mut channels, &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.cancel_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_cancel_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_cancel_batch(&mut channels, &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut channels,
        &mut false,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_cancel_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_cancel_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![Err(TestError::Completable {
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut channels,
        &mut false,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .finish_batch(&mut channels, &mut false, &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .finish_batch(&mut channels, &mut false, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_finish_batch(&mut channels, &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.finish_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_finish_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_finish_batch(&mut channels, &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut channels,
        &mut false,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_finish_batch(&mut channels, &mut false, &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_finish_batch_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![Err(TestError::Completable {
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut channels, &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut channels,
        &mut false,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_abort_start_batch_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    assert!(
        stream
            .abort_start_batch(&mut channels, &mut false, permanent)
            .is_success()
    );

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_abort_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
                when: now
            }),
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.start_batch(&mut channels, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    let retry = stream.abort_start_batch(&mut channels, &mut false, permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    assert!(
        stream
            .retry_abort_start_batch(&mut channels, &mut false, retry)
            .is_success()
    );

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_succeed() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .add(&mut channels, &mut false, &"hello", &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec!["hello"],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .add(&mut channels, &mut false, &"hello", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_add(&mut channels, &mut false, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec!["hello"],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut channels, &mut false, &"nothing", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_complete_success() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let err = stream.add(&mut channels, &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream
        .complete_add(&mut channels, &mut false, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec!["hello"],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut channels, &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(
            &mut channels,
            &mut false,
            &"nothing",
            &batch,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let res = stream
        .retry_add(&mut channels, &mut false, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec!["hello"],
            parties: vec![1, 2]
        }]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_complete_complete() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut channels, &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut channels,
        &mut false,
        &"hello",
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut channels, &mut false, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec!["hello"],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_add_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![Err(TestError::Completable {
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
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let batch = stream
        .start_batch(&mut channels, vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut channels, &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut channels,
        &mut false,
        &"nothing",
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert_eq!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Success(Some(now))),
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .push_frags(&mut channels, LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );

    let res = stream
        .push_frags(&mut channels, LargeObjID::from(2 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream
        .push_frags(&mut channels, LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(2 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_complete_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_complete_complete() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut channels,
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_frags_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_frags(
        &mut channels,
        LargeObjID::from(1 as u64),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Success(Some(now))),
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let res = stream
        .push_offer(&mut channels, hash_0.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );

    let res = stream
        .push_offer(&mut channels, hash_1.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_retry_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let retry = stream
        .push_offer(&mut channels, hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut channels, hash.clone(), &mut frags, retry)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_complete_retry() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut channels, hash.clone(), &mut frags, retry)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_complete_complete() {
    init();

    let now = Instant::now();
    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut channels,
        hash.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(
            &mut channels,
            hash.clone(),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}

#[test]
fn test_shared_offer_complete_permanent() {
    init();

    let test_endpoint = TestEndpoint::from(TEST_ENDPOINT);
    let test_param = TestChannelParam {
        accepts: HashSet::from([test_endpoint.clone()])
    };
    let test_stream_id = StreamID::new(
        test_endpoint.clone(),
        TEST_CHANNEL_ID.to_string(),
        test_param
    );
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2]))],
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
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let inner = TestChannel::new(test_stream_id.clone(), inner, vec![]);
    let resolve = TestAddrsScript {
        addrs: vec![Ok(RetryResult::Success((
            vec![(test_endpoint, String::from(TEST_ENDPOINT))],
            None
        )))]
    };
    let channels = TestChannelsScript {
        req_streams: vec![(
            test_stream_id,
            Ok(RetryResult::Success((Some(inner.clone()), None, None)))
        )],
        listen: vec![],
        shutdown_listen: vec![]
    };
    let (mut stream, mut channels) = make_selector(resolve, channels);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream.refresh(&mut channels).expect("Expected success");

    if let RetryResult::Success(when) = res {
        assert!(when.is_none());
    } else {
        panic!("Expected success");
    }

    let err = stream.push_offer(&mut channels, hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut channels,
        hash.clone(),
        &mut frags,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(
        inner
            .inner()
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert_eq!(
        inner
            .inner()
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(inner.inner().failures.is_empty());
    assert!(
        inner
            .inner()
            .reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        inner
            .inner()
            .batch_reports
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
}
