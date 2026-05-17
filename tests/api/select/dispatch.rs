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

use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::ids::AscendingCount;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_streams::config::DispatchConfig;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::select::dispatch::DispatchSelector;
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
use constellation_streams::stream::LargeObjOfferStream;
use constellation_streams::stream::LargeObjStream;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::StreamReporter;

use crate::init;

#[test]
fn test_private_select_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let batch = stream
        .create_batch(&mut (), &mut (), &selections)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let retry = stream
        .create_batch(&mut (), &mut (), &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_complete_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_create_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut (),
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let retry = stream.start_batch(&mut ()).expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_complete_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

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

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_start_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .cancel_batch(&mut (), &mut (), &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .cancel_batch(&mut (), &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_cancel_batch(&mut (), &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_cancel_batch(&mut (), &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_cancel_batch(&mut (), &mut (), &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_cancel_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_cancel_batch(&mut (), &mut (), &batch, completable);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .finish_batch(&mut (), &mut (), &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .finish_batch(&mut (), &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_finish_batch(&mut (), &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut (), &mut (), &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_finish_batch(&mut (), &mut (), &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_finish_batch(&mut (), &mut (), &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut (), &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_finish_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_finish_batch(&mut (), &mut (), &batch, completable);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_abort_start_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    assert!(stream
        .abort_start_batch(&mut (), &mut (), permanent)
        .is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_abort_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut ());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    let retry = stream.abort_start_batch(&mut (), &mut (), permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::StartError]
    );

    assert!(stream
        .retry_abort_start_batch(&mut (), &mut (), retry)
        .is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .add(&mut (), &mut (), &"hello", &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .add(&mut (), &mut (), &"hello", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_add(&mut (), &mut (), &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut (), &mut (), &"nothing", &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut (), &"nothing", &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let res = stream
        .retry_add(&mut (), &mut (), &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_add(&mut (), &mut (), &"hello", &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_add_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream.start_batch(&mut ()).expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err =
        stream.complete_add(&mut (), &mut (), &"nothing", &batch, completable);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );

    let res = stream
        .push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let retry = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_complete_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(
            &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_complete_complete() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

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

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_frags_complete_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

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

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );

    let res = stream
        .push_offer(&mut (), hash_1.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let retry = stream
        .push_offer(&mut (), hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut (), hash.clone(), &mut frags, retry)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), ()))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut (), hash.clone(), &mut frags, retry)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_complete_complete() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_private_offer_complete_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_select_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());

    let res = if let RetryIndefResult::Success(res) = stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success")
    {
        res
    } else {
        panic!("Expected success")
    };

    assert_eq!(res, vec![1, 2]);

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let retry = stream
        .create_batch(&mut (), &mut (), &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_complete_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_create_batch(&mut (), &mut (), &selections, retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_create_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream
        .select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .is_ok());

    let err = stream.create_batch(&mut (), &mut (), &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut (),
        &mut (),
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_create_batch(&mut (), &mut (), &selections, completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let retry = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_complete_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_complete_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let batch = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

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

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_start_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .cancel_batch(&mut (), &mut false, &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .cancel_batch(&mut (), &mut false, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_cancel_batch(&mut (), &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut (), &mut false, &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.cancel_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_cancel_batch(&mut (), &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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

    let err =
        stream.complete_cancel_batch(&mut (), &mut false, &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_cancel_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.cancel_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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

    let err =
        stream.complete_cancel_batch(&mut (), &mut false, &batch, completable);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .finish_batch(&mut (), &mut false, &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![1, 2],
            msgs: vec![]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .finish_batch(&mut (), &mut false, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_finish_batch(&mut (), &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![1, 2],
            msgs: vec![]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut (), &mut false, &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.finish_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![1, 2],
            msgs: vec![]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_finish_batch(&mut (), &mut false, &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![1, 2],
            msgs: vec![]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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

    let err =
        stream.complete_finish_batch(&mut (), &mut false, &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_finish_batch(&mut (), &mut false, &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![1, 2],
            msgs: vec![]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_finish_batch_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.finish_batch(&mut (), &mut false, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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

    let err =
        stream.complete_finish_batch(&mut (), &mut false, &batch, completable);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_abort_start_batch_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    assert!(stream
        .abort_start_batch(&mut (), &mut false, permanent)
        .is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_abort_start_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let err = stream.start_batch(&mut (), vec![1, 2, 3].iter());
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    let retry = stream.abort_start_batch(&mut (), &mut false, permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::StartError]
    );

    assert!(stream
        .retry_abort_start_batch(&mut (), &mut false, retry)
        .is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_succeed() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let res = stream
        .add(&mut (), &mut false, &"hello", &batch)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![1, 2],
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    let retry = stream
        .add(&mut (), &mut false, &"hello", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_add(&mut (), &mut false, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![1, 2],
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut (), &mut false, &"nothing", &batch);
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_complete_success() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let err = stream.add(&mut (), &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut false, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(err.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![1, 2],
            msgs: vec!["hello"]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut (), &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut false, &"nothing", &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner
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
        .retry_add(&mut (), &mut false, &"hello", &batch, retry)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![1, 2],
            msgs: vec!["hello"]
        }]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_complete_complete() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut (), &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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

    let err =
        stream.complete_add(&mut (), &mut false, &"hello", &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        .complete_add(&mut (), &mut false, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![1, 2],
            msgs: vec!["hello"]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_add_complete_permanent() {
    init();

    let test_id = "test-stream";
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let batch = stream
        .start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let batch = if let RetryIndefResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(
        inner
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );

    let err = stream.add(&mut (), &mut false, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner
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
        &mut (),
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
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            msgs: vec![],
            parties: vec![1, 2]
        },]
    );
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );

    let res = stream
        .push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let retry = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_complete_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut (),
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

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64)]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(
            &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_complete_complete() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

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

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        )
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_frags_complete_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

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

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let res = stream
        .push_offer(&mut (), hash_0.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0.clone(),]
    );

    let res = stream
        .push_offer(&mut (), hash_1.clone(), &mut frags)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_retry_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let retry = stream
        .push_offer(&mut (), hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut (), hash.clone(), &mut frags, retry)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .offers
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");

    if let RetryIndefResult::Success(res) = res {
        assert_eq!(res, (Some(now), vec![1, 2, 3]))
    } else {
        panic!("Expected success")
    }

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_complete_retry() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let res = stream
        .retry_push_offer(&mut (), hash.clone(), &mut frags, retry)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_complete_complete() {
    init();

    let now = Instant::now();
    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash.clone(), &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());
    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}

#[test]
fn test_shared_offer_complete_permanent() {
    init();

    let test_id = "test-stream";
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
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
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let inner: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config).expect("Expected success");
    let res = stream
        .report_stream(&(), test_id, inner.clone())
        .expect("Expected success");

    assert!(res.is_none());

    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_push_offer(
        &mut (),
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
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );

    let (completable, permanent) = err.split();

    assert!(completable.is_none());
    assert!(permanent.is_some());

    assert!(inner
        .batches
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .frags
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert_eq!(
        inner.frags.try_borrow().expect("try_borrow failed").deref(),
        &vec![]
    );
    assert!(inner.failures.is_empty());
    assert!(inner
        .reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
    assert!(inner
        .batch_reports
        .try_borrow()
        .expect("try_borrow failed")
        .is_empty());
}
