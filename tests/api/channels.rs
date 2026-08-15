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

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::hashid::SHA3ID;
use constellation_common::retry::Retry;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_streams::channels::SharedPrivateChannelStream;
use constellation_streams::channels::SharedPrivateMatchError;
use constellation_streams::channels::SharedPrivateStreamCaches;
use constellation_streams::channels::SharedPrivateStreamParties;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::stream::LargeObjOfferStream;
use constellation_streams::stream::LargeObjStream;
use constellation_streams::stream::Parties;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestIndefPartiesAction;
use constellation_streams::stream::test::TestPartiesRetry;
use constellation_streams::stream::test::TestPermanentBatchError;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestSharedBatchState;
use constellation_streams::stream::test::TestSharedStream;
use constellation_streams::stream::test::TestSharedStreamScript;
use constellation_streams::stream::test::TestStartBatchError;

use crate::init;

#[test]
fn test_private_select_succeed() {
    init();

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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    assert!(matches!(
        stream.select(&mut (), &mut batches),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_indef() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let indef = stream
        .select(&mut (), &mut batches)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let retry = stream
        .select(&mut (), &mut batches)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(
        stream.retry_select(&mut (), &mut batches, retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_permanent() {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_succeed() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_select(&mut (), &mut batches, completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_select(&mut (), &mut batches, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(
        stream.retry_select(&mut (), &mut batches, retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_select(&mut (), &mut batches, completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_succeed() {
    init();

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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    assert!(matches!(
        stream.select(&mut (), &mut batches),
        Ok(RetryIndefResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_indef() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 3])))],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let indef = stream
        .select(&mut (), &mut batches)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(vec![2, 3])),
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let retry = stream
        .select(&mut (), &mut batches)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(
        stream.retry_select(&mut (), &mut batches, retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_permanent() {
    init();

    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Success {
                    parties: vec![1, 2, 3]
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_select(&mut (), &mut batches, completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Retry {
                        retry: TestPartiesRetry {
                            parties: vec![2, 3],
                            when: now
                        }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(vec![1, 2, 3])),
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_select(&mut (), &mut batches, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(
        stream.retry_select(&mut (), &mut batches, retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Error {
                    err: Box::new(TestError::Permanent {
                        err: TestPermanentError {
                            scope: ErrorScope::Session
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefPartiesAction::Success {
                                parties: vec![1, 2, 3]
                            }
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        stream: test_stream,
        party: 0
    };
    let mut batches = SharedPrivateStreamCaches::default();

    let err = stream.select(&mut (), &mut batches);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_select(&mut (), &mut batches, completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected private")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    assert!(matches!(
        stream.create_batch(&mut batches, &mut flags, &selections),
        Ok(RetryResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let retry = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_create_batch(&mut batches, &mut flags, &selections, retry),
        Ok(RetryResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        ),
        Ok(RetryResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_create_batch(&mut batches, &mut flags, &selections, retry),
        Ok(RetryResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut batches,
        &mut flags,
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut batches,
        &mut flags,
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        ),
        Ok(RetryResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    assert!(matches!(
        stream.create_batch(&mut batches, &mut flags, &selections),
        Ok(RetryResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let retry = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_create_batch(&mut batches, &mut flags, &selections, retry),
        Ok(RetryResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        ),
        Ok(RetryResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        )
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_create_batch(&mut batches, &mut flags, &selections, retry),
        Ok(RetryResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut batches,
        &mut flags,
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();

    let err = stream.create_batch(&mut batches, &mut flags, &selections);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(
        &mut batches,
        &mut flags,
        &selections,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_create_batch(
            &mut batches,
            &mut flags,
            &selections,
            completable
        ),
        Ok(RetryResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_succeed() {
    init();

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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    assert!(matches!(
        stream.start_batch(&mut ()),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let retry = stream.start_batch(&mut ()).expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let retry = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_start_batch(&mut (), retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_indef() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let indef = stream.start_batch(&mut ()).expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_select_permanent() {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestStartBatchError::Select {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_create_permanent() {
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_succeed() {
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_start_batch(&mut (), completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_start_batch(&mut (), completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_select_complete_indef() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let indef = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_start_batch(&mut (), retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success { val: () }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected private")
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
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestPrivateBatchState::StartError]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_succeed() {
    init();

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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    assert!(matches!(
        stream.start_batch(&mut ()),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![0],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_both_retry_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry { when: now })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2, 3])),
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let retry = stream.start_batch(&mut ()).expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let retry = stream
        .retry_start_batch(&mut (), retry)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_start_batch(&mut (), retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![0],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_indef() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Indef(Parties::All))],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let indef = stream.start_batch(&mut ()).expect("Expected success");

    assert!(indef.is_indef());

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_create_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2, 3]))],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_both_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Success {
                    parties: vec![0, 1, 2, 3]
                }
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_start_batch(&mut (), completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![0],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_both_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Error {
                    err: Box::new(TestError::Completable {
                        err: TestCompletableError {
                            scope: ErrorScope::Retryable,
                            action: TestIndefPartiesAction::Success {
                                parties: vec![0, 1, 2, 3]
                            }
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_start_batch(&mut (), completable),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![0],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_select_complete_indef() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Err(TestError::Completable {
            err: TestCompletableError {
                scope: ErrorScope::Retryable,
                action: TestIndefPartiesAction::Indef {
                    parties: vec![0, 1, 2]
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let indef = stream
        .complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_both_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Retry {
                        retry: TestPartiesRetry {
                            parties: vec![0, 1, 2, 3],
                            when: now
                        }
                    }
                }
            }),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_start_batch(&mut (), retry),
        Ok(RetryIndefResult::Success(_))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![0],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_start_batch_both_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![1, 2, 3]
                    }
                }
            }),
            Ok(RetryIndefResult::Success(vec![1, 2])),
        ],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .is_empty()
        );
    } else {
        panic!("Expected shared")
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
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &[TestSharedBatchState::StartError]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.cancel_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let retry = stream
        .cancel_batch(&mut (), &mut flags, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_cancel_batch(&mut (), &mut flags, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_complete_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_cancel_batch(&mut (), &mut flags, &batch, completable),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_cancel_batch(&mut (), &mut flags, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_cancel_batch(&mut (), &mut flags, &batch, completable),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_cancel_batch_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.cancel_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.cancel_batch(&mut (), &mut selected, &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let retry = stream
        .cancel_batch(&mut (), &mut selected, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_cancel_batch(&mut (), &mut selected, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.cancel_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.cancel_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_cancel_batch(
            &mut (),
            &mut selected,
            &batch,
            completable
        ),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.cancel_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut (), &mut selected, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_cancel_batch(&mut (), &mut selected, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.cancel_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut (),
        &mut selected,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_cancel_batch(
            &mut (),
            &mut selected,
            &batch,
            completable
        ),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Canceled,]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_cancel_batch_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.cancel_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(
        &mut (),
        &mut selected,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.finish_batch(&mut (), &mut flags, &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let retry = stream
        .finish_batch(&mut (), &mut flags, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_finish_batch(&mut (), &mut flags, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_complete_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_finish_batch(&mut (), &mut flags, &batch, completable),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut (), &mut flags, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_finish_batch(&mut (), &mut flags, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_finish_batch(&mut (), &mut flags, &batch, completable),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Finished { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_finish_batch_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.finish_batch(&mut (), &mut flags, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.finish_batch(&mut (), &mut selected, &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let retry = stream
        .finish_batch(&mut (), &mut selected, &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_finish_batch(&mut (), &mut selected, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.finish_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.finish_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_finish_batch(
            &mut (),
            &mut selected,
            &batch,
            completable
        ),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.finish_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut (), &mut selected, &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_finish_batch(&mut (), &mut selected, &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.finish_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut (),
        &mut selected,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_finish_batch(
            &mut (),
            &mut selected,
            &batch,
            completable
        ),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Finished {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_finish_batch_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.finish_batch(&mut (), &mut selected, &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(
        &mut (),
        &mut selected,
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_abort_start_batch_succeed() {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::StartError]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.abort_start_batch(&mut (), &mut flags, permanent),
        RetryResult::Success(())
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
}

#[test]
fn test_private_abort_start_batch_retry() {
    init();

    let now = Instant::now();
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::StartError]
        );
    } else {
        panic!("Expected private")
    };

    let retry = stream.abort_start_batch(&mut (), &mut flags, permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::StartError]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_abort_start_batch(&mut (), &mut flags, retry),
        RetryResult::Success(())
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Aborted,]
    );
}

#[test]
fn test_shared_abort_start_batch_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2, 3]))],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };
    let mut selections = SharedPrivateStreamCaches::default();
    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::StartError]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.abort_start_batch(&mut (), &mut selections, permanent),
        RetryResult::Success(())
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
}

#[test]
fn test_shared_abort_start_batch_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![Ok(RetryIndefResult::Success(vec![0, 1, 2, 3]))],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };
    let mut selections = SharedPrivateStreamCaches::default();
    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestStartBatchError::Create {
                err: TestPermanentBatchError {
                    scope: ErrorScope::Session,
                    ..
                },
                ..
            }
        }
    ));

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::StartError]
        );
    } else {
        panic!("Expected shared")
    };

    let retry = stream.abort_start_batch(&mut (), &mut selections, permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::StartError]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_abort_start_batch(&mut (), &mut selections, retry),
        RetryResult::Success(())
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Aborted,]
    );
}

#[test]
fn test_private_add_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.add(&mut (), &mut flags, &"hello", &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let retry = stream
        .add(&mut (), &mut flags, &"goodbye", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_add(&mut (), &mut flags, &"hello", &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.add(&mut (), &mut flags, &"nothing", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_complete_succeed() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(&mut (), &mut flags, &"nothing", &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    assert!(matches!(
        stream.retry_add(&mut (), &mut flags, &"hello", &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_complete_complete() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
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
            }),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut flags, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live {
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_add_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut flags = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let err = stream.add(&mut (), &mut flags, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestPrivateBatchState::Live { msgs: vec![] },]
        );
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut (),
        &mut flags,
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
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestPrivateBatchState::Live { msgs: vec![] },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.add(&mut (), &mut selected, &"hello", &batch),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let retry = stream
        .add(&mut (), &mut selected, &"goodbye", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_add(&mut (), &mut selected, &"hello", &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.add(&mut (), &mut selected, &"nothing", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_complete_succeed() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.add(&mut (), &mut selected, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut selected, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.add(&mut (), &mut selected, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(&mut (), &mut selected, &"nothing", &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.retry_add(&mut (), &mut selected, &"hello", &batch, retry),
        Ok(RetryResult::Success(()))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_complete_complete() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![Ok(RetryResult::Success(()))],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
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
            }),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.add(&mut (), &mut selected, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut (),
        &mut selected,
        &"hello",
        &batch,
        completable
    );
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut selected, &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec!["hello"]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_add_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut batches: SharedPrivateStreamCaches<(), Vec<usize>> =
        SharedPrivateStreamCaches::default();
    let mut flags = SharedPrivateStreamCaches::default();
    let selections = SharedPrivateStreamCaches::default();
    let batch = stream
        .create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };
    let mut selected = SharedPrivateStreamCaches::default();

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let err = stream.add(&mut (), &mut selected, &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .batches
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![TestSharedBatchState::Live {
                parties: vec![],
                msgs: vec![]
            },]
        );
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(
        &mut (),
        &mut selected,
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
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![TestSharedBatchState::Live {
            parties: vec![],
            msgs: vec![]
        },]
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![LargeObjID::from(1 as u64),]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_indef() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::All))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags),
        Ok(RetryIndefResult::Indef(Parties::All))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let retry = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_permanent() {
    init();

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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_complete_succeed() {
    init();

    let now = Instant::now();
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
                action: TestIndefAction::Success { val: Some(now) }
            }
        })],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_complete_retry() {
    init();

    let now = Instant::now();
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
            Ok(RetryIndefResult::Success((Some(now), ()))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_complete_permanent() {
    init();

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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_frags_complete_complete() {
    init();

    let now = Instant::now();
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![LargeObjID::from(1 as u64),]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64), LargeObjID::from(2 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_indef() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![Ok(RetryIndefResult::Indef(Parties::All))],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert!(matches!(
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags),
        Ok(RetryIndefResult::Indef(Parties::All))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let retry = stream
        .push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_permanent() {
    init();

    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(2 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_complete_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    assert!(matches!(
        stream.retry_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            retry
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
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
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_frags_complete_complete() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    let err =
        stream.push_frags(&mut (), LargeObjID::from(1 as u64), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    assert!(matches!(
        stream.complete_push_frags(
            &mut (),
            LargeObjID::from(1 as u64),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![LargeObjID::from(1 as u64),]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));

    assert!(matches!(
        stream.push_offer(&mut (), hash_0.clone(), &mut frags),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![hash_0.clone(),]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.push_offer(&mut (), hash_1.clone(), &mut frags),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_indef() {
    init();

    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::All))],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    assert!(matches!(
        stream.push_offer(&mut (), hash.clone(), &mut frags),
        Ok(RetryIndefResult::Indef(Parties::All))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = stream
        .push_offer(&mut (), hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_permanent() {
    init();

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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_offer(
            &mut (),
            hash.clone(),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_complete_permanent() {
    init();

    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Private {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_offer_complete_complete() {
    init();

    let now = Instant::now();
    let script = TestPrivateStreamScript {
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
    let test_stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Private {
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

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

    if let SharedPrivateChannelStream::Private { stream } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected private")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    assert!(matches!(
        stream.complete_push_offer(
            &mut (),
            hash.clone(),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((
            Some(_),
            SharedPrivateStreamParties::Private { parties: () }
        )))
    ));

    let stream = if let SharedPrivateChannelStream::Private { stream } = stream
    {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert!(matches!(
        stream.push_offer(&mut (), hash_0.clone(), &mut frags),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .offers
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![hash_0.clone(),]
        );
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.push_offer(&mut (), hash_1.clone(), &mut frags),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash_0, hash_1]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_indef() {
    init();

    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![Ok(RetryIndefResult::Indef(Parties::All))],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(
        stream.push_offer(&mut (), hash.clone(), &mut frags),
        Ok(RetryIndefResult::Indef(Parties::All))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let retry = stream
        .push_offer(&mut (), hash.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash.clone()]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_permanent() {
    init();

    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_complete_succeed() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_offer(
            &mut (),
            hash.clone(),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_complete_retry() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
            Ok(RetryIndefResult::Success(Some(now))),
        ],
        report_failure: vec![],
        inbound: vec![]
    };
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    assert!(matches!(
        stream.retry_push_offer(&mut (), hash.clone(), &mut frags, retry),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_complete_permanent() {
    init();

    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(
        permanent,
        SharedPrivateMatchError::Shared {
            err: TestPermanentError {
                scope: ErrorScope::Session
            }
        }
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_offer_complete_complete() {
    init();

    let now = Instant::now();
    let script = TestSharedStreamScript {
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
    let test_stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut stream: SharedPrivateChannelStream<
        TestPrivateStream<&str, &str, SHA3ID>,
        TestSharedStream<&str, &str, SHA3ID>,
        usize
    > = SharedPrivateChannelStream::Shared {
        party: 0,
        stream: test_stream
    };
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash = hasher.hash_bytes(once(&[0x00 as u8][..]));

    let err = stream.push_offer(&mut (), hash.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

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

    if let SharedPrivateChannelStream::Shared { stream, .. } = &stream {
        assert_eq!(
            stream
                .frags
                .try_borrow()
                .expect("try_borrow failed")
                .deref(),
            &vec![]
        );
    } else {
        panic!("Expected shared")
    }

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(
        stream.complete_push_offer(
            &mut (),
            hash.clone(),
            &mut frags,
            completable
        ),
        Ok(RetryIndefResult::Success((Some(_), _)))
    ));

    let stream =
        if let SharedPrivateChannelStream::Shared { stream, .. } = stream {
            stream
        } else {
            panic!("Expected shared")
        };

    assert_eq!(
        stream
            .offers
            .try_borrow()
            .expect("try_borrow failed")
            .deref(),
        &vec![hash]
    );
    assert!(
        stream
            .batches
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(
        stream
            .frags
            .try_borrow()
            .expect("try_borrow failed")
            .is_empty()
    );
    assert!(stream.failures.is_empty());
}
