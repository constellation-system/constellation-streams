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
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_streams::channels::SharedPrivateChannelStream;
use constellation_streams::channels::SharedPrivateMatchError;
use constellation_streams::channels::SharedPrivateStreamCaches;
use constellation_streams::stream::Parties;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestIndefPartiesAction;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPermanentBatchError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestPartiesRetry;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestSharedBatchState;
use constellation_streams::stream::test::TestSharedStream;
use constellation_streams::stream::test::TestSharedStreamScript;
use constellation_streams::stream::test::TestStartBatchError;


#[test]
fn test_private_select_succeed() {
    let script = TestPrivateStreamScript {
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

    assert!(matches!(stream.select(&mut (), &mut batches),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}


#[test]
fn test_private_select_indef() {
    let script = TestPrivateStreamScript {
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

    let indef = stream.select(&mut (), &mut batches)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_retry_succeed() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
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

    let retry = stream.select(&mut (), &mut batches)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(stream.retry_select(&mut (), &mut batches, retry),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_permanent() {
    let script = TestPrivateStreamScript {
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
    assert!(matches!(permanent,
                     SharedPrivateMatchError::Private {
                         err: TestPermanentError { scope: ErrorScope::Session }
                     }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: ()
                    }
                }
            }),
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

    assert!(matches!(stream.complete_select(&mut (), &mut batches, completable),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
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

    let retry = stream.complete_select(&mut (), &mut batches, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(stream.retry_select(&mut (), &mut batches, retry),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![
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
    assert!(matches!(permanent,
                     SharedPrivateMatchError::Private {
                         err: TestPermanentError { scope: ErrorScope::Session }
                     }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_select_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestIndefAction::Success {
                                    val: ()
                                }
                            }
                        })
                    }
                }
            }),
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
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_select(&mut (), &mut batches, completable),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_succeed() {
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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

    assert!(matches!(stream.select(&mut (), &mut batches),
                     Ok(RetryIndefResult::Success(()))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_indef() {
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 3]))),
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

    let indef = stream.select(&mut (), &mut batches)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_retry_succeed() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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

    let retry = stream.select(&mut (), &mut batches)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(stream.retry_select(&mut (), &mut batches, retry),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_permanent() {
    let script = TestSharedStreamScript {
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
    assert!(matches!(permanent,
                     SharedPrivateMatchError::Shared {
                         err: TestPermanentError { scope: ErrorScope::Session }
                     }));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_succeed() {
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

    assert!(matches!(stream.complete_select(&mut (), &mut batches, completable),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_retry() {
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

    let retry = stream.complete_select(&mut (), &mut batches, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(matches!(stream.retry_select(&mut (), &mut batches, retry),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_permanent() {
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
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
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent,
                     SharedPrivateMatchError::Shared {
                         err: TestPermanentError { scope: ErrorScope::Session }
                     }));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_select_complete_complete() {
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
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
            }),
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
    let err = stream.complete_select(&mut (), &mut batches, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_select(&mut (), &mut batches, completable),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
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

    assert!(matches!(stream.create_batch(&mut batches, &mut flags, &selections),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_retry_succeed() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
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

    let retry = stream.create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    assert!(matches!(stream.retry_create_batch(&mut batches, &mut flags,
                                               &selections, retry),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Private {
        err: TestPermanentError {
            scope: ErrorScope::Session,
        }
    }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_create_batch(&mut batches, &mut flags,
                                                  &selections, completable),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry {
                            when: now
                        }
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

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_create_batch(&mut batches, &mut flags,
                                             &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    assert!(matches!(stream.retry_create_batch(&mut batches, &mut flags,
                                               &selections, retry),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut batches, &mut flags,
                                           &selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Private {
        err: TestPermanentError {
            scope: ErrorScope::Session,
        }
    }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_create_batch_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestAction::Success {
                                    val: ()
                                }
                            }
                        })
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut batches, &mut flags,
                                           &selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_create_batch(&mut batches, &mut flags,
                                                  &selections, completable),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_succeed() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
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

    assert!(matches!(stream.create_batch(&mut batches, &mut flags, &selections),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_retry_succeed() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
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

    let retry = stream.create_batch(&mut batches, &mut flags, &selections)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    assert!(matches!(stream.retry_create_batch(&mut batches, &mut flags,
                                               &selections, retry),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_permanent() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Shared {
        err: TestPermanentError {
            scope: ErrorScope::Session,
        }
    }));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_succeed() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_create_batch(&mut batches, &mut flags,
                                                  &selections, completable),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_retry() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry {
                            when: now
                        }
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

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_create_batch(&mut batches, &mut flags,
                                             &selections, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    assert!(matches!(stream.retry_create_batch(&mut batches, &mut flags,
                                               &selections, retry),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_permanent() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut batches, &mut flags,
                                           &selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Shared {
        err: TestPermanentError {
            scope: ErrorScope::Session,
        }
    }));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_shared_create_batch_complete_complete() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestAction::Success {
                                    val: ()
                                }
                            }
                        })
                    }
                }
            }),
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

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut batches, &mut flags,
                                           &selections, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected shared")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert!(matches!(stream.complete_create_batch(&mut batches, &mut flags,
                                                  &selections, completable),
                     Ok(RetryResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Shared {
        stream, ..
    } = stream {
        stream
    } else {
        panic!("Expected shared")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
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

    assert!(matches!(stream.start_batch(&mut ()),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_retry_succeed() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
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

    let retry = stream.start_batch(&mut ())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    let retry = stream.retry_start_batch(&mut (), retry)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    if let SharedPrivateChannelStream::Private {
        stream
    } = &stream {
        assert!(stream.batches.is_empty());
    } else {
        panic!("Expected private")
    };

    assert!(matches!(stream.retry_start_batch(&mut (), retry),
                     Ok(RetryIndefResult::Success(_))));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_indef() {
    let script = TestPrivateStreamScript {
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

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_select_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        create_batch: vec![
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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Private {
        err: TestStartBatchError::Select {
            err: TestPermanentError {
                scope: ErrorScope::Session,
                ..
            },
            ..
        }
    }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_create_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, SharedPrivateMatchError::Private {
        err: TestStartBatchError::Create {
            err: TestPermanentBatchError {
                scope: ErrorScope::Session,
                ..
            },
            ..
        }
    }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(), &[TestPrivateBatchState::StartError]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

/*
#[test]
fn test_private_start_batch_both_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestIndefAction::Success {
                                    val: ()
                                }
                            }
                        })
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Completable {
                            err: TestCompletableError {
                                scope: ErrorScope::Retryable,
                                action: TestAction::Success {
                                    val: ()
                                }
                            }
                        })
                    }
                }
            }),
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

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_select_complete_indef() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Indef
                }
            }),
        ],
        create_batch: vec![
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

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let indef = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(indef.is_indef());

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
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
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Retry {
                        retry: TestRetry {
                            when: now
                        }
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

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream.batches.is_empty());

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream.batches.is_empty());

    let batch = stream.retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_private_start_batch_both_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: ()
                    }
                }
            }),
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Error {
                        err: Box::new(TestError::Permanent {
                            err: TestPermanentError {
                                scope: ErrorScope::Session,
                            }
                        })
                    }
                }
            }),
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

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

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
    assert!(matches!(permanent, TestStartBatchError::Create {
        err: TestPermanentBatchError {
            scope: ErrorScope::Session,
            ..
        },
        ..
    }));

    let stream = if let SharedPrivateChannelStream::Private {
        stream
    } = stream {
        stream
    } else {
        panic!("Expected private")
    };

    assert_eq!(stream.batches.as_ref(), &[TestPrivateBatchState::StartError]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}
*/
