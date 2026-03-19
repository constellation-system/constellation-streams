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
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryIndefResult;
use constellation_streams::stream::PullStream;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamPartyID;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::PushStreamPrivateSingle;
use constellation_streams::stream::PushStreamReportBatchError;
use constellation_streams::stream::PushStreamReportError;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::PushStreamSharedSingle;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefAction;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestPermanentBatchError;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestStartBatchError;

#[test]
fn test_test_stream_private_pull() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let first = stream.pull();
    let second = stream.pull();
    let third = stream.pull();

    assert_eq!(first, Ok("hello"));
    assert_eq!(second, Ok("goodbye"));
    assert_eq!(third, Err(TestPermanentError {
        scope: ErrorScope::Session
    }));
}

#[test]
fn test_test_stream_private_select_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    assert_eq!(stream.select(&mut (), &mut ()),
               Ok(RetryIndefResult::Success(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_indef() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    assert_eq!(stream.select(&mut (), &mut ()),
               Ok(RetryIndefResult::Indef(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_retry_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let retry = stream.select(&mut (), &mut ())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.retry_select(&mut (), &mut (), retry),
               Ok(RetryIndefResult::Success(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_complete_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_select(&mut (), &mut (), completable),
               Ok(RetryIndefResult::Success(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_complete_retry() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_select(&mut (), &mut (), completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.retry_select(&mut (), &mut (), retry),
               Ok(RetryIndefResult::Success(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_complete_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_select_complete_complete() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.select(&mut (), &mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_select(&mut (), &mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_select(&mut (), &mut (), completable),
               Ok(RetryIndefResult::Success(())));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_create_batch_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_create_batch_retry_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let retry = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream.batches.is_empty());

    let batch = stream.retry_create_batch(&mut (), &mut (), &(), retry)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_create_batch_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_create_batch_complete_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_create_batch(&mut (), &mut (), &(), completable)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_create_batch_complete_retry() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_create_batch(&mut (), &mut (), &(), completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream.batches.is_empty());

    let batch = stream.retry_create_batch(&mut (), &mut (), &(), retry)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_create_batch_complete_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut (), &mut (), &(), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_create_batch_complete_complete() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.create_batch(&mut (), &mut (), &());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_create_batch(&mut (), &mut (), &(), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert!(stream.batches.is_empty());

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_create_batch(&mut (), &mut (), &(), completable)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.start_batch(&mut ())
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_select_retry_succeed() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let retry = stream.start_batch(&mut ())
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
fn test_test_stream_private_start_batch_create_retry_succeed() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let retry = stream.start_batch(&mut ())
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
fn test_test_stream_private_start_batch_both_retry_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let retry = stream.start_batch(&mut ())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert!(stream.batches.is_empty());

    let retry = stream.retry_start_batch(&mut (), retry)
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
fn test_test_stream_private_start_batch_indef() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let indef = stream.start_batch(&mut ()).expect("Expected success");

    assert!(indef.is_indef());

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_select_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.start_batch(&mut ());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, TestStartBatchError::Select {
        err: TestPermanentError {
            scope: ErrorScope::Session,
            ..
        },
        ..
    }));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_create_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.start_batch(&mut ());
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

    assert_eq!(stream.batches.as_ref(), &[TestPrivateBatchState::StartError]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_select_complete_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_create_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_both_complete_succeed() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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
fn test_test_stream_private_start_batch_select_complete_complete() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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
fn test_test_stream_private_start_batch_create_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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
fn test_test_stream_private_start_batch_both_complete_complete() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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
fn test_test_stream_private_start_batch_select_complete_indef() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_select_complete_retry() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    let batch = stream.retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_create_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    let batch = stream.retry_start_batch(&mut (), retry)
        .expect("Expected success");

    assert!(batch.is_success());

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
fn test_test_stream_private_start_batch_both_complete_retry() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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
fn test_test_stream_private_start_batch_select_complete_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert!(matches!(permanent, TestStartBatchError::Select {
        err: TestPermanentError {
            scope: ErrorScope::Session,
            ..
        },
        ..
    }));

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_create_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    assert_eq!(stream.batches.as_ref(), &[TestPrivateBatchState::StartError]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_start_batch_both_complete_permanent() {
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

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

    assert_eq!(stream.batches.as_ref(), &[TestPrivateBatchState::StartError]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.cancel_batch(&mut (), &mut (), &batch),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Canceled,
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let retry = stream.cancel_batch(&mut (), &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_cancel_batch(&mut (), &mut (), &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Canceled,
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
fn test_test_stream_private_cancel_batch_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_cancel_batch(&mut (), &mut (),
                                            &batch, completable),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Canceled,
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_cancel_batch(&mut (), &mut (),
                                             &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_cancel_batch(&mut (), &mut (), &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Canceled,
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(&mut (), &mut (),
                                           &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_cancel_batch(&mut (), &mut (),
                                            &batch, completable),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Canceled,
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_cancel_batch_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
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
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.cancel_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_cancel_batch(&mut (), &mut (),
                                           &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
fn test_test_stream_private_finish_batch_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.finish_batch(&mut (), &mut (), &batch),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let retry = stream.finish_batch(&mut (), &mut (), &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_finish_batch(&mut (), &mut (), &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
fn test_test_stream_private_finish_batch_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_finish_batch(&mut (), &mut (),
                                            &batch, completable),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_finish_batch(&mut (), &mut (),
                                             &batch, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_finish_batch(&mut (), &mut (), &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(&mut (), &mut (),
                                           &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_finish_batch(&mut (), &mut (),
                                            &batch, completable),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_finish_batch_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
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
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.finish_batch(&mut (), &mut (), &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_finish_batch(&mut (), &mut (),
                                           &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
fn test_test_stream_private_abort_start_batch_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.start_batch(&mut ());
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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::StartError
               ]);

    assert_eq!(stream.abort_start_batch(&mut (), &mut (), permanent),
               RetryResult::Success(()));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Aborted,
               ]);
}

#[test]
fn test_test_stream_private_abort_start_batch_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            }),
        ],
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let err = stream.start_batch(&mut ());
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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::StartError
               ]);

    let retry = stream.abort_start_batch(&mut (), &mut (), permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::StartError
               ]);

    assert_eq!(stream.retry_abort_start_batch(&mut (), &mut (), retry),
               RetryResult::Success(()));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Aborted,
               ]);
}

#[test]
fn test_test_stream_private_add_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.add(&mut (), &mut (), &"hello", &batch),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_add_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let retry = stream.add(&mut (), &mut (), &"goodbye", &batch)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_add(&mut (), &mut (), &"hello", &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_add_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"nothing", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
fn test_test_stream_private_add_complete_succeed() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_add_complete_retry() {
    let now = Instant::now();
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
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
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    assert_eq!(stream.retry_add(&mut (), &mut (), &"hello", &batch, retry),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_add_complete_complete() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
                                action: TestAction::Success {
                                    val: ()
                                }
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(&mut (), &mut (), &"hello",
                                  &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut (), &"hello", &batch, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_add_complete_permanent() {
    let script = TestPrivateStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
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
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);

    let batch = stream.create_batch(&mut (), &mut (), &())
        .expect("Expected success");
    let batch = if let RetryResult::Success(batch) = batch {
        batch
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let err = stream.add(&mut (), &mut (), &"hello", &batch);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Live {
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let err = stream.complete_add(&mut (), &mut (), &"nothing",
                                  &batch, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

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
/*

#[test]
fn test_test_stream_private_frags() {
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
            Ok(RetryIndefResult::Indef(Parties::All)),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(now)
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(1 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Success((Some(now), ()))));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
               ]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Success((Some(now), ()))));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Indef(Parties::All)));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let retry = stream.push_frags(&mut (), LargeObjID::from(1 as u64),
                                  &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let err = stream
        .retry_push_frags(&mut (), LargeObjID::from(1 as u64),
                          &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(&mut (), LargeObjID::from(1 as u64),
                             &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let err = stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(&mut (), LargeObjID::from(2 as u64),
                             &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let err = stream.retry_push_frags(&mut (), LargeObjID::from(2 as u64),
                                      &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_push_frags(&mut (), LargeObjID::from(2 as u64),
                             &mut frags, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    assert!(stream.batches.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_private_offer() {
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
            Ok(RetryIndefResult::Indef(Parties::All)),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(now)
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));

    assert_eq!(stream.push_offer(&mut (), hash_0.clone(), &mut frags),
               Ok(RetryIndefResult::Success((Some(now), ()))));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
               ]);

    assert_eq!(stream.push_offer(&mut (), hash_1.clone(), &mut frags),
               Ok(RetryIndefResult::Success((Some(now), ()))));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    assert_eq!(stream.push_offer(&mut (), hash_1.clone(), &mut frags),
               Ok(RetryIndefResult::Indef(Parties::All)));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let retry = stream.push_offer(&mut (), hash_1.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let err = stream
        .retry_push_offer(&mut (), hash_1.clone(), &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let err = stream.push_offer(&mut (), hash_1.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(&mut (), hash_1.clone(), &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let err = stream.retry_push_offer(&mut (), hash_1.clone(),
                                      &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_push_offer(&mut (), hash_1.clone(),
                             &mut frags, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.push_offer(&mut (), hash_1.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_pull() {
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());

    let first = stream.pull();
    let second = stream.pull();
    let third = stream.pull();

    assert_eq!(first, Ok("hello"));
    assert_eq!(second, Ok("goodbye"));
    assert_eq!(third, Err(TestPermanentError {
        scope: ErrorScope::Session
    }));
}

#[test]
fn test_test_stream_shared_select() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 3]))),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![1, 2, 3]
                    }
                }
            }),
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
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Indef {
                        parties: vec![1, 2, 3]
                    }
                }
            }),
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
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![1, 2, 3].into_iter());
    let mut selections = Vec::new();

    assert_eq!(stream.select(&mut (), &mut selections, vec![1, 2, 3].iter()),
               Ok(RetryIndefResult::Success(vec![1, 2])));
    assert_eq!(selections, vec![1, 2]);

    let mut selections = Vec::new();
    let retry = stream.select(&mut (), &mut selections, vec![1, 2, 3].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    let indef = stream.retry_select(&mut (), &mut selections, retry)
        .expect("Expected success");

    assert!(indef.is_indef());

    let mut selections = Vec::new();
    let err = stream.select(&mut (), &mut selections, vec![1, 2, 3].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    assert_eq!(stream.complete_select(&mut (), &mut selections, completable),
               Ok(RetryIndefResult::Success(vec![1, 2, 3])));
    assert_eq!(selections, vec![1, 2, 3]);

    let mut selections = Vec::new();
    let err = stream.select(&mut (), &mut selections, vec![1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_select(&mut (), &mut selections, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    let err = stream.retry_select(&mut (), &mut selections, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let indef = stream.complete_select(&mut (), &mut selections, completable)
        .expect("Expected success");
    assert!(indef.is_indef());

    let mut selections = Vec::new();
    let err = stream.select(&mut (), &mut selections, vec![0, 1].iter());
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
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let mut selections = Vec::new();
    let err = stream.select(&mut (), &mut selections, vec![0, 1].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_create_batch() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let batch = stream.create_batch(&mut (), &mut (), &vec![1, 2, 3])
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);

    let retry = stream.create_batch(&mut (), &mut (), &vec![0, 1, 2])
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_create_batch(&mut (), &mut (),
                                        &vec![0, 2, 3], retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_create_batch(&mut (), &mut (),
                                             &vec![0, 2, 3], completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    let err = stream.create_batch(&mut (), &mut (), &vec![2, 3]);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream.complete_create_batch(&mut (), &mut (),
                                             &vec![2, 3], completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    let err = stream.retry_create_batch(&mut (), &mut (),
                                        &vec![2, 3], retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_create_batch(&mut (), &mut (),
                                           &vec![2, 3], completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.create_batch(&mut (), &mut (), &vec![1, 2]);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 2, 3],
                       msgs: vec![]
                   }
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_start_batch() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let batch = stream.start_batch(&mut (), vec![0, 1, 2, 3].iter())
        .expect("Expected success");

    assert!(batch.is_success());
    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);

    let retry = stream.start_batch(&mut (), vec![1, 2, 3].iter())
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let batch = stream.complete_start_batch(&mut (), completable)
        .expect("Expected success");

    assert!(batch.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   }
               ]);

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   }
               ]);

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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   }
               ]);

    let err = stream.retry_start_batch(&mut (), retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   }
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream.complete_start_batch(&mut (), completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::StartError
               ]);

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

    let err = stream.start_batch(&mut (), vec![0, 2, 3].iter());
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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::StartError,
                   TestSharedBatchState::StartError
               ]);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_cancel_batch() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let batch_1 = stream.create_batch(&mut (), &mut (), &vec![0, 1, 2])
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);

    let mut flag = false;

    assert_eq!(stream.cancel_batch(&mut (), &mut flag, &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(flag);

    let batch_2 = stream.create_batch(&mut (), &mut (), &vec![1, 2])
        .expect("Expected success");
    let batch_2 = if let RetryResult::Success(batch_2) = batch_2 {
        batch_2
    } else {
        panic!("Expected success")
    };
    let mut flag = false;
    let retry = stream.cancel_batch(&mut (), &mut flag, &batch_2)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let err = stream.retry_cancel_batch(&mut (), &mut flag, &batch_2, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_cancel_batch(&mut (), &mut flag, &batch_2, completable)
        .expect("Expected success");

    assert!(res.is_success());
    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
               ]);
    assert!(flag);

    let batch_3 = stream.create_batch(&mut (), &mut (), &vec![1, 2, 3])
        .expect("Expected success");
    let batch_3 = if let RetryResult::Success(batch_3) = batch_3 {
        batch_3
    } else {
        panic!("Expected success")
    };
    let mut flag = false;
    let err = stream.cancel_batch(&mut (), &mut flag, &batch_3);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_cancel_batch(&mut (), &mut flag, &batch_3, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let err = stream.retry_cancel_batch(&mut (), &mut flag, &batch_3, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_cancel_batch(&mut (), &mut flag, &batch_3, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let batch_4 = stream.create_batch(&mut (), &mut (), &vec![0, 1, 3])
        .expect("Expected success");
    let batch_4 = if let RetryResult::Success(batch_4) = batch_4 {
        batch_4
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 3],
                       msgs: vec![]
                   }
               ]);

    let mut flag = false;
    let err = stream.cancel_batch(&mut (), &mut flag, &batch_4);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Canceled,
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 3],
                       msgs: vec![]
                   }
               ]);
    assert!(!flag);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_finish_batch() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let batch_1 = stream.create_batch(&mut (), &mut (), &vec![0, 1, 2])
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);

    let mut flag = false;

    assert_eq!(stream.finish_batch(&mut (), &mut flag, &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(flag);

    let batch_2 = stream.create_batch(&mut (), &mut (), &vec![1, 2, 3])
        .expect("Expected success");
    let batch_2 = if let RetryResult::Success(batch_2) = batch_2 {
        batch_2
    } else {
        panic!("Expected success")
    };
    let mut flag = false;
    let retry = stream.finish_batch(&mut (), &mut flag, &batch_2)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let mut flag = false;
    let err = stream.retry_finish_batch(&mut (), &mut flag, &batch_2, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_finish_batch(&mut (), &mut flag, &batch_2, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
               ]);
    assert!(flag);

    let batch_3 = stream.create_batch(&mut (), &mut (), &vec![1, 2])
        .expect("Expected success");
    let batch_3 = if let RetryResult::Success(batch_3) = batch_3 {
        batch_3
    } else {
        panic!("Expected success")
    };
    let mut flag = false;
    let err = stream.finish_batch(&mut (), &mut flag, &batch_3);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_finish_batch(&mut (), &mut flag, &batch_3, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let err = stream.retry_finish_batch(&mut (), &mut flag, &batch_3, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_finish_batch(&mut (), &mut flag, &batch_3, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let batch_4 = stream.create_batch(&mut (), &mut (), &vec![0, 1])
        .expect("Expected success");
    let batch_4 = if let RetryResult::Success(batch_4) = batch_4 {
        batch_4
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 1],
                       msgs: vec![]
                   }
               ]);

    let mut flag = false;
    let err = stream.finish_batch(&mut (), &mut flag, &batch_4);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Finished {
                       parties: vec![1, 2, 3],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![1, 2],
                       msgs: vec![]
                   },
                   TestSharedBatchState::Live {
                       parties: vec![0, 1],
                       msgs: vec![]
                   }
               ]);
    assert!(!flag);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_abort_start_batch() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            }),
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![
            RetryResult::Success(()),
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
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::StartError
               ]);

    let mut flag = false;

    assert_eq!(stream.abort_start_batch(&mut (), &mut flag, permanent),
               RetryResult::Success(()));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Aborted,
               ]);
    assert!(flag);

    let err = stream.start_batch(&mut (), vec![0, 1, 2].iter());
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

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Aborted,
                   TestSharedBatchState::StartError
               ]);

    let mut flag = false;
    let retry = stream.abort_start_batch(&mut (), &mut flag, permanent);
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Aborted,
                   TestSharedBatchState::StartError
               ]);
    assert!(!flag);

    assert_eq!(stream.retry_abort_start_batch(&mut (), &mut flag, retry),
               RetryResult::Success(()));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Aborted,
                   TestSharedBatchState::Aborted
               ]);
    assert!(flag);
}

#[test]
fn test_test_stream_shared_add() {
    let now = Instant::now();
    let script = TestSharedStreamScript {
        select: vec![],
        create_batch: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
            Ok(RetryResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());

    let batch_1 = stream.create_batch(&mut (), &mut (), &vec![0, 1, 2, 3])
        .expect("Expected success");
    let batch_1 = if let RetryResult::Success(batch_1) = batch_1 {
        batch_1
    } else {
        panic!("Expected success")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec![]
                   },
               ]);

    let mut flag = false;

    assert_eq!(stream.add(&mut (), &mut flag, &"hello", &batch_1),
               Ok(RetryResult::Success(())));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(flag);

    let mut flag = false;
    let retry = stream.add(&mut (), &mut flag, &"goodbye", &batch_1)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(!flag);

    let err = stream.retry_add(&mut (), &mut flag, &"goodbye", &batch_1, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_add(&mut (), &mut flag, &"goodbye", &batch_1, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(flag);

    let mut flag = false;
    let err = stream.add(&mut (), &mut flag, &"nothing", &batch_1);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_add(&mut (), &mut flag, &"nothing", &batch_1, completable)
        .expect("Expected success");
    let retry = if let RetryResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(!flag);

    let err = stream.retry_add(&mut (), &mut flag, &"nothing", &batch_1, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_add(&mut (), &mut flag, &"hello", &batch_1, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(!flag);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.add(&mut (), &mut flag, &"nothing", &batch_1);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2, 3],
                       msgs: vec!["hello", "goodbye"]
                   },
               ]);
    assert!(!flag);

    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_frags() {
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
            Ok(RetryIndefResult::Indef(Parties::All)),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(now)
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(1 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Success((Some(now), vec![0, 1, 2]))));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
               ]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Success((Some(now), vec![0, 1, 2]))));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    assert_eq!(stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                 &mut frags),
               Ok(RetryIndefResult::Indef(Parties::All)));

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let retry = stream.push_frags(&mut (), LargeObjID::from(1 as u64),
                                  &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let err = stream
        .retry_push_frags(&mut (), LargeObjID::from(1 as u64),
                          &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_frags(&mut (), LargeObjID::from(1 as u64),
                             &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let err = stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_frags(&mut (), LargeObjID::from(2 as u64),
                             &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let err = stream.retry_push_frags(&mut (), LargeObjID::from(2 as u64),
                                      &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_push_frags(&mut (), LargeObjID::from(2 as u64),
                             &mut frags, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.push_frags(&mut (), LargeObjID::from(2 as u64),
                                &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.frags.as_ref(),
               &vec![
                   LargeObjID::from(1 as u64),
                   LargeObjID::from(2 as u64),
                   LargeObjID::from(1 as u64),
               ]);

    assert!(stream.batches.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}

#[test]
fn test_test_stream_shared_offer() {
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
            Ok(RetryIndefResult::Indef(Parties::All)),
            Ok(RetryIndefResult::Retry(TestRetry {
                when: now
            })),
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefAction::Success {
                        val: Some(now)
                    }
                }
            }),
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
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        report_failure: vec![],
        inbound: vec![Ok("hello"), Ok("goodbye"),
                      Err(TestPermanentError {
                          scope: ErrorScope::Session
                      })]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2, 3].into_iter());
    let mut frags = OutboundFrags::new(Retry::default(), vec![0x55; 2048]);
    let hasher = SHA3Algo::default();
    let hash_0 = hasher.hash_bytes(once(&[0x00 as u8][..]));
    let hash_1 = hasher.hash_bytes(once(&[0x01 as u8][..]));

    assert_eq!(stream.push_offer(&mut (), hash_0.clone(), &mut frags),
               Ok(RetryIndefResult::Success((Some(now), vec![0, 1, 2, 3]))));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
               ]);

    assert_eq!(stream.push_offer(&mut (), hash_1.clone(), &mut frags),
               Ok(RetryIndefResult::Success((Some(now), vec![0, 1, 2, 3]))));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    assert_eq!(stream.push_offer(&mut (), hash_1.clone(), &mut frags),
               Ok(RetryIndefResult::Indef(Parties::All)));

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let retry = stream.push_offer(&mut (), hash_1.clone(), &mut frags)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let err = stream
        .retry_push_offer(&mut (), hash_1.clone(), &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let res = stream
        .complete_push_offer(&mut (), hash_0.clone(), &mut frags, completable)
        .expect("Expected success");

    assert!(res.is_success());

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let err = stream.push_offer(&mut (), hash_1.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());

    let retry = stream
        .complete_push_offer(&mut (), hash_1.clone(), &mut frags, completable)
        .expect("Expected success");
    let retry = if let RetryIndefResult::Retry(retry) = retry {
        retry
    } else {
        panic!("Expected retry")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let err = stream.retry_push_offer(&mut (), hash_1.clone(),
                                      &mut frags, retry);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let completable = completable.expect("Expected Some");

    assert!(permanent.is_none());
    let err = stream
        .complete_push_offer(&mut (), hash_1.clone(),
                             &mut frags, completable);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    let err = stream.push_offer(&mut (), hash_1.clone(), &mut frags);
    let err = if let Err(err) = err {
        err
    } else {
        panic!("Expected error")
    };

    let (completable, permanent) = err.split();
    let permanent = permanent.expect("Expected Some");

    assert!(completable.is_none());
    assert_eq!(permanent, TestPermanentError {
        scope: ErrorScope::Session,
    });

    assert_eq!(stream.offers.as_ref(),
               &vec![
                   hash_0.clone(),
                   hash_1.clone(),
                   hash_0.clone(),
               ]);

    assert!(stream.batches.is_empty());
    assert!(stream.frags.is_empty());
    assert!(stream.failures.is_empty());
}
*/
