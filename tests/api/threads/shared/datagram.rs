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
use std::ops::Deref;
use std::time::Duration;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::config::CreateWithParam;
use constellation_common::hashid::SHA3ID;
use constellation_common::net::test::TestSharedMsgs;
use constellation_common::net::test::TestMsgsError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryIndefResult;
use constellation_streams::config::SharedDatagramModeConfig;
use constellation_streams::stream::Parties;
use constellation_streams::stream::test::TestAbortRetry;
use constellation_streams::stream::test::TestAction;
use constellation_streams::stream::test::TestCompletableError;
use constellation_streams::stream::test::TestError;
use constellation_streams::stream::test::TestIndefPartiesAction;
use constellation_streams::stream::test::TestPermanentError;
use constellation_streams::stream::test::TestRetry;
use constellation_streams::stream::test::TestSharedBatchState;
use constellation_streams::stream::test::TestSharedStream;
use constellation_streams::stream::test::TestSharedStreamScript;
use constellation_streams::threads::PushMode;
use constellation_streams::threads::shared::SharedDatagramPushMode;

use crate::init;

#[test]
fn test_send_from_outbound_succeed() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>,
                            Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();
    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_retry_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_add_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_create_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_complete_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_create_complete_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_add_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_add_complete_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_retry_select_create_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_retry_select_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_retry_select_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_retry_create_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_retry_create_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_retry_add_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_select_indef() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_retry_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: later
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, later)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: later
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, later)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: later
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, later)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_create_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_complete_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_select_indef_create_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_select_indef_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_select_indef_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let later = when + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Indef(Parties::Some(vec![0, 1, 2]))),
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(later)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(later));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_indefs(&mut (), &mut msgs, &mut stream)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_retry_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_retry_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}


#[test]
fn test_send_from_outbound_complete_select_imm_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}


#[test]
fn test_send_from_outbound_complete_create_imm_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_retry_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_imm_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_retry_finish() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_complete_create_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_complete_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_complete_create_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_complete_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_complete_add_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_complete_add() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_imm_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
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
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_create_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_imm_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_imm_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_complete_finish_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_add_finish_create() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Finished {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_complete_select_imm_create_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select_create_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        cancel_batch: vec![],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select_imm_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select_imm_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::Retryable,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_select_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestIndefPartiesAction::Success {
                        parties: vec![0, 1, 2]
                    }
                }
            }),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_create_imm_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_create_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_create_imm_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_create_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_add_imm_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_complete_add_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_select_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert_eq!(stream.reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestPermanentError {
                       scope: ErrorScope::Session,
                   }
               ]);
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}

#[test]
fn test_send_from_outbound_create_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
        abort_start_batch: vec![
            RetryResult::Success(()),
        ],
        add: vec![],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_create_permanent_abort_retry() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::StartError,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Aborted,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_add_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_add_permanent_retry_cancel() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_add_permanent_complete_cancel_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_add_permanent_complete_cancel() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_add_permanent_cancel_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec![]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_finish_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_finish_permanent_retry_cancel() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Ok(RetryResult::Retry(TestRetry {
                when: when
            })),
            Ok(RetryResult::Success(())),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);

    let next = mode
        .retry_pending(&mut (), &mut msgs, &mut stream, &tokens, when)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_finish_permanent_complete_cancel_imm() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_finish_permanent_complete_cancel() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
        create_batch: vec![
            Ok(RetryResult::Success(())),
        ],
        cancel_batch: vec![
            Err(TestError::Completable {
                err: TestCompletableError {
                    scope: ErrorScope::WouldBlock,
                    action: TestAction::Success {
                        val: ()
                    }
                }
            }),
        ],
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>,
                            Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);

    let next = mode
        .complete_pending(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, None);

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Canceled,
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}

#[test]
fn test_send_from_outbound_finish_permanent_cancel_permanent() {
    init();

    let when = Instant::now() + Duration::from_secs(1);
    let script = TestSharedStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(vec![0, 1, 2])),
        ],
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
        finish_batch: vec![
            Err(TestError::Permanent {
                err: TestPermanentError {
                    scope: ErrorScope::Session,
                }
            })
        ],
        abort_start_batch: vec![],
        add: vec![
            Ok(RetryResult::Success(())),
        ],
        push_frags: vec![],
        push_offers: vec![],
        report_failure: vec![],
        inbound: vec![]
    };
    let mut stream: TestSharedStream<&str, &str, SHA3ID> =
        TestSharedStream::new(script, vec![0, 1, 2].into_iter());
    let script: Vec<Result<(Option<Vec<(Vec<usize>, Vec<&str>)>>,
                            Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec![(vec![0, 1, 2], vec!["hello"])]), Some(when)))
    ];
    let mut msgs = TestSharedMsgs::new(script);
    let config = SharedDatagramModeConfig::default();
    let mut mode: SharedDatagramPushMode<
        &str,
        TestSharedStream<&str, &str, SHA3ID>,
        ()
    > = SharedDatagramPushMode::create(config, &stream)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   TestSharedBatchState::Live {
                       parties: vec![0, 1, 2],
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert_eq!(stream.batch_reports.try_borrow().expect("try_borrow failed").deref(),
               &vec![
                   (0, TestPermanentError {
                       scope: ErrorScope::Session,
                   })
               ]);
}
