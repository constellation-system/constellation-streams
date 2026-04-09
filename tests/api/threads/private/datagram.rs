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
use std::time::Duration;
use std::time::Instant;

use constellation_streams::config::PrivateDatagramModeConfig;
use constellation_common::config::Create;
use constellation_common::hashid::SHA3ID;
use constellation_common::net::test::TestPrivateMsgs;
use constellation_common::net::test::TestMsgsError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryIndefResult;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::threads::PushMode;
use constellation_streams::threads::private::PrivateDatagramPushMode;

#[test]
fn test_send_from_outbound_succeed() {
    let when = Instant::now() + Duration::from_secs(1);
    let script = TestPrivateStreamScript {
        select: vec![
            Ok(RetryIndefResult::Success(())),
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
    let mut stream: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let script: Vec<Result<(Option<Vec<&str>>, Option<Instant>),
                           TestMsgsError>> = vec![
        Ok((Some(vec!["hello"]), Some(when)))
    ];
    let mut msgs = TestPrivateMsgs::new(script);
    let config = PrivateDatagramModeConfig::default();
    let mut mode: PrivateDatagramPushMode<
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = PrivateDatagramPushMode::create(config)
        .expect("Expected success");
    let tokens = HashSet::new();

    let next = mode
        .send_from_outbound(&mut (), &mut msgs, &mut stream, &tokens)
        .expect("Expected success");

    assert_eq!(next, Some(when));

    assert_eq!(stream.batches.as_ref(),
               &vec![
                   TestPrivateBatchState::Finished {
                       msgs: vec!["hello"]
                   },
               ]);
    assert!(stream.frags.is_empty());
    assert!(stream.offers.is_empty());
    assert!(stream.failures.is_empty());
}
