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

use std::time::Duration;
use std::time::Instant;

use constellation_common::config::Create;
use constellation_common::ids::AscendingCount;
use constellation_common::hashid::SHA3ID;
use constellation_common::net::test::TestMsgsError;
use constellation_common::net::test::TestPrivateMsgs;
use constellation_common::retry::RetryIndefResult;
use constellation_streams::config::DispatchConfig;
use constellation_streams::select::dispatch::DispatchSelector;
use constellation_streams::stream::PushStream;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamPrivate;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::StreamReporter;
use constellation_streams::stream::test::TestPrivateBatchState;
use constellation_streams::stream::test::TestPrivateStream;
use constellation_streams::stream::test::TestPrivateStreamScript;
use constellation_streams::threads::PushMode;

use crate::init;

#[test]
fn test_private_select_succeed() {
    init();

    let test_id = "test-stream";
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
    let inner: TestPrivateStream<&str, &str, SHA3ID> =
        TestPrivateStream::new(script);
    let config = DispatchConfig::default();
    let mut stream: DispatchSelector<
        AscendingCount<u128>,
        &str,
        TestPrivateStream<&str, &str, SHA3ID>,
        ()
    > = DispatchSelector::create(config)
        .expect("Expected success");

    let res = stream.report_stream(&(), test_id, inner)
        .expect("Expected success");
    let mut selections = stream.empty_selections();

    assert!(res.is_none());
    assert!(stream.select(&mut (), &mut selections).is_ok());

    let mut streams = stream.take().expect("Expected success");

    assert_eq!(streams.len(), 1);

    let (id, stream) = streams.pop().expect("Expected some");

    assert_eq!(id, test_id);

    assert!(stream.batches.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.frags.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.offers.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.failures.is_empty());
    assert!(stream.reports.try_borrow().expect("try_borrow failed").is_empty());
    assert!(stream.batch_reports.try_borrow().expect("try_borrow failed").is_empty());
}
