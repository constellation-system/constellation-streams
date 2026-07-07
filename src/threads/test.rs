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
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::time::Instant;

use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::next_retry;
use mio::Token;

use crate::threads::PushMode;
use crate::threads::PushModeResult;

#[derive(Debug)]
pub struct TestError {
    pub scope: ErrorScope
}

pub struct SimpleTestStream {
    pub sends: Vec<String>
}

pub struct TestPushModeScriptElem {
    pub sends: Option<(Option<Instant>, Vec<String>)>,
    pub retries: Option<Box<(Instant, TestPushModeScriptElem)>>,
    pub indefs: Option<Box<TestPushModeScriptElem>>,
    pub completes: Option<Box<TestPushModeScriptElem>>
}

pub struct TestPushMode {
    script: Vec<TestPushModeScriptElem>,
    retries: Vec<(Instant, TestPushModeScriptElem)>,
    indefs: Vec<TestPushModeScriptElem>,
    completes: Vec<TestPushModeScriptElem>
}

impl<'a, Ctx> CreateWithParam<&'a mut Ctx> for TestPushMode {
    type Config = Vec<TestPushModeScriptElem>;
    type CreateError = Infallible;

    #[inline]
    fn create(
        mut config: Self::Config,
        _ctx: &'a mut Ctx
    ) -> Result<Self, Self::CreateError> {
        config.reverse();

        Ok(TestPushMode {
            script: config,
            retries: Vec::new(),
            indefs: Vec::new(),
            completes: Vec::new()
        })
    }
}

impl TestPushMode {
    fn process_script_elem(
        &mut self,
        stream: &mut SimpleTestStream,
        elem: TestPushModeScriptElem
    ) -> Option<Instant> {
        let TestPushModeScriptElem { sends, retries, indefs, completes } = elem;

        if let Some(retries) = retries {
            self.retries.push(*retries)
        }

        if let Some(indefs) = indefs {
            self.indefs.push(*indefs)
        }

        if let Some(completes) = completes {
            self.completes.push(*completes)
        }

        sends
            .and_then(|(next, mut sends)| {
                stream.sends.append(&mut sends);

                next
            })
    }
}

impl<Ctx> PushMode<SimpleTestStream, (), Ctx> for TestPushMode {
    type SendError = TestError;
    type RetryError = TestError;
    type RetryIndefError = TestError;

    fn send_from_outbound(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut SimpleTestStream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError> {
        let elem = self.script.pop().expect("Expected script element");
        let next_outbound = self.process_script_elem(stream, elem);
        let next_retry = self.retries.iter().map(|(when, _)| *when).min();

        Ok(PushModeResult {
            next_outbound: next_outbound,
            next_retry: next_retry,
            has_completes: !self.completes.is_empty()
        })
    }

    fn retry_pending(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut SimpleTestStream,
        _live: &HashSet<Token>,
        now: Instant
    ) -> Result<PushModeResult, Self::SendError> {
        let mut curr = None;
        let retries: Vec<_> = self.retries.drain(..).collect();

        for (when, elem) in retries {
            if when <= now {
                let next_outbound = self.process_script_elem(stream, elem);

                curr = next_retry(&curr, &next_outbound)
            } else {
                self.retries.push((when, elem))
            }
        }

        let next_retry = self.retries.iter().map(|(when, _)| *when).min();

        Ok(PushModeResult {
            next_outbound: curr,
            next_retry: next_retry,
            has_completes: !self.completes.is_empty()
        })
    }

    fn complete_pending(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut SimpleTestStream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError> {
        let mut curr = None;
        let completes: Vec<_> = self.completes.drain(..).collect();

        for elem in completes {
            let next_outbound = self.process_script_elem(stream, elem);

            curr = next_retry(&curr, &next_outbound)
        }

        let next_retry = self.retries.iter().map(|(when, _)| *when).min();

        Ok(PushModeResult {
            next_outbound: curr,
            next_retry: next_retry,
            has_completes: !self.completes.is_empty()
        })
    }

    fn retry_indefs(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut SimpleTestStream,
    ) -> Result<PushModeResult, Self::SendError> {
        let mut curr = None;
        let completes: Vec<_> = self.indefs.drain(..).collect();

        for elem in completes {
            let next_outbound = self.process_script_elem(stream, elem);

            curr = next_retry(&curr, &next_outbound)
        }

        let next_retry = self.retries.iter().map(|(when, _)| *when).min();

        Ok(PushModeResult {
            next_outbound: curr,
            next_retry: next_retry,
            has_completes: !self.completes.is_empty()
        })
    }
}

impl ScopedError for TestError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope.clone()
    }
}

impl Display for TestError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test error {}", self.scope)
    }
}
