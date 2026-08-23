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
use std::vec::IntoIter;

use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;

use crate::addrs::Addrs;
use crate::addrs::AddrsCreate;
use crate::select::OutboundEndpointConfig;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TestEndpoint(String);

#[derive(Clone, Default)]
pub struct TestAddrsScript {
    pub addrs: Vec<
        Result<
            RetryResult<(Vec<(TestEndpoint, String)>, Option<Instant>)>,
            TestAddrsError
        >
    >
}

pub struct TestAddrs {
    addrs: Vec<
        Result<
            RetryResult<(Vec<(TestEndpoint, String)>, Option<Instant>)>,
            TestAddrsError
        >
    >,
    origins: HashSet<String>,
    curr: Option<(Vec<(TestEndpoint, String)>, Option<Instant>, Instant)>
}

#[derive(Clone, Debug)]
pub struct TestAddrsError {
    scope: ErrorScope
}

impl From<String> for TestEndpoint {
    #[inline]
    fn from(val: String) -> TestEndpoint {
        TestEndpoint(val)
    }
}

impl From<TestEndpoint> for String {
    #[inline]
    fn from(val: TestEndpoint) -> String {
        val.0
    }
}

impl From<&'_ str> for TestEndpoint {
    #[inline]
    fn from(val: &str) -> TestEndpoint {
        TestEndpoint(val.to_string())
    }
}

impl AsRef<str> for TestEndpoint {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl OutboundEndpointConfig<()> for TestEndpoint {
    #[inline]
    fn outbound_nego_param(&self) {}
}

impl<Ctx> AddrsCreate<Ctx> for TestAddrs {
    type Config = TestAddrsScript;
    type CreateError = Infallible;
    type OriginConfig = TestEndpoint;

    #[inline]
    fn create<I>(
        _ctx: &mut Ctx,
        mut script: Self::Config,
        origins: I
    ) -> Result<Self, Self::CreateError>
    where
        I: Iterator<Item = Self::OriginConfig> {
        let origins: HashSet<String> =
            origins.map(|endpoint| endpoint.0).collect();

        script.addrs.reverse();

        Ok(TestAddrs {
            addrs: script.addrs,
            origins: origins,
            curr: None
        })
    }
}

impl Addrs for TestAddrs {
    type Addr = TestEndpoint;
    type AddrsError = TestAddrsError;
    type AddrsIter = IntoIter<(Self::Addr, Self::Origin, Instant)>;
    type Origin = String;

    #[inline]
    fn refresh_when(&self) -> Option<Instant> {
        self.curr.as_ref().and_then(|(_, when, _)| *when)
    }

    fn addrs(
        &mut self
    ) -> Result<RetryResult<(Self::AddrsIter, Option<Instant>)>, Self::AddrsError>
    {
        let now = Instant::now();

        let (addrs, next, last) = match &self.curr {
            Some((addrs, None, last)) => (addrs, None, *last),
            Some((addrs, Some(when), last)) if *when > now => {
                (addrs, Some(*when), *last)
            }
            _ => match self.addrs.pop().expect("Expected scripted action")? {
                RetryResult::Success((mut addrs, when)) => {
                    addrs.retain(|(_, origin)| self.origins.contains(origin));
                    self.curr = Some((addrs, when, now));

                    if let Some((addrs, _, _)) = &self.curr {
                        (addrs, when, now)
                    } else {
                        panic!("Shouldn't be empty")
                    }
                }
                RetryResult::Retry(when) => {
                    return Ok(RetryResult::Retry(when));
                }
            }
        };

        let out: Vec<(Self::Addr, Self::Origin, Instant)> = addrs
            .iter()
            .map(|(addr, origin)| (addr.clone(), origin.clone(), last))
            .collect();

        Ok(RetryResult::Success((out.into_iter(), next)))
    }
}

impl ScopedError for TestAddrsError {
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl Display for TestAddrsError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test stream error")
    }
}

impl Display for TestEndpoint {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}
