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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Instant;
use std::vec::IntoIter;

use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use mio::Token;

use crate::addrs::test::TestEndpoint;
use crate::channels::ChannelParam;
use crate::channels::Channels;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TestStreamID {
    pub channel: String,
    pub param: TestChannelParam,
    pub endpoint: TestEndpoint
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestChannelParam {
    pub accepts: HashSet<TestEndpoint>
}

pub struct TestStream<Stream> {
    id: TestStreamID,
    stream: Stream,
    shutdown: Vec<
        Result<
            RetryResult<(Option<Vec<TestChannelParam>>, Option<Instant>)>,
            TestChannelsError
        >
    >
}

#[derive(Debug)]
pub struct TestChannelsError {
    pub scope: ErrorScope
}

pub struct TestChannels<Stream> {
    req_streams: HashMap<
        TestStreamID,
        Vec<(
            Result<
                RetryResult<(
                    Option<Stream>,
                    Option<Vec<TestChannelParam>>,
                    Option<Instant>
                )>,
                TestChannelsError
            >,
            Option<Instant>
        )>
    >,
    listen: Vec<
        Result<
            RetryResult<(
                Vec<TestStream<Stream>>,
                Vec<TestStreamID>,
                Option<Vec<(String, Option<Vec<TestChannelParam>>)>>,
                Option<Instant>
            )>,
            TestChannelsError
        >
    >,
    shutdown_listen: Vec<Result<Option<Option<Instant>>, TestChannelsError>>,
    pub actives: HashMap<
        (String, TestChannelParam),
        Vec<
            Result<
                RetryResult<(Option<Vec<TestChannelParam>>, Option<Instant>)>,
                TestChannelsError
            >
        >
    >
}

pub struct TestChannelsScript<Stream> {
    pub req_streams: Vec<(
        TestStreamID,
        Result<
            RetryResult<(
                Option<Stream>,
                Option<Vec<TestChannelParam>>,
                Option<Instant>
            )>,
            TestChannelsError
        >
    )>,
    pub listen: Vec<
        Result<
            RetryResult<(
                Vec<TestStream<Stream>>,
                Vec<TestStreamID>,
                Option<Vec<(String, Option<Vec<TestChannelParam>>)>>,
                Option<Instant>
            )>,
            TestChannelsError
        >
    >,
    pub shutdown_listen:
        Vec<Result<Option<Option<Instant>>, TestChannelsError>>
}

impl ChannelParam<TestEndpoint> for TestChannelParam {
    #[inline]
    fn accepts_addr(
        &self,
        addr: &TestEndpoint
    ) -> bool {
        self.accepts.contains(addr)
    }
}

impl Hash for TestChannelParam {
    fn hash<H>(
        &self,
        hash: &mut H
    ) where
        H: Hasher {
        let mut strs: Vec<&TestEndpoint> = self.accepts.iter().collect();

        strs.sort();
        strs.hash(hash);
    }
}

impl ScopedError for TestChannelsError {
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl<'a, Ctx, Stream> CreateWithParam<&'a mut Ctx> for TestChannels<Stream>
where
    Stream: Clone
{
    type Config = TestChannelsScript<Stream>;
    type CreateError = Infallible;

    fn create(
        config: Self::Config,
        _ctx: &'a mut Ctx
    ) -> Result<Self, Self::CreateError> {
        let TestChannelsScript {
            req_streams,
            mut listen,
            mut shutdown_listen
        } = config;
        let nreqs = req_streams.len();
        let mut reqs: HashMap<
            TestStreamID,
            Vec<(
                Result<
                    RetryResult<(
                        Option<Stream>,
                        Option<Vec<TestChannelParam>>,
                        Option<Instant>
                    )>,
                    TestChannelsError
                >,
                Option<Instant>
            )>
        > = HashMap::with_capacity(nreqs);
        let mut when: Option<Instant>;

        for (id, res) in req_streams {
            when = match res {
                Ok(RetryResult::Success((_, _, next))) => next,
                Ok(RetryResult::Retry(next)) => Some(next),
                _ => None
            };

            match reqs.entry(id) {
                Entry::Occupied(mut ent) => ent.get_mut().push((res, when)),
                Entry::Vacant(ent) => {
                    let mut streams = Vec::with_capacity(nreqs);

                    streams.push((res, when));
                    ent.insert(streams);
                }
            }
        }

        for streams in reqs.values_mut() {
            streams.reverse();
        }

        listen.reverse();
        shutdown_listen.reverse();

        Ok(TestChannels {
            req_streams: reqs,
            listen: listen,
            shutdown_listen: shutdown_listen,
            actives: HashMap::new()
        })
    }
}

impl<Ctx, Stream> Channels<Ctx> for TestChannels<Stream>
where
    Stream: Clone
{
    type Addr = TestEndpoint;
    type ChannelID = String;
    type OutNegoParam = ();
    type Param = TestChannelParam;
    type ParamsError = Infallible;
    type ParamsIter<I>
        = IntoIter<(
        String,
        RetryResult<(Vec<TestChannelParam>, Option<Instant>)>
    )>
    where
        I: Iterator<Item = Self::ChannelID>;
    type ReqStreamError = TestChannelsError;
    type Stream = Stream;

    fn req_stream(
        &mut self,
        _ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        endpoint: &Self::Addr,
        _nego_param: &Self::OutNegoParam
    ) -> Result<
        RetryResult<(
            Option<Self::Stream>,
            Option<Vec<Self::Param>>,
            Option<Instant>
        )>,
        Self::ReqStreamError
    > {
        let id = TestStreamID {
            channel: channel.clone(),
            param: param.clone(),
            endpoint: endpoint.clone()
        };

        self.req_streams
            .get_mut(&id)
            .expect("Expected script")
            .pop()
            .expect("Expected scripted action")
            .0
    }

    fn params<I>(
        &mut self,
        _ctx: &mut Ctx,
        channels: I
    ) -> Result<Self::ParamsIter<I>, Self::ParamsError>
    where
        I: Iterator<Item = Self::ChannelID> {
        let channels: HashSet<String> = channels.collect();
        let mut params: Vec<(
            String,
            RetryResult<(Vec<TestChannelParam>, Option<Instant>)>
        )> = Vec::with_capacity(self.req_streams.len());

        for (id, script) in self.req_streams.iter() {
            if channels.contains(&id.channel) {
                if let Some((_, when)) = script.last() {
                    params.push((
                        id.channel.clone(),
                        RetryResult::Success((vec![id.param.clone()], *when))
                    ));
                }
            }
        }

        Ok(params.into_iter())
    }

    fn channel_id(
        &self,
        name: &str
    ) -> Option<Self::ChannelID> {
        Some(name.to_string())
    }
}

impl<Stream, Ctx> ChannelsListen<Ctx> for TestChannels<Stream>
where
    Stream: Clone
{
    type EndpointIter = IntoIter<(TestEndpoint, String, TestChannelParam)>;
    type ListenError = TestChannelsError;
    type StreamIter =
        IntoIter<(TestEndpoint, String, TestChannelParam, Stream)>;

    fn listen(
        &mut self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<
        RetryResult<(
            Self::StreamIter,
            Self::EndpointIter,
            Option<Vec<(Self::ChannelID, Option<Vec<Self::Param>>)>>,
            Option<Instant>
        )>,
        Self::ListenError
    > {
        self.listen
            .pop()
            .expect("Expected scripted action")
            .map(|res| {
                res.map(|(streams, ids, refreshes, when)| {
                    let streams: Vec<(
                        TestEndpoint,
                        String,
                        TestChannelParam,
                        Stream
                    )> = streams
                        .into_iter()
                        .map(|mut val| {
                            let key =
                                (val.id.channel.clone(), val.id.param.clone());

                            val.shutdown.reverse();

                            if self.actives.insert(key, val.shutdown).is_some()
                            {
                                panic!("Stream {:?} already exists", val.id)
                            }

                            (
                                val.id.endpoint,
                                val.id.channel,
                                val.id.param,
                                val.stream
                            )
                        })
                        .collect();
                    let ids: Vec<(TestEndpoint, String, TestChannelParam)> =
                        ids.into_iter()
                            .map(|id| (id.endpoint, id.channel, id.param))
                            .collect();

                    if let Some(refreshes) = &refreshes {
                        let refreshes: HashSet<(String, TestChannelParam)> =
                            refreshes
                                .iter()
                                .flat_map(|(id, params)| {
                                    params.iter().flat_map(move |params| {
                                        params.iter().map(move |param| {
                                            (id.clone(), param.clone())
                                        })
                                    })
                                })
                                .collect();

                        self.actives.retain(|key, _| refreshes.contains(key));
                    }

                    (streams.into_iter(), ids.into_iter(), refreshes, when)
                })
            })
    }
}

impl<Stream, Ctx> ChannelsShutdown<Ctx> for TestChannels<Stream>
where
    Stream: Clone
{
    type ShutdownListenError = TestChannelsError;
    type ShutdownStreamError = TestChannelsError;

    fn shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        channel: &Self::ChannelID,
        param: &Self::Param,
        _session: Self::Stream
    ) -> Result<
        RetryResult<(Option<Vec<Self::Param>>, Option<Instant>)>,
        Self::ShutdownStreamError
    > {
        self.actives
            .get_mut(&(channel.clone(), param.clone()))
            .expect("Stream not active")
            .pop()
            .expect("Expected scripted action")
    }

    fn shutdown_listen(
        mut self,
        _ctx: &mut Ctx,
        _tokens: &HashSet<Token>
    ) -> Result<Option<(Self, Option<Instant>)>, Self::ShutdownListenError>
    {
        self.shutdown_listen
            .pop()
            .expect("Expected scripted action")
            .map(|res| res.map(|when| (self, when)))
    }
}

impl Display for TestChannelsError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test stream error")
    }
}

impl Display for TestChannelParam {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test param:")?;

        for name in self.accepts.iter() {
            write!(f, " {}", name)?;
        }

        Ok(())
    }
}

impl Display for TestStreamID {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(
            f,
            "test stream, channel: {}, {}, addr: {}",
            self.channel, self.param, self.endpoint
        )
    }
}
