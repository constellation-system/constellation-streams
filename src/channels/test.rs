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

use constellation_auth::authn::AuthNed;
use constellation_auth::cred::NullCred;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashID;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::WithRetryWhen;
use mio::Token;

use crate::addrs::test::TestEndpoint;
use crate::channels::ChannelParam;
use crate::channels::Channels;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::large_obj::LargeObjID;
use crate::stream::LargeObjStream;
use crate::stream::LargeObjOfferStream;
use crate::stream::Parties;
use crate::stream::PullStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamShared;
use crate::stream::StreamID;

pub type TestStreamID = StreamID<TestEndpoint, String, TestChannelParam>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestChannelParam {
    pub accepts: HashSet<TestEndpoint>
}

#[derive(Clone)]
pub struct TestChannel<Stream> {
    id: TestStreamID,
    stream: Stream,
    shutdown: Vec<
        Result<
            RetryResult<(Option<Vec<TestChannelParam>>, Option<Instant>)>,
            TestChannelsError
        >
    >
}

#[derive(Clone, Debug)]
pub struct TestChannelsError {
    pub scope: ErrorScope
}

pub struct TestChannels<Stream> {
    req_streams: HashMap<
        TestStreamID,
        Vec<(
            Result<
                RetryResult<(
                    Option<TestChannel<Stream>>,
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
                Vec<TestChannel<Stream>>,
                Vec<TestStreamID>,
                Option<Vec<(String, Option<Vec<TestChannelParam>>)>>,
                Option<Instant>
            )>,
            TestChannelsError
        >
    >,
    shutdown_listen: Vec<Result<Option<Option<Instant>>, TestChannelsError>>,
    pub actives: HashSet<(String, TestChannelParam)>
}

pub struct TestChannelsScript<Stream> {
    pub req_streams: Vec<(
        TestStreamID,
        Result<
            RetryResult<(
                Option<TestChannel<Stream>>,
                Option<Vec<TestChannelParam>>,
                Option<Instant>
            )>,
            TestChannelsError
        >
    )>,
    pub listen: Vec<
        Result<
            RetryResult<(
                Vec<TestChannel<Stream>>,
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

impl<Stream> AuthNed<NullCred, TestChannel<Stream>> for TestChannel<Stream> {
    #[inline]
    fn prin(&self) -> &NullCred {
        &NullCred
    }

    #[inline]
    fn get(&self) -> &Self {
        self
    }

    #[inline]
    fn get_mut(&mut self) -> &mut Self {
        self
    }

    #[inline]
    fn take(self) -> (NullCred, Self) {
        (NullCred, self)
    }
}

impl<Stream> TestChannel<Stream> {
    pub fn new(
        id: TestStreamID,
        stream: Stream,
        mut shutdown: Vec<
            Result<
                RetryResult<(Option<Vec<TestChannelParam>>, Option<Instant>)>,
                TestChannelsError
            >
        >
    ) -> Self {
        shutdown.reverse();

        TestChannel {
            id: id,
            stream: stream,
            shutdown: shutdown
        }
    }

    pub fn inner(&self) -> &Stream {
        &self.stream
    }
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

impl<Stream> TestChannel<Stream> {
    pub fn id(&self) -> &TestStreamID {
        &self.id
    }
}

impl<Stream, Ctx> PushStream<Ctx> for TestChannel<Stream>
where Stream: PushStream<Ctx> {
    type BatchID = Stream::BatchID;
    type CancelBatchError = Stream::CancelBatchError;
    type CancelBatchRetry = Stream::CancelBatchRetry;
    type FinishBatchError = Stream::FinishBatchError;
    type FinishBatchRetry = Stream::FinishBatchRetry;
    type StreamFlags = Stream::StreamFlags;
    type ReportError = Stream::ReportError;

    fn finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError> {
        self.stream.finish_batch(ctx, flags, batch)
    }

    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError> {
        self.stream.retry_finish_batch(ctx, flags, batch, retry)
    }

    fn complete_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError> {
        self.stream.complete_finish_batch(ctx, flags, batch, err)
    }

    fn cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError> {
        self.stream.cancel_batch(ctx, flags, batch)
    }

    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError> {
        self.stream.retry_cancel_batch(ctx, flags, batch, retry)
    }

    fn complete_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError> {
        self.stream.complete_cancel_batch(ctx, flags, batch, err)
    }

    fn cancel_batches(&mut self) {
        self.stream.cancel_batches()
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        self.stream.report_failure(batch)
    }
}

impl<T, Stream, Ctx> PushStreamAdd<T, Ctx> for TestChannel<Stream>
where Stream: PushStreamAdd<T, Ctx> {
    type AddError = Stream::AddError;
    type AddRetry = Stream::AddRetry;

    fn add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.stream.add(ctx, flags, msg, batch)
    }

    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.stream.retry_add(ctx, flags, msg, batch, retry)
    }

    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &T,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.stream.complete_add(ctx, flags, msg, batch, err)
    }
}

impl<Stream, Ctx> PushStreamPrivate<Ctx> for TestChannel<Stream>
where Stream: PushStreamPrivate<Ctx> {
    type SelectError = Stream::SelectError;
    type SelectRetry = Stream::SelectRetry;
    type CreateBatchError = Stream::CreateBatchError;
    type CreateBatchRetry = Stream::CreateBatchRetry;
    type StartBatchError = Stream::StartBatchError;
    type StartBatchRetry = Stream::StartBatchRetry;
    type AbortBatchRetry = Stream::AbortBatchRetry;
    type Selections = Stream::Selections;
    type StartBatchStreamBatches = Stream::StartBatchStreamBatches;

    fn select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        self.stream.select(ctx, selections)
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        self.stream.retry_select(ctx, selections, retry)
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        self.stream.complete_select(ctx, selections, err)
    }

    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.create_batch(ctx, batches, selections)
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.retry_create_batch(ctx, batches, selections, retry)
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.complete_create_batch(ctx, batches, selections, err)
    }

    fn start_batch(
        &mut self,
        ctx: &mut Ctx
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.stream.start_batch(ctx)
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.stream.retry_start_batch(ctx, retry)
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.stream.complete_start_batch(ctx, err)
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        self.stream.abort_start_batch(ctx, flags, err)
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        self.stream.retry_abort_start_batch(ctx, flags, retry)
    }
}

impl<Stream> PushStreamPartyID for TestChannel<Stream>
where Stream: PushStreamPartyID {
    type PartyID = Stream::PartyID;
}

impl<Stream, Ctx> PushStreamShared<Ctx> for TestChannel<Stream>
where Stream: PushStreamShared<Ctx> {
    type SelectError = Stream::SelectError;
    type SelectRetry = Stream::SelectRetry;
    type CreateBatchError = Stream::CreateBatchError;
    type CreateBatchRetry = Stream::CreateBatchRetry;
    type StartBatchError = Stream::StartBatchError;
    type StartBatchRetry = Stream::StartBatchRetry;
    type AbortBatchRetry = Stream::AbortBatchRetry;
    type Selections = Stream::Selections;
    type StartBatchStreamBatches = Stream::StartBatchStreamBatches;
    type BatchPartiesIter = Stream::BatchPartiesIter;
    type BatchPartiesError = Stream::BatchPartiesError;
    type IndefParties = Stream::IndefParties;

    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError> {
        self.stream.batch_parties(batch_id)
    }

    fn select<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        self.stream.select(ctx, selections, parties)
    }

    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        self.stream.retry_select(ctx, selections, retry)
    }

    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Vec<Self::PartyID>,
            Self::SelectRetry,
            Parties<Self::IndefParties>
        >,
        Self::SelectError
    > {
        self.stream.complete_select(ctx, selections, err)
    }

    fn create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.create_batch(ctx, batches, selections)
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.retry_create_batch(ctx, batches, selections, retry)
    }

    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.stream.complete_create_batch(ctx, batches, selections, err)
    }

    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        self.stream.start_batch(ctx, parties)
    }

    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        self.stream.retry_start_batch(ctx, retry)
    }

    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            Self::BatchID,
            Self::StartBatchRetry,
            Parties<Self::IndefParties>
        >,
        Self::StartBatchError
    > {
        self.stream.complete_start_batch(ctx, err)
    }

    fn abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        self.stream.abort_start_batch(ctx, flags, err)
    }

    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        self.stream.retry_abort_start_batch(ctx, flags, retry)
    }
}

impl<Stream, Ctx> LargeObjStream<Ctx> for TestChannel<Stream>
where Stream: LargeObjStream<Ctx> {
    type PushFragError = Stream::PushFragError;
    type PushFragRetry = Stream::PushFragRetry;
    type Frags = Stream::Frags;
    type Parties = Stream::Parties;

    fn push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.stream.push_frags(ctx, id, frags)
    }

    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.stream.retry_push_frags(ctx, id, frags, retry)
    }

    fn complete_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushFragRetry,
            Parties<Self::Parties>
        >,
        Self::PushFragError
    > {
        self.stream.complete_push_frags(ctx, id, frags, err)
    }
}

impl<H, Stream, Ctx> LargeObjOfferStream<H, Ctx> for TestChannel<Stream>
where Stream: LargeObjOfferStream<H, Ctx>,
      H: HashID {
    type PushOfferError = Stream::PushOfferError;
    type PushOfferRetry = Stream::PushOfferRetry;

    fn push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.stream.push_offer(ctx, hash, frags)
    }

    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.stream.retry_push_offer(ctx, hash, frags, retry)
    }

    fn complete_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<
            (Option<Instant>, Self::Parties),
            Self::PushOfferRetry,
            Parties<Self::Parties>
        >,
        Self::PushOfferError
    > {
        self.stream.complete_push_offer(ctx, hash, frags, err)
    }
}

impl<Stream, Msg> PullStream<Msg> for TestChannel<Stream>
where Stream: PullStream<Msg> {
    type PullError = Stream::PullError;

    #[inline]
    fn pull(&mut self) -> Result<Msg, Self::PullError> {
        self.stream.pull()
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
                        Option<TestChannel<Stream>>,
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
            actives: HashSet::new()
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
    type Stream = TestChannel<Stream>;

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
        let id = StreamID::new(endpoint.clone(), channel.clone(),
                               param.clone());

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
            if channels.contains(id.channel()) {
                if let Some((_, when)) = script.last() {
                    params.push((
                        id.channel().clone(),
                        RetryResult::Success((vec![id.param().clone()], *when))
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
        IntoIter<(TestEndpoint, String, TestChannelParam, TestChannel<Stream>)>;

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
                        TestChannel<Stream>
                    )> = streams
                        .into_iter()
                        .map(|val| {
                            let key =
                                (val.id.channel().clone(),
                                 val.id.param().clone());

                            let _ = self.actives.insert(key);

                            (
                                val.id.party_addr().clone(),
                                val.id.channel().clone(),
                                val.id.param().clone(),
                                val
                            )
                        })
                        .collect();
                    let ids: Vec<(TestEndpoint, String, TestChannelParam)> =
                        ids.into_iter()
                        .map(|id| (id.party_addr().clone(),
                                   id.channel().clone(),
                                   id.param().clone()))
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

                        self.actives.retain(|key| refreshes.contains(key));
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
    type ShutdownStreamRetry = WithRetryWhen<Self::Stream>;

    fn shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        mut session: Self::Stream
    ) -> Result<
        RetryResult<(Option<Vec<Self::Param>>, Option<Instant>),
                    Self::ShutdownStreamRetry>,
        Self::ShutdownStreamError
    > {
        session
            .shutdown
            .pop()
            .expect("Expected scripted action")
            .map(|res| res.map_retry(|when| WithRetryWhen::new(session, when)))
    }

    fn retry_shutdown_stream(
        &mut self,
        _ctx: &mut Ctx,
        _channel: &Self::ChannelID,
        _param: &Self::Param,
        retry: Self::ShutdownStreamRetry
    ) -> Result<
        RetryResult<
            (Option<Vec<Self::Param>>, Option<Instant>),
            Self::ShutdownStreamRetry
        >,
        Self::ShutdownStreamError
    > {
        let (mut session, _) = retry.take();

        session
            .shutdown
            .pop()
            .expect("Expected scripted action")
            .map(|res| res.map_retry(|when| WithRetryWhen::new(session, when)))
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
