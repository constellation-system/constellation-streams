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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNed;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::BasicAuthNed;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::cred::NullCred;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::shutdown::ShutdownFlag;
use mio::Token;
use mio::Waker;

use crate::addrs::test::TestEndpoint;
use crate::channels::test::TestStreamID;
use crate::channels::test::TestChannelParam;
use crate::channels::test::TestChannels;
use crate::channels::test::TestChannelsScript;
use crate::channels::test::TestChannelsError;
use crate::stream::PullStream;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::PushModeResult;
use crate::threads::dispatch::Dispatch;
use crate::threads::dispatch::Dispatched;
use crate::threads::dispatch::DispatchInboundTypes;
use crate::threads::dispatch::DispatchEntryTypes;
use crate::threads::dispatch::DispatchTypes;
use crate::threads::poll::PollThreadTypes;

pub struct TestDispatchScriptEntry {
    msgs: Arc<Mutex<Vec<BasicAuthNed<NullCred, String>>>>,
    stream_script: Vec<Result<RetryResult<Option<Instant>, TestRefreshRetry>,
                              TestRefreshError>>
}

pub struct TestDispatch {
    script: Vec<TestDispatchScriptEntry>
}

#[derive(Clone, Debug)]
pub struct TestError {
    pub scope: ErrorScope
}

#[derive(Clone)]
pub struct TestChannel {
    id: String,
    script: Arc<Mutex<Vec<Result<String, TestError>>>>
}

pub struct TestStream {
    pub sends: Arc<Mutex<Vec<String>>>,
    pub reports: HashSet<(TestStreamID, String)>,
    refresh_script: Vec<Result<RetryResult<Option<Instant>, TestRefreshRetry>,
                               TestRefreshError>>
}

#[derive(Clone)]
pub struct TestPushModeScriptElem {
    pub sends: Option<(Option<Instant>, Vec<String>)>,
    pub retries: Option<Box<(Instant, TestPushModeScriptElem)>>,
    pub indefs: Option<Box<TestPushModeScriptElem>>,
    pub completes: Option<Box<TestPushModeScriptElem>>
}

pub struct TestPushMode {
    script: Vec<Result<TestPushModeScriptElem, TestError>>,
    retries: Vec<(Instant, TestPushModeScriptElem)>,
    indefs: Vec<TestPushModeScriptElem>,
    completes: Vec<TestPushModeScriptElem>
}

#[derive(Default)]
pub struct TestRecv {
    pub msgs: Arc<Mutex<Vec<BasicAuthNed<NullCred, String>>>>
}

pub struct ThreadTestTypes;

#[derive(Clone, Debug)]
pub struct TestRefreshRetry {
    when: Instant,
    result: Arc<Result<RetryResult<Option<Instant>, TestRefreshRetry>,
                       TestRefreshError>>
}

#[derive(Clone, Debug)]
pub struct TestCompletableError {
    scope: ErrorScope,
    result: Arc<Result<RetryResult<Option<Instant>, TestRefreshRetry>,
                       TestRefreshError>>
}

#[derive(Clone, Debug)]
pub enum TestRefreshError {
    Completable {
        result: TestCompletableError
    },
    Permanent {
        err: TestError
    }
}

impl ScopedError for TestCompletableError {
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl RecoverableError for TestRefreshError {
    type Completable = TestCompletableError;
    type Permanent = TestError;

    fn split(
        self
    ) -> (Option<<TestRefreshError as RecoverableError>::Completable>,
          Option<<TestRefreshError as RecoverableError>::Permanent>) {
        match self {
            TestRefreshError::Completable { result } => (Some(result), None),
            TestRefreshError::Permanent { err } => (None, Some(err))
        }
    }
}

impl RetryWhen for TestRefreshRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

impl Create for TestDispatch {
    type Config = Vec<TestDispatchScriptEntry>;
    type CreateError = Infallible;

    #[inline]
    fn create(
        mut script: Vec<TestDispatchScriptEntry>,
    ) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestDispatch {
            script: script
        })
    }
}

impl<Ctx> Dispatch<ThreadTestTypes, Ctx> for TestDispatch {
    type PushStream = TestStream;
    type Msgs = ();
    type Recv = TestRecv;
    type DispatchError = Infallible;

    fn dispatch(
        &mut self,
        _ctx: &mut Ctx,
        _prin: &NullCred,
        _notify: Arc<Waker>
    ) -> Result<
        Dispatched<
            ThreadTestTypes,
            Self::PushStream,
            Self::Msgs,
            Self::Recv
        >,
        Self::DispatchError
    > {
        let TestDispatchScriptEntry { msgs, stream_script } =
            self.script.pop().expect("Expected scripted action");
        let Ok(stream) = TestStream::create(stream_script);
        let recv = TestRecv {
            msgs: msgs
        };

        Ok(Dispatched::new(
            ShutdownFlag::new(),
            stream,
            (),
            PassthruMsgAuthN::default(),
            recv
        ))
    }
}

impl Create for TestStream {
    type Config = Vec<Result<RetryResult<Option<Instant>, TestRefreshRetry>,
                             TestRefreshError>>;
    type CreateError = Infallible;

    fn create(mut script: Self::Config) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestStream {
            sends: Arc::new(Mutex::new(Vec::new())),
            reports: HashSet::new(),
            refresh_script: script,
        })
    }
}

impl CreateWithParam<String> for TestChannel {
    type Config = Vec<Result<String, TestError>>;
    type CreateError = Infallible;

    #[inline]
    fn create(
        mut script: Vec<Result<String, TestError>>,
        id: String
    ) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestChannel {
            id: id,
            script: Arc::new(Mutex::new(script))
        })
    }
}

impl<Ctx> StreamRefresh<Ctx> for TestStream {
    type RefreshRetry = TestRefreshRetry;
    type RefreshError = TestRefreshError;

    fn refresh(
        &mut self,
        _ctx: &mut Ctx
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        self.refresh_script.pop().expect("Expected scripted action")
    }

    fn retry_refresh(
        &mut self,
        _ctx: &mut Ctx,
        retry: Self::RefreshRetry
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        retry.result.as_ref().clone()
    }

    fn complete_refresh(
        &mut self,
        _ctx: &mut Ctx,
        errs: <Self::RefreshError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Option<Instant>, Self::RefreshRetry>,
        Self::RefreshError
    > {
        errs.result.as_ref().clone()
    }
}

impl PullStream<String> for TestChannel {
    type PullError = TestError;

    #[inline]
    fn pull(&mut self) -> Result<String, Self::PullError> {
        self.script.lock().expect("lock failed")
            .pop().expect("Expected scripted action")
    }
}

impl AuthNMsgRecv<NullCred, String, BasicAuthNed<NullCred, String>>
    for TestRecv {
    type RecvError = Infallible;

    #[inline]
    fn recv_auth_msg(
        &mut self,
        msg: BasicAuthNed<NullCred, String>
    ) -> Result<(), Self::RecvError> {
        self.msgs.lock().expect("lock failed").push(msg);

        Ok(())
    }
}

impl<Ctx> CreateWithParam<&'_ Ctx> for TestPushMode {
    type Config = Vec<Result<TestPushModeScriptElem, TestError>>;
    type CreateError = Infallible;

    #[inline]
    fn create(
        mut config: Self::Config,
        _ctx: &Ctx
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
        stream: &mut TestStream,
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
                stream.sends.lock().expect("lock failed").append(&mut sends);

                next
            })
    }
}

impl StreamReporter<NullCred, TestStreamID, BasicAuthNed<NullCred, TestChannel>>
    for TestStream {
    type ReportStreamError = Infallible;

    fn report_stream(
        &mut self,
        _party: &NullCred,
        id: TestStreamID,
        stream: BasicAuthNed<NullCred, TestChannel>
    ) -> Result<Option<BasicAuthNed<NullCred, TestChannel>>,
                Self::ReportStreamError> {
        if !self.reports.insert((id, stream.get().id.clone())) {
            Ok(Some(stream))
        } else {
            Ok(None)
        }
    }
}

impl<Ctx> PushMode<TestStream, (), Ctx> for TestPushMode {
    type SendError = TestError;
    type RetryError = TestError;
    type RetryIndefError = TestError;

    fn send_from_outbound(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut TestStream,
        _live: &HashSet<Token>
    ) -> Result<PushModeResult, Self::SendError> {
        let elem = self.script.pop().expect("Expected script element")?;
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
        stream: &mut TestStream,
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
        stream: &mut TestStream,
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
        stream: &mut TestStream,
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

impl<Ctx> PollThreadTypes<Ctx> for ThreadTestTypes
where
    Ctx: 'static + Send {
    type Addr = TestEndpoint;
    type ChannelParam = TestChannelParam;
    type ChannelID = String;
    type MsgPrin = NullCred;
    type SessionPrin = NullCred;
    type AuthNChan = BasicAuthNed<NullCred, TestChannel>;
    type Chan = TestChannel;
    type PullError = TestError;
    type RefreshRetry = TestRefreshRetry;
    type RefreshCompletableError = TestCompletableError;
    type RefreshPermanentError = TestError;
    type RefreshError = TestRefreshError;
    type Stream = TestStream;
    type InMsg = String;
    type AuthNMsg = BasicAuthNed<NullCred, String>;
    type Wrapper = String;
    type Msgs = ();
    type ChansConfig = TestChannelsScript<BasicAuthNed<NullCred, TestChannel>>;
    type ChansCreateError = Infallible;
    type ChanShutdownRetry = Instant;
    type ChanShutdownError = TestChannelsError;
    type Chans = TestChannels<BasicAuthNed<NullCred, TestChannel>>;
    type MsgAuthConfig = ();
    type MsgAuth = PassthruMsgAuthN<String, NullCred>;
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type RecvError = Infallible;
    type Recv = TestRecv;
    type ModeConfig = Vec<Result<TestPushModeScriptElem, TestError>>;
    type ModeCreateError = Infallible;
    type Mode = TestPushMode;
}

impl DispatchInboundTypes for ThreadTestTypes {
    type InMsg = String;
    type Wrapper = String;
    type OutMsg = String;
    type SessionPrin = NullCred;
    type MsgPrin = NullCred;
    type AuthNMsg = BasicAuthNed<NullCred, String>;
    type MsgAuthError = Infallible;
    type MsgAuth = PassthruMsgAuthN<String, NullCred>;
}

impl<Ctx> DispatchEntryTypes<Ctx> for ThreadTestTypes
where
    Ctx: 'static + Send {
    type Addr = TestEndpoint;
    type ChannelParam = TestChannelParam;
    type ChannelID = String;
    type PullError = TestError;
    type RefreshRetry = TestRefreshRetry;
    type RefreshCompletableError = TestCompletableError;
    type RefreshPermanentError = TestError;
    type RefreshError = TestRefreshError;
    type ReportStreamError = Infallible;
    type Stream = TestStream;
    type Msgs = ();
    type RecvError = Infallible;
    type Recv = TestRecv;
    type Chan = TestChannel;
    type AuthNChan = BasicAuthNed<NullCred, TestChannel>;
    type ModeConfig = Vec<Result<TestPushModeScriptElem, TestError>>;
    type ModeCreateError = Infallible;
    type Mode = TestPushMode;
    type ChansConfig = TestChannelsScript<BasicAuthNed<NullCred, TestChannel>>;
    type ChansCreateError = Infallible;
    type ChanShutdownRetry = Instant;
    type ChanShutdownError = TestChannelsError;
    type Chans = TestChannels<BasicAuthNed<NullCred, TestChannel>>;
}

impl<Ctx> DispatchTypes<Ctx> for ThreadTestTypes
where
    Ctx: 'static + Send {
    type Disp = TestDispatch;
    type DispatchError = Infallible;
}
