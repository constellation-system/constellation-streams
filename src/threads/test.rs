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
use constellation_common::retry::WithRetryWhen;
use constellation_common::retry::next_retry;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use mio::Token;

use crate::addrs::test::TestEndpoint;
use crate::channels::test::TestChannel;
use crate::channels::test::TestChannelParam;
use crate::channels::test::TestChannels;
use crate::channels::test::TestChannelsError;
use crate::channels::test::TestChannelsScript;
use crate::channels::test::TestStreamID;
use crate::stream::PullStream;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::PushModeResult;
use crate::threads::dispatch::Dispatch;
use crate::threads::dispatch::DispatchEntryTypes;
use crate::threads::dispatch::DispatchInboundTypes;
use crate::threads::dispatch::DispatchTypes;
use crate::threads::dispatch::Dispatched;
use crate::threads::types::PollThreadTypes;

#[derive(Default)]
pub struct TestDispatchScriptEntry {
    pub msgs: Arc<Mutex<Vec<BasicAuthNed<NullCred, String>>>>,
    pub stream_script: Vec<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >
}

pub struct TestDispatch {
    script: Vec<TestDispatchScriptEntry>
}

#[derive(Clone, Debug)]
pub struct TestError {
    pub scope: ErrorScope
}

#[derive(Clone)]
pub struct TestChannelCore {
    script: Vec<Result<String, TestError>>
}

pub struct TestStream {
    pub sends: Arc<Mutex<Vec<String>>>,
    pub reports: Arc<Mutex<HashSet<TestStreamID>>>,
    refresh_script: Vec<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >
}

#[derive(Clone)]
pub struct TestPushModeScriptElem {
    pub sends: Option<(Option<Instant>, Vec<String>)>,
    pub retries:
        Option<Box<(Instant, Result<TestPushModeScriptElem, TestError>)>>,
    pub indefs: Option<Box<Result<TestPushModeScriptElem, TestError>>>,
    pub completes: Option<Box<Result<TestPushModeScriptElem, TestError>>>
}

pub struct TestPushMode {
    script: Vec<Result<TestPushModeScriptElem, TestError>>,
    retries: Vec<(Instant, Result<TestPushModeScriptElem, TestError>)>,
    indefs: Vec<Result<TestPushModeScriptElem, TestError>>,
    completes: Vec<Result<TestPushModeScriptElem, TestError>>
}

#[derive(Default)]
pub struct TestRecv {
    pub msgs: Arc<Mutex<Vec<BasicAuthNed<NullCred, String>>>>
}

pub struct ThreadTestTypes;

#[derive(Clone, Debug)]
pub struct TestRefreshRetry {
    pub when: Instant,
    pub result: Arc<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >
}

#[derive(Clone, Debug)]
pub struct TestCompletableError {
    pub scope: ErrorScope,
    pub result: Arc<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >
}

#[derive(Clone, Debug)]
pub enum TestRefreshError {
    Completable { result: TestCompletableError },
    Permanent { err: TestError }
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
    ) -> (
        Option<<TestRefreshError as RecoverableError>::Completable>,
        Option<<TestRefreshError as RecoverableError>::Permanent>
    ) {
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
        mut script: Vec<TestDispatchScriptEntry>
    ) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestDispatch { script: script })
    }
}

impl<Ctx> Dispatch<ThreadTestTypes, Ctx> for TestDispatch {
    type DispatchError = Infallible;
    type Msgs = ();
    type PushStream = TestStream;
    type Recv = TestRecv;

    fn dispatch(
        &mut self,
        ctx: &mut Ctx,
        _prin: &NullCred,
        shutdown: ShutdownFlag,
        _notify: Notify
    ) -> Result<
        Dispatched<ThreadTestTypes, Self::PushStream, Self::Msgs, Self::Recv>,
        Self::DispatchError
    > {
        let TestDispatchScriptEntry {
            msgs,
            stream_script
        } = self.script.pop().expect("Expected scripted action");
        let Ok(stream) = TestStream::create(stream_script, ctx);
        let recv = TestRecv { msgs: msgs };

        Ok(Dispatched::new(
            shutdown,
            stream,
            (),
            PassthruMsgAuthN::default(),
            recv
        ))
    }
}

impl<'a, Ctx> CreateWithParam<&'a mut Ctx> for TestStream {
    type Config = Vec<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >;
    type CreateError = Infallible;

    fn create(
        mut script: Self::Config,
        _ctx: &'a mut Ctx
    ) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestStream {
            sends: Arc::new(Mutex::new(Vec::new())),
            reports: Arc::new(Mutex::new(HashSet::new())),
            refresh_script: script
        })
    }
}

impl Create for TestChannelCore {
    type Config = Vec<Result<String, TestError>>;
    type CreateError = Infallible;

    #[inline]
    fn create(
        mut script: Vec<Result<String, TestError>>
    ) -> Result<Self, Self::CreateError> {
        script.reverse();

        Ok(TestChannelCore { script: script })
    }
}

impl<Ctx> StreamRefresh<Ctx> for TestStream {
    type RefreshError = TestRefreshError;
    type RefreshRetry = TestRefreshRetry;

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

impl PullStream<String> for TestChannelCore {
    type PullError = TestError;

    #[inline]
    fn pull(&mut self) -> Result<String, Self::PullError> {
        self.script.pop().expect("Expected scripted action")
    }
}

impl AuthNMsgRecv<NullCred, String, BasicAuthNed<NullCred, String>>
    for TestRecv
{
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
        let TestPushModeScriptElem {
            sends,
            retries,
            indefs,
            completes
        } = elem;

        if let Some(retries) = retries {
            self.retries.push(*retries)
        }

        if let Some(indefs) = indefs {
            self.indefs.push(*indefs)
        }

        if let Some(completes) = completes {
            self.completes.push(*completes)
        }

        sends.and_then(|(next, mut sends)| {
            stream.sends.lock().expect("lock failed").append(&mut sends);

            next
        })
    }
}

impl<Stream> StreamReporter<NullCred, TestStreamID, Stream> for TestStream {
    type ReportStreamError = Infallible;

    fn report_stream(
        &mut self,
        _party: &NullCred,
        id: TestStreamID,
        stream: Stream
    ) -> Result<Option<Stream>, Self::ReportStreamError> {
        if !self.reports.lock().expect("lock failed").insert(id) {
            Ok(Some(stream))
        } else {
            Ok(None)
        }
    }
}

impl<Ctx> PushMode<TestStream, (), Ctx> for TestPushMode {
    type RetryError = TestError;
    type RetryIndefError = TestError;
    type SendError = TestError;

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
        let mut errs = Vec::new();

        for (when, elem) in retries {
            if when <= now {
                match elem {
                    Ok(elem) => {
                        let next_outbound =
                            self.process_script_elem(stream, elem);

                        curr = next_retry(&curr, &next_outbound)
                    }
                    Err(err) => {
                        errs.push(err);
                    }
                }
            } else {
                self.retries.push((when, elem))
            }
        }

        if let Some(err) = errs.pop() {
            Err(err)
        } else {
            let next_retry = self.retries.iter().map(|(when, _)| *when).min();

            Ok(PushModeResult {
                next_outbound: curr,
                next_retry: next_retry,
                has_completes: !self.completes.is_empty()
            })
        }
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
        let mut errs = Vec::new();

        for elem in completes {
            match elem {
                Ok(elem) => {
                    let next_outbound = self.process_script_elem(stream, elem);

                    curr = next_retry(&curr, &next_outbound)
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }

        if let Some(err) = errs.pop() {
            Err(err)
        } else {
            let next_retry = self.retries.iter().map(|(when, _)| *when).min();

            Ok(PushModeResult {
                next_outbound: curr,
                next_retry: next_retry,
                has_completes: !self.completes.is_empty()
            })
        }
    }

    fn retry_indefs(
        &mut self,
        _ctx: &mut Ctx,
        _msgs: &mut (),
        stream: &mut TestStream
    ) -> Result<PushModeResult, Self::SendError> {
        let mut curr = None;
        let indefs: Vec<_> = self.indefs.drain(..).collect();
        let mut errs = Vec::new();

        for elem in indefs {
            match elem {
                Ok(elem) => {
                    let next_outbound = self.process_script_elem(stream, elem);

                    curr = next_retry(&curr, &next_outbound)
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }

        if let Some(err) = errs.pop() {
            Err(err)
        } else {
            let next_retry = self.retries.iter().map(|(when, _)| *when).min();

            Ok(PushModeResult {
                next_outbound: curr,
                next_retry: next_retry,
                has_completes: !self.completes.is_empty()
            })
        }
    }
}

impl ScopedError for TestError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
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
    Ctx: 'static + Send
{
    type Addr = TestEndpoint;
    type AuthNChan = TestChannel<TestChannelCore>;
    type AuthNMsg = BasicAuthNed<NullCred, String>;
    type Chan = TestChannel<TestChannelCore>;
    type ChanShutdownError = TestChannelsError;
    type ChanShutdownRetry = WithRetryWhen<TestChannel<TestChannelCore>>;
    type ChannelID = String;
    type ChannelParam = TestChannelParam;
    type Chans = TestChannels<TestChannelCore>;
    type ChansConfig = TestChannelsScript<TestChannelCore>;
    type ChansCreateError = Infallible;
    type InMsg = String;
    type Mode = TestPushMode;
    type ModeConfig = Vec<Result<TestPushModeScriptElem, TestError>>;
    type ModeCreateError = Infallible;
    type MsgAuth = PassthruMsgAuthN<String, NullCred>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = NullCred;
    type Msgs = ();
    type PullError = TestError;
    type Recv = TestRecv;
    type RecvError = Infallible;
    type RefreshCompletableError = TestCompletableError;
    type RefreshError = TestRefreshError;
    type RefreshPermanentError = TestError;
    type RefreshRetry = TestRefreshRetry;
    type SessionPrin = NullCred;
    type Stream = TestStream;
    type StreamConfig = Vec<
        Result<
            RetryResult<Option<Instant>, TestRefreshRetry>,
            TestRefreshError
        >
    >;
    type StreamCreateError = Infallible;
    type Wrapper = String;
}

impl DispatchInboundTypes for ThreadTestTypes {
    type AuthNMsg = BasicAuthNed<NullCred, String>;
    type InMsg = String;
    type MsgAuth = PassthruMsgAuthN<String, NullCred>;
    type MsgAuthError = Infallible;
    type MsgPrin = NullCred;
    type OutMsg = String;
    type SessionPrin = NullCred;
    type Wrapper = String;
}

impl<Ctx> DispatchEntryTypes<Ctx> for ThreadTestTypes
where
    Ctx: 'static + Send
{
    type Addr = TestEndpoint;
    type AuthNChan = TestChannel<TestChannelCore>;
    type Chan = TestChannel<TestChannelCore>;
    type ChanShutdownError = TestChannelsError;
    type ChanShutdownRetry = WithRetryWhen<TestChannel<TestChannelCore>>;
    type ChannelID = String;
    type ChannelParam = TestChannelParam;
    type Chans = TestChannels<TestChannelCore>;
    type ChansConfig = TestChannelsScript<TestChannelCore>;
    type ChansCreateError = Infallible;
    type Mode = TestPushMode;
    type ModeConfig = Vec<Result<TestPushModeScriptElem, TestError>>;
    type ModeCreateError = Infallible;
    type Msgs = ();
    type PullError = TestError;
    type Recv = TestRecv;
    type RecvError = Infallible;
    type RefreshCompletableError = TestCompletableError;
    type RefreshError = TestRefreshError;
    type RefreshPermanentError = TestError;
    type RefreshRetry = TestRefreshRetry;
    type ReportStreamError = Infallible;
    type Stream = TestStream;
}

impl<Ctx> DispatchTypes<Ctx> for ThreadTestTypes
where
    Ctx: 'static + Send
{
    type Disp = TestDispatch;
    type DispatchError = Infallible;
}
