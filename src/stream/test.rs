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

use std::cell::RefCell;
use std::convert::Infallible;
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;

use crate::frags::OutboundFrags;
use crate::large_obj::LargeObjID;
use crate::large_obj::LargeObjMsg;
use crate::stream::LargeObjStream;
use crate::stream::LargeObjOfferStream;
use crate::stream::Parties;
use crate::stream::PullStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::threads::private::PrivateLargeObjPushModeTypes;
use crate::threads::shared::SharedLargeObjPushModeTypes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateBatchState<T> {
    Live {
        msgs: Vec<T>
    },
    Finished {
        msgs: Vec<T>
    },
    Canceled,
    StartError,
    Aborted
}

#[derive(Clone)]
pub struct TestPrivateStreamScript<In> {
    pub select: Vec<Result<RetryIndefResult<(), TestRetry>,
                           TestError<TestIndefAction<()>>>>,
    pub create_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub finish_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub cancel_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub abort_start_batch: Vec<RetryResult<(), TestAbortRetry>>,
    pub add: Vec<Result<RetryResult<(), TestRetry>,
                        TestError<TestAction<()>>>>,
    pub push_frags: Vec<Result<
        RetryIndefResult<(Option<Instant>, ()),
                         TestRetry,
                         Parties<()>>,
        TestError<TestIndefAction<Option<Instant>>>
    >>,
    pub push_offers: Vec<Result<
        RetryIndefResult<(Option<Instant>, ()),
                         TestRetry,
                         Parties<()>>,
        TestError<TestIndefAction<Option<Instant>>>
    >>,
    pub report_failure: Vec<Result<(), TestPermanentError>>,
    pub inbound: Vec<Result<In, TestPermanentError>>
}

#[derive(Clone)]
pub struct TestPrivateStream<In, Out, H>
where H: HashID {
    pub batches: Rc<RefCell<Vec<TestPrivateBatchState<Out>>>>,
    pub frags: Rc<RefCell<Vec<LargeObjID>>>,
    pub offers: Rc<RefCell<Vec<H>>>,
    pub failures: Rc<Vec<usize>>,
    pub batch_reports: Rc<RefCell<Vec<(usize, TestPermanentError)>>>,
    pub reports: Rc<RefCell<Vec<TestPermanentError>>>,
    script: Rc<RefCell<TestPrivateStreamScript<In>>>
}

#[derive(Copy, Clone, Default)]
pub struct TestLargeObjPushModeTypes<In, H>
where H: HashAlgo {
    msg: PhantomData<In>,
    hash: PhantomData<H>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestSharedBatchState<T> {
    Live {
        parties: Vec<usize>,
        msgs: Vec<T>
    },
    Finished {
        parties: Vec<usize>,
        msgs: Vec<T>
    },
    Canceled,
    StartError,
    Aborted
}

#[derive(Clone)]
pub struct TestSharedStreamScript<In> {
    pub select: Vec<Result<RetryIndefResult<Vec<usize>, TestRetry,
                                            Parties<Vec<usize>>>,
                           TestError<TestIndefPartiesAction>>>,
    pub create_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub finish_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub cancel_batch: Vec<Result<RetryResult<(), TestRetry>,
                                 TestError<TestAction<()>>>>,
    pub abort_start_batch: Vec<RetryResult<(), TestAbortRetry>>,
    pub add: Vec<Result<RetryResult<(), TestRetry>,
                        TestError<TestAction<()>>>>,
    pub push_frags: Vec<Result<
        RetryIndefResult<Option<Instant>,
                         TestRetry,
                         Parties<Vec<usize>>>,
        TestError<TestIndefAction<Option<Instant>>>
    >>,
    pub push_offers: Vec<Result<
        RetryIndefResult<Option<Instant>,
                         TestRetry,
                         Parties<Vec<usize>>>,
        TestError<TestIndefAction<Option<Instant>>>
    >>,
    pub report_failure: Vec<Result<(), TestPermanentError>>,
    pub inbound: Vec<Result<In, TestPermanentError>>
}

#[derive(Clone)]
pub struct TestSharedStream<In, Out, H>
where H: HashID {
    pub batches: Rc<RefCell<Vec<TestSharedBatchState<Out>>>>,
    pub frags: Rc<RefCell<Vec<LargeObjID>>>,
    pub offers: Rc<RefCell<Vec<H>>>,
    pub failures: Rc<Vec<usize>>,
    pub batch_reports: Rc<RefCell<Vec<(usize, TestPermanentError)>>>,
    pub reports: Rc<RefCell<Vec<TestPermanentError>>>,
    script: Rc<RefCell<TestSharedStreamScript<In>>>,
    parties: Vec<(usize, ())>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestRetry {
    pub when: Instant
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPartiesRetry {
    pub parties: Vec<usize>,
    pub when: Instant
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPermanentError {
    pub scope: ErrorScope
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPermanentBatchError {
    pub batch: usize,
    pub scope: ErrorScope
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestAbortRetry {
    pub when: Instant,
    pub batch: usize
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestCompletableError<Act> {
    pub scope: ErrorScope,
    pub action: Act
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestError<Act> {
    Permanent {
        err: TestPermanentError
    },
    Completable {
        err: TestCompletableError<Act>
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestBatchError<Act> {
    Permanent {
        err: TestPermanentBatchError
    },
    Completable {
        err: TestCompletableError<Act>
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestReportBatchError {
    batch: usize
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestStartBatchError<Select, Create, Selections> {
    Select {
        err: Select,
        selections: Selections
    },
    Create {
        err: Create,
        selections: Selections
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestStartBatchRetry<Select, Create, Selections> {
    Select {
        retry: Select,
        selections: Selections
    },
    Create {
        retry: Create,
        selections: Selections
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestPrivateInboundAction<In> {
    Success {
        msg: In
    },
    Error {
        err: Box<TestError<TestPrivateInboundAction<In>>>
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestAction<T> {
    Success {
        val: T
    },
    Retry {
        retry: TestRetry
    },
    Error {
        err: Box<TestError<TestAction<T>>>
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestIndefAction<T> {
    Success {
        val: T
    },
    Retry {
        retry: TestRetry
    },
    Indef,
    Error {
        err: Box<TestError<TestIndefAction<T>>>
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestIndefPartiesAction {
    Success {
        parties: Vec<usize>
    },
    Retry {
        retry: TestPartiesRetry
    },
    Indef {
        parties: Vec<usize>
    },
    Error {
        err: Box<TestError<TestIndefPartiesAction>>
    }
}

impl<Ctx, In, H> PrivateLargeObjPushModeTypes<Ctx>
    for TestLargeObjPushModeTypes<In, H>
where H: Clone + HashAlgo,
      H::HashID: Clone + Debug + Display + Eq + Hash,
      In: Clone {
    type Frags = OutboundFrags;
    type BatchID = usize;
    type HashID = H::HashID;
    type Hash = H;
    type StreamFlags = ();
    type StartBatchError = TestStartBatchError<
        TestError<TestIndefAction<()>>,
        TestBatchError<TestAction<()>>,
        ()
    >;
    type StartBatchErrorCompletable =
        TestStartBatchError<TestCompletableError<TestIndefAction<()>>,
                            TestCompletableError<TestAction<()>>,
                            ()>;
    type CancelBatchErrorCompletable = TestCompletableError<TestAction<()>>;
    type CancelBatchError = TestError<TestAction<()>>;
    type FinishBatchErrorCompletable = TestCompletableError<TestAction<()>>;
    type FinishBatchError = TestError<TestAction<()>>;
    type AddErrorCompletable = TestCompletableError<TestAction<()>>;
    type AddError = TestError<TestAction<()>>;
    type PushFragError = TestError<TestIndefAction<Option<Instant>>>;
    type PushFragErrorCompletable =
        TestCompletableError<TestIndefAction<Option<Instant>>>;
    type PushOfferError = TestError<TestIndefAction<Option<Instant>>>;
    type PushOfferErrorCompletable =
        TestCompletableError<TestIndefAction<Option<Instant>>>;
    type Stream = TestPrivateStream<In, LargeObjMsg<H::HashID>, H::HashID>;
}

impl<Ctx, In, H> SharedLargeObjPushModeTypes<Ctx>
    for TestLargeObjPushModeTypes<In, H>
where H: Clone + HashAlgo,
      H::HashID: Clone + Debug + Display + Eq + Hash,
      In: Clone {
    type PartyID = usize;
    type Parties = Vec<usize>;
    type IndefParties = Vec<usize>;
    type PartiesError = Infallible;
    type Frags = OutboundFrags;
    type BatchID = usize;
    type HashID = H::HashID;
    type Hash = H;
    type StreamFlags = bool;
    type StartBatchError = TestStartBatchError<
        TestError<TestIndefPartiesAction>,
        TestBatchError<TestAction<()>>,
        Vec<usize>
    >;
    type StartBatchErrorCompletable =
        TestStartBatchError<TestCompletableError<TestIndefPartiesAction>,
                            TestCompletableError<TestAction<()>>,
                            Vec<usize>>;
    type CancelBatchErrorCompletable = TestCompletableError<TestAction<()>>;
    type CancelBatchError = TestError<TestAction<()>>;
    type FinishBatchErrorCompletable = TestCompletableError<TestAction<()>>;
    type FinishBatchError = TestError<TestAction<()>>;
    type AddErrorCompletable = TestCompletableError<TestAction<()>>;
    type AddError = TestError<TestAction<()>>;
    type PushFragError = TestError<TestIndefAction<Option<Instant>>>;
    type PushFragErrorCompletable =
        TestCompletableError<TestIndefAction<Option<Instant>>>;
    type PushOfferError = TestError<TestIndefAction<Option<Instant>>>;
    type PushOfferErrorCompletable =
        TestCompletableError<TestIndefAction<Option<Instant>>>;
    type Stream = TestSharedStream<In, LargeObjMsg<H::HashID>, H::HashID>;
}

impl<In, Out, H> PushStreamReportBatchError<TestPermanentError, usize>
    for TestPrivateStream<In, Out, H>
where H: HashID {
    type ReportBatchError = TestReportBatchError;

    fn report_error_with_batch(
        &mut self,
        batch: &usize,
        error: &TestPermanentError
    ) -> Result<(), Self::ReportBatchError> {
       self.batch_reports
            .try_borrow_mut()
            .expect("try_borrow failed")
            .push((*batch, error.clone()));

        Ok(())
    }
}
impl<In, Out, H> PushStreamReportError<TestPermanentError>
    for TestPrivateStream<In, Out, H>
where H: HashID {
    type ReportError = Infallible;

    fn report_error(
        &mut self,
        error: &TestPermanentError
    ) -> Result<(), Self::ReportError> {
       self.reports
            .try_borrow_mut()
            .expect("try_borrow failed")
            .push(error.clone());

        Ok(())
    }
}

impl<In, Out, H> PushStreamReportError<TestPermanentBatchError>
    for TestPrivateStream<In, Out, H>
where H: HashID {
    type ReportError = TestReportBatchError;

    fn report_error(
        &mut self,
        error: &TestPermanentBatchError
    ) -> Result<(), Self::ReportError> {
        let batch = error.batch;
        let error = TestPermanentError {
            scope: error.scope.clone()
        };

        self.report_error_with_batch(&batch, &error)
    }
}

impl<In, Out, H> PushStreamReportError<
        TestStartBatchError<TestPermanentError, TestPermanentBatchError, ()>
    >
    for TestPrivateStream<In, Out, H>
where H: HashID {
    type ReportError = TestReportBatchError;

    fn report_error(
        &mut self,
        error: &TestStartBatchError<TestPermanentError,
                                    TestPermanentBatchError, ()>
    ) -> Result<(), Self::ReportError> {
        match error {
            TestStartBatchError::Select { err, .. } => {
                let Ok(res) = self.report_error(err);

                Ok(res)
            },
            TestStartBatchError::Create { err, .. } =>
                self.report_error(err),
        }
    }
}

impl<In, Out, H> TestPrivateStream<In, Out, H>
where H: HashID {
    #[inline]
    pub fn new(
        mut script: TestPrivateStreamScript<In>
    ) -> Self {
        script.select.reverse();
        script.create_batch.reverse();
        script.finish_batch.reverse();
        script.cancel_batch.reverse();
        script.abort_start_batch.reverse();
        script.add.reverse();
        script.push_frags.reverse();
        script.push_offers.reverse();
        script.report_failure.reverse();
        script.inbound.reverse();

        TestPrivateStream {
            failures: Rc::new(Vec::new()),
            batches: Rc::new(RefCell::new(Vec::new())),
            frags: Rc::new(RefCell::new(Vec::new())),
            offers: Rc::new(RefCell::new(Vec::new())),
            batch_reports: Rc::new(RefCell::new(Vec::new())),
            reports: Rc::new(RefCell::new(Vec::new())),
            script: Rc::new(RefCell::new(script))
        }
    }
}

impl<In, Out, H> TestSharedStream<In, Out, H>
where H: HashID {
    #[inline]
    pub fn new<I>(
        mut script: TestSharedStreamScript<In>,
        parties: I
    ) -> Self
    where I: Iterator<Item = usize> {
        let parties = parties.map(|party| (party, ())).collect();

        script.select.reverse();
        script.create_batch.reverse();
        script.finish_batch.reverse();
        script.cancel_batch.reverse();
        script.abort_start_batch.reverse();
        script.add.reverse();
        script.push_frags.reverse();
        script.push_offers.reverse();
        script.report_failure.reverse();
        script.inbound.reverse();

        TestSharedStream {
            failures: Rc::new(Vec::new()),
            batches: Rc::new(RefCell::new(Vec::new())),
            frags: Rc::new(RefCell::new(Vec::new())),
            offers: Rc::new(RefCell::new(Vec::new())),
            batch_reports: Rc::new(RefCell::new(Vec::new())),
            reports: Rc::new(RefCell::new(Vec::new())),
            script: Rc::new(RefCell::new(script)),
            parties: parties
        }
    }
}

impl RetryWhen for TestRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

impl RetryWhen for TestPartiesRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

impl RetryWhen for TestAbortRetry {
    #[inline]
    fn when(&self) -> Instant {
        self.when
    }
}

impl<Select, Create, Selections> RetryWhen
    for TestStartBatchRetry<Select, Create, Selections>
where Select: RetryWhen,
      Create: RetryWhen,
{
    #[inline]
    fn when(&self) -> Instant {
        match self {
            TestStartBatchRetry::Select { retry, .. } => retry.when(),
            TestStartBatchRetry::Create { retry, .. } => retry.when(),
        }
    }
}

impl<T> ScopedError for TestCompletableError<T> {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl ScopedError for TestPermanentError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl ScopedError for TestPermanentBatchError {
    #[inline]
    fn scope(&self) -> ErrorScope {
        self.scope
    }
}

impl<Select, Create, Selections> ScopedError
    for TestStartBatchError<Select, Create, Selections>
where Select: ScopedError,
      Create: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            TestStartBatchError::Select { err, .. } => err.scope(),
            TestStartBatchError::Create { err, .. } => err.scope(),
        }
    }
}

impl<T> RecoverableError for TestError<T> {
    type Completable = TestCompletableError<T>;
    type Permanent = TestPermanentError;

    #[inline]
    fn split(self) -> (Option<TestCompletableError<T>>,
                       Option<TestPermanentError>) {
        match self {
            TestError::Completable { err } => (Some(err), None),
            TestError::Permanent { err } => (None, Some(err))
        }
    }
}

impl<T> RecoverableError for TestBatchError<T> {
    type Completable = TestCompletableError<T>;
    type Permanent = TestPermanentBatchError;

    #[inline]
    fn split(self) -> (Option<TestCompletableError<T>>,
                       Option<TestPermanentBatchError>) {
        match self {
            TestBatchError::Completable { err } => (Some(err), None),
            TestBatchError::Permanent { err } => (None, Some(err))
        }
    }
}

impl<Select, Create, Selections> RecoverableError
    for TestStartBatchError<Select, Create, Selections>
where Select: RecoverableError,
      Create: RecoverableError,
      Selections: Clone
{
    type Completable = TestStartBatchError<Select::Completable,
                                           Create::Completable,
                                           Selections>;
    type Permanent = TestStartBatchError<Select::Permanent,
                                                Create::Permanent,
                                                ()>;

    #[inline]
    fn split(self) -> (Option<TestStartBatchError<Select::Completable,
                                                         Create::Completable,
                                                         Selections>>,
                       Option<TestStartBatchError<Select::Permanent,
                                                         Create::Permanent,
                                                         ()>>) {
        match self {
            TestStartBatchError::Select { err, selections } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| TestStartBatchError::Select {
                    selections: selections,
                    err: err
                }),
                 permanent.map(|err| TestStartBatchError::Select {
                    selections: (),
                     err: err
                 }))
            },
            TestStartBatchError::Create { err, selections } => {
                let (completable, permanent) = err.split();

                (completable.map(|err| TestStartBatchError::Create {
                    selections: selections,
                    err: err
                }),
                 permanent.map(|err| TestStartBatchError::Create {
                    selections: (),
                     err: err
                 }))
            }
        }
    }
}

impl<In, Out, H> PullStream<In> for TestPrivateStream<In, Out, H>
where H: HashID {
    type PullError = TestPermanentError;

    fn pull(&mut self) -> Result<In, Self::PullError> {
        self.script.try_borrow_mut().expect("try_borrow failed")
            .inbound
            .pop().expect("Expected scripted action")
    }
}

impl<In, Out, H> PullStream<In> for TestSharedStream<In, Out, H>
where H: HashID {
    type PullError = TestPermanentError;

    fn pull(&mut self) -> Result<In, Self::PullError> {
        self.script.try_borrow_mut().expect("try_borrow failed")
            .inbound
            .pop().expect("Expected scripted action")
    }
}

impl<Ctx, In, Out, H> PushStream<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type BatchID = usize;
    type CancelBatchError = TestError<TestAction<()>>;
    type CancelBatchRetry = TestRetry;
    type FinishBatchError = TestError<TestAction<()>>;
    type FinishBatchRetry = TestRetry;
    type StreamFlags = ();
    type ReportError = TestPermanentError;

    fn finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .finish_batch
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            let msgs = if let TestPrivateBatchState::Live { msgs } = &self
                .batches.try_borrow().expect("try_borrow failed")[*batch] {
                msgs.clone()
            } else {
                panic!("Expected live batch")
            };

            self.batches.try_borrow_mut()
                .expect("try_borrow failed")[*batch] =
                TestPrivateBatchState::Finished { msgs: msgs };
        }

        out
    }

    #[inline]
    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        self.finish_batch(ctx, flags, batch)
    }

    fn complete_finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        match err.action {
            TestAction::Success { .. } => {
                let msgs = if let TestPrivateBatchState::Live { msgs } = &self
                    .batches.try_borrow().expect("try_borrow failed")[*batch] {
                    msgs.clone()
                } else {
                    panic!("Expected live batch")
                };

                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] =
                    TestPrivateBatchState::Finished { msgs: msgs };

                Ok(RetryResult::Success(()))
            }
            TestAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestAction::Error { err } => Err(*err)
        }
    }

    fn cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .cancel_batch
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            self.batches.try_borrow_mut()
                .expect("try_borrow failed")[*batch] =
                TestPrivateBatchState::Canceled;
        }

        out
    }

    #[inline]
    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        self.cancel_batch(ctx, flags, batch)
    }

    fn complete_cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        match err.action {
            TestAction::Success { .. } => {
                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] =
                    TestPrivateBatchState::Canceled;

                Ok(RetryResult::Success(()))
            }
            TestAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestAction::Error { err } => Err(*err)
        }
    }

    fn cancel_batches(&mut self) {
        for i in 0..self.batches.try_borrow()
            .expect("try_borrow failed").len() {
            if let TestPrivateBatchState::Live { .. } =
                &self.batches.try_borrow().expect("try_borrow failed")[i] {
                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[i] =
                    TestPrivateBatchState::Canceled;
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .report_failure
            .pop().expect("Expected scripted action");

        if out.is_ok() {
            Rc::get_mut(&mut self.failures)
                .expect("get_mut failed")
                .push(*batch)
        }

        out
    }
}

impl<Ctx, In, Out, H> PushStream<Ctx> for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type BatchID = usize;
    type CancelBatchError = TestError<TestAction<()>>;
    type CancelBatchRetry = TestRetry;
    type FinishBatchError = TestError<TestAction<()>>;
    type FinishBatchRetry = TestRetry;
    type StreamFlags = bool;
    type ReportError = TestPermanentError;

    fn finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        if !*flags {
            let out = self.script.try_borrow_mut().expect("try_borrow failed")
                .finish_batch
                .pop().expect("Expected scripted action");

            if matches!(out, Ok(RetryResult::Success(_))) {
                let (parties, msgs) = if let TestSharedBatchState::Live {
                    parties, msgs
                } = &self.batches.try_borrow()
                    .expect("try_borrow failed")[*batch] {
                    (parties.clone(), msgs.clone())
                } else {
                    panic!("Expected live batch")
                };

                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] =
                    TestSharedBatchState::Finished {
                        parties: parties,
                        msgs: msgs
                    };

                *flags = true;
            }

            out
        } else {
            Ok(RetryResult::Success(()))
        }
    }

    #[inline]
    fn retry_finish_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        self.finish_batch(ctx, flags, batch)
    }

    fn complete_finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::FinishBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>,
                Self::FinishBatchError> {
        if !*flags {
            match err.action {
                TestAction::Success { .. } => {
                    let (parties, msgs) = if let TestSharedBatchState::Live {
                        parties, msgs
                    } = &self.batches.try_borrow_mut()
                        .expect("try_borrow failed")[*batch] {
                        (parties.clone(), msgs.clone())
                    } else {
                        panic!("Expected live batch")
                    };

                    self.batches.try_borrow_mut()
                        .expect("try_borrow failed")[*batch] =
                        TestSharedBatchState::Finished {
                            parties: parties,
                            msgs: msgs
                        };

                    *flags = true;

                    Ok(RetryResult::Success(()))
                }
                TestAction::Retry { retry } =>
                    Ok(RetryResult::Retry(retry)),
                TestAction::Error { err } => Err(*err)
            }
        } else {
            Ok(RetryResult::Success(()))
        }
    }

    fn cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        if !*flags {
            let out = self.script.try_borrow_mut().expect("try_borrow failed")
                .cancel_batch
                .pop().expect("Expected scripted action");

            if matches!(out, Ok(RetryResult::Success(_))) {
                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] =
                    TestSharedBatchState::Canceled;

                *flags = true;
            }

            out
        } else {
            Ok(RetryResult::Success(()))
        }
    }

    #[inline]
    fn retry_cancel_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        _retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        self.cancel_batch(ctx, flags, batch)
    }

    fn complete_cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        batch: &Self::BatchID,
        err: <Self::CancelBatchError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>,
                Self::CancelBatchError> {
        if !*flags {
            match err.action {
                TestAction::Success { .. } => {
                    self.batches.try_borrow_mut()
                        .expect("try_borrow failed")[*batch] =
                        TestSharedBatchState::Canceled;

                    *flags = true;

                    Ok(RetryResult::Success(()))
                }
                TestAction::Retry { retry } =>
                    Ok(RetryResult::Retry(retry)),
                TestAction::Error { err } => Err(*err)
            }
        } else {
            Ok(RetryResult::Success(()))
        }
    }

    fn cancel_batches(&mut self) {
        for i in 0..self.batches.try_borrow()
            .expect("try_borrow failed").len() {
            if let TestSharedBatchState::Live { .. } =
                &self.batches.try_borrow().expect("try_borrow failed")[i] {
                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[i] =
                    TestSharedBatchState::Canceled;
            }
        }
    }

    fn report_failure(
        &mut self,
        batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .report_failure
            .pop().expect("Expected scripted action");

        if out.is_ok() {
            Rc::get_mut(&mut self.failures)
                .expect("get_mut failed")
                .push(*batch)
        }

        out
    }
}

impl<Ctx, In, Out, H> PushStreamAdd<Out, Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type AddError = TestError<TestAction<()>>;
    type AddRetry = TestRetry;

    fn add(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .add
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryResult::Success(_))) {
            if let TestPrivateBatchState::Live { msgs } =
                &mut self.batches.try_borrow_mut()
                .expect("try_borrow failed")[*batch] {
                msgs.push(msg.clone())
            } else {
                panic!("Expected live batch")
            };
        }

        out
    }

    #[inline]
    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        _retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.add(ctx, flags, msg, batch)
    }

    fn complete_add(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        match err.action {
            TestAction::Success { .. } => {
                if let TestPrivateBatchState::Live { msgs } =
                    &mut self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] {
                        msgs.push(msg.clone())
                    } else {
                        panic!("Expected live batch")
                    };

                Ok(RetryResult::Success(()))
            }
            TestAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestAction::Error { err } => Err(*err)
        }
    }
}

impl<Ctx, In, Out, H> PushStreamAdd<Out, Ctx> for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type AddError = TestError<TestAction<()>>;
    type AddRetry = TestRetry;

    fn add(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        if !*flags {
            let out = self.script.try_borrow_mut().expect("try_borrow failed")
                .add
                .pop().expect("Expected scripted action");

            if matches!(out, Ok(RetryResult::Success(_))) {
                if let TestSharedBatchState::Live { msgs, .. } =
                    &mut self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[*batch] {
                    msgs.push(msg.clone())
                } else {
                    panic!("Expected live batch")
                };

                *flags = true;
            }

            out
        } else {
            Ok(RetryResult::Success(()))
        }
    }

    #[inline]
    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        _retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.add(ctx, flags, msg, batch)
    }

    fn complete_add(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Out,
        batch: &Self::BatchID,
        err: <Self::AddError as RecoverableError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        if !*flags {
            match err.action {
                TestAction::Success { .. } => {
                    if let TestSharedBatchState::Live { msgs, .. } =
                        &mut self.batches.try_borrow_mut()
                        .expect("try_borrow failed")[*batch] {
                            msgs.push(msg.clone())
                        } else {
                            panic!("Expected live batch")
                        };

                    *flags = true;

                    Ok(RetryResult::Success(()))
                }
                TestAction::Retry { retry } =>
                    Ok(RetryResult::Retry(retry)),
                TestAction::Error { err } => Err(*err)
            }
        } else {
            Ok(RetryResult::Success(()))
        }
    }
}

impl<Ctx, In, Out, H> PushStreamPrivate<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type SelectError = TestError<TestIndefAction<()>>;
    type SelectRetry = TestRetry;
    type CreateBatchError = TestError<TestAction<()>>;
    type CreateBatchRetry = TestRetry;
    type StartBatchError = TestStartBatchError<
        Self::SelectError,
        TestBatchError<TestAction<()>>,
        ()
    >;
    type StartBatchRetry = TestStartBatchRetry<
        Self::SelectRetry,
        Self::CreateBatchRetry,
        ()
    >;
    type AbortBatchRetry = TestAbortRetry;
    type Selections = ();
    type StartBatchStreamBatches = ();

    fn select(
        &mut self,
        _ctx: &mut Ctx,
        _selections: &mut Self::Selections
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .select
            .pop().expect("Expected scripted action");

        out
    }

    #[inline]
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        _retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        self.select(ctx, selections)
    }

    fn complete_select(
        &mut self,
        _ctx: &mut Ctx,
        _selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<(), Self::SelectRetry>, Self::SelectError> {
        match err.action {
            TestIndefAction::Success { .. } =>
                Ok(RetryIndefResult::Success(())),
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef => Ok(RetryIndefResult::Indef(())),
            TestIndefAction::Error { err } => Err(*err),
        }
    }

    fn create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        _selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.script.try_borrow_mut().expect("try_borrow failed")
            .create_batch
            .pop().expect("Expected scripted action")
            .map(|res| res.map(|_| {
                let mut batches = self.batches.try_borrow_mut()
                    .expect("try_borrow failed");
                let out = batches.len();

                batches.push(TestPrivateBatchState::Live {
                    msgs: Vec::new()
                });

                out
            }))
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        _retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.create_batch(ctx, batches, selections)
    }

    fn complete_create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        _selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        match err.action {
            TestAction::Success { .. } => {
                let mut batches = self.batches.try_borrow_mut()
                    .expect("try_borrow failed");
                let out = batches.len();

                batches.push(TestPrivateBatchState::Live {
                    msgs: Vec::new()
                });

                Ok(RetryResult::Success(out))
            }
            TestAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestAction::Error { err } => Err(*err)
        }
    }

    #[inline]
    fn start_batch(
        &mut self,
        ctx: &mut Ctx,
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        self.select(ctx, &mut ())
            .map_err(|err| TestStartBatchError::Select {
                selections: (),
                err: err
            })?
            .map_retry(|retry| TestStartBatchRetry::Select {
                selections: (),
                retry: retry
            })
            .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &())
                 .map(RetryIndefResult::from)
                 .map_err(|err| match err {
                     TestError::Permanent { err } => {
                         let mut batches = self.batches.try_borrow_mut()
                             .expect("try_borrow failed");
                         let batch = batches.len();

                         batches.push(TestPrivateBatchState::StartError);

                         TestStartBatchError::Create {
                             selections: (),
                             err: TestBatchError::Permanent {
                                 err: TestPermanentBatchError {
                                     batch: batch,
                                     scope: err.scope
                                 }
                             }
                         }
                     }
                     TestError::Completable { err } =>
                         TestStartBatchError::Create {
                             selections: (),
                             err: TestBatchError::Completable {
                                 err: err
                             }
                         }
                 })?
                 .map_retry(|retry| TestStartBatchRetry::Create {
                     selections: (),
                     retry: retry
                 })))
    }

    #[inline]
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match retry {
            TestStartBatchRetry::Select { retry, .. } => self
                .retry_select(ctx, &mut (), retry)
                .map_err(|err| TestStartBatchError::Select {
                    selections: (),
                    err: err
                })?
                .map_retry(|retry| TestStartBatchRetry::Select {
                     selections: (),
                    retry: retry
                })
                .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &())
                     .map(RetryIndefResult::from)
                     .map_err(|err| match err {
                         TestError::Permanent { err } => {
                             let mut batches = self.batches.try_borrow_mut()
                                 .expect("try_borrow failed");
                             let batch = batches.len();

                             batches.push(TestPrivateBatchState::StartError);

                             TestStartBatchError::Create {
                                 selections: (),
                                 err: TestBatchError::Permanent {
                                     err: TestPermanentBatchError {
                                         batch: batch,
                                         scope: err.scope
                                     }
                                 }
                             }
                         }
                         TestError::Completable { err } =>
                             TestStartBatchError::Create {
                                 selections: (),
                                 err: TestBatchError::Completable {
                                     err: err
                                 }
                             }
                     })?
                     .map_retry(|retry| TestStartBatchRetry::Create {
                         selections: (),
                         retry: retry
                     }))),
            TestStartBatchRetry::Create { retry, .. } => Ok(self
                .retry_create_batch(ctx, &mut (), &(), retry)
                .map(RetryIndefResult::from)
                .map_err(|err| match err {
                    TestError::Permanent { err } => {
                        let mut batches = self.batches.try_borrow_mut()
                            .expect("try_borrow failed");
                        let batch = batches.len();

                        batches.push(TestPrivateBatchState::StartError);

                        TestStartBatchError::Create {
                            selections: (),
                            err: TestBatchError::Permanent {
                                err: TestPermanentBatchError {
                                    batch: batch,
                                    scope: err.scope
                                }
                            }
                        }
                    }
                    TestError::Completable { err } =>
                        TestStartBatchError::Create {
                            selections: (),
                            err: TestBatchError::Completable {
                                err: err
                            }
                        }
                })?
                .map_retry(|retry| TestStartBatchRetry::Create {
                    selections: (),
                    retry: retry
                }))
        }
    }

    #[inline]
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        match err {
            TestStartBatchError::Select { err, .. } => self
                .complete_select(ctx, &mut (), err)
                .map_err(|err| TestStartBatchError::Select {
                    selections: (),
                    err: err
                })?
                .map_retry(|retry| TestStartBatchRetry::Select {
                    selections: (),
                    retry: retry
                })
                .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &())
                     .map(RetryIndefResult::from)
                     .map_err(|err| match err {
                         TestError::Permanent { err } => {
                             let mut batches = self.batches.try_borrow_mut()
                                 .expect("try_borrow failed");
                             let batch = batches.len();

                             batches.push(TestPrivateBatchState::StartError);

                             TestStartBatchError::Create {
                                 selections: (),
                                 err: TestBatchError::Permanent {
                                     err: TestPermanentBatchError {
                                         batch: batch,
                                         scope: err.scope
                                     }
                                 }
                             }
                         }
                         TestError::Completable { err } =>
                             TestStartBatchError::Create {
                                 selections: (),
                                 err: TestBatchError::Completable {
                                     err: err
                                 }
                             }
                     })?
                     .map_retry(|retry| TestStartBatchRetry::Create {
                         selections: (),
                         retry: retry
                     }))),
            TestStartBatchError::Create { err, .. } => Ok(self
                .complete_create_batch(ctx, &mut (), &(), err)
                .map(RetryIndefResult::from)
                .map_err(|err| match err {
                    TestError::Permanent { err } => {
                        let mut batches = self.batches.try_borrow_mut()
                            .expect("try_borrow failed");
                        let batch = batches.len();

                        batches.push(TestPrivateBatchState::StartError);

                        TestStartBatchError::Create {
                            selections: (),
                            err: TestBatchError::Permanent {
                                err: TestPermanentBatchError {
                                    batch: batch,
                                    scope: err.scope
                                }
                            }
                        }
                    }
                    TestError::Completable { err } =>
                        TestStartBatchError::Create {
                            selections: (),
                            err: TestBatchError::Completable {
                                err: err
                            }
                        }
                })?
                .map_retry(|retry| TestStartBatchRetry::Create {
                    selections: (),
                    retry: retry
                }))
        }
    }

    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        if let TestStartBatchError::Create { err, .. } = err {
            let out = self.script.try_borrow_mut().expect("try_borrow failed")
                .abort_start_batch
                .pop().expect("Expected scripted action")
                .map_retry(|res| TestAbortRetry {
                    when: res.when,
                    batch: err.batch
                });

            if out.is_success() {
                self.batches.try_borrow_mut()
                    .expect("try_borrow failed")[err.batch] =
                    TestPrivateBatchState::Aborted;
            }

            out
        } else {
            RetryResult::Success(())
        }
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let err = TestStartBatchError::Create {
            selections: (),
            err: TestPermanentBatchError {
                scope: ErrorScope::Retryable,
                batch: retry.batch
            }
        };

        self.abort_start_batch(ctx, flags, err)
    }
}

impl<In, Out, H> PushStreamPartyID for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PartyID = usize;
}

impl<In, Out, H> PushStreamParties for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PartiesIter = std::vec::IntoIter<(Self::PartyID, Self::PartyInfo)>;
    type PartyInfo = ();
    type PartiesError = Infallible;

    fn parties(&self) -> Result<Self::PartiesIter, Self::PartiesError> {
        Ok(self.parties.clone().into_iter())
    }
}

fn filter_select_error(
    filter: HashSet<usize>,
    err: TestError<TestIndefPartiesAction>
) -> TestError<TestIndefPartiesAction> {
    match err {
        TestError::Completable {
            err: TestCompletableError { action, scope }
        } => {
            let action = match action {
                TestIndefPartiesAction::Success { parties } => {
                    let parties = parties.into_iter()
                        .filter(|party| filter.contains(&party))
                        .collect();

                    TestIndefPartiesAction::Success { parties: parties }
                }
                TestIndefPartiesAction::Retry {
                    retry: TestPartiesRetry { parties, when }
                } => {
                    let parties = parties.into_iter()
                        .filter(|party| filter.contains(&party))
                        .collect();

                    TestIndefPartiesAction::Retry {
                        retry: TestPartiesRetry {
                            parties: parties,
                            when: when
                        }
                    }
                }
                TestIndefPartiesAction::Indef { parties } => {
                    let parties = parties.into_iter()
                        .filter(|party| filter.contains(&party))
                        .collect();

                    TestIndefPartiesAction::Indef { parties: parties }
                }
                TestIndefPartiesAction::Error { err } => {
                    let err = filter_select_error(filter, *err);

                    TestIndefPartiesAction::Error {
                        err: Box::new(err)
                    }
                }
            };

            TestError::Completable {
                err: TestCompletableError {
                    action: action,
                    scope: scope
                }
            }
        },
        err => err
    }
}

impl<Ctx, In, Out, H> PushStreamShared<Ctx> for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type SelectError = TestError<TestIndefPartiesAction>;
    type SelectRetry = TestPartiesRetry;
    type CreateBatchError = TestError<TestAction<()>>;
    type CreateBatchRetry = TestRetry;
    type StartBatchError = TestStartBatchError<
        Self::SelectError,
        TestBatchError<TestAction<()>>,
        Vec<usize>
    >;
    type StartBatchRetry = TestStartBatchRetry<
        Self::SelectRetry,
        Self::CreateBatchRetry,
        Vec<usize>
    >;
    type AbortBatchRetry = TestAbortRetry;
    type Selections = Vec<Self::PartyID>;
    type StartBatchStreamBatches = ();
    type BatchPartiesIter = std::vec::IntoIter<Self::PartyID>;
    type BatchPartiesError = Infallible;
    type IndefParties = Vec<Self::PartyID>;

    #[inline]
    fn batch_parties(
        &self,
        batch_id: &Self::BatchID
    ) -> Result<Self::BatchPartiesIter, Self::BatchPartiesError> {
        if let TestSharedBatchState::Live {
            parties, ..
        } = &self.batches.try_borrow().expect("try_borrow failed")[*batch_id] {
            Ok(parties.clone().into_iter())
        } else {
            panic!("batch is not live")
        }
    }

    fn select<'a, I>(
        &mut self,
        _ctx: &mut Ctx,
        selections: &mut Self::Selections,
        parties: I
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError>
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .select
            .pop().expect("Expected scripted action");

        match out {
            Ok(RetryIndefResult::Success(filter)) => {
                let filter: HashSet<Self::PartyID> = filter
                    .into_iter().collect();
                let parties: Vec<Self::PartyID> = parties
                    .filter(|party| filter.contains(party))
                    .cloned().collect();

                for party in parties.iter() {
                    selections.push(party.clone())
                }

                Ok(RetryIndefResult::Success(parties))
            }
            Ok(RetryIndefResult::Retry(retry)) => {
                let parties = parties.cloned().collect();
                let retry = TestPartiesRetry {
                    parties: parties,
                    when: retry.when
                };

                Ok(RetryIndefResult::Retry(retry))
            },
            Ok(RetryIndefResult::Indef(indef)) => match indef {
                Parties::Some(filter) => {
                    let filter: HashSet<Self::PartyID> = filter
                        .into_iter().collect();
                    let parties = parties
                        .filter(|party| filter.contains(party))
                        .cloned().collect();

                    Ok(RetryIndefResult::Indef(Parties::Some(parties)))
                }
                Parties::All => Ok(RetryIndefResult::Indef(Parties::All))
            }
            Err(err) => {
                let filter = parties.cloned().collect();
                let err = filter_select_error(filter, err);

                Err(err)
            }
        }
    }

    #[inline]
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        retry: Self::SelectRetry
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError> {
        self.select(ctx, selections, retry.parties.iter())
    }

    fn complete_select(
        &mut self,
        _ctx: &mut Ctx,
        selections: &mut Self::Selections,
        err: <Self::SelectError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Vec<Self::PartyID>,
                                 Self::SelectRetry,
                                 Parties<Self::IndefParties>>,
                Self::SelectError> {
        match err.action {
            TestIndefPartiesAction::Success { parties } => {
                *selections = parties.clone();

                Ok(RetryIndefResult::Success(parties))
            },
            TestIndefPartiesAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefPartiesAction::Indef { parties } =>
                Ok(RetryIndefResult::Indef(Parties::Some(parties))),
            TestIndefPartiesAction::Error { err } => Err(*err),
        }
    }

    fn create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.script.try_borrow_mut().expect("try_borrow failed")
            .create_batch
            .pop().expect("Expected scripted action")
            .map(|res| res.map(|_| {
                let mut batches = self.batches.try_borrow_mut()
                    .expect("try_borrow failed");
                let out = batches.len();

                batches.push(TestSharedBatchState::Live {
                    parties: selections.clone(),
                    msgs: Vec::new()
                });

                out
            }))
    }

    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        _retry: Self::CreateBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        self.create_batch(ctx, batches, selections)
    }

    fn complete_create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        err: <Self::CreateBatchError as RecoverableError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        match err.action {
            TestAction::Success { .. } => {
                let mut batches = self.batches.try_borrow_mut()
                    .expect("try_borrow failed");
                let out = batches.len();

                batches.push(TestSharedBatchState::Live {
                    parties: selections.clone(),
                    msgs: Vec::new()
                });

                Ok(RetryResult::Success(out))
            }
            TestAction::Retry { retry } => Ok(RetryResult::Retry(retry)),
            TestAction::Error { err } => Err(*err)
        }
    }

    #[inline]
    fn start_batch<'a, I>(
        &mut self,
        ctx: &mut Ctx,
        parties: I
    ) -> Result<
        RetryIndefResult<Self::BatchID,
                         Self::StartBatchRetry,
                         Parties<Self::IndefParties>>,
        Self::StartBatchError
    >
    where
        I: Iterator<Item = &'a Self::PartyID>,
        Self::PartyID: 'a {
        let mut selections = Vec::new();

        self.select(ctx, &mut selections, parties)
            .map_err(|err| TestStartBatchError::Select {
                selections: selections.clone(),
                err: err
            })?
            .map_retry(|retry| TestStartBatchRetry::Select {
                selections: selections.clone(),
                retry: retry
            })
            .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &selections)
                 .map(RetryIndefResult::from)
                 .map_err(|err| match err {
                     TestError::Permanent { err } => {
                         let mut batches = self.batches.try_borrow_mut()
                             .expect("try_borrow failed");
                         let batch = batches.len();

                         batches.push(TestSharedBatchState::StartError);

                         TestStartBatchError::Create {
                             selections: selections.clone(),
                             err: TestBatchError::Permanent {
                                 err: TestPermanentBatchError {
                                     batch: batch,
                                     scope: err.scope
                                 }
                             }
                         }
                     }
                     TestError::Completable { err } =>
                         TestStartBatchError::Create {
                             selections: selections.clone(),
                             err: TestBatchError::Completable {
                                 err: err
                             }
                         }
                 })?
                 .map_retry(|retry| TestStartBatchRetry::Create {
                     selections: selections.clone(),
                     retry: retry
                 })))
    }

    #[inline]
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        retry: Self::StartBatchRetry
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::StartBatchRetry,
                                 Parties<Self::IndefParties>>,
                Self::StartBatchError> {
        match retry {
            TestStartBatchRetry::Select { retry, mut selections } => self
                .retry_select(ctx, &mut selections, retry)
                .map_err(|err| TestStartBatchError::Select {
                    selections: selections.clone(),
                    err: err
                })?
                .map_retry(|retry| TestStartBatchRetry::Select {
                    selections: selections.clone(),
                    retry: retry
                })
                .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &selections)
                     .map(RetryIndefResult::from)
                     .map_err(|err| match err {
                         TestError::Permanent { err } => {
                             let mut batches = self.batches.try_borrow_mut()
                                 .expect("try_borrow failed");
                             let batch = batches.len();

                             batches.push(TestSharedBatchState::StartError);

                             TestStartBatchError::Create {
                                 selections: selections.clone(),
                                 err: TestBatchError::Permanent {
                                     err: TestPermanentBatchError {
                                         batch: batch,
                                         scope: err.scope
                                     }
                                 }
                             }
                         }
                         TestError::Completable { err } =>
                             TestStartBatchError::Create {
                                 selections: selections.clone(),
                                 err: TestBatchError::Completable {
                                     err: err
                                 }
                             }
                     })?
                     .map_retry(|retry| TestStartBatchRetry::Create {
                         selections: selections.clone(),
                         retry: retry
                     }))),
            TestStartBatchRetry::Create { retry, selections } => Ok(self
                .retry_create_batch(ctx, &mut (), &selections, retry)
                .map(RetryIndefResult::from)
                .map_err(|err| match err {
                    TestError::Permanent { err } => {
                        let mut batches = self.batches.try_borrow_mut()
                            .expect("try_borrow failed");
                        let batch = batches.len();

                        batches.push(TestSharedBatchState::StartError);

                        TestStartBatchError::Create {
                            selections: selections.clone(),
                            err: TestBatchError::Permanent {
                                err: TestPermanentBatchError {
                                    batch: batch,
                                    scope: err.scope
                                }
                            }
                        }
                    }
                    TestError::Completable { err } =>
                        TestStartBatchError::Create {
                            selections: selections.clone(),
                            err: TestBatchError::Completable {
                                err: err
                            }
                        }
                })?
                .map_retry(|retry| TestStartBatchRetry::Create {
                    selections: selections.clone(),
                    retry: retry
                }))
        }
    }

    #[inline]
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        err: <Self::StartBatchError as RecoverableError>::Completable
    ) -> Result<RetryIndefResult<Self::BatchID,
                                 Self::StartBatchRetry,
                                 Parties<Self::IndefParties>>,
                Self::StartBatchError> {
        match err {
            TestStartBatchError::Select { err, mut selections } => self
                .complete_select(ctx, &mut selections, err)
                .map_err(|err| TestStartBatchError::Select {
                    selections: selections.clone(),
                    err: err
                })?
                .map_retry(|retry| TestStartBatchRetry::Select {
                    selections: selections.clone(),
                    retry: retry
                })
                .flat_map_ok(|_| Ok(self.create_batch(ctx, &mut (), &selections)
                     .map(RetryIndefResult::from)
                     .map_err(|err| match err {
                         TestError::Permanent { err } => {
                             let mut batches = self.batches.try_borrow_mut()
                                 .expect("try_borrow failed");
                             let batch = batches.len();

                             batches.push(TestSharedBatchState::StartError);

                             TestStartBatchError::Create {
                                 selections: selections.clone(),
                                 err: TestBatchError::Permanent {
                                     err: TestPermanentBatchError {
                                         batch: batch,
                                         scope: err.scope
                                     }
                                 }
                             }
                         }
                         TestError::Completable { err } =>
                             TestStartBatchError::Create {
                                 selections: selections.clone(),
                                 err: TestBatchError::Completable {
                                     err: err
                                 }
                             }
                     })?
                     .map_retry(|retry| TestStartBatchRetry::Create {
                         selections: selections.clone(),
                         retry: retry
                     }))),
            TestStartBatchError::Create { err, selections } => Ok(self
                .complete_create_batch(ctx, &mut (), &selections, err)
                .map(RetryIndefResult::from)
                .map_err(|err| match err {
                    TestError::Permanent { err } => {
                        let mut batches = self.batches.try_borrow_mut()
                            .expect("try_borrow failed");
                        let batch = batches.len();

                        batches.push(TestSharedBatchState::StartError);

                        TestStartBatchError::Create {
                            selections: selections.clone(),
                            err: TestBatchError::Permanent {
                                err: TestPermanentBatchError {
                                    batch: batch,
                                    scope: err.scope
                                }
                            }
                        }
                    }
                    TestError::Completable { err } =>
                        TestStartBatchError::Create {
                            selections: selections.clone(),
                            err: TestBatchError::Completable {
                                err: err
                            }
                        }
                })?
                .map_retry(|retry| TestStartBatchRetry::Create {
                    selections: selections.clone(),
                    retry: retry
                }))
        }
    }

    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        err: <Self::StartBatchError as RecoverableError>::Permanent
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        if !*flags {
            if let TestStartBatchError::Create { err, .. } = err {
                let out = self.script.try_borrow_mut()
                    .expect("try_borrow failed")
                    .abort_start_batch
                    .pop().expect("Expected scripted action")
                    .map_retry(|res| TestAbortRetry {
                        when: res.when,
                        batch: err.batch
                    });

                if out.is_success() {
                    self.batches.try_borrow_mut()
                        .expect("try_borrow failed")[err.batch] =
                        TestSharedBatchState::Aborted;

                    *flags = true;
                }

                out
            } else {
                RetryResult::Success(())
            }
        } else {
            RetryResult::Success(())
        }
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        let err = TestStartBatchError::Create {
            selections: (),
            err: TestPermanentBatchError {
                scope: ErrorScope::Retryable,
                batch: retry.batch
            }
        };

        self.abort_start_batch(ctx, flags, err)
    }
}

impl<Ctx, In, Out, H> LargeObjStream<Ctx> for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushFragError = TestError<TestIndefAction<Option<Instant>>>;
    type PushFragRetry = TestRetry;
    type Frags = OutboundFrags;
    type Parties = ();

    fn push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .push_frags
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            self.frags.try_borrow_mut().expect("try_borrow failed").push(id);
        }

        out
    }

    #[inline]
    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.push_frags(ctx, id, frags)
    }

    fn complete_push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                self.frags
                    .try_borrow_mut()
                    .expect("try_borrow failed")
                    .push(id);

                Ok(RetryIndefResult::Success((val, ())))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

impl<Ctx, In, Out, H> LargeObjStream<Ctx> for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushFragError = TestError<TestIndefAction<Option<Instant>>>;
    type PushFragRetry = TestRetry;
    type Frags = OutboundFrags;
    type Parties = Vec<usize>;

    fn push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .push_frags
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            self.frags.try_borrow_mut().expect("try_borrow failed").push(id);
        }

        out.map(|res| res.map(|out| {
            let parties = self.parties.iter().cloned()
                .map(|(party, ())| party).collect();

            (out, parties)
        }))
    }

    #[inline]
    fn retry_push_frags(
        &mut self,
        ctx: &mut Ctx,
        id: LargeObjID,
        frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        self.push_frags(ctx, id, frags)
    }

    fn complete_push_frags(
        &mut self,
        _ctx: &mut Ctx,
        id: LargeObjID,
        _frags: &mut Self::Frags,
        err: <Self::PushFragError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushFragRetry,
                         Parties<Self::Parties>>,
        Self::PushFragError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                self.frags
                    .try_borrow_mut()
                    .expect("try_borrow failed")
                    .push(id);

                let parties = self.parties.iter().cloned()
                    .map(|(party, ())| party).collect();

                Ok(RetryIndefResult::Success((val, parties)))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

impl<In, Out, H> PushStreamReportBatchError<TestPermanentError, usize>
    for TestSharedStream<In, Out, H>
where H: HashID {
    type ReportBatchError = TestReportBatchError;

    fn report_error_with_batch(
       &mut self,
        batch: &usize,
        error: &TestPermanentError
    ) -> Result<(), Self::ReportBatchError> {
       self.batch_reports
            .try_borrow_mut()
            .expect("try_borrow failed")
            .push((*batch, error.clone()));

       Ok(())
    }
}

impl<In, Out, H> PushStreamReportError<TestPermanentError>
   for TestSharedStream<In, Out, H>
where H: HashID {
    type ReportError = Infallible;

   fn report_error(
        &mut self,
        error: &TestPermanentError
    ) -> Result<(), Self::ReportError> {
       self.reports
            .try_borrow_mut()
            .expect("try_borrow failed")
            .push(error.clone());

        Ok(())
    }
}

impl<In, Out, H> PushStreamReportError<TestPermanentBatchError>
    for TestSharedStream<In, Out, H>
where H: HashID {
    type ReportError = TestReportBatchError;

    fn report_error(
       &mut self,
        error: &TestPermanentBatchError
    ) -> Result<(), Self::ReportError> {
       let batch = error.batch;
        let error = TestPermanentError {
            scope: error.scope.clone()
        };

        self.report_error_with_batch(&batch, &error)
    }
}

impl<In, Out, H> PushStreamReportError<
        TestStartBatchError<TestPermanentError, TestPermanentBatchError, ()>
   >
    for TestSharedStream<In, Out, H>
where H: HashID {
    type ReportError = TestReportBatchError;
    fn report_error(
        &mut self,
        error: &TestStartBatchError<TestPermanentError,
                                   TestPermanentBatchError, ()>
    ) -> Result<(), Self::ReportError> {
        match error {
            TestStartBatchError::Select { err, .. } => {
               let Ok(res) = self.report_error(err);

                Ok(res)
            },
            TestStartBatchError::Create { err, .. } =>
                self.report_error(err),
        }
    }
}


impl<Ctx, In, Out, H> LargeObjOfferStream<H, Ctx>
    for TestPrivateStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushOfferError = TestError<TestIndefAction<Option<Instant>>>;
    type PushOfferRetry = TestRetry;

    fn push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .push_offers
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            self.offers
                .try_borrow_mut()
                .expect("try_borrow failed")
                .push(hash)
        }

        out
    }

    #[inline]
    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        _retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.push_offer(ctx, hash, frags)
    }

    fn complete_push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                self.offers
                    .try_borrow_mut()
                    .expect("try_borrow failed")
                    .push(hash);

                Ok(RetryIndefResult::Success((val, ())))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

impl<Ctx, In, Out, H> LargeObjOfferStream<H, Ctx>
    for TestSharedStream<In, Out, H>
where Out: Clone,
      H: HashID {
    type PushOfferError = TestError<TestIndefAction<Option<Instant>>>;
    type PushOfferRetry = TestRetry;

    fn push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        let out = self.script.try_borrow_mut().expect("try_borrow failed")
            .push_offers
            .pop().expect("Expected scripted action");

        if matches!(out, Ok(RetryIndefResult::Success(_))) {
            self.offers
                .try_borrow_mut()
                .expect("try_borrow failed")
                .push(hash)
        }

        out.map(|res| res.map(|out| {
            let parties = self.parties.iter().cloned()
                .map(|(party, ())| party).collect();

            (out, parties)
        }))
    }

    #[inline]
    fn retry_push_offer(
        &mut self,
        ctx: &mut Ctx,
        hash: H,
        frags: &mut Self::Frags,
        _retry: Self::PushOfferRetry
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        self.push_offer(ctx, hash, frags)
    }

    fn complete_push_offer(
        &mut self,
        _ctx: &mut Ctx,
        hash: H,
        _frags: &mut Self::Frags,
        err: <Self::PushOfferError as RecoverableError>::Completable
    ) -> Result<
        RetryIndefResult<(Option<Instant>, Self::Parties),
                         Self::PushOfferRetry,
                         Parties<Self::Parties>>,
        Self::PushOfferError
    > {
        match err.action {
            TestIndefAction::Success { val } => {
                self.offers
                    .try_borrow_mut()
                    .expect("try_borrow failed")
                    .push(hash);

                let parties = self.parties.iter().cloned()
                    .map(|(party, ())| party).collect();

                Ok(RetryIndefResult::Success((val, parties)))
            }
            TestIndefAction::Retry { retry } =>
                Ok(RetryIndefResult::Retry(retry)),
            TestIndefAction::Indef =>
                Ok(RetryIndefResult::Indef(Parties::All)),
            TestIndefAction::Error { err } => Err(*err),
        }
    }
}

impl Display for TestPermanentError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test error")
    }
}

impl Display for TestPermanentBatchError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "test error")
    }
}

impl<Select, Create, Selections> Display
    for TestStartBatchError<Select, Create, Selections>
where Select: Display,
      Create: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            TestStartBatchError::Select { err, .. } => err.fmt(f),
            TestStartBatchError::Create { err, .. } => err.fmt(f),
        }
    }
}

impl Display for TestReportBatchError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "bad batch ID {}", self.batch)
    }
}
