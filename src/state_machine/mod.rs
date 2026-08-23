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

//! Raw protocol state machines.
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Read;
use std::io::Write;
use std::ptr::read;

use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;

/// Discriminator used to indicate end and non-end states.
///
/// This is used by [RawOnceMachineState::end].
pub enum OnceMachineAction<State, Value> {
    /// Indicates that this is not a stop state.
    Continue(State),
    /// Indicates that this is a stop state, yielding a `Value`.
    Stop(Value)
}

/// Base trait for machine states.
///
/// These, together with `Params` define the actions for a protocol
/// state machine.  There are two flavors of these,
/// [RawOnceMachineState], which represents "one-shot" protocols that
/// run to completion and yield a result, and [RawStreamMachineState],
/// which represents protocols that produce a stream of values.
pub trait RawMachineState: Sized {
    /// State machine fixed parameters.
    type Params;
    /// Result value(s).
    type Value;
    /// Error type.
    type Error: RecoverableError;

    /// Get the starting state.
    fn start(params: &Self::Params) -> Self;

    /// Convert an error produced while writing into an error state.
    fn error(
        params: &Self::Params,
        error: <Self::Error as RecoverableError>::Permanent
    ) -> Self;

    /// Reset after a completable error.
    #[inline]
    fn complete(
        self,
        _error: <Self::Error as RecoverableError>::Completable
    ) -> Self {
        self
    }

    /// Write the next protocol message to `stream`.
    ///
    /// It is acceptable to write nothing here, if the present
    /// protocol state does not send any message.
    fn write<W>(
        &mut self,
        params: &Self::Params,
        stream: &mut W
    ) -> Result<(), Self::Error>
    where
        W: Write;

    /// Select the next state, possibly reading from `stream`.
    ///
    /// It is acceptable to not read anything, if the present protocol
    /// state does not need to.
    fn read_select<R>(
        self,
        params: &Self::Params,
        stream: &mut R
    ) -> Result<Self, Self::Error>
    where
        R: Read;
}

pub trait RawOnceMachineState: RawMachineState {
    /// Indicate whether this is an ending state, and produce a result
    /// if it is.
    fn end(
        self,
        params: &Self::Params
    ) -> OnceMachineAction<
        Self,
        Result<Self::Value, <Self::Error as RecoverableError>::Permanent>
    >;
}

pub trait RawStreamMachineState: RawMachineState {
    fn value(self) -> Result<(Self, Option<Self::Value>), Self::Error>;
}

pub struct RawStateMachine<State: RawMachineState> {
    params: State::Params,
    state: State
}

pub enum RawStateMachineError<State>
where
    State: RawMachineState {
    Completable {
        err: <State::Error as RecoverableError>::Completable,
        params: State::Params,
        state: State
    },
    Permanent {
        err: <State::Error as RecoverableError>::Permanent
    }
}

impl<State> RawStateMachine<State>
where
    State: RawMachineState
{
    /// Create a new `RawStateMachine` from its parameters.
    #[inline]
    pub fn new(params: State::Params) -> Self {
        let state = State::start(&params);

        RawStateMachine {
            params: params,
            state: state
        }
    }

    #[inline]
    pub fn complete(
        err: <RawStateMachineError<State> as RecoverableError>::Completable
    ) -> Self {
        let (err, state, params) = err;
        let state = state.complete(err);

        RawStateMachine {
            params: params,
            state: state
        }
    }

    /// Step the state machine once with split streams.
    ///
    /// This will first write a protocol message to `send`, then
    /// read a protocol message from `recv` and select the next state.
    #[inline]
    pub fn step<S>(
        &mut self,
        stream: &mut S
    ) -> Result<(), <State::Error as RecoverableError>::Completable>
    where
        S: Read + Write {
        self.state
            .write(&self.params, stream)
            .and_then(|_| unsafe {
                let state = read(&self.state);

                state.read_select(&self.params, stream).map(|newstate| {
                    self.state = newstate;
                })
            })
            .or_else(|err| {
                let (completable, permanent) = err.split();

                if let Some(permanent) = permanent {
                    self.state = State::error(&self.params, permanent)
                }

                match completable {
                    Some(err) => Err(err),
                    None => Ok(())
                }
            })
    }

    /// Step the state machine once with split streams.
    ///
    /// This will first write a protocol message to `send`, then
    /// read a protocol message from `recv` and select the next state.
    #[inline]
    pub fn step_split<R, W>(
        &mut self,
        recv: &mut R,
        send: &mut W
    ) -> Result<(), <State::Error as RecoverableError>::Completable>
    where
        R: Read,
        W: Write {
        self.state
            .write(&self.params, send)
            .and_then(|_| unsafe {
                let state = read(&self.state);

                state.read_select(&self.params, recv).map(|newstate| {
                    self.state = newstate;
                })
            })
            .or_else(|err| {
                let (completable, permanent) = err.split();

                if let Some(permanent) = permanent {
                    self.state = State::error(&self.params, permanent)
                }

                match completable {
                    Some(err) => Err(err),
                    None => Ok(())
                }
            })
    }
}

impl<State> RawStateMachine<State>
where
    State: RawOnceMachineState
{
    /// Get the result if the machine is in an end state; otherwise,
    /// return back the original state.
    #[inline]
    pub fn end(
        mut self
    ) -> OnceMachineAction<
        Self,
        Result<State::Value, <State::Error as RecoverableError>::Permanent>
    > {
        self.state = match self.state.end(&self.params) {
            OnceMachineAction::Continue(state) => state,
            OnceMachineAction::Stop(out) => {
                return OnceMachineAction::Stop(out);
            }
        };

        OnceMachineAction::Continue(self)
    }

    /// Run the state machine to completion and return the result.
    pub fn run<S>(
        self,
        stream: &mut S
    ) -> Result<State::Value, RawStateMachineError<State>>
    where
        S: Read + Write {
        let RawStateMachine { mut state, params } = self;

        loop {
            state = match state.end(&params) {
                OnceMachineAction::Continue(state) => state,
                OnceMachineAction::Stop(out) => {
                    return out.map_err(|err| {
                        RawStateMachineError::Permanent { err: err }
                    });
                }
            };

            match state.write(&params, stream) {
                Ok(()) => unsafe {
                    match read(&state).read_select(&params, stream) {
                        Ok(newstate) => {
                            state = newstate;
                        }
                        Err(err) => {
                            let (completable, permanent) = err.split();

                            if let Some(permanent) = permanent {
                                state = State::error(&params, permanent)
                            }

                            if let Some(err) = completable {
                                return Err(
                                    RawStateMachineError::Completable {
                                        params: params,
                                        state: state,
                                        err: err
                                    }
                                );
                            }
                        }
                    }
                },
                Err(err) => {
                    let (completable, permanent) = err.split();

                    if let Some(permanent) = permanent {
                        state = State::error(&params, permanent)
                    }

                    if let Some(err) = completable {
                        return Err(RawStateMachineError::Completable {
                            params: params,
                            state: state,
                            err: err
                        });
                    }
                }
            }
        }
    }

    /// Run the state machine to completion with split streams and
    /// return the result.
    pub fn run_split<R, W>(
        self,
        recv: &mut R,
        send: &mut W
    ) -> Result<State::Value, RawStateMachineError<State>>
    where
        R: Read,
        W: Write {
        let RawStateMachine { mut state, params } = self;

        loop {
            state = match state.end(&params) {
                OnceMachineAction::Continue(state) => state,
                OnceMachineAction::Stop(out) => {
                    return out.map_err(|err| {
                        RawStateMachineError::Permanent { err: err }
                    });
                }
            };

            match state.write(&params, send) {
                Ok(()) => unsafe {
                    match read(&state).read_select(&params, recv) {
                        Ok(newstate) => {
                            state = newstate;
                        }
                        Err(err) => {
                            let (completable, permanent) = err.split();

                            if let Some(permanent) = permanent {
                                state = State::error(&params, permanent)
                            }

                            if let Some(err) = completable {
                                return Err(
                                    RawStateMachineError::Completable {
                                        params: params,
                                        state: state,
                                        err: err
                                    }
                                );
                            }
                        }
                    }
                },
                Err(err) => {
                    let (completable, permanent) = err.split();

                    if let Some(permanent) = permanent {
                        state = State::error(&params, permanent)
                    }

                    if let Some(err) = completable {
                        return Err(RawStateMachineError::Completable {
                            params: params,
                            state: state,
                            err: err
                        });
                    }
                }
            }
        }
    }
}

impl<State> RawStateMachine<State>
where
    State: RawOnceMachineState,
    State::Error: RecoverableError
{
}

impl<State> RecoverableError for RawStateMachineError<State>
where
    State: RawMachineState
{
    type Completable = (
        <State::Error as RecoverableError>::Completable,
        State,
        State::Params
    );
    type Permanent = <State::Error as RecoverableError>::Permanent;

    fn split(
        self
    ) -> (
        Option<(
            <State::Error as RecoverableError>::Completable,
            State,
            State::Params
        )>,
        Option<<State::Error as RecoverableError>::Permanent>
    ) {
        match self {
            RawStateMachineError::Completable { err, state, params } => {
                (Some((err, state, params)), None)
            }
            RawStateMachineError::Permanent { err } => (None, Some(err))
        }
    }
}

impl<State> ScopedError for RawStateMachineError<State>
where
    State: RawMachineState
{
    fn scope(&self) -> ErrorScope {
        match self {
            RawStateMachineError::Completable { .. } => ErrorScope::Retryable,
            RawStateMachineError::Permanent { err } => err.scope()
        }
    }
}

impl<State> Display for RawStateMachineError<State>
where
    State: RawMachineState
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            RawStateMachineError::Completable { .. } => {
                write!(f, "completable error")
            }
            RawStateMachineError::Permanent { err } => write!(f, "{}", err)
        }
    }
}

impl<State> Debug for RawStateMachineError<State>
where
    State: RawMachineState
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            RawStateMachineError::Completable { .. } => {
                write!(f, "Completable {{ .. }}")
            }
            RawStateMachineError::Permanent { err } => write!(f, "{:?}", err)
        }
    }
}

#[cfg(test)]
use std::io::Error;
#[cfg(test)]
use std::io::ErrorKind;

#[cfg(test)]
enum TestState {
    WriteZeroSplitOneTwo,
    WriteOneExpectThree,
    LoopTwoBreakThree,
    End,
    Error(TestError)
}

#[cfg(test)]
#[derive(Debug)]
enum TestError {
    BadInput,
    IOError(Error)
}

#[cfg(test)]
impl Display for TestError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            TestError::IOError(err) => write!(f, "{}", err),
            TestError::BadInput => write!(f, "bad input")
        }
    }
}

#[cfg(test)]
impl ScopedError for TestError {
    fn scope(&self) -> ErrorScope {
        match self {
            TestError::IOError(err) => err.scope(),
            TestError::BadInput => ErrorScope::Session
        }
    }
}

#[cfg(test)]
impl RecoverableError for TestError {
    type Completable = ();
    type Permanent = TestError;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            TestError::IOError(err) => {
                let (completable, permanent) = err.split();

                (completable, permanent.map(|err| TestError::IOError(err)))
            }
            TestError::BadInput => (None, Some(TestError::BadInput))
        }
    }
}

#[cfg(test)]
impl PartialEq for TestError {
    fn eq(
        &self,
        other: &TestError
    ) -> bool {
        match (self, other) {
            (TestError::IOError(a), TestError::IOError(b)) => {
                a.kind() == b.kind()
            }
            (TestError::BadInput, TestError::BadInput) => true,
            _ => false
        }
    }
}

#[cfg(test)]
impl RawMachineState for TestState {
    type Error = TestError;
    type Params = ();
    type Value = ();

    fn start(_params: &()) -> Self {
        TestState::WriteZeroSplitOneTwo
    }

    fn error(
        _params: &(),
        error: TestError
    ) -> Self {
        TestState::Error(error)
    }

    fn write<W>(
        &mut self,
        _params: &(),
        stream: &mut W
    ) -> Result<(), TestError>
    where
        W: Write {
        match self {
            TestState::WriteZeroSplitOneTwo => stream.write_all(&[0]),
            TestState::WriteOneExpectThree => stream.write_all(&[1]),
            _ => Ok(())
        }
        .map_err(|err| TestError::IOError(err))
    }

    fn read_select<R>(
        self,
        _params: &(),
        stream: &mut R
    ) -> Result<Self, TestError>
    where
        R: Read {
        match self {
            TestState::WriteZeroSplitOneTwo => {
                let mut buf = [0];

                stream
                    .read_exact(&mut buf[..])
                    .map_err(|err| TestError::IOError(err))?;

                match buf[0] {
                    1 => Ok(TestState::LoopTwoBreakThree),
                    2 => Ok(TestState::WriteOneExpectThree),
                    _ => Err(TestError::BadInput)
                }
            }
            TestState::WriteOneExpectThree => {
                let mut buf = [0];

                stream
                    .read_exact(&mut buf[..])
                    .map_err(|err| TestError::IOError(err))?;

                match buf[0] {
                    3 => Ok(TestState::End),
                    _ => Err(TestError::BadInput)
                }
            }
            TestState::LoopTwoBreakThree => {
                let mut buf = [0];

                stream
                    .read_exact(&mut buf[..])
                    .map_err(|err| TestError::IOError(err))?;

                match buf[0] {
                    2 => Ok(TestState::LoopTwoBreakThree),
                    3 => Ok(TestState::End),
                    _ => Err(TestError::BadInput)
                }
            }
            out => Ok(out)
        }
    }
}

#[cfg(test)]
impl RawOnceMachineState for TestState {
    fn end(
        self,
        _params: &()
    ) -> OnceMachineAction<Self, Result<(), TestError>> {
        match self {
            TestState::End => OnceMachineAction::Stop(Ok(())),
            TestState::Error(err) => OnceMachineAction::Stop(Err(err)),
            out => OnceMachineAction::Continue(out)
        }
    }
}

#[cfg(test)]
struct TestReader {
    script: Vec<Result<&'static [u8], Error>>
}

#[cfg(test)]
impl TestReader {
    fn new(mut script: Vec<Result<&'static [u8], Error>>) -> Self {
        script.reverse();

        TestReader { script: script }
    }
}

#[cfg(test)]
impl Read for TestReader {
    fn read(
        &mut self,
        buf: &mut [u8]
    ) -> Result<usize, Error> {
        match self.script.pop() {
            Some(op) => match op {
                Ok(src) => {
                    buf[0] = src[0];

                    Ok(1)
                }
                Err(err) => Err(err)
            },
            None => Err(Error::new(
                ErrorKind::UnexpectedEof,
                "read script exhausted"
            ))
        }
    }
}

#[cfg(test)]
struct TestWriter {
    log: Vec<Vec<u8>>
}

#[cfg(test)]
impl TestWriter {
    fn new() -> Self {
        TestWriter { log: Vec::new() }
    }

    fn take(self) -> Vec<Vec<u8>> {
        self.log
    }
}

#[cfg(test)]
impl Write for TestWriter {
    fn write(
        &mut self,
        buf: &[u8]
    ) -> Result<usize, Error> {
        let mut elem = Vec::with_capacity(buf.len());

        for byte in buf.iter() {
            elem.push(*byte)
        }

        self.log.push(elem);

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
fn test_error_writer_error() -> Error {
    Error::new(ErrorKind::Other, "test error")
}

#[cfg(test)]
struct TestErrorWriter;

#[cfg(test)]
impl Write for TestErrorWriter {
    fn write(
        &mut self,
        _buf: &[u8]
    ) -> Result<usize, Error> {
        Err(test_error_writer_error())
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
fn run_scripted_test(
    script: Vec<Result<&'static [u8], Error>>,
    expected_writes: &[Vec<u8>],
    expected_result: Result<(), TestError>
) {
    let mut writer = TestWriter::new();
    let mut reader = TestReader::new(script);
    let machine: RawStateMachine<TestState> = RawStateMachine::new(());
    let actual_result =
        machine
            .run_split(&mut reader, &mut writer)
            .map_err(|err| match err {
                RawStateMachineError::Permanent { err } => err,
                _ => panic!("unexpected completable error")
            });
    let actual_writes = writer.take();

    assert_eq!(actual_writes, expected_writes);
    assert_eq!(actual_result, expected_result);
}

#[test]
fn test_no_loop() {
    run_scripted_test(vec![Ok(&[2]), Ok(&[3])], &[vec![0], vec![1]], Ok(()))
}

#[test]
fn test_loop_none() {
    run_scripted_test(vec![Ok(&[1]), Ok(&[3])], &[vec![0]], Ok(()))
}

#[test]
fn test_loop_once() {
    run_scripted_test(vec![Ok(&[1]), Ok(&[2]), Ok(&[3])], &[vec![0]], Ok(()))
}

#[test]
fn test_loop_twice() {
    run_scripted_test(
        vec![Ok(&[1]), Ok(&[2]), Ok(&[2]), Ok(&[3])],
        &[vec![0]],
        Ok(())
    )
}

#[test]
fn test_no_loop_eof() {
    let err = TestError::IOError(Error::new(
        ErrorKind::UnexpectedEof,
        "read script exhausted"
    ));

    run_scripted_test(vec![Ok(&[2])], &[vec![0], vec![1]], Err(err));
}

#[test]
fn test_loop_none_eof() {
    let err = TestError::IOError(Error::new(
        ErrorKind::UnexpectedEof,
        "read script exhausted"
    ));

    run_scripted_test(vec![Ok(&[1])], &[vec![0]], Err(err));
}

#[test]
fn test_loop_once_eof() {
    let err = TestError::IOError(Error::new(
        ErrorKind::UnexpectedEof,
        "read script exhausted"
    ));

    run_scripted_test(vec![Ok(&[1]), Ok(&[2])], &[vec![0]], Err(err));
}

#[test]
fn test_loop_twice_eof() {
    let err = TestError::IOError(Error::new(
        ErrorKind::UnexpectedEof,
        "read script exhausted"
    ));

    run_scripted_test(vec![Ok(&[1]), Ok(&[2]), Ok(&[2])], &[vec![0]], Err(err));
}

#[test]
fn test_bad_input() {
    run_scripted_test(vec![Ok(&[4])], &[vec![0]], Err(TestError::BadInput));
}

#[test]
fn test_no_loop_bad_input() {
    run_scripted_test(
        vec![Ok(&[2]), Ok(&[4])],
        &[vec![0], vec![1]],
        Err(TestError::BadInput)
    );
}

#[test]
fn test_loop_none_bad_input() {
    run_scripted_test(
        vec![Ok(&[1]), Ok(&[4])],
        &[vec![0]],
        Err(TestError::BadInput)
    )
}

#[test]
fn test_loop_once_bad_input() {
    run_scripted_test(
        vec![Ok(&[1]), Ok(&[2]), Ok(&[4])],
        &[vec![0]],
        Err(TestError::BadInput)
    )
}

#[test]
fn test_loop_twice_bad_input() {
    run_scripted_test(
        vec![Ok(&[1]), Ok(&[2]), Ok(&[2]), Ok(&[4])],
        &[vec![0]],
        Err(TestError::BadInput)
    )
}

#[test]
fn test_bad_write() {
    let mut reader = TestReader::new(vec![Ok(&[2]), Ok(&[3])]);
    let machine: RawStateMachine<TestState> = RawStateMachine::new(());
    let actual_result = machine.run_split(&mut reader, &mut TestErrorWriter);

    assert!(matches!(
        actual_result,
        Err(RawStateMachineError::Permanent {
            err: TestError::IOError(_)
        })
    ));
}
