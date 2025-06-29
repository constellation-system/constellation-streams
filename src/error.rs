// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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

//! Error-management utilities.
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use constellation_common::error::CodecStreamError;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;

use crate::stream::CompoundBatchID;

pub trait ErrorReportInfo<Info> {
    fn report_info(&self) -> Option<Info>;
}

/// Common type of errors that can occur with compound batches.
///
/// This is a convenience type for any implementations that use
/// [CompoundBatches](crate::stream::CompoundBatches).
#[derive(Debug)]
pub enum CompoundBatchError<Idx, Success, Err> {
    /// An error occurred in the underlying batch function.
    Batch {
        /// An [ErrorSet] representing the errors that occurred in the
        /// batch function.
        errs: ErrorSet<Idx, Success, Err>
    },
    /// A bad batch ID was provided.
    BadID {
        /// The batch ID that was provided.
        id: CompoundBatchID
    }
}

/// A collection of both errors and successes that happened when
/// performing a batch operation.
///
/// This type represents a common idiom used in the implementation of
/// [PushStream](crate::stream::PushStream) combinators, particularly
/// those that involve multiple sub-streams.  It is often necessary to
/// collect results from an operation that contain a mix of successes
/// and errors, so that the call can be either retried or aborted.
/// This type implements this functionality.
#[derive(Debug)]
pub struct ErrorSet<Idx, Success, Err> {
    /// Successes that occurred for each index.
    successes: Vec<(Idx, Success)>,
    /// Errors that occurred for each index.
    errors: Vec<(Idx, Err)>
}

/// Wrapper around a [RecoverableError] that contains a set of parties.
///
/// This is useful for some implementations of
/// [PushStreamSharedSingle](crate::stream::PushStreamSharedSingle).
#[derive(Debug)]
pub struct PartiesBatchError<Parties, Err> {
    /// Set of parties.
    parties: Parties,
    /// The core error.
    err: Err
}

/// Wrapper for errors that need to indicate no selection was made.
#[derive(Debug)]
pub enum SelectionsError<Inner, Info> {
    /// Underlying error type.
    Inner {
        /// Underlying error.
        inner: Inner
    },
    /// Selections were not created for a stream.
    NoSelections {
        /// Information about selections.
        info: Info
    }
}

impl<Codec, IO, T> ErrorReportInfo<T> for CodecStreamError<Codec, IO>
where
    Codec: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let CodecStreamError::Codec { err } = self {
            err.report_info()
        } else {
            None
        }
    }
}

impl<Inner, Info> ScopedError for SelectionsError<Inner, Info>
where
    Inner: ScopedError
{
    fn scope(&self) -> ErrorScope {
        match self {
            SelectionsError::Inner { inner } => inner.scope(),
            SelectionsError::NoSelections { .. } => ErrorScope::Unrecoverable
        }
    }
}

impl<Inner, Info> RecoverableError for SelectionsError<Inner, Info>
where
    Inner: RecoverableError,
    Info: Debug
{
    type Completable = Inner::Completable;
    type Permanent = SelectionsError<Inner::Permanent, Info>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            SelectionsError::Inner { inner } => {
                let (completable, permanent) = inner.split();

                (
                    completable,
                    permanent.map(|err| SelectionsError::Inner { inner: err })
                )
            }
            SelectionsError::NoSelections { info } => {
                (None, Some(SelectionsError::NoSelections { info: info }))
            }
        }
    }
}

impl<Inner, Info, T> ErrorReportInfo<T> for SelectionsError<Inner, Info>
where
    Inner: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let SelectionsError::Inner { inner } = self {
            inner.report_info()
        } else {
            None
        }
    }
}

impl<T> ErrorReportInfo<T> for asn1rs::io::per::err::Error {
    #[inline]
    fn report_info(&self) -> Option<T> {
        None
    }
}

impl<T> ErrorReportInfo<T> for Infallible {
    #[inline]
    fn report_info(&self) -> Option<T> {
        None
    }
}

impl<Parties, Err> PartiesBatchError<Parties, Err> {
    #[inline]
    pub fn new(
        parties: Parties,
        err: Err
    ) -> Self {
        PartiesBatchError {
            parties: parties,
            err: err
        }
    }

    #[inline]
    pub fn take(self) -> (Parties, Err) {
        (self.parties, self.err)
    }

    #[inline]
    pub fn take_parties(self) -> Parties {
        self.parties
    }
}

impl<Parties, Err> RecoverableError for PartiesBatchError<Parties, Err>
where
    Err: RecoverableError
{
    type Completable = PartiesBatchError<Parties, Err::Completable>;
    type Permanent = Err::Permanent;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        let PartiesBatchError { parties, err } = self;
        let (completable, permanent) = err.split();

        (
            completable.map(|err| PartiesBatchError {
                parties: parties,
                err: err
            }),
            permanent
        )
    }
}

impl<Idx, Success, Err> ScopedError for CompoundBatchError<Idx, Success, Err>
where
    Err: ScopedError,
    Success: Clone,
    Idx: Clone + Display
{
    fn scope(&self) -> ErrorScope {
        match self {
            CompoundBatchError::Batch { errs } => errs.scope(),
            CompoundBatchError::BadID { .. } => ErrorScope::Batch
        }
    }
}

impl<Idx, Success, Err> RecoverableError
    for CompoundBatchError<Idx, Success, Err>
where
    Err: RecoverableError,
    Success: Clone + Debug,
    Idx: Clone + Debug + Display
{
    type Completable = ErrorSet<Idx, Success, Err::Completable>;
    type Permanent = CompoundBatchError<Idx, Success, Err::Permanent>;

    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            CompoundBatchError::Batch { errs } => {
                let (completable, permanent) = errs.split();

                (
                    completable,
                    permanent
                        .map(|errs| CompoundBatchError::Batch { errs: errs })
                )
            }
            CompoundBatchError::BadID { id } => {
                (None, Some(CompoundBatchError::BadID { id }))
            }
        }
    }
}

impl<Idx, Success, Err> ScopedError for ErrorSet<Idx, Success, Err>
where
    Err: ScopedError,
    Success: Clone,
    Idx: Clone + Display
{
    fn scope(&self) -> ErrorScope {
        let mut scope = ErrorScope::Retryable;

        for (_, err) in self.errors.iter() {
            scope = scope.max(err.scope())
        }

        scope
    }
}

impl<Idx, Success, Err> RecoverableError for ErrorSet<Idx, Success, Err>
where
    Err: RecoverableError,
    Success: Clone + Debug,
    Idx: Clone + Debug + Display
{
    type Completable = ErrorSet<Idx, Success, Err::Completable>;
    type Permanent = ErrorSet<Idx, Success, Err::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        let ErrorSet { successes, errors } = self;
        let len = errors.len();
        let mut permanents: Option<Vec<(Idx, Err::Permanent)>> = None;
        let mut completables: Option<Vec<(Idx, Err::Completable)>> = None;

        for (idx, err) in errors {
            let (completable, permanent) = err.split();

            if let Some(completable) = completable {
                match &mut completables {
                    Some(completables) => {
                        completables.push((idx.clone(), completable))
                    }
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push((idx.clone(), completable));

                        completables = Some(vec)
                    }
                }
            }

            if let Some(permanent) = permanent {
                match &mut permanents {
                    Some(permanents) => {
                        permanents.push((idx.clone(), permanent))
                    }
                    None => {
                        let mut vec = Vec::with_capacity(len);

                        vec.push((idx.clone(), permanent));

                        permanents = Some(vec)
                    }
                }
            }
        }

        let completable = completables.map(|completables| ErrorSet {
            successes: successes.clone(),
            errors: completables
        });
        let permanent = permanents.map(|permanents| ErrorSet {
            successes: successes.clone(),
            errors: permanents
        });

        (completable, permanent)
    }
}

impl<Idx, Success, Err> ErrorSet<Idx, Success, Err> {
    #[inline]
    pub fn create(
        successes: Vec<(Idx, Success)>,
        errors: Vec<(Idx, Err)>
    ) -> Self {
        ErrorSet {
            successes: successes,
            errors: errors
        }
    }

    #[inline]
    pub fn successes(&self) -> &[(Idx, Success)] {
        &self.successes
    }

    #[inline]
    pub fn errors(&self) -> &[(Idx, Err)] {
        &self.errors
    }

    #[inline]
    pub fn take(self) -> (Vec<(Idx, Success)>, Vec<(Idx, Err)>) {
        (self.successes, self.errors)
    }
}

impl<Inner, Info> Display for SelectionsError<Inner, Info>
where
    Inner: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            SelectionsError::Inner { inner } => inner.fmt(f),
            SelectionsError::NoSelections { .. } => {
                write!(f, "no selections for stream")
            }
        }
    }
}

impl<Idx, Success, Err> Display for ErrorSet<Idx, Success, Err>
where
    Idx: Display,
    Err: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        writeln!(f, "errors for parties:")?;

        for (idx, err) in &self.errors {
            writeln!(f, "{}: {}", idx, err)?;
        }

        Ok(())
    }
}

impl<Idx, Success, Err> Display for CompoundBatchError<Idx, Success, Err>
where
    Idx: Display,
    Err: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            CompoundBatchError::Batch { errs } => errs.fmt(f),
            CompoundBatchError::BadID { id } => {
                write!(f, "invalid batch ID ({})", id)
            }
        }
    }
}
