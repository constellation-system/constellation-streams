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

//! Error-management utilities.
//!
//! This module contains several common error types used throughout
//! this package.
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use constellation_common::error::CodecStreamError;
use constellation_common::error::ErrorScope;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::util::LazyInitVec;

use crate::stream::CompoundBatchID;

// XXX Possibly rename this and move it into common.

/// Trait for errors that may contain specific information.
pub trait ErrorReportInfo<Info> {
    /// Extract the information from the error, if it exists.
    fn report_info(&self) -> Option<Info>;
}

/// Common type of errors that can occur with compound batches.
///
/// This is a convenience type for any implementations that use
/// [`CompoundBatches`](crate::stream::CompoundBatches).
///
/// # Type Parameters
///
/// - `Idx`: Type of indexes.
///
/// - `Success`: Type of information recorded for successes.
///
/// - `Err`: Type of information recorded for errors.
#[derive(Debug)]
pub enum CompoundBatchError<Idx, Success, Err> {
    /// An error occurred in the underlying batch function.
    Batch {
        /// An [`ErrorSet`] representing the errors that occurred in the
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
/// [`PushStream`](crate::stream::PushStream) combinators, particularly
/// those that involve multiple sub-streams.  It is often necessary to
/// collect results from an operation that contain a mix of successes
/// and errors, so that the call can be either retried or aborted.
/// This type implements this functionality.
///
/// # Type Parameters
///
/// - `Idx`: Type of indexes.
///
/// - `Success`: Type of information recorded for successes.
///
/// - `Err`: Type of information recorded for errors.
#[derive(Debug)]
pub struct ErrorSet<Idx, Success, Err> {
    /// Successes that occurred for each index.
    successes: Vec<(Idx, Success)>,
    /// Errors that occurred for each index.
    errors: Vec<(Idx, Err)>
}

/// Wrapper around a [`RecoverableError`] that contains a set of parties.
///
/// This is useful for some implementations of
/// [`PushStreamSharedSingle`](crate::stream::PushStreamSharedSingle).
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
    /// Create a `PartiesBatchError` from its components.
    ///
    /// # Parameters
    ///
    /// - `parties`: The parties.
    ///
    /// - `error`: The error.
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

    /// Decompose this `PartiesBatchError` into its components.
    ///
    /// # Return Value
    ///
    /// Returns a tuple consisting of the following:
    ///
    /// 1. The parties.
    ///
    /// 1. The error.
    #[inline]
    pub fn take(self) -> (Parties, Err) {
        (self.parties, self.err)
    }

    // XXX see if this is really necessary.

    /// Decompose this `PartiesBatchError` into just the parties.
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
        let mut permanents = LazyInitVec::new(len);
        let mut completables = LazyInitVec::new(len);

        for (idx, err) in errors {
            let (completable, permanent) = err.split();

            if let Some(completable) = completable {
                completables.push((idx.clone(), completable))
            }

            if let Some(permanent) = permanent {
                permanents.push((idx.clone(), permanent))
            }
        }

        let completable = completables.take().map(|completables| ErrorSet {
            successes: successes.clone(),
            errors: completables
        });
        let permanent = permanents.take().map(|permanents| ErrorSet {
            successes: successes,
            errors: permanents
        });

        (completable, permanent)
    }
}

impl<Idx, Success, Err> ErrorSet<Idx, Success, Err> {
    /// Create an `ErrorSet` from its components.
    ///
    /// # Parameters
    ///
    /// - `successes`: The indexes and information for successes.
    ///
    /// - `errors`: The indexes and information for errors.
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

    /// Get the indexes and information for successes.
    #[inline]
    pub fn successes(&self) -> &[(Idx, Success)] {
        &self.successes
    }

    /// Get the indexes and information for errors.
    #[inline]
    pub fn errors(&self) -> &[(Idx, Err)] {
        &self.errors
    }

    /// Decompose an `ErrorSet` into its components.
    ///
    /// # Return Value
    ///
    /// Returns a tuple consisting of the following:
    ///
    /// 1. The indexes and information for successes.
    ///
    /// 1. The indexes and information for errors.
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
