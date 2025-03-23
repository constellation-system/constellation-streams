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

//! Manager threads for various kinds of push streams.

use std::fmt::Display;
use std::time::Instant;

use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use log::error;
use log::trace;

use crate::error::BatchError;
use crate::stream::LargeObjStream;
use crate::stream::PushStreamReportError;

pub mod private;
pub mod shared;

pub trait PushModeCreate {
    type Config: Clone;

    fn create(config: Self::Config) -> Self;
}

pub trait PushModeRetry<Stream, Ctx>: PushModeCreate {
    type RetryError: Display + ScopedError;

    fn retry_pending(
        &mut self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        now: Instant
    ) -> Result<Option<Instant>, Self::RetryError>;
}

pub trait PushMode<Stream, Msgs, Ctx>: PushModeRetry<Stream, Ctx> {
    type SendError: Display + ScopedError;

    fn send_from_outbound(
        &mut self,
        ctx: &mut Ctx,
        msgs: &mut Msgs,
        stream: &mut Stream,
    ) -> Result<Option<Instant>, Self::SendError>;
}

pub(crate) enum LargeObjEntry<ObjID, Stream, Ctx>
where
    Stream: LargeObjStream<ObjID, Ctx>,
    ObjID: Clone + Into<usize> {
    PushFrags {
        id: ObjID,
        retry: Stream::PushFragRetry
    }
}

impl<ObjID, Stream, Ctx> RetryWhen for LargeObjEntry<ObjID, Stream, Ctx>
where
    Stream: LargeObjStream<ObjID, Ctx>,
    ObjID: Clone + Into<usize> {

    fn when(&self) -> Instant {
        match self {
            LargeObjEntry::PushFrags { retry, .. } => retry.when()
        }
    }
}

impl<ObjID, Stream, Ctx> LargeObjEntry<ObjID, Stream, Ctx>
where
    Stream: LargeObjStream<ObjID, Ctx>
      + PushStreamReportError<
            <Stream::PushFragError as BatchError>::Permanent
        >,
    ObjID: Clone + Into<usize> {

    fn complete_push_frags(
        ctx: &mut Ctx,
        stream: &mut Stream,
        frags: &mut Stream::Frags,
        id: ObjID,
        err: Stream::PushFragError
    ) -> RetryResult<(), Self> {
        trace!(target: "large-obj-entry",
               "attempting to recover from error while pushing fragments");

        match err.split() {
            (Some(completable), None) => match stream.complete_push_frags(
                ctx,
                id.clone(),
                frags,
                completable
            ) {
                // It succeeded.
                Ok(RetryResult::Success(())) => {
                    trace!(target: "large-obj-entry",
                       "successfully finished batch");

                    RetryResult::Success(())
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(LargeObjEntry::PushFrags {
                        retry: retry,
                        id: id
                    })
                }
                // More errors; recurse again.
                Err(err) => {
                    Self::complete_push_frags(ctx, stream, frags, id, err)
                }
            }
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred.
                error!(target: "large-obj-entry",
                       "unrecoverable error pushing fragments: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error(&permanent)
                {
                    error!(target: "large-obj-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                RetryResult::Success(())
            }
            (None, None) => {
                error!(target: "large-obj-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    pub(crate) fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream,
        frags: &mut Stream::Frags
    ) -> RetryResult<(), Self> {
        match self {
            LargeObjEntry::PushFrags { id, retry } => match stream
                .retry_push_frags(ctx, id.clone(), frags, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(())) => RetryResult::Success(()),
                 // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(LargeObjEntry::PushFrags {
                        retry: retry,
                        id: id
                    })
                }
                Err(err) => Self::complete_push_frags(ctx, stream, frags, id, err)
            }
        }
    }
}
