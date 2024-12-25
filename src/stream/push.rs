// Copyright © 2024 The Johns Hopkins Applied Physics Laboratory LLC.
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

use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::net::SharedMsgs;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::error::BatchError;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamAddParty;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::PushStreamSharedSingle;

enum PushEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamAdd<Msg, Ctx>
        + PushStreamAddParty<Ctx>
        + Send,
    Msg: Clone + Send {
    Batch {
        msgs: Vec<Msg>,
        parties: Vec<Stream::PartyID>,
        batches: Stream::StartBatchStreamBatches,
        retry: Stream::StartBatchRetry
    },
    Abort {
        flags: Stream::StreamFlags,
        retry: Stream::AbortBatchRetry
    },
    Parties {
        msgs: Vec<Msg>,
        batch: Stream::BatchID,
        retry: Stream::AddPartiesRetry
    },
    Add {
        msgs: Vec<Msg>,
        msg: Msg,
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::AddRetry
    },
    Finish {
        batch: Stream::BatchID,
        retry: Stream::FinishBatchRetry
    },
    Cancel {
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::CancelBatchRetry
    }
}

pub struct PushStreamSharedThread<Msg, Msgs, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamSharedSingle<Msg, Ctx>
        + PushStreamShared
        + PushStreamParties
        + Send,
    Stream::StartBatchStreamBatches: Send,
    Stream::StartBatchRetry: Send,
    Stream::AbortBatchRetry: Send,
    Stream::AddPartiesRetry: Send,
    Stream::AddRetry: Send,
    Stream::FinishBatchRetry: Send,
    Stream::CancelBatchRetry: Send,
    Stream::StreamFlags: Send,
    Stream::PartyID: From<usize> + Send,
    Stream::BatchID: Send,
    Msgs: SharedMsgs<Stream::PartyID, Msg>,
    Msg: Clone + Send,
    Ctx: Send + Sync {
    ctx: Ctx,
    /// Buffer for sends in progress.
    pending: Vec<PushEntry<Msg, Stream, Ctx>>,
    /// Source of outbound messages.
    msgs: Msgs,
    /// `Notify` instance used to indicate that new messages are
    /// available to be sent.
    notify: Notify,
    /// Flag to use to shut the stream down.
    shutdown: ShutdownFlag,
    /// Stream to use to send.
    stream: Stream
}

impl<Msg, Stream, Ctx> RetryWhen for PushEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamAdd<Msg, Ctx>
        + PushStreamAddParty<Ctx>
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + Send,
    Msg: Clone + Send
{
    fn when(&self) -> Instant {
        match self {
            PushEntry::Batch { retry, .. } => retry.when(),
            PushEntry::Abort { retry, .. } => retry.when(),
            PushEntry::Parties { retry, .. } => retry.when(),
            PushEntry::Add { retry, .. } => retry.when(),
            PushEntry::Finish { retry, .. } => retry.when(),
            PushEntry::Cancel { retry, .. } => retry.when()
        }
    }
}

impl<Msg, Stream, Ctx> PushEntry<Msg, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamAddParty<Ctx>
        + PushStreamAdd<Msg, Ctx>
        + Send,
    Stream::PartyID: From<usize>,
    Msg: 'static + Clone + Send
{
    fn complete_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::CancelBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while cancelling message");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_cancel_batch(
                    ctx,
                    &mut flags,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        trace!(target: "push-entry",
                           "successfully completed cancellation");

                        RetryResult::Success(())
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Cancel {
                            batch: batch_id,
                            retry: retry,
                            flags: flags
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_cancel_batch(
                        ctx, stream, flags, batch_id, err
                    )
                }
            }
            // Unrecoverable errors occurred canceling the batch.
            (_, Some(permanent)) => {
                error!(target: "push-entry",
                       "unrecoverable error canceling batch: {}",
                       permanent);

                RetryResult::Success(())
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.cancel_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Cancel {
                    batch: batch_id,
                    retry: retry,
                    flags: flags
                })
            }
            Err(err) => {
                Self::complete_cancel_batch(ctx, stream, flags, batch_id, err)
            }
        }
    }

    fn complete_finish(
        ctx: &mut Ctx,
        stream: &mut Stream,
        flags: &mut Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::FinishBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while finishing batch");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_finish_batch(
                    ctx,
                    flags,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "push-entry",
                           "successfully finished batch");

                        RetryResult::Success(())
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Finish {
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => {
                        Self::complete_finish(ctx, stream, flags, batch_id, err)
                    }
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred canceling the batch.
                error!(target: "push-entry",
                       "unrecoverable error finishing batch: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_finish_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.finish_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Finish {
                    batch: batch_id.clone(),
                    retry: retry
                })
            }
            Err(err) => Self::complete_finish(
                ctx,
                stream,
                &mut flags,
                batch_id.clone(),
                err
            )
        }
    }

    fn complete_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID,
        err: Stream::AddError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while adding message");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_add(
                    ctx,
                    &mut flags,
                    &msg,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "push-entry",
                           "successfully added message");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Add {
                            msgs: msgs,
                            msg: msg,
                            flags: flags,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_add(
                        ctx, stream, flags, msgs, msg, batch_id, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "push-entry",
                       "unrecoverable error adding message: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_add_msg(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match stream.add(ctx, &mut flags, &msg, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Add {
                    msgs: msgs,
                    msg: msg,
                    flags: flags,
                    batch: batch_id,
                    retry: retry
                })
            }
            Err(err) => {
                Self::complete_add(ctx, stream, flags, msgs, msg, batch_id, err)
            }
        }
    }

    fn try_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match msgs.pop() {
            Some(msg) => {
                let flags = stream.empty_flags();

                Self::try_add_msg(ctx, stream, flags, msgs, msg, batch_id)
            }
            None => Self::try_finish_batch(ctx, stream, batch_id)
        }
    }

    fn complete_add_parties(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>,
        batch_id: Stream::BatchID,
        err: Stream::AddPartiesError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while adding parties");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_add_parties(ctx, &batch_id, completable) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "push-entry",
                           "successfully added parties");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Parties {
                            msgs: msgs,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_add_parties(
                        ctx, stream, msgs, batch_id, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "push-entry",
                       "unrecoverable error adding parties: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_add_parties(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match stream.add_parties(ctx, parties.into_iter(), &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Parties {
                    msgs: msgs,
                    batch: batch_id,
                    retry: retry
                })
            }
            Err(err) => {
                Self::complete_add_parties(ctx, stream, msgs, batch_id, err)
            }
        }
    }

    fn complete_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>,
        mut batches: Stream::StartBatchStreamBatches,
        err: Stream::StartBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "push-entry",
               "attempting to recover from error while creating batch");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_start_batch(
                    ctx,
                    &mut batches,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(batch_id)) => {
                        trace!(target: "push-entry",
                           "successfully created batch");

                        Self::try_add_parties(
                            ctx, stream, parties, msgs, batch_id
                        )
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Batch {
                            msgs: msgs,
                            batches: batches,
                            parties: parties,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_start_batch(
                        ctx, stream, parties, msgs, batches, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "push-entry",
                       "unrecoverable error adding parties: {}",
                       permanent);

                // Report the failure
                if let Err(err) = stream.report_error(&permanent) {
                    error!(target: "push-entry",
                           "failed to report errors to stream: {}",
                           err);
                }

                let mut flags = stream.empty_flags();

                stream
                    .abort_start_batch(ctx, &mut flags, permanent)
                    .map_retry(|retry| PushEntry::Abort {
                        flags: flags,
                        retry: retry
                    })
            }
            (None, None) => {
                error!(target: "push-entry",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        let mut batches = stream.empty_batches();

        match stream.start_batch(ctx, &mut batches) {
            // It succeeded.
            Ok(RetryResult::Success(batch_id)) => {
                Self::try_add_parties(ctx, stream, parties, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(PushEntry::Batch {
                    msgs: msgs,
                    batches: batches,
                    parties: parties,
                    retry: retry
                })
            }
            Err(err) => Self::complete_start_batch(
                ctx, stream, parties, msgs, batches, err
            )
        }
    }

    fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> RetryResult<(), Self> {
        match self {
            PushEntry::Batch {
                msgs,
                parties,
                mut batches,
                retry
            } => match stream.retry_start_batch(ctx, &mut batches, retry) {
                // It succeeded.
                Ok(RetryResult::Success(batch_id)) => {
                    Self::try_add_parties(ctx, stream, parties, msgs, batch_id)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Batch {
                        msgs: msgs,
                        batches: batches,
                        parties: parties,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_start_batch(
                    ctx, stream, parties, msgs, batches, err
                )
            },
            PushEntry::Abort { mut flags, retry } => stream
                .retry_abort_start_batch(ctx, &mut flags, retry)
                .map_retry(|retry| PushEntry::Abort {
                    flags: flags,
                    retry: retry
                }),
            PushEntry::Parties { msgs, batch, retry } => {
                match stream.retry_add_parties(ctx, &batch, retry) {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        Self::try_add(ctx, stream, msgs, batch)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Parties {
                            msgs: msgs,
                            batch: batch,
                            retry: retry
                        })
                    }
                    Err(err) => Self::complete_add_parties(
                        ctx, stream, msgs, batch, err
                    )
                }
            }
            PushEntry::Add {
                msgs,
                msg,
                batch,
                mut flags,
                retry
            } => match stream.retry_add(ctx, &mut flags, &msg, &batch, retry) {
                // It succeeded.
                Ok(RetryResult::Success(_)) => {
                    Self::try_add(ctx, stream, msgs, batch)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Add {
                        msgs: msgs,
                        msg: msg,
                        flags: flags,
                        batch: batch,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_add(
                    ctx, stream, flags, msgs, msg, batch, err
                )
            },
            PushEntry::Finish { batch, retry } => {
                let mut flags = stream.empty_flags();

                match stream.retry_finish_batch(ctx, &mut flags, &batch, retry)
                {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(PushEntry::Finish {
                            batch: batch,
                            retry: retry
                        })
                    }
                    Err(err) => Self::complete_finish(
                        ctx, stream, &mut flags, batch, err
                    )
                }
            }
            PushEntry::Cancel {
                batch,
                retry,
                mut flags
            } => match stream.retry_cancel_batch(ctx, &mut flags, &batch, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(PushEntry::Cancel {
                        batch: batch,
                        retry: retry,
                        flags: flags
                    })
                }
                Err(err) => {
                    Self::complete_cancel_batch(ctx, stream, flags, batch, err)
                }
            }
        }
    }

    #[inline]
    fn from_try_send(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        Self::try_start_batch(ctx, stream, parties, msgs)
    }
}

impl<Msg, Msgs, Stream, Ctx> PushStreamSharedThread<Msg, Msgs, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamSharedSingle<Msg, Ctx>
        + PushStreamShared
        + PushStreamParties
        + Send,
    Stream::StartBatchStreamBatches: Send,
    Stream::StartBatchRetry: Send,
    Stream::AbortBatchRetry: Send,
    Stream::AddPartiesRetry: Send,
    Stream::AddRetry: Send,
    Stream::FinishBatchRetry: Send,
    Stream::CancelBatchRetry: Send,
    Stream::StreamFlags: Send,
    Stream::PartyID: From<usize> + Send,
    Stream::BatchID: Send,
    Msgs: 'static + SharedMsgs<Stream::PartyID, Msg> + Send,
    Msg: 'static + Clone + Send,
    Ctx: 'static + Send + Sync
{
    #[inline]
    pub fn create(
        ctx: Ctx,
        msgs: Msgs,
        notify: Notify,
        stream: Stream,
        shutdown: ShutdownFlag
    ) -> Self {
        PushStreamSharedThread {
            pending: Vec::new(),
            msgs: msgs,
            notify: notify,
            shutdown: shutdown,
            stream: stream,
            ctx: ctx
        }
    }

    #[inline]
    pub fn with_capacity(
        ctx: Ctx,
        msgs: Msgs,
        notify: Notify,
        stream: Stream,
        shutdown: ShutdownFlag,
        size: usize
    ) -> Self {
        PushStreamSharedThread {
            pending: Vec::with_capacity(size),
            msgs: msgs,
            notify: notify,
            shutdown: shutdown,
            stream: stream,
            ctx: ctx
        }
    }

    /// Get the `Notify` used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Notify {
        self.notify.clone()
    }

    fn update_from_outbound(
        &mut self
    ) -> Result<Option<Instant>, Msgs::MsgsError> {
        debug!(target: "push-stream-shared-thread",
               "fetching new outbound messages");

        let (groups, next) = self.msgs.msgs()?;

        if let Some(groups) = groups {
            // Go through each group and try sending it
            for (parties, msgs) in groups {
                if let RetryResult::Retry(retry) = PushEntry::from_try_send(
                    &mut self.ctx,
                    &mut self.stream,
                    parties,
                    msgs
                ) {
                    // We got a retry somewhere along the process, store it.
                    self.pending.push(retry)
                }
            }
        }

        Ok(next)
    }

    fn retry_pending(
        &mut self,
        now: Instant
    ) -> Option<Instant> {
        debug!(target: "push-stream-shared-thread",
               "retrying pending operations");

        let mut curr = Vec::with_capacity(self.pending.len());

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.pending
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        while self.pending.last().map_or(false, |ent| ent.when() <= now) {
            match self.pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "push-stream-shared-thread",
                           "pop should not be empty");
                }
            }
        }

        // The last entry should now be the first time past the present.
        let out = self.pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            if let RetryResult::Retry(retry) =
                ent.exec(&mut self.ctx, &mut self.stream)
            {
                // We got a retry somewhere along the process, store it.
                self.pending.push(retry)
            }
        }

        out
    }

    fn run(mut self) {
        let mut next_outbound = Some(Instant::now());
        let mut next_pending = None;
        let mut valid = true;

        info!(target: "push-stream-shared-thread",
              "consensus component send thread starting");

        while valid && self.shutdown.is_live() {
            let now = Instant::now();

            if let Some(when) = next_outbound &&
                when <= now
            {
                match self.update_from_outbound() {
                    Ok(next) => next_outbound = next,
                    Err(err) => {
                        error!(target: "push-stream-shared-thread",
                               "error obtaining messages: {}",
                               err);

                        if err.scope() >= ErrorScope::Shutdown {
                            valid = false
                        }
                    }
                }
            }

            if let Some(next) = next_pending &&
                next <= now
            {
                next_pending = self.retry_pending(now)
            }

            match next_pending.map_or(next_outbound, |next| {
                next_outbound.map(|when| when.max(next))
            }) {
                Some(when) => {
                    let now = Instant::now();

                    if now < when {
                        let duration = when - now;

                        trace!(target: "push-stream-shared-thread",
                               "next activity at {}.{:03}",
                               duration.as_secs(), duration.subsec_millis());

                        match self.notify.wait_timeout(duration) {
                            Ok(notify) => {
                                if notify {
                                    next_outbound = Some(now)
                                }
                            }
                            Err(err) => {
                                error!(target: "push-stream-shared-thread",
                                       "error waiting for notification: {}",
                                       err);

                                valid = false
                            }
                        }
                    }
                }
                None => {
                    trace!(target: "push-stream-shared-thread",
                           "waiting for notification indefinitely");

                    match self.notify.wait() {
                        Ok(_) => next_outbound = Some(now),
                        Err(err) => {
                            error!(target: "push-stream-shared-thread",
                                   "error waiting for notification: {}",
                                   err);

                            valid = false
                        }
                    }
                }
            }
        }

        debug!(target: "push-stream-shared-thread",
               "consensus component send thread exiting");
    }

    pub fn start(self) -> JoinHandle<()> {
        spawn(move || self.run())
    }
}
