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

//! Low-level streams from low-level I/O traits and codecs.
//!
//! This module provides implementations of the stream interfaces for
//! combinations of low-level I/O ([Read] and [Write]) instances
//! together with [DatagramCodec]s.  This provides the base-level
//! implementations of private unicast streams.
use std::convert::Infallible;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Condvar;

use constellation_auth::cred::Credentials;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use log::error;

use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::stream::ConcurrentStream;
use crate::stream::PullStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;
use crate::stream::PushStreamReportError;

/// Private unicast stream built from a [Read]/[Write] instance and a
/// [DatagramCodec].
///
/// This represents a private unicast stream that directly encodes and
/// sends messages.  This instance performs no batching at all, and
/// will ignore all batching-related API calls.  It will immediately
/// encode and send messages when its implementation of
/// [add](PushStreamAdd::add) is called.
pub struct DatagramCodecStream<Msg, Stream, Codec: DatagramCodec<Msg> + Send> {
    msg: PhantomData<Msg>,
    /// Codec to use.
    codec: Codec,
    /// Low-level IO stream.
    stream: Stream
}

/// Errors that can occur when sending a message.
pub enum DatagramCodecStreamError<Codec, IO> {
    /// Error occurred when encoding or decoding the message.
    Codec {
        /// Error while encoding or decoding the message.
        codec: Codec
    },
    /// I/O level error occurred.
    IO {
        /// Error while writing to the I/O object.
        err: IO
    }
}

impl<Codec, IO, T> ErrorReportInfo<T> for DatagramCodecStreamError<Codec, IO>
where
    Codec: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let DatagramCodecStreamError::Codec { codec } = self {
            codec.report_info()
        } else {
            None
        }
    }
}

impl<Codec, IO> ScopedError for DatagramCodecStreamError<Codec, IO>
where
    IO: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            DatagramCodecStreamError::Codec { .. } => ErrorScope::Msg,
            DatagramCodecStreamError::IO { err } => err.scope()
        }
    }
}

impl<Msg, Stream, Codec> ConcurrentStream
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Codec: DatagramCodec<Msg> + Send,
    Stream: ConcurrentStream
{
    #[inline]
    fn condvar(&self) -> Arc<Condvar> {
        self.stream.condvar()
    }
}

impl<Encode, Write> BatchError for DatagramCodecStreamError<Encode, Write>
where
    Encode: Display,
    Write: BatchError
{
    type Completable = DatagramCodecStreamError<Infallible, Write::Completable>;
    type Permanent = DatagramCodecStreamError<Encode, Write::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            DatagramCodecStreamError::Codec { codec } => {
                (None, Some(DatagramCodecStreamError::Codec { codec }))
            }
            DatagramCodecStreamError::IO { err } => {
                let (completable, permanent) = err.split();

                (
                    completable
                        .map(|res| DatagramCodecStreamError::IO { err: res }),
                    permanent
                        .map(|res| DatagramCodecStreamError::IO { err: res })
                )
            }
        }
    }
}

impl<Msg, Stream, Codec> DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    #[inline]
    pub fn create(
        codec: Codec,
        stream: Stream
    ) -> Self {
        DatagramCodecStream {
            msg: PhantomData,
            codec: codec,
            stream: stream
        }
    }
}

impl<Msg, Stream, Codec> Credentials for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Credentials,
    Codec: DatagramCodec<Msg> + Send
{
    type Cred<'a> = Stream::Cred<'a>
    where Self: 'a;
    type CredError = Stream::CredError;

    #[inline]
    fn creds(&self) -> Result<Option<Self::Cred<'_>>, Self::CredError> {
        self.stream.creds()
    }
}

impl<Ctx, Msg, Stream, Codec> PushStream<Ctx>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type BatchID = ();
    type CancelBatchError = Infallible;
    type CancelBatchRetry = Infallible;
    type FinishBatchError = Infallible;
    type FinishBatchRetry = Infallible;
    type ReportError = Infallible;
    type StreamFlags = ();

    #[inline]
    fn empty_flags_with_capacity(_size: usize) -> Self::StreamFlags {}

    #[inline]
    fn finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn retry_finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID,
        _retry: Self::FinishBatchRetry
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        error!(target: "datagram-codec-stream",
               "should never call retry_finish_batch");

        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn complete_finish_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID,
        _err: <Self::FinishBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::FinishBatchRetry>, Self::FinishBatchError>
    {
        error!(target: "datagram-codec-stream",
               "should never call complete_finish_batch");

        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn retry_cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID,
        _retry: Self::CancelBatchRetry
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        error!(target: "datagram-codec-stream",
               "should never call retry_cancel_batch");

        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn complete_cancel_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _batch: &Self::BatchID,
        _err: <Self::CancelBatchError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelBatchRetry>, Self::CancelBatchError>
    {
        error!(target: "datagram-codec-stream",
               "should never call complete_cancel_batch");

        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn cancel_batches(&mut self) {}

    #[inline]
    fn report_failure(
        &mut self,
        _batch: &Self::BatchID
    ) -> Result<(), Self::ReportError> {
        Ok(())
    }
}

impl<Msg, Stream, Codec> PushStreamReportError<Infallible>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Codec: DatagramCodec<Msg> + Send
{
    type ReportError = Infallible;

    fn report_error(
        &mut self,
        _error: &Infallible
    ) -> Result<(), Self::ReportError> {
        error!(target: "datagram-codec-stream",
               "should never call report_error");

        Ok(())
    }
}

impl<Msg, Stream, Codec> PushStreamPartyID
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type PartyID = ();
}

impl<Ctx, Msg, Stream, Codec> PushStreamAdd<Msg, Ctx>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type AddError = DatagramCodecStreamError<Codec::EncodeError, Error>;
    type AddRetry = Infallible;

    #[inline]
    fn add(
        &mut self,
        ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        msg: &Msg,
        _batch: &Self::BatchID
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.push(ctx, msg)
    }

    #[inline]
    fn retry_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        _retry: Self::AddRetry
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        error!(target: "datagram-codec-stream",
               "should never call retry_add");

        self.add(ctx, flags, msg, batch)
    }

    #[inline]
    fn complete_add(
        &mut self,
        ctx: &mut Ctx,
        flags: &mut Self::StreamFlags,
        msg: &Msg,
        batch: &Self::BatchID,
        _err: <Self::AddError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::AddRetry>, Self::AddError> {
        self.add(ctx, flags, msg, batch)
    }
}

impl<Ctx, Msg, Stream, Codec> PushStreamPrivate<Ctx>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type AbortBatchRetry = Infallible;
    type CreateBatchError = Infallible;
    type CreateBatchRetry = Infallible;
    type SelectError = Infallible;
    type SelectRetry = Infallible;
    type Selections = ();
    type StartBatchError = Infallible;
    type StartBatchRetry = Infallible;
    type StartBatchStreamBatches = ();

    #[inline]
    fn empty_selections_with_capacity(_size: usize) -> Self::Selections {}

    #[inline]
    fn empty_batches_with_capacity(
        _size: usize
    ) -> Self::StartBatchStreamBatches {
    }

    #[inline]
    fn select(
        &mut self,
        _ctx: &mut Ctx,
        _selections: &mut Self::Selections
    ) -> Result<RetryResult<Self::BatchID, Self::SelectRetry>, Self::SelectError>
    {
        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn retry_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        _retry: Self::SelectRetry
    ) -> Result<RetryResult<Self::BatchID, Self::SelectRetry>, Self::SelectError>
    {
        error!(target: "datagram-codec-stream",
               "should never call retry_select");

        self.select(ctx, selections)
    }

    #[inline]
    fn complete_select(
        &mut self,
        ctx: &mut Ctx,
        selections: &mut Self::Selections,
        _err: <Self::SelectError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::SelectRetry>, Self::SelectError>
    {
        error!(target: "datagram-codec-stream",
               "should never call complete_select");

        self.select(ctx, selections)
    }

    #[inline]
    fn create_batch(
        &mut self,
        _ctx: &mut Ctx,
        _batches: &mut Self::StartBatchStreamBatches,
        _selections: &Self::Selections
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn retry_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        _retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        error!(target: "datagram-codec-stream",
               "should never call retry_create_batch");

        self.create_batch(ctx, batches, selections)
    }

    #[inline]
    fn complete_create_batch(
        &mut self,
        ctx: &mut Ctx,
        batches: &mut Self::StartBatchStreamBatches,
        selections: &Self::Selections,
        _err: <Self::CreateBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::CreateBatchRetry>,
        Self::CreateBatchError
    > {
        error!(target: "datagram-codec-stream",
               "should never call complete_create_batch");

        self.create_batch(ctx, batches, selections)
    }

    #[inline]
    fn start_batch(
        &mut self,
        _ctx: &mut Ctx
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        Ok(RetryResult::Success(()))
    }

    #[inline]
    fn retry_start_batch(
        &mut self,
        ctx: &mut Ctx,
        _retry: Self::StartBatchRetry
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        error!(target: "datagram-codec-stream",
               "should never call retry_start_batch");

        self.start_batch(ctx)
    }

    #[inline]
    fn complete_start_batch(
        &mut self,
        ctx: &mut Ctx,
        _err: <Self::StartBatchError as BatchError>::Completable
    ) -> Result<
        RetryResult<Self::BatchID, Self::StartBatchRetry>,
        Self::StartBatchError
    > {
        error!(target: "datagram-codec-stream",
               "should never call complete_start_batch");

        self.start_batch(ctx)
    }

    #[inline]
    fn abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _err: <Self::StartBatchError as BatchError>::Completable
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        RetryResult::Success(())
    }

    #[inline]
    fn retry_abort_start_batch(
        &mut self,
        _ctx: &mut Ctx,
        _flags: &mut Self::StreamFlags,
        _retry: Self::AbortBatchRetry
    ) -> RetryResult<(), Self::AbortBatchRetry> {
        error!(target: "datagram-codec-stream",
               "should never call retry_abort_start_batch");

        RetryResult::Success(())
    }
}

impl<Msg, Stream, Codec> PullStream<Msg>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Read,
    Codec: DatagramCodec<Msg> + Send,
    [(); Codec::MAX_BYTES]:
{
    type PullError = DatagramCodecStreamError<Codec::DecodeError, Error>;

    fn pull(&mut self) -> Result<Msg, Self::PullError> {
        // ISSUE #4: avoid creating arrays like this
        let mut buf = [0; Codec::MAX_BYTES];

        let readlen = self
            .stream
            .read(&mut buf)
            .map_err(|err| DatagramCodecStreamError::IO { err: err })?;

        self.codec
            .decode(&buf[..readlen])
            .map(|(msg, _)| msg)
            .map_err(|err| DatagramCodecStreamError::Codec { codec: err })
    }
}

impl<Ctx, Msg, Stream, Codec> PushStreamPrivateSingle<Msg, Ctx>
    for DatagramCodecStream<Msg, Stream, Codec>
where
    Stream: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type CancelPushError = Infallible;
    type CancelPushRetry = Infallible;
    type PushError = DatagramCodecStreamError<Codec::EncodeError, Error>;
    type PushRetry = Infallible;

    #[inline]
    fn push(
        &mut self,
        _ctx: &mut Ctx,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        // ISSUE #5: Find a way to avoid repeatedly encoding messages
        // like this
        let buf = self
            .codec
            .encode_to_vec(msg)
            .map_err(|err| DatagramCodecStreamError::Codec { codec: err })?;

        self.stream
            .write_all(&buf)
            .map(RetryResult::Success)
            .map_err(|err| DatagramCodecStreamError::IO { err: err })
    }

    #[inline]
    fn retry_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        _retry: Self::PushRetry
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        error!(target: "datagram-codec-stream",
               "should never call retry_push");

        self.push(ctx, msg)
    }

    #[inline]
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        _err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.push(ctx, msg)
    }

    fn cancel_push(
        &mut self,
        _ctx: &mut Ctx,
        _err: <Self::PushError as BatchError>::Permanent
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        Ok(RetryResult::Success(()))
    }

    fn retry_cancel_push(
        &mut self,
        _ctx: &mut Ctx,
        _retry: Self::CancelPushRetry
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        error!(target: "datagram-codec-stream",
               "should never call retry_cancel_push");

        Ok(RetryResult::Success(()))
    }

    fn complete_cancel_push(
        &mut self,
        _ctx: &mut Ctx,
        _err: <Self::CancelPushError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::CancelPushRetry>, Self::CancelPushError>
    {
        error!(target: "datagram-codec-stream",
               "should never call complete_cancel_push");

        Ok(RetryResult::Success(()))
    }
}

impl<Encode, Write> Display for DatagramCodecStreamError<Encode, Write>
where
    Encode: Display,
    Write: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DatagramCodecStreamError::Codec { codec } => codec.fmt(f),
            DatagramCodecStreamError::IO { err } => err.fmt(f)
        }
    }
}
