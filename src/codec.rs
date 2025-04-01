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
use std::time::Instant;

use constellation_auth::cred::Credentials;
use constellation_common::codec::BytestreamCodec;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::CodecStreamError;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;
use log::error;

use crate::error::BatchError;
use crate::error::ErrorReportInfo;
use crate::frags::OutboundFrags;
use crate::large_obj::LargeObjDataError;
use crate::large_obj::LargeObjMsg;
use crate::large_obj::LargeObjMsgCodec;
use crate::large_obj::LargeObjMsgEncodeError;
use crate::stream::ConcurrentStream;
use crate::stream::LargeObjStream;
use crate::stream::PullStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamPrivateSingle;

/// Private unicast stream built from a [Read]/[Write] instance and a
/// [DatagramCodec].
///
/// This represents a private unicast stream that directly encodes and
/// sends messages.  This instance performs no batching at all, and
/// will ignore all batching-related API calls.  It will immediately
/// encode and send messages when its implementation of
/// [add](PushStreamAdd::add) is called.
pub struct DatagramCodecStream<Msg, IO, Codec: DatagramCodec<Msg> + Send> {
    msg: PhantomData<Msg>,
    /// Codec to use.
    codec: Codec,
    /// Low-level IO stream.
    io: IO
}

pub struct BytestreamCodecStream<Msg, IO, Codec: BytestreamCodec<Msg> + Send> {
    msg: PhantomData<Msg>,
    /// Codec to use.
    codec: Codec,
    /// Low-level IO stream.
    io: IO
}

/// Errors that can occur when sending an object fragment.
pub enum DatagramCodecFragError<Codec> {
    Frag { err: LargeObjDataError },
    Stream { err: Codec }
}

impl<Codec, T> ErrorReportInfo<T> for DatagramCodecFragError<Codec>
where
    Codec: ErrorReportInfo<T>
{
    #[inline]
    fn report_info(&self) -> Option<T> {
        if let DatagramCodecFragError::Frag { err } = self {
            err.report_info()
        } else {
            None
        }
    }
}

impl<Msg, IO, Codec> ConcurrentStream for DatagramCodecStream<Msg, IO, Codec>
where
    Codec: DatagramCodec<Msg> + Send,
    IO: ConcurrentStream
{
    #[inline]
    fn condvar(&self) -> Arc<Condvar> {
        self.io.condvar()
    }
}

impl<Codec> ScopedError for DatagramCodecFragError<Codec>
where
    Codec: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            DatagramCodecFragError::Frag { err } => err.scope(),
            DatagramCodecFragError::Stream { err } => err.scope()
        }
    }
}

impl<Msg, IO, Codec> BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send
{
    #[inline]
    pub fn create(
        codec: Codec,
        io: IO
    ) -> Self {
        BytestreamCodecStream {
            msg: PhantomData,
            codec: codec,
            io: io
        }
    }
}

impl<Msg, IO, Codec> DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: DatagramCodec<Msg> + Send
{
    #[inline]
    pub fn create(
        codec: Codec,
        io: IO
    ) -> Self {
        DatagramCodecStream {
            msg: PhantomData,
            codec: codec,
            io: io
        }
    }
}

impl<Stream> BatchError for DatagramCodecFragError<Stream>
where
    Stream: BatchError
{
    type Completable = DatagramCodecFragError<Stream::Completable>;
    type Permanent = DatagramCodecFragError<Stream::Permanent>;

    #[inline]
    fn split(self) -> (Option<Self::Completable>, Option<Self::Permanent>) {
        match self {
            DatagramCodecFragError::Frag { err } => {
                (None, Some(DatagramCodecFragError::Frag { err: err }))
            }
            DatagramCodecFragError::Stream { err } => {
                let (completable, permanent) = err.split();

                (
                    completable
                        .map(|res| DatagramCodecFragError::Stream { err: res }),
                    permanent
                        .map(|res| DatagramCodecFragError::Stream { err: res })
                )
            }
        }
    }
}

impl<Msg, IO, Codec> Credentials for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Credentials,
    Codec: BytestreamCodec<Msg> + Send
{
    type Cred = IO::Cred;
    type CredError = IO::CredError;

    #[inline]
    fn creds(&self) -> Result<Option<Self::Cred>, Self::CredError> {
        self.io.creds()
    }
}

impl<Msg, IO, Codec> Credentials for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Credentials,
    Codec: DatagramCodec<Msg> + Send
{
    type Cred = IO::Cred;
    type CredError = IO::CredError;

    #[inline]
    fn creds(&self) -> Result<Option<Self::Cred>, Self::CredError> {
        self.io.creds()
    }
}

impl<Ctx, Msg, IO, Codec> PushStream<Ctx>
    for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send
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

impl<Ctx, Msg, IO, Codec> PushStream<Ctx>
    for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
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

impl<Msg, IO, Codec> PushStreamPartyID for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send
{
    type PartyID = ();
}

impl<Msg, IO, Codec> PushStreamPartyID for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type PartyID = ();
}

impl<Ctx, Msg, IO, Codec> PushStreamAdd<Msg, Ctx>
    for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send,
    Codec::StreamEncodeError: BatchError
{
    type AddError = Codec::StreamEncodeError;
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

impl<Ctx, Msg, IO, Codec> PushStreamAdd<Msg, Ctx>
    for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type AddError = CodecStreamError<Codec::EncodeError, Error>;
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

impl<Ctx, Msg, IO, Codec> PushStreamPrivate<Ctx>
    for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send
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

impl<Ctx, Msg, IO, Codec> PushStreamPrivate<Ctx>
    for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
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

impl<Msg, IO, Codec> PullStream<Msg> for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Read,
    Codec: BytestreamCodec<Msg> + Send
{
    type PullError = Codec::StreamDecodeError;

    fn pull(&mut self) -> Result<Msg, Self::PullError> {
        self.codec
            .decode_from_stream(&mut self.io)
            .map(|(msg, _)| msg)
    }
}

impl<Msg, IO, Codec> PullStream<Msg> for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Read,
    Codec: DatagramCodec<Msg> + Send
{
    type PullError = CodecStreamError<Codec::DecodeError, Error>;

    fn pull(&mut self) -> Result<Msg, Self::PullError> {
        // ISSUE #4: avoid creating arrays like this
        let mut buf = vec![0; Codec::MAX_BYTES];

        let readlen = self
            .io
            .read(&mut buf)
            .map_err(|err| CodecStreamError::IO { err: err })?;

        self.codec
            .decode(&buf[..readlen])
            .map(|(msg, _)| msg)
            .map_err(|err| CodecStreamError::Codec { err: err })
    }
}

impl<Ctx, Msg, IO, Codec> PushStreamPrivateSingle<Msg, Ctx>
    for BytestreamCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: BytestreamCodec<Msg> + Send,
    Codec::StreamEncodeError: BatchError
{
    type CancelPushError = Infallible;
    type CancelPushRetry = Infallible;
    type PushError = Codec::StreamEncodeError;
    type PushRetry = Infallible;

    #[inline]
    fn push(
        &mut self,
        _ctx: &mut Ctx,
        msg: &Msg
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.codec
            .encode_to_stream(&mut self.io, msg)
            .map(|_| RetryResult::Success(()))
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

        self.push(ctx, msg).map(|_| RetryResult::Success(()))
    }

    #[inline]
    fn complete_push(
        &mut self,
        ctx: &mut Ctx,
        msg: &Msg,
        _err: <Self::PushError as BatchError>::Completable
    ) -> Result<RetryResult<Self::BatchID, Self::PushRetry>, Self::PushError>
    {
        self.push(ctx, msg).map(|_| RetryResult::Success(()))
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

impl<Ctx, Msg, IO, Codec> PushStreamPrivateSingle<Msg, Ctx>
    for DatagramCodecStream<Msg, IO, Codec>
where
    IO: Write,
    Codec: DatagramCodec<Msg> + Send
{
    type CancelPushError = Infallible;
    type CancelPushRetry = Infallible;
    type PushError = CodecStreamError<Codec::EncodeError, Error>;
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
            .map_err(|err| CodecStreamError::Codec { err: err })?;

        self.io
            .write_all(&buf)
            .map(RetryResult::Success)
            .map_err(|err| CodecStreamError::IO { err: err })
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

impl<Ctx, ObjID, Stream> LargeObjStream<ObjID, Ctx>
    for DatagramCodecStream<LargeObjMsg, Stream, LargeObjMsgCodec>
where
    ObjID: Into<usize>,
    Stream: Write
{
    type Frags = OutboundFrags;
    type PushFragError =
        DatagramCodecFragError<CodecStreamError<LargeObjMsgEncodeError, Error>>;
    type PushFragRetry = Instant;

    fn push_frag(
        &mut self,
        ctx: &mut Ctx,
        id: ObjID,
        frags: &mut Self::Frags
    ) -> Result<RetryResult<(), Self::PushFragRetry>, Self::PushFragError> {
        LargeObjMsg::frags(frags, id.into(), 1024)
            .map_err(|err| DatagramCodecFragError::Frag { err: err })?
            .map_ok(|msg| {
                self.push(ctx, &msg).map_err(|err| {
                    DatagramCodecFragError::Stream { err: err }
                })?;

                Ok(())
            })
    }

    fn retry_push_frag(
        &mut self,
        ctx: &mut Ctx,
        id: ObjID,
        frags: &mut Self::Frags,
        _retry: Self::PushFragRetry
    ) -> Result<RetryResult<(), Self::PushFragRetry>, Self::PushFragError> {
        self.push_frag(ctx, id, frags)
    }

    fn complete_push_frag(
        &mut self,
        ctx: &mut Ctx,
        id: ObjID,
        frags: &mut Self::Frags,
        _err: <Self::PushFragError as BatchError>::Completable
    ) -> Result<RetryResult<(), Self::PushFragRetry>, Self::PushFragError> {
        self.push_frag(ctx, id, frags)
    }
}

impl<Encode> Display for DatagramCodecFragError<Encode>
where
    Encode: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            DatagramCodecFragError::Frag { err } => err.fmt(f),
            DatagramCodecFragError::Stream { err } => err.fmt(f)
        }
    }
}
