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

use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::AuthNed;
use constellation_auth::authn::MsgAuthN;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::RecoverableError;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::net::PrivateMsgs;
use constellation_common::net::SharedMsgs;
use constellation_common::retry::RetryIndefResult;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::sched::DenseItemID;

use crate::addrs::Addrs;
use crate::addrs::AddrsCreate;
use crate::channels::ChannelParam;
use crate::channels::Channels;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::config::PartyConfig;
use crate::config::PrivateDatagramModeConfig;
use crate::config::PrivateLargeObjModeConfig;
use crate::config::SharedDatagramModeConfig;
use crate::config::SharedLargeObjModeConfig;
use crate::config::StreamMulticasterConfig;
use crate::error::CompoundBatchError;
use crate::error::ErrorReportInfo;
use crate::error::ErrorSet;
use crate::error::SelectionsError;
use crate::frags::Frags;
use crate::large_obj::LargeObjMsg;
use crate::large_obj::LargeObjProto;
use crate::large_obj::LargeObjProtoTypes;
use crate::multicast::MulticastStreamIdx;
use crate::multicast::StreamMulticaster;
use crate::multicast::StreamMulticasterFrags;
use crate::multicast::StreamMulticasterSelections;
use crate::multicast::StreamMulticasterStartError;
use crate::select::ConnChannelID;
use crate::select::OutboundEndpointConfig;
use crate::select::SelectorBatchError;
use crate::select::SelectorBatchSelectError;
use crate::select::SelectorSelections;
use crate::select::StreamSelector;
use crate::select::StreamSelectorBatch;
use crate::select::StreamSelectorCreateError;
use crate::select::StreamSelectorSelectRefreshError;
use crate::select::ThreadedStreamSelectorError;
use crate::select::dispatch::DispatchSelector;
use crate::select::dispatch::DispatchSelectorRefreshError;
use crate::select::dispatch::DispatchSelectorSelectError;
use crate::stream::CompoundBatchID;
use crate::stream::LargeObjOfferStream;
use crate::stream::LargeObjStream;
use crate::stream::PullStream;
use crate::stream::PushStream;
use crate::stream::PushStreamAdd;
use crate::stream::PushStreamParties;
use crate::stream::PushStreamPartyID;
use crate::stream::PushStreamPrivate;
use crate::stream::PushStreamReportBatchError;
use crate::stream::PushStreamReportError;
use crate::stream::PushStreamShared;
use crate::stream::StreamFinishCancel;
use crate::stream::StreamID;
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::threads::PushMode;
use crate::threads::dispatch::Dispatch;
use crate::threads::dispatch::DispatchThreadCtx;
use crate::threads::poll::PollThreadCtx;
use crate::threads::private::PrivateDatagramPushMode;
use crate::threads::private::PrivateLargeObjPushMode;
use crate::threads::shared::SharedDatagramPushMode;
use crate::threads::shared::SharedLargeObjPushMode;

pub trait PrivateLargeObjPushModeTypes<Ctx> {
    type Frags: Frags;
    type BatchID: Clone + Debug + Display;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddErrorCompletable: ScopedError;
    type AddError: RecoverableError<Completable = Self::AddErrorCompletable>;
    type CancelBatchErrorCompletable: ScopedError;
    type CancelBatchError: RecoverableError<
        Completable = Self::CancelBatchErrorCompletable
    >;
    type FinishBatchErrorCompletable: ScopedError;
    type FinishBatchError: RecoverableError<
        Completable = Self::FinishBatchErrorCompletable
    >;
    type StartBatchErrorCompletable: ScopedError;
    type StartBatchError: RecoverableError<
        Completable = Self::StartBatchErrorCompletable
    >;
    type PushFragErrorCompletable: ScopedError;
    type PushFragError: RecoverableError<
        Completable = Self::PushFragErrorCompletable
    >;
    type PushOfferErrorCompletable: ScopedError;
    type PushOfferError: RecoverableError<
        Completable = Self::PushOfferErrorCompletable
    >;
    type StreamFlags: Default;
    type Stream: PushStreamReportBatchError<
            <Self::FinishBatchError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::PushFragError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::PushOfferError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Self::AddError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamPrivate<Ctx, StartBatchError = Self::StartBatchError>
        + PushStreamAdd<LargeObjMsg<Self::HashID>, Ctx, AddError = Self::AddError>
        + PushStream<
            Ctx,
            BatchID = Self::BatchID,
            StreamFlags = Self::StreamFlags,
            CancelBatchError = Self::CancelBatchError,
            FinishBatchError = Self::FinishBatchError
        > + LargeObjOfferStream<
            Self::HashID,
            Ctx,
            PushOfferError = Self::PushOfferError
        > + LargeObjStream<
            Ctx,
            Frags = Self::Frags,
            PushFragError = Self::PushFragError
        >;
}

pub trait SharedLargeObjPushModeTypes<Ctx> {
    type Parties: IntoIterator<Item = Self::PartyID>;
    type Frags: Frags;
    type BatchID: Clone + Debug + Display;
    type PartyID: Clone + Debug + Display + From<usize> + Eq + Hash + Ord;
    type HashID: Clone + Debug + Display + Hash + HashID + Eq;
    type Hash: Clone + HashAlgo<HashID = Self::HashID>;
    type AddErrorCompletable: ScopedError;
    type AddError: RecoverableError<Completable = Self::AddErrorCompletable>;
    type CancelBatchErrorCompletable: ScopedError;
    type CancelBatchError: RecoverableError<
        Completable = Self::CancelBatchErrorCompletable
    >;
    type FinishBatchErrorCompletable: ScopedError;
    type FinishBatchError: RecoverableError<
        Completable = Self::FinishBatchErrorCompletable
    >;
    type StartBatchErrorCompletable: ScopedError;
    type StartBatchError: RecoverableError<
        Completable = Self::StartBatchErrorCompletable
    >;
    type PushFragErrorCompletable: ScopedError;
    type PushFragError: RecoverableError<
        Completable = Self::PushFragErrorCompletable
    >;
    type PushOfferErrorCompletable: ScopedError;
    type PushOfferError: RecoverableError<
        Completable = Self::PushOfferErrorCompletable
    >;
    type IndefParties: IntoIterator<Item = Self::PartyID>;
    type PartiesError: Debug + Display + ScopedError;
    type StreamFlags: Default;
    type Stream: PushStreamReportBatchError<
            <Self::FinishBatchError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::StartBatchError as RecoverableError>::Permanent
        > + PushStreamReportBatchError<
            <Self::AddError as RecoverableError>::Permanent,
            Self::BatchID
        > + PushStreamReportError<
            <Self::PushFragError as RecoverableError>::Permanent
        > + PushStreamReportError<
            <Self::PushOfferError as RecoverableError>::Permanent
        > + LargeObjStream<
            Ctx,
            Frags = Self::Frags,
            Parties = Self::Parties,
            PushFragError = Self::PushFragError
        > + LargeObjOfferStream<
            Self::HashID,
            Ctx,
            PushOfferError = Self::PushOfferError
        > + PushStreamShared<
            Ctx,
            IndefParties = Self::IndefParties,
            StartBatchError = Self::StartBatchError
        > + PushStreamPartyID<PartyID = Self::PartyID>
        + PushStreamParties<PartiesError = Self::PartiesError>
        + PushStreamAdd<LargeObjMsg<Self::HashID>, Ctx, AddError = Self::AddError>
        + PushStream<
            Ctx,
            BatchID = Self::BatchID,
            StreamFlags = Self::StreamFlags,
            CancelBatchError = Self::CancelBatchError,
            FinishBatchError = Self::FinishBatchError
        >;
}

pub trait PollThreadTypes<Ctx>
where
    Ctx: Send {
    type Addr: Clone + Debug + Display + Eq + Hash;
    type ChannelParam: Clone + Debug + Display + Eq + Hash;
    type ChannelID: Clone + Debug + Display + Eq + Hash;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: Display + Send;
    type AuthNChan: Clone
        + AuthNed<Self::SessionPrin>
        + PullStream<Self::Wrapper, PullError = Self::PullError>;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError: ScopedError;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<
            Completable = Self::RefreshCompletableError,
            Permanent = Self::RefreshPermanentError
        >;
    type StreamConfig: Send;
    type StreamCreateError: Debug + Display;
    type Stream: StreamRefresh<
            PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        > + StreamReporter<
            Self::SessionPrin,
            StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
            Self::AuthNChan
        > + for<'a> CreateWithParam<
            &'a mut PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>,
            Config = Self::StreamConfig,
            CreateError = Self::StreamCreateError
        >;
    type InMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin>;
    type Wrapper;
    type Msgs: Send;
    type ChansConfig: Send;
    type ChansCreateError: Debug + Display;
    type ChanShutdownRetry: RetryWhen;
    type ChanShutdownError: Debug + Display;
    type Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = Self::ChansConfig,
            CreateError = Self::ChansCreateError
        > + Channels<
            Ctx,
            Addr = Self::Addr,
            Param = Self::ChannelParam,
            Stream = Self::AuthNChan,
            ChannelID = Self::ChannelID
        > + ChannelsListen<Ctx>
        + ChannelsShutdown<
            Ctx,
            ShutdownStreamError = Self::ChanShutdownError,
            ShutdownStreamRetry = Self::ChanShutdownRetry
        >;
    type MsgAuthConfig: Send;
    type MsgAuth: Create<
            Config = Self::MsgAuthConfig,
            CreateError = Self::MsgAuthCreateError
        > + MsgAuthN<
            Self::InMsg,
            Self::Wrapper,
            Prin = Self::MsgPrin,
            AuthNMsg = Self::AuthNMsg,
            SessionPrin = Self::SessionPrin,
            Error = Self::MsgAuthError
        >;
    type MsgAuthCreateError: Debug + Display;
    type MsgAuthError: Debug + Display + ScopedError;
    type RecvError: Debug + Display + ScopedError;
    type Recv: AuthNMsgRecv<Self::MsgPrin, Self::AuthNMsg, RecvError = Self::RecvError>
        + Send;
    type ModeConfig: Send;
    type ModeCreateError: Debug + Display;
    type Mode: PushMode<
            Self::Stream,
            Self::Msgs,
            PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>
        > + for<'a> CreateWithParam<
            &'a Self::Stream,
            Config = Self::ModeConfig,
            CreateError = Self::ModeCreateError
        >;
}

pub trait DispatchInboundTypes {
    type InMsg;
    type Wrapper;
    type OutMsg;
    type SessionPrin: Clone + Display + Eq + Hash;
    type MsgPrin: Clone + Display + Eq + Hash;
    type AuthNMsg: AuthNed<Self::MsgPrin>;
    type MsgAuthError: Debug + Display + ScopedError;
    type MsgAuth: Clone
        + MsgAuthN<
            Self::InMsg,
            Self::Wrapper,
            Prin = Self::MsgPrin,
            SessionPrin = Self::SessionPrin,
            AuthNMsg = Self::AuthNMsg,
            Error = Self::MsgAuthError
        >;
}

pub trait DispatchEntryTypes<Ctx>: DispatchInboundTypes {
    type Addr: Clone + Debug + Display + Eq + Hash;
    type ChannelParam: Clone
        + Debug
        + Display
        + Eq
        + Hash
        + ChannelParam<Self::Addr>;
    type ChannelID: Clone + Debug + Display + Eq + Hash;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError: ScopedError;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<
            Completable = Self::RefreshCompletableError,
            Permanent = Self::RefreshPermanentError
        >;
    type ReportStreamError: Debug + Display + ScopedError;
    type Stream: StreamRefresh<
            DispatchThreadCtx<Self::Chans, Ctx>,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        > + StreamReporter<
            Self::SessionPrin,
            StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
            Self::AuthNChan,
            ReportStreamError = Self::ReportStreamError
        >;
    type Msgs: Send;
    type RecvError: Debug + Display + ScopedError;
    type Recv: AuthNMsgRecv<Self::MsgPrin, Self::AuthNMsg, RecvError = Self::RecvError>;
    type AuthNChan: Clone
        + AuthNed<Self::SessionPrin>
        + PullStream<Self::Wrapper, PullError = Self::PullError>;
    type ModeConfig: Clone + Send;
    type ModeCreateError: Debug + Display;
    type Mode: PushMode<Self::Stream, Self::Msgs, DispatchThreadCtx<Self::Chans, Ctx>>
        + for<'a> CreateWithParam<
            &'a Self::Stream,
            Config = Self::ModeConfig,
            CreateError = Self::ModeCreateError
        >;
    type ChansConfig: Send;
    type ChansCreateError: Debug + Display;
    type ChanShutdownRetry: RetryWhen;
    type ChanShutdownError: Debug + Display;
    type Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = Self::ChansConfig,
            CreateError = Self::ChansCreateError
        > + Channels<
            Ctx,
            Addr = Self::Addr,
            Param = Self::ChannelParam,
            Stream = Self::AuthNChan,
            ChannelID = Self::ChannelID
        > + ChannelsListen<Ctx>
        + ChannelsShutdown<
            Ctx,
            ShutdownStreamError = Self::ChanShutdownError,
            ShutdownStreamRetry = Self::ChanShutdownRetry
        >;
}

pub trait DispatchTypes<Ctx>: DispatchEntryTypes<Ctx> + Sized {
    type DispatchError: Debug + Display + ScopedError;
    type Disp: Dispatch<
            Self,
            DispatchThreadCtx<Self::Chans, Ctx>,
            Msgs = Self::Msgs,
            Recv = Self::Recv,
            PushStream = Self::Stream,
            DispatchError = Self::DispatchError
        > + Send;
}

#[derive(Debug)]
pub struct DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    ctx: PhantomData<Ctx>,
    hash: PhantomData<H>
}

#[derive(Debug)]
pub struct SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    ctx: PhantomData<Ctx>,
    hash: PhantomData<H>
}

#[derive(Debug)]
pub struct MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    party: PhantomData<Party>,
    ctx: PhantomData<Ctx>,
    hash: PhantomData<H>
}

#[derive(Debug)]
pub struct DatagramSelectorPollTypes<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send {
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    msgauth: PhantomData<MsgAuth>,
    wrapper: PhantomData<Wrapper>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>
}

#[derive(Debug)]
pub struct LargeObjSelectorPollTypes<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash {
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    types: PhantomData<Types>,
    ctx: PhantomData<Ctx>
}

#[derive(Debug)]
pub struct DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                 Epochs, Chans, ChansConfig,
                                 ChansCreateError, Resolve,
                                 Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    msgauth: PhantomData<MsgAuth>,
    wrapper: PhantomData<Wrapper>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>,
}

#[derive(Debug)]
pub struct LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                 ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    types: PhantomData<Types>,
    ctx: PhantomData<Ctx>,
}

#[derive(Debug)]
pub struct DatagramMulticastPollTypes<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send {
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    msgauth: PhantomData<MsgAuth>,
    wrapper: PhantomData<Wrapper>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>
}

#[derive(Debug)]
pub struct LargeObjMulticastPollTypes<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash {
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    types: PhantomData<Types>,
    ctx: PhantomData<Ctx>
}

impl<Epochs, H, Resolve, Ctx> Clone
    for DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        DispatchLargeObjPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Clone
    for SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        SelectorLargeObjPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<Party, Epochs, H, Resolve, Ctx> Clone
    for MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        MulticastLargeObjPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            party: self.party,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Clone
    for DatagramSelectorPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send
{
    #[inline]
    fn clone(&self) -> Self {
        DatagramSelectorPollTypes {
            msgauth: self.msgauth,
            wrapper: self.wrapper,
            resolve: self.resolve,
            outmsg: self.outmsg,
            epochs: self.epochs,
            inmsg: self.inmsg,
            chans: self.chans,
            msgs: self.msgs,
            recv: self.recv,
            ctx: self.ctx
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Clone
    for LargeObjSelectorPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        LargeObjSelectorPollTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            outmsg: self.outmsg,
            inmsg: self.inmsg,
            chans: self.chans,
            types: self.types,
            ctx: self.ctx
        }
    }
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> Clone
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
    #[inline]
    fn clone(&self) -> Self {
        DatagramDispatchTypes {
            msgauth: self.msgauth,
            wrapper: self.wrapper,
            resolve: self.resolve,
            outmsg: self.outmsg,
            epochs: self.epochs,
            inmsg: self.inmsg,
            chans: self.chans,
            msgs: self.msgs,
            recv: self.recv,
            ctx: self.ctx
        }
    }
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Types, Ctx> Clone
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
    #[inline]
    fn clone(&self) -> Self {
        LargeObjDispatchTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            outmsg: self.outmsg,
            inmsg: self.inmsg,
            chans: self.chans,
            types: self.types,
            ctx: self.ctx
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Clone
    for DatagramMulticastPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send
{
    #[inline]
    fn clone(&self) -> Self {
        DatagramMulticastPollTypes {
            msgauth: self.msgauth,
            wrapper: self.wrapper,
            resolve: self.resolve,
            outmsg: self.outmsg,
            epochs: self.epochs,
            inmsg: self.inmsg,
            chans: self.chans,
            msgs: self.msgs,
            recv: self.recv,
            ctx: self.ctx
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Clone
    for LargeObjMulticastPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        LargeObjMulticastPollTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            outmsg: self.outmsg,
            inmsg: self.inmsg,
            chans: self.chans,
            types: self.types,
            ctx: self.ctx
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Default
    for DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn default() -> Self {
        DispatchLargeObjPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Default
    for SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn default() -> Self {
        SelectorLargeObjPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<Party, Epochs, H, Resolve, Ctx> Default
    for MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    #[inline]
    fn default() -> Self {
        MulticastLargeObjPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            party: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Default
    for DatagramSelectorPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send
{
    #[inline]
    fn default() -> Self {
        DatagramSelectorPollTypes {
            msgauth: PhantomData,
            wrapper: PhantomData,
            resolve: PhantomData,
            outmsg: PhantomData,
            epochs: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            msgs: PhantomData,
            recv: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Default
    for LargeObjSelectorPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn default() -> Self {
        LargeObjSelectorPollTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            outmsg: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            types: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> Default
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
    #[inline]
    fn default() -> Self {
        DatagramDispatchTypes {
            msgauth: PhantomData,
            wrapper: PhantomData,
            resolve: PhantomData,
            outmsg: PhantomData,
            epochs: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            msgs: PhantomData,
            recv: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Types, Ctx> Default
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
    #[inline]
    fn default() -> Self {
        LargeObjDispatchTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            outmsg: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            types: PhantomData,
            ctx: PhantomData,
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Default
    for DatagramMulticastPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send
{
    #[inline]
    fn default() -> Self {
        DatagramMulticastPollTypes {
            msgauth: PhantomData,
            wrapper: PhantomData,
            resolve: PhantomData,
            outmsg: PhantomData,
            epochs: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            msgs: PhantomData,
            recv: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Default
    for LargeObjMulticastPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn default() -> Self {
        LargeObjMulticastPollTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            outmsg: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            types: PhantomData,
            ctx: PhantomData
        }
    }
}

unsafe impl<Epochs, H, Resolve, Ctx> Send
    for DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<Epochs, H, Resolve, Ctx> Send
    for SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<Party, Epochs, H, Resolve, Ctx> Send
    for MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Send
    for DatagramSelectorPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Send
    for LargeObjSelectorPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

unsafe impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> Send
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv< MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
}

unsafe impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
            ChansCreateError, Resolve, Types, Ctx> Send
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Send
    for DatagramMulticastPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Send
    for LargeObjMulticastPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

unsafe impl<Epochs, H, Resolve, Ctx> Sync
    for DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<Epochs, H, Resolve, Ctx> Sync
    for SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<Party, Epochs, H, Resolve, Ctx> Sync
    for MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Sync
    for DatagramSelectorPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Sync
    for LargeObjSelectorPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

unsafe impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> Sync
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg >,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
}

unsafe impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
            ChansCreateError, Resolve, Types, Ctx> Sync
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> Sync
    for DatagramMulticastPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send
{
}

unsafe impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> Sync
    for LargeObjMulticastPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

impl<Epochs, H, Resolve, Ctx> PrivateLargeObjPushModeTypes<Ctx>
    for DispatchLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type Frags = <Ctx::Stream as LargeObjStream<Ctx>>::Frags;
    type BatchID = StreamSelectorBatch<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::BatchID
    >;
    type HashID = H::HashID;
    type Hash = H;
    type AddErrorCompletable = <
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
            as RecoverableError
    >::Completable;
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
    >;
    type CancelBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError as RecoverableError
    >::Completable;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type FinishBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError as RecoverableError
    >::Completable;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type StartBatchErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<
                StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type PushFragErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushFragError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<
                StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjStream<Ctx>>::PushFragError,
            Epochs::Item
        >
    >;
    type PushOfferErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushOfferError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            DispatchSelectorSelectError<
                StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;
    type Stream = DispatchSelector<
        Epochs,
        StreamID<Ctx::Addr, Ctx::ChannelID, Ctx::Param>,
        Ctx::Stream,
        Ctx
    >;
}

impl<Epochs, H, Resolve, Ctx> PrivateLargeObjPushModeTypes<Ctx>
    for SelectorLargeObjPushModeTypes<Epochs, H, Resolve, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type Frags = <Ctx::Stream as LargeObjStream<Ctx>>::Frags;
    type BatchID = StreamSelectorBatch<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::BatchID
    >;
    type HashID = H::HashID;
    type Hash = H;
    type AddErrorCompletable = <
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
            as RecoverableError
    >::Completable;
    type AddError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
    >;
    type CancelBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError as RecoverableError
    >::Completable;
    type CancelBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
    >;
    type FinishBatchErrorCompletable = <
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError as RecoverableError
    >::Completable;
    type FinishBatchError = SelectorBatchError<
        Epochs::Item,
        <Ctx::Stream as PushStream<Ctx>>::FinishBatchError
    >;
    type StartBatchErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type StartBatchError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError,
            Epochs::Item
        >
    >;
    type PushFragErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushFragError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjStream<Ctx>>::PushFragError,
            Epochs::Item
        >
    >;
    type PushOfferErrorCompletable = SelectorBatchSelectError<
        Infallible,
        (),
        <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
         as RecoverableError>::Completable,
        Epochs::Item
    >;
    type PushOfferError = SelectorBatchError<
        Epochs::Item,
        SelectorBatchSelectError<
            StreamSelectorSelectRefreshError<
                Resolve::AddrsError,
                Ctx::ParamsError,
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;
    type Stream = StreamSelector<Epochs, Resolve, Ctx>;
}

impl<Party, Epochs, H, Resolve, Ctx> SharedLargeObjPushModeTypes<Ctx>
    for MulticastLargeObjPushModeTypes<Party, Epochs, H, Resolve, Ctx>
where
    Party: Clone + Debug + Display + Eq + Hash,
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    H: Clone + HashAlgo,
    H::HashID: Clone + Debug + Display + Hash + HashID + Eq,
    Ctx: Channels<()>,
    Ctx::OutNegoParam: Clone + Eq + Hash,
    Ctx::Stream: Clone + LargeObjStream<Ctx>
        + LargeObjOfferStream<H::HashID, Ctx>
        + LargeObjStream<Ctx>
        + PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStream<Ctx>>::BatchID: Display,
    <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
     as RecoverableError>::Permanent: ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Ctx::Addr>,
    Resolve::Origin: Clone + Display + Eq + Hash
{
    type Parties = Vec<MulticastStreamIdx>;
    type Frags = StreamMulticasterFrags<<Ctx::Stream as LargeObjStream<Ctx>>::Frags>;
    type BatchID = CompoundBatchID;
    type PartyID = MulticastStreamIdx;
    type HashID = H::HashID;
    type Hash = H;
    type AddErrorCompletable = ErrorSet<
        MulticastStreamIdx,
        RetryResult<
            (),
            <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddRetry
        >,
        <<Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
         as RecoverableError>::Completable
    >;
    type AddError = CompoundBatchError<
        MulticastStreamIdx,
        RetryResult<
            (),
            <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddRetry
        >,
        SelectorBatchError<
            Epochs::Item,
            <Ctx::Stream as PushStreamAdd<LargeObjMsg<H::HashID>, Ctx>>::AddError
        >
    >;
    type CancelBatchErrorCompletable = ErrorSet<
        MulticastStreamIdx,
        RetryResult<
            (),
            <Ctx::Stream as PushStream<Ctx>>::CancelBatchRetry
        >,
        <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
         as RecoverableError>::Completable
    >;
    type CancelBatchError = CompoundBatchError<
        MulticastStreamIdx,
        RetryResult<
            (),
            <Ctx::Stream as PushStream<Ctx>>::CancelBatchRetry
        >,
        SelectorBatchError<
            Epochs::Item,
            <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
        >
    >;
    type FinishBatchErrorCompletable = ErrorSet<
        MulticastStreamIdx,
        RetryResult<
            (),
            StreamFinishCancel<
                <Ctx::Stream as PushStream<Ctx>>::FinishBatchRetry,
                <Ctx::Stream as PushStream<Ctx>>::CancelBatchRetry
            >
        >,
        StreamFinishCancel<
            <<Ctx::Stream as PushStream<Ctx>>::FinishBatchError
             as RecoverableError>::Completable,
            <<Ctx::Stream as PushStream<Ctx>>::CancelBatchError
             as RecoverableError>::Completable,
        >
    >;
    type FinishBatchError = CompoundBatchError<
        MulticastStreamIdx,
        RetryResult<
            (),
            StreamFinishCancel<
                <Ctx::Stream as PushStream<Ctx>>::FinishBatchRetry,
                <Ctx::Stream as PushStream<Ctx>>::CancelBatchRetry
            >
        >,
        StreamFinishCancel<
            SelectorBatchError<
                Epochs::Item,
                <Ctx::Stream as PushStream<Ctx>>::FinishBatchError
            >,
            SelectorBatchError<
                Epochs::Item,
                <Ctx::Stream as PushStream<Ctx>>::CancelBatchError
            >
        >
    >;
    type StartBatchErrorCompletable = StreamMulticasterStartError<
        ErrorSet<
            MulticastStreamIdx,
            RetryIndefResult<
                (),
                SelectorBatchSelectError<
                    Instant,
                    (),
                    <Ctx::Stream as PushStreamPrivate<Ctx>>::SelectRetry,
                    Epochs::Item
                >
            >,
            SelectorBatchSelectError<
                Infallible,
                (),
                <<Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError
                 as RecoverableError>::Completable,
                Epochs::Item
            >
        >,
        ErrorSet<
            MulticastStreamIdx,
            RetryResult<
                StreamSelectorBatch<
                    Epochs::Item,
                    <Ctx::Stream as PushStream<Ctx>>::BatchID
                >,
                <Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry
            >,
            <<Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
             as RecoverableError>::Completable
        >,
        StreamMulticasterSelections<
            SelectorSelections<
                DenseItemID<Epochs::Item>,
                <Ctx::Stream as PushStreamPrivate<Ctx>>::Selections
            >
        >,
        <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches
    >;
    type StartBatchError = StreamMulticasterStartError<
        SelectionsError<
            ErrorSet<
                MulticastStreamIdx,
                RetryIndefResult<
                    (),
                    SelectorBatchSelectError<
                        Instant,
                        (),
                        <Ctx::Stream as PushStreamPrivate<Ctx>>::SelectRetry,
                        Epochs::Item
                    >
                >,
                SelectorBatchError<
                    Epochs::Item,
                    SelectorBatchSelectError<
                        StreamSelectorSelectRefreshError<
                            Resolve::AddrsError,
                            Ctx::ParamsError,
                            StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>,
                                     Ctx::Param>
                        >,
                        (),
                        <Ctx::Stream as PushStreamPrivate<Ctx>>::SelectError,
                        Epochs::Item
                    >
                >
            >,
            usize
        >,
        SelectionsError<
            ErrorSet<
                MulticastStreamIdx,
                RetryResult<
                    StreamSelectorBatch<
                        Epochs::Item,
                        <Ctx::Stream as PushStream<Ctx>>::BatchID
                    >,
                    <Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchRetry
                >,
                SelectionsError<
                    SelectorBatchError<
                        Epochs::Item,
                        <Ctx::Stream as PushStreamPrivate<Ctx>>::CreateBatchError
                    >,
                    ()
                >
            >,
            usize
        >,
        StreamMulticasterSelections<
            SelectorSelections<
                DenseItemID<Epochs::Item>,
                <Ctx::Stream as PushStreamPrivate<Ctx>>::Selections
            >
        >,
        <Ctx::Stream as PushStreamPrivate<Ctx>>::StartBatchStreamBatches
    >;
    type PushFragErrorCompletable = ErrorSet<
        MulticastStreamIdx,
        RetryIndefResult<
            Option<Instant>,
            SelectorBatchSelectError<
                Instant,
                (),
                <Ctx::Stream as LargeObjStream<Ctx>>::PushFragRetry,
                Epochs::Item
            >
        >,
        SelectorBatchSelectError<
            Infallible,
            (),
            <<Ctx::Stream as LargeObjStream<Ctx>>::PushFragError
             as RecoverableError>::Completable,
            Epochs::Item
        >
    >;
    type PushFragError = ErrorSet<
        MulticastStreamIdx,
        RetryIndefResult<
            Option<Instant>,
            SelectorBatchSelectError<
                Instant,
                (),
                <Ctx::Stream as LargeObjStream<Ctx>>::PushFragRetry,
                Epochs::Item
            >
        >,
        SelectorBatchError<
            Epochs::Item,
            SelectorBatchSelectError<
                StreamSelectorSelectRefreshError<
                    Resolve::AddrsError,
                    Ctx::ParamsError,
                    StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>,
                             Ctx::Param>
                >,
                (),
                <Ctx::Stream as LargeObjStream<Ctx>>::PushFragError,
                Epochs::Item
            >
        >
    >;
    type PushOfferErrorCompletable = ErrorSet<
        MulticastStreamIdx,
        RetryIndefResult<
            Option<Instant>,
            SelectorBatchSelectError<
                Instant,
                (),
                <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferRetry,
                Epochs::Item
            >
        >,
        SelectorBatchSelectError<
            Infallible,
            (),
            <<Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError
             as RecoverableError>::Completable,
            Epochs::Item
        >
    >;
    type PushOfferError = ErrorSet<
        MulticastStreamIdx,
        RetryIndefResult<
            Option<Instant>,
            SelectorBatchSelectError<
                Instant,
                (),
                <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferRetry,
                Epochs::Item
            >
        >,
        SelectorBatchError<
            Epochs::Item,
            SelectorBatchSelectError<
                StreamSelectorSelectRefreshError<
                    Resolve::AddrsError,
                    Ctx::ParamsError,
                    StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
                >,
                (),
                <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError,
                Epochs::Item
            >
        >
    >;
    type IndefParties = Vec<MulticastStreamIdx>;
    type PartiesError = Infallible;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;
    type Stream = StreamMulticaster<
        Party,
        StreamSelector<Epochs, Resolve, Ctx>,
        Ctx
    >;
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> PollThreadTypes<Ctx>
    for DatagramSelectorPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send + Sync,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: PrivateMsgs<OutMsg> + Send
{
    type Addr = Chans::Addr;
    type AuthNChan = Chans::Stream;
    type AuthNMsg = MsgAuth::AuthNMsg;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChannelID = Chans::ChannelID;
    type ChannelParam = Chans::Param;
    type Chans = Chans;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type InMsg = InMsg;
    type Mode = PrivateDatagramPushMode<
        OutMsg,
        StreamSelector<
            Epochs,
            Resolve,
            PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
    type ModeConfig = PrivateDatagramModeConfig;
    type ModeCreateError = Infallible;
    type MsgAuth = MsgAuth;
    type MsgAuthConfig = MsgAuth::Config;
    type MsgAuthCreateError = MsgAuth::CreateError;
    type MsgAuthError = MsgAuth::Error;
    type MsgPrin = MsgAuth::Prin;
    type Msgs = Msgs;
    type PullError = <Chans::Stream as PullStream<Wrapper>>::PullError;
    type Recv = Recv;
    type RecvError = Recv::RecvError;
    type RefreshCompletableError = Infallible;
    type RefreshError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshPermanentError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshRetry = Instant;
    type SessionPrin = MsgAuth::SessionPrin;
    type Stream = StreamSelector<
        Epochs,
        Resolve,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
    type StreamConfig = PartyConfig<
        Resolve::Config,
        Epochs::Config,
        String,
        Resolve::OriginConfig
    >;
    type StreamCreateError =
        StreamSelectorCreateError<Resolve::CreateError, Epochs::CreateError>;
    type Wrapper = Wrapper;
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> PollThreadTypes<Ctx>
    for LargeObjSelectorPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    type Addr = Chans::Addr;
    type AuthNChan = Chans::Stream;
    type AuthNMsg = Types::AuthNMsg;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChannelID = Chans::ChannelID;
    type ChannelParam = Chans::Param;
    type Chans = Chans;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type InMsg = InMsg;
    type Mode = PrivateLargeObjPushMode<
        SelectorLargeObjPushModeTypes<
            Epochs,
            Types::Hash,
            Resolve,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
    type ModeConfig = PrivateLargeObjModeConfig;
    type ModeCreateError = Infallible;
    type MsgAuth = Types::MsgAuthN;
    type MsgAuthConfig = <Types::MsgAuthN as Create>::Config;
    type MsgAuthCreateError = <Types::MsgAuthN as Create>::CreateError;
    type MsgAuthError = Types::AuthNError;
    type MsgPrin = <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin;
    type Msgs = LargeObjProto<
        InMsg,
        OutMsg,
        (),
        <Chans::Stream as LargeObjStream<
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        >>::Frags,
        Types
    >;
    type PullError = <Chans::Stream as PullStream<Types::Wrapper>>::PullError;
    type Recv = Types::Recv;
    type RecvError =
        <Types::Recv as AuthNMsgRecv<Types::Prin, Types::AuthNMsg>>::RecvError;
    type RefreshCompletableError = Infallible;
    type RefreshError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshPermanentError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshRetry = Instant;
    type SessionPrin = Types::SessionPrin;
    type Stream = StreamSelector<
        Epochs,
        Resolve,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
    type StreamConfig = PartyConfig<
        Resolve::Config,
        Epochs::Config,
        String,
        Resolve::OriginConfig
    >;
    type StreamCreateError =
        StreamSelectorCreateError<Resolve::CreateError, Epochs::CreateError>;
    type Wrapper = Types::Wrapper;
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> DispatchInboundTypes
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
    type InMsg = InMsg;
    type Wrapper = Wrapper;
    type OutMsg = OutMsg;
    type SessionPrin = MsgAuth::SessionPrin;
    type MsgPrin = MsgAuth::Prin;
    type AuthNMsg = MsgAuth::AuthNMsg;
    type MsgAuthError = MsgAuth::Error;
    type MsgAuth = MsgAuth;
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Msgs, Recv, Ctx> DispatchEntryTypes<Ctx>
    for DatagramDispatchTypes<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans,
                              ChansConfig, ChansCreateError, Resolve,
                              Msgs, Recv, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Default + Debug + Display + Eq,
    OutMsg: Clone,
    MsgAuth: Clone + Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Prin: Eq + Hash,
    ChansCreateError: Debug + Display,
    ChansConfig: Send,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Wrapper>
    + StreamReporter<
        MsgAuth::SessionPrin,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
    >,
    Chans::Param: Clone + Debug + Display + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<OutMsg, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg>,
    Msgs: PrivateMsgs<OutMsg> + Send,
{
    type Addr = Chans::Addr;
    type ChannelParam = Chans::Param;
    type ChannelID = Chans::ChannelID;
    type PullError = <Chans::Stream as PullStream<Wrapper>>::PullError;
    type RefreshRetry = Instant;
    type RefreshCompletableError = Infallible;
    type RefreshPermanentError = Infallible;
    type RefreshError = Infallible;
    type ReportStreamError = WithMutexPoison<DispatchSelectorRefreshError<
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>
    >>;
    type Stream = DispatchSelector<
        Epochs,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
        DispatchThreadCtx<Chans, Ctx>
    >;
    type Msgs = Msgs;
    type Recv = Recv;
    type RecvError = Recv::RecvError;
    type AuthNChan = Chans::Stream;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type Chans = Chans;
    type ModeConfig = PrivateDatagramModeConfig;
    type ModeCreateError = Infallible;
    type Mode = PrivateDatagramPushMode<
        OutMsg,
        DispatchSelector<
            Epochs,
            StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
            Chans::Stream,
            DispatchThreadCtx<Chans, Ctx>
        >,
        DispatchThreadCtx<Chans, Ctx>
    >;
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Types, Ctx> DispatchInboundTypes
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
    type InMsg = InMsg;
    type OutMsg = OutMsg;
    type Wrapper = Types::Wrapper;
    type SessionPrin = Types::SessionPrin;
    type MsgPrin = <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin;
    type AuthNMsg = Types::AuthNMsg;
    type MsgAuth = Types::MsgAuthN;
    type MsgAuthError = Types::AuthNError;
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Resolve, Types, Ctx> DispatchEntryTypes<Ctx>
    for LargeObjDispatchTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                              ChansCreateError, Resolve, Types, Ctx>
where
    Epochs: Create + Iterator,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                   CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin>
    + PushStream<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    DispatchThreadCtx<Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          DispatchThreadCtx<Chans, Ctx>>
    + PullStream<Types::Wrapper>,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::BatchID: Display,
    <<Chans::Stream as PushStreamPrivate<DispatchThreadCtx<Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, DispatchThreadCtx<Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<DispatchThreadCtx<Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, DispatchThreadCtx<Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<DispatchThreadCtx<Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::MsgAuthN: Clone + Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Prin: Eq + Hash,
    Types::IDs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::Recv: Send,
{
    type Addr = Chans::Addr;
    type ChannelParam = Chans::Param;
    type ChannelID = Chans::ChannelID;
    type AuthNChan = Chans::Stream;
    type RefreshRetry = Instant;
    type RefreshCompletableError = Infallible;
    type RefreshPermanentError = Infallible;
    type RefreshError = Infallible;
    type ReportStreamError = WithMutexPoison<DispatchSelectorRefreshError<
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>
    >>;
    type Stream = DispatchSelector<
        Epochs,
        StreamID<Chans::Addr, Chans::ChannelID, Chans::Param>,
        Chans::Stream,
        DispatchThreadCtx<Chans, Ctx>
    >;
    type Msgs = LargeObjProto<
        InMsg,
        OutMsg,
        (),
        <Chans::Stream as LargeObjStream<DispatchThreadCtx<Chans, Ctx>>>::Frags,
        Types
    >;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type Chans = Chans;
    type PullError = <Chans::Stream as PullStream<Types::Wrapper>>::PullError;
    type Recv = Types::Recv;
    type RecvError =
        <Types::Recv as AuthNMsgRecv<Types::Prin, Types::AuthNMsg>>::RecvError;
    type ModeConfig = PrivateLargeObjModeConfig;
    type ModeCreateError = Infallible;
    type Mode = PrivateLargeObjPushMode<
        DispatchLargeObjPushModeTypes<
            Epochs, Types::Hash, Resolve,
            DispatchThreadCtx<Chans, Ctx>
        >,
        DispatchThreadCtx<Chans, Ctx>
    >;
}

impl<
    InMsg,
    OutMsg,
    Wrapper,
    MsgAuth,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Msgs,
    Recv,
    Ctx
> PollThreadTypes<Ctx>
    for DatagramMulticastPollTypes<
        InMsg,
        OutMsg,
        Wrapper,
        MsgAuth,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Msgs,
        Recv,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    OutMsg: Clone,
    MsgAuth: Create + MsgAuthN<InMsg, Wrapper>,
    MsgAuth::Config: Send,
    MsgAuth::SessionPrin: Send,
    MsgAuth::Prin: Eq + Hash,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<MsgAuth::SessionPrin>
        + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
        + PullStream<Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        OutMsg,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default + Send,
    Recv: AuthNMsgRecv<MsgAuth::Prin, MsgAuth::AuthNMsg> + Send,
    Msgs: SharedMsgs<MulticastStreamIdx, OutMsg> + Send
{
    type Addr = Chans::Addr;
    type AuthNChan = Chans::Stream;
    type AuthNMsg = MsgAuth::AuthNMsg;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChannelID = Chans::ChannelID;
    type ChannelParam = Chans::Param;
    type Chans = Chans;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type InMsg = InMsg;
    type Mode = SharedDatagramPushMode<
        OutMsg,
        StreamMulticaster<
            MsgAuth::SessionPrin,
            StreamSelector<
                Epochs,
                Resolve,
                PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
            >,
            PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
    type ModeConfig = SharedDatagramModeConfig;
    type ModeCreateError = Infallible;
    type MsgAuth = MsgAuth;
    type MsgAuthConfig = MsgAuth::Config;
    type MsgAuthCreateError = MsgAuth::CreateError;
    type MsgAuthError = MsgAuth::Error;
    type MsgPrin = MsgAuth::Prin;
    type Msgs = Msgs;
    type PullError = <Chans::Stream as PullStream<Wrapper>>::PullError;
    type Recv = Recv;
    type RecvError = Recv::RecvError;
    type RefreshCompletableError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        Infallible
    >;
    type RefreshError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>
    >;
    type RefreshPermanentError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>
    >;
    type RefreshRetry = Vec<RetryResult<Option<Instant>, Instant>>;
    type SessionPrin = MsgAuth::SessionPrin;
    type Stream = StreamMulticaster<
        MsgAuth::SessionPrin,
        StreamSelector<
            Epochs,
            Resolve,
            PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
    type StreamConfig = StreamMulticasterConfig<
        MsgAuth::SessionPrin,
        PartyConfig<
            Resolve::Config,
            Epochs::Config,
            String,
            Resolve::OriginConfig
        >
    >;
    type StreamCreateError =
        StreamSelectorCreateError<Resolve::CreateError, Epochs::CreateError>;
    type Wrapper = Wrapper;
}

impl<
    InMsg,
    OutMsg,
    Epochs,
    Chans,
    ChansConfig,
    ChansCreateError,
    Resolve,
    Types,
    Ctx
> PollThreadTypes<Ctx>
    for LargeObjMulticastPollTypes<
        InMsg,
        OutMsg,
        Epochs,
        Chans,
        ChansConfig,
        ChansCreateError,
        Resolve,
        Types,
        Ctx
    >
where
    Ctx: Send,
    Epochs: Create + Iterator,
    Epochs::Config: Default + Send,
    Epochs::Item: Clone + Debug + Display + Default + Eq,
    InMsg: Send,
    OutMsg: Clone + Send,
    ChansConfig: Send,
    ChansCreateError: Debug + Display,
    Chans: for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = ChansConfig,
            CreateError = ChansCreateError
        > + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>,
    Chans::Stream: Clone
        + AuthNed<Types::SessionPrin>
        + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
        + PushStreamAdd<
            LargeObjMsg<Types::HashID>,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + LargeObjOfferStream<
            Types::HashID,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        > + PullStream<Types::Wrapper>
        + PushStreamParties,
    Chans::OutNegoParam: Clone + Eq + Hash,
    Chans::OutNegoParam: Clone + Eq + Hash,
    <Chans::Stream as PushStreamPartyID>::PartyID: Debug + Display,
    <Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::BatchID: Display,
    <Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags: Send,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::StartBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamAdd<
        LargeObjMsg<Types::HashID>,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::AddError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::FinishBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CancelBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjStream<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushFragError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as LargeObjOfferStream<
        Types::HashID,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::PushOfferError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::SelectError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Completable: ScopedError,
    <<Chans::Stream as PushStreamPrivate<
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >>::CreateBatchError as RecoverableError>::Permanent:
        ErrorReportInfo<DenseItemID<Epochs::Item>>,
    Resolve: Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>,
    Resolve::Config: Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send,
    Resolve::OriginConfig:
        Clone + OutboundEndpointConfig<Chans::OutNegoParam> + Send,
    Resolve::Config: Clone + Default,
    Types: LargeObjProtoTypes<InMsg, OutMsg> + Send,
    Types::Hash: Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::Msgs: Send,
    Types::IDs: Send,
    Types::MsgAuthN: Create + Send,
    Types::AuthNError: ScopedError,
    Types::SessionPrin: Send + Sync,
    Types::Recv: Send,
    <Types::MsgAuthN as Create>::Config: Send,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    type Addr = Chans::Addr;
    type AuthNChan = Chans::Stream;
    type AuthNMsg = Types::AuthNMsg;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChannelID = Chans::ChannelID;
    type ChannelParam = Chans::Param;
    type Chans = Chans;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type InMsg = InMsg;
    type Mode = SharedLargeObjPushMode<
        MulticastLargeObjPushModeTypes<
            Types::SessionPrin,
            Epochs,
            Types::Hash,
            Resolve,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
    type ModeConfig = SharedLargeObjModeConfig;
    type ModeCreateError = Infallible;
    type MsgAuth = Types::MsgAuthN;
    type MsgAuthConfig = <Types::MsgAuthN as Create>::Config;
    type MsgAuthCreateError = <Types::MsgAuthN as Create>::CreateError;
    type MsgAuthError = Types::AuthNError;
    type MsgPrin = <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin;
    type Msgs = LargeObjProto<
        InMsg,
        OutMsg,
        MulticastStreamIdx,
        StreamMulticasterFrags<
            <Chans::Stream as LargeObjStream<
                PollThreadCtx<Types::SessionPrin, Chans, Ctx>
            >>::Frags
        >,
        Types
    >;
    type PullError = <Chans::Stream as PullStream<Types::Wrapper>>::PullError;
    type Recv = Types::Recv;
    type RecvError =
        <Types::Recv as AuthNMsgRecv<Types::Prin, Types::AuthNMsg>>::RecvError;
    type RefreshCompletableError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        Infallible
    >;
    type RefreshError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>
    >;
    type RefreshPermanentError = ErrorSet<
        MulticastStreamIdx,
        RetryResult<Option<Instant>, Instant>,
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>
    >;
    type RefreshRetry = Vec<RetryResult<Option<Instant>, Instant>>;
    type SessionPrin = Types::SessionPrin;
    type Stream = StreamMulticaster<
        Types::SessionPrin,
        StreamSelector<
            Epochs,
            Resolve,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
    type StreamConfig = StreamMulticasterConfig<
        Types::SessionPrin,
        PartyConfig<
            Resolve::Config,
            Epochs::Config,
            String,
            Resolve::OriginConfig
        >
    >;
    type StreamCreateError =
        StreamSelectorCreateError<Resolve::CreateError, Epochs::CreateError>;
    type Wrapper = Types::Wrapper;
}
