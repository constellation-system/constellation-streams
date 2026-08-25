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
use crate::channels::Channels;
use crate::channels::ChannelsListen;
use crate::channels::ChannelsShutdown;
use crate::config::PartyConfig;
use crate::config::PrivateDatagramModeConfig;
use crate::config::PrivateLargeObjModeConfig;
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
use crate::select::StreamSelectorCreateError;
use crate::select::StreamSelectorBatch;
use crate::select::StreamSelectorSelectRefreshError;
use crate::select::ThreadedStreamSelectorError;
use crate::select::dispatch::DispatchSelector;
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
use crate::stream::StreamRefresh;
use crate::stream::StreamReporter;
use crate::stream::StreamID;
use crate::threads::PushMode;
use crate::threads::poll::PollThreadCtx;
use crate::threads::private::PrivateDatagramPushMode;
use crate::threads::private::PrivateLargeObjPushMode;

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
    Ctx: 'static + Send {
    type Addr: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type ChannelParam: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type ChannelID: 'static + Clone + Debug + Display + Eq + Hash + Send;
    type MsgPrin: Clone + Display + Eq + Hash;
    type SessionPrin: 'static + Display + Send;
    type AuthNChan: 'static
        + Clone
        + AuthNed<Self::SessionPrin, Self::Chan>
        + Send;
    type Chan: Clone + PullStream<Self::Wrapper, PullError = Self::PullError>;
    type PullError: Debug + Display + ScopedError;
    type RefreshRetry: RetryWhen;
    type RefreshCompletableError: ScopedError + Send;
    type RefreshPermanentError: Debug + Display + ScopedError;
    type RefreshError: Debug
        + RecoverableError<
            Completable = Self::RefreshCompletableError,
            Permanent = Self::RefreshPermanentError
        >;
    type StreamConfig;
    type StreamCreateError: Debug + Display;
    type Stream: 'static
        + StreamRefresh<
            PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>,
            RefreshRetry = Self::RefreshRetry,
            RefreshError = Self::RefreshError
        >
        + StreamReporter<
            Self::SessionPrin,
            StreamID<Self::Addr, Self::ChannelID, Self::ChannelParam>,
            Self::AuthNChan
        >
        + for<'a> CreateWithParam<
            &'a mut PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>,
            Config = Self::StreamConfig,
            CreateError = Self::StreamCreateError
        >
        + Send;
    type InMsg;
    type AuthNMsg: AuthNed<Self::MsgPrin, Self::InMsg>;
    type Wrapper;
    type Msgs: 'static + Send;
    type ChansConfig;
    type ChansCreateError: Debug + Display;
    type ChanShutdownRetry: RetryWhen + Send;
    type ChanShutdownError: Debug + Display;
    type Chans: 'static
        + for<'a> CreateWithParam<
            &'a mut Ctx,
            Config = Self::ChansConfig,
            CreateError = Self::ChansCreateError
        >
        + Channels<
            Ctx,
            Addr = Self::Addr,
            Param = Self::ChannelParam,
            Stream = Self::AuthNChan,
            ChannelID = Self::ChannelID
        >
        + ChannelsListen<Ctx>
        + ChannelsShutdown<
            Ctx,
            ShutdownStreamError = Self::ChanShutdownError,
            ShutdownStreamRetry = Self::ChanShutdownRetry
        >
        + Send;
    type MsgAuthConfig;
    type MsgAuth: 'static
        + Create<
            Config = Self::MsgAuthConfig,
            CreateError = Self::MsgAuthCreateError
        >
        + MsgAuthN<
            Self::InMsg,
            Self::Wrapper,
            Prin = Self::MsgPrin,
            AuthNMsg = Self::AuthNMsg,
            SessionPrin = Self::SessionPrin,
            Error = Self::MsgAuthError
        >
        + Send;
    type MsgAuthCreateError: Debug + Display;
    type MsgAuthError: Debug + Display + ScopedError;
    type RecvError: Debug + Display + ScopedError;
    type Recv: 'static
        + AuthNMsgRecv<
            Self::MsgPrin,
            Self::InMsg,
            Self::AuthNMsg,
            RecvError = Self::RecvError
        >
        + Send;
    type ModeConfig;
    type ModeCreateError: Debug + Display;
    type Mode: 'static
        + PushMode<
            Self::Stream,
            Self::Msgs,
            PollThreadCtx<Self::SessionPrin, Self::Chans, Ctx>
        >
        + for<'a> CreateWithParam<
            &'a Self::Stream,
            Config = Self::ModeConfig,
            CreateError = Self::ModeCreateError
        >
        + Send;
}

pub struct DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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

pub struct SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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

pub struct MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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
    ctx: PhantomData<Ctx>,
    hash: PhantomData<H>
}

pub struct DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                     Epochs, Chans, ChansConfig,
                                     ChansCreateError, Chan, Resolve,
                                     Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    msgauth: PhantomData<MsgAuth>,
    chan: PhantomData<Chan>,
    wrapper: PhantomData<Wrapper>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    msgs: PhantomData<Msgs>,
    recv: PhantomData<Recv>,
    ctx: PhantomData<Ctx>,
}

pub struct LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                     ChansCreateError, Chan, Resolve,
                                     Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    resolve: PhantomData<Resolve>,
    epochs: PhantomData<Epochs>,
    chan: PhantomData<Chan>,
    outmsg: PhantomData<OutMsg>,
    inmsg: PhantomData<InMsg>,
    chans: PhantomData<Chans>,
    types: PhantomData<Types>,
    ctx: PhantomData<Ctx>,
}

impl<Epochs, H, Resolve, Ctx> Clone
    for DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        DispatchLargeObjDatagramPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Clone
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        SelectorLargeObjDatagramPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Clone
    for MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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
        MulticastLargeObjDatagramPushModeTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            hash: self.hash,
            ctx: self.ctx
        }
    }
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Msgs, Recv, Ctx> Clone
    for DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                        Epochs, Chans, ChansConfig,
                                        ChansCreateError, Chan, Resolve,
                                        Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
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
            chan: self.chan,
            msgs: self.msgs,
            recv: self.recv,
            ctx: self.ctx
        }
    }
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Types, Ctx> Clone
    for LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve,
                                  Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn clone(&self) -> Self {
        LargeObjSelectorPollTypes {
            resolve: self.resolve,
            epochs: self.epochs,
            chan: self.chan,
            outmsg: self.outmsg,
            inmsg: self.inmsg,
            chans: self.chans,
            types: self.types,
            ctx: self.ctx,
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Default
    for DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        DispatchLargeObjDatagramPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Default
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        SelectorLargeObjDatagramPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<Epochs, H, Resolve, Ctx> Default
    for MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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
        MulticastLargeObjDatagramPushModeTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            hash: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Msgs, Recv, Ctx> Default
    for DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                        Epochs, Chans, ChansConfig,
                                        ChansCreateError, Chan, Resolve,
                                        Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
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
            chan: PhantomData,
            msgs: PhantomData,
            recv: PhantomData,
            ctx: PhantomData
        }
    }
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Types, Ctx> Default
    for LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve,
                                  Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    #[inline]
    fn default() -> Self {
        LargeObjSelectorPollTypes {
            resolve: PhantomData,
            epochs: PhantomData,
            chan: PhantomData,
            outmsg: PhantomData,
            inmsg: PhantomData,
            chans: PhantomData,
            types: PhantomData,
            ctx: PhantomData,
        }
    }
}

unsafe impl<Epochs, H, Resolve, Ctx> Send
    for DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
    for MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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

unsafe impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
            ChansCreateError, Chan, Resolve, Msgs, Recv, Ctx> Send
    for DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                        Epochs, Chans, ChansConfig,
                                        ChansCreateError, Chan, Resolve,
                                        Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
{
}

unsafe impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
            ChansCreateError, Chan, Resolve, Types, Ctx> Send
    for LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve,
                                  Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

unsafe impl<Epochs, H, Resolve, Ctx> Sync
    for DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
    for MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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

unsafe impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
            ChansCreateError, Chan, Resolve, Msgs, Recv, Ctx> Sync
    for DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                        Epochs, Chans, ChansConfig,
                                        ChansCreateError, Chan, Resolve,
                                        Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
{
}

unsafe impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
            ChansCreateError, Chan, Resolve, Types, Ctx> Sync
    for LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve,
                                  Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
}

impl<Epochs, H, Resolve, Ctx> PrivateLargeObjPushModeTypes<Ctx>
    for DispatchLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
            DispatchSelectorSelectError<
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
            DispatchSelectorSelectError<
                StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>
            >,
            (),
            <Ctx::Stream as LargeObjOfferStream<H::HashID, Ctx>>::PushOfferError,
            Epochs::Item
        >
    >;
    type StreamFlags = <Ctx::Stream as PushStream<Ctx>>::StreamFlags;
    type Stream = DispatchSelector<
        Epochs,
        StreamID<Ctx::Addr, ConnChannelID<Ctx::ChannelID>, Ctx::Param>,
        Ctx::Stream,
        Ctx
    >;
}

impl<Epochs, H, Resolve, Ctx> PrivateLargeObjPushModeTypes<Ctx>
    for SelectorLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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

impl<Epochs, H, Resolve, Ctx> SharedLargeObjPushModeTypes<Ctx>
    for MulticastLargeObjDatagramPushModeTypes<Epochs, H, Resolve, Ctx>
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
        + PushStreamParties
        + PushStreamPrivate<Ctx>
        + PushStream<Ctx>,
    <Ctx::Stream as PushStreamPartyID>::PartyID: Debug + Display,
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
        <Ctx::Stream as PushStreamPartyID>::PartyID,
        StreamSelector<Epochs, Resolve, Ctx>,
        Ctx
    >;
}

impl<InMsg, OutMsg, Wrapper, MsgAuth, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Msgs, Recv, Ctx>
    PollThreadTypes<Ctx>
    for DatagramSelectorPollTypes<InMsg, OutMsg, Wrapper, MsgAuth,
                                  Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve,
                                  Msgs, Recv, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Wrapper> + Send + Sync,
    OutMsg: 'static + Clone + Send,
    MsgAuth: 'static + Create + MsgAuthN<InMsg, Wrapper> + Send,
    MsgAuth::Prin: Eq + Hash,
    MsgAuth::SessionPrin: 'static + Send + Sync,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<MsgAuth::SessionPrin, Chan>
    + PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>
    + PullStream<Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<OutMsg, PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Recv: 'static
        + AuthNMsgRecv<
            MsgAuth::Prin,
            InMsg,
            MsgAuth::AuthNMsg,
        >
        + Send,
    Msgs: 'static + PrivateMsgs<OutMsg> + Send
{
    type Addr = Chans::Addr;
    type ChannelParam = Chans::Param;
    type ChannelID = Chans::ChannelID;
    type MsgPrin = MsgAuth::Prin;
    type SessionPrin = MsgAuth::SessionPrin;
    type AuthNChan = Chans::Stream;
    type Chan = Chan;
    type RefreshRetry = Instant;
    type RefreshCompletableError = Infallible;
    type RefreshPermanentError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshError = ThreadedStreamSelectorError<Resolve::AddrsError,
                                                    Chans::ParamsError>;
    type StreamCreateError = StreamSelectorCreateError<Resolve::CreateError,
                                                       Epochs::CreateError>;
    type StreamConfig = PartyConfig<
        Resolve::Config,
        Epochs::Config,
        String,
        Resolve::OriginConfig
    >;
    type Stream = StreamSelector<
        Epochs,
        Resolve,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
    type InMsg = InMsg;
    type AuthNMsg = MsgAuth::AuthNMsg;
    type Wrapper = Wrapper;
    type Msgs = Msgs;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type Chans = Chans;
    type PullError = Chan::PullError;
    type MsgAuthConfig = MsgAuth::Config;
    type MsgAuth = MsgAuth;
    type MsgAuthCreateError = MsgAuth::CreateError;
    type MsgAuthError = MsgAuth::Error;
    type Recv = Recv;
    type RecvError = Recv::RecvError;
    type ModeConfig = PrivateDatagramModeConfig;
    type ModeCreateError = Infallible;
    type Mode = PrivateDatagramPushMode<
        OutMsg,
        StreamSelector<
            Epochs,
            Resolve,
            PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<MsgAuth::SessionPrin, Chans, Ctx>
    >;
}

impl<InMsg, OutMsg, Epochs, Chans, ChansConfig,
     ChansCreateError, Chan, Resolve, Types, Ctx>
    PollThreadTypes<Ctx>
    for LargeObjSelectorPollTypes<InMsg, OutMsg, Epochs, Chans, ChansConfig,
                                  ChansCreateError, Chan, Resolve, Types, Ctx>
where
    Epochs: 'static + Create + Iterator + Send + Sync,
    Epochs::Config: Default,
    Epochs::Item: Clone + Debug + Display + Default + Eq + Send + Sync,
    Chan: Clone + PullStream<Types::Wrapper> + Send + Sync,
    InMsg: 'static + Send,
    OutMsg: 'static + Clone + Send,
    ChansCreateError: Debug + Display,
    Chans: 'static
        + for<'a> CreateWithParam<&'a mut Ctx, Config = ChansConfig,
                                  CreateError = ChansCreateError>
        + Channels<Ctx>
        + ChannelsListen<Ctx>
        + ChannelsShutdown<Ctx>
        + Send + Sync,
    Chans::Stream: Clone + AuthNed<Types::SessionPrin, Chan>
    + PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PushStreamAdd<LargeObjMsg<Types::HashID>,
                    PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + LargeObjOfferStream<Types::HashID,
                          PollThreadCtx<Types::SessionPrin, Chans, Ctx>>
    + PullStream<Types::Wrapper>
    + Send + Sync,
    Chans::Addr: Send + Sync,
    Chans::Param: Send + Sync,
    Chans::ChannelID: Send + Sync,
    Chans::OutNegoParam: Clone + Eq + Hash + Send,
    Chans::ShutdownStreamRetry: Send,
    Chans::OutNegoParam: Clone + Eq + Hash + Send + Sync,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::BatchID: Display + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StreamFlags: Send,
    <<Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::StartBatchRetry: Send,
    <Chans::Stream as PushStreamPrivate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AbortBatchRetry: Send,
    <<Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStreamAdd<LargeObjMsg<Types::HashID>, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::AddRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::FinishBatchRetry: Send,
    <<Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as PushStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::CancelBatchRetry: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushFragRetry: Send,
    <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags: Send,
    <<Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags as Frags>::Param: Send + Sync,
    <<Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferError
     as RecoverableError>::Completable: ScopedError + Send,
    <Chans::Stream as LargeObjOfferStream<Types::HashID, PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::PushOfferRetry: Send,
    Ctx: 'static + Send + Sync,
    Resolve: 'static + Addrs<Addr = Chans::Addr>
        + AddrsCreate<PollThreadCtx<Types::SessionPrin, Chans, Ctx>> + Send,
    Resolve::Origin: Clone + Debug + Display + Eq + Hash + Send + Sync,
    Resolve::OriginConfig: Clone + OutboundEndpointConfig<Chans::OutNegoParam>,
    Resolve::Config: Clone + Default,
    Types: 'static + LargeObjProtoTypes<InMsg, OutMsg>,
    Types::Hash: 'static + Clone + HashAlgo + Send,
    Types::HashID: Clone + Debug + Display + Hash + HashID + Eq + Send,
    Types::IDs: Send,
    Types::Recv: 'static + Send,
    Types::Msgs: Send,
    Types::Decoder: Send,
    Types::Encoder: Send,
    Types::MsgAuthN: Create + Send,
    Types::SessionPrin: Send + Sync,
    Types::AuthNError: ScopedError,
    <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin: Eq + Hash
{
    type Addr = Chans::Addr;
    type ChannelParam = Chans::Param;
    type ChannelID = Chans::ChannelID;
    type MsgPrin = <Types::MsgAuthN as MsgAuthN<InMsg, Types::Wrapper>>::Prin;
    type SessionPrin = Types::SessionPrin;
    type AuthNChan = Chans::Stream;
    type Chan = Chan;
    type RefreshRetry = Instant;
    type RefreshCompletableError = Infallible;
    type RefreshPermanentError =
        ThreadedStreamSelectorError<Resolve::AddrsError, Chans::ParamsError>;
    type RefreshError = ThreadedStreamSelectorError<Resolve::AddrsError,
                                                    Chans::ParamsError>;
    type StreamCreateError = StreamSelectorCreateError<Resolve::CreateError,
                                                       Epochs::CreateError>;
    type StreamConfig = PartyConfig<
        Resolve::Config,
        Epochs::Config,
        String,
        Resolve::OriginConfig
    >;
    type Stream = StreamSelector<
        Epochs,
        Resolve,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
    type InMsg = InMsg;
    type AuthNMsg = Types::AuthNMsg;
    type Wrapper = Types::Wrapper;
    type Msgs = LargeObjProto<
        InMsg,
        OutMsg,
        (),
        <Chans::Stream as LargeObjStream<PollThreadCtx<Types::SessionPrin, Chans, Ctx>>>::Frags,
        Types
    >;
    type ChansConfig = ChansConfig;
    type ChansCreateError = ChansCreateError;
    type ChanShutdownRetry = Chans::ShutdownStreamRetry;
    type ChanShutdownError = Chans::ShutdownStreamError;
    type Chans = Chans;
    type PullError = Chan::PullError;
    type MsgAuthConfig = <Types::MsgAuthN as Create>::Config;
    type MsgAuth = Types::MsgAuthN;
    type MsgAuthCreateError = <Types::MsgAuthN as Create>::CreateError;
    type MsgAuthError = Types::AuthNError;
    type Recv = Types::Recv;
    type RecvError =
        <Types::Recv
         as AuthNMsgRecv<Types::Prin, InMsg, Types::AuthNMsg>>::RecvError;
    type ModeConfig = PrivateLargeObjModeConfig;
    type ModeCreateError = Infallible;
    type Mode = PrivateLargeObjPushMode<
        SelectorLargeObjDatagramPushModeTypes<
            Epochs, Types::Hash, Resolve,
            PollThreadCtx<Types::SessionPrin, Chans, Ctx>
        >,
        PollThreadCtx<Types::SessionPrin, Chans, Ctx>
    >;
}
