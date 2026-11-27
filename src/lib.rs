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

//! Streams API for the Constellation distributed systems platform.
//!
//! The streams API provides a mid-level abstraction that is loosely
//! inspired by the reactive streams concept.  In Constellation, this
//! acts as an abstraction layer on top of low-level channels.  Where
//! channels represent communication of raw bytes with a particular
//! *endpoint*, a stream is meant to represent communication of
//! *objects* with a particular *counterparty*.
//!
//! # Basic Streams
//!
//! The base-level stream used by most applications is
//! [`DatagramCodecStream`](crate::codec::DatagramCodecStream).  This
//! creates a stream out of an underlying channel.
//!
//! The [`Channels`](crate::channels::Channels) trait is an
//! abstraction for objects that create channels proactively in this
//! way.  The [`PullStream`](crate::stream::PullStream) trait
//! represents objects that allow incoming sessions to be obtained,
//! and implementations will generally create
//! [`DatagramCodecStream`](crate::codec::DatagramCodecStream)s.  This
//! trait acts as the primary interface to a lower-level API.
//!
//! ## Push vs. Pull
//!
//! The streams API provides a "push" side, which facilitates sending
//! of messages, and a "pull" side which facilitates receiving them.
//!
//! ## Push API
//!
//! The "push" API handles batching as a first-class concept, with
//! sending of single messages being a derived form.  The
//! [`PushStream`](crate::stream::PushStream) trait is the top-level
//! trait for this API, which handles completing batches.  Key
//! subtraits are [`PushStreamAdd`](crate::stream::PushStreamAdd),
//! which handles adding messages to a batch, and
//! [`PushStreamPrivate`](crate::stream::PushStreamPrivate)/[`PushStreamShared`](crate::stream::PushStreamShared),
//! which handle creating batches.  It also provides a number of
//! useful combinators for building push streams:
//!
//! - [`StreamSelector`](crate::select::StreamSelector): Selects from among
//!   several options for communicating with a given counterparty, and maintains
//!   a success/failure history for informing the selection.
//!
//!   - [`DispatchSelector`](crate::select::dispatch::DispatchSelector):
//!     Similar to [`StreamSelector`](crate::select::StreamSelector),
//!     but does not request underlying streams from a
//!     [`Channels`](crate::channels::Channels) instance; rather, it
//!     maintains a set that have been provided and selects amongst
//!     them.  This is used for a "dispatch"-style pattern.
//!
//! - [`StreamMulticaster`](crate::multicast::StreamMulticaster): Synthetic
//!   multicasting combinator, manages communications with multiple parties as a
//!   single stream.
//!
//! - [`SharedPrivateChannelStream`](crate::channels::SharedPrivateChannelStream):
//!   Allows shared (true multicast) and private (unicast) streams to
//!   be combined by a higher-level combinator.  This in turn allows
//!   [`StreamMulticaster`](crate::multicast::StreamMulticaster) and
//!   [`StreamSelector`](crate::select::StreamSelector) to correctly
//!   manage a mix of shared and private streams, combining the
//!   communications of parties who select the same shared stream.
//!
//! The "push" API also returns
//! [`PullStream`](crate::stream::PullStream)s that were created by
//! calls to certain functions that request streams.
//!
//! ### Batching
//!
//! The "push" API is designed around batching as the principal
//! abstraction.  The [`PushStream`](crate::stream::PushStream) trait
//! hierarchy provides functions for creating batches, adding messages
//! to them, and then finishing and sending batches.  The API is also
//! designed in a way that does not incur additional overhead for
//! streams that send individual messages as opposed to batching
//! (which is the case for
//! [`DatagramCodecStream`](crate::codec::DatagramCodecStream)).
//!
//! A simple API for pushing individual messages is provided as a
//! *derived* form; however, applications should generally use the
//! batching API where possible.
//!
//! ### Mixed Unicast and Multicast Streams
//!
//! The "push" API is designed to correctly implement synthetic
//! multicasting, using a mix of true multicast and private unicast
//! streams.  A nesting of
//! [`StreamMulticaster`](crate::multicast::StreamMulticaster),
//! [`StreamSelector`](crate::select::StreamSelector), and
//! [`SharedPrivateChannelStream`](crate::channels::SharedPrivateChannelStream)
//! permits a configuration to be specified wherein multiple
//! counterparties exist, each of which has a set of possible streams
//! that can be used to contact them, each of which may be a private
//! unicast or true multicast stream.
//!
//! The "push" API manages such a case, and is able to select a mix of
//! true multicast and private unicast streams in order to fulfill a
//! request at the top-level
//! [`StreamMulticaster`](crate::multicast::StreamMulticaster).  If
//! the same true multicast channel is selected independently by
//! multiple [`StreamSelector`](crate::select::StreamSelector)s, then
//! only one message will be generated for that stream.
//!
//! The API does *not* attempt to coordinate selection of channels
//! amongst different counterparties in a
//! [`StreamMulticaster`](crate::multicast::StreamMulticaster), as
//! this would permit one party to potentially carry out a
//! denial-of-service against others.
//!
//! ## Pull API
//!
//! The "pull" API consists of objects that manage incoming new
//! sessions, as well as incoming messages.  Unlike the "push" API,
//! there is only one stream trait:
//! [`PullStream`](crate::stream::PullStream), and applications
//! typically work directly with
//! [`DatagramCodecStream`](crate::codec::DatagramCodecStream)s or a
//! similar low-level stream type.
//!
//! [`PullStream`](crate::stream::PullStream)s can be obtained from a
//! [`Channels`](crate::channels::Channels) instance via the
//! [`listen`](crate::channels::ChannelsListen::listen) function.
//! They are also returned from some functions in the push API, which
//! request new low-level streams.
//!
//! The API also manages message-level authentication on incoming
//! messages.  Messages delivered to the upper layer are then
//! identified by a *counterparty*, not an address.
//!
//! # Large Object Protocol
//!
//! The [`LargeObjProto`](crate::large_obj::LargeObjProto) type
//! implements a protocol that manages fragmentation and delivery of
//! arbitrarily large messages over channels that have some small MTU.
//! It can be used in conjunction with the "push" API to seamlessly
//! manage fragmentation of objects and delivery over multiple
//! different possible channels.
//!
//! [`LargeObjProto`](crate::large_obj::LargeObjProto) accomplishes
//! this through sub-traits of
//! [`PushStream`](crate::stream::PushStream) which manage sending of
//! data fragments.
//! [`LargeObjProto`](crate::large_obj::LargeObjProto) itself manages
//! the set of data fragments that need to be sent, as well as
//! selection and retransmission of individual fragments.
//!
//! # Push Modes
//!
//! The [`PushMode`](crate::threads::PushMode) trait provides a
//! simpler top-level interface for the "push" API.  Implementations
//! of this trait manage the details of using the
//! [`PushStream`](crate::stream::PushStream) trait hierarchy,
//! permitting applications to abstract over many possible
//! arrangements of streams with a single trait API.  This also
//! manages the complexity of using a batching-based API.
//!
//! Four implementations of [`PushMode`](crate::threads::PushMode) are
//! provided:
//! [`PrivateDatagramPushMode`](crate::threads::private::PrivateDatagramPushMode),
//! [`PrivateLargeObjPushMode`](crate::threads::private::PrivateLargeObjPushMode),
//! [`SharedDatagramPushMode`](crate::threads::shared::SharedDatagramPushMode),
//! and
//! [`SharedLargeObjPushMode`](crate::threads::shared::SharedLargeObjPushMode).
//! These form a "quadrant" on two axes:
//!
//! - **Private vs. Shared**: The private variants use a
//!   [`StreamSelector`](crate::select::StreamSelector) at the top
//!   level to manage communications with a single party.  The shared
//!   variants use a
//!   [`StreamMulticaster`](crate::multicast::StreamMulticaster) at
//!   the top level to manage communications with multiple parties.
//!
//! - **Datagram vs Large Object**: The datagram variants send
//!   messages as individual datagrams.  The large object variants use
//!   [`LargeObjProto`](crate::large_obj::LargeObjProto) to send
//!   larger messages.
//!
//! # Manager Threads
//!
//! Two [`mio`]-based thread implementations are provided that manage
//! the "push" and "pull" sides of the streams API:
//!
//! - [`poll`](crate::threads::poll): Manages a fixed stream for
//!   communicating with a specific counterparty or set of
//!   counterparties.  This thread implementation is parameterized on
//!   [`PushMode`](crate::threads::PushMode), permitting use with any
//!   of the implementations thereof.  Connection attempts by unknown
//!   parties will be rejected.`
//!
//! - [`dispatch`](crate::threads::dispatch): Manages a dynamic set of
//!   counterparties that each have individual sessions that are
//!   created upon first contact.  A
//!   [`DispatchSelector`](crate::select::dispatch::DispatchSelector)
//!   stream will be created for each session, and subsequent streams
//!   created for the same party will be added to the
//!   [`DispatchSelector`](crate::select::dispatch::DispatchSelector).
#![allow(clippy::redundant_field_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

pub mod addrs;
pub mod channels;
pub mod codec;
pub mod config;
pub mod error;
pub mod frags;
pub mod large_obj;
pub mod multicast;
pub mod select;
pub mod state_machine;
pub mod stream;
pub mod threads;
pub mod xciap;

#[allow(clippy::all)]
#[rustfmt::skip]
mod generated;

#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
use log::LevelFilter;

#[cfg(test)]
static INIT: Once = Once::new();

#[cfg(test)]
fn init() {
    INIT.call_once(|| {
        env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .init()
    })
}
