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

//! Streams API for the Constellation distributed systems platform.
//!
//! The streams API provides a mid-level abstraction over the channels
//! abstraction that is loosely inspired by the reactive streams
//! concept.  Where channels represent communication of raw bytes with
//! a particular *endpoint*, a stream is meant to represent
//! communication of *objects* with a particular *counterparty*.
//!
//! # Basic Streams
//!
//! The base-level stream used by most applications is
//! [DatagramCodecStream](crate::codec::DatagramCodecStream).  This
//! creates a stream out of an underlying channel.
//!
//! The [Channels](crate::channels::Channels) trait is an abstraction
//! for objects that create channels proactively in this way.  The
//! [PullStreamListener](crate::stream::PullStreamListener) trait
//! represents objects that allow incoming sessions to be obtained,
//! and implementations will generally create `DatagramCodecStream`s.
//!
//! # Push vs. Pull
//!
//! The streams API provides a "push" side, which facilitates sending
//! of messages, and a "pull" side which facilitates receiving them.
//!
//! ## Push API
//!
//! The "push" API handles batching as a first-class concept, with
//! sending of single messages being a derived form.  It also provides
//! a number of useful combinators for building push streams:
//!
//! * [StreamSelector](crate::select::StreamSelector): Selects from
//!   among several options for communicating with a given
//!   counterparty, and maintains a success/failure history for
//!   informing the selection.
//!
//! * [StreamMulticaster](crate::multicast::StreamMulticaster): Synthetic
//!   multicasting combinator, manages communications with multiple
//!   parties as a single stream.
//!
//! * [SharedPrivateChannelStream](crate::channels::SharedPrivateChannelStream):
//! Allows shared (genuine multicast) and private (unicast) streams to
//! be combined by a higher-level combinator.  This in turn allows
//! `StreamMulticaster` and `StreamSelector` to correctly manage a mix
//! of shared and private streams, combining the communications of
//! parties who select the same shared stream.
//!
//! The push API also interacts with the pull API to install any new
//! streams created when sending messages into the pull side,
//! guaranteeing coherence between the two sides.
//!
//! ## Pull API
//!
//! The "pull" API consists of objects that manage incoming new
//! sessions, as well as incoming messages.  New sessions are
//! converted into streams and installed into both the push and the
//! pull side, guaranteeing coherence between the two sides.
//!
//! The API also manages message-level authentication on incoming
//! messages.  Messages delivered to the upper layer are then
//! identified by a *counterparty*, not an address.
#![feature(let_chains)]
#![feature(generic_const_exprs)]
#![allow(incomplete_features)]
#![allow(clippy::redundant_field_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

pub mod addrs;
pub mod channels;
pub mod codec;
pub mod config;
pub mod error;
pub mod multicast;
pub mod select;
pub mod state_machine;
pub mod stream;
pub mod xciap;

#[allow(clippy::all)]
#[rustfmt::skip]
mod generated;
