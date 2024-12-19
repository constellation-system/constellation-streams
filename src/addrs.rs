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

//! Traits for sources of addresses.
//!
//! The [Addrs] trait defined in this module provides an abstraction
//! for obtaining addresses from some set of sources.  The most
//! typical use of this is with a DNS-style resolver, with the names
//! serving as the origins.  `Addrs` provides basic functionality for
//! periodically refreshing the resolution of addresses, obtaining the
//! current set of addresses, and determining when next to refresh.
use std::fmt::Display;
use std::hash::Hash;
use std::time::Instant;

use constellation_common::error::ScopedError;
use constellation_common::retry::RetryResult;

/// Trait for sources of addresses that may need to be periodically
/// refreshed.
///
/// The canonical example of an instance of this trait is a DNS
/// resolver, which has a fixed set of names it resolves, and does so
/// periodically.
pub trait Addrs {
    /// Type of address sources.
    ///
    /// This is the thing from which an address is derived.  In the
    /// DNS example, this is the DNS name.
    type Origin;
    /// Type of addresses.
    type Addr: Clone + Display + Eq + Hash;
    /// Type of iterators for the results.
    ///
    /// This indicates the address, the source from which it
    /// originates, and when it was last cached.
    type AddrsIter: Iterator<Item = (Self::Addr, Self::Origin, Instant)>;
    /// Type of errors that can occur when resolving addresses.
    type AddrsError: Display + ScopedError;

    /// Get the earliest future time that addresses will be refreshed.
    ///
    /// All calls to [addrs](Addrs::addrs) between now and the
    /// return value of this function will be redundant.
    ///
    /// Note: this refers only to the refresh time policy for
    /// *this* instance; other instances may have a separate policy,
    /// and may refresh addresses before the return result of this
    /// function.
    fn refresh_when(&self) -> Option<Instant>;

    /// Check whether this source needs to be refreshed.
    #[inline]
    fn needs_refresh(&self) -> bool {
        self.refresh_when()
            .map_or(false, |when| when < Instant::now())
    }

    /// Get the set of addresses, their sources, and when to refresh next.
    ///
    /// The next refresh time will be `None` if this does not need to
    /// be refreshed ever.
    fn addrs(
        &mut self
    ) -> Result<RetryResult<(Self::AddrsIter, Option<Instant>)>, Self::AddrsError>;
}

/// Sub-trait of [Addrs] that can be created from a configuration object.
pub trait AddrsCreate<Ctx, Origins>: Sized + Addrs {
    /// Type of configurations from which this is created.
    type Config;
    /// Type of errors that can occur during creation.
    type CreateError: Display;

    /// Create an instance of this `Addrs` from its configuration.
    fn create(
        ctx: &mut Ctx,
        config: Self::Config,
        origin: Origins
    ) -> Result<Self, Self::CreateError>;
}
