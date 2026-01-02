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

use std::collections::HashMap;
use std::fmt::Display;
use std::io::Error;
use std::sync::Arc;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::ScopedError;
use constellation_common::shutdown::ShutdownFlag;
use log::error;
use log::info;
use mio::Events;
use mio::Poll;
use mio::Registry;
use mio::Waker;

pub struct PollThread {
    notify: Arc<Waker>,
    shutdown: ShutdownFlag,
    poll: Poll,
    nevents: usize
}

impl PollThread {
    /// Get the [Waker] used to signal availability of new messages
    /// to this thread.
    #[inline]
    pub fn notify(&self) -> Arc<Waker> {
        self.notify.clone()
    }

    fn run(mut self) {
        let mut valid = true;
        let mut events = Events::with_capacity(self.nevents);
        let mut next_pending = None;
        let mut next_outbound = None;
        let mut now = Instant::now();

        info!(target: "poll-thread",
              "mio polling thread starting");

        // Loop until told to shut down.
        while {
            let next = next_pending.map_or(next_outbound, |next| {
                next_outbound.map(|when: Instant| when.max(next))
            });

            now = Instant::now();

            valid && self.shutdown.is_live() &&
            // Skip polling if the time has already elapsed.
                next.is_some_and(|next: Instant| next < now) ||
                {
                    let duration = next.map(|next| next - now);

                    self.poll
                        .poll(&mut events, duration)
                        .inspect_err(|err| {
                            error!(target: "poll-thread",
                                   "error polling: {}",
                                   err)
                        })
                        .is_ok()
                }
        } {
            // First push all pending messages.
            if let Some(next) = next_pending &&
                next <= now
            {}

            // Do pulls before pushing new messages.

            // Push new messages.
            if let Some(when) = next_outbound &&
                when <= now
            {}
        }
    }

    pub fn start(self) -> Result<JoinHandle<()>, Error> {
        Builder::new()
            .name(String::from("poll-thread"))
            .spawn(move || self.run())
    }
}
