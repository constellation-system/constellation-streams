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

//! Implementation of a [History] instance suitable for unreliable
//! channel multiplexing.
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use constellation_common::sched::History;

use crate::config::FarSchedulerConfig;

/// A logistic curve with the integer values precomputed.
struct LogisticCurve {
    vals: Vec<f32>
}

#[derive(Clone)]
struct SuccessRecord {
    /// Precomputed exp(-rate * time).
    time_decay: f32,
    /// Precomputed 1 + (score / nretries)
    score: f32,
    /// Time when the success was recorded.
    when: Instant
}

#[derive(Clone)]
struct SuccessRecords {
    records: VecDeque<SuccessRecord>
}

#[derive(Clone)]
struct FailRecord {
    /// Precomputed exp(-rate * time).
    time_decay: f32,
    when: Instant
}

#[derive(Clone)]
struct FailRecords {
    records: VecDeque<FailRecord>
}

pub(crate) struct FarHistoryConfig {
    retry_curve: LogisticCurve,
    retry_max_count: usize,
    success_score: f32,
    success_max_count: usize,
    success_decay_rate: f32,
    success_max_decay_time: Duration,
    success_cancel_fails: usize,
    fail_score: f32,
    fail_max_count: usize,
    fail_decay_rate: f32,
    fail_max_decay_time: Duration
}

#[derive(Clone)]
pub(crate) struct FarHistory {
    successes: SuccessRecords,
    failures: FailRecords,
    nretries: usize,
    cached: Option<f32>
}

fn time_decay(
    rate: f32,
    time: Duration
) -> f32 {
    let time = -rate * time.as_secs_f32();

    time.exp()
}

impl LogisticCurve {
    /// Precompute all the values for the logistic curve.
    fn create(
        steepness: f32,
        nentries: usize
    ) -> Self {
        let mut vals = Vec::with_capacity(nentries);
        let halfway = nentries as f32 / 2.0;

        // Clamp the first to 0 and last to 1.

        for i in 1..nentries - 1 {
            let x = -steepness * (i as f32 - halfway);

            vals.push(1.0 / (1.0 + x.exp()))
        }

        LogisticCurve { vals: vals }
    }

    /// Get the value of the logistic curve at the coordinate `idx`.
    #[inline]
    fn get(
        &self,
        idx: usize
    ) -> f32 {
        if idx == 0 {
            0.0
        } else if idx > self.vals.len() {
            1.0
        } else {
            self.vals[idx]
        }
    }
}

impl SuccessRecords {
    #[inline]
    fn with_capacity(max: usize) -> Self {
        SuccessRecords {
            records: VecDeque::with_capacity(max)
        }
    }

    /// Add a success record.
    fn add(
        &mut self,
        success_score: f32,
        nretries: usize,
        rate: f32,
        max_time: Duration,
        max: usize
    ) {
        // Pop one if we need to make space.
        if self.records.len() >= max {
            self.records.pop_front();
        }

        let score = (success_score / nretries as f32) + 1.0;
        let now = Instant::now();
        let decay = match self.records.back() {
            Some(SuccessRecord { when: latest, .. }) if *latest < now => {
                let interval = max_time.min(now - *latest);

                time_decay(rate, interval)
            }
            // No entry, so no time decay.
            _ => 1.0
        };

        self.records.push_back(SuccessRecord {
            time_decay: decay,
            score: score,
            when: now
        })
    }

    fn score(
        &self,
        rate: f32,
        max_time: Duration,
        curr: Instant
    ) -> (f32, Instant) {
        match self.records.back() {
            Some(SuccessRecord { when: latest, .. }) => {
                let mut iter = self.records.iter();

                // Pop the first one, because the procedure is different.
                match iter.next() {
                    Some(SuccessRecord { score, .. }) => {
                        let mut accum = *score;

                        for SuccessRecord {
                            time_decay, score, ..
                        } in iter
                        {
                            accum = ((accum - 1.0) * *time_decay) + 1.0;
                            accum *= *score;
                        }

                        if curr > *latest {
                            let interval = max_time.min(curr - *latest);

                            (score * time_decay(rate, interval), *latest)
                        } else {
                            (accum, *latest)
                        }
                    }
                    // This shouldn't happen, but we can handle it.
                    None => (1.0, *latest)
                }
            }
            // Empty records produces a goodness of 1.0
            None => (1.0, curr)
        }
    }
}

impl FailRecords {
    #[inline]
    fn with_capacity(max: usize) -> Self {
        FailRecords {
            records: VecDeque::with_capacity(max)
        }
    }

    /// Add a failure record.
    fn add(
        &mut self,
        rate: f32,
        max: usize,
        max_time: Duration
    ) {
        // Pop one if we need to make space.
        if self.records.len() >= max {
            self.records.pop_front();
        }

        let now = Instant::now();
        let decay = match self.records.back() {
            Some(FailRecord { when: latest, .. }) if *latest < now => {
                let interval = max_time.min(now - *latest);

                time_decay(rate, interval)
            }
            // No entry, so no time decay.
            _ => 1.0
        };

        self.records.push_back(FailRecord {
            time_decay: decay,
            when: now
        });
    }

    /// Delete the `n` most recent
    fn remove(
        &mut self,
        n: usize
    ) {
        let n = n.min(self.records.len());

        for _ in 0..n {
            self.records.pop_back();
        }
    }

    /// Compute the badness score.
    fn score(
        &self,
        fail_score: f32,
        rate: f32,
        max_time: Duration,
        curr: Instant
    ) -> (f32, Instant) {
        match self.records.back() {
            Some(FailRecord { when: latest, .. }) => {
                let mut score = 0.0;

                for FailRecord { time_decay, .. } in self.records.iter() {
                    score *= time_decay;
                    score += 1.0;
                    score *= fail_score;
                }

                if curr > *latest {
                    let interval = max_time.min(curr - *latest);
                    let decayed = score * time_decay(rate, interval);

                    (decayed, *latest)
                } else {
                    (score, *latest)
                }
            }
            None => (0.0, curr)
        }
    }
}

impl FarHistory {
    fn compute_score(
        &self,
        config: &FarHistoryConfig
    ) -> f32 {
        let now = Instant::now();
        let (goodness, goodness_when) = self.successes.score(
            config.success_decay_rate,
            config.success_max_decay_time,
            now
        );
        let (badness, badness_when) = self.failures.score(
            config.fail_score,
            config.fail_decay_rate,
            config.fail_max_decay_time,
            now
        );
        let retry_factor = config.retry_curve.get(self.nretries);

        if goodness_when > badness_when {
            (goodness * retry_factor) - badness
        } else {
            goodness - (badness * retry_factor)
        }
    }
}

impl History for FarHistory {
    type Config = FarHistoryConfig;

    #[inline]
    fn new(config: &FarHistoryConfig) -> Self {
        FarHistory {
            successes: SuccessRecords::with_capacity(config.success_max_count),
            failures: FailRecords::with_capacity(config.fail_max_count),
            nretries: 0,
            cached: None
        }
    }

    #[inline]
    fn success(
        &mut self,
        config: &FarHistoryConfig
    ) {
        self.successes.add(
            config.success_score,
            self.nretries,
            config.success_decay_rate,
            config.success_max_decay_time,
            config.success_max_count
        );
        self.failures.remove(config.success_cancel_fails);
        self.nretries = 0;
    }

    #[inline]
    fn failure(
        &mut self,
        config: &FarHistoryConfig
    ) {
        self.failures.add(
            config.fail_decay_rate,
            config.fail_max_count,
            config.fail_max_decay_time
        );
    }

    #[inline]
    fn cache_score(
        &mut self,
        config: &FarHistoryConfig
    ) {
        if self.cached.is_none() {
            self.cached = Some(self.compute_score(config))
        }
    }

    #[inline]
    fn clear_score_cache(&mut self) {
        self.cached = None
    }

    #[inline]
    fn score(
        &self,
        config: &FarHistoryConfig
    ) -> f32 {
        match self.cached {
            Some(score) => score,
            None => self.compute_score(config)
        }
    }

    #[inline]
    fn nretries(&self) -> usize {
        self.nretries
    }

    #[inline]
    fn retry(
        &mut self,
        config: &FarHistoryConfig
    ) {
        self.nretries = config.retry_max_count.min(self.nretries + 1);
    }
}

impl From<&'_ FarSchedulerConfig> for FarHistoryConfig {
    #[inline]
    fn from(val: &FarSchedulerConfig) -> Self {
        FarHistoryConfig {
            retry_curve: LogisticCurve::create(
                val.retry_steepness(),
                val.retry_max_count()
            ),
            retry_max_count: val.retry_max_count(),
            success_score: val.success_score(),
            success_max_count: val.success_max_count(),
            success_decay_rate: val.success_decay_rate(),
            success_max_decay_time: val.success_max_decay_time(),
            success_cancel_fails: val.success_cancel_fails(),
            fail_score: val.fail_score(),
            fail_max_count: val.fail_max_count(),
            fail_decay_rate: val.fail_decay_rate(),
            fail_max_decay_time: val.fail_max_decay_time()
        }
    }
}
