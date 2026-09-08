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

//! Configuration objects.
use std::time::Duration;

use constellation_common::retry::Retry;
use serde::Deserialize;
use serde::Serialize;

/// Configuration for how to manage batch slots.
///
/// This is a configuration object describing how to manage the number
/// of batch slots in a [PushStream](crate::stream::PushStream)
/// implementation.
///
/// This object describes the minimium number of slots, and the
/// policies for increasing and decreasing them.
///
/// # Managing Batch Slots
///
/// Batch slots will be increased when all slots are occupied, at
/// which point the number of slots will be multiplied by
/// `extend_ratio` to get the new number.  If the ratio of filled
/// slots drops below `min_fill_ratio`, then the number of slots will
/// be reduced.  This will be done by multiplying the total number of
/// slots by `reduce_ratio` to get the new number.  However, at no
/// point will the number of slots be reduced below `min_batch_slots`.
///
/// # YAML Format
///
/// The YAML format has four fields, each of which has a default value:
///
/// - `min-batch-slots`: The minimum number of batch slots that can ever exist.
///   The default is `16`.
///
/// - `min-fill-ratio`: The ratio of filled batch slots at which the total
///   number will be reduced.  The default is `0.25`.
///
/// - `extend-ratio`: The number that will be multiplied by the current number
///   of batch slots to get the new number when increasing the number of slots.
///   The default is `1.5`.
///
/// - `reduce-ratio`: The number that will be multiplied by the current number
///   of batch slots to get the new number when reducing the number of slots.
///   The default is `0.5`.
///
///
/// ## Examples
///
/// The following is an example of a YAML configuration with all
/// fields represented:
/// ```yaml
/// min-batch-slots: 20
/// min-fill-ratio: 0.2
/// extend-ratio: 1.66
/// reduce-ratio: 0.66
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "far-scheduler-config")]
#[serde(default)]
pub struct BatchSlotsConfig {
    #[serde(default = "BatchSlotsConfig::default_min_batch_slots")]
    min_batch_slots: usize,
    #[serde(default = "BatchSlotsConfig::default_min_fill_ratio")]
    min_fill_ratio: f32,
    #[serde(default = "BatchSlotsConfig::default_extend_ratio")]
    extend_ratio: f32,
    #[serde(default = "BatchSlotsConfig::default_reduce_ratio")]
    reduce_ratio: f32
}

/// Description of a single connection group.
///
/// This is a group of endpoints together with a group of channel
/// names that can potentially be used to reach those endpoints.
/// While mismatched combinations of endpoints and channels (e.g. an
/// IP address and a Unix socket channel) will be detected and
/// removed, configurations should generally try to make sure that
/// endpoints and channels are properly matched.
///
/// # YAML Format
///
/// The YAML format has two fields, both of which are mandatory:
///
/// * `channel-names`: An array of names of channels (defined elsewhere) that
///   can be used to reach the endpoints.
///
/// * `endpoints`: An array of endpoints (often IP addresses and/or Unix socket
///   addresses).
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "connection")]
pub struct ConnectionConfig<Channel, Verify, Endpoint>
where
    Verify: Default {
    /// Names of channels over which to connect.
    channel_names: Vec<Channel>,
    /// Endpoints to which to connect.
    endpoints: Vec<ConnectionEndpoint<Verify, Endpoint>>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "connection-endpoint")]
pub struct ConnectionEndpoint<Verify, Endpoint> {
    addr: Endpoint,
    #[serde(default)]
    verify: Verify
}

/// Parameters for the far-channel multiplexing scheme.
///
/// # Multiplexer Scoring Scheme
///
/// This scheme consists of three functions: "goodness", "badness",
/// and the retry factor.  Goodness and badness are both computed in a
/// similar manner.  Goodness is computed as follows:
///
///  - A maximum of `success_max_count` historical successes are remembered,
///    together with the times at which they occurred, and the number of retries
///    that had previously occurred.
///
///  - The goodness value exponentially decays towards `1.0` over any time
///    interval by the following function: `new = (old - 1.0) *
///    exp(success_decay_rate * time) + 1.0`, where `time` is the length of the
///    time interval, clamped to `success_max_decay_time`.
///
///  - The goodness value starts at `1.0`, just before the occurrence of the
///    first success, so no time decay happens for the first success.
///
///  - The goodness value is subjected to time-decay, then multiplied by `1.0 +
///    (success_score / nretries)`, where `nretries` is the number of retries
///    prior to the success, clamped to `retry_max_count`.
///
///  - The process is repeated forward for each recorded success.
///
///  - At the end, the result is subjected to time-decay for the interval from
///    the occurrence of the last success to the present time.
///
/// The computation for badness is similar, but differs in some ways:
///
///  - A maximum of `fail_max_count` historical failures are remembered,
///    together with the times at which they occurred. Additionally, each
///    success will erase the most recent `success_cancel_fails` failures from
///    the record.
///
///  - The badness value exponentially decays towards `0.0` over any time
///    interval by the following function: `new = old * exp(fail_decay_rate *
///    time)`, where `time` is the length of the time interval, clamped to
///    `fail_max_decay_time`.
///
///  - The badness value starts at `0.0`, just before the occurrence of the
///    first success, so no time decay happens for the first failure.
///
///  - The badness value is subjected to time-decay, then recalculated as `new =
///    (old + 1.0) * fail_score` at each failure.
///
///  - The process is repeated forward for each recorded success.
///
///  - At the end, the result is subjected to time-decay for the interval from
///    the occurrence of the last failure to the present time.
///
/// The computation of the retry factor is done using a logistic
/// growth function as follows: `factor = 1 / (1 +
/// exp(-retry_steepness(nretries - (max_retry_count / 2))))`, where
/// `nretries` is the number of retries clamped to `max_retry_count`.
///
/// The overall score is computed as follows:
///
///  - If the most recent result was a failure, then the badness value is
///    multiplied by the retry factor; otherwise, the goodness value is
///    multiplied by the retry factor.
///
///  - The badness value is then subtracted from the goodness value to get the
///    score.
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "far-scheduler-config")]
#[serde(default)]
pub struct FarSchedulerConfig {
    #[serde(default = "FarSchedulerConfig::default_success_score")]
    success_score: f32,
    #[serde(default = "FarSchedulerConfig::default_success_max_count")]
    success_max_count: usize,
    #[serde(default = "FarSchedulerConfig::default_success_decay_rate")]
    success_decay_rate: f32,
    #[serde(default = "FarSchedulerConfig::default_success_max_decay_time")]
    success_max_decay_time: Duration,
    #[serde(default = "FarSchedulerConfig::default_success_cancel_fails")]
    success_cancel_fails: usize,
    #[serde(default = "FarSchedulerConfig::default_fail_score")]
    fail_score: f32,
    #[serde(default = "FarSchedulerConfig::default_fail_max_count")]
    fail_max_count: usize,
    #[serde(default = "FarSchedulerConfig::default_fail_decay_rate")]
    fail_decay_rate: f32,
    #[serde(default = "FarSchedulerConfig::default_fail_max_decay_time")]
    fail_max_decay_time: Duration,
    #[serde(default = "FarSchedulerConfig::default_retry_steepness")]
    retry_steepness: f32,
    #[serde(default = "FarSchedulerConfig::default_retry_max_count")]
    retry_max_count: usize
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "far-scheduler-config")]
#[serde(default)]
pub struct LargeObjProtoConfig<Encoder, Decoder, IDs>
where
    Decoder: Default,
    Encoder: Default,
    IDs: Default {
    /// Retry for sending fragments.
    #[serde(
        default = "LargeObjProtoConfig::<Encoder, Decoder, IDs>::default_retry"
    )]
    retry: Retry,
    #[serde(default)]
    encoder: Encoder,
    #[serde(default)]
    decoder: Decoder,
    #[serde(default)]
    ids: IDs,
    #[serde(
        default = "LargeObjProtoConfig::<Encoder, Decoder, IDs>::default_tombstone_duration"
    )]
    tombstone_duration: Duration,
    #[serde(default)]
    inbound_size_hint: Option<usize>,
    #[serde(default)]
    outbound_size_hint: Option<usize>
}

/// Configuration of a counterparty for a
/// [StreamSelector](crate::select::StreamSelector).
///
/// This provides information on the different ways to reach a given
/// counterparty, how to resolve names, and how to configure the scheduler.
///
/// # YAML Format
///
/// The YAML format has five fields, only one of which is mandatory:
///
/// * `connections`: An array of [ConnectionConfig] structures, each specifying
///   a combination of endpoints and channels to reach those endpoints. Multiple
///   such structures are allowed, in order to permit users to group endpoints
///   and channels appropriately.
///
/// * `scheduler`: A [FarSchedulerConfig] structure to use for configuring the
///   scheduler.
///
/// * `resolve`: A structure specifying the parameters for the name resolved.
///   The exact format depends on the `Resolver` type parameter.
///
/// * `retry`: Retry configuration used for generating backoff delays.
///
/// * `size-hint`: Size hint used to create the set of streams.
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct PartyConfig<Resolver, Epochs, Channel, Verify, Endpoint>
where
    Resolver: Default,
    Verify: Default,
    Epochs: Default {
    /// Scheduler configuration.
    #[serde(default)]
    scheduler: FarSchedulerConfig,
    /// Resolver to use for resolving endpoint addresses.
    #[serde(default)]
    resolve: Resolver,
    /// Configuration for ID generator for epochs.
    #[serde(default)]
    epochs: Epochs,
    /// Retry configuration.
    #[serde(default)]
    retry: Retry,
    /// Possible ways to form connections to the party.
    connections: Vec<ConnectionConfig<Channel, Verify, Endpoint>>,
    #[serde(default)]
    size_hint: Option<usize>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct MulticastPartyConfig<PartyID, Stream> {
    party: PartyID,
    #[serde(flatten)]
    stream: Stream
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "poll-config")]
pub struct PollThreadConfig<Channels, Mode, Stream, AuthN> {
    channels: Channels,
    authn: AuthN,
    #[serde(flatten)]
    stream: Stream,
    #[serde(flatten)]
    #[serde(default)]
    mode: Mode,
    #[serde(
        default = "PollThreadConfig::<Channels, Mode, Stream, AuthN>::default_nevents"
    )]
    num_events: usize,
    #[serde(default)]
    num_sessions: Option<usize>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "dispatch-config")]
pub struct DispatchThreadConfig<Channels, Mode> {
    channels: Channels,
    #[serde(flatten)]
    #[serde(default)]
    mode: Mode,
    #[serde(
        default = "DispatchThreadConfig::<Channels, Mode>::default_nevents"
    )]
    num_events: usize,
    #[serde(default)]
    num_sessions: Option<usize>,
    #[serde(default)]
    num_dispatched: Option<usize>,
    #[serde(default)]
    num_tokens: Option<usize>
}

#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct PrivateDatagramModeConfig {
    /// Size hint for the pending retries.
    #[serde(default)]
    retries_hint: Option<usize>
}

#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct SharedLargeObjModeConfig {
    /// Size hint for the pending retries.
    #[serde(default)]
    msg_retries_hint: Option<usize>,
    #[serde(default)]
    frag_retries_hint: Option<usize>
}

#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct PrivateLargeObjModeConfig {
    /// Size hint for the pending retries.
    #[serde(default)]
    msg_retries_hint: Option<usize>,
    #[serde(default)]
    frag_retries_hint: Option<usize>
}

#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "party-config")]
pub struct SharedDatagramModeConfig {
    /// Size hint for the pending retries.
    #[serde(default)]
    retries_hint: Option<usize>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "stream-multicaster")]
pub struct StreamMulticasterConfig<PartyID, Stream> {
    #[serde(default)]
    #[serde(flatten)]
    batch_slots: BatchSlotsConfig,
    parties: Vec<MulticastPartyConfig<PartyID, Stream>>
}

#[derive(
    Clone, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[serde(rename = "dispatch-config")]
pub struct DispatchConfig<Epochs>
where
    Epochs: Default {
    /// Scheduler configuration.
    #[serde(default)]
    scheduler: FarSchedulerConfig,
    /// Configuration for ID generator for epochs.
    #[serde(default)]
    epochs: Epochs,
    /// Retry configuration.
    #[serde(default)]
    retry: Retry,
    #[serde(default)]
    size_hint: Option<usize>
}

impl<Channels, Mode> DispatchThreadConfig<Channels, Mode> {
    #[inline]
    pub fn new(
        channels: Channels,
        mode: Mode,
        num_events: usize,
        num_sessions: Option<usize>,
        num_dispatched: Option<usize>,
        num_tokens: Option<usize>
    ) -> Self {
        DispatchThreadConfig {
            channels: channels,
            mode: mode,
            num_events: num_events,
            num_sessions: num_sessions,
            num_dispatched: num_dispatched,
            num_tokens: num_tokens
        }
    }

    #[inline]
    pub fn channels(&self) -> &Channels {
        &self.channels
    }

    #[inline]
    pub fn mode(&self) -> &Mode {
        &self.mode
    }

    #[inline]
    pub fn nevents(&self) -> usize {
        self.num_events
    }

    #[inline]
    pub fn nsession(&self) -> Option<usize> {
        self.num_sessions
    }

    #[inline]
    pub fn ndispatched(&self) -> Option<usize> {
        self.num_dispatched
    }

    #[inline]
    pub fn ntokens(&self) -> Option<usize> {
        self.num_tokens
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        Channels,
        Mode,
        usize,
        Option<usize>,
        Option<usize>,
        Option<usize>
    ) {
        (
            self.channels,
            self.mode,
            self.num_events,
            self.num_sessions,
            self.num_dispatched,
            self.num_tokens
        )
    }

    fn default_nevents() -> usize {
        1024
    }
}

impl<Channels, Mode, Stream, AuthN>
    PollThreadConfig<Channels, Mode, Stream, AuthN>
{
    #[inline]
    pub fn new(
        channels: Channels,
        mode: Mode,
        stream: Stream,
        authn: AuthN,
        nevents: usize,
        nsessions: Option<usize>
    ) -> Self {
        PollThreadConfig {
            channels: channels,
            mode: mode,
            stream: stream,
            authn: authn,
            num_events: nevents,
            num_sessions: nsessions
        }
    }

    #[inline]
    pub fn channels(&self) -> &Channels {
        &self.channels
    }

    #[inline]
    pub fn mode(&self) -> &Mode {
        &self.mode
    }

    #[inline]
    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    #[inline]
    pub fn authn(&self) -> &AuthN {
        &self.authn
    }

    #[inline]
    pub fn nevents(&self) -> usize {
        self.num_events
    }

    #[inline]
    pub fn nsession(&self) -> Option<usize> {
        self.num_sessions
    }

    #[inline]
    pub fn take(self) -> (Channels, Mode, Stream, AuthN, usize, Option<usize>) {
        (
            self.channels,
            self.mode,
            self.stream,
            self.authn,
            self.num_events,
            self.num_sessions
        )
    }

    fn default_nevents() -> usize {
        1024
    }
}

impl PrivateDatagramModeConfig {
    #[inline]
    pub fn new(retries_hint: Option<usize>) -> Self {
        PrivateDatagramModeConfig {
            retries_hint: retries_hint
        }
    }

    #[inline]
    pub fn retries_hint(&self) -> Option<usize> {
        self.retries_hint
    }

    #[inline]
    pub fn take(self) -> Option<usize> {
        self.retries_hint
    }
}

impl PrivateLargeObjModeConfig {
    #[inline]
    pub fn new(
        msg_retries_hint: Option<usize>,
        frag_retries_hint: Option<usize>
    ) -> Self {
        PrivateLargeObjModeConfig {
            frag_retries_hint: frag_retries_hint,
            msg_retries_hint: msg_retries_hint
        }
    }

    #[inline]
    pub fn frag_retries_hint(&self) -> Option<usize> {
        self.frag_retries_hint
    }

    #[inline]
    pub fn msg_retries_hint(&self) -> Option<usize> {
        self.msg_retries_hint
    }

    #[inline]
    pub fn take(self) -> (Option<usize>, Option<usize>) {
        (self.msg_retries_hint, self.frag_retries_hint)
    }
}

impl SharedDatagramModeConfig {
    #[inline]
    pub fn new(retries_hint: Option<usize>) -> Self {
        SharedDatagramModeConfig {
            retries_hint: retries_hint
        }
    }

    #[inline]
    pub fn retries_hint(&self) -> Option<usize> {
        self.retries_hint
    }

    #[inline]
    pub fn take(self) -> Option<usize> {
        self.retries_hint
    }
}

impl SharedLargeObjModeConfig {
    #[inline]
    pub fn new(
        msg_retries_hint: Option<usize>,
        frag_retries_hint: Option<usize>
    ) -> Self {
        SharedLargeObjModeConfig {
            frag_retries_hint: frag_retries_hint,
            msg_retries_hint: msg_retries_hint
        }
    }

    #[inline]
    pub fn frag_retries_hint(&self) -> Option<usize> {
        self.frag_retries_hint
    }

    #[inline]
    pub fn msg_retries_hint(&self) -> Option<usize> {
        self.msg_retries_hint
    }

    #[inline]
    pub fn take(self) -> (Option<usize>, Option<usize>) {
        (self.msg_retries_hint, self.frag_retries_hint)
    }
}

impl BatchSlotsConfig {
    /// Get the minimum number of batch slots.
    #[inline]
    pub fn min_batch_slots(&self) -> usize {
        self.min_batch_slots
    }

    /// Get the fill ratio at which the total number of slots will be
    /// reduced.
    #[inline]
    pub fn min_fill_ratio(&self) -> f32 {
        self.min_fill_ratio
    }

    /// Get the ratio for reducing the number of batch slots.
    ///
    /// The current number of slots will be multiplied by this when
    /// the current fill ratio drops below
    /// [min_fill_ratio](BatchSlotsConfig::min_fill_ratio).
    #[inline]
    pub fn reduce_ratio(&self) -> f32 {
        self.reduce_ratio
    }

    /// Get the ratio for increasing the number of batch slots.
    ///
    /// The current number of batch slots will be multiplied by this
    /// when it is empty.
    #[inline]
    pub fn extend_ratio(&self) -> f32 {
        self.extend_ratio
    }

    #[inline]
    fn default_min_batch_slots() -> usize {
        16
    }

    #[inline]
    fn default_min_fill_ratio() -> f32 {
        0.25
    }

    #[inline]
    fn default_reduce_ratio() -> f32 {
        0.5
    }

    #[inline]
    fn default_extend_ratio() -> f32 {
        1.5
    }
}

impl Default for BatchSlotsConfig {
    #[inline]
    fn default() -> BatchSlotsConfig {
        BatchSlotsConfig {
            reduce_ratio: BatchSlotsConfig::default_reduce_ratio(),
            extend_ratio: BatchSlotsConfig::default_extend_ratio(),
            min_batch_slots: BatchSlotsConfig::default_min_batch_slots(),
            min_fill_ratio: BatchSlotsConfig::default_min_fill_ratio()
        }
    }
}

impl FarSchedulerConfig {
    /// Get the base score factor for successes.
    #[inline]
    pub fn success_score(&self) -> f32 {
        self.success_score
    }

    /// Get the exponential decay rate for success scores.
    #[inline]
    pub fn success_decay_rate(&self) -> f32 {
        self.success_decay_rate
    }

    /// Get the maximum decay time for success scores.
    #[inline]
    pub fn success_max_decay_time(&self) -> Duration {
        self.success_max_decay_time
    }

    /// Get the maximum number of successes that are considered.
    #[inline]
    pub fn success_max_count(&self) -> usize {
        self.success_max_count
    }

    /// Get the number of failures cancelled by each success.
    #[inline]
    pub fn success_cancel_fails(&self) -> usize {
        self.success_cancel_fails
    }

    /// Get the base score factor for failures.
    #[inline]
    pub fn fail_score(&self) -> f32 {
        self.fail_score
    }

    /// Get the exponential decay rate for failure scores.
    #[inline]
    pub fn fail_decay_rate(&self) -> f32 {
        self.fail_decay_rate
    }

    /// Get the maximum decay time for failure scores.
    #[inline]
    pub fn fail_max_decay_time(&self) -> Duration {
        self.fail_max_decay_time
    }

    /// Get the maximum number of failures that are considered.
    #[inline]
    pub fn fail_max_count(&self) -> usize {
        self.fail_max_count
    }

    /// Get the maximum number of retries that are considered.
    #[inline]
    pub fn retry_max_count(&self) -> usize {
        self.retry_max_count
    }

    /// Get the steepness factor of the retry curve.
    #[inline]
    pub fn retry_steepness(&self) -> f32 {
        self.retry_steepness
    }

    #[inline]
    fn default_success_score() -> f32 {
        10.0
    }

    #[inline]
    fn default_success_max_count() -> usize {
        10
    }

    #[inline]
    fn default_success_decay_rate() -> f32 {
        1.0
    }

    #[inline]
    fn default_success_max_decay_time() -> Duration {
        Duration::from_secs(30)
    }

    #[inline]
    fn default_success_cancel_fails() -> usize {
        2
    }

    #[inline]
    fn default_fail_score() -> f32 {
        20.0
    }

    #[inline]
    fn default_fail_max_count() -> usize {
        10
    }

    #[inline]
    fn default_fail_decay_rate() -> f32 {
        0.2
    }

    #[inline]
    fn default_fail_max_decay_time() -> Duration {
        Duration::from_secs(150)
    }

    #[inline]
    fn default_retry_steepness() -> f32 {
        5.0
    }

    #[inline]
    fn default_retry_max_count() -> usize {
        10
    }
}

impl Default for FarSchedulerConfig {
    #[inline]
    fn default() -> Self {
        FarSchedulerConfig {
            success_score: FarSchedulerConfig::default_success_score(),
            success_max_count: FarSchedulerConfig::default_success_max_count(),
            success_decay_rate: FarSchedulerConfig::default_success_decay_rate(
            ),
            success_max_decay_time:
                FarSchedulerConfig::default_success_max_decay_time(),
            success_cancel_fails:
                FarSchedulerConfig::default_success_cancel_fails(),
            fail_score: FarSchedulerConfig::default_fail_score(),
            fail_max_count: FarSchedulerConfig::default_fail_max_count(),
            fail_decay_rate: FarSchedulerConfig::default_fail_decay_rate(),
            fail_max_decay_time:
                FarSchedulerConfig::default_fail_max_decay_time(),
            retry_steepness: FarSchedulerConfig::default_retry_steepness(),
            retry_max_count: FarSchedulerConfig::default_retry_max_count()
        }
    }
}

impl<Channel, Verify, Endpoint> ConnectionConfig<Channel, Verify, Endpoint>
where
    Verify: Default
{
    /// Create a new `ConnectionConfig` from its components.
    ///
    /// The arguments of this function correspond to similarly-named
    /// fields in the YAML format.  See documentation for details.
    #[inline]
    pub fn new(
        channel_names: Vec<Channel>,
        endpoints: Vec<ConnectionEndpoint<Verify, Endpoint>>
    ) -> Self {
        ConnectionConfig {
            channel_names: channel_names,
            endpoints: endpoints
        }
    }

    /// Get the array of channel names.
    #[inline]
    pub fn channel_names(&self) -> &[Channel] {
        &self.channel_names
    }

    /// Get the array of endpoints.
    #[inline]
    pub fn endpoints(&self) -> &[ConnectionEndpoint<Verify, Endpoint>] {
        &self.endpoints
    }

    /// Deconstruct this into the channel configuration, channel
    /// names, and endpoints.
    #[inline]
    pub fn take(
        self
    ) -> (Vec<Channel>, Vec<ConnectionEndpoint<Verify, Endpoint>>) {
        (self.channel_names, self.endpoints)
    }
}

impl<Verify, Endpoint> ConnectionEndpoint<Verify, Endpoint> {
    #[inline]
    pub fn new(
        addr: Endpoint,
        verify: Verify
    ) -> Self {
        ConnectionEndpoint {
            addr: addr,
            verify: verify
        }
    }

    #[inline]
    pub fn addr(&self) -> &Endpoint {
        &self.addr
    }

    #[inline]
    pub fn verify(&self) -> &Verify {
        &self.verify
    }

    #[inline]
    pub fn take(self) -> (Endpoint, Verify) {
        (self.addr, self.verify)
    }
}

impl<Encoder, Decoder, IDs> Default
    for LargeObjProtoConfig<Encoder, Decoder, IDs>
where
    Decoder: Default,
    Encoder: Default,
    IDs: Default
{
    #[inline]
    fn default() -> Self {
        LargeObjProtoConfig {
            inbound_size_hint: None,
            outbound_size_hint: None,
            tombstone_duration:
                LargeObjProtoConfig::<Encoder, Decoder, IDs>::default_tombstone_duration(),
            retry: LargeObjProtoConfig::<Encoder, Decoder, IDs>::default_retry(),
            encoder: Encoder::default(),
            decoder: Decoder::default(),
            ids: IDs::default()
        }
    }
}

impl<Encoder, Decoder, IDs> LargeObjProtoConfig<Encoder, Decoder, IDs>
where
    Decoder: Default,
    Encoder: Default,
    IDs: Default
{
    #[inline]
    pub fn new(
        retry: Retry,
        encoder: Encoder,
        decoder: Decoder,
        ids: IDs,
        tombstone_duration: Duration,
        inbound_size_hint: Option<usize>,
        outbound_size_hint: Option<usize>
    ) -> Self {
        LargeObjProtoConfig {
            inbound_size_hint: inbound_size_hint,
            outbound_size_hint: outbound_size_hint,
            tombstone_duration: tombstone_duration,
            encoder: encoder,
            decoder: decoder,
            retry: retry,
            ids: ids
        }
    }

    #[inline]
    pub fn inbound_size_hint(&self) -> &Option<usize> {
        &self.inbound_size_hint
    }

    #[inline]
    pub fn outbound_size_hint(&self) -> &Option<usize> {
        &self.outbound_size_hint
    }

    #[inline]
    pub fn tombstone_duration(&self) -> Duration {
        self.tombstone_duration
    }

    #[inline]
    pub fn retry(&self) -> &Retry {
        &self.retry
    }

    #[inline]
    pub fn decoder(&self) -> &Decoder {
        &self.decoder
    }

    #[inline]
    pub fn encoder(&self) -> &Encoder {
        &self.encoder
    }

    #[inline]
    pub fn ids(&self) -> &IDs {
        &self.ids
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        Retry,
        Encoder,
        Decoder,
        IDs,
        Duration,
        Option<usize>,
        Option<usize>
    ) {
        (
            self.retry,
            self.encoder,
            self.decoder,
            self.ids,
            self.tombstone_duration,
            self.inbound_size_hint,
            self.outbound_size_hint
        )
    }

    fn default_retry() -> Retry {
        Retry::TERRESTRIAL_NETWORK_DEFAULT
    }

    fn default_tombstone_duration() -> Duration {
        Duration::from_secs(10)
    }
}

impl<Resolver, Epochs, Channel, Verify, Endpoint>
    PartyConfig<Resolver, Epochs, Channel, Verify, Endpoint>
where
    Resolver: Default,
    Verify: Default,
    Epochs: Default
{
    #[inline]
    pub fn new(
        scheduler: FarSchedulerConfig,
        resolve: Resolver,
        epochs: Epochs,
        retry: Retry,
        connections: Vec<ConnectionConfig<Channel, Verify, Endpoint>>,
        size_hint: Option<usize>
    ) -> Self {
        PartyConfig {
            scheduler: scheduler,
            resolve: resolve,
            epochs: epochs,
            retry: retry,
            connections: connections,
            size_hint: size_hint
        }
    }

    /// Get the scheduler configuration.
    #[inline]
    pub fn scheduler(&self) -> &FarSchedulerConfig {
        &self.scheduler
    }

    /// Get the resolver configuration.
    #[inline]
    pub fn resolve(&self) -> &Resolver {
        &self.resolve
    }

    /// Get the retry configuration.
    #[inline]
    pub fn retry(&self) -> &Retry {
        &self.retry
    }

    /// Get the epoch ID generator configuration.
    #[inline]
    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Get the set of possible connections.
    #[inline]
    pub fn connections(
        &self
    ) -> &[ConnectionConfig<Channel, Verify, Endpoint>] {
        &self.connections
    }

    /// Get the size hint.
    #[inline]
    pub fn size_hint(&self) -> Option<usize> {
        self.size_hint
    }

    /// Deconstruct this into the scheduler configuration, the
    /// resolver configuration, the size hint, and the retry
    /// configuration, the set of possible connections.
    #[inline]
    pub fn take(
        self
    ) -> (
        FarSchedulerConfig,
        Resolver,
        Epochs,
        Retry,
        Option<usize>,
        Vec<ConnectionConfig<Channel, Verify, Endpoint>>
    ) {
        (
            self.scheduler,
            self.resolve,
            self.epochs,
            self.retry,
            self.size_hint,
            self.connections
        )
    }
}

impl<PartyID, Stream> StreamMulticasterConfig<PartyID, Stream> {
    #[inline]
    pub fn new(
        parties: Vec<MulticastPartyConfig<PartyID, Stream>>,
        batch_slots: BatchSlotsConfig
    ) -> Self {
        StreamMulticasterConfig {
            batch_slots: batch_slots,
            parties: parties
        }
    }

    #[inline]
    pub fn batch_slots(&self) -> &BatchSlotsConfig {
        &self.batch_slots
    }

    #[inline]
    pub fn parties(&self) -> &[MulticastPartyConfig<PartyID, Stream>] {
        &self.parties
    }

    #[inline]
    pub fn take(
        self
    ) -> (Vec<MulticastPartyConfig<PartyID, Stream>>, BatchSlotsConfig) {
        (self.parties, self.batch_slots)
    }
}

impl<PartyID, Stream> MulticastPartyConfig<PartyID, Stream> {
    #[inline]
    pub fn new(
        party: PartyID,
        stream: Stream
    ) -> Self {
        MulticastPartyConfig {
            party: party,
            stream: stream
        }
    }

    #[inline]
    pub fn party(&self) -> &PartyID {
        &self.party
    }

    #[inline]
    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    #[inline]
    pub fn take(self) -> (PartyID, Stream) {
        (self.party, self.stream)
    }
}

impl<Epochs> DispatchConfig<Epochs>
where
    Epochs: Default
{
    /// Get the scheduler configuration.
    #[inline]
    pub fn scheduler(&self) -> &FarSchedulerConfig {
        &self.scheduler
    }

    /// Get the retry configuration.
    #[inline]
    pub fn retry(&self) -> &Retry {
        &self.retry
    }

    /// Get the epoch ID generator configuration.
    #[inline]
    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Get the size hint.
    #[inline]
    pub fn size_hint(&self) -> Option<usize> {
        self.size_hint
    }

    /// Deconstruct this into the scheduler configuration, the
    /// resolver configuration, the size hint, and the retry
    /// configuration, the set of possible connections.
    #[inline]
    pub fn take(self) -> (FarSchedulerConfig, Epochs, Retry, Option<usize>) {
        (self.scheduler, self.epochs, self.retry, self.size_hint)
    }
}

#[cfg(test)]
use std::net::SocketAddr;

#[test]
fn test_connection_config() {
    let yaml = concat!(
        "channel-names:\n",
        "  - \"chan-1\"\n",
        "  - \"chan-2\"\n",
        "endpoints:\n",
        "  - addr: 10.10.10.10:10000\n",
    );
    let addr: SocketAddr = "10.10.10.10:10000".parse().unwrap();
    let endpoint = ConnectionEndpoint {
        addr: addr,
        verify: ()
    };
    let channames = vec!["chan-1", "chan-2"];
    let endpoints = vec![endpoint];
    let expected = ConnectionConfig::new(channames, endpoints);
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual);
}

// #[test]
// fn test_party_config() {
// let yaml = concat!(
// "retry:\n",
// "  factor: 100\n",
// "  exp-base: 2.0\n",
// "  exp-factor: 1.0\n",
// "  exp-rounds-cap: 20\n",
// "  linear-factor: 1.0\n",
// "  linear-rounds-cap: 50\n",
// "  max-random: 100\n",
// "  addend: 50\n",
// "connections:\n",
// "  - channel-names:\n",
// "      - \"chan-1\"\n",
// "      - \"chan-2\"\n",
// "    endpoints:\n",
// "      - 10.10.10.10:10000\n",
// "  - channel-names:\n",
// "      - \"chan-2\"\n",
// "    endpoints:\n",
// "      - 10.10.10.10:9999\n",
// "udp:\n",
// "  addr: 10.10.10.10\n",
// "  port: 10000\n"
// );
// let retry = Retry::new(100, 2.0, 1.0, 20, 1.0, Some(50), 100, 50);
// let addr: SocketAddr = "10.10.10.10:10000".parse().unwrap();
// let udp = UDPFarChannelConfig::new(addr.ip(), addr.port());
// let endpoint = CompoundFarChannelConfig::UDP { udp: udp };
// let expected = PartyConfig {
// retry: retry,
// endpoint: endpoint
// };
// let actual = serde_yaml::from_str(yaml).unwrap();
//
// assert_eq!(expected, actual);
// }
