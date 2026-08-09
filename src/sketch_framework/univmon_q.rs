//! Experimental UnivMon-Q: universal frequency measurements with ordered quantile queries.
//!
//! Each numeric value is encoded into an order-preserving `u64` key. One
//! 128-bit key hash is split into independent fields for CountSketch rows and
//! the Joltik terminal stratum. Updates touch one physical CountSketch layer.
//! A separate coordinated bottom-k sample of stream occurrences is keyed by
//! `(source_id, local_sequence)`. Query-time recursion reconstructs the
//! logical UnivMon hierarchy; reliable heavy frequencies are combined with
//! the residual occurrence sample for rank and quantile estimates.
//!
//! Provenance: adapted from the experimental `zaoxing/univmon-quantile`
//! implementation and integrated with Sketchlib's numeric input, pluggable
//! hashing, merge, and native MessagePack APIs.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use rmp_serde::decode::Error as RmpDecodeError;
use rmp_serde::encode::Error as RmpEncodeError;
use serde::{Deserialize, Serialize};

use crate::common::input::data_input_to_f64;
use crate::common::numerical::NumericalValue;
use crate::{DataInput, DefaultXxHasher, SketchHasher};

const WIRE_VERSION: u8 = 2;
const OCCURRENCE_HASH_SEED_DOMAIN: usize = usize::MAX / 3;
static NEXT_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// Memory, accuracy, and hashing controls for [`UnivMonQ`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnivMonQConfig {
    /// Number of geometrically sampled logical substreams.
    pub levels: usize,
    /// Counters in each row of the upper-level CountSketch.
    pub width: usize,
    /// Levels between width halvings. Zero keeps the same width at every level.
    pub width_halving_period: u8,
    /// CountSketch rows. This must be odd so the median is unambiguous.
    pub depth: usize,
    /// Counter representation: compact saturating 32-bit or wide 64-bit.
    pub counter_bits: u8,
    /// Maximum candidate identities retained at each terminal stratum.
    pub candidates: usize,
    /// Coordinated stream-occurrence samples retained for ordered queries.
    /// Set to zero when rank/CDF/quantile queries are not required.
    pub ordered_samples: usize,
    /// Seed index passed to [`SketchHasher::hash128_seeded`].
    pub hash_seed: usize,
}

impl Default for UnivMonQConfig {
    fn default() -> Self {
        Self {
            levels: 10,
            width: 4096,
            width_halving_period: 0,
            depth: 5,
            counter_bits: 32,
            candidates: 1024,
            ordered_samples: 1024,
            hash_seed: 5,
        }
    }
}

impl UnivMonQConfig {
    /// Chooses the smallest hierarchy whose deepest sample fits in the
    /// candidate table with probability at least `1 - failure_probability`.
    /// `max_updates` is the aggregate window bound across all merged shards.
    pub fn with_window_bound(
        mut self,
        max_updates: u64,
        failure_probability: f64,
    ) -> Result<Self, UnivMonQError> {
        if self.candidates == 0 {
            return Err(UnivMonQError::new("candidates must be positive"));
        }
        if !failure_probability.is_finite() || !(0.0..1.0).contains(&failure_probability) {
            return Err(UnivMonQError::new("failure probability must be in (0, 1)"));
        }
        let log_inverse_delta = (1.0 / failure_probability).ln();
        for levels in 2..=63 {
            let mean = max_updates as f64 / 2.0_f64.powi((levels - 1) as i32);
            let upper =
                mean + (2.0 * mean * log_inverse_delta).sqrt() + (2.0 / 3.0) * log_inverse_delta;
            if upper < self.candidates as f64 {
                self.levels = levels;
                return Ok(self);
            }
        }
        Err(UnivMonQError::new(
            "window bound does not fit the candidate table within 63 levels",
        ))
    }
}

/// Configuration, merge, or decoded-state error for [`UnivMonQ`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnivMonQError(String);

impl UnivMonQError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for UnivMonQError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for UnivMonQError {}

/// A recovered value and its normalized cumulative rank.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UnivMonQPoint {
    /// Recovered numeric value.
    pub value: f64,
    /// Monotone CDF estimate in `[0, 1]`.
    pub rank: f64,
}

#[derive(Clone, Copy, Debug)]
struct RecoveredCandidate {
    key: u64,
    frequency: f64,
    terminal: usize,
}

#[derive(Debug)]
struct CandidateRecovery {
    physical_f2: Vec<f64>,
    by_terminal: Vec<Vec<RecoveredCandidate>>,
}

/// One retained stream occurrence. Splitting the 128-bit priority into two
/// words keeps the record at 24 bytes instead of introducing `u128` alignment
/// padding on common 64-bit targets.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
struct OrderedOccurrence {
    priority_high: u64,
    priority_low: u64,
    key: u64,
}

/// Reusable, immutable query state prepared from a [`UnivMonQ`] sketch.
///
/// Preparing the view reconstructs the logical UnivMon hierarchy and ordered
/// CDF once. Use it when answering more than one aggregate or ordered query
/// from the same sketch snapshot. Point-frequency queries continue to read the
/// underlying CountSketch directly.
#[derive(Debug)]
pub struct UnivMonQQuery<'a, H: SketchHasher = DefaultXxHasher> {
    sketch: &'a UnivMonQ<H>,
    logical_heavy: Vec<Vec<RecoveredCandidate>>,
    heavy_hitters: Vec<RecoveredCandidate>,
    cdf: Vec<UnivMonQPoint>,
}

#[derive(Clone, Copy, Debug)]
struct HashLayout {
    bucket_bits: u32,
    terminal_offset: u32,
    terminal_bits: u32,
}

impl HashLayout {
    fn new(config: UnivMonQConfig) -> Result<Self, UnivMonQError> {
        let bucket_bits = usize::BITS - (config.width - 1).leading_zeros();
        let row_bits = bucket_bits + 1;
        let terminal_offset = row_bits * config.depth as u32;
        let terminal_bits = config.levels as u32 - 1;
        let used_bits = terminal_offset + terminal_bits;
        if config.depth > 64 || used_bits > 128 {
            return Err(UnivMonQError::new(
                "hash layout needs more than the available 128 hash bits",
            ));
        }
        Ok(Self {
            bucket_bits,
            terminal_offset,
            terminal_bits,
        })
    }

    #[inline(always)]
    fn terminal_level(self, hash: u128) -> usize {
        let mask = (1_u128 << self.terminal_bits) - 1;
        let field = (hash >> self.terminal_offset) & mask;
        (field.trailing_zeros() as usize).min(self.terminal_bits as usize)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Counters {
    I32(Vec<i32>),
    I64(Vec<i64>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PackedCountSketch {
    width: usize,
    depth: usize,
    counters: Counters,
}

impl PackedCountSketch {
    fn new(width: usize, depth: usize, counter_bits: u8) -> Self {
        Self {
            width,
            depth,
            counters: if counter_bits == 32 {
                Counters::I32(vec![0; width * depth])
            } else {
                Counters::I64(vec![0; width * depth])
            },
        }
    }

    #[inline(always)]
    fn update(&mut self, hash: u128, bucket_bits: u32) {
        match &mut self.counters {
            Counters::I32(counters) => {
                update_rows(counters, self.width, self.depth, hash, bucket_bits)
            }
            Counters::I64(counters) => {
                update_rows(counters, self.width, self.depth, hash, bucket_bits)
            }
        }
    }

    fn estimate(&self, hash: u128, bucket_bits: u32) -> i64 {
        let mut estimates = [0_i64; 64];
        match &self.counters {
            Counters::I32(counters) => estimate_rows(
                counters,
                self.width,
                self.depth,
                hash,
                bucket_bits,
                &mut estimates,
            ),
            Counters::I64(counters) => estimate_rows(
                counters,
                self.width,
                self.depth,
                hash,
                bucket_bits,
                &mut estimates,
            ),
        }
        estimates[..self.depth].sort_unstable();
        estimates[self.depth / 2].max(0)
    }

    fn estimated_f2(&self) -> f64 {
        let mut rows = Vec::with_capacity(self.depth);
        match &self.counters {
            Counters::I32(counters) => f2_rows(counters, self.width, self.depth, &mut rows),
            Counters::I64(counters) => f2_rows(counters, self.width, self.depth, &mut rows),
        }
        rows.sort_unstable_by(f64::total_cmp);
        rows[self.depth / 2]
    }

    fn merge(&mut self, other: &Self) {
        match (&mut self.counters, &other.counters) {
            (Counters::I32(left), Counters::I32(right)) => {
                for (left, right) in left.iter_mut().zip(right) {
                    *left = left.saturating_add(*right);
                }
            }
            (Counters::I64(left), Counters::I64(right)) => {
                for (left, right) in left.iter_mut().zip(right) {
                    *left = left.saturating_add(*right);
                }
            }
            _ => unreachable!("matching configurations use matching counter types"),
        }
    }

    fn clear(&mut self) {
        match &mut self.counters {
            Counters::I32(counters) => counters.fill(0),
            Counters::I64(counters) => counters.fill(0),
        }
    }

    fn bytes(&self) -> usize {
        match &self.counters {
            Counters::I32(counters) => counters.len() * size_of::<i32>(),
            Counters::I64(counters) => counters.len() * size_of::<i64>(),
        }
    }

    fn matches(&self, width: usize, depth: usize, counter_bits: u8) -> bool {
        if self.width != width || self.depth != depth {
            return false;
        }
        match &self.counters {
            Counters::I32(values) => counter_bits == 32 && values.len() == width * depth,
            Counters::I64(values) => counter_bits == 64 && values.len() == width * depth,
        }
    }
}

trait Counter: Copy {
    fn saturating_add(self, other: Self) -> Self;
    fn saturating_neg(self) -> Self;
    fn to_i64(self) -> i64;
    fn one() -> Self;
}

impl Counter for i32 {
    fn saturating_add(self, other: Self) -> Self {
        self.saturating_add(other)
    }
    fn saturating_neg(self) -> Self {
        self.saturating_neg()
    }
    fn to_i64(self) -> i64 {
        i64::from(self)
    }
    fn one() -> Self {
        1
    }
}

impl Counter for i64 {
    fn saturating_add(self, other: Self) -> Self {
        self.saturating_add(other)
    }
    fn saturating_neg(self) -> Self {
        self.saturating_neg()
    }
    fn to_i64(self) -> i64 {
        self
    }
    fn one() -> Self {
        1
    }
}

fn update_rows<T: Counter>(
    counters: &mut [T],
    width: usize,
    depth: usize,
    hash: u128,
    bucket_bits: u32,
) {
    let row_bits = bucket_bits + 1;
    let bucket_mask = (1_u128 << bucket_bits) - 1;
    let width_mask = width.is_power_of_two().then_some(width - 1);
    for row in 0..depth {
        let field = hash >> (row as u32 * row_bits);
        let bucket_code = (field & bucket_mask) as usize;
        let bucket = width_mask.map_or_else(|| bucket_code % width, |mask| bucket_code & mask);
        let delta = if (field >> bucket_bits) & 1 == 0 {
            T::one()
        } else {
            T::one().saturating_neg()
        };
        let index = row * width + bucket;
        counters[index] = counters[index].saturating_add(delta);
    }
}

fn estimate_rows<T: Counter>(
    counters: &[T],
    width: usize,
    depth: usize,
    hash: u128,
    bucket_bits: u32,
    estimates: &mut [i64; 64],
) {
    let row_bits = bucket_bits + 1;
    let bucket_mask = (1_u128 << bucket_bits) - 1;
    let width_mask = width.is_power_of_two().then_some(width - 1);
    for (row, estimate) in estimates.iter_mut().enumerate().take(depth) {
        let field = hash >> (row as u32 * row_bits);
        let bucket_code = (field & bucket_mask) as usize;
        let bucket = width_mask.map_or_else(|| bucket_code % width, |mask| bucket_code & mask);
        let counter = counters[row * width + bucket].to_i64();
        *estimate = if (field >> bucket_bits) & 1 == 0 {
            counter
        } else {
            counter.saturating_neg()
        };
    }
}

fn f2_rows<T: Counter>(counters: &[T], width: usize, depth: usize, rows: &mut Vec<f64>) {
    for row in 0..depth {
        let start = row * width;
        rows.push(
            counters[start..start + width]
                .iter()
                .map(|counter| (counter.to_i64() as f64).powi(2))
                .sum(),
        );
    }
}

#[derive(Clone, Debug)]
struct Level {
    sketch: PackedCountSketch,
    candidate_scores: HashMap<u64, u64>,
    candidate_heap: BinaryHeap<Reverse<(u64, u64)>>,
    candidate_capacity: usize,
    ever_evicted: bool,
}

impl Level {
    fn new(width: usize, depth: usize, counter_bits: u8, candidates: usize) -> Self {
        Self {
            sketch: PackedCountSketch::new(width, depth, counter_bits),
            candidate_scores: HashMap::with_capacity(candidates),
            candidate_heap: BinaryHeap::with_capacity(candidates),
            candidate_capacity: candidates,
            ever_evicted: false,
        }
    }

    fn update(&mut self, key: u64, hash: u128, bucket_bits: u32) {
        self.sketch.update(hash, bucket_bits);
        let score = self.sketch.estimate(hash, bucket_bits).max(0) as u64;
        if let Some(stored_score) = self.candidate_scores.get_mut(&key) {
            *stored_score = score;
            return;
        }
        if self.candidate_scores.len() < self.candidate_capacity {
            self.candidate_scores.insert(key, score);
            self.candidate_heap.push(Reverse((score, key)));
            return;
        }
        if let Some((minimum, evicted)) = self.lightest_candidate() {
            self.ever_evicted = true;
            if score > minimum {
                self.candidate_scores.remove(&evicted);
                self.candidate_scores.insert(key, score);
                self.candidate_heap.push(Reverse((score, key)));
            } else {
                self.candidate_heap.push(Reverse((minimum, evicted)));
            }
        }
    }

    fn lightest_candidate(&mut self) -> Option<(u64, u64)> {
        loop {
            let Reverse((stored_score, key)) = *self.candidate_heap.peek()?;
            let Some(&current_score) = self.candidate_scores.get(&key) else {
                self.candidate_heap.pop();
                continue;
            };
            if current_score != stored_score {
                self.candidate_heap.pop();
                self.candidate_heap.push(Reverse((current_score, key)));
                continue;
            }
            self.candidate_heap.pop();
            return Some((stored_score, key));
        }
    }

    fn merge(&mut self, other: &Self, bucket_bits: u32, hash_key: impl Fn(u64) -> u128) {
        self.sketch.merge(&other.sketch);
        let mut combined: HashSet<u64> = self.candidate_scores.keys().copied().collect();
        combined.extend(other.candidate_scores.keys().copied());
        let combined_len = combined.len();
        let mut retained: Vec<(u64, u64)> = combined
            .into_iter()
            .map(|key| {
                let hash = hash_key(key);
                let score = self.sketch.estimate(hash, bucket_bits).max(0) as u64;
                (key, score)
            })
            .collect();
        retained.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        retained.truncate(self.candidate_capacity);
        self.ever_evicted =
            self.ever_evicted || other.ever_evicted || combined_len > self.candidate_capacity;
        self.candidate_scores.clear();
        self.candidate_heap.clear();
        for (key, count) in retained {
            self.candidate_scores.insert(key, count);
            self.candidate_heap.push(Reverse((count, key)));
        }
    }

    fn clear(&mut self) {
        self.sketch.clear();
        self.candidate_scores.clear();
        self.candidate_heap.clear();
        self.ever_evicted = false;
    }
}

/// Experimental mergeable universal sketch extended with rank, CDF, and quantile queries.
///
/// `H` is the same pluggable hash abstraction used throughout Sketchlib. The
/// default uses [`DefaultXxHasher`]. Values use `f64::total_cmp` ordering,
/// including distinct `-0.0`/`0.0` and deterministic placement of NaNs.
#[derive(Clone, Debug)]
pub struct UnivMonQ<H: SketchHasher = DefaultXxHasher> {
    config: UnivMonQConfig,
    hash_layout: HashLayout,
    levels: Vec<Level>,
    count: u64,
    min: Option<u64>,
    max: Option<u64>,
    source_id: u64,
    next_sequence: u64,
    ordered_heap: BinaryHeap<OrderedOccurrence>,
    hasher: PhantomData<H>,
}

impl<'a, H: SketchHasher> UnivMonQQuery<'a, H> {
    /// Number of observations in the prepared sketch snapshot.
    pub fn count(&self) -> u64 {
        self.sketch.count
    }

    /// Exact minimum under `f64::total_cmp` ordering.
    pub fn min(&self) -> Option<f64> {
        self.sketch.min()
    }

    /// Exact maximum under `f64::total_cmp` ordering.
    pub fn max(&self) -> Option<f64> {
        self.sketch.max()
    }

    /// CountSketch point-frequency estimate for a numeric value.
    pub fn estimate_frequency(&self, value: f64) -> u64 {
        self.sketch.estimate_frequency(value)
    }

    /// Experimental fixed-threshold rank estimate from the UnivMon recurrence.
    ///
    /// Unlike [`Self::rank`], this does not use or require the ordered sample.
    /// It evaluates the separable subset function
    /// `g_x(key, frequency) = frequency * I[key <= x]` over the recovered
    /// hierarchy. Individual thresholds inherit UnivMon's candidate-recovery
    /// assumptions; estimates at different thresholds are not jointly forced
    /// to be monotone.
    pub fn estimate_rank_universal(&self, value: f64) -> Option<u64> {
        if self.count() == 0 {
            return None;
        }
        let key = float_to_ordered(value);
        if key < self.sketch.min? {
            return Some(0);
        }
        if key >= self.sketch.max? {
            return Some(self.count());
        }
        Some(
            estimate_keyed_sum_from(&self.logical_heavy, |candidate_key, frequency| {
                if candidate_key <= key { frequency } else { 0.0 }
            })
            .round()
            .clamp(0.0, self.count() as f64) as u64,
        )
    }

    /// Estimates `sum_x g(f_x)` using the prepared logical hierarchy.
    ///
    /// As with UnivMon's generic estimator, useful accuracy requires a
    /// frequency function compatible with the retained-heavy-item recurrence;
    /// callers should normally use the provided F0/F2/entropy methods.
    pub fn estimate_g_sum<F>(&self, g: F) -> f64
    where
        F: Fn(f64) -> f64,
    {
        estimate_g_sum_from(&self.logical_heavy, g)
    }

    /// UnivMon estimate of the number of distinct numeric values.
    pub fn estimate_distinct(&self) -> f64 {
        self.estimate_g_sum(|_| 1.0).clamp(0.0, self.count() as f64)
    }

    /// UnivMon estimate of the frequency vector's second moment.
    pub fn estimate_f2(&self) -> f64 {
        self.estimate_g_sum(|frequency| frequency * frequency)
            .max(0.0)
    }

    /// Estimated Shannon entropy in nats.
    pub fn estimate_entropy(&self) -> f64 {
        if self.count() == 0 {
            return 0.0;
        }
        let frequency_log_frequency = self.estimate_g_sum(|frequency| {
            if frequency > 0.0 {
                frequency * frequency.ln()
            } else {
                0.0
            }
        });
        ((self.count() as f64).ln() - frequency_log_frequency / self.count() as f64).max(0.0)
    }

    /// Recovered heavy values and estimated frequencies in descending order.
    pub fn heavy_hitters(&self, k: usize) -> Vec<(f64, u64)> {
        if k == 0 || self.count() == 0 {
            return Vec::new();
        }
        self.heavy_hitters
            .iter()
            .take(k)
            .map(|candidate| {
                (
                    ordered_to_float(candidate.key),
                    candidate.frequency.round().max(0.0) as u64,
                )
            })
            .collect()
    }

    /// Estimated number of observations at or below `value`.
    pub fn rank(&self, value: f64) -> Option<u64> {
        if self.count() == 0 || self.sketch.config.ordered_samples == 0 {
            return None;
        }
        let min = self.min()?;
        let max = self.max()?;
        if value.total_cmp(&min).is_lt() {
            return Some(0);
        }
        if !value.total_cmp(&max).is_lt() {
            return Some(self.count());
        }
        let index = self
            .cdf
            .partition_point(|point| !point.value.total_cmp(&value).is_gt());
        Some(if index == 0 {
            0
        } else {
            (self.cdf[index - 1].rank * self.count() as f64)
                .round()
                .clamp(0.0, self.count() as f64) as u64
        })
    }

    /// Estimated value at normalized rank `q`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count() == 0 || !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        if q == 0.0 {
            return self.min();
        }
        if q == 1.0 {
            return self.max();
        }
        if self.sketch.config.ordered_samples == 0 {
            return None;
        }
        let target = (q * self.count() as f64).ceil().max(1.0) / self.count() as f64;
        let index = self.cdf.partition_point(|point| point.rank < target);
        self.cdf
            .get(index.min(self.cdf.len().saturating_sub(1)))
            .map(|point| point.value)
    }

    /// Answers several quantiles while reusing the prepared CDF.
    pub fn quantiles(&self, quantiles: &[f64]) -> Vec<Option<f64>> {
        quantiles.iter().map(|&q| self.quantile(q)).collect()
    }

    /// Returns the prepared monotone CDF without rebuilding it.
    pub fn cdf(&self) -> &[UnivMonQPoint] {
        &self.cdf
    }
}

impl<H: SketchHasher> Default for UnivMonQ<H> {
    fn default() -> Self {
        Self::with_hasher(UnivMonQConfig::default()).expect("default UnivMon-Q config is valid")
    }
}

impl<H: SketchHasher> UnivMonQ<H> {
    /// Creates an empty sketch using `H` and the supplied configuration.
    pub fn with_hasher(config: UnivMonQConfig) -> Result<Self, UnivMonQError> {
        Self::with_hasher_and_source_id(config, allocate_source_id())
    }

    /// Creates an empty sketch with an explicit distributed source identity.
    ///
    /// Every concurrently mergeable source must use a different ID. Ordered
    /// occurrence priorities are derived from `(source_id, local_sequence)`.
    pub fn with_hasher_and_source_id(
        config: UnivMonQConfig,
        source_id: u64,
    ) -> Result<Self, UnivMonQError> {
        validate_config(config)?;
        let hash_layout = HashLayout::new(config)?;
        let levels = (0..config.levels)
            .map(|level| {
                Level::new(
                    level_width(config, level),
                    config.depth,
                    config.counter_bits,
                    config.candidates,
                )
            })
            .collect();
        Ok(Self {
            config,
            hash_layout,
            levels,
            count: 0,
            min: None,
            max: None,
            source_id,
            next_sequence: 0,
            ordered_heap: BinaryHeap::with_capacity(config.ordered_samples),
            hasher: PhantomData,
        })
    }

    /// Returns the immutable configuration required by compatible merges.
    pub fn config(&self) -> UnivMonQConfig {
        self.config
    }

    /// Identity used to coordinate occurrence priorities from this source.
    pub fn source_id(&self) -> u64 {
        self.source_id
    }

    /// Adds one numeric value.
    #[inline(always)]
    pub fn update(&mut self, value: &f64) {
        self.update_value(*value);
    }

    /// Adds any Sketchlib numeric primitive after projection to `f64`.
    #[inline(always)]
    pub fn add<T: NumericalValue>(&mut self, value: &T) {
        self.update_value(value.to_f64());
    }

    /// Adds a type-erased numeric input. Strings and byte arrays are rejected.
    pub fn update_data_input(&mut self, value: &DataInput) -> Result<(), &'static str> {
        let value =
            data_input_to_f64(value).map_err(|_| "UnivMon-Q sketch only accepts numeric inputs")?;
        self.update_value(value);
        Ok(())
    }

    #[inline(always)]
    fn update_value(&mut self, value: f64) {
        let key = float_to_ordered(value);
        let hash = self.hash_key(key);
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("UnivMon-Q source sequence exhausted");
        self.count = self.count.saturating_add(1);
        self.min = Some(self.min.map_or(key, |old| old.min(key)));
        self.max = Some(self.max.map_or(key, |old| old.max(key)));
        let terminal = self.hash_layout.terminal_level(hash);
        self.levels[terminal].update(key, hash, self.hash_layout.bucket_bits);
        if self.config.ordered_samples > 0 {
            let occurrence = self.occurrence(key, sequence);
            self.update_ordered_sample(occurrence);
        }
    }

    /// Merges a shard built with the identical configuration and hash profile.
    pub fn merge(&mut self, other: &Self) -> Result<(), UnivMonQError> {
        if self.config != other.config {
            return Err(UnivMonQError::new(
                "cannot merge UnivMon-Q sketches with different configurations",
            ));
        }
        self.count = self.count.saturating_add(other.count);
        self.min = match (self.min, other.min) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.max = match (self.max, other.max) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        let hash_seed = self.config.hash_seed;
        let bucket_bits = self.hash_layout.bucket_bits;
        for (left, right) in self.levels.iter_mut().zip(&other.levels) {
            left.merge(right, bucket_bits, |key| {
                H::hash128_seeded(hash_seed, &DataInput::U64(key))
            });
        }
        self.merge_ordered_samples(other);
        Ok(())
    }

    /// Removes all observations without changing configuration or allocations.
    pub fn clear(&mut self) {
        self.count = 0;
        self.min = None;
        self.max = None;
        self.ordered_heap.clear();
        for level in &mut self.levels {
            level.clear();
        }
    }

    /// Number of observations processed, including duplicates.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Alias for [`Self::count`].
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Whether the sketch has no observations.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Exact minimum under `f64::total_cmp` ordering.
    pub fn min(&self) -> Option<f64> {
        self.min.map(ordered_to_float)
    }

    /// Exact maximum under `f64::total_cmp` ordering.
    pub fn max(&self) -> Option<f64> {
        self.max.map(ordered_to_float)
    }

    /// CountSketch point-frequency estimate for a numeric value.
    pub fn estimate_frequency(&self, value: f64) -> u64 {
        if self.is_empty() {
            return 0;
        }
        let key = float_to_ordered(value);
        self.frequency_key(key).max(0) as u64
    }

    /// Experimental fixed-threshold rank estimate from the UnivMon recurrence.
    ///
    /// This path is independent of `ordered_samples`. Prefer
    /// [`Self::prepare_queries`] when evaluating more than one threshold.
    pub fn estimate_rank_universal(&self, value: f64) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        self.prepare_queries().estimate_rank_universal(value)
    }

    /// Reconstructs a reusable query snapshot.
    ///
    /// Use this when answering multiple universal metrics or ordered queries
    /// without intervening updates. Candidate recovery, CountSketch F2 scans,
    /// and CDF construction are performed once for the whole batch.
    pub fn prepare_queries(&self) -> UnivMonQQuery<'_, H> {
        let recovery = self.recover_candidates();
        let logical_heavy = self.logical_heavy_sets_from(&recovery);
        let heavy_hitters = self.top_candidates_from(&recovery);
        let ordered_heavy = self.assisted_ordered_heavy(&logical_heavy, &heavy_hitters);
        let cdf = self
            .cdf_keys_from(&ordered_heavy)
            .into_iter()
            .map(|(value, rank)| UnivMonQPoint {
                value: ordered_to_float(value),
                rank,
            })
            .collect();
        UnivMonQQuery {
            sketch: self,
            logical_heavy,
            heavy_hitters,
            cdf,
        }
    }

    /// Estimates `sum_x g(f_x)` using the UnivMon recurrence.
    ///
    /// Call [`Self::prepare_queries`] and use [`UnivMonQQuery::estimate_g_sum`]
    /// when evaluating more than one function over the same sketch snapshot.
    pub fn estimate_g_sum<F>(&self, g: F) -> f64
    where
        F: Fn(f64) -> f64,
    {
        if self.is_empty() {
            return 0.0;
        }
        let recovery = self.recover_candidates();
        let heavy = self.logical_heavy_sets_from(&recovery);
        estimate_g_sum_from(&heavy, g)
    }

    /// UnivMon estimate of the number of distinct numeric values.
    pub fn estimate_distinct(&self) -> f64 {
        self.estimate_g_sum(|_| 1.0).clamp(0.0, self.count as f64)
    }

    /// UnivMon estimate of the frequency vector's second moment.
    pub fn estimate_f2(&self) -> f64 {
        self.estimate_g_sum(|frequency| frequency * frequency)
            .max(0.0)
    }

    /// Estimated Shannon entropy in nats.
    pub fn estimate_entropy(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let frequency_log_frequency = self.estimate_g_sum(|frequency| {
            if frequency > 0.0 {
                frequency * frequency.ln()
            } else {
                0.0
            }
        });
        ((self.count as f64).ln() - frequency_log_frequency / self.count as f64).max(0.0)
    }

    /// Recovered heavy values and estimated frequencies in descending order.
    pub fn heavy_hitters(&self, k: usize) -> Vec<(f64, u64)> {
        if k == 0 || self.is_empty() {
            return Vec::new();
        }
        let recovery = self.recover_candidates();
        self.top_candidates_from(&recovery)
            .into_iter()
            .take(k)
            .map(|candidate| {
                (
                    ordered_to_float(candidate.key),
                    candidate.frequency.round().max(0.0) as u64,
                )
            })
            .collect()
    }

    /// Estimated number of observations at or below `value`.
    /// Returns `None` when ordered sampling is disabled or the sketch is empty.
    pub fn rank(&self, value: f64) -> Option<u64> {
        if self.is_empty() || self.config.ordered_samples == 0 {
            return None;
        }
        let key = float_to_ordered(value);
        if key < self.min? {
            return Some(0);
        }
        if key >= self.max? {
            return Some(self.count);
        }
        let cdf = self.cdf_keys();
        let index = cdf.partition_point(|point| point.0 <= key);
        Some(if index == 0 {
            0
        } else {
            (cdf[index - 1].1 * self.count as f64)
                .round()
                .clamp(0.0, self.count as f64) as u64
        })
    }

    /// Estimated value at normalized rank `q`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.is_empty() || !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        if q == 0.0 {
            return self.min();
        }
        if q == 1.0 {
            return self.max();
        }
        if self.config.ordered_samples == 0 {
            return None;
        }
        let cdf = self.cdf_keys();
        self.quantile_from_cdf(q, &cdf)
    }

    /// Alias matching the DDSketch quantile naming convention.
    pub fn get_value_at_quantile(&self, q: f64) -> Option<f64> {
        self.quantile(q)
    }

    /// Answers several quantiles with one CDF reconstruction.
    pub fn quantiles(&self, quantiles: &[f64]) -> Vec<Option<f64>> {
        if self.is_empty() {
            return vec![None; quantiles.len()];
        }
        let cdf = self.cdf_keys();
        quantiles
            .iter()
            .map(|&q| self.quantile_from_cdf(q, &cdf))
            .collect()
    }

    /// Recovered monotone CDF breakpoints.
    pub fn cdf(&self) -> Vec<UnivMonQPoint> {
        self.cdf_keys()
            .into_iter()
            .map(|(value, rank)| UnivMonQPoint {
                value: ordered_to_float(value),
                rank,
            })
            .collect()
    }

    /// Conservative storage estimate for counters and bounded metadata.
    pub fn estimated_memory_bytes(&self) -> usize {
        let counters: usize = self.levels.iter().map(|level| level.sketch.bytes()).sum();
        let candidates = self.config.levels
            * self.config.candidates
            * (size_of::<(u64, u64)>() + 2 * size_of::<usize>());
        let candidate_heaps = self.config.levels * self.config.candidates * size_of::<(u64, u64)>();
        let eviction_history = self.config.levels * size_of::<bool>();
        let ordered = self.config.ordered_samples * size_of::<OrderedOccurrence>();
        counters + candidates + candidate_heaps + eviction_history + ordered
    }

    fn cdf_keys(&self) -> Vec<(u64, f64)> {
        if self.is_empty() || self.config.ordered_samples == 0 {
            return Vec::new();
        }
        let recovery = self.recover_candidates();
        let logical_heavy = self.logical_heavy_sets_from(&recovery);
        let heavy_hitters = self.top_candidates_from(&recovery);
        let heavy = self.assisted_ordered_heavy(&logical_heavy, &heavy_hitters);
        self.cdf_keys_from(&heavy)
    }

    fn cdf_keys_from(&self, global_heavy: &[(u64, f64)]) -> Vec<(u64, f64)> {
        if self.is_empty() || self.config.ordered_samples == 0 {
            return Vec::new();
        }
        let mut heavy: BTreeMap<u64, f64> = global_heavy.iter().copied().collect();
        let raw_heavy_total: f64 = heavy.values().sum();
        if raw_heavy_total > self.count as f64 {
            let scale = self.count as f64 / raw_heavy_total;
            for frequency in heavy.values_mut() {
                *frequency *= scale;
            }
        }
        let heavy_total: f64 = heavy.values().sum();
        let residual_total = (self.count as f64 - heavy_total).max(0.0);
        let mut residual_keys: Vec<u64> = self
            .ordered_heap
            .iter()
            .filter(|occurrence| !heavy.contains_key(&occurrence.key))
            .map(|occurrence| occurrence.key)
            .collect();
        if residual_keys.is_empty() && residual_total > 0.0 {
            heavy.clear();
            residual_keys = self
                .ordered_heap
                .iter()
                .map(|occurrence| occurrence.key)
                .collect();
        }
        let residual_total = if heavy.is_empty() {
            self.count as f64
        } else {
            (self.count as f64 - heavy.values().sum::<f64>()).max(0.0)
        };
        let residual_weight = if residual_keys.is_empty() {
            0.0
        } else {
            residual_total / residual_keys.len() as f64
        };
        let mut weights = heavy;
        for key in residual_keys {
            *weights.entry(key).or_insert(0.0) += residual_weight;
        }
        weights.entry(self.min.unwrap()).or_insert(0.0);
        weights.entry(self.max.unwrap()).or_insert(0.0);
        let mut running = 0.0;
        let mut points: Vec<(u64, f64)> = weights
            .into_iter()
            .map(|(value, weight)| {
                running += weight;
                (value, (running / self.count as f64).clamp(0.0, 1.0))
            })
            .collect();
        if let Some(last) = points.last_mut() {
            last.1 = 1.0;
        }
        isotonicize(&mut points);
        points
    }

    fn recover_candidates(&self) -> CandidateRecovery {
        let physical_f2 = self
            .levels
            .iter()
            .map(|level| level.sketch.estimated_f2())
            .collect();
        let by_terminal = self
            .levels
            .iter()
            .enumerate()
            .map(|(terminal, level)| {
                level
                    .candidate_scores
                    .keys()
                    .filter_map(|&key| {
                        let hash = self.hash_key(key);
                        let frequency =
                            level.sketch.estimate(hash, self.hash_layout.bucket_bits) as f64;
                        (frequency > 0.0).then_some(RecoveredCandidate {
                            key,
                            frequency,
                            terminal,
                        })
                    })
                    .collect()
            })
            .collect();
        CandidateRecovery {
            physical_f2,
            by_terminal,
        }
    }

    fn top_candidates_from(&self, recovery: &CandidateRecovery) -> Vec<RecoveredCandidate> {
        let mut candidates: Vec<_> = recovery.by_terminal.iter().flatten().copied().collect();
        candidates.sort_unstable_by(|left, right| {
            right
                .frequency
                .total_cmp(&left.frequency)
                .then_with(|| left.key.cmp(&right.key))
        });
        candidates.truncate(self.config.candidates);
        candidates
    }

    fn assisted_ordered_heavy(
        &self,
        logical_heavy: &[Vec<RecoveredCandidate>],
        heavy_hitters: &[RecoveredCandidate],
    ) -> Vec<(u64, f64)> {
        if self.count == 0 || self.config.ordered_samples == 0 {
            return Vec::new();
        }
        let f2 = estimate_g_sum_from(logical_heavy, |frequency| frequency * frequency).max(0.0);
        let concentration = f2 / (self.count as f64).powi(2);
        if concentration < 1.0 / self.config.ordered_samples as f64 {
            return Vec::new();
        }
        let threshold = (f2 / self.config.width as f64).sqrt();
        heavy_hitters
            .iter()
            .take(64)
            .filter(|candidate| candidate.frequency >= threshold)
            .map(|candidate| (candidate.key, candidate.frequency))
            .collect()
    }

    fn logical_heavy_sets_from(
        &self,
        recovery: &CandidateRecovery,
    ) -> Vec<Vec<RecoveredCandidate>> {
        let mut logical = vec![Vec::new(); self.levels.len()];
        let mut suffix = Vec::with_capacity(self.config.candidates * 2);
        let mut suffix_f2 = 0.0;
        let mut suffix_candidates = 0_usize;
        let mut suffix_ever_evicted = false;
        for terminal in (0..self.levels.len()).rev() {
            suffix_f2 += recovery.physical_f2[terminal];
            suffix_candidates += recovery.by_terminal[terminal].len();
            suffix_ever_evicted |= self.levels[terminal].ever_evicted;
            suffix.extend(recovery.by_terminal[terminal].iter().copied());
            suffix.sort_unstable_by(|left, right| {
                right
                    .frequency
                    .total_cmp(&left.frequency)
                    .then_with(|| left.key.cmp(&right.key))
            });
            suffix.truncate(self.config.candidates);
            let mut recovered = suffix.clone();
            let complete = !suffix_ever_evicted && suffix_candidates <= self.config.candidates;
            if !complete {
                let threshold = 2.0 * (suffix_f2 / self.config.candidates as f64).sqrt();
                recovered.retain(|candidate| candidate.frequency >= threshold);
            }
            logical[terminal] = recovered;
        }
        logical
    }

    fn quantile_from_cdf(&self, q: f64, cdf: &[(u64, f64)]) -> Option<f64> {
        if self.is_empty() || !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        if q == 0.0 {
            return self.min();
        }
        if q == 1.0 {
            return self.max();
        }
        if self.config.ordered_samples == 0 {
            return None;
        }
        let target = (q * self.count as f64).ceil().max(1.0) / self.count as f64;
        let index = cdf.partition_point(|point| point.1 < target);
        cdf.get(index.min(cdf.len().saturating_sub(1)))
            .map(|point| ordered_to_float(point.0))
    }

    fn update_ordered_sample(&mut self, occurrence: OrderedOccurrence) {
        let capacity = self.config.ordered_samples;
        if capacity == 0 {
            return;
        }
        if self.ordered_heap.len() < capacity {
            self.ordered_heap.push(occurrence);
            return;
        }
        if self
            .ordered_heap
            .peek()
            .is_some_and(|largest| occurrence < *largest)
        {
            self.ordered_heap.pop();
            self.ordered_heap.push(occurrence);
        }
    }

    fn merge_ordered_samples(&mut self, other: &Self) {
        let capacity = self.config.ordered_samples;
        if capacity == 0 {
            return;
        }
        for &occurrence in &other.ordered_heap {
            self.update_ordered_sample(occurrence);
        }
    }

    #[inline(always)]
    fn occurrence(&self, key: u64, sequence: u64) -> OrderedOccurrence {
        let identity = (u128::from(self.source_id) << 64) | u128::from(sequence);
        let priority = H::hash128_seeded(
            self.config.hash_seed ^ OCCURRENCE_HASH_SEED_DOMAIN,
            &DataInput::U128(identity),
        );
        OrderedOccurrence {
            priority_high: (priority >> 64) as u64,
            priority_low: priority as u64,
            key,
        }
    }

    #[inline(always)]
    fn hash_key(&self, key: u64) -> u128 {
        H::hash128_seeded(self.config.hash_seed, &DataInput::U64(key))
    }

    #[inline(always)]
    fn sample_level(&self, key: u64) -> usize {
        self.hash_layout.terminal_level(self.hash_key(key))
    }

    fn frequency_key(&self, key: u64) -> i64 {
        let hash = self.hash_key(key);
        let terminal = self.hash_layout.terminal_level(hash);
        self.levels[terminal]
            .sketch
            .estimate(hash, self.hash_layout.bucket_bits)
    }
}

#[derive(Serialize, Deserialize)]
struct LevelWire {
    sketch: PackedCountSketch,
    candidates: Vec<(u64, u64)>,
    #[serde(default)]
    ever_evicted: Option<bool>,
}

#[derive(Serialize, Deserialize)]
struct UnivMonQWire {
    version: u8,
    config: UnivMonQConfig,
    levels: Vec<LevelWire>,
    count: u64,
    min: Option<u64>,
    max: Option<u64>,
    #[serde(default)]
    source_id: u64,
    #[serde(default)]
    next_sequence: u64,
    #[serde(default)]
    ordered_occurrences: Vec<OrderedOccurrence>,
    /// Version-1 distinct-key sample. It cannot be upgraded to an occurrence
    /// sample without the discarded occurrence identities.
    #[serde(default)]
    ordered_frequencies: Vec<(u64, u64)>,
}

impl UnivMonQ<DefaultXxHasher> {
    /// Creates an empty sketch using Sketchlib's default XXH3 hasher.
    pub fn new(config: UnivMonQConfig) -> Result<Self, UnivMonQError> {
        Self::with_hasher(config)
    }

    /// Creates a sketch with an explicit ID for its update source.
    ///
    /// Use a stable, globally unique partition or shard ID when sketches may
    /// be serialized or merged across processes.
    pub fn new_with_source_id(
        config: UnivMonQConfig,
        source_id: u64,
    ) -> Result<Self, UnivMonQError> {
        Self::with_hasher_and_source_id(config, source_id)
    }

    /// Serializes the default-hasher sketch to native MessagePack bytes.
    /// This is not yet an ASAPv1 cross-language wire kind.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let mut levels = Vec::with_capacity(self.levels.len());
        for level in &self.levels {
            let mut candidates: Vec<(u64, u64)> = level
                .candidate_scores
                .iter()
                .map(|(&key, &count)| (key, count))
                .collect();
            candidates.sort_unstable();
            levels.push(LevelWire {
                sketch: level.sketch.clone(),
                candidates,
                ever_evicted: Some(level.ever_evicted),
            });
        }
        let mut ordered_occurrences = self.ordered_heap.clone().into_vec();
        ordered_occurrences.sort_unstable();
        let wire = UnivMonQWire {
            version: WIRE_VERSION,
            config: self.config,
            levels,
            count: self.count,
            min: self.min,
            max: self.max,
            source_id: self.source_id,
            next_sequence: self.next_sequence,
            ordered_occurrences,
            ordered_frequencies: Vec::new(),
        };
        rmp_serde::to_vec_named(&wire)
    }

    /// Deserializes and validates native UnivMon-Q MessagePack state.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let wire: UnivMonQWire = rmp_serde::from_slice(bytes)?;
        if wire.version != 1 && wire.version != WIRE_VERSION {
            return Err(decode_error(format!(
                "unsupported UnivMon-Q wire version {}",
                wire.version
            )));
        }
        if !wire.ordered_frequencies.is_empty() {
            return Err(decode_error(
                "version-1 distinct ordered samples cannot be upgraded to occurrence samples",
            ));
        }
        let mut result = Self::new_with_source_id(wire.config, wire.source_id)
            .map_err(|error| decode_error(error.to_string()))?;
        if wire.levels.len() != result.config.levels {
            return Err(decode_error("UnivMon-Q level count does not match config"));
        }
        let valid_extrema = if wire.count == 0 {
            wire.min.is_none() && wire.max.is_none()
        } else {
            wire.min.is_some() && wire.max.is_some()
        };
        if !valid_extrema {
            return Err(decode_error(
                "UnivMon-Q count/min/max state is inconsistent",
            ));
        }
        if wire.min.zip(wire.max).is_some_and(|(min, max)| min > max) {
            return Err(decode_error("UnivMon-Q minimum exceeds maximum"));
        }
        for (index, (target, source)) in result.levels.iter_mut().zip(wire.levels).enumerate() {
            if !source.sketch.matches(
                level_width(result.config, index),
                result.config.depth,
                result.config.counter_bits,
            ) {
                return Err(decode_error(format!(
                    "UnivMon-Q CountSketch layout mismatch at level {index}"
                )));
            }
            if source.candidates.len() > result.config.candidates {
                return Err(decode_error(format!(
                    "UnivMon-Q candidate capacity exceeded at level {index}"
                )));
            }
            let candidate_len = source.candidates.len();
            let ever_evicted = source
                .ever_evicted
                .unwrap_or(candidate_len == result.config.candidates);
            let candidates: HashMap<u64, u64> = source.candidates.into_iter().collect();
            if candidates.len() != candidate_len {
                return Err(decode_error("duplicate UnivMon-Q candidate keys"));
            }
            target.sketch = source.sketch;
            target.candidate_scores = candidates;
            target.ever_evicted = ever_evicted;
            target.candidate_heap.clear();
            for (&key, &count) in &target.candidate_scores {
                target.candidate_heap.push(Reverse((count, key)));
            }
        }
        for (index, level) in result.levels.iter().enumerate() {
            if level
                .candidate_scores
                .keys()
                .any(|key| result.sample_level(*key) != index)
            {
                return Err(decode_error(format!(
                    "UnivMon-Q candidate stored in the wrong terminal level {index}"
                )));
            }
        }
        if wire.ordered_occurrences.len() > result.config.ordered_samples {
            return Err(decode_error("UnivMon-Q ordered sample capacity exceeded"));
        }
        let occurrence_len = wire.ordered_occurrences.len();
        let unique_occurrences: HashSet<_> = wire.ordered_occurrences.iter().copied().collect();
        if unique_occurrences.len() != occurrence_len {
            return Err(decode_error("duplicate UnivMon-Q ordered occurrences"));
        }
        if result.config.ordered_samples == 0 && !wire.ordered_occurrences.is_empty() {
            return Err(decode_error(
                "UnivMon-Q has ordered state while ordered sampling is disabled",
            ));
        }
        result.count = wire.count;
        result.min = wire.min;
        result.max = wire.max;
        result.next_sequence = wire.next_sequence;
        result.ordered_heap = BinaryHeap::from(wire.ordered_occurrences);
        Ok(result)
    }
}

fn allocate_source_id() -> u64 {
    NEXT_SOURCE_ID
        .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("UnivMon-Q automatic source IDs exhausted")
}

fn validate_config(config: UnivMonQConfig) -> Result<(), UnivMonQError> {
    if !(2..=63).contains(&config.levels) {
        return Err(UnivMonQError::new("levels must be in 2..=63"));
    }
    if config.width == 0 {
        return Err(UnivMonQError::new("width must be positive"));
    }
    if config.depth == 0 || config.depth % 2 == 0 {
        return Err(UnivMonQError::new("depth must be a positive odd number"));
    }
    if !matches!(config.counter_bits, 32 | 64) {
        return Err(UnivMonQError::new("counter_bits must be 32 or 64"));
    }
    if config.candidates == 0 {
        return Err(UnivMonQError::new("candidates must be positive"));
    }
    Ok(())
}

fn level_width(config: UnivMonQConfig, level: usize) -> usize {
    let shift = if config.width_halving_period == 0 {
        0
    } else {
        level as u32 / u32::from(config.width_halving_period)
    };
    let recovery_floor = config.candidates.min(config.width);
    config
        .width
        .checked_shr(shift)
        .unwrap_or(0)
        .max(recovery_floor)
}

#[inline(always)]
fn float_to_ordered(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 0 {
        bits ^ (1_u64 << 63)
    } else {
        !bits
    }
}

#[inline(always)]
fn ordered_to_float(value: u64) -> f64 {
    let bits = if value >> 63 == 1 {
        value ^ (1_u64 << 63)
    } else {
        !value
    };
    f64::from_bits(bits)
}

fn estimate_g_sum_from<F>(heavy: &[Vec<RecoveredCandidate>], g: F) -> f64
where
    F: Fn(f64) -> f64,
{
    estimate_keyed_sum_from(heavy, |_, frequency| g(frequency))
}

fn estimate_keyed_sum_from<F>(heavy: &[Vec<RecoveredCandidate>], g: F) -> f64
where
    F: Fn(u64, f64) -> f64,
{
    let Some(last) = heavy.len().checked_sub(1) else {
        return 0.0;
    };
    let mut estimate: f64 = heavy[last]
        .iter()
        .map(|candidate| g(candidate.key, candidate.frequency))
        .sum();
    for level in (0..last).rev() {
        let correction: f64 = heavy[level]
            .iter()
            .map(|candidate| {
                let sign = if candidate.terminal == level {
                    1.0
                } else {
                    -1.0
                };
                sign * g(candidate.key, candidate.frequency)
            })
            .sum();
        estimate = 2.0 * estimate + correction;
    }
    estimate
}

fn isotonicize(points: &mut [(u64, f64)]) {
    #[derive(Clone, Copy)]
    struct Block {
        start: usize,
        end: usize,
        sum: f64,
        weight: f64,
    }
    let mut blocks: Vec<Block> = Vec::with_capacity(points.len());
    for (index, point) in points.iter().enumerate() {
        blocks.push(Block {
            start: index,
            end: index + 1,
            sum: point.1,
            weight: 1.0,
        });
        while blocks.len() >= 2 {
            let right = blocks[blocks.len() - 1];
            let left = blocks[blocks.len() - 2];
            if left.sum / left.weight <= right.sum / right.weight {
                break;
            }
            blocks.pop();
            blocks.pop();
            blocks.push(Block {
                start: left.start,
                end: right.end,
                sum: left.sum + right.sum,
                weight: left.weight + right.weight,
            });
        }
    }
    for block in blocks {
        let rank = (block.sum / block.weight).clamp(0.0, 1.0);
        for point in &mut points[block.start..block.end] {
            point.1 = rank;
        }
    }
}

fn decode_error(message: impl Into<String>) -> RmpDecodeError {
    RmpDecodeError::Uncategorized(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct LegacyLevelWire {
        sketch: PackedCountSketch,
        candidates: Vec<(u64, u64)>,
    }

    #[derive(Serialize)]
    struct LegacyUnivMonQWire {
        version: u8,
        config: UnivMonQConfig,
        levels: Vec<LegacyLevelWire>,
        count: u64,
        min: Option<u64>,
        max: Option<u64>,
        ordered_frequencies: Vec<(u64, u64)>,
    }

    fn tiny_config() -> UnivMonQConfig {
        UnivMonQConfig {
            levels: 8,
            width: 128,
            width_halving_period: 0,
            depth: 5,
            counter_bits: 64,
            candidates: 32,
            ordered_samples: 32,
            hash_seed: 9,
        }
    }

    #[test]
    fn candidate_eviction_history_distinguishes_full_from_truncated() {
        let mut level = Level::new(32, 1, 64, 2);
        level.update(1, 1, 5);
        level.update(2, 2, 5);
        assert!(!level.ever_evicted, "filling the table is not an eviction");

        level.update(3, 3, 5);
        assert!(level.ever_evicted);
        level.clear();
        assert!(!level.ever_evicted);
    }

    #[test]
    fn candidate_merge_uses_zero_error_for_full_complete_summary() {
        let mut left = Level::new(32, 1, 64, 2);
        for _ in 0..100 {
            left.update(1, 1, 5);
        }
        left.update(2, 2, 5);
        assert!(!left.ever_evicted);

        let mut right = Level::new(32, 1, 64, 2);
        right.update(3, 3, 5);
        right.update(3, 3, 5);
        left.merge(&right, 5, u128::from);

        assert!(left.ever_evicted, "the merged union exceeded capacity");
        assert_eq!(left.candidate_scores.get(&1), Some(&100));
        assert_eq!(left.candidate_scores.get(&3), Some(&2));
        assert!(!left.candidate_scores.contains_key(&2));
    }

    #[test]
    fn native_messagepack_preserves_candidate_eviction_history() {
        let config = UnivMonQConfig {
            levels: 2,
            candidates: 1,
            ..tiny_config()
        };
        let mut sketch = UnivMonQ::new(config).unwrap();
        for value in [1.0, 2.0, 3.0] {
            sketch.update(&value);
        }
        assert!(sketch.levels.iter().any(|level| level.ever_evicted));

        let bytes = sketch.serialize_to_bytes().unwrap();
        let decoded = UnivMonQ::deserialize_from_bytes(&bytes).unwrap();
        let original: Vec<bool> = sketch
            .levels
            .iter()
            .map(|level| level.ever_evicted)
            .collect();
        let restored: Vec<bool> = decoded
            .levels
            .iter()
            .map(|level| level.ever_evicted)
            .collect();
        assert_eq!(restored, original);
    }

    #[test]
    fn legacy_full_candidate_table_decodes_as_possibly_evicted() {
        let config = UnivMonQConfig {
            levels: 2,
            candidates: 1,
            ordered_samples: 0,
            ..tiny_config()
        };
        let mut sketch = UnivMonQ::new(config).unwrap();
        sketch.update(&7.0);
        let full_level = sketch
            .levels
            .iter()
            .position(|level| level.candidate_scores.len() == config.candidates)
            .unwrap();
        assert!(!sketch.levels[full_level].ever_evicted);

        let legacy = LegacyUnivMonQWire {
            version: WIRE_VERSION,
            config,
            levels: sketch
                .levels
                .iter()
                .map(|level| LegacyLevelWire {
                    sketch: level.sketch.clone(),
                    candidates: level
                        .candidate_scores
                        .iter()
                        .map(|(&key, &score)| (key, score))
                        .collect(),
                })
                .collect(),
            count: sketch.count,
            min: sketch.min,
            max: sketch.max,
            ordered_frequencies: Vec::new(),
        };
        let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        let decoded = UnivMonQ::deserialize_from_bytes(&bytes).unwrap();
        assert!(decoded.levels[full_level].ever_evicted);
    }

    #[test]
    fn l2_heavy_candidate_survives_a_diffuse_tail() {
        let config = UnivMonQConfig {
            levels: 2,
            width: 4096,
            depth: 5,
            counter_bits: 64,
            candidates: 32,
            ordered_samples: 0,
            ..tiny_config()
        };
        let mut sketch = UnivMonQ::new(config).unwrap();
        let terminal = 0;
        let target = (0_u64..)
            .find(|value| sketch.sample_level(float_to_ordered(*value as f64)) == terminal)
            .unwrap();
        for _ in 0..200 {
            sketch.add(&target);
        }

        let mut distinct_tail = 0_usize;
        for value in 1_u64.. {
            if value != target && sketch.sample_level(float_to_ordered(value as f64)) == terminal {
                sketch.add(&value);
                distinct_tail += 1;
                if distinct_tail == 50_000 {
                    break;
                }
            }
        }

        let exact_f2 = 200_f64.powi(2) + distinct_tail as f64;
        let l2_threshold = (exact_f2 / config.candidates as f64).sqrt();
        assert!(200.0 > 2.0 * l2_threshold);
        let target_key = float_to_ordered(target as f64);
        assert!(sketch.levels[terminal].ever_evicted);
        assert!(
            sketch.levels[terminal]
                .candidate_scores
                .contains_key(&target_key),
            "an item above twice the L2 threshold must remain recoverable"
        );
        let recovery = sketch.recover_candidates();
        assert!(
            sketch.logical_heavy_sets_from(&recovery)[terminal]
                .iter()
                .any(|candidate| candidate.key == target_key)
        );
    }

    #[test]
    fn exact_for_tiny_frequency_vector() {
        let mut sketch = UnivMonQ::new(tiny_config()).unwrap();
        for value in [5.0, 1.0, 2.0, 2.0, 9.0, 5.0, 5.0] {
            sketch.update(&value);
        }
        assert_eq!(sketch.rank(2.0), Some(3));
        assert_eq!(sketch.estimate_rank_universal(2.0), Some(3));
        assert_eq!(sketch.rank(5.0), Some(6));
        assert_eq!(sketch.quantile(0.5), Some(5.0));
        assert_eq!(sketch.quantile(0.0), Some(1.0));
        assert_eq!(sketch.quantile(1.0), Some(9.0));
        assert_eq!(sketch.estimate_frequency(5.0), 3);
        assert_eq!(sketch.estimate_distinct(), 4.0);
        assert_eq!(sketch.estimate_f2(), 15.0);
        assert_eq!(sketch.estimate_g_sum(|frequency| frequency), 7.0);
        assert_eq!(sketch.heavy_hitters(1), vec![(5.0, 3)]);

        let query = sketch.prepare_queries();
        assert_eq!(query.count(), sketch.count());
        assert_eq!(query.estimate_frequency(5.0), 3);
        assert_eq!(query.estimate_distinct(), 4.0);
        assert_eq!(query.estimate_f2(), 15.0);
        assert_eq!(query.estimate_g_sum(|frequency| frequency), 7.0);
        assert_eq!(query.heavy_hitters(1), vec![(5.0, 3)]);
        assert_eq!(query.rank(2.0), Some(3));
        assert_eq!(query.estimate_rank_universal(2.0), Some(3));
        assert_eq!(
            query.quantiles(&[0.0, 0.5, 1.0]),
            vec![Some(1.0), Some(5.0), Some(9.0)]
        );
        assert_eq!(query.cdf(), sketch.cdf());
    }

    #[test]
    fn data_input_api_rejects_non_numeric_values() {
        let mut sketch = UnivMonQ::new(tiny_config()).unwrap();
        sketch.update_data_input(&DataInput::I64(-10)).unwrap();
        sketch.update_data_input(&DataInput::F32(2.5)).unwrap();
        assert!(sketch.update_data_input(&DataInput::Str("no")).is_err());
        assert_eq!(sketch.count(), 2);
    }

    #[test]
    fn float_encoding_matches_total_order() {
        let values = [
            f64::NEG_INFINITY,
            -10.0,
            -0.0,
            0.0,
            10.0,
            f64::INFINITY,
            f64::NAN,
        ];
        let encoded: Vec<u64> = values
            .iter()
            .map(|value| float_to_ordered(*value))
            .collect();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        for value in values {
            assert_eq!(
                ordered_to_float(float_to_ordered(value)).to_bits(),
                value.to_bits()
            );
        }
    }

    #[test]
    fn merge_matches_one_pass_for_complete_candidates() {
        let values = [5.0, 1.0, 9.0, 5.0, 2.0, 100.0, 2.0, 5.0, -7.0];
        let mut one_pass = UnivMonQ::new(tiny_config()).unwrap();
        for value in values {
            one_pass.update(&value);
        }
        let mut left = UnivMonQ::new(tiny_config()).unwrap();
        let mut right = UnivMonQ::new(tiny_config()).unwrap();
        for value in &values[..4] {
            left.update(value);
        }
        for value in &values[4..] {
            right.update(value);
        }
        left.merge(&right).unwrap();
        assert_eq!(left.count(), one_pass.count());
        assert_eq!(left.cdf(), one_pass.cdf());
        assert_eq!(left.estimate_f2(), one_pass.estimate_f2());
    }

    #[test]
    fn prepared_queries_match_direct_queries_after_candidate_eviction() {
        let config = UnivMonQConfig {
            candidates: 8,
            ordered_samples: 64,
            ..tiny_config()
        };
        let mut sketch = UnivMonQ::new(config).unwrap();
        for index in 0..5_000_u64 {
            let value = if index % 5 == 0 {
                42.0
            } else {
                (index.wrapping_mul(6364136223846793005) % 997) as f64
            };
            sketch.update(&value);
        }

        let direct_f0 = sketch.estimate_distinct();
        let direct_f2 = sketch.estimate_f2();
        let direct_l1 = sketch.estimate_g_sum(|frequency| frequency);
        let direct_entropy = sketch.estimate_entropy();
        let direct_heavy = sketch.heavy_hitters(5);
        let direct_rank = sketch.rank(500.0);
        let direct_universal_rank = sketch.estimate_rank_universal(500.0);
        let direct_quantiles = sketch.quantiles(&[0.1, 0.5, 0.9, 0.99]);
        let direct_cdf = sketch.cdf();

        let query = sketch.prepare_queries();
        assert_eq!(query.estimate_distinct(), direct_f0);
        assert_eq!(query.estimate_f2(), direct_f2);
        assert_eq!(query.estimate_g_sum(|frequency| frequency), direct_l1);
        assert_eq!(query.estimate_entropy(), direct_entropy);
        assert_eq!(query.heavy_hitters(5), direct_heavy);
        assert_eq!(query.rank(500.0), direct_rank);
        assert_eq!(query.estimate_rank_universal(500.0), direct_universal_rank);
        assert_eq!(query.quantiles(&[0.1, 0.5, 0.9, 0.99]), direct_quantiles);
        assert_eq!(query.cdf(), direct_cdf);
    }

    #[test]
    fn native_messagepack_round_trip_preserves_queries() {
        let mut sketch = UnivMonQ::new(tiny_config()).unwrap();
        for value in (0..1000).map(|value| (value % 97) as f64) {
            sketch.update(&value);
        }
        let bytes = sketch.serialize_to_bytes().unwrap();
        let decoded = UnivMonQ::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.config(), sketch.config());
        assert_eq!(decoded.source_id(), sketch.source_id());
        assert_eq!(decoded.count(), sketch.count());
        assert_eq!(decoded.cdf(), sketch.cdf());
        assert_eq!(decoded.estimate_f2(), sketch.estimate_f2());
    }

    #[test]
    fn occurrence_sample_merge_is_associative() {
        fn shard(source_id: u64, offset: u64) -> UnivMonQ {
            let mut sketch = UnivMonQ::new_with_source_id(tiny_config(), source_id).unwrap();
            for index in 0..1_000_u64 {
                sketch.add(&((index.wrapping_mul(17) + offset) % 991));
            }
            sketch
        }

        let (a, b, c) = (shard(10, 0), shard(20, 1), shard(30, 2));
        let mut left_associative = a.clone();
        left_associative.merge(&b).unwrap();
        left_associative.merge(&c).unwrap();

        let mut right_branch = b;
        right_branch.merge(&c).unwrap();
        let mut right_associative = a;
        right_associative.merge(&right_branch).unwrap();

        let mut left_sample = left_associative.ordered_heap.clone().into_sorted_vec();
        let mut right_sample = right_associative.ordered_heap.clone().into_sorted_vec();
        left_sample.sort_unstable();
        right_sample.sort_unstable();
        assert_eq!(left_sample, right_sample);
        assert_eq!(left_associative.cdf(), right_associative.cdf());
    }

    #[test]
    fn legacy_distinct_ordered_sample_is_rejected() {
        let legacy = LegacyUnivMonQWire {
            version: 1,
            config: tiny_config(),
            levels: Vec::new(),
            count: 1,
            min: Some(float_to_ordered(1.0)),
            max: Some(float_to_ordered(1.0)),
            ordered_frequencies: vec![(float_to_ordered(1.0), 1)],
        };
        let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        assert!(UnivMonQ::deserialize_from_bytes(&bytes).is_err());
    }

    #[test]
    fn synthetic_uniform_quantiles_have_small_rank_error() {
        let config = UnivMonQConfig {
            levels: 10,
            width: 2048,
            width_halving_period: 5,
            depth: 3,
            counter_bits: 32,
            candidates: 256,
            ordered_samples: 512,
            hash_seed: 5,
        };
        let mut sketch = UnivMonQ::new(config).unwrap();
        let n = 100_000_u64;
        for value in 0..n {
            sketch.update(&(value as f64));
        }
        for q in [0.01, 0.1, 0.5, 0.9, 0.99] {
            let estimate = sketch.quantile(q).unwrap();
            let normalized_rank_error = ((estimate + 1.0) / n as f64 - q).abs();
            assert!(
                normalized_rank_error < 0.03,
                "q={q}, estimate={estimate}, error={normalized_rank_error}"
            );
        }
    }

    #[test]
    fn eight_way_merge_retains_multi_metric_accuracy() {
        let config = UnivMonQConfig {
            levels: 10,
            width: 4096,
            width_halving_period: 7,
            depth: 3,
            counter_bits: 64,
            candidates: 512,
            ordered_samples: 1024,
            hash_seed: 5,
        };
        let mut shards: Vec<UnivMonQ> = (0..8).map(|_| UnivMonQ::new(config).unwrap()).collect();
        let mut truth = Vec::with_capacity(100_000);
        let mut exact_frequencies = HashMap::new();
        for index in 0..100_000_u64 {
            let value = if index % 5 < 2 {
                42.0
            } else {
                ((index.wrapping_mul(6364136223846793005) >> 17) % 20_000) as f64
            };
            shards[index as usize % 8].update(&value);
            truth.push(value);
            *exact_frequencies
                .entry(float_to_ordered(value))
                .or_insert(0_u64) += 1;
        }
        while shards.len() > 1 {
            let mut next = Vec::with_capacity(shards.len().div_ceil(2));
            let mut iter = shards.into_iter();
            while let Some(mut left) = iter.next() {
                if let Some(right) = iter.next() {
                    left.merge(&right).unwrap();
                }
                next.push(left);
            }
            shards = next;
        }
        let merged = shards.pop().unwrap();
        truth.sort_unstable_by(f64::total_cmp);
        for q in [0.01, 0.1, 0.5, 0.9, 0.99] {
            let estimate = merged.quantile(q).unwrap();
            let lower = truth.partition_point(|value| value.total_cmp(&estimate).is_lt()) as f64
                / truth.len() as f64;
            let upper = truth.partition_point(|value| value.total_cmp(&estimate).is_le()) as f64
                / truth.len() as f64;
            let error = if q < lower {
                lower - q
            } else if q > upper {
                q - upper
            } else {
                0.0
            };
            assert!(error < 0.03, "q={q}, estimate={estimate}, error={error}");
        }
        let exact_f0 = exact_frequencies.len() as f64;
        let exact_f2: f64 = exact_frequencies
            .values()
            .map(|frequency| (*frequency as f64).powi(2))
            .sum();
        assert!((merged.estimate_distinct() / exact_f0 - 1.0).abs() < 0.1);
        assert!((merged.estimate_f2() / exact_f2 - 1.0).abs() < 0.1);
        assert_eq!(merged.heavy_hitters(1)[0].0, 42.0);
    }

    #[test]
    fn clear_retains_configuration() {
        let mut sketch = UnivMonQ::new(tiny_config()).unwrap();
        sketch.update(&1.0);
        sketch.clear();
        assert!(sketch.is_empty());
        assert_eq!(sketch.config(), tiny_config());
        assert_eq!(sketch.quantile(0.5), None);
    }
}
