//! DDSketch quantile sketch implementation.
//!
//! A mergeable, relative-error quantile sketch that maps values into
//! logarithmically-spaced buckets, guaranteeing a relative accuracy of alpha
//! for every quantile query.
//!
//! Provenance:
//! This file was adapted from earlier DDSketch work in the private
//! `approx-telemetry/asap_sketchlib` repository. Original contributor for that
//! implementation: Srinath Ramachandran. It was later migrated and modified in
//! this repository.
//!
//! Reference:
//! - Masson, Rim & Lee, "DDSketch: A Fast and Fully-Mergeable Quantile Sketch
//!   with Relative-Error Guarantees," PVLDB 12(12), 2019.
//!   <https://www.vldb.org/pvldb/vol12/p2195-masson.pdf>

use crate::DataInput;
use crate::asapv1::admission_rate::{self, admit_all};
use crate::common::input::data_input_to_f64;
use crate::common::numerical::NumericalValue;
use crate::common::structures::Vector1D;
use crate::octo_delta::{DdSign, DdSketchBucketDelta};

/// The ASAPv1 DDSketch delta (kind_id `0x05 0x81`).
mod delta;
/// ASAPv1 wire serialization (kind_id `0x05 0x01`).
mod wire;

pub use delta::DdSketchDelta;

// Number of buckets to grow by when expanding.
const GROW_CHUNK: usize = 128;

#[derive(Debug, Clone)]
struct Buckets {
    counts: Vector1D<u64>,
    offset: i32,
}

impl Default for Buckets {
    fn default() -> Self {
        Self::new()
    }
}

impl Buckets {
    fn new() -> Self {
        Self {
            counts: Vector1D::from_vec(Vec::new()),
            offset: 0,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    #[inline(always)]
    fn range(&self) -> Option<(i32, i32)> {
        if self.counts.is_empty() {
            None
        } else {
            let left = self.offset;
            let right = self.offset + self.counts.len() as i32 - 1;
            Some((left, right))
        }
    }

    /// Ensure bucket k exists, using growth in chunks.
    #[inline(always)]
    fn ensure(&mut self, k: i32) {
        if self.counts.is_empty() {
            self.counts = Vector1D::from_vec(vec![0u64; GROW_CHUNK]);
            self.offset = k - (GROW_CHUNK as i32 / 2);
            return;
        }

        let (left, right) = self.range().unwrap();

        if k < left {
            let needed = (left - k) as usize;
            let grow = needed.max(GROW_CHUNK);

            let mut v = vec![0u64; grow];
            v.extend_from_slice(self.counts.as_slice());

            self.counts = Vector1D::from_vec(v);
            self.offset -= grow as i32;
        } else if k > right {
            let needed = (k - right) as usize;
            let grow = needed.max(GROW_CHUNK);

            let mut v = self.counts.clone().into_vec();
            v.resize(v.len() + grow, 0);
            self.counts = Vector1D::from_vec(v);
        }
    }

    #[inline(always)]
    fn add_one(&mut self, k: i32) {
        // this is the method that gets called on every sample insertion
        let idx_i32 = k - self.offset;

        if idx_i32 >= 0 {
            let idx = idx_i32 as usize;
            let slice = self.counts.as_mut_slice();
            if idx < slice.len() {
                slice[idx] += 1;
                return;
            }
        }

        // This is the method that gets called only on rare expansions
        self.ensure(k);
        let idx = (k - self.offset) as usize;
        self.counts.as_mut_slice()[idx] += 1;
    }
}

/// Mergeable, relative-error quantile sketch using logarithmically-spaced buckets.
///
/// ASAPv1 serialization lives in the `wire` submodule (kind_id `0x05 0x01`); it
/// carries `alpha`, both bucket stores and the zero count, and recovers `count`
/// by summing them.
///
/// The state is the buckets alone. A sum, minimum or maximum of the values is
/// not a DDSketch readout: keep it with a separate exact aggregate next to the
/// sketch.
#[derive(Debug)]
pub struct DdSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    inv_log_gamma: f64,

    positive_store: Buckets,
    count: u64,
    negative_store: Buckets,
    zero_count: u64,
    admission_rate: f64,
}

/// The state a decoded [`DdSketch`] is rebuilt from. `gamma` / `log_gamma` /
/// `inv_log_gamma` follow from `alpha` and `count` is the sum of the buckets.
struct DdSketchState {
    alpha: f64,
    positive_store: Buckets,
    negative_store: Buckets,
    zero_count: u64,
    admission_rate: f64,
}

impl TryFrom<DdSketchState> for DdSketch {
    type Error = String;

    fn try_from(state: DdSketchState) -> Result<Self, Self::Error> {
        let count = wire::check_state(&state)?;
        admission_rate::to_key(state.admission_rate).map_err(|e| format!("DDSketch {e}"))?;
        let gamma = (1.0 + state.alpha) / (1.0 - state.alpha);
        let log_gamma = gamma.ln();
        Ok(Self {
            alpha: state.alpha,
            gamma,
            log_gamma,
            inv_log_gamma: 1.0 / log_gamma,
            positive_store: state.positive_store,
            count,
            negative_store: state.negative_store,
            zero_count: state.zero_count,
            admission_rate: state.admission_rate,
        })
    }
}

/// Smallest and largest finite positive values whose bucket index is
/// representable without integer overflow (index within `i32`) or
/// `exp`/`powf` overflow, mirroring DataDog's logarithmic_mapping.go
/// `minIndexableValue`/`maxIndexableValue`. Values outside this range are
/// handled without growing the dense store: smaller magnitudes enter the
/// zero bucket, while larger magnitudes are rejected.
///
/// Single source of truth shared by `DdSketch` and tests.
pub fn ddsketch_indexable_bounds(alpha: f64) -> (f64, f64) {
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    let inv_log_gamma = 1.0 / gamma.ln();
    // 709.0 is just under ln(f64::MAX) so exp() stays finite.
    const EXP_OVERFLOW: f64 = 709.0;
    let min = ((f64::from(i32::MIN)) / inv_log_gamma + 1.0)
        .exp()
        .max(f64::MIN_POSITIVE * gamma);
    let max = ((f64::from(i32::MAX)) / inv_log_gamma - 1.0)
        .exp()
        .min(EXP_OVERFLOW.exp() / (2.0 * gamma) * (gamma + 1.0));
    (min, max)
}

impl DdSketch {
    /// Creates a new DDSketch with relative accuracy guarantee `alpha` (must be in `(0, 1)`).
    pub fn new(alpha: f64) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "alpha must be in (0,1)");
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let log_gamma = gamma.ln();
        let inv_log_gamma = 1.0 / log_gamma;

        Self {
            alpha,
            gamma,
            log_gamma,
            inv_log_gamma,
            positive_store: Buckets::new(),
            count: 0,
            negative_store: Buckets::new(),
            zero_count: 0,
            admission_rate: admit_all(),
        }
    }

    /// Sets the probability `p` with which each sample was admitted. `count`
    /// covers admitted samples alone; a consumer divides it by `p`.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// The probability with which each sample was admitted; `1` unless set.
    pub fn admission_rate(&self) -> f64 {
        self.admission_rate
    }

    /// Adds a finite signed sample. Tiny magnitudes enter the zero bucket.
    /// Invalid or too-large values are ignored by this compatibility API;
    /// use [`Self::try_add`] to receive an explicit error.
    pub fn add<T: NumericalValue>(&mut self, val: &T) {
        let _ = self.try_add(val);
    }

    /// Fallible update. Tiny magnitudes enter the zero bucket, matching DataDog;
    /// relative error is not guaranteed for nonzero values mapped to zero.
    pub fn try_add<T: NumericalValue>(&mut self, val: &T) -> Result<(), &'static str> {
        let v = val.to_f64();
        if !v.is_finite() {
            return Err("DDSketch requires finite values");
        }
        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(self.alpha);
        if v.abs() > max_indexable {
            return Err("DDSketch value exceeds indexable range");
        }
        self.count = self.count.checked_add(1).ok_or("DDSketch count overflow")?;
        if v.abs() < min_indexable {
            self.zero_count += 1;
        } else {
            let key = self.key_for(v.abs());
            if v < 0.0 {
                self.negative_store.add_one(key);
            } else {
                self.positive_store.add_one(key);
            }
        }
        Ok(())
    }

    /// Positive magnitude bucket index; use `signed_bucket_for` for signed samples.
    ///
    /// Exposed so an OctoSketch worker can hold one-byte counters over the same
    /// bucket space without duplicating the logarithmic mapping.
    pub fn bucket_index_for(&self, value: f64) -> Option<i32> {
        if !(value.is_finite() && value > 0.0) {
            return None;
        }
        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(self.alpha);
        if value < min_indexable || value > max_indexable {
            return None;
        }
        Some(self.key_for(value))
    }

    /// Select the signed store and magnitude index for an accepted observation.
    pub fn signed_bucket_for(&self, value: f64) -> Option<(DdSign, i32)> {
        let (min, max) = ddsketch_indexable_bounds(self.alpha);
        if !value.is_finite() || value.abs() > max {
            return None;
        }
        if value.abs() < min {
            return Some((DdSign::Zero, 0));
        }
        Some((
            if value < 0.0 {
                DdSign::Negative
            } else {
                DdSign::Positive
            },
            self.key_for(value.abs()),
        ))
    }

    /// Adds a promoted bucket count from an OctoSketch worker.
    ///
    /// The bucket and `count` change exactly as the same samples' `add`s
    /// would change them.
    pub fn apply_delta(&mut self, delta: DdSketchBucketDelta) {
        if delta.sign == DdSign::Zero {
            self.apply_zero_delta(delta.count);
            return;
        }
        if delta.count == 0 {
            return;
        }
        // `merge` checks that the two sketches share an alpha; a delta carries
        // no alpha to check, so bound the index by what this sketch's own
        // mapping can produce. A worker built with a much finer alpha would
        // otherwise hand over an index near i32::MAX and grow the dense store
        // across the whole gap. Out-of-range values are dropped, which is what
        // `add` already does with values it cannot index.
        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(self.alpha);
        let (lowest, highest) = (self.key_for(min_indexable), self.key_for(max_indexable));
        if delta.index < lowest || delta.index > highest {
            return;
        }
        let store = if delta.sign == DdSign::Negative {
            &mut self.negative_store
        } else {
            &mut self.positive_store
        };
        store.ensure(delta.index);
        let slot = (delta.index - store.offset) as usize;
        store.counts.as_mut_slice()[slot] += delta.count;
        self.count += delta.count;
    }

    /// Returns the estimated value at quantile `q`, or `None` if the sketch is
    /// empty or `q` is NaN: the representative of the bucket holding the
    /// order statistic at nearest rank `ceil(q * n)` (1-based). `q <= 0` reads
    /// the lowest occupied bucket and `q >= 1` the highest, so every answer,
    /// the extremes included, is within `alpha` relative of a sample when that
    /// sample's magnitude is indexable. A nonzero magnitude below the
    /// indexable range is counted as zero and reads `0.0`.
    pub fn get_value_at_quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || q.is_nan() {
            return None;
        }
        let rank = ((q * self.count as f64).ceil() as u64).clamp(1, self.count);
        Some(self.value_at_rank(rank - 1))
    }

    /// Returns the quantile at `q` interpolated linearly between the two
    /// adjacent order statistics at rank `q * (n - 1)` (0-based), the
    /// convention of PromQL `quantile_over_time` and SQL `percentile_cont`.
    /// [`Self::get_value_at_quantile`] keeps its nearest-rank convention.
    ///
    /// - `Ok(None)` for an empty sketch.
    /// - `Err` for `q` outside `[0, 1]`, NaN included; mapping such a `q` to
    ///   `-inf` / `+inf` (as PromQL does) is the caller's job.
    /// - Every order statistic, rank 0 and rank `n - 1` included, is its
    ///   bucket's representative (negative store, zero bucket or positive
    ///   store).
    ///
    /// Error: each neighbour is within `alpha` relative of the true order
    /// statistic when its magnitude is indexable, so when both neighbours are
    /// indexable and share a sign the result is within `alpha` relative of the
    /// true interpolated value. Across zero the bound is absolute:
    /// `alpha * ((1 - f) |lower| + f |upper|)` with `f` the fractional rank,
    /// which need not be small relative to a result near zero. Magnitudes
    /// below the indexable range are counted in the zero bucket and read as
    /// `0`. A sketch never holds NaN: [`Self::try_add`] refuses non-finite
    /// values. Ranks are computed in `f64`, exact up to `2^53` samples.
    pub fn quantile_interpolated(&self, q: f64) -> Result<Option<f64>, String> {
        if !(0.0..=1.0).contains(&q) {
            return Err(format!("DDSketch quantile {q} is outside [0, 1]"));
        }
        if self.count == 0 {
            return Ok(None);
        }
        let rank = q * (self.count - 1) as f64;
        let lower_rank = rank.floor() as u64;
        let upper_rank = (rank.ceil() as u64).min(self.count - 1);
        let fraction = rank - lower_rank as f64;
        let lower = self.value_at_rank(lower_rank);
        if fraction == 0.0 {
            return Ok(Some(lower));
        }
        let upper = self.value_at_rank(upper_rank);
        Ok(Some(lower * (1.0 - fraction) + upper * fraction))
    }

    /// The estimated order statistic at 0-based `rank` (`rank < count`): the
    /// representative of the bucket holding it, `0.0` in the zero bucket.
    fn value_at_rank(&self, rank: u64) -> f64 {
        debug_assert!(rank < self.count);
        // The last occupied bucket walked, returned if the buckets ever held
        // fewer samples than `count` says (no code path allows it).
        let mut last = 0.0;
        let mut seen = 0u64;
        let negative = self.negative_store.counts.as_slice();
        for (i, &count) in negative.iter().enumerate().rev() {
            if count == 0 {
                continue;
            }
            seen += count;
            last = -self.bin_representative(self.negative_store.offset + i as i32);
            if seen > rank {
                return last;
            }
        }
        if self.zero_count > 0 {
            seen += self.zero_count;
            last = 0.0;
            if seen > rank {
                return last;
            }
        }
        let positive = self.positive_store.counts.as_slice();
        for (i, &count) in positive.iter().enumerate() {
            if count == 0 {
                continue;
            }
            seen += count;
            last = self.bin_representative(self.positive_store.offset + i as i32);
            if seen > rank {
                return last;
            }
        }
        last
    }

    /// Returns the total number of samples inserted so far.
    pub fn get_count(&self) -> u64 {
        self.count
    }

    /// Returns the relative-accuracy parameter `alpha`.
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Bytes this sketch holds: the struct and the bucket slots both stores
    /// have allocated.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            + (self.positive_store.counts.capacity() + self.negative_store.counts.capacity())
                * size_of::<u64>()
    }

    /// Upper bound on [`Self::memory_bytes`] for a sketch of `alpha`, built or
    /// decoded, after `n` values with magnitudes in `[min_magnitude,
    /// max_magnitude]`, some negative when `negatives`, in any order and
    /// through merges. Saturates at `usize::MAX`.
    ///
    /// Each sign present holds one store: 128 slots for one value and, for
    /// more, at most `127 + s` slots for a span of `s <= 64` bucket indices,
    /// `2 * (s + 126)` above (less for two values). A NaN bound is unbounded;
    /// a negative `max_magnitude`, one below `min_magnitude`, or a range below
    /// the indexable one holds no buckets.
    ///
    /// Panics when `alpha` is outside `(0, 1)`, as [`Self::new`] does.
    pub fn estimated_memory_bytes(
        alpha: f64,
        n: usize,
        min_magnitude: f64,
        max_magnitude: f64,
        negatives: bool,
    ) -> usize {
        let sketch = Self::new(alpha);
        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(alpha);
        let low = min_magnitude.max(min_indexable);
        let high = max_magnitude.min(max_indexable);
        if n == 0 || low > high {
            return size_of::<Self>();
        }
        let span =
            usize::try_from(i64::from(sketch.key_for(high)) - i64::from(sketch.key_for(low)) + 1)
                .unwrap_or(usize::MAX);
        // Slots one store holds after `values` of the values; two values grow
        // the first 128-slot block once, or merge two blocks.
        let store_slots = |values: usize| match values {
            0 => 0,
            1 => GROW_CHUNK,
            _ if span <= GROW_CHUNK / 2 => GROW_CHUNK + span - 1,
            2 => (2 * GROW_CHUNK)
                .max(span.saturating_add(GROW_CHUNK - 1))
                .max(span.saturating_sub(GROW_CHUNK / 2 + 1).saturating_mul(2)),
            _ => span.saturating_add(GROW_CHUNK - 2).saturating_mul(2),
        };
        let slots = if negatives {
            // Past three values a store's bound stops growing.
            let n = n.min(6);
            (0..=n)
                .map(|values| store_slots(values).saturating_add(store_slots(n - values)))
                .max()
                .unwrap_or(0)
        } else {
            store_slots(n)
        };
        size_of::<Self>().saturating_add(slots.saturating_mul(size_of::<u64>()))
    }

    /// Returns the raw bucket-count slice. Each entry is the number of
    /// samples in the bucket whose absolute index is `positive_store_offset() + i`.
    pub fn positive_store_counts(&self) -> &[u64] {
        self.positive_store.counts.as_slice()
    }

    /// Returns the absolute bucket index corresponding to
    /// `positive_store_counts()[0]`.
    pub fn positive_store_offset(&self) -> i32 {
        self.positive_store.offset
    }

    pub fn negative_store_counts(&self) -> &[u64] {
        self.negative_store.counts.as_slice()
    }
    pub fn negative_store_offset(&self) -> i32 {
        self.negative_store.offset
    }
    pub fn zero_count(&self) -> u64 {
        self.zero_count
    }

    pub fn apply_zero_delta(&mut self, count: u64) {
        self.count += count;
        self.zero_count += count;
    }

    /// Merges another DDSketch into this one. Returns `Err` if the two sketches
    /// use different index mappings (different `alpha`/`gamma`): merging under a
    /// mismatched mapping would reinterpret one sketch's bucket indices under
    /// the other's γ and silently corrupt every quantile. Also returns `Err`
    /// when the admission rates differ.
    ///
    /// This is a REAL runtime check, not a `debug_assert!`: a `debug_assert!`
    /// compiles out in release builds, leaving a mismatched merge to corrupt
    /// results with no signal at all. DataDog's `MergeWith` and sketchlib-go's
    /// Go `Merge` both return an error here.
    pub fn merge(&mut self, other: &DdSketch) -> Result<(), String> {
        if (self.alpha - other.alpha).abs() >= 1e-12 || (self.gamma - other.gamma).abs() >= 1e-12 {
            return Err(format!(
                "cannot merge DDSketches with different index mappings: alpha {} vs {}",
                self.alpha, other.alpha
            ));
        }
        admission_rate::check_equal(self.admission_rate, other.admission_rate)
            .map_err(|e| format!("cannot merge DDSketches: {e}"))?;

        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other.clone();
            return Ok(());
        }

        self.count += other.count;

        // Merge bucket vectors
        Self::merge_store(&mut self.positive_store, &other.positive_store);
        Self::merge_store(&mut self.negative_store, &other.negative_store);
        self.zero_count += other.zero_count;
        Ok(())
    }

    #[inline(always)]
    fn key_for(&self, v: f64) -> i32 {
        debug_assert!(v > 0.0);
        (v.ln() * self.inv_log_gamma).floor() as i32
    }

    /// Lower edge γ^k of bucket k.
    #[inline]
    fn lower_bound(&self, k: i32) -> f64 {
        self.gamma.powf(k as f64)
    }

    /// Representative of bucket k: the lower bound γ^k scaled by (1+α), matching
    /// DataDog's logarithmic_mapping.go `Value = LowerBound(index) * (1 +
    /// RelativeAccuracy())`. This makes the relative error EXACTLY α at both
    /// bucket edges, as the advertised α-accuracy guarantee requires.
    #[inline]
    fn bin_representative(&self, k: i32) -> f64 {
        self.lower_bound(k) * (1.0 + self.alpha)
    }

    fn merge_store(store: &mut Buckets, other: &Buckets) {
        if other.is_empty() {
            return;
        }
        if store.is_empty() {
            *store = other.clone();
            return;
        }

        let (self_l, self_r) = store.range().unwrap();
        let (other_l, other_r) = other.range().unwrap();

        let new_l = self_l.min(other_l);
        let new_r = self_r.max(other_r);
        let new_len = (new_r - new_l + 1) as usize;

        let mut merged = vec![0u64; new_len];

        // Copy self
        for (i, &c) in store.counts.as_slice().iter().enumerate() {
            let k = self_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        // Add other
        for (i, &c) in other.counts.as_slice().iter().enumerate() {
            let k = other_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        store.counts = Vector1D::from_vec(merged);
        store.offset = new_l;
    }
}

impl Clone for DdSketch {
    fn clone(&self) -> Self {
        Self {
            alpha: self.alpha,
            gamma: self.gamma,
            log_gamma: self.log_gamma,
            inv_log_gamma: self.inv_log_gamma,
            positive_store: self.positive_store.clone(),
            count: self.count,
            negative_store: self.negative_store.clone(),
            zero_count: self.zero_count,
            admission_rate: self.admission_rate,
        }
    }
}

impl DdSketch {
    /// Adds a sample converted from a [`DataInput`]; returns an error for non-numeric inputs.
    #[inline(always)]
    pub fn add_input(&mut self, v: &DataInput) -> Result<(), &'static str> {
        let value = data_input_to_f64(v).map_err(|_| "DDSketch only accepts numeric inputs")?;
        self.try_add(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact interpolated quantile of `values` at `q`: linear between the
    /// order statistics at ranks `floor(q (n - 1))` and `ceil(q (n - 1))`.
    fn exact_interpolated(values: &[f64], q: f64) -> f64 {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let rank = q * (sorted.len() - 1) as f64;
        let (lower, upper) = (rank.floor() as usize, rank.ceil() as usize);
        let fraction = rank - lower as f64;
        sorted[lower] * (1.0 - fraction) + sorted[upper] * fraction
    }

    fn sketch_of(alpha: f64, values: &[f64]) -> DdSketch {
        let mut sketch = DdSketch::new(alpha);
        for v in values {
            sketch.try_add(v).expect("finite, indexable value");
        }
        sketch
    }

    /// If `count` ever exceeded the buckets, a quantile reads the last occupied
    /// bucket instead of panicking.
    #[test]
    fn a_rank_past_the_buckets_reads_the_last_occupied_bucket() {
        let mut sketch = DdSketch::new(0.01);
        sketch.add(&-4.0);
        sketch.count += 5;
        assert_eq!(
            sketch.get_value_at_quantile(1.0),
            Some(-sketch.bin_representative(sketch.key_for(4.0)))
        );
        sketch.add(&8.0);
        let top = sketch.bin_representative(sketch.key_for(8.0));
        assert_eq!(sketch.get_value_at_quantile(1.0), Some(top));
        assert_eq!(sketch.quantile_interpolated(1.0), Ok(Some(top)));
    }

    #[test]
    fn quantile_interpolated_two_values() {
        let sketch = sketch_of(0.01, &[20.0, 40.0]);
        for (q, expected) in [(0.0, 20.0), (0.5, 30.0), (0.9, 38.0), (1.0, 40.0)] {
            let actual = sketch.quantile_interpolated(q).unwrap().unwrap();
            assert!(
                (actual - expected).abs() <= 0.01 * expected,
                "q={q}: {actual} against {expected}"
            );
        }
        // The nearest-rank readout keeps its convention: rank ceil(0.9 * 2) = 2,
        // the bucket holding 40.
        let nearest = sketch.get_value_at_quantile(0.9).unwrap();
        assert!((nearest - 40.0).abs() <= (0.01 + 1e-9) * 40.0, "{nearest}");
    }

    #[test]
    fn quantile_interpolated_one_value_and_empty() {
        let one = sketch_of(0.01, &[20.0]);
        let bucket = one.quantile_interpolated(0.0).unwrap().unwrap();
        assert!((bucket - 20.0).abs() <= (0.01 + 1e-9) * 20.0, "{bucket}");
        for q in [0.3, 1.0] {
            assert_eq!(one.quantile_interpolated(q).unwrap(), Some(bucket));
        }
        let empty = DdSketch::new(0.01);
        assert_eq!(empty.quantile_interpolated(0.5).unwrap(), None);
    }

    #[test]
    fn quantile_interpolated_refuses_q_outside_the_unit_interval() {
        let sketch = sketch_of(0.01, &[1.0, 2.0]);
        for q in [-0.1, 1.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(sketch.quantile_interpolated(q).is_err(), "q={q}");
        }
        assert!(DdSketch::new(0.01).quantile_interpolated(2.0).is_err());
    }

    #[test]
    fn quantile_interpolated_signed_values() {
        let alpha = 0.01;
        let values = [-40.0, -20.0, -5.0, 0.0, 10.0, 30.0, 70.0];
        let sketch = sketch_of(alpha, &values);
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        for i in 0..=100 {
            let q = f64::from(i) / 100.0;
            let actual = sketch.quantile_interpolated(q).unwrap().unwrap();
            let expected = exact_interpolated(&values, q);
            let rank = q * (values.len() - 1) as f64;
            let fraction = rank - rank.floor();
            let (lower, upper) = (sorted[rank.floor() as usize], sorted[rank.ceil() as usize]);
            // Each neighbour is within alpha of its own magnitude, so the
            // result is within alpha of the weighted magnitudes; that is a
            // relative bound when both neighbours share a sign.
            let bound = alpha * ((1.0 - fraction) * lower.abs() + fraction * upper.abs());
            assert!(
                (actual - expected).abs() <= bound + 1e-12,
                "q={q}: {actual} against {expected}, bound {bound}"
            );
            if lower * upper > 0.0 {
                assert!((actual - expected).abs() <= alpha * expected.abs() + 1e-12);
            }
        }
    }

    #[test]
    fn quantile_interpolated_matches_the_exact_value_on_a_large_stream() {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let alpha = 0.01;
        let mut rng = StdRng::seed_from_u64(0xDD5E_ED01);
        let values: Vec<f64> = (0..20_000)
            .map(|_| {
                let magnitude = (rng.random_range(-3.0..6.0_f64)).exp();
                if rng.random_bool(0.2) {
                    -magnitude
                } else {
                    magnitude
                }
            })
            .collect();
        let sketch = sketch_of(alpha, &values);
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        for i in 0..=200 {
            let q = f64::from(i) / 200.0;
            let actual = sketch.quantile_interpolated(q).unwrap().unwrap();
            let expected = exact_interpolated(&values, q);
            let rank = q * (values.len() - 1) as f64;
            let fraction = rank - rank.floor();
            let (lower, upper) = (sorted[rank.floor() as usize], sorted[rank.ceil() as usize]);
            let bound = alpha * ((1.0 - fraction) * lower.abs() + fraction * upper.abs());
            assert!(
                (actual - expected).abs() <= bound * (1.0 + 1e-9) + 1e-12,
                "q={q}: {actual} against {expected}, bound {bound}"
            );
        }
    }

    #[test]
    fn insert_and_query_basic() {
        let mut s = DdSketch::new(0.01);
        let vals = [0.0, -5.0, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0];
        for &v in &vals {
            s.add(&v);
        }

        // Every finite sample is retained, including zero and negatives.
        assert_eq!(s.get_count(), 9);

        let ps = [0.0, 0.5, 0.9, 0.99, 1.0];
        let mut prev = f64::NEG_INFINITY;
        for &p in &ps {
            let q = s.get_value_at_quantile(p).expect("quantile");
            assert!(q >= prev - 1e-12, "non-monotone at p={p}: {q} < {prev}");
            assert!((-5.0 * 1.01..=1000.0 * 1.01).contains(&q), "p={p}: {q}");
            prev = q;
        }
    }

    #[test]
    fn empty_quantile_returns_none() {
        let s = DdSketch::new(0.01);
        assert!(s.get_value_at_quantile(0.5).is_none());
        assert!(s.get_value_at_quantile(0.0).is_none());
        assert!(s.get_value_at_quantile(1.0).is_none());
        assert_eq!(s.get_count(), 0);
    }

    #[test]
    fn merge_two_sketches_combines_counts_and_bounds() {
        const ALPHA: f64 = 0.01;

        let mut s1 = DdSketch::new(ALPHA);
        let mut s2 = DdSketch::new(ALPHA);

        let vals1 = [1.0, 2.0, 3.0, 4.0];
        let vals2 = [5.0, 10.0, 20.0];

        for v in vals1 {
            s1.add(&v);
        }
        for v in vals2 {
            s2.add(&v);
        }

        s1.merge(&s2).unwrap();

        assert_eq!(s1.get_count(), (vals1.len() + vals2.len()) as u64);

        // The extreme quantiles read the lowest and highest buckets, within
        // alpha of the smallest and largest samples.
        let lowest = s1.get_value_at_quantile(0.0).unwrap();
        let highest = s1.get_value_at_quantile(1.0).unwrap();
        assert!((lowest - 1.0).abs() <= (ALPHA + 1e-9) * 1.0, "{lowest}");
        assert!((highest - 20.0).abs() <= (ALPHA + 1e-9) * 20.0, "{highest}");

        let mid = s1.get_value_at_quantile(0.5).unwrap();
        assert!((lowest..=highest).contains(&mid));
    }

    #[test]
    fn dds_serialization_round_trip() {
        let mut s = DdSketch::new(0.01);
        for v in [1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0] {
            s.add(&v);
        }

        let encoded = s.to_asapv1().expect("DDSketch serialization fail");
        assert!(
            !encoded.is_empty(),
            "encoded bytes should not be empty for DDSketch"
        );
        let decoded = DdSketch::from_asapv1(&encoded).expect("DDSketch deserialization fail");

        // `count` is summed back from the buckets, and every quantile reads
        // the same buckets, so both come back exact.
        assert_eq!(decoded.get_count(), s.get_count());
        for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
            assert_eq!(
                decoded.get_value_at_quantile(q),
                s.get_value_at_quantile(q),
                "quantile p={q} diverged after a round trip"
            );
        }
    }

    // DataDog-parity tests.

    #[test]
    fn representative_within_alpha_at_bucket_edges() {
        for &alpha in &[0.001, 0.01, 0.05, 0.1] {
            let d = DdSketch::new(alpha);
            for &k in &[-100i32, -1, 0, 1, 7, 500] {
                let lo = d.lower_bound(k);
                let hi = d.lower_bound(k + 1);
                let rep = d.bin_representative(k);
                assert!(rep >= lo && rep <= hi, "rep {rep} outside [{lo},{hi}]");
                assert!(
                    (rep - lo).abs() / lo <= alpha + 1e-9,
                    "alpha={alpha} k={k}: lower-edge relerr exceeds alpha"
                );
                assert!(
                    (rep - hi).abs() / hi <= alpha + 1e-9,
                    "alpha={alpha} k={k}: upper-edge relerr exceeds alpha"
                );
            }
        }
    }

    #[test]
    fn merge_alpha_mismatch_is_a_real_runtime_error() {
        let mut a = DdSketch::new(0.01);
        let b = DdSketch::new(0.02);
        a.add(&5.0);
        assert!(a.merge(&b).is_err(), "mismatched-mapping merge must Err");

        let mut c = DdSketch::new(0.01);
        let mut d = DdSketch::new(0.01);
        c.add(&3.0);
        d.add(&7.0);
        assert!(c.merge(&d).is_ok(), "matched-mapping merge must succeed");
        assert_eq!(c.get_count(), 2);
    }

    #[test]
    fn untrackable_extreme_is_dropped() {
        // A single finite-but-extreme outlier outside the indexable range must
        // not be recorded, so the dense bucket store never spans the whole gap.
        let mut d = DdSketch::new(0.01);
        for i in 1..=2000 {
            d.add(&(f64::from(i)));
        }
        let count_before = d.get_count();
        let span_before = d.positive_store.counts.as_slice().len();

        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(0.01);
        d.add(&(max_indexable * 10.0));
        d.add(&(min_indexable / 10.0));
        assert_eq!(
            d.get_count(),
            count_before + 1,
            "tiny value belongs to zero bucket"
        );
        assert_eq!(
            d.positive_store.counts.as_slice().len(),
            span_before,
            "store span grew from an untrackable extreme"
        );

        // A large-but-trackable value is still recorded.
        d.add(&(max_indexable / 2.0));
        assert_eq!(d.get_count(), count_before + 2);
    }
}
