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
use crate::views::{ViewError, ViewReader, ViewWriter};

/// The ASAPv1 DDSketch delta (kind_id `0x05 0x80`).
mod delta;
/// ASAPv1 wire serialization (kind_id `0x05 0x00`).
mod wire;

pub use delta::DdSketchDelta;

// Number of buckets to grow by when expanding.
const GROW_CHUNK: usize = 128;

/// The most buckets one store of a view may span (first to last index).
/// The owned sketch a view merges into holds its stores dense, so a view
/// spanning more would make a few bytes allocate gigabytes (a tiny alpha
/// puts samples a factor of two apart about `ln 2 / (2 alpha)` buckets
/// apart). 2^20 buckets is 8 MiB of `u64` counts. It covers any samples
/// between 1e-300 and 1e300 at alpha >= 1e-3, and samples spread by a
/// factor of 1e9 at alpha >= 1e-5; a sketch spread wider is refused.
const MAX_VIEW_BUCKET_SPAN: i64 = 1 << 20;

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

    /// Drops the empty slots at both ends and the spare capacity: the
    /// occupied range of bucket indices and its counts are unchanged.
    fn shrink_to_fit(&mut self) {
        let counts = self.counts.as_slice();
        let Some(first) = counts.iter().position(|&c| c != 0) else {
            self.counts = Vector1D::from_vec(Vec::new());
            self.offset = 0;
            return;
        };
        let last = counts
            .iter()
            .rposition(|&c| c != 0)
            .expect("a nonzero count");
        if first == 0 && last + 1 == counts.len() && counts.len() == self.counts.capacity() {
            return;
        }
        let kept = counts[first..=last].to_vec();
        self.offset += first as i32;
        self.counts = Vector1D::from_vec(kept);
    }
}

/// Mergeable, relative-error quantile sketch using logarithmically-spaced buckets.
///
/// ASAPv1 serialization lives in the `wire` submodule (kind_id `0x05 0x00`); it
/// carries `alpha`, both bucket stores, the zero count and `sum` / `min` /
/// `max`, and recovers `count` by summing them.
#[derive(Debug)]
pub struct DdSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    inv_log_gamma: f64,

    positive_store: Buckets,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    negative_store: Buckets,
    zero_count: u64,
    admission_rate: f64,
}

/// The state a decoded [`DdSketch`] is rebuilt from. `gamma` / `log_gamma` /
/// `inv_log_gamma` follow from `alpha` and `count` is the sum of the buckets.
struct DdSketchState {
    alpha: f64,
    positive_store: Buckets,
    sum: f64,
    min: f64,
    max: f64,
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
            sum: state.sum,
            min: state.min,
            max: state.max,
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
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            negative_store: Buckets::new(),
            zero_count: 0,
            admission_rate: admit_all(),
        }
    }

    /// Sets the probability `p` with which each sample was admitted. `count`
    /// and `sum` cover admitted samples alone; a consumer divides them by `p`.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// The probability with which each sample was admitted; `1` unless set.
    pub fn admission_rate(&self) -> f64 {
        self.admission_rate
    }

    /// Advances the running `sum` by `delta`, saturating at `f64::MAX` instead
    /// of reaching `+inf`. The wire format refuses a non-finite `sum` on a
    /// populated store, so an unguarded `+=` would leave a legally-ingested
    /// sketch that can never be serialized. `count` and the bucket store stay
    /// exact, so only `sum` degrades, and only past `f64::MAX`.
    #[inline(always)]
    fn add_to_sum(&mut self, delta: f64) {
        let next = self.sum + delta;
        self.sum = if next.is_finite() {
            next
        } else {
            f64::MAX.copysign(next)
        };
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
        self.add_to_sum(v);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
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
    /// A delta carries only a bucket and a count, so `sum`, `min` and `max` are
    /// advanced with the bucket's representative value - the same α-bounded
    /// estimate a deserialize-and-recompute produces. Quantiles and `count`
    /// stay exact with respect to the bucket store.
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

        let representative = self.bin_representative(delta.index)
            * if delta.sign == DdSign::Negative {
                -1.0
            } else {
                1.0
            };
        self.count += delta.count;
        self.add_to_sum(representative * delta.count as f64);
        if representative < self.min {
            self.min = representative;
        }
        if representative > self.max {
            self.max = representative;
        }
    }

    /// Returns the estimated value at quantile `q` (in `[0, 1]`), or `None` if the sketch is empty.
    pub fn get_value_at_quantile(&self, q: f64) -> Option<f64> {
        self.as_view().get_value_at_quantile(q)
    }

    /// Returns the quantile at `q` interpolated linearly between the two
    /// adjacent order statistics at rank `q * (n - 1)` (0-based), the
    /// convention of PromQL `quantile_over_time` and SQL `percentile_cont`.
    /// [`Self::get_value_at_quantile`] keeps its nearest-rank convention.
    ///
    /// - `Ok(None)` for an empty sketch.
    /// - `Err` for `q` outside `[0, 1]`, NaN included; mapping such a `q` to
    ///   `-inf` / `+inf` (as PromQL does) is the caller's job.
    /// - Rank 0 and rank `n - 1` are the exactly retained minimum and
    ///   maximum; every other order statistic is its bucket's representative
    ///   (negative store, zero bucket or positive store), clamped to
    ///   `[min, max]`.
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
        self.as_view().quantile_interpolated(q)
    }

    /// This sketch as a read-only view: the same answers, read in place.
    pub fn as_view(&self) -> DdSketchView<'_> {
        DdSketchView {
            alpha: self.alpha,
            gamma: self.gamma,
            count: self.count,
            sum: self.sum,
            min: self.min,
            max: self.max,
            zero_count: self.zero_count,
            admission_rate: self.admission_rate,
            positive: StoreRef::of(&self.positive_store),
            negative: StoreRef::of(&self.negative_store),
        }
    }

    /// This sketch in its zero-copy view layout (see [`crate::views`]):
    /// `DDV1`, version, `alpha`, `count`, `sum`, `min`, `max`, `zero_count`,
    /// `admission_rate`, the count width in bytes (`1`, `2`, `4` or `8`: the
    /// narrowest that holds every bucket count) and a `u32` that is `1` for
    /// the sparse form, `0` for the dense one; then per sign (positive,
    /// negative) `offset: i32` and `len: u32`; then per sign, dense: the
    /// counts of buckets `offset..offset + len`, or sparse: `[u32; len]`
    /// bucket indices less `offset` (increasing) and their counts. Only
    /// occupied buckets and the range between them are written; the form
    /// taking fewer bytes is chosen.
    pub fn to_view_bytes(&self) -> Vec<u8> {
        // Each store's occupied range.
        fn occupied(store: &Buckets) -> (i32, &[u64]) {
            let counts = store.counts.as_slice();
            match counts.iter().position(|&c| c != 0) {
                None => (0, &counts[..0]),
                Some(first) => {
                    let last = counts
                        .iter()
                        .rposition(|&c| c != 0)
                        .expect("a nonzero count");
                    (store.offset + first as i32, &counts[first..=last])
                }
            }
        }
        let (positive_offset, positive) = occupied(&self.positive_store);
        let (negative_offset, negative) = occupied(&self.negative_store);
        let largest = positive.iter().chain(negative).copied().max().unwrap_or(0);
        let width: usize = match largest {
            0..=0xff => 1,
            0x100..=0xffff => 2,
            0x1_0000..=0xffff_ffff => 4,
            _ => 8,
        };
        let nonzero = |counts: &[u64]| counts.iter().filter(|&&c| c != 0).count();
        let padded = |bytes: usize| bytes.div_ceil(8) * 8;
        let dense = padded(positive.len() * width) + padded(negative.len() * width);
        let sparse_entries = (nonzero(positive), nonzero(negative));
        let sparse = padded(sparse_entries.0 * 4)
            + padded(sparse_entries.0 * width)
            + padded(sparse_entries.1 * 4)
            + padded(sparse_entries.1 * width);
        let is_sparse = sparse < dense;
        let mut writer = ViewWriter::new(DD_VIEW_MAGIC);
        writer
            .f64(self.alpha)
            .u64(self.count)
            .f64(self.sum)
            .f64(self.min)
            .f64(self.max)
            .u64(self.zero_count)
            .f64(self.admission_rate)
            .u32(width as u32)
            .u32(u32::from(is_sparse))
            .i32(positive_offset)
            .u32(if is_sparse {
                sparse_entries.0
            } else {
                positive.len()
            } as u32)
            .i32(negative_offset)
            .u32(if is_sparse {
                sparse_entries.1
            } else {
                negative.len()
            } as u32);
        for counts in [positive, negative] {
            let kept: Vec<u64> = if is_sparse {
                let keys: Vec<u32> = counts
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| **c != 0)
                    .map(|(i, _)| i as u32)
                    .collect();
                writer.u32s(&keys);
                counts.iter().copied().filter(|&c| c != 0).collect()
            } else {
                counts.to_vec()
            };
            match width {
                1 => writer.u8s(&kept.iter().map(|&c| c as u8).collect::<Vec<_>>()),
                2 => writer.u16s(&kept.iter().map(|&c| c as u16).collect::<Vec<_>>()),
                4 => writer.u32s(&kept.iter().map(|&c| c as u32).collect::<Vec<_>>()),
                _ => writer.u64s(&kept),
            };
        }
        writer.finish()
    }

    /// Returns the total number of samples inserted so far.
    pub fn get_count(&self) -> u64 {
        self.count
    }

    /// Returns the minimum sample seen, or `None` if the sketch is empty.
    pub fn min(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.min)
        }
    }

    /// Returns the maximum sample seen, or `None` if the sketch is empty.
    pub fn max(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.max)
        }
    }

    /// Returns the relative-accuracy parameter `alpha`.
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Frees what this sketch holds only for later inserts: the empty
    /// bucket slots at both ends of each store and their spare capacity
    /// (stores grow 128 slots at a time). For a sketch that stops receiving
    /// inserts, such as the sketch of a closed window kept for queries.
    ///
    /// Every bucket count, the count, sum, minimum and maximum are
    /// unchanged, so every answer and merge is the same as before; the
    /// sketch stays usable and grows again on a later insert. Its ASAPv1
    /// bytes are shorter (the stores' empty end slots are not written), and
    /// decode to a sketch with the same answers.
    pub fn shrink_to_fit(&mut self) {
        self.positive_store.shrink_to_fit();
        self.negative_store.shrink_to_fit();
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

    /// Returns the running sum. Exact over the values passed to [`Self::add`];
    /// a bucket promoted through [`Self::apply_delta`] contributes its bucket
    /// representative instead, and the total saturates at `f64::MAX` rather
    /// than overflowing to `+inf`.
    pub fn sum(&self) -> f64 {
        self.sum
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
        if count != 0 {
            self.min = self.min.min(0.0);
            self.max = self.max.max(0.0);
        }
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
        self.merge_view(&other.as_view())
    }

    /// Merges a view (a stored sketch read in place) into this one, as
    /// [`Self::merge`] merges an owned sketch: the same checks, the same
    /// answers. Allocates only when this sketch's stores must grow.
    ///
    /// A view holds only each store's occupied buckets, so the merged
    /// stores (and so [`Self::to_view_bytes`]) equal a merge of the view's
    /// owned sketch after [`Self::shrink_to_fit`], which can differ from a merge of the
    /// original sketch only in empty slots at a store's ends.
    pub fn merge_view(&mut self, other: &DdSketchView<'_>) -> Result<(), String> {
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
            self.count = other.count;
            self.sum = other.sum;
            self.min = other.min;
            self.max = other.max;
            self.zero_count = other.zero_count;
            self.positive_store = other.positive.to_buckets();
            self.negative_store = other.negative.to_buckets();
            return Ok(());
        }

        self.count += other.count;
        self.add_to_sum(other.sum);
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Merge bucket vectors
        Self::merge_store(&mut self.positive_store, other.positive);
        Self::merge_store(&mut self.negative_store, other.negative);
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

    /// Adds `other`'s counts to `store` in place, growing `store` (once, to
    /// the union of both ranges) only when `other` reaches past it.
    fn merge_store(store: &mut Buckets, other: StoreRef<'_>) {
        if other.is_empty() {
            return;
        }
        if store.is_empty() {
            *store = other.to_buckets();
            return;
        }
        let (self_l, self_r) = store.range().unwrap();
        let (other_l, other_r) = other.range();
        if other_l < self_l || other_r > self_r {
            let new_l = self_l.min(other_l);
            let new_r = self_r.max(other_r);
            let mut merged = vec![0u64; (new_r - new_l + 1) as usize];
            let at = (self_l - new_l) as usize;
            let old = store.counts.as_slice();
            merged[at..at + old.len()].copy_from_slice(old);
            store.counts = Vector1D::from_vec(merged);
            store.offset = new_l;
        }
        other.add_into(store.counts.as_mut_slice(), store.offset);
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
            sum: self.sum,
            min: self.min,
            max: self.max,
            negative_store: self.negative_store.clone(),
            zero_count: self.zero_count,
            admission_rate: self.admission_rate,
        }
    }
}

const DD_VIEW_MAGIC: [u8; 4] = *b"DDV1";

/// Bucket counts, borrowed at the width they are stored in: the narrowest
/// of `u8`, `u16`, `u32` and `u64` that holds every count.
#[derive(Debug, Clone, Copy)]
enum Counts<'a> {
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl Counts<'_> {
    fn len(&self) -> usize {
        match self {
            Self::U8(counts) => counts.len(),
            Self::U16(counts) => counts.len(),
            Self::U32(counts) => counts.len(),
            Self::U64(counts) => counts.len(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> u64 {
        match self {
            Self::U8(counts) => u64::from(counts[index]),
            Self::U16(counts) => u64::from(counts[index]),
            Self::U32(counts) => u64::from(counts[index]),
            Self::U64(counts) => counts[index],
        }
    }

    /// The sum of the counts, `None` on overflow. One loop per width, with
    /// no per-element dispatch.
    fn total(&self) -> Option<u64> {
        // Under 2^20 entries (the span limit) of at most u32::MAX each, a
        // narrow store's sum fits a u64.
        fn widened<T: Copy + Into<u64>>(counts: &[T]) -> u64 {
            counts.iter().map(|&c| c.into()).sum()
        }
        match self {
            Self::U8(counts) => Some(widened(counts)),
            Self::U16(counts) => Some(widened(counts)),
            Self::U32(counts) => Some(widened(counts)),
            Self::U64(counts) => counts
                .iter()
                .try_fold(0u64, |total, &c| total.checked_add(c)),
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::U8(_) => 1,
            Self::U16(_) => 2,
            Self::U32(_) => 4,
            Self::U64(_) => 8,
        }
    }
}

/// One sign's bucket store, borrowed. Dense (`keys` is `None`): bucket
/// `offset + i` holds count `i`. Sparse: bucket `offset + keys[i]` holds
/// count `i`, keys strictly increasing, and every other bucket is empty.
#[derive(Debug, Clone, Copy)]
struct StoreRef<'a> {
    offset: i32,
    counts: Counts<'a>,
    keys: Option<&'a [u32]>,
}

/// The `(bucket, count)` pairs of a [`StoreRef`], from either end.
struct BucketIter<'a, 'b> {
    store: &'b StoreRef<'a>,
    front: usize,
    back: usize,
}

impl Iterator for BucketIter<'_, '_> {
    type Item = (i32, u64);

    fn next(&mut self) -> Option<(i32, u64)> {
        (self.front < self.back).then(|| {
            self.front += 1;
            self.store.bucket(self.front - 1)
        })
    }
}

impl DoubleEndedIterator for BucketIter<'_, '_> {
    fn next_back(&mut self) -> Option<(i32, u64)> {
        (self.front < self.back).then(|| {
            self.back -= 1;
            self.store.bucket(self.back)
        })
    }
}

impl<'a> StoreRef<'a> {
    fn of(store: &'a Buckets) -> Self {
        Self {
            offset: store.offset,
            counts: Counts::U64(store.counts.as_slice()),
            keys: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.counts.len() == 0
    }

    /// Entry `i`: its bucket index and count. The index fits an `i32`
    /// (checked when the view was read).
    #[inline]
    fn bucket(&self, i: usize) -> (i32, u64) {
        let key = match self.keys {
            Some(keys) => keys[i] as i32,
            None => i as i32,
        };
        (self.offset + key, self.counts.get(i))
    }

    /// Adds every count to `dst`, whose first slot is bucket `dst_offset`
    /// and which covers this store's range. One loop per width and form:
    /// a dense store adds a slice, a sparse one scatters by key.
    fn add_into(&self, dst: &mut [u64], dst_offset: i32) {
        fn dense<T: Copy + Into<u64>>(dst: &mut [u64], src: &[T]) {
            for (d, &c) in dst[..src.len()].iter_mut().zip(src) {
                *d += c.into();
            }
        }
        // `base` is this store's offset in `dst`: at or above 0 for a dense
        // store; for a sparse one, base + its first key is.
        fn sparse<T: Copy + Into<u64>>(dst: &mut [u64], base: i64, keys: &[u32], src: &[T]) {
            for (&k, &c) in keys.iter().zip(src) {
                dst[(base + i64::from(k)) as usize] += c.into();
            }
        }
        let base = i64::from(self.offset) - i64::from(dst_offset);
        match (self.keys, self.counts) {
            (None, Counts::U8(c)) => dense(&mut dst[base as usize..], c),
            (None, Counts::U16(c)) => dense(&mut dst[base as usize..], c),
            (None, Counts::U32(c)) => dense(&mut dst[base as usize..], c),
            (None, Counts::U64(c)) => dense(&mut dst[base as usize..], c),
            (Some(k), Counts::U8(c)) => sparse(dst, base, k, c),
            (Some(k), Counts::U16(c)) => sparse(dst, base, k, c),
            (Some(k), Counts::U32(c)) => sparse(dst, base, k, c),
            (Some(k), Counts::U64(c)) => sparse(dst, base, k, c),
        }
    }

    fn buckets<'b>(&'b self) -> BucketIter<'a, 'b> {
        BucketIter {
            store: self,
            front: 0,
            back: self.counts.len(),
        }
    }

    /// The first and last bucket index, for a non-empty store.
    fn range(&self) -> (i32, i32) {
        (self.bucket(0).0, self.bucket(self.counts.len() - 1).0)
    }

    fn to_buckets(self) -> Buckets {
        if self.is_empty() {
            return Buckets::new();
        }
        let (first, last) = self.range();
        let mut counts = vec![0u64; (last - first) as usize + 1];
        self.add_into(&mut counts, first);
        Buckets {
            counts: Vector1D::from_vec(counts),
            offset: first,
        }
    }
}

/// A read-only DDSketch over borrowed bucket counts: a stored sketch used
/// in place (from [`DdSketch::to_view_bytes`] bytes), or an owned one
/// ([`DdSketch::as_view`]). Answers exactly as the owned sketch.
#[derive(Debug, Clone, Copy)]
pub struct DdSketchView<'a> {
    alpha: f64,
    gamma: f64,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    zero_count: u64,
    admission_rate: f64,
    positive: StoreRef<'a>,
    negative: StoreRef<'a>,
}

impl<'a> DdSketchView<'a> {
    /// The view of bytes written by [`DdSketch::to_view_bytes`], borrowed
    /// in place; `bytes` must start 8-byte aligned.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, ViewError> {
        let mut reader = ViewReader::new(bytes, DD_VIEW_MAGIC)?;
        let alpha = reader.f64()?;
        if !(alpha > 0.0 && alpha < 1.0) {
            return Err(ViewError(format!(
                "DDSketch alpha {alpha} is outside (0, 1)"
            )));
        }
        let count = reader.u64()?;
        let sum = reader.f64()?;
        let min = reader.f64()?;
        let max = reader.f64()?;
        let zero_count = reader.u64()?;
        let admission_rate = reader.f64()?;
        let width = reader.u32()?;
        let sparse = match reader.u32()? {
            0 => false,
            1 => true,
            other => return Err(ViewError(format!("DDSketch store form {other}"))),
        };
        let positive_offset = reader.i32()?;
        let positive_len = reader.u32()? as usize;
        let negative_offset = reader.i32()?;
        let negative_len = reader.u32()? as usize;
        let mut store = |offset: i32, len: usize| -> Result<StoreRef<'a>, ViewError> {
            let keys = if sparse {
                Some(reader.u32s(len)?)
            } else {
                None
            };
            let counts = match width {
                1 => Counts::U8(reader.u8s(len)?),
                2 => Counts::U16(reader.u16s(len)?),
                4 => Counts::U32(reader.u32s(len)?),
                8 => Counts::U64(reader.u64s(len)?),
                other => return Err(ViewError(format!("DDSketch count width {other}"))),
            };
            // Every bucket index fits an i32; sparse keys increase.
            let last_key = match keys {
                Some(keys) => {
                    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                        return Err(ViewError("DDSketch sparse keys not increasing".into()));
                    }
                    keys.last().copied().unwrap_or(0)
                }
                None => len.saturating_sub(1) as u32,
            };
            if len > 0
                && i32::try_from(last_key)
                    .ok()
                    .and_then(|key| offset.checked_add(key))
                    .is_none()
            {
                return Err(ViewError("DDSketch bucket index overflows".into()));
            }
            Ok(StoreRef {
                offset,
                counts,
                keys,
            })
        };
        let positive = store(positive_offset, positive_len)?;
        let negative = store(negative_offset, negative_len)?;
        let buckets: u64 = positive
            .counts
            .total()
            .zip(negative.counts.total())
            .and_then(|(p, n)| p.checked_add(n)?.checked_add(zero_count))
            .ok_or_else(|| ViewError("DDSketch counts overflow".into()))?;
        if buckets != count {
            return Err(ViewError(format!(
                "DDSketch count {count} differs from its buckets' {buckets}"
            )));
        }
        admission_rate::to_key(admission_rate).map_err(|e| ViewError(format!("DDSketch {e}")))?;
        if count > 0 && !(min <= max && min.is_finite() && max.is_finite()) || sum.is_nan() {
            return Err(ViewError(format!(
                "DDSketch min {min} / max {max} / sum {sum} are not a valid range"
            )));
        }
        // Every bucket index a real sketch can hold: from its smallest
        // sample's (or, when a store may hold values down to zero, the
        // smallest indexable magnitude's) to its largest sample's, plus the
        // empty slots a growing store keeps at its ends (under one growth
        // chunk each).
        let inv_log_gamma = 1.0 / ((1.0 + alpha) / (1.0 - alpha)).ln();
        let key = |v: f64| (v.ln() * inv_log_gamma).floor();
        let slack = GROW_CHUNK as f64 + 1.0;
        // Needed only for a store whose samples may come close to zero
        // (its `exp` costs more than the rest of the check).
        let indexable_lowest = || key(ddsketch_indexable_bounds(alpha).0) - slack;
        // A store's magnitudes lie in [smallest, largest]; `smallest` is 0
        // when samples of that sign may come arbitrarily close to zero.
        let store_ok = |store: &StoreRef, smallest: f64, largest: f64| -> bool {
            if store.is_empty() {
                return true;
            }
            let (first, last) = store.range();
            let lowest = if smallest > 0.0 {
                key(smallest) - slack
            } else {
                indexable_lowest()
            };
            let highest = if largest > 0.0 {
                key(largest) + slack
            } else {
                lowest + GROW_CHUNK as f64
            };
            f64::from(first) >= lowest
                && f64::from(last) <= highest
                && i64::from(last) - i64::from(first) < MAX_VIEW_BUCKET_SPAN
        };
        let (positive_range, negative_range) = if count > 0 {
            ((min.max(0.0), max), ((-max).max(0.0), -min))
        } else {
            ((0.0, 0.0), (0.0, 0.0))
        };
        if !store_ok(&positive, positive_range.0, positive_range.1)
            || !store_ok(&negative, negative_range.0, negative_range.1)
        {
            return Err(ViewError(format!(
                "DDSketch bucket range outside its samples' range or over \
                 {MAX_VIEW_BUCKET_SPAN} buckets"
            )));
        }
        Ok(Self {
            alpha,
            gamma: (1.0 + alpha) / (1.0 - alpha),
            count,
            sum,
            min,
            max,
            zero_count,
            admission_rate,
            positive,
            negative,
        })
    }

    /// The owned sketch this view reads.
    pub fn to_owned_sketch(&self) -> DdSketch {
        let mut sketch = DdSketch::new(self.alpha);
        sketch.admission_rate = self.admission_rate;
        sketch
            .merge_view(self)
            .expect("a view merges into an empty sketch of its alpha");
        sketch
    }

    /// The width of its stored bucket counts, in bytes: 1, 2, 4 or 8.
    pub fn count_width(&self) -> usize {
        self.positive.counts.width()
    }

    /// Whether its stores are written sparse (occupied buckets only).
    pub fn is_sparse(&self) -> bool {
        self.positive.keys.is_some()
    }

    /// Representative of bucket k, as [`DdSketch`] computes it.
    #[inline]
    fn bin_representative(&self, k: i32) -> f64 {
        self.gamma.powf(k as f64) * (1.0 + self.alpha)
    }

    /// Returns the total number of samples.
    pub fn get_count(&self) -> u64 {
        self.count
    }

    /// The sum of the samples.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Returns the minimum sample, or `None` if the sketch is empty.
    pub fn min(&self) -> Option<f64> {
        (self.count != 0).then_some(self.min)
    }

    /// Returns the maximum sample, or `None` if the sketch is empty.
    pub fn max(&self) -> Option<f64> {
        (self.count != 0).then_some(self.max)
    }

    /// Returns the relative-accuracy parameter `alpha`.
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Returns the estimated value at quantile `q` (in `[0, 1]`), or `None` if the sketch is empty.
    pub fn get_value_at_quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || q.is_nan() {
            return None;
        }
        if q <= 0.0 {
            return Some(self.min);
        }
        if q >= 1.0 {
            return Some(self.max);
        }

        let rank = ((q * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (bin, count) in self.negative.buckets().rev() {
            seen += count;
            if seen >= rank {
                return Some((-self.bin_representative(bin)).clamp(self.min, self.max));
            }
        }
        seen += self.zero_count;
        if seen >= rank {
            return Some(0.0);
        }

        for (bin, c) in self.positive.buckets() {
            if c == 0 {
                continue;
            }
            seen += c;
            if seen >= rank {
                let mut v = self.bin_representative(bin);
                if v < self.min {
                    v = self.min;
                }
                if v > self.max {
                    v = self.max;
                }
                return Some(v);
            }
        }

        Some(self.max)
    }

    /// Returns the quantile at `q` interpolated linearly between the two
    /// adjacent order statistics at rank `q * (n - 1)` (0-based), the
    /// convention of PromQL `quantile_over_time` and SQL `percentile_cont`.
    /// [`Self::get_value_at_quantile`] keeps its nearest-rank convention.
    ///
    /// - `Ok(None)` for an empty sketch.
    /// - `Err` for `q` outside `[0, 1]`, NaN included; mapping such a `q` to
    ///   `-inf` / `+inf` (as PromQL does) is the caller's job.
    /// - Rank 0 and rank `n - 1` are the exactly retained minimum and
    ///   maximum; every other order statistic is its bucket's representative
    ///   (negative store, zero bucket or positive store), clamped to
    ///   `[min, max]`.
    ///
    /// Error: each neighbour is within `alpha` relative of the true order
    /// statistic when its magnitude is indexable, so when both neighbours are
    /// indexable and share a sign the result is within `alpha` relative of the
    /// true interpolated value. Across zero the bound is absolute:
    /// `alpha * ((1 - f) |lower| + f |upper|)` with `f` the fractional rank,
    /// which need not be small relative to a result near zero. Magnitudes
    /// below the indexable range are counted in the zero bucket and read as
    /// `0`. A sketch never holds NaN: [`DdSketch::try_add`] refuses non-finite
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
    /// exact minimum and maximum at the ends, otherwise the representative
    /// of the bucket holding it, clamped to `[min, max]`.
    fn value_at_rank(&self, rank: u64) -> f64 {
        debug_assert!(rank < self.count);
        if rank == 0 {
            return self.min;
        }
        if rank == self.count - 1 {
            return self.max;
        }
        let mut seen = 0u64;
        for (bin, count) in self.negative.buckets().rev() {
            seen += count;
            if seen > rank {
                return (-self.bin_representative(bin)).clamp(self.min, self.max);
            }
        }
        seen += self.zero_count;
        if seen > rank {
            return 0.0_f64.clamp(self.min, self.max);
        }
        for (bin, count) in self.positive.buckets() {
            seen += count;
            if seen > rank {
                return self.bin_representative(bin).clamp(self.min, self.max);
            }
        }
        self.max
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

    /// A shrunk sketch answers, merges and keeps growing as before, and holds
    /// only its occupied bucket range.
    #[test]
    fn shrink_to_fit_keeps_answers_and_frees_slots() {
        let values: Vec<f64> = (0..100)
            .map(|i| {
                if i % 7 == 0 {
                    -(1.0 + i as f64)
                } else {
                    3.0 + (i as f64).powf(1.3)
                }
            })
            .collect();
        let full = sketch_of(0.05, &values);
        let mut shrunk = full.clone();
        shrunk.shrink_to_fit();
        assert!(shrunk.memory_bytes() < full.memory_bytes());
        let qs = [0.0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0];
        let answers = |sketch: &DdSketch| -> Vec<Option<f64>> {
            qs.iter()
                .map(|&q| sketch.quantile_interpolated(q).unwrap())
                .collect()
        };
        assert_eq!(answers(&shrunk), answers(&full));
        assert_eq!(shrunk.get_count(), full.get_count());
        assert_eq!(shrunk.sum().to_bits(), full.sum().to_bits());
        assert_eq!(shrunk.min(), full.min());
        assert_eq!(shrunk.max(), full.max());
        // The encoding drops the empty slots (shorter bytes) and decodes to
        // a sketch with the same answers, count and scalars; decoding the
        // full sketch's bytes and shrinking it gives the shrunk bytes.
        let shrunk_bytes = shrunk.to_asapv1().unwrap();
        let full_bytes = full.to_asapv1().unwrap();
        assert!(shrunk_bytes.len() < full_bytes.len());
        let decoded = DdSketch::from_asapv1(&shrunk_bytes).unwrap();
        assert_eq!(answers(&decoded), answers(&full));
        assert_eq!(decoded.get_count(), full.get_count());
        assert_eq!(decoded.sum().to_bits(), full.sum().to_bits());
        assert_eq!((decoded.min(), decoded.max()), (full.min(), full.max()));
        let mut decoded_full = DdSketch::from_asapv1(&full_bytes).unwrap();
        decoded_full.shrink_to_fit();
        assert_eq!(decoded_full.to_asapv1().unwrap(), shrunk_bytes);
        // Shrinking twice changes nothing.
        let mut twice = shrunk.clone();
        twice.shrink_to_fit();
        assert_eq!(twice.to_asapv1().unwrap(), shrunk_bytes);
        let other = sketch_of(0.05, &[0.5, 1e4, 2.0, -3e3]);
        let (mut merged_full, mut merged_shrunk) = (full.clone(), shrunk.clone());
        merged_full.merge(&other).unwrap();
        merged_shrunk.merge(&other).unwrap();
        assert_eq!(answers(&merged_shrunk), answers(&merged_full));
        let (mut grown_full, mut grown_shrunk) = (full.clone(), shrunk.clone());
        for v in [1e6, 1e-3, -1e5, 42.0] {
            grown_full.add(&v);
            grown_shrunk.add(&v);
        }
        assert_eq!(answers(&grown_shrunk), answers(&grown_full));
        let mut empty = DdSketch::new(0.05);
        empty.shrink_to_fit();
        assert_eq!(empty.get_count(), 0);
    }

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
        // The nearest-rank readout keeps its convention.
        assert_eq!(sketch.get_value_at_quantile(0.9), Some(40.0));
    }

    #[test]
    fn quantile_interpolated_one_value_and_empty() {
        let one = sketch_of(0.01, &[20.0]);
        for q in [0.0, 0.3, 1.0] {
            assert_eq!(one.quantile_interpolated(q).unwrap(), Some(20.0));
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
            assert!(q <= s.max().unwrap() + 1e-12);
            assert!(q >= s.min().unwrap() - 1e-12);
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

        // counts and bounds
        assert_eq!(s1.get_count(), (vals1.len() + vals2.len()) as u64);
        assert_eq!(s1.min().unwrap(), 1.0);
        assert_eq!(s1.max().unwrap(), 20.0);

        // extreme quantiles should match bounds
        assert_eq!(s1.get_value_at_quantile(0.0).unwrap(), 1.0);
        assert_eq!(s1.get_value_at_quantile(1.0).unwrap(), 20.0);

        // sanity: middle quantile is within [min, max]
        let mid = s1.get_value_at_quantile(0.5).unwrap();
        assert!((1.0..=20.0).contains(&mid));
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

        // `count` is summed back from the buckets; `sum`/`min`/`max` are carried
        // on the wire, so every scalar and every quantile comes back exact.
        assert_eq!(decoded.get_count(), s.get_count());
        assert_eq!(decoded.sum(), s.sum());
        assert_eq!(decoded.min(), s.min());
        assert_eq!(decoded.max(), s.max());
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
