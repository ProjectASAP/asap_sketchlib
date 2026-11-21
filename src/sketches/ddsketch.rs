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

/// One store's bucket counts. A store that still receives values keeps
/// 8-byte counts; sealing a store ([`DdSketch::shrink_to_fit`]) narrows
/// them to 4 bytes when every count fits in a `u32`, halving the bytes a
/// stored window holds. A count past `u32::MAX` keeps the 8-byte layout.
/// Any change to a narrow store widens it first, so merge accumulators
/// always add in `u64`.
#[derive(Debug, Clone)]
enum Counts {
    Wide(Vector1D<u64>),
    Narrow(Box<[u32]>),
}

#[derive(Debug, Clone)]
struct Buckets {
    counts: Counts,
    offset: i32,
}

impl Default for Buckets {
    fn default() -> Self {
        Self::new()
    }
}

impl Buckets {
    fn new() -> Self {
        Self::wide(Vec::new(), 0)
    }

    fn wide(counts: Vec<u64>, offset: i32) -> Self {
        Self {
            counts: Counts::Wide(Vector1D::from_vec(counts)),
            offset,
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    fn as_ref(&self) -> CountsRef<'_> {
        match &self.counts {
            Counts::Wide(counts) => CountsRef::Wide(counts.as_slice()),
            Counts::Narrow(counts) => CountsRef::Narrow(counts),
        }
    }

    /// Bytes the counts have allocated.
    fn allocated_bytes(&self) -> usize {
        match &self.counts {
            Counts::Wide(counts) => counts.capacity() * size_of::<u64>(),
            Counts::Narrow(counts) => counts.len() * size_of::<u32>(),
        }
    }

    /// The 8-byte counts, widening narrow ones first.
    #[inline(always)]
    fn wide_mut(&mut self) -> &mut Vector1D<u64> {
        if let Counts::Narrow(narrow) = &self.counts {
            self.counts = Counts::Wide(Vector1D::from_vec(
                narrow.iter().map(|&c| u64::from(c)).collect(),
            ));
        }
        match &mut self.counts {
            Counts::Wide(counts) => counts,
            Counts::Narrow(_) => unreachable!("widened above"),
        }
    }

    #[inline(always)]
    fn range(&self) -> Option<(i32, i32)> {
        if self.is_empty() {
            None
        } else {
            let left = self.offset;
            let right = self.offset + self.len() as i32 - 1;
            Some((left, right))
        }
    }

    /// Ensure bucket k exists, using growth in chunks.
    #[inline(always)]
    fn ensure(&mut self, k: i32) {
        if self.is_empty() {
            self.counts = Counts::Wide(Vector1D::from_vec(vec![0u64; GROW_CHUNK]));
            self.offset = k - (GROW_CHUNK as i32 / 2);
            return;
        }

        let (left, right) = self.range().unwrap();

        if k < left {
            let needed = (left - k) as usize;
            let grow = needed.max(GROW_CHUNK);

            let mut v = vec![0u64; grow];
            v.extend_from_slice(self.wide_mut().as_slice());

            self.counts = Counts::Wide(Vector1D::from_vec(v));
            self.offset -= grow as i32;
        } else if k > right {
            let needed = (k - right) as usize;
            let grow = needed.max(GROW_CHUNK);

            let mut v = self.wide_mut().clone().into_vec();
            v.resize(v.len() + grow, 0);
            self.counts = Counts::Wide(Vector1D::from_vec(v));
        }
    }

    /// Adds `count` to bucket `k`, growing the store to hold it.
    #[inline(always)]
    fn add(&mut self, k: i32, count: u64) {
        self.ensure(k);
        let idx = (k - self.offset) as usize;
        self.wide_mut().as_mut_slice()[idx] += count;
    }

    #[inline(always)]
    fn add_one(&mut self, k: i32) {
        // this is the method that gets called on every sample insertion
        let idx_i32 = k - self.offset;

        if idx_i32 >= 0
            && let Counts::Wide(counts) = &mut self.counts
        {
            let slice = counts.as_mut_slice();
            let idx = idx_i32 as usize;
            if idx < slice.len() {
                slice[idx] += 1;
                return;
            }
        }

        // This is the method that gets called only on rare expansions
        self.add(k, 1);
    }

    /// Seals the store: drops its empty end slots and spare capacity, and
    /// narrows its counts to 4 bytes when each fits in a `u32`. The
    /// occupied range of bucket indices and its counts are unchanged.
    fn shrink_to_fit(&mut self) {
        self.drop_empty_slots();
        if let Counts::Wide(counts) = &self.counts
            && let Ok(narrow) = counts
                .as_slice()
                .iter()
                .map(|&c| u32::try_from(c))
                .collect::<Result<Box<[u32]>, _>>()
        {
            self.counts = Counts::Narrow(narrow);
        }
    }

    /// Drops the empty slots at both ends and the spare capacity, keeping
    /// the counts' width.
    fn drop_empty_slots(&mut self) {
        let counts = self.as_ref();
        let Some(first) = counts.iter().position(|c| c != 0) else {
            *self = Self::new();
            return;
        };
        let last = counts
            .iter()
            .rposition(|c| c != 0)
            .expect("a nonzero count");
        match &mut self.counts {
            Counts::Wide(wide) => {
                if first == 0 && last + 1 == wide.len() && wide.len() == wide.capacity() {
                    return;
                }
                *wide = Vector1D::from_vec(wide.as_slice()[first..=last].to_vec());
            }
            Counts::Narrow(narrow) => {
                if first == 0 && last + 1 == narrow.len() {
                    return;
                }
                *narrow = narrow[first..=last].into();
            }
        }
        self.offset += first as i32;
    }
}

/// One store's bucket counts, borrowed at their stored width.
#[derive(Debug, Clone, Copy)]
enum CountsRef<'a> {
    Wide(&'a [u64]),
    Narrow(&'a [u32]),
}

impl<'a> CountsRef<'a> {
    #[inline(always)]
    fn len(self) -> usize {
        match self {
            Self::Wide(counts) => counts.len(),
            Self::Narrow(counts) => counts.len(),
        }
    }

    #[inline(always)]
    fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Each count as a `u64`, in bucket order.
    #[inline(always)]
    fn iter(self) -> CountsIter<'a> {
        match self {
            Self::Wide(counts) => CountsIter::Wide(counts.iter()),
            Self::Narrow(counts) => CountsIter::Narrow(counts.iter()),
        }
    }

    /// The counts as 4-byte values, each at most `u32::MAX`: borrowed when
    /// stored narrow.
    fn to_narrow(self) -> std::borrow::Cow<'a, [u32]> {
        match self {
            Self::Narrow(counts) => std::borrow::Cow::Borrowed(counts),
            Self::Wide(counts) => counts
                .iter()
                .map(|&c| u32::try_from(c).expect("checked to fit a u32"))
                .collect::<Vec<_>>()
                .into(),
        }
    }

    /// The counts as 8-byte values: borrowed when stored wide.
    fn to_wide(self) -> std::borrow::Cow<'a, [u64]> {
        match self {
            Self::Wide(counts) => std::borrow::Cow::Borrowed(counts),
            Self::Narrow(_) => self.iter().collect::<Vec<_>>().into(),
        }
    }

    /// The first count's address, for checking where a view reads from.
    fn as_ptr(self) -> *const u8 {
        match self {
            Self::Wide(counts) => counts.as_ptr().cast(),
            Self::Narrow(counts) => counts.as_ptr().cast(),
        }
    }
}

/// [`CountsRef::iter`]: each count widened to `u64`.
#[derive(Debug, Clone)]
enum CountsIter<'a> {
    Wide(std::slice::Iter<'a, u64>),
    Narrow(std::slice::Iter<'a, u32>),
}

impl Iterator for CountsIter<'_> {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        match self {
            Self::Wide(counts) => counts.next().copied(),
            Self::Narrow(counts) => counts.next().map(|&c| u64::from(c)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for CountsIter<'_> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<u64> {
        match self {
            Self::Wide(counts) => counts.next_back().copied(),
            Self::Narrow(counts) => counts.next_back().map(|&c| u64::from(c)),
        }
    }
}

impl ExactSizeIterator for CountsIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Wide(counts) => counts.len(),
            Self::Narrow(counts) => counts.len(),
        }
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
        store.add(delta.index, delta.count);

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
    /// See [`DdSketchView::quantile_interpolated`].
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
    /// `admission_rate`, the count width in bytes (`4` when every bucket
    /// count fits a `u32`, as in a stored window; `8` otherwise) and a
    /// reserved `u32`, then per sign (positive, negative) `offset: i32`,
    /// `len: u32`, and the bucket counts `[u32 or u64; len]`. A sealed
    /// sketch's 4-byte counts ([`Self::shrink_to_fit`]) are written as
    /// they are held.
    pub fn to_view_bytes(&self) -> Vec<u8> {
        let (positive, negative) = (self.positive_store.as_ref(), self.negative_store.as_ref());
        let narrow = positive
            .iter()
            .chain(negative.iter())
            .all(|count| count <= u64::from(u32::MAX));
        let mut writer = ViewWriter::new(DD_VIEW_MAGIC);
        writer
            .f64(self.alpha)
            .u64(self.count)
            .f64(self.sum)
            .f64(self.min)
            .f64(self.max)
            .u64(self.zero_count)
            .f64(self.admission_rate)
            .u32(if narrow { 4 } else { 8 })
            .u32(0)
            .i32(self.positive_store.offset)
            .u32(positive.len() as u32)
            .i32(self.negative_store.offset)
            .u32(negative.len() as u32);
        if narrow {
            writer
                .u32s(&positive.to_narrow())
                .u32s(&negative.to_narrow());
        } else {
            writer.u64s(&positive.to_wide()).u64s(&negative.to_wide());
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
    ///
    /// Each store keeps its counts in 4 bytes instead of 8 when every count
    /// fits in a `u32` (always, below 2^32 values), and in 8 bytes
    /// otherwise. A later insert or merge into this sketch widens the
    /// counts back to 8 bytes first. The ASAPv1 encoding does not depend on
    /// the width.
    pub fn shrink_to_fit(&mut self) {
        self.positive_store.shrink_to_fit();
        self.negative_store.shrink_to_fit();
    }

    /// Bytes this sketch holds: the struct and the bucket slots both stores
    /// have allocated.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            + self.positive_store.allocated_bytes()
            + self.negative_store.allocated_bytes()
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
    pub fn positive_store_counts(&self) -> std::borrow::Cow<'_, [u64]> {
        self.positive_store.as_ref().to_wide()
    }

    /// Returns the absolute bucket index corresponding to
    /// `positive_store_counts()[0]`.
    pub fn positive_store_offset(&self) -> i32 {
        self.positive_store.offset
    }

    pub fn negative_store_counts(&self) -> std::borrow::Cow<'_, [u64]> {
        self.negative_store.as_ref().to_wide()
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
    /// result. Allocates only when this sketch's stores must grow.
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

    fn merge_store(store: &mut Buckets, other: StoreRef<'_>) {
        if other.counts.is_empty() {
            return;
        }
        if store.is_empty() {
            *store = other.to_buckets();
            return;
        }

        let (self_l, self_r) = store.range().unwrap();
        let (other_l, other_r) = (other.offset, other.offset + other.counts.len() as i32 - 1);

        let new_l = self_l.min(other_l);
        let new_r = self_r.max(other_r);
        let new_len = (new_r - new_l + 1) as usize;

        let mut merged = vec![0u64; new_len];

        // Copy self
        for (i, c) in store.as_ref().iter().enumerate() {
            let k = self_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        // Add other
        for (i, c) in other.counts.iter().enumerate() {
            let k = other_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        *store = Buckets::wide(merged, new_l);
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

/// One sign's bucket store, borrowed: bucket `offset + i` holds `counts[i]`.
#[derive(Debug, Clone, Copy)]
struct StoreRef<'a> {
    offset: i32,
    counts: CountsRef<'a>,
}

impl<'a> StoreRef<'a> {
    fn of(store: &'a Buckets) -> Self {
        Self {
            offset: store.offset,
            counts: store.as_ref(),
        }
    }

    fn to_buckets(self) -> Buckets {
        Buckets::wide(
            self.counts.iter().collect(),
            if self.counts.is_empty() {
                0
            } else {
                self.offset
            },
        )
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
        let _reserved = reader.u32()?;
        let positive_offset = reader.i32()?;
        let positive_len = reader.u32()? as usize;
        let negative_offset = reader.i32()?;
        let negative_len = reader.u32()? as usize;
        let (positive, negative) = match width {
            4 => (
                CountsRef::Narrow(reader.u32s(positive_len)?),
                CountsRef::Narrow(reader.u32s(negative_len)?),
            ),
            8 => (
                CountsRef::Wide(reader.u64s(positive_len)?),
                CountsRef::Wide(reader.u64s(negative_len)?),
            ),
            other => {
                return Err(ViewError(format!(
                    "DDSketch count width {other} is not 4 or 8 bytes"
                )));
            }
        };
        let buckets: u64 = positive
            .iter()
            .chain(negative.iter())
            .try_fold(zero_count, |total, c| total.checked_add(c))
            .ok_or_else(|| ViewError("DDSketch counts overflow".into()))?;
        if buckets != count {
            return Err(ViewError(format!(
                "DDSketch count {count} differs from its buckets' {buckets}"
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
            positive: StoreRef {
                offset: positive_offset,
                counts: positive,
            },
            negative: StoreRef {
                offset: negative_offset,
                counts: negative,
            },
        })
    }

    /// The owned sketch this view reads, with 8-byte counts (as an open
    /// sketch or a merge accumulator holds them).
    pub fn to_owned_sketch(&self) -> DdSketch {
        let mut sketch = DdSketch::new(self.alpha);
        sketch.admission_rate = self.admission_rate;
        sketch
            .merge_view(self)
            .expect("a view merges into an empty sketch of its alpha");
        sketch
    }

    /// The bucket counts' first slot, for checking where a view reads from.
    pub fn bucket_counts_ptr(&self) -> Option<*const u8> {
        (!self.positive.counts.is_empty()).then_some(self.positive.counts.as_ptr())
    }

    /// The width of its stored bucket counts, in bytes: 4 or 8.
    pub fn count_width(&self) -> usize {
        match self.positive.counts {
            CountsRef::Narrow(_) => 4,
            CountsRef::Wide(_) => 8,
        }
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
        for (i, count) in self.negative.counts.iter().enumerate().rev() {
            seen += count;
            if seen >= rank {
                return Some(
                    (-self.bin_representative(self.negative.offset + i as i32))
                        .clamp(self.min, self.max),
                );
            }
        }
        seen += self.zero_count;
        if seen >= rank {
            return Some(0.0);
        }

        let slice = self.positive.counts;
        let offset = self.positive.offset;

        for (i, c) in slice.iter().enumerate() {
            if c == 0 {
                continue;
            }
            seen += c;
            if seen >= rank {
                let bin = offset + i as i32;
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
        let negative = self.negative.counts;
        for (i, count) in negative.iter().enumerate().rev() {
            seen += count;
            if seen > rank {
                let bin = self.negative.offset + i as i32;
                return (-self.bin_representative(bin)).clamp(self.min, self.max);
            }
        }
        seen += self.zero_count;
        if seen > rank {
            return 0.0_f64.clamp(self.min, self.max);
        }
        let positive = self.positive.counts;
        for (i, count) in positive.iter().enumerate() {
            seen += count;
            if seen > rank {
                let bin = self.positive.offset + i as i32;
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

    /// `n` values over six decades, one in seven negative, some zero.
    fn spread_values(n: usize) -> Vec<f64> {
        (0..n)
            .map(|i| {
                let magnitude = 10f64.powf(((i as u64 * 2_654_435_761) % 6_000) as f64 / 1_000.0);
                match i % 7 {
                    0 => -magnitude,
                    3 if i % 21 == 3 => 0.0,
                    _ => magnitude,
                }
            })
            .collect()
    }

    fn occupied_slots(sketch: &DdSketch) -> usize {
        sketch.positive_store_counts().len() + sketch.negative_store_counts().len()
    }

    fn is_narrow(sketch: &DdSketch) -> bool {
        matches!(sketch.positive_store.counts, Counts::Narrow(_))
            && matches!(sketch.negative_store.counts, Counts::Narrow(_))
    }

    fn is_wide(sketch: &DdSketch) -> bool {
        matches!(sketch.positive_store.counts, Counts::Wide(_))
            && matches!(sketch.negative_store.counts, Counts::Wide(_))
    }

    /// Where a DDSketch view records its counts' width in bytes (4 or 8):
    /// after the magic, layout version and seven 8-byte scalars.
    const VIEW_COUNT_BYTES_AT: usize = 4 + 4 + 7 * 8;

    const QS: [f64; 11] = [
        0.0, 1e-4, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.9999, 1.0,
    ];

    /// A store's nonzero buckets as `(index, count)`, whatever empty slots
    /// it holds.
    fn occupied(counts: &[u64], offset: i32) -> Vec<(i32, u64)> {
        (offset..)
            .zip(counts.iter().copied())
            .filter(|&(_, c)| c != 0)
            .collect()
    }

    /// Every answer a sketch gives: count, sum, min, max, the zero bucket,
    /// both stores' occupied buckets, and the nearest-rank and interpolated
    /// quantiles at `QS` (with the q = 0 and q = 1 extremes).
    fn every_answer(sketch: &DdSketch) -> String {
        format!(
            "{} {} {:?} {:?} {} {:?} {:?} {:?} {:?}",
            sketch.get_count(),
            sketch.sum().to_bits(),
            sketch.min(),
            sketch.max(),
            sketch.zero_count(),
            occupied(
                &sketch.positive_store_counts(),
                sketch.positive_store_offset()
            ),
            occupied(
                &sketch.negative_store_counts(),
                sketch.negative_store_offset()
            ),
            QS.map(|q| sketch.get_value_at_quantile(q)),
            QS.map(|q| sketch.quantile_interpolated(q).unwrap()),
        )
    }

    /// A sealed sketch keeps each bucket count in 4 bytes: it holds its
    /// struct and 4 B per occupied bucket slot, half the open layout's 8 B.
    #[test]
    fn a_sealed_sketch_holds_four_bytes_per_bucket() {
        let mut sealed = sketch_of(0.01, &spread_values(10_000));
        sealed.shrink_to_fit();
        let slots = occupied_slots(&sealed);
        assert!(slots > 1_000, "{slots}");
        assert!(is_narrow(&sealed));
        assert_eq!(sealed.memory_bytes(), size_of::<DdSketch>() + 4 * slots);
    }

    /// A sealed sketch answers exactly as the open sketch it was sealed
    /// from, the q = 0 and q = 1 extremes included, and so does a sketch
    /// with one empty store.
    #[test]
    fn sealed_counts_answer_as_open_counts() {
        for values in [
            spread_values(10_000),
            spread_values(1),
            vec![3.0, 3.0, 5.0, 1e6],
            vec![-3.0, -2.0, -2.0],
            vec![0.0, 0.0],
        ] {
            let open = sketch_of(0.01, &values);
            let mut sealed = open.clone();
            sealed.shrink_to_fit();
            assert_eq!(every_answer(&sealed), every_answer(&open), "{values:?}");
            assert_eq!(sealed.to_asapv1().unwrap(), {
                let mut shrunk_open = open.clone();
                shrunk_open.positive_store.drop_empty_slots();
                shrunk_open.negative_store.drop_empty_slots();
                shrunk_open.to_asapv1().unwrap()
            });
        }
        assert!(
            sketch_of(0.01, &[])
                .quantile_interpolated(0.5)
                .unwrap()
                .is_none()
        );
    }

    /// Merging sealed sketches into an accumulator gives the accumulator
    /// merging their open copies gives, and the accumulator keeps 8-byte
    /// counts, including when its first merge copies a sealed sketch; it
    /// then adds past `u32::MAX`.
    #[test]
    fn merging_sealed_sketches_into_an_open_accumulator_is_exact() {
        let open: Vec<DdSketch> = (0..60)
            .map(|w| {
                let values = spread_values(200 + w);
                sketch_of(0.01, &values[w..])
            })
            .collect();
        let sealed: Vec<DdSketch> = open
            .iter()
            .map(|sketch| {
                let mut sealed = sketch.clone();
                sealed.shrink_to_fit();
                sealed
            })
            .collect();
        let (mut from_open, mut from_sealed) = (DdSketch::new(0.01), DdSketch::new(0.01));
        for (o, s) in open.iter().zip(&sealed) {
            from_open.merge(o).unwrap();
            from_sealed.merge(s).unwrap();
            assert!(is_wide(&from_sealed));
        }
        assert_eq!(every_answer(&from_sealed), every_answer(&from_open));

        let mut near_max = sketch_of(0.01, &[7.0]);
        near_max.apply_delta(DdSketchBucketDelta {
            sign: DdSign::Positive,
            index: near_max.key_for(7.0),
            count: u64::from(u32::MAX) - 1,
        });
        near_max.shrink_to_fit();
        assert!(is_narrow(&near_max));
        let mut accumulator = DdSketch::new(0.01);
        accumulator.merge(&near_max).unwrap();
        accumulator.merge(&near_max).unwrap();
        assert_eq!(
            accumulator.positive_store_counts().as_ref(),
            &[2 * u64::from(u32::MAX)]
        );
        assert_eq!(accumulator.get_count(), 2 * u64::from(u32::MAX));
    }

    /// A bucket count past `u32::MAX` keeps the 8-byte layout when sealed,
    /// with the same answers; a count of exactly `u32::MAX` narrows.
    #[test]
    fn a_bucket_past_u32_max_keeps_eight_byte_counts_when_sealed() {
        let mut doubled = sketch_of(0.01, &[1.0, 2.0, 1.0, -4.0]);
        for _ in 0..32 {
            let copy = doubled.clone();
            doubled.merge(&copy).unwrap();
        }
        assert_eq!(doubled.get_count(), 4 << 32);
        let open = doubled.clone();
        doubled.shrink_to_fit();
        assert!(is_wide(&doubled));
        assert_eq!(
            doubled.memory_bytes(),
            size_of::<DdSketch>() + 8 * occupied_slots(&doubled)
        );
        assert_eq!(every_answer(&doubled), every_answer(&open));

        let mut at_max = sketch_of(0.01, &[7.0]);
        at_max.apply_delta(DdSketchBucketDelta {
            sign: DdSign::Positive,
            index: at_max.key_for(7.0),
            count: u64::from(u32::MAX) - 1,
        });
        at_max.shrink_to_fit();
        assert!(is_narrow(&at_max));
        assert_eq!(
            at_max.positive_store_counts().as_ref(),
            &[u64::from(u32::MAX)]
        );
        at_max.add(&7.0);
        assert_eq!(
            at_max.positive_store_counts().as_ref(),
            &[u64::from(u32::MAX) + 1],
            "an insert into a sealed sketch widens its counts first"
        );
    }

    /// A view records its counts' width: open and sealed sketches whose
    /// counts fit a `u32` write 4-byte counts (a sealed one as it holds
    /// them), one with a count past `u32::MAX` 8-byte counts, and a view of
    /// either answers and merges as the owned sketch, reading the counts in
    /// place. Any other width is refused.
    #[test]
    fn views_record_and_read_both_count_widths() {
        let open = sketch_of(0.01, &spread_values(5_000));
        let mut sealed = open.clone();
        sealed.shrink_to_fit();
        let mut overflowing = sketch_of(0.01, &[1.0, -2.0]);
        for _ in 0..33 {
            let copy = overflowing.clone();
            overflowing.merge(&copy).unwrap();
        }
        overflowing.shrink_to_fit();
        assert!(is_wide(&overflowing));
        for (sketch, count_bytes) in [(&open, 4), (&sealed, 4), (&overflowing, 8)] {
            let bytes = sketch.to_view_bytes();
            assert_eq!(bytes[VIEW_COUNT_BYTES_AT], count_bytes);
            let words = crate::views::aligned_copy(&bytes);
            let in_place = crate::views::aligned_bytes(&words, bytes.len());
            let view = DdSketchView::from_bytes(in_place).unwrap();
            assert_eq!(view.count_width(), usize::from(count_bytes));
            let ptr = view.bucket_counts_ptr().unwrap();
            assert!(in_place.as_ptr_range().contains(&ptr));
            let mut via_view = DdSketch::new(0.01);
            via_view.merge_view(&view).unwrap();
            let mut via_owned = DdSketch::new(0.01);
            via_owned.merge(sketch).unwrap();
            assert_eq!(every_answer(&via_view), every_answer(&via_owned));
            for q in QS {
                assert_eq!(
                    view.get_value_at_quantile(q),
                    sketch.get_value_at_quantile(q)
                );
                assert_eq!(
                    view.quantile_interpolated(q),
                    sketch.quantile_interpolated(q)
                );
            }
        }
        // Header (width, reserved, two offsets and lengths after the
        // scalars), then each store's counts padded to 8 bytes.
        let padded = |bytes: usize| bytes.div_ceil(8) * 8;
        let header = VIEW_COUNT_BYTES_AT + 8 + 16;
        assert_eq!(
            sealed.to_view_bytes().len(),
            header
                + padded(4 * sealed.positive_store_counts().len())
                + padded(4 * sealed.negative_store_counts().len())
        );
        let mut bad = sealed.to_view_bytes();
        bad[VIEW_COUNT_BYTES_AT] = 2;
        let words = crate::views::aligned_copy(&bad);
        assert!(DdSketchView::from_bytes(crate::views::aligned_bytes(&words, bad.len())).is_err());
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
        let span_before = d.positive_store.len();

        let (min_indexable, max_indexable) = ddsketch_indexable_bounds(0.01);
        d.add(&(max_indexable * 10.0));
        d.add(&(min_indexable / 10.0));
        assert_eq!(
            d.get_count(),
            count_before + 1,
            "tiny value belongs to zero bucket"
        );
        assert_eq!(
            d.positive_store.len(),
            span_before,
            "store span grew from an untrackable extreme"
        );

        // A large-but-trackable value is still recorded.
        d.add(&(max_indexable / 2.0));
        assert_eq!(d.get_count(), count_before + 2);
    }
}
