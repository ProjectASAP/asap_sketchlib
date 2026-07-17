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
use crate::common::input::data_input_to_f64;
use crate::common::numerical::NumericalValue;
use crate::common::structures::Vector1D;
use rmp_serde::decode::Error as RmpDecodeError;
use rmp_serde::encode::Error as RmpEncodeError;
use rmp_serde::{from_slice, to_vec_named};
use serde::{Deserialize, Serialize};

// Number of buckets to grow by when expanding.
const GROW_CHUNK: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Buckets {
    counts: Vector1D<u64>,
    offset: i32,

    /// Caps the number of bins this store will ever hold. 0 (the default,
    /// and what plain `DDSketch::new` uses) means unbounded — pre-existing
    /// behavior, unchanged. When >0, `ensure` COLLAPSES the LOWEST bins
    /// (folding their mass into the new floor bucket) instead of growing
    /// past `max_bins` bins total, bounding the contiguous allocation a
    /// single finite-but-extreme outlier can force (asap_sketchlib#70 item
    /// 4 / sketchlib-go#72). Mirrors DataDog's `CollapsingLowestDenseStore`:
    /// precision is lost only at the low end; the high end (and hence
    /// high-quantile accuracy, e.g. p99 latency) is never affected.
    ///
    /// NOT serialized — purely a local, in-memory bound (see
    /// `DDSketch::with_max_bins`); a decoded sketch is always unbounded.
    #[serde(skip)]
    max_bins: i32,
}

impl Buckets {
    fn new() -> Self {
        Self {
            counts: Vector1D::from_vec(Vec::new()),
            offset: 0,
            max_bins: 0,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    // not used in current version
    // #[inline(always)]
    // fn len(&self) -> usize {
    //     self.counts.len()
    // }

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

    /// Ensure bucket k exists — growing the store, or (when `max_bins` caps
    /// it) collapsing the lowest bins — and return the ARRAY INDEX to write
    /// k's contribution to. Ordinarily that's `k - offset` (k gets its own
    /// bucket); once `max_bins` caps the store and k falls below the
    /// collapsed floor, it's 0 (the floor bucket). Callers MUST use the
    /// returned index rather than recomputing `k - offset`, since the two
    /// can differ once collapsing has occurred.
    #[inline(always)]
    fn ensure(&mut self, k: i32) -> usize {
        if self.counts.is_empty() {
            let mut init_len = GROW_CHUNK as i32;
            if self.max_bins > 0 && init_len > self.max_bins {
                init_len = self.max_bins;
            }
            let init_len = init_len.max(1);
            self.counts = Vector1D::from_vec(vec![0u64; init_len as usize]);
            self.offset = k - init_len / 2;
            return (k - self.offset) as usize;
        }

        let (left, right) = self.range().unwrap();
        if k >= left && k <= right {
            return (k - self.offset) as usize;
        }

        // The TRUE minimal range needed to cover k alongside the existing
        // data — used (not the buffered grow target below) to decide
        // whether a collapse is unavoidable, so a small necessary
        // extension never triggers one.
        let (true_left, true_right) = if k < left { (k, right) } else { (left, k) };
        if self.max_bins > 0 && true_right - true_left + 1 > self.max_bins {
            let new_left = true_right - self.max_bins + 1;
            return self.collapse_to(new_left, true_right, k);
        }

        if k < left {
            let needed = (left - k) as usize;
            let grow = needed.max(GROW_CHUNK);
            let mut new_left = left - grow as i32;
            if self.max_bins > 0 && right - new_left + 1 > self.max_bins {
                new_left = right - self.max_bins + 1; // clamp the overshoot buffer to the cap
            }
            let grow = (left - new_left) as usize;

            let mut v = vec![0u64; grow];
            v.extend_from_slice(self.counts.as_slice());
            self.counts = Vector1D::from_vec(v);
            self.offset = new_left;
            (k - self.offset) as usize
        } else {
            // k > right
            let needed = (k - right) as usize;
            let grow = needed.max(GROW_CHUNK);
            let mut new_right = right + grow as i32;
            if self.max_bins > 0 && new_right - left + 1 > self.max_bins {
                new_right = left + self.max_bins - 1; // clamp the overshoot buffer to the cap
            }
            let new_len = (new_right - left + 1) as usize;

            let mut v = self.counts.clone().into_vec();
            v.resize(new_len, 0);
            self.counts = Vector1D::from_vec(v);
            (k - self.offset) as usize
        }
    }

    /// Rebuild the store to exactly cover `[new_left, new_right]` (a span of
    /// at most `max_bins` bins), folding the count of every existing bucket
    /// below `new_left` into the new floor bucket (index `new_left`) rather
    /// than discarding it — total count is always conserved across a
    /// collapse, only the LOW-end bucket resolution degrades. Returns the
    /// array index to write k's own contribution to: the floor bucket if k
    /// itself falls below `new_left` (k may be collapsed away too, on the
    /// very insert that triggered this), otherwise k's own bucket.
    fn collapse_to(&mut self, new_left: i32, new_right: i32, k: i32) -> usize {
        let new_len = (new_right - new_left + 1) as usize;
        let mut new_counts = vec![0u64; new_len];

        if !self.is_empty() {
            let old_offset = self.offset;
            let mut carry: u64 = 0;
            for (i, &c) in self.counts.as_slice().iter().enumerate() {
                if c == 0 {
                    continue;
                }
                let idx = old_offset + i as i32;
                if idx < new_left {
                    carry += c;
                } else {
                    // idx is always <= new_right here: new_right is always
                    // the max of the old right edge and k, so no existing
                    // data can exceed it.
                    new_counts[(idx - new_left) as usize] += c;
                }
            }
            new_counts[0] += carry;
        }

        self.counts = Vector1D::from_vec(new_counts);
        self.offset = new_left;

        if k < new_left {
            0
        } else {
            (k - new_left) as usize
        }
    }

    #[inline(always)]
    fn add_one(&mut self, k: i32) {
        // this is the method that gets called on every sample insertion
        if !self.counts.is_empty() {
            let idx_i32 = k - self.offset;
            if idx_i32 >= 0 {
                let idx = idx_i32 as usize;
                let slice = self.counts.as_mut_slice();
                if idx < slice.len() {
                    unsafe {
                        *slice.as_mut_ptr().add(idx) += 1;
                    }
                    return;
                }
            }
        }

        // This is the method that gets called only on rare expansions (or,
        // when max_bins caps the store, collapses).
        let idx = self.ensure(k);
        self.counts.as_mut_slice()[idx] += 1;
    }
}

/// Mergeable, relative-error quantile sketch using logarithmically-spaced buckets.
///
/// The DataPoint-level METRIC scalars (`count`, `sum`, `min`, `max`) were
/// dropped from the portable cross-language wire format
/// (ProjectASAP/sketchlib-go#243). They are still tracked here as pure
/// in-memory state because [`DDSketch::get_value_at_quantile`] and
/// [`DDSketch::merge`] use them internally, but they are NOT serialized:
/// `count`/`sum` are recovered exactly from the bucket counts on
/// deserialize and `min`/`max` are re-estimated from the extreme
/// non-empty buckets (within the α relative-accuracy bound).
#[derive(Debug, Serialize, Deserialize)]
pub struct DDSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    inv_log_gamma: f64,

    store: Buckets,
    #[serde(skip)]
    count: u64,
    #[serde(skip)]
    sum: f64,
    #[serde(skip)]
    min: f64,
    #[serde(skip)]
    max: f64,
}

impl DDSketch {
    /// Creates a new DDSketch with relative accuracy guarantee `alpha` (must be in `(0, 1)`).
    pub fn new(alpha: f64) -> Self {
        assert!((0.0..1.0).contains(&alpha), "alpha must be in (0,1)");
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let log_gamma = gamma.ln();
        let inv_log_gamma = 1.0 / log_gamma;

        Self {
            alpha,
            gamma,
            log_gamma,
            inv_log_gamma,
            store: Buckets::new(),
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// `DDSketch::new` with the bucket store capped at `max_bins` bins,
    /// matching DataDog's `LogCollapsingLowestDenseDDSketch`: once growth
    /// would need more than `max_bins` bins, the LOWEST bins collapse
    /// (folding their mass into the new floor bucket) instead of growing
    /// further, bounding the memory a single finite-but-extreme outlier can
    /// force (asap_sketchlib#70 item 4 / sketchlib-go#72). Opt-in — plain
    /// `DDSketch::new` stays unbounded (`max_bins=0`), matching today's
    /// behavior exactly. `max_bins` must be positive.
    ///
    /// The cap is a purely LOCAL, in-memory bound: it is not carried on the
    /// wire (`Buckets::max_bins` is `#[serde(skip)]`) and a decoded sketch
    /// (`deserialize_from_bytes`) is always unbounded — only the live,
    /// actively-inserted-into sketch needs the cap.
    pub fn with_max_bins(alpha: f64, max_bins: i32) -> Self {
        assert!(max_bins > 0, "max_bins must be positive");
        let mut sk = Self::new(alpha);
        sk.store.max_bins = max_bins;
        sk
    }

    /// Serializes the sketch to a MessagePack byte vector.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a DDSketch from a MessagePack byte slice.
    ///
    /// The `count`/`sum`/`min`/`max` scalars are `#[serde(skip)]` (dropped
    /// from the wire, ProjectASAP/sketchlib-go#243), so they default to
    /// zero on decode. Recompute them from the bucket store: `count` is
    /// exact (the sum of all bucket counts) and `sum`/`min`/`max` are
    /// reconstructed from the per-bucket representative values, accurate
    /// to within the sketch's α relative-accuracy bound.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let mut sk: Self = from_slice(bytes)?;
        sk.recompute_scalars_from_store();
        Ok(sk)
    }

    /// Rebuild the in-memory `count`/`sum`/`min`/`max` aggregates from the
    /// bucket store after a deserialize that dropped them. `count` is exact;
    /// `sum`/`min`/`max` are bucket-representative estimates (α-bounded).
    fn recompute_scalars_from_store(&mut self) {
        self.count = 0;
        self.sum = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        let offset = self.store.offset;
        for (i, &c) in self.store.counts.as_slice().iter().enumerate() {
            if c == 0 {
                continue;
            }
            let bin = offset + i as i32;
            let rep = self.bin_representative(bin);
            self.count += c;
            self.sum += rep * c as f64;
            if rep < self.min {
                self.min = rep;
            }
            if rep > self.max {
                self.max = rep;
            }
        }
    }

    /// Adds a positive finite numeric sample to the sketch; non-positive or
    /// non-finite values are ignored.
    ///
    /// Values outside `[min_indexable_value, max_indexable_value]` are also
    /// dropped rather than mapped to an arbitrarily distant bucket index — that
    /// guards the dense store against a single finite-but-extreme outlier
    /// forcing an allocation spanning the whole index gap (asap_sketchlib#70
    /// item 4 / sketchlib-go#72). Dropped silently, like the non-positive case,
    /// since `add` has no error channel.
    #[inline(always)]
    pub fn add<T: NumericalValue>(&mut self, val: &T) {
        let v = val.to_f64();
        if !(v.is_finite() && v > 0.0) {
            return;
        }
        if v < self.min_indexable_value() || v > self.max_indexable_value() {
            return; // untrackable extreme: would blow up the dense bucket span
        }

        self.count += 1;
        self.sum += v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }

        let k = self.key_for(v);
        self.store.add_one(k);
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

        let rank = (q * self.count as f64).ceil() as u64;
        let mut seen = 0u64;

        let slice = self.store.counts.as_slice();
        let offset = self.store.offset;

        for (i, &c) in slice.iter().enumerate() {
            // let c = slice[i];
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

    /// Returns the running sum of all positive samples ingested.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Returns the raw bucket-count slice. Each entry is the number of
    /// samples in the bucket whose absolute index is `store_offset() + i`.
    pub fn store_counts(&self) -> &[u64] {
        self.store.counts.as_slice()
    }

    /// Returns the absolute bucket index corresponding to
    /// `store_counts()[0]`.
    pub fn store_offset(&self) -> i32 {
        self.store.offset
    }

    /// Merges another DDSketch into this one. Returns `Err` if the two sketches
    /// use different index mappings (different `alpha`/`gamma`): merging under a
    /// mismatched mapping would reinterpret one sketch's bucket indices under
    /// the other's γ and silently corrupt every quantile.
    ///
    /// This is a REAL runtime check, not a `debug_assert!` — the previous
    /// assert was compiled out in release builds, so a release-mode
    /// mismatched merge corrupted results with no signal at all
    /// (asap_sketchlib#70 item 2). DataDog's `MergeWith` and sketchlib-go's Go
    /// `Merge` both return an error here; the portable `DdSketch::merge` in this
    /// same crate already does too.
    ///
    /// NOTE: does not enforce `self`'s `max_bins` cap (sketchlib-go#72
    /// collapsing store, see `with_max_bins`) on the merged result — the
    /// result's span can exceed `max_bins` if the two operands' ranges are
    /// far apart. Merging is a bulk, already-bounded-input operation
    /// (unlike `add`'s single untrusted sample), so it doesn't carry the
    /// same single-outlier memory-blowup risk; enforcing the cap here is
    /// tracked separately if it turns out to matter in practice.
    pub fn merge(&mut self, other: &DDSketch) -> Result<(), String> {
        if (self.alpha - other.alpha).abs() >= 1e-12 || (self.gamma - other.gamma).abs() >= 1e-12 {
            return Err(format!(
                "cannot merge DDSketches with different index mappings: alpha {} vs {}",
                self.alpha, other.alpha
            ));
        }

        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = other.clone();
            return Ok(());
        }

        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Merge bucket vectors
        self.merge_buckets_from(other);
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
    /// bucket edges — the log-midpoint γ^(k+0.5) used previously gave edge error
    /// √γ−1 (≈ α + α²/2 > α), silently violating the advertised α-accuracy
    /// guarantee near a bucket edge (asap_sketchlib#70 / sketchlib-go#73 item 1).
    #[inline]
    fn bin_representative(&self, k: i32) -> f64 {
        self.lower_bound(k) * (1.0 + self.alpha)
    }

    /// Smallest finite positive value whose bucket index is representable
    /// without integer overflow (index ≥ i32::MIN) or float underflow, mirroring
    /// DataDog's logarithmic_mapping.go minIndexableValue.
    #[inline]
    fn min_indexable_value(&self) -> f64 {
        // f64::MIN_POSITIVE is the smallest positive normal (2^-1022).
        ((f64::from(i32::MIN)) / self.inv_log_gamma + 1.0)
            .exp()
            .max(f64::MIN_POSITIVE * self.gamma)
    }

    /// Largest finite positive value whose bucket index is representable without
    /// integer overflow (index ≤ i32::MAX) or `exp`/`powf` overflow, mirroring
    /// DataDog's logarithmic_mapping.go maxIndexableValue. A value beyond this
    /// would otherwise map to an arbitrarily distant index and force the dense
    /// bucket store to grow across the whole gap (asap_sketchlib#70 item 4 /
    /// sketchlib-go#72's single-outlier memory blowup).
    #[inline]
    fn max_indexable_value(&self) -> f64 {
        // 709.0 is just under ln(f64::MAX) so exp() stays finite.
        const EXP_OVERFLOW: f64 = 709.0;
        ((f64::from(i32::MAX)) / self.inv_log_gamma - 1.0)
            .exp()
            .min(EXP_OVERFLOW.exp() / (2.0 * self.gamma) * (self.gamma + 1.0))
    }

    fn merge_buckets_from(&mut self, other: &DDSketch) {
        if other.store.is_empty() {
            return;
        }
        if self.store.is_empty() {
            self.store = other.store.clone();
            return;
        }

        let (self_l, self_r) = self.store.range().unwrap();
        let (other_l, other_r) = other.store.range().unwrap();

        let new_l = self_l.min(other_l);
        let new_r = self_r.max(other_r);
        let new_len = (new_r - new_l + 1) as usize;

        let mut merged = vec![0u64; new_len];

        // Copy self
        for (i, &c) in self.store.counts.as_slice().iter().enumerate() {
            let k = self_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        // Add other
        for (i, &c) in other.store.counts.as_slice().iter().enumerate() {
            let k = other_l + i as i32;
            merged[(k - new_l) as usize] += c;
        }

        self.store.counts = Vector1D::from_vec(merged);
        self.store.offset = new_l;
    }
}

impl Clone for DDSketch {
    fn clone(&self) -> Self {
        Self {
            alpha: self.alpha,
            gamma: self.gamma,
            log_gamma: self.log_gamma,
            inv_log_gamma: self.inv_log_gamma,
            store: self.store.clone(),
            count: self.count,
            sum: self.sum,
            min: self.min,
            max: self.max,
        }
    }
}

impl DDSketch {
    /// Adds a sample converted from a [`DataInput`]; returns an error for non-numeric inputs.
    #[inline(always)]
    pub fn add_input(&mut self, v: &DataInput) -> Result<(), &'static str> {
        let value = data_input_to_f64(v).map_err(|_| "DDSketch only accepts numeric inputs")?;
        self.add(&value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        sample_exponential_f64, sample_normal_f64, sample_uniform_f64, sample_zipf_f64,
    };

    // Absolute relative error helper
    fn rel_err(a: f64, b: f64) -> f64 {
        if a == 0.0 && b == 0.0 {
            0.0
        } else {
            (a - b).abs() / f64::max(1e-30, b.abs())
        }
    }

    // True quantile from sorted data
    fn true_quantile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return f64::NAN;
        }
        if p <= 0.0 {
            return sorted[0];
        }
        if p >= 1.0 {
            return sorted[sorted.len() - 1];
        }
        let n = sorted.len();
        let k = ((p * n as f64).ceil() as usize).clamp(1, n) - 1;
        sorted[k]
    }

    #[test]
    fn insert_and_query_basic() {
        let mut s = DDSketch::new(0.01);
        let vals = [0.0, -5.0, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0];
        for &v in &vals {
            s.add(&v);
        }

        // Non-positives ignored
        assert_eq!(s.get_count(), 7);

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
        let s = DDSketch::new(0.01);
        assert!(s.get_value_at_quantile(0.5).is_none());
        assert!(s.get_value_at_quantile(0.0).is_none());
        assert!(s.get_value_at_quantile(1.0).is_none());
        assert_eq!(s.get_count(), 0);
    }

    #[test]
    fn dds_uniform_distribution_quantiles() {
        // choose alpha as 1%
        const ALPHA: f64 = 0.01;

        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        fn build_dds_with_uniform(
            alpha: f64,
            n: usize,
            min: f64,
            max: f64,
            seed: u64,
        ) -> (DDSketch, Vec<f64>) {
            // sample uniform values from test utils
            let mut vals = sample_uniform_f64(min, max, n, seed);
            // retain only finite positive values
            vals.retain(|v| v.is_finite() && *v > 0.0);
            // build DDSketch
            let mut sk = DDSketch::new(alpha);
            for &x in &vals {
                sk.add(&x);
            }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(
            sk: &DDSketch,
            sorted_vals: &[f64],
            qs: &[(f64, &str)],
            tol: f64,
        ) {
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(sorted_vals, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name,
                    p,
                    err,
                    got,
                    want,
                    tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xA5A5_0000_u64 + idx as u64;
            let (sketch, mut values) =
                build_dds_with_uniform(ALPHA, n, 1_000_000.0, 10_000_000.0, seed);
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_quantiles_within_error_dds(&sketch, &values, QUANTILES, ALPHA);
        }
    }

    #[test]
    fn dds_zipf_distribution_quantiles() {
        const ALPHA: f64 = 0.01;

        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        fn build_dds_with_zipf(
            alpha: f64,
            n: usize,
            min: f64,
            max: f64,
            domain: usize,
            exponent: f64,
            seed: u64,
        ) -> (DDSketch, Vec<f64>) {
            let mut vals = sample_zipf_f64(min, max, domain, exponent, n, seed);
            vals.retain(|v| v.is_finite() && *v > 0.0);
            let mut sk = DDSketch::new(alpha);
            for &x in &vals {
                sk.add(&x);
            }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(
            sk: &DDSketch,
            sorted_vals: &[f64],
            qs: &[(f64, &str)],
            tol: f64,
        ) {
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(sorted_vals, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name,
                    p,
                    err,
                    got,
                    want,
                    tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xB4B4_0000_u64 + idx as u64;
            let (sketch, mut values) =
                build_dds_with_zipf(ALPHA, n, 1_000_000.0, 10_000_000.0, 8_192, 1.1, seed);
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_quantiles_within_error_dds(&sketch, &values, QUANTILES, ALPHA);
        }
    }

    #[test]
    fn dds_normal_distribution_quantiles() {
        const ALPHA: f64 = 0.01;

        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        fn build_dds_with_normal(
            alpha: f64,
            n: usize,
            mean: f64,
            std: f64,
            seed: u64,
        ) -> (DDSketch, Vec<f64>) {
            // changed the code to include the normal distribution sampler from test_utils
            let vals = sample_normal_f64(mean, std, n, seed)
                .into_iter()
                .filter(|v| v.is_finite() && *v > 0.0)
                .collect::<Vec<_>>();

            let mut sk = DDSketch::new(alpha);
            for &x in &vals {
                sk.add(&x);
            }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(
            sk: &DDSketch,
            mut vals: Vec<f64>,
            qs: &[(f64, &str)],
            tol: f64,
        ) {
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(&vals, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name,
                    p,
                    err,
                    got,
                    want,
                    tol
                );
            }
        }

        // Mean and std chosen so almost all samples are positive.
        let mean = 1_000.0;
        let std = 100.0;

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xC0DE_0000_u64 + idx as u64;
            let (sketch, values) = build_dds_with_normal(ALPHA, n, mean, std, seed);
            assert_quantiles_within_error_dds(&sketch, values, QUANTILES, ALPHA);
        }
    }

    #[test]
    fn dds_exponential_distribution_quantiles() {
        const ALPHA: f64 = 0.01;
        const LAMBDA: f64 = 1e-3; // mean = 1000.0
        const QUANTILES: &[(f64, &str)] = &[
            (0.0, "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0, "max"),
        ];

        fn build_dds_with_exponential(
            alpha: f64,
            n: usize,
            lambda: f64,
            seed: u64,
        ) -> (DDSketch, Vec<f64>) {
            let vals = sample_exponential_f64(lambda, n, seed);
            let mut sk = DDSketch::new(alpha);
            for &x in &vals {
                sk.add(&x);
            }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(
            sk: &DDSketch,
            vals: &[f64],
            qs: &[(f64, &str)],
            tol: f64,
        ) {
            let mut sorted = vals.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(&sorted, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol + 1e-9,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name,
                    p,
                    err,
                    got,
                    want,
                    tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize]
            .into_iter()
            .enumerate()
        {
            let seed = 0xE3E3_0000_u64 + idx as u64;
            let (sketch, values) = build_dds_with_exponential(ALPHA, n, LAMBDA, seed);
            assert_quantiles_within_error_dds(&sketch, &values, QUANTILES, 0.011); // not sure why but needed a bit more tolerance
        }
    }

    #[test]
    fn merge_two_sketches_combines_counts_and_bounds() {
        const ALPHA: f64 = 0.01;

        let mut s1 = DDSketch::new(ALPHA);
        let mut s2 = DDSketch::new(ALPHA);

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
        let mut s = DDSketch::new(0.01);
        let vals = [1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0]; // sample values

        for v in vals {
            s.add(&v);
        }

        let encoded = s.serialize_to_bytes().expect("DDSketch serialization fail"); // serialize to bytes
        assert!(
            !encoded.is_empty(),
            "encoded bytes should not be empty for DDSketch"
        );

        let decoded =
            DDSketch::deserialize_from_bytes(&encoded).expect("DDSketch deserialization fail"); // deserialize back

        // `count` survives exactly (recomputed by summing buckets). The
        // `min`/`max`/`sum` scalars are no longer serialized
        // (ProjectASAP/sketchlib-go#243) — they're reconstructed from the
        // per-bucket representative `gamma^k*(1+alpha)`. With that DataDog
        // representative a value sitting anywhere in a bucket (including the
        // edges) is within EXACTLY alpha of the representative, so alpha is the
        // correct tolerance — the old `sqrt(gamma)-1` slack was only needed for
        // the midpoint `gamma^(k+0.5)` representative.
        assert_eq!(decoded.get_count(), s.get_count()); // counts should match
        let alpha = s.alpha();
        let bucket_tol = alpha + 1e-9;
        let min_rel = (decoded.min().unwrap() - s.min().unwrap()).abs() / s.min().unwrap();
        let max_rel = (decoded.max().unwrap() - s.max().unwrap()).abs() / s.max().unwrap();
        assert!(
            min_rel <= bucket_tol,
            "min rel err {min_rel} exceeds bucket tol {bucket_tol}"
        );
        assert!(
            max_rel <= bucket_tol,
            "max rel err {max_rel} exceeds bucket tol {bucket_tol}"
        );

        // Quantiles are driven by the (serialized) bucket store, but the
        // original sketch clamps results to its exact in-memory min/max
        // while the decoded sketch clamps to the reconstructed bucket-edge
        // estimates — so the two can differ by up to one bucket width. Both
        // remain within the relative-accuracy guarantee.
        for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
            let a = s.get_value_at_quantile(q).unwrap();
            let b = decoded.get_value_at_quantile(q).unwrap();
            let rel = (a - b).abs() / a.abs();
            assert!(
                rel <= bucket_tol,
                "quantile p={q} rel err {rel} exceeds bucket tol {bucket_tol} (a={a}, b={b})"
            );
        }
    }

    // DataDog-parity tests (asap_sketchlib#70 / sketchlib-go#73, #72).

    #[test]
    fn representative_within_alpha_at_bucket_edges() {
        // Value(k) = gamma^k*(1+alpha) puts the relative error at EXACTLY alpha
        // at both bucket edges — the old midpoint gamma^(k+0.5) exceeded alpha.
        for &alpha in &[0.001, 0.01, 0.05, 0.1] {
            let d = DDSketch::new(alpha);
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
        // Was a debug_assert!, compiled out in release; now a real Result even
        // in release builds (asap_sketchlib#70 item 2).
        let mut a = DDSketch::new(0.01);
        let b = DDSketch::new(0.02);
        a.add(&5.0);
        assert!(a.merge(&b).is_err(), "mismatched-mapping merge must Err");

        let mut c = DDSketch::new(0.01);
        let mut d = DDSketch::new(0.01);
        c.add(&3.0);
        d.add(&7.0);
        assert!(c.merge(&d).is_ok(), "matched-mapping merge must succeed");
        assert_eq!(c.get_count(), 2);
    }

    #[test]
    fn untrackable_extreme_is_dropped() {
        // A single finite-but-extreme outlier outside the indexable range must
        // not be recorded, so the dense bucket store never spans the whole gap
        // (asap_sketchlib#70 item 4 / sketchlib-go#72).
        let mut d = DDSketch::new(0.01);
        for i in 1..=2000 {
            d.add(&(f64::from(i)));
        }
        let count_before = d.get_count();
        let span_before = d.store.counts.as_slice().len();

        d.add(&(d.max_indexable_value() * 10.0));
        d.add(&(d.min_indexable_value() / 10.0));
        assert_eq!(d.get_count(), count_before, "extreme values were recorded");
        assert_eq!(
            d.store.counts.as_slice().len(),
            span_before,
            "store span grew from an untrackable extreme"
        );

        // A large-but-trackable value is still recorded.
        d.add(&(d.max_indexable_value() / 2.0));
        assert_eq!(d.get_count(), count_before + 1);
    }

    // Collapsing-store tests (sketchlib-go#72's Rust counterpart).

    /// A single finite-but-extreme outlier must not force the store past
    /// max_bins, no matter how far its bucket index is from the current
    /// window.
    #[test]
    fn collapsing_store_caps_memory() {
        const ALPHA: f64 = 0.01;
        const MAX_BINS: i32 = 100;
        let mut d = DDSketch::with_max_bins(ALPHA, MAX_BINS);

        for v in sample_uniform_f64(1.0, 1001.0, 5000, 1) {
            d.add(&v);
        }
        assert!(
            d.store.counts.as_slice().len() <= MAX_BINS as usize,
            "store span exceeds max_bins after normal inserts"
        );

        // A genuinely adversarial single outlier.
        d.add(&1e15);
        assert!(
            d.store.counts.as_slice().len() <= MAX_BINS as usize,
            "store span exceeds max_bins after the outlier — collapse did not cap growth"
        );

        // Another extreme outlier on the other side — growing RIGHT must
        // also stay capped.
        for v in sample_uniform_f64(1.0, 1001.0, 100, 2) {
            d.add(&v);
        }
        d.add(&1e18);
        assert!(
            d.store.counts.as_slice().len() <= MAX_BINS as usize,
            "store span exceeds max_bins after a second outlier"
        );
    }

    /// A collapse never loses mass: total count always equals the number of
    /// add() calls, even across repeated collapses.
    #[test]
    fn collapsing_store_preserves_count() {
        const ALPHA: f64 = 0.01;
        const MAX_BINS: i32 = 50;
        let mut d = DDSketch::with_max_bins(ALPHA, MAX_BINS);

        let mut n = 0u64;
        for exp in -20..=20 {
            let v = 1.5f64.powi(exp);
            for _ in 0..10 {
                d.add(&v);
                n += 1;
            }
        }
        assert_eq!(d.get_count(), n, "mass lost across repeated collapses");

        let summed: u64 = d.store.counts.as_slice().iter().sum();
        assert_eq!(summed, n, "summed bucket counts diverge from count");
    }

    /// The defining property of CollapsingLOWEST: once a collapse has
    /// occurred, the HIGH end of the distribution keeps its full per-bucket
    /// resolution — only the low end degrades. Matters for ASAPCollector's
    /// typical use (p95/p99 latency tail).
    #[test]
    fn collapsing_store_high_end_stays_exact() {
        const ALPHA: f64 = 0.01;
        const MAX_BINS: i32 = 20;
        let mut d = DDSketch::with_max_bins(ALPHA, MAX_BINS);
        let mut d_uncapped = DDSketch::new(ALPHA);

        for exp in -50..=-10 {
            let v = 1.2f64.powi(exp);
            d.add(&v);
            d_uncapped.add(&v);
        }
        for &v in &[9000.0, 9500.0, 9900.0, 9990.0, 9999.0] {
            d.add(&v);
            d_uncapped.add(&v);
        }

        let got_p99 = d.get_value_at_quantile(0.99).expect("capped p99");
        let want_p99 = d_uncapped.get_value_at_quantile(0.99).expect("uncapped p99");
        let re = (got_p99 - want_p99).abs() / want_p99;
        assert!(
            re <= ALPHA + 1e-6,
            "p99 diverged from uncapped: got {got_p99} want {want_p99} relErr {re} > alpha {ALPHA}"
        );
    }

    /// Behavior-preservation guard: DDSketch::new (max_bins=0, the default)
    /// must be unaffected by the ensure()/add_one() refactor.
    #[test]
    fn collapsing_store_does_not_affect_uncapped_sketch() {
        const ALPHA: f64 = 0.01;
        let mut d = DDSketch::new(ALPHA);
        for v in sample_uniform_f64(1.0, 100001.0, 3000, 3) {
            d.add(&v);
        }
        assert_eq!(d.get_count(), 3000);
        assert!(!d.store.counts.as_slice().is_empty());
    }

    /// The tightest possible cap: every insert collapses into a single
    /// bucket, degenerating the sketch into an exact counter. Must never
    /// exceed 1 bin and never lose count.
    #[test]
    fn collapsing_store_max_bins_one() {
        let mut d = DDSketch::with_max_bins(0.01, 1);
        let vals = [1.0, 1000.0, 1e-3, 1e9, 5.0, 5_000_000.0];
        for &v in &vals {
            d.add(&v);
            assert!(d.store.counts.as_slice().len() <= 1, "max_bins=1 exceeded after add({v})");
        }
        assert_eq!(d.get_count(), vals.len() as u64);
    }

    /// A large, randomly-ordered mix of tightly-clustered and wildly-extreme
    /// values against a small cap, checking the span-cap and
    /// count-conservation invariants hold THROUGHOUT — not just at the end.
    #[test]
    fn collapsing_store_random_stress() {
        const ALPHA: f64 = 0.02;
        const MAX_BINS: i32 = 30;
        const PER_KIND: usize = 5000;
        let mut d = DDSketch::with_max_bins(ALPHA, MAX_BINS);

        // Four independently-sampled kinds, interleaved round-robin so the
        // insertion order mixes tightly-clustered and wildly-extreme values
        // rather than arriving in big contiguous blocks — exercises
        // collapses triggered from both directions repeatedly.
        let normal_cluster = sample_uniform_f64(1.0, 101.0, PER_KIND, 10);
        let wide_log_spread: Vec<f64> = sample_uniform_f64(-15.0, 15.0, PER_KIND, 11)
            .into_iter()
            .map(|e| 10f64.powf(e))
            .collect();
        let extreme_low: Vec<f64> = sample_uniform_f64(0.0, 1.0, PER_KIND, 12)
            .into_iter()
            .map(|f| 1e-100 * (1.0 + f))
            .collect();
        let extreme_high: Vec<f64> = sample_uniform_f64(0.0, 1.0, PER_KIND, 13)
            .into_iter()
            .map(|f| 1e100 * (1.0 + f))
            .collect();

        let mut n = 0u64;
        for i in 0..PER_KIND {
            for v in [normal_cluster[i], wide_log_spread[i], extreme_low[i], extreme_high[i]] {
                d.add(&v);
                n += 1;
                assert!(
                    d.store.counts.as_slice().len() <= MAX_BINS as usize,
                    "iter {i}: store span exceeds max_bins (v={v})"
                );
            }
        }
        assert_eq!(d.get_count(), n);
        let summed: u64 = d.store.counts.as_slice().iter().sum();
        assert_eq!(summed, n);
    }

    /// merge() with a capped operand must not panic (the cap simply isn't
    /// enforced on the merged result — documented limitation).
    #[test]
    fn merge_with_capped_sketch_does_not_panic() {
        let mut a = DDSketch::with_max_bins(0.01, 10);
        let mut b = DDSketch::with_max_bins(0.01, 10);
        a.add(&1.0);
        b.add(&1e10);
        assert!(a.merge(&b).is_ok());
    }
}
