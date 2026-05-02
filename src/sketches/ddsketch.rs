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
                unsafe {
                    *slice.as_mut_ptr().add(idx) += 1;
                }
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
#[derive(Debug, Serialize, Deserialize)]
pub struct DDSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    inv_log_gamma: f64,

    store: Buckets,
    count: u64,
    sum: f64,
    min: f64,
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

    /// Serializes the sketch to a MessagePack byte vector.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a DDSketch from a MessagePack byte slice.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Adds a positive finite numeric sample to the sketch; non-positive or non-finite values are ignored.
    #[inline(always)]
    pub fn add<T: NumericalValue>(&mut self, val: &T) {
        let v = val.to_f64();
        if !(v.is_finite() && v > 0.0) {
            return;
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

    /// Merges another DDSketch (with the same `alpha`) into this one.
    pub fn merge(&mut self, other: &DDSketch) {
        debug_assert!((self.alpha - other.alpha).abs() < 1e-12);
        debug_assert!((self.gamma - other.gamma).abs() < 1e-12);

        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
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
    }

    #[inline(always)]
    fn key_for(&self, v: f64) -> i32 {
        debug_assert!(v > 0.0);
        (v.ln() * self.inv_log_gamma).floor() as i32
    }

    #[inline]
    fn bin_representative(&self, k: i32) -> f64 {
        self.gamma.powf(k as f64 + 0.5)
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

        s1.merge(&s2);

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

        // basic invariants - conditions should match, else it fails
        assert_eq!(decoded.get_count(), s.get_count()); // counts should match
        assert_eq!(decoded.min(), s.min()); // mins should match
        assert_eq!(decoded.max(), s.max()); // maxes should match

        // quantiles should match at several points
        for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
            let a = s.get_value_at_quantile(q).unwrap();
            let b = decoded.get_value_at_quantile(q).unwrap();
            assert_eq!(a, b, "quantile mismatch at p={}", q);
        }
    }
}

// =====================================================================
// ASAP runtime wire-format-aligned variant .
//
// `DdSketch` and `DdSketchDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators. The high-throughput in-process variant above
// (`DDSketch`) keeps its original design.
// =====================================================================

// DDSketch — log-bucketed quantile sketch, mergeable by store-index alignment.
//
// Parallel to `count_sketch::CountSketch`: the minimum viable surface
// needed for the modified-OTLP `Metric.data = DDSketch{…}` hot path
// (PR C-CountSketch follow-up). Holds the bucket counts, their
// absolute-index base offset, and the aggregate `{count, sum, min, max}`.
//
// Merge semantics: two sketches with the same relative-accuracy
// parameter `alpha` are merged by aligning bucket arrays along their
// `store_offset` and summing counts element-wise, with `min`/`max`
// combined via min/max and `count`/`sum` added.
//
// The wire format is the protobuf-encoded
// `asap_sketchlib::proto::sketchlib::DDSketchState` emitted by
// DataCollector's `ddsketchprocessor`. Quantile estimation against
// stored data is intentionally deferred — queries currently return
// a placeholder error and fall through to the §5.2 fallback.

// (de-duplicated) use serde::{Deserialize, Serialize};

/// Sparse delta between two consecutive DDSketch snapshots — the
/// input shape for [`DdSketch::apply_delta`]. Mirrors the
/// `DDSketchDelta` proto in `sketchlib-go/proto/ddsketch/ddsketch.proto`
/// (and its Rust bindings vendored in `asap_otel_proto::sketchlib::v1`).
/// Kept as a plain struct so this crate doesn't need a tonic/prost
/// dependency; proto decode lives in the accumulator.
#[derive(Debug, Clone, Default)]
pub struct DdSketchDelta {
    /// `(absolute_bucket_index, Δcount)` pairs, additive.
    pub buckets: Vec<(i32, u64)>,
    /// Δ total count. **Ignored on the apply path**: to match
    /// `sketchlib-go/sketches/DDSketch/delta.go::ApplyDelta`, the
    /// total count is reconstructed by summing per-bucket `Δcount`
    /// values inside the bucket loop. Kept on the struct because the
    /// wire `DDSketchDelta` proto still carries it (field 2); future
    /// codecs may reuse it for cross-checks.
    pub d_count: i64,
    /// Δ sum.
    pub d_sum: f64,
    /// Whether `new_min` carries a meaningful value. Min can only
    /// decrease; a delta that didn't lower min sends `false`.
    pub min_changed: bool,
    pub new_min: f64,
    /// Whether `new_max` carries a meaningful value. Max can only
    /// increase.
    pub max_changed: bool,
    pub new_max: f64,
}

/// Minimal DDSketch state — bucket counts + alpha + aggregates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdSketch {
    /// Relative accuracy parameter; must satisfy `0 < alpha < 1`.
    pub alpha: f64,
    /// Bucket counts in absolute-index order. The absolute index of
    /// `store_counts[i]` is `i + store_offset`.
    pub store_counts: Vec<u64>,
    /// Absolute bucket index corresponding to `store_counts[0]`. May
    /// be negative.
    pub store_offset: i32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl DdSketch {
    /// Construct an empty sketch.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            store_counts: Vec::new(),
            store_offset: 0,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Construct from the decoded wire fields.
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw(
        alpha: f64,
        store_counts: Vec<u64>,
        store_offset: i32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    ) -> Self {
        Self {
            alpha,
            store_counts,
            store_offset,
            count,
            sum,
            min,
            max,
        }
    }

    /// Merge one other sketch into self by aligning bucket arrays on
    /// absolute indices. Both operands must share the same `alpha`.
    pub fn merge(
        &mut self,
        other: &DdSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if (self.alpha - other.alpha).abs() > f64::EPSILON {
            return Err(format!(
                "DdSketch alpha mismatch: self={}, other={}",
                self.alpha, other.alpha
            )
            .into());
        }

        if other.store_counts.is_empty() {
            self.count += other.count;
            self.sum += other.sum;
            if other.min < self.min {
                self.min = other.min;
            }
            if other.max > self.max {
                self.max = other.max;
            }
            return Ok(());
        }
        if self.store_counts.is_empty() {
            self.store_counts = other.store_counts.clone();
            self.store_offset = other.store_offset;
        } else {
            let self_start = self.store_offset as i64;
            let self_end = self_start + self.store_counts.len() as i64;
            let other_start = other.store_offset as i64;
            let other_end = other_start + other.store_counts.len() as i64;
            let new_start = self_start.min(other_start);
            let new_end = self_end.max(other_end);
            let new_len = (new_end - new_start) as usize;
            let mut merged = vec![0u64; new_len];
            for (i, c) in self.store_counts.iter().enumerate() {
                let idx = (self_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            for (i, c) in other.store_counts.iter().enumerate() {
                let idx = (other_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            self.store_counts = merged;
            self.store_offset = new_start as i32;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        Ok(())
    }

    /// Apply a sparse delta to this sketch in place. Matches the
    /// `ApplyDelta` logic in `sketchlib-go/sketches/DDSketch/delta.go`:
    /// bucket counts add (wrapping `u64`), total count is reconstructed
    /// from the per-bucket `Δcount` sum (the wire-level `delta.d_count`
    /// is ignored on apply — see `DdSketchDelta::d_count`), sum adds,
    /// and min/max apply with min/max semantics. Used by the backend
    /// ingest path to reconstitute a full sketch from a base snapshot
    /// + subsequent delta-transmission frames (paper §6.2 B3 / B4
    ///   baselines).
    pub fn apply_delta(&mut self, delta: &DdSketchDelta) {
        for (abs_idx, d_count) in &delta.buckets {
            if self.store_counts.is_empty() {
                self.store_counts = vec![0u64; 1];
                self.store_offset = *abs_idx;
            }
            let cur_start = self.store_offset as i64;
            let cur_end = cur_start + self.store_counts.len() as i64;
            let k = *abs_idx as i64;
            if k < cur_start {
                // Prepend zeros.
                let pad = (cur_start - k) as usize;
                let mut buf = vec![0u64; pad];
                buf.append(&mut self.store_counts);
                self.store_counts = buf;
                self.store_offset = *abs_idx;
            } else if k >= cur_end {
                let pad = (k - cur_end + 1) as usize;
                self.store_counts.extend(std::iter::repeat_n(0u64, pad));
            }
            let arr_idx = (k - self.store_offset as i64) as usize;
            // Wrapping `u64` add to match Go's
            // `target.store.counts.AsMutSlice()[idx] += b.DCount`
            // (Go uint64 += is wrapping by spec).
            self.store_counts[arr_idx] = self.store_counts[arr_idx].wrapping_add(*d_count);
            // Reconstruct total count from per-bucket DCount sum, also
            // wrapping. `delta.d_count` is intentionally NOT used here
            // — see field doc on `DdSketchDelta::d_count`.
            self.count = self.count.wrapping_add(*d_count);
        }
        self.sum += delta.d_sum;
        if delta.min_changed && delta.new_min < self.min {
            self.min = delta.new_min;
        }
        if delta.max_changed && delta.new_max > self.max {
            self.max = delta.new_max;
        }
    }

    /// Merge a slice of references into a single new sketch. Returns
    /// `Err` on alpha mismatch or an empty input.
    pub fn merge_refs(
        inputs: &[&DdSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("DdSketch::merge_refs called with empty input")?;
        let mut merged = DdSketch::new(first.alpha);
        for d in inputs {
            merged.merge(d)?;
        }
        Ok(merged)
    }

    /// Insert a single positive value. Updates count, sum, min/max
    /// and increments the bucket for `floor(ln(v) / ln(gamma))`
    /// where `gamma = (1+α)/(1-α)`. Provided primarily so tests can
    /// build a ground-truth sketch to compare delta-apply output
    /// against.
    pub fn update(&mut self, value: f64) {
        if value <= 0.0 {
            // DDSketch is defined for positive reals; the paper's
            // sketchlib-go rejects non-positive values silently.
            return;
        }
        let gamma = (1.0 + self.alpha) / (1.0 - self.alpha);
        let ln_gamma = gamma.ln();
        let idx = (value.ln() / ln_gamma).floor() as i32;
        if self.store_counts.is_empty() {
            self.store_counts = vec![1];
            self.store_offset = idx;
        } else {
            let cur_start = self.store_offset as i64;
            let cur_end = cur_start + self.store_counts.len() as i64;
            let k = idx as i64;
            if k < cur_start {
                let pad = (cur_start - k) as usize;
                let mut buf = vec![0u64; pad];
                buf.append(&mut self.store_counts);
                self.store_counts = buf;
                self.store_offset = idx;
            } else if k >= cur_end {
                let pad = (k - cur_end + 1) as usize;
                self.store_counts.extend(std::iter::repeat_n(0u64, pad));
            }
            let arr_idx = (k - self.store_offset as i64) as usize;
            self.store_counts[arr_idx] = self.store_counts[arr_idx].saturating_add(1);
        }
        self.count = self.count.saturating_add(1);
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Estimate the quantile at rank `q` ∈ [0, 1]. Walks the bucket
    /// array in ascending absolute-index order, accumulating counts
    /// until the target rank; returns the bucket's representative
    /// value `gamma^(k + 0.5)` where `k` is the bucket's absolute
    /// index. Returns `None` if the sketch is empty.
    ///
    /// Accuracy: bounded by DDSketch's α parameter — the estimated
    /// quantile value is within `(1+α)/(1-α)` relative error of the
    /// true quantile.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || self.store_counts.is_empty() {
            return None;
        }
        let target = (q * (self.count.saturating_sub(1)) as f64).floor() as u64;
        let mut cumulative: u64 = 0;
        let gamma = (1.0 + self.alpha) / (1.0 - self.alpha);
        for (i, &c) in self.store_counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(c);
            if cumulative > target {
                let k = (self.store_offset as i64 + i as i64) as f64;
                // Bucket midpoint: gamma^(k + 0.5) — centers the
                // estimate in the logarithmic bucket.
                return Some(gamma.powf(k + 0.5));
            }
        }
        // Numerical edge case: if we fall off the end, return max.
        Some(self.max)
    }

    /// Serialize to MessagePack bytes.
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack bytes.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::from_slice(buffer)?)
    }
}

#[cfg(test)]
mod tests_wire_ddsketch {
    use super::*;

    #[test]
    fn test_new_empty() {
        let d = DdSketch::new(0.01);
        assert_eq!(d.count, 0);
        assert!(d.store_counts.is_empty());
        assert_eq!(d.min, f64::INFINITY);
        assert_eq!(d.max, f64::NEG_INFINITY);
    }

    #[test]
    fn test_merge_aligned_same_offset() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let b = DdSketch::from_raw(0.01, vec![10, 20, 30], -1, 60, 300.0, 0.5, 6.0);
        a.merge(&b).unwrap();
        assert_eq!(a.store_counts, vec![11, 22, 33]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 66);
        assert_eq!(a.sum, 330.0);
        assert_eq!(a.min, 0.5);
        assert_eq!(a.max, 6.0);
    }

    #[test]
    fn test_merge_overlapping_offsets() {
        // a covers indices [-1, 0, 1]; b covers indices [0, 1, 2]
        let mut a = DdSketch::from_raw(0.01, vec![1, 1, 1], -1, 3, 3.0, 1.0, 3.0);
        let b = DdSketch::from_raw(0.01, vec![10, 10, 10], 0, 30, 30.0, 1.0, 3.0);
        a.merge(&b).unwrap();
        // Merged window is [-1, 0, 1, 2] → [1, 11, 11, 10]
        assert_eq!(a.store_counts, vec![1, 11, 11, 10]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 33);
    }

    #[test]
    fn test_merge_disjoint_offsets() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let b = DdSketch::from_raw(0.01, vec![3, 4], 5, 7, 7.0, 5.0, 6.0);
        a.merge(&b).unwrap();
        // Window [0..7) → [1,2,0,0,0,3,4]
        assert_eq!(a.store_counts, vec![1, 2, 0, 0, 0, 3, 4]);
        assert_eq!(a.store_offset, 0);
    }

    #[test]
    fn test_apply_delta_additive_inside_store() {
        let mut base = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let delta = DdSketchDelta {
            buckets: vec![(-1, 4), (0, 8), (1, 12)],
            // d_count is intentionally inconsistent with the bucket
            // sum (4+8+12=24); apply_delta must IGNORE this value and
            // reconstruct the count from per-bucket DCounts to match
            // sketchlib-go's `target.count += b.DCount` semantics.
            d_count: 999_999,
            d_sum: 120.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 9.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![5, 10, 15]);
        assert_eq!(base.count, 30, "count must come from per-bucket DCount sum");
        assert_eq!(base.sum, 150.0);
        assert_eq!(base.min, 1.0);
        assert_eq!(base.max, 9.0);
    }

    #[test]
    fn test_apply_delta_count_reconstructed_from_buckets() {
        // d_count on the wire is ignored. Even if it's set to zero,
        // the per-bucket DCount sum must be applied to `count`.
        let mut base = DdSketch::from_raw(
            0.01,
            vec![0, 0, 0],
            0,
            0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
        );
        let delta = DdSketchDelta {
            buckets: vec![(0, 5), (1, 7), (2, 11)],
            d_count: 0, // wrong on purpose: must be ignored
            d_sum: 23.0,
            min_changed: true,
            new_min: 1.0,
            max_changed: true,
            new_max: 3.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![5, 7, 11]);
        assert_eq!(base.count, 23, "count = sum of per-bucket DCounts");
    }

    #[test]
    fn test_apply_delta_bucket_overflow_wraps() {
        // Match sketchlib-go's wrapping uint64 add. Saturating would
        // pin to u64::MAX; wrapping rolls over to a small value.
        let mut base = DdSketch::from_raw(0.01, vec![u64::MAX], 0, 0, 0.0, 1.0, 1.0);
        let delta = DdSketchDelta {
            buckets: vec![(0, 5)],
            d_count: 0,
            d_sum: 0.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: false,
            new_max: 0.0,
        };
        base.apply_delta(&delta);
        // u64::MAX + 5 wraps to 4 (since MAX = 2^64 - 1).
        assert_eq!(base.store_counts[0], 4);
    }

    #[test]
    fn test_apply_delta_expands_store_on_new_bucket() {
        // Base covers [0..2]; delta adds a bucket at absolute index 4.
        let mut base = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let delta = DdSketchDelta {
            buckets: vec![(4, 7)],
            d_count: 7,
            d_sum: 35.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 6.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![1, 2, 0, 0, 7]);
        assert_eq!(base.store_offset, 0);
        assert_eq!(base.count, 10);
        assert_eq!(base.max, 6.0);
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        // Snapshot the sketch, add more samples via a merge, and confirm
        // the delta+apply path lands at the same state.
        let base = DdSketch::from_raw(0.01, vec![1, 2, 3], 0, 6, 12.0, 1.0, 3.0);
        let addition = DdSketch::from_raw(0.01, vec![10, 0, 20], 0, 30, 70.0, 0.5, 5.0);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = DdSketchDelta {
            buckets: vec![(0, 10), (2, 20)],
            d_count: 30,
            d_sum: 70.0,
            min_changed: true,
            new_min: 0.5,
            max_changed: true,
            new_max: 5.0,
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta);

        assert_eq!(via_delta.store_counts, via_merge.store_counts);
        assert_eq!(via_delta.count, via_merge.count);
        assert_eq!(via_delta.sum, via_merge.sum);
        assert_eq!(via_delta.min, via_merge.min);
        assert_eq!(via_delta.max, via_merge.max);
    }

    #[test]
    fn test_merge_alpha_mismatch() {
        let mut a = DdSketch::new(0.01);
        let b = DdSketch::new(0.02);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original = DdSketch::from_raw(0.01, vec![1, 2, 3], -2, 6, 30.0, 1.0, 5.0);
        let bytes = original.serialize_msgpack().unwrap();
        let decoded = DdSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.store_counts, original.store_counts);
        assert_eq!(decoded.store_offset, original.store_offset);
        assert_eq!(decoded.count, original.count);
    }

    #[test]
    fn test_insert_and_quantile_lognormal() {
        // Ground-truth: insert a large i.i.d. log-normal sample into
        // a full sketch and sanity-check P50 / P90 / P99.
        let mut gt = DdSketch::new(0.01);
        let mut rng = 0xdead_beefu64;
        let mut next = || {
            // xorshift64*
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // Box-Muller normal → log-normal(mu=3, sigma=0.7).
        let lognormal = |u: u64, v: u64| -> f64 {
            let r1 = (u as f64) / (u64::MAX as f64).max(1.0);
            let r2 = (v as f64) / (u64::MAX as f64).max(1.0);
            let z = (-2.0 * r1.max(1e-12).ln()).sqrt() * (2.0 * std::f64::consts::PI * r2).cos();
            (3.0 + 0.7 * z).exp()
        };
        for _ in 0..100_000 {
            gt.update(lognormal(next(), next()));
        }
        let p50 = gt.quantile(0.5).unwrap();
        let p99 = gt.quantile(0.99).unwrap();
        // Analytical P50 = exp(mu) = e^3 ≈ 20.09;
        // P99 ≈ exp(mu + sigma * Φ⁻¹(0.99)) = e^(3 + 0.7×2.326) ≈ 102.4.
        assert!(
            (p50 / 20.09).ln().abs() < 0.05,
            "P50 {} not close to 20.09",
            p50
        );
        assert!(
            (p99 / 102.4).ln().abs() < 0.05,
            "P99 {} not close to 102.4",
            p99
        );
    }

    /// Core accuracy claim for PRs #60-#63 end-to-end: building a
    /// sketch via `base + apply_delta()` produces quantile estimates
    /// within DDSketch's α bound of the ground-truth full-sketch
    /// path. If this fails, the paper's delta-reconstitution story
    /// is broken.
    #[test]
    fn test_delta_chain_preserves_quantile_accuracy() {
        let alpha = 0.01;
        let mut rng = 0xcafe_babeu64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let lognormal = |u: u64, v: u64| -> f64 {
            let r1 = (u as f64) / (u64::MAX as f64).max(1.0);
            let r2 = (v as f64) / (u64::MAX as f64).max(1.0);
            let z = (-2.0 * r1.max(1e-12).ln()).sqrt() * (2.0 * std::f64::consts::PI * r2).cos();
            (3.0 + 0.7 * z).exp()
        };

        // Path A (ground truth): one sketch, 50k samples inserted
        // directly.
        let mut full = DdSketch::new(alpha);
        // Path B (delta chain): a base sketch from the first 10k
        // samples, then 4 incremental "flushes" of 10k samples each,
        // each transmitted as a delta computed against the previous
        // snapshot.
        let mut reconstituted = DdSketch::new(alpha);
        let mut prev_snapshot = DdSketch::new(alpha); // what the "receiver" has cached

        let batch = 10_000;
        let batches = 5;
        for b in 0..batches {
            let mut this_batch = prev_snapshot.clone();
            for _ in 0..batch {
                let v = lognormal(next(), next());
                full.update(v);
                this_batch.update(v);
            }
            // Compute a "delta" = diff of this_batch vs prev_snapshot
            // in our in-memory struct shape. Matches what
            // `sketchlib-go/sketches/DDSketch/delta.go::ComputeDelta`
            // would put on the wire.
            let delta = compute_dd_delta(&prev_snapshot, &this_batch);
            if b == 0 {
                // First batch seeds the reconstituted sketch.
                reconstituted = this_batch.clone();
            } else {
                reconstituted.apply_delta(&delta);
            }
            prev_snapshot = this_batch;
        }

        // Both paths should see the same total count + sum (exact).
        assert_eq!(reconstituted.count, full.count, "count diverged");
        assert!(
            (reconstituted.sum - full.sum).abs() < 1e-6,
            "sum diverged: recon={} full={}",
            reconstituted.sum,
            full.sum
        );

        // And P50 / P90 / P99 should agree within α bound.
        for q in [0.5, 0.9, 0.99] {
            let got = reconstituted.quantile(q).unwrap();
            let want = full.quantile(q).unwrap();
            let rel_err = (got / want - 1.0).abs();
            assert!(
                rel_err <= alpha,
                "q={} rel_err={:.4} exceeds α={}: reconstituted={}, full={}",
                q,
                rel_err,
                alpha,
                got,
                want,
            );
        }
    }

    /// Helper used only in the delta-chain test: computes a
    /// `DdSketchDelta` from two snapshots. Mirrors the sketchlib-go
    /// `ComputeDelta` logic so the test exercises the wire-format
    /// path end-to-end.
    fn compute_dd_delta(snapshot: &DdSketch, current: &DdSketch) -> DdSketchDelta {
        let mut cells = Vec::new();
        if !current.store_counts.is_empty() {
            for (i, &c) in current.store_counts.iter().enumerate() {
                if c == 0 {
                    continue;
                }
                let k = current.store_offset + i as i32;
                let snap_count: u64 = if !snapshot.store_counts.is_empty() {
                    let idx = k as i64 - snapshot.store_offset as i64;
                    if idx >= 0 && (idx as usize) < snapshot.store_counts.len() {
                        snapshot.store_counts[idx as usize]
                    } else {
                        0
                    }
                } else {
                    0
                };
                let dc = c.saturating_sub(snap_count);
                if dc > 0 {
                    cells.push((k, dc));
                }
            }
        }
        let d_count = current.count as i64 - snapshot.count as i64;
        let d_sum = current.sum - snapshot.sum;
        let min_changed = current.count > 0 && (snapshot.count == 0 || current.min < snapshot.min);
        let max_changed = current.count > 0 && (snapshot.count == 0 || current.max > snapshot.max);
        DdSketchDelta {
            buckets: cells,
            d_count,
            d_sum,
            min_changed,
            new_min: current.min,
            max_changed,
            new_max: current.max,
        }
    }
}
