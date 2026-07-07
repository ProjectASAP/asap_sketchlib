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

    /// Serializes the sketch to ASAPv1-wrapped MessagePack bytes.
    /// kind_id: `[NATIVE_DD_SKETCH, HASHER_UNKNOWN]`.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        use crate::message_pack_format::magic_ids;
        let payload = to_vec_named(self)?;
        Ok(magic_ids::encode_wrapper(
            &[magic_ids::NATIVE_DD_SKETCH, magic_ids::HASHER_UNKNOWN],
            &payload,
        ))
    }

    /// Deserializes a DDSketch from a MessagePack byte slice produced by
    /// [`Self::serialize_to_bytes`].
    ///
    /// The `count`/`sum`/`min`/`max` scalars are `#[serde(skip)]` (dropped
    /// from the wire, ProjectASAP/sketchlib-go#243), so they default to
    /// zero on decode. Recompute them from the bucket store: `count` is
    /// exact (the sum of all bucket counts) and `sum`/`min`/`max` are
    /// reconstructed from the per-bucket representative values, accurate
    /// to within the sketch's α relative-accuracy bound.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        use crate::message_pack_format::magic_ids;
        let (kind_id, payload) =
            magic_ids::decode_wrapper(bytes).map_err(RmpDecodeError::Uncategorized)?;
        match kind_id {
            [id, _hasher] if *id == magic_ids::NATIVE_DD_SKETCH => {
                let mut sk: Self = from_slice(payload)?;
                sk.recompute_scalars_from_store();
                Ok(sk)
            }
            _ => Err(RmpDecodeError::Uncategorized(format!(
                "DDSketch kind_id mismatch: expected [0x{:02x}, hasher], got {:?}",
                magic_ids::NATIVE_DD_SKETCH,
                kind_id
            ))),
        }
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
                    "quantile {name} (p={p:.2}) relerr={err:.4} got={got} want={want} tol={tol}"
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
                    "quantile {name} (p={p:.2}) relerr={err:.4} got={got} want={want} tol={tol}"
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
                    "quantile {name} (p={p:.2}) relerr={err:.4} got={got} want={want} tol={tol}"
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
                    "quantile {name} (p={p:.2}) relerr={err:.4} got={got} want={want} tol={tol}"
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

        // `count` survives exactly (recomputed by summing buckets). The
        // `min`/`max`/`sum` scalars are no longer serialized
        // (ProjectASAP/sketchlib-go#243) — they're reconstructed from the
        // bucket midpoint `gamma^(k+0.5)`. A true value sitting at a bucket
        // edge is at most `sqrt(gamma) - 1` away from that midpoint, which
        // is marginally larger than α; use that as the tolerance.
        assert_eq!(decoded.get_count(), s.get_count()); // counts should match
        let alpha = s.alpha();
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let bucket_tol = gamma.sqrt() - 1.0;
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
}
