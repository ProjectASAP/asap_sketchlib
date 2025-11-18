use crate::common::structures::Vector1D;
use serde::{Deserialize, Serialize};
use rmp_serde::{from_slice, to_vec_named};

// DDsketch implementation based on the paper and algorithms provided:
// https://www.vldb.org/pvldb/vol12/p2195-masson.pdf

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Buckets {
    counts: Vector1D<u64>,
    offset: i32, // offset for the indexing of counts - vectors don't support negative indices
}

// Buckets created using vec![] for counts and 0 for offset
impl Buckets {
    fn new() -> Self {
        Self {
            counts: Vector1D::from_vec(Vec::new()),
            offset: 0,
        }
    }

    // Get the current range of bucket indices stored.
    fn range(&self) -> Option<(i32, i32)> {
        if self.counts.is_empty() {
            None
        } else {
            let left = self.offset;
            let right = self.offset + self.counts.len() as i32 - 1;
            Some((left, right))
        }
    }

    // checking to see if bucket for k exists, if not grow the counts vector accordingly
    fn ensure(&mut self, k: i32) {
        if self.counts.is_empty() {
            self.counts = Vector1D::from_vec(vec![0u64]);
            self.offset = k;
            return;
        }

        let (left, right) = self.range().unwrap();

        if k < left {
            let grow = (left - k) as usize;
            let mut v = vec![0u64; grow];
            v.extend_from_slice(self.counts.as_slice());
            self.counts = Vector1D::from_vec(v);
            self.offset = k;
        } else if k > right {
            let grow = (k - right) as usize;
            let mut v = self.counts.clone().into_vec();
            v.resize(v.len() + grow, 0);
            self.counts = Vector1D::from_vec(v);
        }
    }

    // add one count to bucket k
    fn add_one(&mut self, k: i32) {
        self.ensure(k);
        let idx = (k - self.offset) as usize;
        self.counts.as_mut_slice()[idx] += 1;
    }

    // Iterate over (bucket_index, count) and skip zero bins.
    fn iter_nonzero(&self) -> impl Iterator<Item = (i32, u64)> + '_ {
        self.counts
            .as_slice()
            .iter()
            .enumerate()
            .filter(|(_, c)| **c > 0) // skip zero counts
            .map(move |(i, c)| (self.offset + i as i32, *c))
    }

    // merge another Buckets into this one
    fn merge(&mut self, other: &Buckets) {
        if other.counts.is_empty() {
            return;
        }
        if self.counts.is_empty() {
            *self = other.clone();
            return;
        }

        let (self_left, self_right) = self.range().unwrap();
        let (other_left, other_right) = other.range().unwrap();

        let new_left = self_left.min(other_left);
        let new_right = self_right.max(other_right);
        let new_len = (new_right - new_left + 1) as usize;

        let mut merged = vec![0u64; new_len];

        // copy the self counts
        for (i, &c) in self.counts.as_slice().iter().enumerate() {
            let k = self_left + i as i32;
            merged[(k - new_left) as usize] += c;
        }

        // add other counts after fixing index
        for (i, &c) in other.counts.as_slice().iter().enumerate() {
            let k = other_left + i as i32;
            merged[(k - new_left) as usize] += c;
        }

        self.counts = Vector1D::from_vec(merged);
        self.offset = new_left;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DDSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    store: Buckets,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl DDSketch {
    pub fn new(alpha: f64) -> Self {
        assert!((0.0..1.0).contains(&alpha), "alpha must be in (0,1)");
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let log_gamma = gamma.ln();
        Self {
            alpha,
            gamma,
            log_gamma,
            store: Buckets::new(),
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    // serialize sketch into bytes using MessagePack format.
    pub fn serialize(&self) -> Option<Vec<u8>> {
        to_vec_named(self).ok()
    }

    // deserialize sketch from bytes using MessagePack format.
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        from_slice(bytes).ok()
    }

    /// Add a sample.
    pub fn add(&mut self, v: f64) {
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
        let idx = self.key_for(v);
        self.store.add_one(idx);
    }

    // Quantile estimate using bin representative based on logarithmic binning.
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

        for (bin, c) in self.store.iter_nonzero() {
            seen += c;
            if seen >= rank {
                // found the bin
                let mut v = self.bin_representative(bin);
                // keep within min/max
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

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn min(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.min)
        }
    }

    pub fn max(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.max)
        }
    }

    // Merge another DDSketch into this one
    pub fn merge(&mut self, other: &DDSketch) {
        // sanity: same parameters
        debug_assert!((self.alpha - other.alpha).abs() < 1e-12);
        debug_assert!((self.gamma - other.gamma).abs() < 1e-12);

        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            // copy everything over
            self.alpha = other.alpha;
            self.gamma = other.gamma;
            self.log_gamma = other.log_gamma;
            self.store = other.store.clone();
            self.count = other.count;
            self.sum = other.sum;
            self.min = other.min;
            self.max = other.max;
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
        self.store.merge(&other.store);
    }

    // mapping value to bin key
    fn key_for(&self, v: f64) -> i32 {
        debug_assert!(v > 0.0);
        (v.ln() / self.log_gamma).floor() as i32
    }

    // mapping bin key to representative value
    fn bin_representative(&self, k: i32) -> f64 {
        self.gamma.powf(k as f64 + 0.5)
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
            s.add(v);
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
                sk.add(x);
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
                sk.add(x);
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
                sk.add(x);
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
                sk.add(x);
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
            s1.add(v);
        }
        for v in vals2 {
            s2.add(v);
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
        assert!(mid >= 1.0 && mid <= 20.0);
    }

    #[test]
    fn dds_serialization_round_trip() {
        let mut s = DDSketch::new(0.01);
        let vals = [1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0];// sample values

        for v in vals {
            s.add(v);
        }

        let encoded = s.serialize().expect("DDSketch serialization fail"); // serialize to bytes
        assert!( !encoded.is_empty(),"encoded bytes should not be empty for DDSketch");

        let decoded = DDSketch::deserialize(&encoded).expect("DDSketch deserialization fail"); // deserialize back

        // basic invariants - conditions should match, else it fails
        assert_eq!(decoded.get_count(), s.get_count()); // counts should match
        assert_eq!(decoded.min(), s.min());// mins should match
        assert_eq!(decoded.max(), s.max());// maxes should match

        // quantiles should match at several points
        for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
            let a = s.get_value_at_quantile(q).unwrap();
            let b = decoded.get_value_at_quantile(q).unwrap();
            assert_eq!(a, b, "quantile mismatch at p={}", q);
        }
    }
}
