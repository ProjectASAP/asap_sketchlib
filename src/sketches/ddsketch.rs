use std::collections::BTreeMap;

// DDsketch implementation based on the paper and algorithms provided:
// https://www.vldb.org/pvldb/vol12/p2195-masson.pdf
#[derive(Debug)]
pub struct DDSketch {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    store: BTreeMap<i32, u64>,
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
            store: BTreeMap::new(),
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,

        }
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
        *self.store.entry(idx).or_insert(0) += 1;
    }



    /// Quantile estimate using bin representative based on logarithmic binning.
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

        for (bin, c) in self.store.iter() {
            seen += *c;
            if seen >= rank {
                // found the bin
                let mut v = self.bin_representative(*bin);
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
    use crate::test_utils::{sample_uniform_f64, sample_zipf_f64, sample_normal_f64, sample_exponential_f64};

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
            (0.0,  "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0,  "max"),
        ];

        fn build_dds_with_uniform(alpha: f64, n: usize, min: f64, max: f64, seed: u64) -> (DDSketch, Vec<f64>) {
            // sample uniform values from test utils
            let mut vals = sample_uniform_f64(min, max, n, seed);
            // retain only finite positive values
            vals.retain(|v| v.is_finite() && *v > 0.0);
            // build DDSketch
            let mut sk = DDSketch::new(alpha);
            for &x in &vals { sk.add(x); }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(sk: &DDSketch, sorted_vals: &[f64], qs: &[(f64, &str)], tol: f64) {
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(sorted_vals, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name, p, err, got, want, tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize].into_iter().enumerate() {
            let seed = 0xA5A5_0000_u64 + idx as u64;
            let (sketch, mut values) = build_dds_with_uniform(ALPHA, n, 1_000_000.0, 10_000_000.0, seed);
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_quantiles_within_error_dds(&sketch, &values, QUANTILES, ALPHA);
        }
    }

    #[test]
    fn dds_zipf_distribution_quantiles() {

        const ALPHA: f64 = 0.01;

        const QUANTILES: &[(f64, &str)] = &[
            (0.0,  "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0,  "max"),
        ];

        fn build_dds_with_zipf(alpha: f64, n: usize, min: f64, max: f64, domain: usize, exponent: f64, seed: u64,) -> (DDSketch, Vec<f64>) {
            let mut vals = sample_zipf_f64(min, max, domain, exponent, n, seed);
            vals.retain(|v| v.is_finite() && *v > 0.0);
            let mut sk = DDSketch::new(alpha);
            for &x in &vals {
                sk.add(x);
            }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(sk: &DDSketch, sorted_vals: &[f64], qs: &[(f64, &str)], tol: f64) {
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(sorted_vals, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name, p, err, got, want, tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize].into_iter().enumerate() {
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
            (0.0,  "min"),
            (0.10, "p10"),
            (0.25, "p25"),
            (0.50, "p50"),
            (0.75, "p75"),
            (0.90, "p90"),
            (1.0,  "max"),
        ];

        fn build_dds_with_normal(alpha: f64, n: usize, mean: f64, std: f64, seed: u64) -> (DDSketch, Vec<f64>) {
            // changed the code to include the normal distribution sampler from test_utils
            let vals = sample_normal_f64(mean, std, n, seed)
                .into_iter()
                .filter(|v| v.is_finite() && *v > 0.0) 
                .collect::<Vec<_>>();

            let mut sk = DDSketch::new(alpha);
            for &x in &vals { sk.add(x); }
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
                    name, p, err, got, want, tol
                );
            }
        }

        // Mean and std chosen so almost all samples are positive.
        let mean = 1_000.0;
        let std  = 100.0;

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize].into_iter().enumerate() {
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

        fn build_dds_with_exponential(alpha: f64, n: usize, lambda: f64, seed: u64) -> (DDSketch, Vec<f64>) {
            let vals = sample_exponential_f64(lambda, n, seed);
            let mut sk = DDSketch::new(alpha);
            for &x in &vals { sk.add(x); }
            (sk, vals)
        }

        fn assert_quantiles_within_error_dds(sk: &DDSketch, vals: &[f64], qs: &[(f64, &str)], tol: f64) {
            let mut sorted = vals.to_vec();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            for &(p, name) in qs {
                let got = sk.get_value_at_quantile(p).expect("quantile");
                let want = true_quantile(&sorted, p);
                let err = rel_err(got, want);
                assert!(
                    err <= tol,
                    "quantile {} (p={:.2}) relerr={:.4} got={} want={} tol={}",
                    name, p, err, got, want, tol
                );
            }
        }

        for (idx, n) in [1_000usize, 5_000usize, 20_000usize].into_iter().enumerate() {
            let seed = 0xE3E3_0000_u64 + idx as u64;
            let (sketch, values) = build_dds_with_exponential(ALPHA, n, LAMBDA, seed);
            assert_quantiles_within_error_dds(&sketch, &values, QUANTILES, ALPHA);
        }
    }

}
