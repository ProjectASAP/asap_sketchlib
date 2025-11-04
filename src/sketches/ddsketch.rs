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
        if self.count == 0 || q.is_nan() { return None; }
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
                // Use a representative for the bin
                let mut v = self.bin_representative(*bin);
                // Clamp to observed bounds
                if v < self.min { v = self.min; }
                if v > self.max { v = self.max; }
                return Some(v);
            }
        }
        Some(self.max)
    }

    pub fn get_count(&self) -> u64 { self.count }
    pub fn min(&self) -> Option<f64> { if self.count == 0 { None } else { Some(self.min) } }
    pub fn max(&self) -> Option<f64> { if self.count == 0 { None } else { Some(self.max) } }

    // mapping value to bin key
    fn key_for(&self, v: f64) -> i32 {
        debug_assert!(v > 0.0);
        (v.ln() / self.log_gamma).floor() as i32
    }
    // bounds for the given bin
    fn bin_bounds(&self, k: i32) -> (f64, f64) {
        let lo = self.gamma.powf(k as f64) / (1.0 + self.alpha);
        let hi = self.gamma.powf(k as f64 + 1.0) / (1.0 + self.alpha);
        (lo, hi)
    }
    // Bin representative value 
    fn bin_representative(&self, k: i32) -> f64 {
        let (lo, hi) = self.bin_bounds(k);
        (lo * hi).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Absolute relative error helper
    fn rel_err(a: f64, b: f64) -> f64 {
        if a == 0.0 && b == 0.0 { 0.0 } else { (a - b).abs() / f64::max(1e-30, b.abs()) }
    }

    // True quantile from sorted data
    fn true_quantile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() { return f64::NAN; }
        if p <= 0.0 { return sorted[0]; }
        if p >= 1.0 { return sorted[sorted.len() - 1]; }
        let n = sorted.len();
        let k = ((p * n as f64).ceil() as usize).clamp(1, n) - 1;
        sorted[k]
    }

    #[test]
    fn insert_and_query_basic() {
        let mut s = DDSketch::new(0.01);
        let vals = [0.0, -5.0, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0];
        for &v in &vals { s.add(v); }

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
    fn accuracy_uniform() {
        let alpha = 0.01;
        let tol = 3.0 * alpha;
        let n = 20_000usize;

        // Deterministic uniform data between 1 and 1000
        let xs: Vec<f64> = (0..n)
            .map(|i| 1.0 + i as f64 * (999.0 / (n - 1) as f64))
            .collect();

        let mut s = DDSketch::new(alpha);
        for &v in &xs { s.add(v); }

        let mut cp = xs.clone();
        cp.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for &p in &[0.5, 0.9, 0.99] {
            let got = s.get_value_at_quantile(p).expect("quantile");
            let want = true_quantile(&cp, p);
            let re = rel_err(got, want);
            assert!(
                re <= tol,
                "p={p:.2} relerr={re:.4} got={got:.6} want={want:.6} (tol={tol:.4})"
            );
        }
    }
}
