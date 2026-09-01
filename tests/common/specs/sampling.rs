// ---------------------------------------------------------------------------
// Nitro: sampling confidence
// ---------------------------------------------------------------------------

/// Confidence band for a Bernoulli-sampled counter, as NitroSketch uses.
///
/// Nitro admits each update with probability `p` and writes an integer weight
/// `W` per admitted update. For a key of true count `f` the number of admitted
/// updates is `X ~ Binomial(f, p)` and the estimate is `sum of W over the X
/// admissions`.
///
/// The implementation draws `W` by **stochastic rounding** of `1/p`: with
/// `q = floor(1/p)` and `r = 1/p - q`, `W = q + Bernoulli(r)`, drawn
/// independently per admitted update. That makes `E[W] = 1/p` for *every*
/// rate rather than only for reciprocal-integer ones, so
///
/// ```text
///   E[est] = f * p * (1/p) = f
///   Var[W] = r (1 - r)
///   Var[est] = E[X] Var[W] + Var[X] E[W]^2
///            = f p r(1-r) + f p (1-p) / p^2
///            = f ( p r (1-r) + (1-p)/p )
/// ```
///
/// so the accepted band is `f +- z * sqrt(f (p r (1-r) + (1-p)/p))`, plus
/// whatever the wrapped sketch's own error contributes. At `p = 1` both terms
/// vanish and the band collapses to zero width, which is correct — full
/// sampling is exact. At a reciprocal-integer rate `r = 0` and the formula
/// reduces to the familiar `sqrt(f (1-p) / p)`.
///
/// # Trial unit
///
/// The randomness is the sampling RNG. One `(seed, key)` probe is one draw;
/// two keys of the *same* sketch share one skip sequence and are dependent, so
/// a binomial must be taken over **sampling seeds**, with each seed's battery
/// of keys reduced to a single outcome first.
///
/// `z` is a Gaussian quantile, so this is an asymptotic normal band on a sum of
/// `f` i.i.d. terms — accurate for the counts these suites use (`f p` in the
/// hundreds) but a model, not an exact tail.
#[derive(Clone, Copy, Debug)]
pub struct SamplingConfidenceSpec {
    pub rate: f64,
    pub z: f64,
}

impl SamplingConfidenceSpec {
    pub fn new(rate: f64, z: f64) -> Self {
        Self { rate, z }
    }

    /// `floor(1/p)` and the fractional part `1/p - floor(1/p)` the
    /// implementation's stochastic rounding uses.
    pub fn weight_parts(&self) -> (f64, f64) {
        if self.rate >= 1.0 {
            return (1.0, 0.0);
        }
        let inv = 1.0 / self.rate;
        let floor = inv.floor();
        (floor, inv - floor)
    }

    /// Expected weight per admitted update — `1/p` exactly, by construction.
    pub fn expected_weight(&self) -> f64 {
        if self.rate >= 1.0 {
            1.0
        } else {
            1.0 / self.rate
        }
    }

    /// Standard deviation of the scaled estimate for a key of count `f`.
    pub fn sigma(&self, true_count: f64) -> f64 {
        if self.rate >= 1.0 {
            return 0.0;
        }
        let p = self.rate;
        let (_, r) = self.weight_parts();
        (true_count * (p * r * (1.0 - r) + (1.0 - p) / p)).sqrt()
    }

    /// Half-width of the accepted band, before any sketch-side error.
    pub fn half_width(&self, true_count: f64) -> f64 {
        self.z * self.sigma(true_count)
    }

    pub fn per_check_failure(&self) -> f64 {
        2.0 * (1.0 - standard_normal_cdf(self.z))
    }

    /// `sketch_slack` is added to the sampling band for the wrapped sketch's
    /// own error (zero when the sketch is exact for the probed key).
    pub fn check(&self, estimate: f64, true_count: f64, sketch_slack: f64) -> Result<(), String> {
        let half = self.half_width(true_count) + sketch_slack;
        if (estimate - true_count).abs() <= half {
            Ok(())
        } else {
            let (q, r) = self.weight_parts();
            Err(format!(
                "rate={} (weight {q}+Bernoulli({r:.4}), E[W]={:.4}): estimate {estimate:.1} vs \
                 true {true_count:.0}, |error| {:.1} > z*sigma + slack = {:.2}*{:.2} + \
                 {sketch_slack:.1} = {half:.1}",
                self.rate,
                self.expected_weight(),
                (estimate - true_count).abs(),
                self.z,
                self.sigma(true_count),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Priority (bottom-k) sampling
// ---------------------------------------------------------------------------

/// `UniformSampling`'s retention and sample-correctness model.
///
/// The sampler draws an independent uniform 64-bit priority per update and
/// retains the `m = ceil(n * rate)` entries of *smallest* priority. Two things
/// follow, and they need very different acceptance rules:
///
/// - **Structural, exact:** `len() == ceil(total_seen * rate)` once anything has
///   been seen (never a band — the truncation target is computed, not sampled),
///   every retained value is one of the values fed in, and no value is retained
///   more times than it was fed.
/// - **Statistical:** because the priorities are i.i.d. and independent of the
///   values, the retained set is a uniform sample of size `m` drawn **without
///   replacement** from the `n` values seen. The sample mean of a numeric
///   stream therefore has
///
///   ```text
///     E[mean_hat]   = mu
///     Var[mean_hat] = (sigma_N^2 / m) * (N - m) / (N - 1)
///   ```
///
///   where `sigma_N^2` is the population variance with divisor `N` and the
///   second factor is the finite-population correction.
///
/// # Trial unit
///
/// One seed is one draw of the whole priority sequence, so one sampler is one
/// trial. Several statistics read off the same sampler are not independent.
#[derive(Clone, Copy, Debug)]
pub struct PrioritySampleSpec {
    pub rate: f64,
    pub z: f64,
}

impl PrioritySampleSpec {
    pub fn new(rate: f64, z: f64) -> Self {
        Self { rate, z }
    }

    /// `ceil(n * rate)` — the retained size, exactly.
    pub fn retained(&self, total_seen: u64) -> usize {
        if total_seen == 0 {
            0
        } else {
            ((total_seen as f64) * self.rate).ceil() as usize
        }
    }

    /// Standard deviation of the sample mean under sampling without
    /// replacement of `m` out of `population` items with population variance
    /// `population_variance` (divisor `N`).
    pub fn mean_sigma(&self, population: usize, sample: usize, population_variance: f64) -> f64 {
        if sample == 0 || population <= 1 {
            return f64::INFINITY;
        }
        let n = population as f64;
        let m = sample as f64;
        (population_variance / m * ((n - m) / (n - 1.0)).max(0.0)).sqrt()
    }

    pub fn per_check_failure(&self) -> f64 {
        2.0 * (1.0 - standard_normal_cdf(self.z))
    }
}

// ---------------------------------------------------------------------------
// Shared numerics
// ---------------------------------------------------------------------------

/// `Phi(z)` via the Abramowitz & Stegun 7.1.26 error-function approximation
/// (absolute error < 1.5e-7), which is far finer than any band it sizes.
pub fn standard_normal_cdf(z: f64) -> f64 {
    0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}
