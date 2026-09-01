use super::sampling::standard_normal_cdf;
use super::statistics::*;

// ---------------------------------------------------------------------------
// HLL / KMV: cardinality confidence bands
// ---------------------------------------------------------------------------

/// Which estimator's error model a cardinality band is derived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CardinalityModel {
    /// Flajolet, Fusy, Gandouet & Meunier (2007): relative standard error
    /// `1.04 / sqrt(m)` for `m = 2^precision` registers. Ertl's maximum
    /// likelihood estimator (2017) attains the same Cramer-Rao bound
    /// `sqrt(3 ln 2 - 1) / sqrt(m) = 1.0389 / sqrt(m)`, so both share this
    /// constant.
    HyperLogLogRegisters,
    /// Historic Inverse Probability (Cohen 2015; Ting 2014): relative standard
    /// error `sqrt(ln 2 / m) = 0.8326 / sqrt(m)`. HIP is strictly tighter than
    /// the register-only estimators because it integrates the insertion
    /// history, so it must not be checked against their constant.
    HyperLogLogHip,
    /// K-Minimum-Values, the `(k-1)/U_(k)` estimator over the `k` smallest
    /// normalized hashes.
    ///
    /// With `n` distinct uniform hashes, `U_(k) ~ Beta(k, n-k+1)`, so
    /// `E[(k-1)/U_(k)] = n` (the estimator is unbiased for `k > 1`) and
    ///
    /// ```text
    ///   Var[(k-1)/U_(k)] = n (n - k + 1) / (k - 2)
    ///   RSE(n, k)        = sqrt( (n - k + 1) / (n (k - 2)) )
    ///                    -> 1 / sqrt(k - 2)   as n -> infinity
    /// ```
    ///
    /// The finite-`n` form is used, because it is exact and because it is
    /// *smaller* than the asymptote — pretending otherwise would hand the
    /// estimator a band it has not earned. `1 / sqrt(k - 1)`, which an earlier
    /// revision of this file called "marginally conservative", is smaller than
    /// `1 / sqrt(k - 2)` and therefore stricter, not looser.
    KMinimumValues,
}

/// A cardinality band `n * (1 +- z * sigma_rel)` with an explicit `z`.
///
/// Nothing here is a flat percentage: `sigma_rel` comes from the estimator's
/// own error model and `z` is stated as a Gaussian quantile.
///
/// # What the `z` band is and is not
///
/// `sigma_rel` is a **standard deviation**, derived exactly. Turning it into a
/// tail probability via `2 (1 - Phi(z))` additionally assumes the estimator is
/// approximately normal, which is an *asymptotic model*, not a theorem:
///
/// - HLL's register-based estimators are asymptotically normal in `m` and `n`,
///   with known deviations near the linear-counting switchover;
/// - KMV's `(k-1)/U_(k)` is a reciprocal Beta variate, right-skewed at small
///   `k`, and only asymptotically normal as `k` grows.
///
/// So a band built from `z` is an **asymptotic, model-based confidence band**.
/// It is quoted as such in the coverage matrix and is never labelled a theorem.
///
/// # Trial unit
///
/// One sketch answering at one cardinality is one draw. Increasing checkpoints
/// on the *same* sketch are nested — the state at `n = 10^5` contains the state
/// at `n = 10^4` — so a battery of checkpoints is one trial, not many. Batteries
/// that need a binomial must vary the hash seed or the identity namespace.
#[derive(Clone, Copy, Debug)]
pub struct CardinalityConfidenceSpec {
    pub model: CardinalityModel,
    /// `m` registers for HLL, `k` retained minima for KMV.
    pub size: usize,
    /// Gaussian quantile. `z = 4` is a two-sided failure probability of
    /// 6.3e-5 per check under the normal model.
    pub z: f64,
}

impl CardinalityConfidenceSpec {
    pub fn hll(precision: u32, z: f64) -> Self {
        Self {
            model: CardinalityModel::HyperLogLogRegisters,
            size: 1usize << precision,
            z,
        }
    }

    pub fn hll_hip(precision: u32, z: f64) -> Self {
        Self {
            model: CardinalityModel::HyperLogLogHip,
            size: 1usize << precision,
            z,
        }
    }

    pub fn kmv(k: usize, z: f64) -> Self {
        Self {
            model: CardinalityModel::KMinimumValues,
            size: k,
            z,
        }
    }

    /// The estimator's relative standard error at `true_distinct` distinct
    /// elements.
    ///
    /// Only KMV's depends on `n`; the HLL constants are asymptotic in `n` by
    /// construction.
    pub fn sigma_rel_at(&self, true_distinct: usize) -> f64 {
        let s = self.size as f64;
        match self.model {
            CardinalityModel::HyperLogLogRegisters => 1.04 / s.sqrt(),
            CardinalityModel::HyperLogLogHip => (std::f64::consts::LN_2 / s).sqrt(),
            CardinalityModel::KMinimumValues => {
                let n = true_distinct as f64;
                let k = s;
                if k <= 2.0 || n <= 0.0 {
                    return f64::INFINITY;
                }
                (((n - k + 1.0).max(0.0)) / (n * (k - 2.0))).sqrt()
            }
        }
    }

    /// The estimator's asymptotic relative standard error.
    pub fn sigma_rel(&self) -> f64 {
        let s = self.size as f64;
        match self.model {
            CardinalityModel::HyperLogLogRegisters => 1.04 / s.sqrt(),
            CardinalityModel::HyperLogLogHip => (std::f64::consts::LN_2 / s).sqrt(),
            CardinalityModel::KMinimumValues => 1.0 / (s - 2.0).sqrt(),
        }
    }

    /// `z * sigma_rel` — the half-width of the accepted relative band, using
    /// the asymptotic RSE.
    pub fn tolerance(&self) -> f64 {
        self.z * self.sigma_rel()
    }

    /// `z * sigma_rel(n)` — the half-width at a specific cardinality.
    pub fn tolerance_at(&self, true_distinct: usize) -> f64 {
        self.z * self.sigma_rel_at(true_distinct)
    }

    /// Two-sided failure probability at `z` under the normal model.
    pub fn per_check_failure(&self) -> f64 {
        2.0 * (1.0 - standard_normal_cdf(self.z))
    }

    /// KMV is exact only while it has seen **fewer** than `k` distinct
    /// elements: `KMV::estimate` returns the retained count verbatim when
    /// `k_vals.len() < k`, and switches to `(k-1)/U_(k)` at `len == k`.
    ///
    /// The `n == k` boundary is therefore *estimated*, not exact — the earlier
    /// `n <= k` form silently demanded exactness of a number whose standard
    /// deviation there is about `sqrt(k / (k-2)) ~ 1`.
    pub fn is_exact_regime(&self, true_distinct: usize) -> bool {
        self.model == CardinalityModel::KMinimumValues && true_distinct < self.size
    }

    pub fn check(&self, estimate: f64, true_distinct: usize) -> Result<(), String> {
        let t = true_distinct as f64;
        if self.is_exact_regime(true_distinct) {
            // Exact regime: the sketch holds every distinct hash it has seen.
            return if (estimate - t).abs() <= 0.5 {
                Ok(())
            } else {
                Err(format!(
                    "n={true_distinct} < k={}: estimate {estimate} must be exact",
                    self.size
                ))
            };
        }
        let tol = self.tolerance_at(true_distinct);
        let rel = ((estimate - t) / t).abs();
        if rel <= tol {
            Ok(())
        } else {
            Err(format!(
                "n={true_distinct}: estimate {estimate:.1} -> relative error {rel:.5} > \
                 z*sigma(n) = {:.2}*{:.5} = {tol:.5} (model {:?}, size {})",
                self.z,
                self.sigma_rel_at(true_distinct),
                self.model,
                self.size
            ))
        }
    }

    pub fn tally_into(&self, tally: &mut Tally, estimate: f64, true_distinct: usize) {
        let outcome = self.check(estimate, true_distinct);
        tally.record(outcome.is_ok(), || outcome.unwrap_err());
    }
}
