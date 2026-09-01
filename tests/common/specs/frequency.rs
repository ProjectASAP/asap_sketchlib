use super::statistics::*;
use crate::common::FreqTruth;

// ---------------------------------------------------------------------------
// Count-Min: one-sided additive error
// ---------------------------------------------------------------------------

/// Count-Min Sketch, Cormode & Muthukrishnan (2005), Theorem 1.
///
/// For a sketch of `d` rows and `w` columns, with `f` the exact frequency
/// vector and `N = ||f||_1`:
///
/// - **structural:** `est(i) >= f(i)` for every key, always, no probability;
/// - **marginal:** `P[ est(i) - f(i) > b * (N - f(i)) / w ] <= b^-d` for any
///   `b > 1`, one key at a time.
///
/// The row estimator is `f(i) + X_r` with `X_r >= 0` and
/// `E[X_r] = (N - f(i)) / w` (only the *other* keys can collide into `i`'s
/// cell). Markov gives `P[X_r >= b * E[X_r]] <= 1/b` per row; the `d` rows use
/// independent hashes and the minimum exceeds the bound only if every row
/// does, hence `b^-d`. The familiar form is `b = e`, giving `e^-d`.
///
/// `N - f(i)` rather than `N` is the tighter and correct tail mass: it is what
/// the expectation actually is, and using `N` would silently hand a hot key a
/// larger budget than the theorem grants it.
///
/// # Simultaneous form
///
/// Because the per-key failure probability decays as `b^-d`, a union bound over
/// `D` probed keys costs only a factor `(D / delta)^(1/d)` in the budget. That
/// gives a genuine statement about *all* `D` keys at once, with no independence
/// assumption between keys, which is what makes a zero-tolerance assertion over
/// a whole key battery legitimate.
///
/// The `d` rows being independent is exact for `RegularPath` (one hash call per
/// row with a distinct seed) and a modelling assumption for `FastPath`, which
/// slices row indices out of one 128-bit hash.
#[derive(Clone, Copy, Debug)]
pub struct CountMinSpec {
    pub rows: usize,
    pub cols: usize,
}

impl CountMinSpec {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self { rows, cols }
    }

    /// `e * (N - f_i) / w` — the marginal additive excess budget for one key.
    pub fn marginal_bound(&self, total_mass: f64, key_mass: f64) -> f64 {
        std::f64::consts::E * (total_mass - key_mass) / self.cols as f64
    }

    /// `e^-d` — the marginal probability a single key's minimum exceeds the
    /// marginal budget.
    pub fn marginal_failure(&self) -> f64 {
        (-(self.rows as f64)).exp()
    }

    /// `b = (D / delta)^(1/d)`: the Markov factor that makes the additive bound
    /// hold for all `D` keys at once with probability `1 - delta`.
    pub fn simultaneous_factor(&self, distinct: usize, delta: f64) -> f64 {
        (distinct.max(1) as f64 / delta).powf(1.0 / self.rows as f64)
    }

    /// `b * (N - f_i) / w` at the simultaneous `b`.
    pub fn simultaneous_bound(
        &self,
        total_mass: f64,
        key_mass: f64,
        distinct: usize,
        delta: f64,
    ) -> f64 {
        self.simultaneous_factor(distinct, delta) * (total_mass - key_mass) / self.cols as f64
    }

    /// Runs the full Count-Min contract over every key in `truth`.
    ///
    /// One-sidedness and the simultaneous additive bound are checked with
    /// ordinary fail-fast assertions.
    pub fn assert_contract<F>(&self, label: &str, truth: &FreqTruth, estimate: F, context: &str)
    where
        F: Fn(i64) -> f64,
    {
        let total = truth.total() as f64;
        let distinct = truth.distinct();
        let factor = self.simultaneous_factor(distinct, SIMULTANEOUS_LEVEL);
        for (key, count) in truth.pairs() {
            let est = estimate(key);
            let f = count as f64;
            assert!(
                est >= f,
                "{label} on {context}: key {key}: est {est} < true {f} (Count-Min must never underestimate)"
            );
            let simul = self.simultaneous_bound(total, f, distinct, SIMULTANEOUS_LEVEL);
            assert!(
                est - f <= simul,
                "{label} on {context}: key {key}: excess {:.1} > b*(N-f)/w = {simul:.1} with b={factor:.2} (true {f}, est {est})",
                est - f
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Count Sketch: two-sided L2 error
// ---------------------------------------------------------------------------

/// Count Sketch, Charikar, Chen & Farach-Colton (2002).
///
/// This is **not** Count-Min's bound and must not reuse `eps * N`. Count
/// Sketch's error is driven by the L2 norm of the *residual* frequency vector,
/// is two-sided (signed), and is rank-independent — a cold key gets the same
/// absolute error band as the hottest one.
///
/// Per row `r`, the estimator is `X_r = s_r(i) * C[r][h_r(i)]`, which is
/// unbiased with
///
/// ```text
///   E[X_r] = f(i)
///   Var[X_r] <= || f_{-i} ||_2^2 / w
/// ```
///
/// where `f_{-i}` is `f` with coordinate `i` removed and `w` is the row width.
/// Chebyshev at `t = sqrt(kappa / w) * || f_{-i} ||_2` gives a per-row failure
/// probability of at most `1 / kappa`. The reported estimate is the median of
/// the `d` rows, so it exceeds `t` only when at least `ceil(d/2)` independent
/// rows do:
///
/// ```text
///   P[ |est(i) - f(i)| > sqrt(kappa / w) * ||f_{-i}||_2 ]
///        <= P[ Binomial(d, 1/kappa) >= ceil(d/2) ]
/// ```
///
/// `kappa = 3` is the usual marginal choice (per-row failure 1/3, so the median
/// is a genuine amplification); it is a declared constant of the spec, not a
/// fudge factor multiplied onto a bound after the fact.
///
/// # Even `d`, and why the threshold is `ceil(d/2)`
///
/// `compute_median_inline_f64` returns the *average of the two middle order
/// statistics* when `d` is even, which is the standard even-length median and
/// is what every even-depth sketch in this crate reports. That changes how many
/// bad rows it takes to break the bound.
///
/// Write `t` for the error scale and call a row *bad* if its estimate leaves
/// `[f - t, f + t]`. For odd `d` the reported value is the middle order
/// statistic, and it is bad only if at least `ceil(d/2)` rows are — the
/// classic majority argument. For even `d` the reported value is
/// `(X_(d/2) + X_(d/2+1)) / 2`:
///
/// - with `d/2 - 1` or fewer bad rows, both middle order statistics are good
///   and their average lies in `[f - t, f + t]`, which is convex — safe;
/// - with `d/2` bad rows **all on the same side**, one of the two middle order
///   statistics is bad and unbounded, so the average can leave the interval by
///   an arbitrary amount.
///
/// At `d = 4` that means **two** bad rows suffice, not three. The threshold is
/// therefore `ceil(d/2) = (d + 1) / 2` in integer arithmetic, which agrees with
/// `d/2 + 1` for odd `d` and is strictly smaller for even `d`.
///
/// This matters because it is the direction that *overstates* the guarantee:
/// `rows / 2 + 1` at `d = 4, kappa = 3` reports a per-key failure probability
/// of `P[Bin(4, 1/3) >= 3] = 0.111` where the estimator only earns
/// `P[Bin(4, 1/3) >= 2] = 0.407`, and it makes the simultaneous `kappa` search
/// stop early — a bound narrower than the theorem supports. `CountL2HH` runs at
/// four rows, so this was live.
///
/// # Simultaneous form
///
/// The keys of one sketch share one hash, so their outcomes are dependent and a
/// binomial over keys is not available. Raising `kappa` until the per-key
/// failure probability is below `delta / D` makes the bound hold for all `D`
/// probed keys at once by a union bound, which needs no independence at all —
/// and the required `kappa` grows only polynomially, because the median tail
/// falls off like `kappa^-ceil(d/2)`.
#[derive(Clone, Copy, Debug)]
pub struct CountSketchSpec {
    pub rows: usize,
    pub cols: usize,
    /// Chebyshev slack for the marginal bound. Per-row failure is `1 / kappa`.
    pub kappa: f64,
}

impl CountSketchSpec {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            kappa: 3.0,
        }
    }

    /// `sqrt(kappa / w) * ||f_{-i}||_2` at an explicit `kappa`.
    pub fn scale_at(&self, kappa: f64, residual_l2: f64) -> f64 {
        (kappa / self.cols as f64).sqrt() * residual_l2
    }

    /// `sqrt(3 / w) * ||f_{-i}||_2` — the marginal error scale.
    pub fn marginal_scale(&self, residual_l2: f64) -> f64 {
        self.scale_at(self.kappa, residual_l2)
    }

    /// `ceil(d/2)` — the smallest number of bad rows that can push the
    /// reported estimate outside the band. See the type docs for the even-`d`
    /// derivation.
    pub fn bad_row_threshold(&self) -> usize {
        self.rows.div_ceil(2)
    }

    /// `P[Binomial(d, 1/kappa) >= ceil(d/2)]` at an explicit `kappa`.
    pub fn key_failure_at(&self, kappa: f64) -> f64 {
        binomial_tail_ge(self.rows, self.bad_row_threshold(), 1.0 / kappa)
    }

    /// Marginal per-key failure probability at `kappa = 3`.
    pub fn marginal_failure(&self) -> f64 {
        self.key_failure_at(self.kappa)
    }

    /// Smallest `kappa` whose per-key failure probability is at most
    /// `delta / D`, so a union bound over `D` keys leaves `delta` overall.
    ///
    /// The tail is monotone decreasing in `kappa`, so a bisection on
    /// `log(kappa)` converges; the search starts from the marginal `kappa` and
    /// doubles until the target is met.
    pub fn simultaneous_kappa(&self, distinct: usize, delta: f64) -> f64 {
        let target = delta / distinct.max(1) as f64;
        let mut lo = self.kappa;
        if self.key_failure_at(lo) <= target {
            return lo;
        }
        let mut hi = lo;
        for _ in 0..80 {
            hi *= 2.0;
            if self.key_failure_at(hi) <= target {
                break;
            }
            lo = hi;
        }
        for _ in 0..200 {
            let mid = (lo * hi).sqrt();
            if self.key_failure_at(mid) <= target {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        hi
    }

    /// Runs the L2 contract over every key in `truth`.
    ///
    /// The residual norm is recomputed per key from the exact frequency
    /// vector: `||f_{-i}||_2 = sqrt(F2 - f(i)^2)`. The simultaneous bound is
    /// asserted with an ordinary fail-fast assertion.
    pub fn assert_contract<F>(&self, label: &str, truth: &FreqTruth, estimate: F, context: &str)
    where
        F: Fn(i64) -> f64,
    {
        let f2 = truth.f2();
        let kappa = self.simultaneous_kappa(truth.distinct(), SIMULTANEOUS_LEVEL);
        for (key, count) in truth.pairs() {
            let f = count as f64;
            let residual_l2 = (f2 - f * f).max(0.0).sqrt();
            let est = estimate(key);
            let err = (est - f).abs();

            let simul = self.scale_at(kappa, residual_l2);
            assert!(
                err <= simul,
                "{label} on {context}: key {key}: |{est:.1} - {f}| = {err:.1} > sqrt(kappa/w)*||f_-i||_2 = {simul:.1} at simultaneous kappa={kappa:.1}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Second moment (F2) from a Count Sketch row
// ---------------------------------------------------------------------------

/// F2 estimation from the counter matrix of a Count Sketch, as `CountL2HH`
/// and UnivMon do.
///
/// Each row's `Y_r = sum_j C[r][j]^2` is the AMS tug-of-war estimator (Alon,
/// Matias & Szegedy 1996; Charikar, Chen & Farach-Colton 2002):
///
/// ```text
///   E[Y_r]   = F2
///   Var[Y_r] = 2 (F2^2 - sum_i f_i^4) / w  <=  2 F2^2 / w
/// ```
///
/// Chebyshev at `t = sqrt(2 kappa / w) * F2` gives a per-row failure
/// probability of `1 / kappa`, and the reported value is the median over the
/// `d` rows, so the query fails only when at least `ceil(d/2)` rows do.
///
/// **Trial unit:** one F2 query is one sketch's single answer, so a battery of
/// F2 checks over distinct sketches *is* a battery of independent trials; two
/// F2 reads from the same sketch are not.
///
/// This is *not* an exactly maintained quantity: the accumulator tracks the
/// sketch's own counters, which carry collisions, so the answer is an estimate
/// with a real error bound and must be checked as one.
#[derive(Clone, Copy, Debug)]
pub struct SecondMomentSpec {
    pub rows: usize,
    pub cols: usize,
    pub kappa: f64,
}

impl SecondMomentSpec {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            kappa: 3.0,
        }
    }

    /// `sqrt(2 kappa / w)` — the relative half-width around the exact F2.
    pub fn relative_bound(&self) -> f64 {
        (2.0 * self.kappa / self.cols as f64).sqrt()
    }

    pub fn per_row_failure(&self) -> f64 {
        1.0 / self.kappa
    }

    /// `ceil(d/2)` bad rows, for the same reason as `CountSketchSpec`: an
    /// even-depth sketch reports the average of the two middle order
    /// statistics, so half the rows going bad in one direction is enough.
    pub fn bad_row_threshold(&self) -> usize {
        self.rows.div_ceil(2)
    }

    pub fn per_query_failure(&self) -> f64 {
        binomial_tail_ge(self.rows, self.bad_row_threshold(), self.per_row_failure())
    }

    pub fn check(&self, estimate: f64, exact_f2: f64) -> Result<(), String> {
        let rel = ((estimate - exact_f2) / exact_f2).abs();
        let bound = self.relative_bound();
        if rel <= bound {
            Ok(())
        } else {
            Err(format!(
                "F2 estimate {estimate:.6e} vs exact {exact_f2:.6e}: relative error {rel:.5} > \
                 sqrt(2*kappa/w) = {bound:.5} (rows={} cols={} kappa={})",
                self.rows, self.cols, self.kappa
            ))
        }
    }
}
