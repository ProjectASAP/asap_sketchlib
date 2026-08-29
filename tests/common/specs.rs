//! Error-model specifications, one per *error metric*, plus the statistical
//! machinery that turns a theorem into an acceptance rule.
//!
//! The specs in [`super::conformance`] carry loose smoke-test tolerances,
//! which is the right thing for wiring up a new adapter but wrong as a
//! statement of theory: KLL promises *rank* error while DDSketch promises
//! *relative value* error, and Count-Min's one-sided additive `eps*N` is not
//! Count Sketch's two-sided `L2/sqrt(w)`. Each spec here models exactly one
//! guarantee and nothing else, so no two families can be judged by the same
//! number by accident.
//!
//! Every spec exposes three things:
//!
//! 1. the **bound formula**, computed from the sketch's own configuration and
//!    the exact ground truth — never a hand-picked percentage;
//! 2. the **per-check failure probability** the theorem allows; and
//! 3. an **acceptance rule** over many checks, derived from the binomial tail
//!    at a fixed test level, so the number of tolerated violations is fixed
//!    before the run rather than after seeing the result.
//!
//! Where a guarantee has no closed form, or the public API cannot expose the
//! dimension a theorem quantifies over, the test says so and is named
//! `*_stays_within_the_documented_empirical_band` rather than being dressed up
//! as theory. See `docs/e2e_coverage_matrix.md` for which is which.

#![allow(dead_code)]

use super::FreqTruth;

// ---------------------------------------------------------------------------
// Binomial machinery
// ---------------------------------------------------------------------------

/// `ln(n choose k)` via log-gamma, stable for the counts used here.
fn ln_choose(n: usize, k: usize) -> f64 {
    if k > n {
        return f64::NEG_INFINITY;
    }
    ln_factorial(n) - ln_factorial(k) - ln_factorial(n - k)
}

/// `ln(n!)` — exact summation below 256, Stirling/Lanczos above.
fn ln_factorial(n: usize) -> f64 {
    if n < 256 {
        (1..=n).map(|i| (i as f64).ln()).sum()
    } else {
        let x = n as f64 + 1.0;
        // Stirling series; accurate to well past the precision this needs.
        (x - 0.5) * x.ln() - x
            + 0.5 * (std::f64::consts::TAU).ln()
            + 1.0 / (12.0 * x)
            + -1.0 / (360.0 * x * x * x)
    }
}

/// `P(Binomial(n, p) >= k)`.
pub fn binomial_tail_ge(n: usize, k: usize, p: f64) -> f64 {
    if k == 0 {
        return 1.0;
    }
    if k > n {
        return 0.0;
    }
    if p <= 0.0 {
        return 0.0;
    }
    if p >= 1.0 {
        return 1.0;
    }
    let (lp, l1p) = (p.ln(), (1.0 - p).ln());
    let mut acc = 0.0;
    for i in k..=n {
        acc += (ln_choose(n, i) + i as f64 * lp + (n - i) as f64 * l1p).exp();
    }
    acc.min(1.0)
}

/// The largest violation count a run may show and still be consistent with a
/// per-check failure probability of `p`, at test level `test_level`.
///
/// Returns the smallest `c` with `P(Binomial(trials, p) > c) <= test_level`.
/// Fixing `test_level` up front is what stops a tolerance from being tuned to
/// whatever the current run happens to produce.
pub fn max_allowed_failures(trials: usize, p: f64, test_level: f64) -> usize {
    if trials == 0 {
        return 0;
    }
    for c in 0..=trials {
        if binomial_tail_ge(trials, c + 1, p) <= test_level {
            return c;
        }
    }
    trials
}

/// Standard test level for every acceptance rule in this file.
///
/// Every stream, every sketch seed and every trial grid in these suites is
/// fixed, so a run is a deterministic function of the code — re-running
/// cannot produce a different verdict and there is no flake to buy insurance
/// against. The level therefore only has to be small enough that a *correct*
/// implementation clears it with room to spare, and large enough that the
/// acceptance rule still bites: at 1e-6 a battery of a few hundred checks
/// tolerates roughly two to five times the theorem's expected violation
/// count, not twenty times.
pub const TEST_LEVEL: f64 = 1e-6;

// ---------------------------------------------------------------------------
// Violation tallies
// ---------------------------------------------------------------------------

/// Accumulates violations across checks (and across trials) so a single
/// acceptance rule is applied once, at the end, to the whole population.
#[derive(Debug, Default)]
pub struct Tally {
    pub checks: usize,
    pub violations: usize,
    /// Up to `SAMPLE_LIMIT` rendered violations, for the failure message.
    pub samples: Vec<String>,
}

const SAMPLE_LIMIT: usize = 12;

impl Tally {
    pub fn record(&mut self, ok: bool, detail: impl FnOnce() -> String) {
        self.checks += 1;
        if !ok {
            self.violations += 1;
            if self.samples.len() < SAMPLE_LIMIT {
                self.samples.push(detail());
            }
        }
    }

    /// Applies the binomial acceptance rule for a per-check failure
    /// probability of `p`, panicking with the full context on rejection.
    pub fn assert_within(self, label: &str, p: f64, context: &str) {
        let allowed = max_allowed_failures(self.checks, p, TEST_LEVEL);
        assert!(
            self.violations <= allowed,
            "{label}: {} of {} checks violated the bound; the theorem's per-check \
             failure probability p={p:.3e} allows at most {allowed} at test level \
             {TEST_LEVEL:.0e}.\n  context: {context}\n  first violations:\n{}",
            self.violations,
            self.checks,
            self.samples
                .iter()
                .map(|s| format!("    {s}"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }

    /// Applies a rule that tolerates no violations at all — for structural
    /// guarantees (Count-Min never underestimates, Bloom never has a false
    /// negative) rather than probabilistic ones.
    pub fn assert_none(self, label: &str, context: &str) {
        assert!(
            self.violations == 0,
            "{label}: {} of {} checks violated a structural (non-probabilistic) \
             guarantee.\n  context: {context}\n  first violations:\n{}",
            self.violations,
            self.checks,
            self.samples
                .iter()
                .map(|s| format!("    {s}"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
}

// ---------------------------------------------------------------------------
// Count-Min: one-sided additive error
// ---------------------------------------------------------------------------

/// Count-Min Sketch, Cormode & Muthukrishnan (2005), Theorem 1.
///
/// For a sketch of `d` rows and `w` columns, with `f` the exact frequency
/// vector and `N = ||f||_1`:
///
/// - **structural:** `est(i) >= f(i)` for every key, always, no probability;
/// - **probabilistic:** `P[ est(i) - f(i) > e * (N - f(i)) / w ] <= e^-d`.
///
/// The row estimator is `f(i) + X_r` with `X_r >= 0` and
/// `E[X_r] = (N - f(i)) / w` (only the *other* keys can collide into `i`'s
/// cell). Markov gives `P[X_r >= e * E[X_r]] <= 1/e` per row; the `d` rows use
/// independent hashes and the minimum exceeds the bound only if every row
/// does, hence `e^-d`.
///
/// `N - f(i)` rather than `N` is the tighter and correct tail mass: it is what
/// the expectation actually is, and using `N` would silently hand a hot key a
/// larger budget than the theorem grants it.
#[derive(Clone, Copy, Debug)]
pub struct CountMinSpec {
    pub rows: usize,
    pub cols: usize,
}

impl CountMinSpec {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self { rows, cols }
    }

    /// `e * (N - f_i) / w` — the additive excess budget for one key.
    pub fn excess_bound(&self, total_mass: f64, key_mass: f64) -> f64 {
        std::f64::consts::E * (total_mass - key_mass) / self.cols as f64
    }

    /// `e^-d` — the probability a single key's minimum exceeds the budget.
    pub fn per_key_failure(&self) -> f64 {
        (-(self.rows as f64)).exp()
    }

    /// Runs the full Count-Min contract over every key in `truth`.
    ///
    /// The one-sided half is asserted with zero tolerated violations (it is
    /// structural); the additive half uses the binomial acceptance rule at
    /// `e^-d`.
    pub fn assert_contract<F>(&self, label: &str, truth: &FreqTruth, estimate: F, context: &str)
    where
        F: Fn(i64) -> f64,
    {
        let total = truth.total() as f64;
        let mut one_sided = Tally::default();
        let mut additive = Tally::default();
        for (key, count) in truth.pairs() {
            let est = estimate(key);
            let f = count as f64;
            one_sided.record(est >= f, || {
                format!("key {key}: est {est} < true {f} (Count-Min must never underestimate)")
            });
            let bound = self.excess_bound(total, f);
            additive.record(est - f <= bound, || {
                format!(
                    "key {key}: excess {:.1} > e*(N-f)/w = {bound:.1} (true {f}, est {est})",
                    est - f
                )
            });
        }
        let ctx = format!(
            "{context}; rows={} cols={} N={total:.0} distinct={}",
            self.rows,
            self.cols,
            truth.distinct()
        );
        one_sided.assert_none(&format!("{label} / one-sided"), &ctx);
        additive.assert_within(
            &format!("{label} / additive e*(N-f)/w"),
            self.per_key_failure(),
            &ctx,
        );
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
/// `kappa = 3` is the usual choice (per-row failure 1/3, so the median is a
/// genuine amplification); it is a declared constant of the spec, not a fudge
/// factor multiplied onto a bound after the fact.
#[derive(Clone, Copy, Debug)]
pub struct CountSketchSpec {
    pub rows: usize,
    pub cols: usize,
    /// Chebyshev slack. Per-row failure probability is `1 / kappa`.
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

    /// `sqrt(kappa / w) * ||f_{-i}||_2` for one key's residual L2 norm.
    pub fn error_scale(&self, residual_l2: f64) -> f64 {
        (self.kappa / self.cols as f64).sqrt() * residual_l2
    }

    /// `1 / kappa` — Chebyshev's per-row failure probability.
    pub fn per_row_failure(&self) -> f64 {
        1.0 / self.kappa
    }

    /// `P[Binomial(d, 1/kappa) >= ceil(d/2)]` — failure after the row median.
    pub fn per_key_failure(&self) -> f64 {
        let majority = self.rows / 2 + 1;
        binomial_tail_ge(self.rows, majority, self.per_row_failure())
    }

    /// Runs the L2 contract over every key in `truth`.
    ///
    /// The residual norm is recomputed per key from the exact frequency
    /// vector: `||f_{-i}||_2 = sqrt(F2 - f(i)^2)`.
    pub fn assert_contract<F>(&self, label: &str, truth: &FreqTruth, estimate: F, context: &str)
    where
        F: Fn(i64) -> f64,
    {
        let mut tally = Tally::default();
        self.tally_into(&mut tally, truth, estimate);
        let ctx = format!(
            "{context}; rows={} cols={} kappa={} F2={:.3e} distinct={}",
            self.rows,
            self.cols,
            self.kappa,
            truth.f2(),
            truth.distinct()
        );
        tally.assert_within(label, self.per_key_failure(), &ctx);
    }

    /// Same checks, accumulated into a caller-owned tally so several
    /// independent trials share one acceptance rule.
    pub fn tally_into<F>(&self, tally: &mut Tally, truth: &FreqTruth, estimate: F)
    where
        F: Fn(i64) -> f64,
    {
        let f2 = truth.f2();
        for (key, count) in truth.pairs() {
            let f = count as f64;
            let residual_l2 = (f2 - f * f).max(0.0).sqrt();
            let bound = self.error_scale(residual_l2);
            let est = estimate(key);
            tally.record((est - f).abs() <= bound, || {
                format!(
                    "key {key}: |{est:.1} - {f}| = {:.1} > sqrt(kappa/w)*||f_-i||_2 = {bound:.1}",
                    (est - f).abs()
                )
            });
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

    pub fn per_query_failure(&self) -> f64 {
        binomial_tail_ge(self.rows, self.rows / 2 + 1, self.per_row_failure())
    }

    pub fn check(&self, estimate: f64, exact_f2: f64) -> Result<(), String> {
        let rel = ((estimate - exact_f2) / exact_f2).abs();
        let bound = self.relative_bound();
        if rel <= bound {
            Ok(())
        } else {
            Err(format!(
                "F2 estimate {estimate:.6e} vs exact {exact_f2:.6e}: relative error {rel:.5} >                  sqrt(2*kappa/w) = {bound:.5} (rows={} cols={} kappa={})",
                self.rows, self.cols, self.kappa
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// KLL: rank error
// ---------------------------------------------------------------------------

/// Normalized-rank-error contract for a KLL sketch of parameter `k`.
///
/// KLL (Karnin, Lang & Liberty, FOCS 2016) promises *rank* error, not value
/// error: for a query at normalized rank `q` the returned value `v` must
/// satisfy
///
/// ```text
///   rank_incl(v) >= q - eps   and   rank_excl(v) <= q + eps
/// ```
///
/// where `rank_excl(v) = |{x < v}| / n` and `rank_incl(v) = |{x <= v}| / n`.
/// Stating it on the *interval* of ranks that `v` occupies is what makes the
/// check correct on streams with many repeated values, where a single value
/// legitimately spans a wide rank range.
///
/// The constant comes from the Apache DataSketches published contract for KLL,
/// `getNormalizedRankError(k, false)`:
///
/// ```text
///   eps(k) = 2.446 / k^0.9433      (single-sided, at 99% confidence)
/// ```
///
/// so `k = 200` gives `eps ~ 0.0165` and a per-query failure probability of
/// 0.01. That is *tighter* than the 0.02/0.03 constants it replaces, and it is
/// tied to `k`: doubling `k` halves the band, which a hard-coded 0.02 does not.
#[derive(Clone, Copy, Debug)]
pub struct RankErrorSpec {
    pub k: usize,
    /// Per-query failure probability the constant is quoted at.
    pub failure_probability: f64,
}

impl RankErrorSpec {
    /// The DataSketches contract at 99% confidence.
    pub fn datasketches(k: usize) -> Self {
        Self {
            k,
            failure_probability: 0.01,
        }
    }

    /// `eps(k) = 2.446 / k^0.9433`.
    pub fn epsilon(&self) -> f64 {
        2.446 / (self.k as f64).powf(0.9433)
    }

    /// Checks one `(q, value)` answer against the rank band of `sorted`.
    ///
    /// `sorted` must be ascending. Returns `Ok(())` or a rendered violation.
    pub fn check(&self, sorted: &[f64], q: f64, value: f64) -> Result<(), String> {
        rank_violation(sorted, q, value, self.epsilon())
            .map(|d| Err(format!("{d} (k={})", self.k)))
            .unwrap_or(Ok(()))
    }

    /// Tallies one sketch's answers over a q grid.
    pub fn tally_into<F>(&self, tally: &mut Tally, sorted: &[f64], qs: &[f64], quantile: F)
    where
        F: Fn(f64) -> f64,
    {
        for &q in qs {
            let v = quantile(q);
            let outcome = self.check(sorted, q, v);
            tally.record(outcome.is_ok(), || outcome.unwrap_err());
        }
    }
}

/// The rank-error predicate itself, independent of where `eps` came from.
///
/// A quantile answer `value` for a query at normalized rank `q` is correct
/// within `eps` when the interval of ranks `value` occupies overlaps
/// `[q - eps, q + eps]`. Stating it on the interval rather than on a single
/// rank is what keeps the predicate correct when values repeat.
///
/// Returns `None` when the answer is within the band, or a rendered violation.
/// Shared so that estimators deriving `eps` from a different theorem — KLL
/// from `k`, UnivMon-Q from its occurrence-sample bound — check the same
/// predicate rather than each rolling their own.
pub fn rank_violation(sorted: &[f64], q: f64, value: f64, eps: f64) -> Option<String> {
    let n = sorted.len();
    assert!(n > 0, "rank error is undefined on an empty stream");
    let excl = sorted.partition_point(|x| *x < value) as f64 / n as f64;
    let incl = sorted.partition_point(|x| *x <= value) as f64 / n as f64;
    if incl >= q - eps && excl <= q + eps {
        None
    } else {
        Some(format!(
            "q={q}: value {value} occupies ranks [{excl:.5}, {incl:.5}], outside \
             [q-eps, q+eps] = [{:.5}, {:.5}] for eps={eps:.5}",
            q - eps,
            q + eps
        ))
    }
}

/// Two-sided Dvoretzky-Kiefer-Wolfowitz / Hoeffding band for an empirical CDF
/// built from `m` uniform samples of the stream:
///
/// ```text
///   P[ sup_x |F_m(x) - F(x)| > sqrt(ln(2/delta) / (2m)) ] <= delta
/// ```
///
/// This is the residual term `epsilon_R` in UnivMon-Q's documented
/// ordered-query bound, and it is the whole bound in the diffuse regime where
/// the heavy set is empty.
pub fn occurrence_sample_epsilon(samples: usize, delta: f64) -> f64 {
    ((2.0 / delta).ln() / (2.0 * samples as f64)).sqrt()
}

// ---------------------------------------------------------------------------
// DDSketch: relative value error
// ---------------------------------------------------------------------------

/// Relative-value-error contract for a DDSketch of accuracy parameter `alpha`.
///
/// DDSketch's guarantee is on the *value*, not the rank: the returned estimate
/// for the `q`-quantile must be within `alpha` relative error of the **exact
/// nearest-rank order statistic** at the same `q`.
///
/// ```text
///   |est - true| / |true| <= alpha + numerical_slack(true)
/// ```
///
/// With the logarithmic mapping `k = floor(ln v / ln gamma)`,
/// `gamma = (1+alpha)/(1-alpha)`, and bucket representative
/// `gamma^k * (1+alpha)`, a value at the bucket's lower edge is over-estimated
/// by exactly `alpha` and one approaching the upper edge is under-estimated by
/// almost `alpha` — so `alpha` is attained but never exceeded in exact
/// arithmetic.
///
/// `numerical_slack` is therefore a *floating-point* term only, on the order of
/// a few ULP: `ln`, `floor` and `powf` compose to a relative error of roughly
/// `|ln v| * f64::EPSILON`. It is emphatically not a percentage of `alpha` —
/// `alpha * 1.05` would accept results that break the advertised guarantee by
/// 5%, which is the entire thing the guarantee exists to forbid.
#[derive(Clone, Copy, Debug)]
pub struct RelativeQuantileSpec {
    pub alpha: f64,
}

impl RelativeQuantileSpec {
    pub fn new(alpha: f64) -> Self {
        Self { alpha }
    }

    /// A few ULP of headroom, scaled by the magnitude of the logarithm because
    /// that is what `powf`/`ln` round-off actually tracks.
    pub fn numerical_slack(&self, true_value: f64) -> f64 {
        8.0 * f64::EPSILON * (1.0 + true_value.abs().ln().abs())
    }

    pub fn tolerance(&self, true_value: f64) -> f64 {
        self.alpha + self.numerical_slack(true_value)
    }

    /// Checks one estimate against the exact order statistic.
    pub fn check(&self, q: f64, estimate: f64, true_value: f64) -> Result<(), String> {
        let tol = self.tolerance(true_value);
        let rel = ((estimate - true_value) / true_value.abs()).abs();
        if rel <= tol {
            Ok(())
        } else {
            Err(format!(
                "q={q}: est {estimate:.10e} vs exact order statistic {true_value:.10e} \
                 -> relative error {rel:.3e} > alpha + slack = {tol:.3e} (alpha={})",
                self.alpha
            ))
        }
    }

    /// Tallies a sketch's answers over a q grid against exact order statistics.
    ///
    /// `sorted` must be ascending; the nearest-rank (ceil) convention is used,
    /// matching `DDSketch::get_value_at_quantile`.
    pub fn tally_into<F>(&self, tally: &mut Tally, sorted: &[f64], qs: &[f64], quantile: F)
    where
        F: Fn(f64) -> Option<f64>,
    {
        let n = sorted.len();
        for &q in qs {
            let idx = ((q.clamp(0.0, 1.0) * n as f64).ceil() as usize).clamp(1, n);
            let truth = sorted[idx - 1];
            match quantile(q) {
                None => tally.record(false, || format!("q={q}: sketch returned None")),
                Some(est) => {
                    let outcome = self.check(q, est, truth);
                    tally.record(outcome.is_ok(), || outcome.unwrap_err());
                }
            }
        }
    }
}

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
    /// K-Minimum-Values: the estimator `(k-1)/U_(k)` has relative standard
    /// error `1 / sqrt(k - 2)`, which this models as `1 / sqrt(k - 1)` — the
    /// customary and marginally conservative form. Below `k` distinct
    /// elements the sketch is exact, not estimated.
    KMinimumValues,
}

/// A cardinality band `n * (1 +- z * sigma_rel)` with an explicit `z`.
///
/// Nothing here is a flat percentage: `sigma_rel` comes from the estimator's
/// own error model and `z` is stated as a Gaussian quantile, so the failure
/// probability of each check is `2 * (1 - Phi(z))` and the whole battery gets
/// a binomial acceptance rule like every other spec.
#[derive(Clone, Copy, Debug)]
pub struct CardinalityConfidenceSpec {
    pub model: CardinalityModel,
    /// `m` registers for HLL, `k` retained minima for KMV.
    pub size: usize,
    /// Gaussian quantile. `z = 4` is a two-sided failure probability of
    /// 6.3e-5 per check.
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

    /// The estimator's relative standard error.
    pub fn sigma_rel(&self) -> f64 {
        let n = self.size as f64;
        match self.model {
            CardinalityModel::HyperLogLogRegisters => 1.04 / n.sqrt(),
            CardinalityModel::HyperLogLogHip => (std::f64::consts::LN_2 / n).sqrt(),
            CardinalityModel::KMinimumValues => 1.0 / (n - 1.0).sqrt(),
        }
    }

    /// `z * sigma_rel` — the half-width of the accepted relative band.
    pub fn tolerance(&self) -> f64 {
        self.z * self.sigma_rel()
    }

    /// Two-sided Gaussian failure probability at `z`.
    pub fn per_check_failure(&self) -> f64 {
        2.0 * (1.0 - standard_normal_cdf(self.z))
    }

    /// KMV below `k` distinct elements keeps every hash it has seen, so the
    /// estimate is exact and no band applies.
    pub fn is_exact_regime(&self, true_distinct: usize) -> bool {
        self.model == CardinalityModel::KMinimumValues && true_distinct <= self.size
    }

    pub fn check(&self, estimate: f64, true_distinct: usize) -> Result<(), String> {
        let t = true_distinct as f64;
        if self.is_exact_regime(true_distinct) {
            // Exact regime: the sketch holds every distinct hash it has seen.
            return if (estimate - t).abs() <= 0.5 {
                Ok(())
            } else {
                Err(format!(
                    "n={true_distinct} <= k={}: estimate {estimate} must be exact",
                    self.size
                ))
            };
        }
        let tol = self.tolerance();
        let rel = ((estimate - t) / t).abs();
        if rel <= tol {
            Ok(())
        } else {
            Err(format!(
                "n={true_distinct}: estimate {estimate:.1} -> relative error {rel:.5} > \
                 z*sigma = {:.2}*{:.5} = {tol:.5} (model {:?}, size {})",
                self.z,
                self.sigma_rel(),
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

// ---------------------------------------------------------------------------
// Nitro: sampling confidence
// ---------------------------------------------------------------------------

/// Confidence band for a Bernoulli-sampled counter, as NitroSketch uses.
///
/// Nitro admits each update with probability `p` and writes a weight of
/// `ceil(1/p)`, so for a key of true count `f` the number of admitted updates
/// is `X ~ Binomial(f, p)` and the estimate is `X * ceil(1/p)`. With `1/p`
/// integral this is unbiased with
///
/// ```text
///   sd(est) = (1/p) * sqrt(f * p * (1-p)) = sqrt(f * (1-p) / p)
/// ```
///
/// so the accepted band is `f +- z * sqrt(f (1-p) / p)`, plus whatever the
/// wrapped sketch's own error contributes. At `p = 1` the band collapses to
/// zero width, which is correct — full sampling is exact.
#[derive(Clone, Copy, Debug)]
pub struct SamplingConfidenceSpec {
    pub rate: f64,
    pub z: f64,
}

impl SamplingConfidenceSpec {
    pub fn new(rate: f64, z: f64) -> Self {
        Self { rate, z }
    }

    /// Standard deviation of the scaled estimate for a key of count `f`.
    pub fn sigma(&self, true_count: f64) -> f64 {
        if self.rate >= 1.0 {
            return 0.0;
        }
        (true_count * (1.0 - self.rate) / self.rate).sqrt()
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
            Err(format!(
                "rate={} : estimate {estimate:.1} vs true {true_count:.0}, |error| {:.1} > \
                 z*sigma + slack = {:.2}*{:.2} + {sketch_slack:.1} = {half:.1}",
                self.rate,
                (estimate - true_count).abs(),
                self.z,
                self.sigma(true_count),
            ))
        }
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
