use super::statistics::*;

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
/// # Where the constant comes from, and what it is not
///
/// The KLL paper's guarantee is asymptotic: a sketch of `O(k)` retained items
/// answers every rank query within `eps = O(1/k)` with constant probability,
/// with the constants left unresolved and no closed form for a specific
/// implementation's compactor schedule.
///
/// The number used here,
///
/// ```text
///   eps(k) = 2.446 / k^0.9433
/// ```
///
/// is **not** that theorem. It is Apache DataSketches' published
/// characterization result: a least-squares fit, over a large body of
/// characterization runs of *their* implementation, to the 99th percentile of
/// the **maximum** rank error observed across a whole quantile grid
/// (`getNormalizedRankError(k, false)`). This crate's KLL implements the same
/// compact layout (capacity decay 2/3, `m = 8`, randomized halving with a
/// shared coin per compaction), so the fit is the right *external empirical
/// contract to hold it to* — a characterization target, not a proof about this
/// code.
///
/// Two consequences for how it must be tested:
///
/// - the quantity the constant bounds is the **maximum** rank error over the
///   grid, so one sketch produces exactly **one** pass/fail outcome — the
///   individual `q` values are not separate Bernoulli draws;
/// - the quoted 1% is the failure probability of that single per-sketch
///   outcome, so a binomial over **independent compaction seeds** is the only
///   legitimate aggregation.
#[derive(Clone, Copy, Debug)]
pub struct KllRankSpec {
    pub k: usize,
    /// Failure probability of one *sketch trial* — of the maximum rank error
    /// over a whole `q` grid, not of a single `q`.
    pub trial_failure_probability: f64,
}

impl KllRankSpec {
    /// The Apache DataSketches characterization target at 99% confidence.
    pub fn datasketches(k: usize) -> Self {
        Self {
            k,
            trial_failure_probability: 0.01,
        }
    }

    /// `eps(k) = 2.446 / k^0.9433`.
    pub fn epsilon(&self) -> f64 {
        2.446 / (self.k as f64).powf(0.9433)
    }

    /// The maximum normalized rank error one sketch shows over `qs`, together
    /// with the `q` that attained it.
    ///
    /// This is the reduction that turns a whole grid of dependent queries into
    /// the single number the contract is quoted for.
    pub fn max_rank_error<F>(&self, sorted: &[f64], qs: &[f64], quantile: F) -> (f64, String)
    where
        F: Fn(f64) -> f64,
    {
        let mut worst = 0.0f64;
        let mut detail = String::from("(no query exceeded rank 0)");
        for &q in qs {
            let v = quantile(q);
            let e = rank_error(sorted, q, v);
            if e >= worst {
                worst = e;
                let (excl, incl) = rank_interval(sorted, v);
                detail = format!(
                    "q={q}: value {v} occupies ranks [{excl:.6}, {incl:.6}], rank error {e:.6}"
                );
            }
        }
        (worst, detail)
    }

    /// Records one sketch trial: pass iff its maximum rank error over `qs` is
    /// within `eps(k)`.
    pub fn record_trial<F>(
        &self,
        tally: &mut Tally,
        label: &str,
        sorted: &[f64],
        qs: &[f64],
        quantile: F,
    ) where
        F: Fn(f64) -> f64,
    {
        let eps = self.epsilon();
        let (worst, detail) = self.max_rank_error(sorted, qs, quantile);
        tally.record(worst <= eps, || {
            format!(
                "{label}: max rank error {worst:.6} > eps(k={}) = {eps:.6}; worst {detail}",
                self.k
            )
        });
    }

    /// Checks one `(q, value)` answer against the rank band of `sorted`.
    ///
    /// Useful where a single query is the whole answer (an exact-window check,
    /// a single-key probe). Aggregating many of these from one sketch under a
    /// binomial is not.
    pub fn check(&self, sorted: &[f64], q: f64, value: f64) -> Result<(), String> {
        rank_violation(sorted, q, value, self.epsilon())
            .map(|d| Err(format!("{d} (k={})", self.k)))
            .unwrap_or(Ok(()))
    }
}

/// The rank interval `[rank_excl(v), rank_incl(v)]` that `value` occupies.
pub fn rank_interval(sorted: &[f64], value: f64) -> (f64, f64) {
    let n = sorted.len();
    assert!(n > 0, "rank error is undefined on an empty stream");
    let excl = sorted.partition_point(|x| *x < value) as f64 / n as f64;
    let incl = sorted.partition_point(|x| *x <= value) as f64 / n as f64;
    (excl, incl)
}

/// Normalized rank error of answering `value` for a query at rank `q`:
/// the distance from `q` to the interval of ranks `value` occupies, and `0`
/// when `q` falls inside it.
///
/// Measuring the distance to the *interval* rather than to a single rank is
/// what keeps the quantity correct on streams with repeated values, where one
/// value legitimately spans a wide rank range.
pub fn rank_error(sorted: &[f64], q: f64, value: f64) -> f64 {
    let (excl, incl) = rank_interval(sorted, value);
    (q - incl).max(excl - q).max(0.0)
}

/// The rank-error predicate itself, independent of where `eps` came from.
///
/// A quantile answer `value` for a query at normalized rank `q` is correct
/// within `eps` when the interval of ranks `value` occupies overlaps
/// `[q - eps, q + eps]`.
///
/// Returns `None` when the answer is within the band, or a rendered violation.
/// Shared so that estimators deriving `eps` from a different argument — KLL
/// from `k`, UnivMon-Q from its occurrence-sample bound — check the same
/// predicate rather than each rolling their own.
pub fn rank_violation(sorted: &[f64], q: f64, value: f64, eps: f64) -> Option<String> {
    let (excl, incl) = rank_interval(sorted, value);
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
/// the heavy set is empty. Because it is a `sup_x` statement it already covers
/// every query against one sketch at once: one sketch is one trial.
pub fn occurrence_sample_epsilon(samples: usize, delta: f64) -> f64 {
    ((2.0 / delta).ln() / (2.0 * samples as f64)).sqrt()
}

// ---------------------------------------------------------------------------
// Kolmogorov distance between a step CDF and an exact empirical CDF
// ---------------------------------------------------------------------------

/// `sup_x |F_hat(x) - F(x)|`, computed exactly, plus the `x` that attains it.
///
/// `estimated` is a step CDF as `(value, cumulative rank)` pairs in ascending
/// value order — the shape `UnivMonQ::cdf` returns. `sorted_truth` is the exact
/// stream, ascending, with multiplicity.
///
/// # Both CDFs are right-continuous, and that makes the sweep exact
///
/// ```text
///   F(x)     = #{ i : x_i <= x } / n
///   F_hat(x) = rank of the last breakpoint whose value is <= x   (0 if none)
/// ```
///
/// Both are step functions that jump only at points of
/// `S = {distinct truth values} ∪ {breakpoint values}`, and both are constant
/// on every interval between consecutive elements of `S`. So `|F_hat - F|` is
/// constant there too, and its value on `[s_k, s_{k+1})` is its value **at**
/// `s_k`. Below `min S` both functions are 0. Evaluating at every element of
/// `S` therefore returns the true supremum over all of `R` — not a sample of
/// it.
///
/// Both inputs are already ascending, so the evaluation is a single linear
/// merge: `O(n + m)` with one pass over each, rather than re-sorting the union
/// and binary-searching both sides at every point.
///
/// # Why this is not the same as a rank-interval check
///
/// Scoring each *estimated breakpoint* against the true rank interval of its
/// own value only ever looks at `x` values the estimate itself chose. An
/// estimate that is right wherever it has a breakpoint and simply *has no
/// breakpoint* across a region carrying a lot of mass scores zero error under
/// that check and arbitrarily large error under this one.
/// `cdf_sup_distance_detects_a_gap_a_breakpoint_scan_misses` in
/// `tests/e2e_quantiles.rs` is exactly that fixture.
pub fn cdf_sup_distance(estimated: &[(f64, f64)], sorted_truth: &[f64]) -> (f64, f64) {
    let n = sorted_truth.len();
    assert!(
        n > 0,
        "the Kolmogorov distance is undefined on an empty stream"
    );

    let mut i = 0usize; // truth observations at or below the current x
    let mut j = 0usize; // breakpoints at or below the current x
    let mut hat = 0.0f64; // F_hat at the current x
    let mut worst = 0.0f64;
    let mut worst_at = f64::NAN;

    while i < n || j < estimated.len() {
        // The next jump point of either function.
        let x = match (sorted_truth.get(i), estimated.get(j)) {
            (Some(t), Some((v, _))) => {
                if t.total_cmp(v) == std::cmp::Ordering::Greater {
                    *v
                } else {
                    *t
                }
            }
            (Some(t), None) => *t,
            (None, Some((v, _))) => *v,
            (None, None) => unreachable!("the loop condition guarantees one side is live"),
        };
        while i < n && sorted_truth[i].total_cmp(&x) != std::cmp::Ordering::Greater {
            i += 1;
        }
        while j < estimated.len() && estimated[j].0.total_cmp(&x) != std::cmp::Ordering::Greater {
            hat = estimated[j].1;
            j += 1;
        }
        let d = (hat - i as f64 / n as f64).abs();
        if d > worst {
            worst = d;
            worst_at = x;
        }
    }
    (worst, worst_at)
}

/// For each estimated breakpoint, the distance from its reported rank to the
/// true rank interval of its own value.
///
/// Kept **only** so a fixture can show that it reports zero where
/// [`cdf_sup_distance`] reports a large error. It is not a Kolmogorov distance
/// and must not be used as one.
pub fn breakpoint_rank_interval_distance(estimated: &[(f64, f64)], sorted_truth: &[f64]) -> f64 {
    let mut worst = 0.0f64;
    for &(value, rank) in estimated {
        let (excl, incl) = rank_interval(sorted_truth, value);
        worst = worst.max((rank - incl).max(excl - rank).max(0.0));
    }
    worst
}

// ---------------------------------------------------------------------------
// DDSketch: relative value error
// ---------------------------------------------------------------------------

/// Which order statistic a DDSketch implementation answers a quantile query
/// with. The two shipped implementations do not agree, and the truth a test
/// compares against has to follow the implementation it is testing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DdRankConvention {
    /// `DDSketch::get_value_at_quantile`: `rank = ceil(q * n)`, 1-based, so the
    /// answer is `sorted[ceil(q*n) - 1]`. `q = 0` and `q = 1` short-circuit to
    /// the exact stored minimum and maximum.
    CeilNearestRank,
    /// Portable `DdSketch::quantile`: `target = floor(q * (n - 1))`, 0-based,
    /// so the answer is `sorted[floor(q*(n-1))]` — the lower-quantile
    /// convention of the DDSketch paper and of DataDog's reference
    /// implementation. No exact minimum or maximum is retained, so the
    /// endpoints are bucket representatives like any other rank.
    LowerFloor,
}

impl DdRankConvention {
    /// The zero-based index into an ascending `sorted` slice this convention
    /// answers `q` with.
    pub fn index(self, n: usize, q: f64) -> usize {
        assert!(
            n > 0,
            "a quantile convention is undefined on an empty stream"
        );
        let q = q.clamp(0.0, 1.0);
        match self {
            DdRankConvention::CeilNearestRank => (((q * n as f64).ceil() as usize).clamp(1, n)) - 1,
            DdRankConvention::LowerFloor => ((q * (n - 1) as f64).floor() as usize).min(n - 1),
        }
    }

    /// The exact order statistic this convention answers `q` with.
    pub fn order_statistic(self, sorted: &[f64], q: f64) -> f64 {
        sorted[self.index(sorted.len(), q)]
    }

    pub fn name(self) -> &'static str {
        match self {
            DdRankConvention::CeilNearestRank => "ceil(q*n) nearest-rank",
            DdRankConvention::LowerFloor => "floor(q*(n-1)) lower-quantile",
        }
    }
}

/// Relative-value-error contract for a DDSketch of accuracy parameter `alpha`.
///
/// DDSketch's guarantee is on the *value*, not the rank: the returned estimate
/// for the `q`-quantile must be within `alpha` relative error of the exact
/// order statistic **at the same `q` under that implementation's own rank
/// convention**:
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
/// arithmetic. The guarantee is deterministic: there is no hash and no
/// sampling, so it tolerates **zero** violations and no statistical model
/// applies at all.
///
/// `numerical_slack` is therefore a *floating-point* term only, on the order of
/// a few ULP: `ln`, `floor` and `powf` compose to a relative error of roughly
/// `|ln v| * f64::EPSILON`. It is emphatically not a percentage of `alpha` —
/// `alpha * 1.05` would accept results that break the advertised guarantee by
/// 5%, which is the entire thing the guarantee exists to forbid.
#[derive(Clone, Copy, Debug)]
pub struct RelativeQuantileSpec {
    pub alpha: f64,
    pub convention: DdRankConvention,
}

impl RelativeQuantileSpec {
    /// A spec for the core `DDSketch`.
    pub fn core(alpha: f64) -> Self {
        Self {
            alpha,
            convention: DdRankConvention::CeilNearestRank,
        }
    }

    /// A spec for the portable `DdSketch`.
    pub fn portable(alpha: f64) -> Self {
        Self {
            alpha,
            convention: DdRankConvention::LowerFloor,
        }
    }

    /// A few ULP of headroom, scaled by the magnitude of the logarithm because
    /// that is what `powf`/`ln` round-off actually tracks.
    ///
    /// The estimate is `gamma^k * (1 + alpha)` with `k = floor(ln v / ln
    /// gamma)`. Three roundings compose: `ln v` (relative error `eps`, absolute
    /// error `|ln v| * eps`), the division by `ln gamma`, and `powf`, whose
    /// result carries a relative error of about `|k * ln gamma| * eps =
    /// |ln v| * eps`. Summing the three and rounding the constant up gives
    /// `8 * eps * (1 + |ln v|)`, which is a few ULP at every magnitude these
    /// suites reach and never a fraction of `alpha`.
    pub fn numerical_slack(&self, true_value: f64) -> f64 {
        8.0 * f64::EPSILON * (1.0 + true_value.abs().ln().abs())
    }

    pub fn tolerance(&self, true_value: f64) -> f64 {
        self.alpha + self.numerical_slack(true_value)
    }

    /// Checks one estimate against an exact value supplied by the caller.
    pub fn check(&self, q: f64, estimate: f64, true_value: f64) -> Result<(), String> {
        let tol = self.tolerance(true_value);
        let rel = ((estimate - true_value) / true_value.abs()).abs();
        if rel <= tol {
            Ok(())
        } else {
            Err(format!(
                "q={q}: est {estimate:.10e} vs exact {true_value:.10e} \
                 -> relative error {rel:.3e} > alpha + slack = {tol:.3e} (alpha={}, {})",
                self.alpha,
                self.convention.name(),
            ))
        }
    }

    /// Tallies a sketch's answers over a q grid against the exact order
    /// statistics **of this spec's own rank convention**.
    ///
    /// `sorted` must be ascending. Using one truth helper for both
    /// implementations would compare each against the other's question.
    pub fn tally_into<F>(&self, tally: &mut Tally, sorted: &[f64], qs: &[f64], quantile: F)
    where
        F: Fn(f64) -> Option<f64>,
    {
        for &q in qs {
            let truth = self.convention.order_statistic(sorted, q);
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
