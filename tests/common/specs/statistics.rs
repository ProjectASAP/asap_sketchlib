//! Error-model specifications, one per *error metric*, plus the statistical
//! machinery that turns a guarantee into an acceptance rule.
//!
//! The specs in [`super::conformance`] carry loose smoke-test tolerances,
//! which is the right thing for wiring up a new adapter but wrong as a
//! statement of theory: KLL promises *rank* error while DDSketch promises
//! *relative value* error, and Count-Min's one-sided additive `eps*N` is not
//! Count Sketch's two-sided `L2/sqrt(w)`. Each spec here models exactly one
//! guarantee and nothing else, so no two families can be judged by the same
//! number by accident.
//!
//! # The statistical unit
//!
//! A bound of the form `P[error > B] <= p` is a statement about **one** draw of
//! the randomness the estimator is built on — one hash choice, one compaction
//! coin sequence, one sampling seed. Turning `n` observed checks into a
//! binomial tail at `p` requires those `n` checks to be `n` *independent draws
//! of that randomness*. Almost none of the natural check batteries are:
//!
//! - several quantiles `q` read off **one** KLL sketch share one compaction
//!   history;
//! - several keys read off **one** Count-Min or Count Sketch share one hash;
//! - increasing checkpoints on **one** HLL or KMV are nested — the later
//!   estimate contains the earlier one's state;
//! - a single-pass sketch and its merged twin share most of their state.
//!
//! So this file offers three acceptance rules and each caller must pick the one
//! that matches what it actually collected:
//!
//! | Rule | When it is valid |
//! | --- | --- |
//! | [`Tally::assert_none`] | structural facts, and *simultaneous* bounds whose failure probability was already union-bounded down over every check in the battery |
//! | [`Tally::assert_independent_binomial`] | every recorded check is a separate draw of the estimator's randomness (a fresh sketch seed, a fresh hash, a fresh sampling seed) |
//! | [`Tally::assert_rate_at_most`] | one fixed realisation, pinned against the guarantee's own marginal probability — a regression pin, not a probability statement |
//!
//! The way a battery over one sketch becomes a legitimate trial is to reduce it
//! to a single outcome first: the sketch's *maximum* rank error over the whole
//! `q` grid, or whether *any* probed key broke a simultaneous bound. That one
//! outcome, repeated over independent seeds, is a binomial.
//!
//! Where a guarantee has no closed form, or the public API cannot expose the
//! dimension it quantifies over, the test says so and is named
//! `*_stays_within_the_documented_empirical_band` rather than being dressed up
//! as theory. See `docs/e2e_coverage_matrix.md` for which is which.

#![allow(dead_code)]

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

/// The largest violation count a run of **independent** trials may show and
/// still be consistent with a per-trial failure probability of `p`, at test
/// level `test_level`.
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
/// acceptance rule still bites.
pub const TEST_LEVEL: f64 = 1e-6;

/// Total failure probability a *simultaneous* (union-bounded) battery is sized
/// at, before it is spread over the checks in the battery.
///
/// A simultaneous bound holds for **every** probed key at once with probability
/// `1 - SIMULTANEOUS_LEVEL`, so the battery tolerates zero violations and needs
/// no independence assumption anywhere. It buys that soundness by being wider
/// than the marginal bound by a factor that grows with the number of keys —
/// slowly, because both families' per-key failure probabilities decay
/// polynomially or exponentially in the bound's scale factor.
pub const SIMULTANEOUS_LEVEL: f64 = 1e-3;

// ---------------------------------------------------------------------------
// Violation tallies
// ---------------------------------------------------------------------------

/// Accumulates violations across checks so a single acceptance rule is applied
/// once, at the end, to the whole population.
///
/// A `Tally` is only a counter. Which acceptance rule is legitimate depends
/// entirely on what the caller recorded into it — see the module docs.
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

    /// Observed violation rate over the recorded checks.
    pub fn rate(&self) -> f64 {
        if self.checks == 0 {
            0.0
        } else {
            self.violations as f64 / self.checks as f64
        }
    }

    fn rendered(&self) -> String {
        self.samples
            .iter()
            .map(|s| format!("    {s}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Binomial acceptance at per-trial failure probability `p`.
    ///
    /// **Only valid when every recorded check is an independent draw of the
    /// randomness the bound quantifies over** — a distinct sketch seed, hash
    /// seed or sampling seed. Several queries against one sketch are not
    /// independent trials; reduce them to one outcome per sketch first.
    pub fn assert_independent_binomial(self, label: &str, p: f64, context: &str) {
        let allowed = max_allowed_failures(self.checks, p, TEST_LEVEL);
        assert!(
            self.violations <= allowed,
            "{label}: {} of {} independent trials violated the bound; a per-trial \
             failure probability p={p:.3e} allows at most {allowed} at test level \
             {TEST_LEVEL:.0e}.\n  context: {context}\n  first violations:\n{}",
            self.violations,
            self.checks,
            self.rendered(),
        );
    }

    /// Applies a rule that tolerates no violations at all.
    ///
    /// For structural guarantees (Count-Min never underestimates, Bloom never
    /// has a false negative, a merge is exact addition) and for *simultaneous*
    /// bounds, whose per-key failure probability has already been union-bounded
    /// down over the whole battery so that a single violation is significant.
    pub fn assert_none(self, label: &str, context: &str) {
        assert!(
            self.violations == 0,
            "{label}: {} of {} checks violated a guarantee that tolerates none.\n  \
             context: {context}\n  first violations:\n{}",
            self.violations,
            self.checks,
            self.rendered(),
        );
    }

    /// Pins the observed violation *rate* of one fixed realisation against the
    /// guarantee's own marginal per-check failure probability.
    ///
    /// This is **not** a probability statement: the sketch, its hash and its
    /// stream are all fixed, so the rate is a deterministic number and there is
    /// exactly one draw. It is a regression pin at a threshold the guarantee
    /// supplies (never a hand-picked percentage), and it is what catches an
    /// estimator whose error grew by an order of magnitude while still landing
    /// inside a union-bounded simultaneous band.
    pub fn assert_rate_at_most(self, label: &str, marginal_p: f64, context: &str) {
        let rate = self.rate();
        assert!(
            rate <= marginal_p,
            "{label}: {} of {} keys ({rate:.4}) exceeded the marginal bound, above the \
             guarantee's own marginal failure probability {marginal_p:.4}. This is a \
             single fixed realisation, so it is a regression pin rather than a tail \
             test.\n  context: {context}\n  first violations:\n{}",
            self.violations,
            self.checks,
            self.rendered(),
        );
    }
}
