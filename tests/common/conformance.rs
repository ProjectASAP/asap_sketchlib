//! Reusable conformance batteries for sketch implementations.
//!
//! New sketches plug into this kit by implementing one or more capability
//! traits (`FrequencyOps`, `SignedFrequencyOps`, `CardinalityOps`,
//! `QuantileOps`, `MergeOps`) and calling the matching `*_battery` functions.
//! Every battery is deterministic (seeded streams) and reports structured
//! failures; call [`BatteryReport::assert_ok`] to turn them into test
//! failures with expected-vs-actual context.
//!
//! See `tests/README.md` for the onboarding recipe.

use super::{FreqTruth, NumericTruth};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct BatteryFailure {
    pub check: String,
    pub detail: String,
}

#[derive(Debug)]
pub struct BatteryReport {
    pub sketch: String,
    pub battery: &'static str,
    pub failures: Vec<BatteryFailure>,
}

impl BatteryReport {
    fn record(&mut self, check: &str, ok: bool, detail: String) {
        if !ok {
            self.failures.push(BatteryFailure {
                check: check.to_string(),
                detail,
            });
        }
    }

    /// Panic with all accumulated failures if any check failed.
    pub fn assert_ok(self) {
        if !self.failures.is_empty() {
            let rendered: Vec<String> = self
                .failures
                .iter()
                .map(|f| format!("  [{}] {}", f.check, f.detail))
                .collect();
            panic!(
                "{} / {} conformance failures:\n{}",
                self.sketch,
                self.battery,
                rendered.join("\n")
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Capability traits (implement these on an adapter for your sketch)
// ---------------------------------------------------------------------------

/// Point-frequency queries over copyable keys.
pub trait FrequencyOps<K> {
    fn ingest(&mut self, key: &K);
    fn estimate(&self, key: &K) -> f64;
}

/// Weighted/signed ingestion for turnstile-capable sketches.
pub trait SignedFrequencyOps<K>: FrequencyOps<K> {
    fn ingest_weighted(&mut self, key: &K, weight: i64);
}

/// Distinct-count sketches over opaque byte keys.
pub trait CardinalityOps {
    fn ingest(&mut self, key: &[u8]);
    fn estimate(&self) -> f64;
}

/// Value-quantile sketches over f64 observations.
pub trait QuantileOps {
    fn update(&mut self, value: f64);
    fn quantile(&self, q: f64) -> f64;
}

/// In-place merge from another instance of the same sketch/config.
pub trait MergeOps {
    fn merge_from(&mut self, other: &Self);
}

// ---------------------------------------------------------------------------
// Tolerance specs
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
pub struct FrequencySpec {
    /// One-sided sketches (e.g. CountMin) must never underestimate.
    pub one_sided: bool,
    /// Relative tolerance applied to dense keys (hot-key accuracy).
    pub rel_tol: f64,
    /// Absolute floor added to tolerances for small counts.
    pub abs_tol: f64,
}

impl Default for FrequencySpec {
    fn default() -> Self {
        Self {
            one_sided: false,
            rel_tol: 0.05,
            abs_tol: 2.0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct CardinalitySpec {
    pub rel_tol: f64,
}

impl Default for CardinalitySpec {
    fn default() -> Self {
        Self { rel_tol: 0.03 }
    }
}

pub const DEFAULT_QUANTILE_QS: [f64; 5] = [0.1, 0.25, 0.5, 0.75, 0.9];

#[derive(Clone, Copy)]
pub struct QuantileSpec {
    /// Rank-tolerance band width for quantile checks.
    pub rank_tol: f64,
    pub qs: [f64; 5],
}

impl Default for QuantileSpec {
    fn default() -> Self {
        Self {
            rank_tol: 0.03,
            qs: DEFAULT_QUANTILE_QS,
        }
    }
}

// ---------------------------------------------------------------------------
// Batteries
// ---------------------------------------------------------------------------

/// Standard frequency battery: hot-key accuracy against exact truth.
///
/// - One-sided sketches must satisfy `est >= true` and stay within
///   `(1 + rel_tol)` of truth plus the absolute floor.
/// - Two-sided sketches must stay within `rel_tol * true + abs_tol`.
pub fn frequency_battery<S, F, K>(
    sketch: &str,
    new_sketch: F,
    stream: &[K],
    truth: &FreqTruth,
    spec: FrequencySpec,
) -> BatteryReport
where
    S: FrequencyOps<K>,
    F: Fn() -> S,
    K: Copy + Eq + Hash + Debug + From<i64>,
{
    let mut report = BatteryReport {
        sketch: sketch.to_string(),
        battery: "frequency",
        failures: vec![],
    };
    let mut sk = new_sketch();
    for k in stream {
        sk.ingest(k);
    }

    // Cold-key probe: an absent key must not carry meaningful mass. For
    // one-sided sketches (Count-Min family) absence is exact — the estimate
    // is structurally 0. Two-sided sketches may show signed noise, so they
    // are exempt here; their accuracy is covered by the dense-key band.
    if spec.one_sided {
        let absent = K::from(i64::MIN); // sentinel outside typical key domains
        let est_absent = sk.estimate(&absent);
        report.record(
            "absent key ~0",
            est_absent >= -spec.abs_tol && est_absent <= spec.abs_tol,
            format!(
                "absent key estimated {est_absent} (allowed |e| <= {})",
                spec.abs_tol
            ),
        );
    }

    for (k_int, count) in truth.pairs() {
        if count < 25 {
            continue; // only dense keys carry statistical meaning
        }
        let key = K::from(k_int);
        let est = sk.estimate(&key);
        if spec.one_sided {
            let hi = count as f64 * (1.0 + spec.rel_tol) + spec.abs_tol;
            report.record(
                "one-sided bound",
                est >= count as f64 && est <= hi,
                format!("key {k_int:?} true {count} est {est} (allowed [{count}, {hi:.1}])"),
            );
        } else {
            let lo = (count as f64) * (1.0 - spec.rel_tol) - spec.abs_tol;
            let hi = (count as f64) * (1.0 + spec.rel_tol) + spec.abs_tol;
            report.record(
                "two-sided bound",
                est >= lo && est <= hi,
                format!("key {k_int:?} true {count} est {est} (allowed [{lo:.1}, {hi:.1}])"),
            );
        }
    }
    report
}

/// Merge-equivalence battery: shard the stream, merge, and require the merged
/// sketch to agree with a single-pass sketch on all dense keys.
pub fn merge_equivalence_battery<S, F, K>(
    sketch: &str,
    new_sketch: F,
    stream: &[K],
    spec: FrequencySpec,
) -> BatteryReport
where
    S: FrequencyOps<K> + MergeOps,
    F: Fn() -> S,
    K: Copy + Eq + Hash + Debug,
{
    let mut report = BatteryReport {
        sketch: sketch.to_string(),
        battery: "merge-equivalence",
        failures: vec![],
    };
    let mut single = new_sketch();
    let mut left = new_sketch();
    let mut right = new_sketch();

    let mut truth_left: HashMap<K, i64> = HashMap::new();
    for (i, k) in stream.iter().enumerate() {
        single.ingest(k);
        if i % 2 == 0 {
            left.ingest(k);
            *truth_left.entry(*k).or_insert(0) += 1;
        } else {
            right.ingest(k);
        }
    }
    left.merge_from(&right);

    for (k, count) in truth_left {
        if count < 25 {
            continue;
        }
        let a = single.estimate(&k);
        let b = left.estimate(&k);
        let slack = spec.abs_tol + spec.rel_tol * count as f64;
        report.record(
            "merged matches single-pass",
            (a - b).abs() <= slack.max(2.0),
            format!("key {k:?}: single {a} merged {b}"),
        );
    }
    report
}

/// Turnstile battery: net-zero cancellation and weighted counting.
pub fn turnstile_battery<S, F, K>(sketch: &str, new_sketch: F, key: K) -> BatteryReport
where
    S: SignedFrequencyOps<K>,
    F: Fn() -> S,
    K: Debug,
{
    let mut report = BatteryReport {
        sketch: sketch.to_string(),
        battery: "turnstile",
        failures: vec![],
    };
    let mut sk = new_sketch();
    sk.ingest_weighted(&key, 500);
    sk.ingest_weighted(&key, -200);
    let est = sk.estimate(&key);
    report.record(
        "weighted net count",
        (est - 300.0).abs() <= 1.0,
        format!("key {key:?}: +500 then -200 estimated {est}, expected ~300"),
    );

    sk.ingest_weighted(&key, -300);
    let zeroed = sk.estimate(&key);
    report.record(
        "net-zero cancellation",
        zeroed.abs() <= 1e-6,
        format!("key {key:?}: fully cancelled estimate {zeroed}, expected 0"),
    );
    report
}

/// Cardinality battery: unique-stream checkpoints and duplicate-replay
/// invariance (re-inserting seen elements must not move the estimate).
pub fn cardinality_battery<S, F>(
    sketch: &str,
    new_sketch: F,
    unique_keys: &[u64],
    checkpoint: usize,
    spec: CardinalitySpec,
) -> BatteryReport
where
    S: CardinalityOps,
    F: Fn() -> S,
{
    let mut report = BatteryReport {
        sketch: sketch.to_string(),
        battery: "cardinality",
        failures: vec![],
    };
    let mut sk = new_sketch();
    for k in &unique_keys[..checkpoint] {
        sk.ingest(k.to_be_bytes().as_slice());
    }
    let t = checkpoint as f64;
    let est = sk.estimate();
    report.record(
        "unique-stream accuracy",
        est >= t * (1.0 - spec.rel_tol) && est <= t * (1.0 + spec.rel_tol),
        format!(
            "distinct {checkpoint}, estimated {est:.0} (±{:.0}%)",
            spec.rel_tol * 100.0
        ),
    );

    for k in &unique_keys[..checkpoint] {
        sk.ingest(k.to_be_bytes().as_slice());
    }
    let replay = sk.estimate();
    report.record(
        "duplicate-replay invariance",
        replay >= t * (1.0 - spec.rel_tol) && replay <= t * (1.0 + spec.rel_tol),
        format!("after replaying duplicates estimated {replay:.0}"),
    );
    report
}

/// Quantile battery: rank-band checks across the standard q grid.
pub fn quantile_battery<S, F>(
    sketch: &str,
    new_sketch: F,
    values: &[f64],
    spec: QuantileSpec,
) -> BatteryReport
where
    S: QuantileOps,
    F: Fn() -> S,
{
    let mut report = BatteryReport {
        sketch: sketch.to_string(),
        battery: "quantile",
        failures: vec![],
    };
    let truth = NumericTruth::new(values.to_vec());
    let mut sk = new_sketch();
    for v in values {
        sk.update(*v);
    }
    for &q in &spec.qs {
        let (lo, hi) = truth.quantile_band(q, spec.rank_tol);
        let est = sk.quantile(q);
        report.record(
            "rank band",
            est >= lo && est <= hi,
            format!("q={q} est {est:.3} outside [{lo:.3}, {hi:.3}]"),
        );
    }
    report
}
