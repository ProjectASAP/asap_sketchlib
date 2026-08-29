//! E2E quantile pipelines on synthetic numeric streams.
//!
//! The two quantile families here answer *different questions* and are held to
//! *different* error metrics:
//!
//! - **KLL** promises **rank** error: the returned value's rank must be within
//!   `eps(k)` of the requested `q`. Its value error is unbounded — on a
//!   heavy-tailed stream a correct KLL can return a value 100x off and still
//!   be within its guarantee — so checking `|est - true| / true` against a
//!   percentage tests nothing KLL claims.
//! - **DDSketch** promises **relative value** error: the returned value must
//!   be within `alpha` of the exact order statistic. Its rank error is
//!   unbounded, so a rank band tests nothing DDSketch claims.
//!
//! Neither may be run through the other's battery. `KllRankSpec` and
//! `RelativeQuantileSpec` in `common::specs` keep them apart.
//!
//! Every sketch here is built with an explicit compaction seed. KLL's coin is
//! the sketch's own randomness and is entirely separate from the stream seed;
//! the wall-clock-seeded `KLL::init_kll` / `KLLDynamic::init_kll` constructors
//! cannot be used in an accuracy test because a failure would not reproduce.
//!
//! # Trial units
//!
//! The KLL number `eps(k) = 2.446 / k^0.9433` is Apache DataSketches'
//! characterization fit to the 99th percentile of the **maximum** rank error
//! over a whole quantile grid — not a theorem about this implementation, and
//! not a per-`q` failure probability. So a KLL battery reduces each sketch to
//! one number (its worst rank error over the grid), each sketch gets its **own
//! compaction seed**, and the binomial is taken over those independent seeds.
//!
//! DDSketch's guarantee is deterministic — bucket width alone, no hash and no
//! sampling — so its batteries tolerate zero violations and no statistical
//! model applies. The two shipped implementations answer a quantile query with
//! **different order statistics**, so each is compared against the truth for
//! its own convention; see `DdRankConvention`.

mod common;

use common::specs::{DdRankConvention, KllRankSpec, RelativeQuantileSpec, Tally};
use common::{
    NumericTruth, assert_between, duplicate_heavy_f64, exponential_f64, log_uniform_f64,
    monotonic_f64, normal_f64, outside_in_ordering, uniform_u64, zipf_f64,
};

use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketch as PortableDds;
use asap_sketchlib::message_pack_format::portable::hydra_kll::HydraKllSketch;
use asap_sketchlib::{
    DDSketch, DataInput, KLL, KLLConfig, KLLDynamic, TumblingWindow, UnivMonQ, UnivMonQConfig,
};
use std::collections::HashMap;

// --------------------------------------------------------------------- KLL

/// The quantile grid every rank-error battery runs, including both endpoints
/// and the far tails the interior grid would otherwise never reach.
const RANK_QS: [f64; 7] = [0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0];

/// Fixed sketch (compaction-coin) seeds. Independent of the stream seeds
/// below: one controls which values arrive, the other controls which of them
/// survive compaction, and conflating the two makes a failure impossible to
/// localise.
const KLL_SKETCH_SEEDS: [u64; 4] = [0x5EED_0001, 0x5EED_0002, 0x5EED_0003, 0x5EED_0004];

/// A distinct compaction seed per trial index, so that no two sketches in a
/// battery share a coin sequence.
///
/// The rank-error batteries are binomials over these trials, which is only
/// valid if each trial is a fresh draw of KLL's randomness. Reusing one seed
/// across shapes or feed modes would silently correlate the outcomes; the
/// multiplier is a large odd constant so consecutive indices land far apart in
/// the seed space.
fn kll_trial_seed(trial: u64) -> u64 {
    0x5EED_0000_0000_0001u64.wrapping_add(trial.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Named stream shapes with a fixed seed each. Covers the light-tailed,
/// heavy-tailed, tie-dense, sorted and adversarially-ordered cases a
/// compaction scheme can behave differently on.
fn rank_streams(trial: usize, n: usize) -> Vec<(&'static str, Vec<f64>)> {
    let s = 0xA5A5_0000u64 + trial as u64 * 7919;
    vec![
        (
            "uniform",
            uniform_u64(n, 100_000_000, s)
                .into_iter()
                .map(|v| v as f64)
                .collect(),
        ),
        ("normal", normal_f64(n, 1_000.0, 250.0, s + 1)),
        ("zipf", zipf_f64(n, 8_192, 1.1, 1e6, 1e7, s + 2)),
        // Fifty distinct values over tens of thousands of observations: a
        // single value legitimately spans several percent of the rank space,
        // which is exactly where a value-error check would misfire and a
        // rank-interval check must not.
        ("duplicate-heavy", duplicate_heavy_f64(n, 50, s + 3)),
        // Sorted input: every compaction sees a run that is already in global
        // order.
        ("monotonic", monotonic_f64(n, 0.0, 1.0)),
        // Adversarial ordering: the same multiset emitted from both ends
        // inward, so no prefix resembles the whole.
        (
            "outside-in",
            outside_in_ordering(normal_f64(n, 5_000.0, 900.0, s + 4)),
        ),
    ]
}

/// How a sketch was fed. Every mode must satisfy the same rank contract —
/// bulk ingestion and merging are supposed to be equivalent to the loop, and
/// a mode that quietly loses weight shows up here as a rank-error failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Feed {
    SinglePass,
    BulkUpdate,
    ShardMerge,
    TumblingMerge,
}

const FEEDS: [Feed; 4] = [
    Feed::SinglePass,
    Feed::BulkUpdate,
    Feed::ShardMerge,
    Feed::TumblingMerge,
];

fn feed_kll(feed: Feed, k: i32, seed: u64, values: &[f64]) -> KLL<f64> {
    match feed {
        Feed::SinglePass => {
            let mut s = KLL::init_kll_with_seed(k, seed);
            for v in values {
                s.update(v);
            }
            s
        }
        Feed::BulkUpdate => {
            let mut s = KLL::init_kll_with_seed(k, seed);
            s.bulk_update(values);
            s
        }
        Feed::ShardMerge => {
            // Four shards, each with its own coin seed, merged pairwise into a
            // tree rather than a chain so merge order is exercised too.
            let mut shards: Vec<KLL<f64>> = (0..4)
                .map(|i| KLL::init_kll_with_seed(k, seed.wrapping_add(0x1000 * (i + 1))))
                .collect();
            for (i, v) in values.iter().enumerate() {
                shards[i % 4].update(v);
            }
            let (mut a, b) = (shards.remove(0), shards.remove(0));
            let (mut c, d) = (shards.remove(0), shards.remove(0));
            a.merge(&b);
            c.merge(&d);
            a.merge(&c);
            a
        }
        Feed::TumblingMerge => {
            // Windows are closed and recycled through the pool; `query_all`
            // merges every retained window plus the active one. Pool reuse
            // runs `clear()`, which re-seeds from the stored seed, so the
            // result is reproducible across rotations.
            let cfg = KLLConfig {
                k: k as usize,
                m: 8,
                seed: Some(seed),
            };
            let window = (values.len() / 8).max(1) as u64;
            let mut tw: TumblingWindow<KLL> = TumblingWindow::new(window, 32, cfg, 4);
            for (t, v) in values.iter().enumerate() {
                tw.insert(t as u64, &DataInput::F64(*v), 0);
            }
            tw.query_all()
        }
    }
}

fn feed_kll_dynamic(feed: Feed, k: i32, seed: u64, values: &[f64]) -> Option<KLLDynamic<f64>> {
    match feed {
        Feed::SinglePass => {
            let mut s = KLLDynamic::init_kll_with_seed(k, seed);
            for v in values {
                s.update(v);
            }
            Some(s)
        }
        Feed::BulkUpdate => {
            let mut s = KLLDynamic::init_kll_with_seed(k, seed);
            s.bulk_update(values);
            Some(s)
        }
        Feed::ShardMerge => {
            let mut shards: Vec<KLLDynamic<f64>> = (0..4)
                .map(|i| KLLDynamic::init_kll_with_seed(k, seed.wrapping_add(0x1000 * (i + 1))))
                .collect();
            for (i, v) in values.iter().enumerate() {
                shards[i % 4].update(v);
            }
            let (mut a, b) = (shards.remove(0), shards.remove(0));
            let (mut c, d) = (shards.remove(0), shards.remove(0));
            a.merge(&b);
            c.merge(&d);
            a.merge(&c);
            Some(a)
        }
        // KLLDynamic is not a `TumblingWindowSketch`; the framework hosts the
        // fixed-layout `KLL` only. Nothing to cover.
        Feed::TumblingMerge => None,
    }
}

/// Both KLL implementations against the Apache DataSketches maximum-rank-error
/// characterization `eps(k) = 2.446 / k^0.9433`, over a grid of
/// `(k, distribution, feed mode)` with an independent compaction seed each.
///
/// # Why the acceptance rule looks like this
///
/// The constant is a least-squares fit to the 99th percentile of the
/// **maximum** rank error DataSketches' characterization runs observed across a
/// whole quantile grid. Two things follow.
///
/// First, the quantity it bounds is a per-sketch maximum, so the seven `q`
/// values of one sketch are *one* outcome, not seven Bernoulli draws: they
/// share a single compaction history and are strongly dependent (a compaction
/// that displaces the median displaces its neighbours too).
///
/// Second, the 1% is that single outcome's failure probability, so the only
/// legitimate battery is over **independent compaction seeds**. Every trial
/// below therefore gets its own seed, derived from its index, and the binomial
/// acceptance rule runs over trials.
///
/// This is a characterization target imported from another implementation of
/// the same compact KLL layout — not a theorem proved about this code — and the
/// test name says `characterization`, not `theorem`.
#[test]
fn kll_family_stays_within_the_datasketches_maximum_rank_error_characterization() {
    const N: usize = 30_000;
    const KS: [i32; 3] = [64, 200, 800];
    const REPEATS: usize = 4;

    let mut tallies: HashMap<String, Tally> = HashMap::new();
    let mut trial = 0u64;
    for repeat in 0..REPEATS {
        for (shape, values) in rank_streams(repeat, N) {
            let truth = NumericTruth::new(values.clone());
            for &k in &KS {
                let spec = KllRankSpec::datasketches(k as usize);
                for &feed in &FEEDS {
                    // One fresh compaction seed per trial: that is what makes
                    // the trials independent draws of KLL's own randomness.
                    let seed = kll_trial_seed(trial);
                    trial += 1;
                    let fixed = feed_kll(feed, k, seed, &values);
                    spec.record_trial(
                        tallies.entry(format!("KLL/{feed:?}")).or_default(),
                        &format!("k={k} shape={shape} seed={seed:#x} n={N}"),
                        truth.sorted(),
                        &RANK_QS,
                        |q| fixed.quantile(q),
                    );

                    let seed = kll_trial_seed(trial);
                    trial += 1;
                    if let Some(dynamic) = feed_kll_dynamic(feed, k, seed, &values) {
                        spec.record_trial(
                            tallies.entry(format!("KLLDynamic/{feed:?}")).or_default(),
                            &format!("k={k} shape={shape} seed={seed:#x} n={N}"),
                            truth.sorted(),
                            &RANK_QS,
                            |q| dynamic.quantile(q),
                        );
                    }
                }
            }
        }
    }

    let mut labels: Vec<String> = tallies.keys().cloned().collect();
    labels.sort();
    for label in labels {
        let tally = tallies.remove(&label).expect("label just enumerated");
        tally.assert_independent_binomial(
            &format!("{label} / maximum normalized rank error per sketch"),
            KllRankSpec::datasketches(200).trial_failure_probability,
            &format!(
                "one trial = one sketch with its own compaction seed, scored on its \
                 worst rank error over the whole q grid. n={N}, k in {KS:?}, \
                 seeds kll_trial_seed(0..), stream shapes {:?}, q grid {RANK_QS:?}",
                rank_streams(0, 1)
                    .iter()
                    .map(|(s, _)| *s)
                    .collect::<Vec<_>>()
            ),
        );
    }
}

/// Rank error must shrink as `k` grows, at the rate the characterization
/// states. A hard-coded tolerance cannot see this: it passes identically at
/// `k = 64` and `k = 800`, so it would not notice a `k` that stopped being
/// wired through to the compactor capacities at all.
///
/// This is a **structural** claim about the `k` -> accuracy wiring, checked on
/// four fixed compaction seeds per `k`. The absolute check that each `k`'s
/// worst-of-four stays inside `eps(k)` is a deterministic regression pin, not a
/// tail test: with only four trials it has no power to measure a 1% per-trial
/// failure rate, and the batteries above are where that is judged.
#[test]
fn kll_rank_error_shrinks_with_k_as_the_characterization_predicts() {
    const N: usize = 100_000;
    let values: Vec<f64> = uniform_u64(N, 100_000_000, 0xC0FF_EE01)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    let truth = NumericTruth::new(values.clone());
    let qs: Vec<f64> = (1..20).map(|i| i as f64 / 20.0).collect();

    let mut worst: Vec<(i32, f64)> = Vec::new();
    for k in [64i32, 256, 1024] {
        let mut w: f64 = 0.0;
        for &seed in &KLL_SKETCH_SEEDS {
            let mut s = KLL::init_kll_with_seed(k, seed);
            for v in &values {
                s.update(v);
            }
            for &q in &qs {
                let v = s.quantile(q);
                let (excl, incl) = truth.rank_interval(v);
                let err = if q < excl {
                    excl - q
                } else if q > incl {
                    q - incl
                } else {
                    0.0
                };
                w = w.max(err);
            }
        }
        worst.push((k, w));
    }

    for (k, w) in &worst {
        let eps = KllRankSpec::datasketches(*k as usize).epsilon();
        assert!(
            *w <= eps,
            "KLL k={k}: worst rank error {w:.5} exceeds eps(k)={eps:.5} \
             (n={N}, sketch seeds {KLL_SKETCH_SEEDS:02x?}, stream seed 0xC0FFEE01)"
        );
    }
    // 16x more capacity must buy at least a 4x tighter rank error; the
    // characterization predicts 16^0.9433 = 13.4x.
    let (k_lo, w_lo) = worst[0];
    let (k_hi, w_hi) = worst[2];
    assert!(
        w_lo >= w_hi * 4.0,
        "raising k from {k_lo} to {k_hi} moved the worst rank error only from \
         {w_lo:.5} to {w_hi:.5}; the characterization predicts a {:.1}x improvement, so \
         k is not reaching the compactor capacities",
        (k_hi as f64 / k_lo as f64).powf(0.9433)
    );
}

// ---------------------------------------------------------------- DDSketch

/// Accuracy parameters spanning three orders of magnitude. The tightest is
/// where floating-point round-off in the logarithmic mapping is most likely
/// to matter, the loosest is where the bucket span per key is widest.
const DDS_ALPHAS: [f64; 4] = [0.001, 0.01, 0.05, 0.1];

/// Same q grid as the rank battery, so the two contracts are compared on the
/// same questions rather than on grids chosen to flatter each sketch.
const DDS_QS: [f64; 7] = [0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0];

/// Streams for the relative-error battery. `adversarial` places a fifth of its
/// mass exactly on bucket lower edges, where the mapping's error is at its
/// maximum of exactly `alpha` and one ULP of drift decides the bucket.
fn dds_streams(alpha: f64, n: usize, seed: u64) -> Vec<(&'static str, Vec<f64>)> {
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    vec![
        (
            "adversarial-bucket-edges",
            log_uniform_f64(n, gamma, 5..40, seed),
        ),
        (
            "normal",
            normal_f64(n, 1_000.0, 250.0, seed + 1)
                .into_iter()
                .filter(|v| *v > 0.0)
                .collect(),
        ),
        ("exponential", exponential_f64(n, 1e-3, seed + 2)),
        (
            "uniform",
            uniform_u64(n, 9_000_000, seed + 3)
                .into_iter()
                .map(|v| 1_000_000.0 + v as f64)
                .collect(),
        ),
        ("zipf", zipf_f64(n, 8_192, 1.1, 1e6, 1e7, seed + 4)),
        // Nine decades in one stream: the bucket store spans a large index
        // range and the mapping is exercised far from 1.0 in both directions.
        (
            "wide-dynamic-range",
            uniform_u64(n, 1_000_000, seed + 5)
                .into_iter()
                .enumerate()
                .map(|(i, v)| 10f64.powi((i % 10) as i32 - 4) * (1.0 + v as f64 / 1_000_000.0))
                .collect(),
        ),
    ]
}

/// DDSketch's relative-value-error guarantee, for the core sketch and the
/// portable wire twin, at every supported alpha.
///
/// Each implementation is compared against the exact order statistic **of its
/// own rank convention**: `DDSketch::get_value_at_quantile` answers
/// `sorted[ceil(q*n) - 1]`, while the portable `DdSketch::quantile` answers
/// `sorted[floor(q*(n-1))]`. They are different questions (see
/// `ddsketch_core_and_portable_answer_different_order_statistics`), so a single
/// truth helper would score one of them against the other one's question and
/// quietly absorb the difference into `alpha`.
///
/// The tolerance is `alpha + numerical_slack`, where the slack is a few ULP of
/// the logarithmic mapping — never a percentage of alpha, which would license
/// breaking the advertised guarantee by that percentage.
#[test]
fn ddsketch_core_and_portable_satisfy_the_relative_value_error_contract() {
    const SAMPLE_SIZES: [usize; 3] = [1_000, 20_000, 100_000];

    for &alpha in &DDS_ALPHAS {
        let core_spec = RelativeQuantileSpec::core(alpha);
        let port_spec = RelativeQuantileSpec::portable(alpha);
        let mut core_tally = Tally::default();
        let mut port_tally = Tally::default();
        for (i, &n) in SAMPLE_SIZES.iter().enumerate() {
            let seed = 3_005_000u64 + i as u64 * 101 + (alpha * 1e6) as u64;
            for (label, values) in dds_streams(alpha, n, seed) {
                let mut core = DDSketch::new(alpha);
                let mut port = PortableDds::new(alpha);
                for v in &values {
                    core.add(v);
                    port.update(*v);
                }
                let truth = NumericTruth::new(values.clone());
                assert_eq!(
                    core.get_count() as usize,
                    truth.len(),
                    "{label} alpha={alpha} n={n} seed={seed}: core dropped samples"
                );
                assert_eq!(
                    port.total_count() as usize,
                    truth.len(),
                    "{label} alpha={alpha} n={n} seed={seed}: portable dropped samples"
                );
                core_spec.tally_into(&mut core_tally, truth.sorted(), &DDS_QS, |q| {
                    core.get_value_at_quantile(q)
                });
                port_spec.tally_into(&mut port_tally, truth.sorted(), &DDS_QS, |q| {
                    port.quantile(q)
                });
            }
        }
        // The guarantee is deterministic, not probabilistic: DDSketch's error
        // comes from bucket width alone, with no hash and no sampling, so a
        // single violation is a defect and none are tolerated.
        let context = format!(
            "alpha={alpha} sizes={SAMPLE_SIZES:?} shapes={:?} q grid {DDS_QS:?}",
            dds_streams(alpha, 1, 0)
                .iter()
                .map(|(s, _)| *s)
                .collect::<Vec<_>>()
        );
        core_tally.assert_none(&format!("core DDSketch alpha={alpha}"), &context);
        port_tally.assert_none(&format!("portable DdSketch alpha={alpha}"), &context);
    }
}

/// The two implementations answer a quantile query with **different order
/// statistics**, and this pins the divergence rather than letting `alpha`
/// absorb it.
///
/// - `DDSketch::get_value_at_quantile` uses `rank = ceil(q * n)`, 1-based.
/// - Portable `DdSketch::quantile` uses `target = floor(q * (n - 1))`, 0-based
///   — the lower-quantile convention of the DDSketch paper and of DataDog's
///   reference implementation, which is also what the wire format's Go twin
///   answers.
///
/// **The decision taken here is to keep both.** The portable type exists to be
/// byte- and answer-compatible with `sketchlib-go`, so its convention is fixed
/// by an external contract; the core type's `ceil` convention is what its own
/// callers have been reading for the life of the API, and it is what lets `q=0`
/// and `q=1` return the exactly retained minimum and maximum. Changing either
/// silently moves numbers under existing callers, and the divergence is only
/// observable at small `n` or ragged `q` — precisely the cases pinned below.
/// What is *not* acceptable is leaving it undocumented, or scoring both against
/// one truth helper, which is what the previous revision did.
///
/// The probes are chosen so the two formulas disagree: at `n = 3, q = 0.4` the
/// core answers `sorted[1]` and the portable `sorted[0]`.
#[test]
fn ddsketch_core_and_portable_answer_different_order_statistics() {
    // (n, q, expected core 0-based index, expected portable 0-based index)
    const PROBES: [(usize, f64, usize, usize); 8] = [
        (3, 0.4, 1, 0),
        (4, 0.34, 1, 1),
        (4, 0.3, 1, 0),
        (7, 0.2, 1, 1),
        (7, 0.6, 4, 3),
        (5, 0.5, 2, 2),
        (10, 0.25, 2, 2),
        (10, 0.15, 1, 1),
    ];

    // Values one bucket apart at the coarsest alpha, so a one-rank difference
    // is a different bucket and therefore a different answer — not two ranks
    // that happen to share a representative.
    const ALPHA: f64 = 0.01;
    let gamma = (1.0 + ALPHA) / (1.0 - ALPHA);

    let mut disagreements = 0usize;
    for &(n, q, core_idx, port_idx) in &PROBES {
        let values: Vec<f64> = (0..n).map(|i| 100.0 * gamma.powi(3 * i as i32)).collect();
        let sorted = values.clone();

        assert_eq!(
            DdRankConvention::CeilNearestRank.index(n, q),
            core_idx,
            "core convention ceil(q*n)-1 at n={n} q={q}"
        );
        assert_eq!(
            DdRankConvention::LowerFloor.index(n, q),
            port_idx,
            "portable convention floor(q*(n-1)) at n={n} q={q}"
        );

        let mut core = DDSketch::new(ALPHA);
        let mut port = PortableDds::new(ALPHA);
        for v in &values {
            core.add(v);
            port.update(*v);
        }

        let core_spec = RelativeQuantileSpec::core(ALPHA);
        let port_spec = RelativeQuantileSpec::portable(ALPHA);
        let core_est = core.get_value_at_quantile(q).expect("non-empty");
        let port_est = port.quantile(q).expect("non-empty");

        if let Err(detail) = core_spec.check(q, core_est, sorted[core_idx]) {
            panic!("core DDSketch n={n} q={q}: {detail}");
        }
        if let Err(detail) = port_spec.check(q, port_est, sorted[port_idx]) {
            panic!("portable DdSketch n={n} q={q}: {detail}");
        }

        if core_idx != port_idx {
            disagreements += 1;
            // Each implementation must be answering *its own* order statistic,
            // so the two answers must differ here: if they agreed, one of them
            // would have silently changed convention.
            assert!(
                core_est != port_est,
                "n={n} q={q}: the conventions pick different order statistics \
                 ({core_idx} vs {port_idx}) but both returned {core_est}"
            );
        }
    }
    assert!(
        disagreements >= 3,
        "the probe set must contain cases where the two conventions genuinely \
         disagree; only {disagreements} did"
    );
}

/// Endpoint behaviour, which is **not** the same on the two implementations.
///
/// - The core sketch tracks the exact minimum and maximum beside the bucket
///   store, and `get_value_at_quantile` short-circuits `q <= 0` and `q >= 1` to
///   them. Its endpoints are therefore *exact*, with zero error.
/// - The portable sketch carries no min/max scalars at all — they were removed
///   from the wire — so its endpoints are ordinary bucket representatives and
///   are only guaranteed within `alpha`. The previous revision of this test
///   claimed it "clamps its bucket representative into [min, max], so its
///   endpoints are exact too", which is not what the code does.
#[test]
fn ddsketch_core_endpoints_are_exact_and_portable_endpoints_are_alpha_relative() {
    for &alpha in &DDS_ALPHAS {
        let values = log_uniform_f64(5_000, (1.0 + alpha) / (1.0 - alpha), 3..30, 4_242);
        let truth = NumericTruth::new(values.clone());
        let mut core = DDSketch::new(alpha);
        let mut port = PortableDds::new(alpha);
        for v in &values {
            core.add(v);
            port.update(*v);
        }
        assert_eq!(
            core.get_value_at_quantile(0.0),
            Some(truth.min()),
            "core alpha={alpha}: q=0 must be the exact minimum"
        );
        assert_eq!(
            core.get_value_at_quantile(1.0),
            Some(truth.max()),
            "core alpha={alpha}: q=1 must be the exact maximum"
        );
        assert_eq!(
            core.min(),
            Some(truth.min()),
            "core alpha={alpha}: min() must be exact"
        );
        assert_eq!(
            core.max(),
            Some(truth.max()),
            "core alpha={alpha}: max() must be exact"
        );

        // The portable twin holds only buckets, so its endpoints get the same
        // relative-value guarantee as any other quantile and nothing more.
        let spec = RelativeQuantileSpec::portable(alpha);
        let (p0, p1) = (port.quantile(0.0).unwrap(), port.quantile(1.0).unwrap());
        if let Err(detail) = spec.check(0.0, p0, truth.min()) {
            panic!("portable alpha={alpha} at q=0: {detail}");
        }
        if let Err(detail) = spec.check(1.0, p1, truth.max()) {
            panic!("portable alpha={alpha} at q=1: {detail}");
        }
    }
}

/// Values placed deliberately on and around a bucket boundary. Whichever side
/// of the edge the mapping lands on, the returned representative must stay
/// within `alpha` — a value at the lower edge is over-estimated by exactly
/// `alpha`, and one that slips into the bucket below is under-estimated by
/// exactly `alpha`, so both sides are tight and neither may exceed it.
#[test]
fn ddsketch_satisfies_the_relative_error_contract_at_bucket_boundaries() {
    for &alpha in &DDS_ALPHAS {
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        // A one-sample sketch answers every q with that one sample, so the two
        // rank conventions coincide here by construction (both index 0) and the
        // probe isolates the mapping from any rank effect.
        let core_spec = RelativeQuantileSpec::core(alpha);
        let port_spec = RelativeQuantileSpec::portable(alpha);
        for k in [-40i32, -7, 0, 1, 13, 60, 200] {
            let edge = gamma.powi(k);
            if edge <= 0.0 || !edge.is_finite() {
                continue;
            }
            let probes = [
                edge,                         // exactly the lower edge
                edge * (1.0 - f64::EPSILON),  // one ULP below it
                edge * (1.0 + f64::EPSILON),  // one ULP above it
                edge * gamma.sqrt(),          // bucket interior
                edge * gamma * (1.0 - 1e-12), // just under the upper edge
            ];
            for probe in probes {
                let mut core = DDSketch::new(alpha);
                let mut port = PortableDds::new(alpha);
                core.add(&probe);
                port.update(probe);
                if core.get_count() == 0 {
                    continue; // outside the indexable range at this alpha
                }
                // A single-sample sketch answers every q with that sample's
                // bucket, so this isolates the mapping from any rank effect.
                let est = core.get_value_at_quantile(0.5).unwrap();
                if let Err(detail) = core_spec.check(0.5, est, probe) {
                    panic!("core DDSketch alpha={alpha} gamma^{k} boundary probe: {detail}");
                }
                let pest = port.quantile(0.5).unwrap();
                if let Err(detail) = port_spec.check(0.5, pest, probe) {
                    panic!("portable DdSketch alpha={alpha} gamma^{k} boundary probe: {detail}");
                }
            }
        }
    }
}

/// Merging shards and replaying promotion deltas must both leave the relative
/// error contract intact, because both write into the same bucket store the
/// guarantee is stated over.
#[test]
fn ddsketch_merge_and_delta_replay_preserve_the_relative_error_contract() {
    use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketchDelta;
    use asap_sketchlib::octo_delta::DdDelta;

    const N: usize = 40_000;
    for &alpha in &DDS_ALPHAS {
        let spec = RelativeQuantileSpec::core(alpha);
        let port_spec = RelativeQuantileSpec::portable(alpha);
        let values = zipf_f64(N, 8_192, 1.1, 1e3, 1e7, 7_654_321 + (alpha * 1e5) as u64);
        let truth = NumericTruth::new(values.clone());

        // Four-way shard merge, core side.
        let mut shards: Vec<DDSketch> = (0..4).map(|_| DDSketch::new(alpha)).collect();
        for (i, v) in values.iter().enumerate() {
            shards[i % 4].add(v);
        }
        let mut merged = shards.remove(0);
        for s in &shards {
            merged.merge(s).expect("same-alpha merge");
        }
        assert_eq!(
            merged.get_count() as usize,
            N,
            "alpha={alpha}: merge must preserve total count"
        );
        let mut merge_tally = Tally::default();
        spec.tally_into(&mut merge_tally, truth.sorted(), &DDS_QS, |q| {
            merged.get_value_at_quantile(q)
        });
        merge_tally.assert_none(
            &format!("core DDSketch alpha={alpha} after a 4-way shard merge"),
            &format!("zipf n={N} over [1e3, 1e7]"),
        );

        // Delta replay, core side: rebuild a sketch from bucket deltas only
        // and require the same contract of it. The replayed sketch's own
        // min/max come from bucket representatives, so its endpoints are
        // within alpha rather than exact — the interior q grid is what the
        // contract covers here.
        let single = {
            let mut s = DDSketch::new(alpha);
            for v in &values {
                s.add(v);
            }
            s
        };
        let mut replayed = DDSketch::new(alpha);
        for (i, &count) in single.store_counts().iter().enumerate() {
            if count > 0 {
                replayed.apply_delta(DdDelta {
                    index: single.store_offset() + i as i32,
                    value: count,
                });
            }
        }
        assert_eq!(
            replayed.get_count(),
            single.get_count(),
            "alpha={alpha}: delta replay must reproduce the total count"
        );
        let mut replay_tally = Tally::default();
        spec.tally_into(&mut replay_tally, truth.sorted(), &DDS_QS[1..6], |q| {
            replayed.get_value_at_quantile(q)
        });
        replay_tally.assert_none(
            &format!("core DDSketch alpha={alpha} rebuilt from bucket deltas"),
            &format!(
                "zipf n={N} over [1e3, 1e7], interior q grid {:?}",
                &DDS_QS[1..6]
            ),
        );

        // Portable side: merge, then a delta carrying the same buckets.
        let mut pa = PortableDds::new(alpha);
        let mut pb = PortableDds::new(alpha);
        for (i, v) in values.iter().enumerate() {
            if i % 2 == 0 {
                pa.update(*v);
            } else {
                pb.update(*v);
            }
        }
        pa.merge(&pb).expect("same-alpha portable merge");
        let mut port_tally = Tally::default();
        port_spec.tally_into(&mut port_tally, truth.sorted(), &DDS_QS, |q| pa.quantile(q));
        port_tally.assert_none(
            &format!("portable DdSketch alpha={alpha} after merge"),
            &format!("zipf n={N} over [1e3, 1e7]"),
        );

        let mut pr = PortableDds::new(alpha);
        let delta = DdSketchDelta {
            buckets: pa
                .store_counts
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .map(|(i, c)| (pa.store_offset + i as i32, *c))
                .collect(),
            d_count: pa.total_count() as i64,
            ..DdSketchDelta::default()
        };
        pr.apply_delta(&delta).expect("benign delta");
        let mut pr_tally = Tally::default();
        port_spec.tally_into(&mut pr_tally, truth.sorted(), &DDS_QS[1..6], |q| {
            pr.quantile(q)
        });
        pr_tally.assert_none(
            &format!("portable DdSketch alpha={alpha} rebuilt from a delta"),
            &format!(
                "zipf n={N} over [1e3, 1e7], interior q grid {:?}",
                &DDS_QS[1..6]
            ),
        );
    }
}

// ---------------------------------------------------------------- UnivMonQ

/// Exact aggregates are exact: `count`, `min` and `max` are maintained
/// directly, not estimated, so no band applies to them at all.
#[test]
fn univmonq_count_min_and_max_are_exact() {
    let mut q = UnivMonQ::new(Default::default()).expect("default config valid");
    let values: Vec<f64> = uniform_u64(20_000, 900, 3008)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    for v in &values {
        q.update(v);
    }
    let truth = NumericTruth::new(values.clone());
    assert_eq!(q.count() as usize, values.len(), "count must be exact");
    assert_eq!(q.min(), Some(truth.min()), "min must be exact");
    assert_eq!(q.max(), Some(truth.max()), "max must be exact");
}

/// UnivMon-Q's bottom layer sees every update, so a point frequency query is
/// a Count Sketch query at `(depth, width)` and carries Count Sketch's L2
/// bound — not a percentage. Its `F2` estimate is the AMS row-sum estimator
/// over the same matrix and carries `sqrt(2*kappa/w)`.
#[test]
fn univmonq_frequency_and_f2_satisfy_the_count_sketch_bounds() {
    use common::FreqTruth;
    use common::specs::{CountSketchSpec, SecondMomentSpec};

    let config = UnivMonQConfig::default();
    let mut q = UnivMonQ::new(config).expect("default config valid");
    let mut truth = FreqTruth::default();

    // Well-separated heavy hitters plus a uniform background.
    for i in 0..40u64 {
        for _ in 0..(i + 1) * 60 {
            q.update(&(i as f64));
            truth.observe(i as i64);
        }
    }
    for k in uniform_u64(20_000, 900, 3008) {
        let key = 100u64 + k % 800;
        q.update(&(key as f64));
        truth.observe(key as i64);
    }

    let context = format!(
        "UnivMonQConfig {{ levels: {}, width: {}, depth: {}, hash_seed: {} }}, \
         40 separated heavy keys + uniform background (stream seed 3008)",
        config.levels, config.width, config.depth, config.hash_seed
    );
    CountSketchSpec::new(config.depth, config.width).assert_contract(
        "UnivMonQ::estimate_frequency (bottom layer Count Sketch)",
        &truth,
        |k| q.estimate_frequency(k as f64) as f64,
        &context,
    );

    let f2_spec = SecondMomentSpec::new(config.depth, config.width);
    if let Err(detail) = f2_spec.check(q.estimate_f2(), truth.f2()) {
        panic!("UnivMonQ::estimate_f2: {detail}\n  context: {context}");
    }
}

/// Ordered queries against the **full** documented bound.
///
/// The contract in `docs/api/api_univmon_q.md` is
///
/// ```text
///   sup_x |F_hat(x) - F(x)| <= 2 E_H + P_hat_R * epsilon_R
/// ```
///
/// where `E_H` is the normalized frequency error over the sketch's internally
/// recovered heavy set, `P_hat_R` is the residual's share of the mass, and
/// `epsilon_R = sqrt(ln(2/delta) / (2 m_R))` is the DKW band over the `m_R`
/// retained occurrence samples backing the residual.
///
/// `E_H` and `m_R` used to be unreachable from outside the crate, so an earlier
/// revision of this test could only cover the diffuse special case where the
/// gate provably does not fire and the heavy set is empty. They are now
/// reported by `UnivMonQQuery::ordered_query_diagnostics`, a read-only view of
/// state the CDF construction already computes, so both regimes are covered
/// here:
///
/// - **diffuse** — 200k observations over 200k distinct values. The adaptive
///   gate `F2_hat / N^2 >= 1 / ordered_samples` does not fire, so the heavy set
///   really is empty; the test asserts that premise from the diagnostics rather
///   than assuming it, and the bound collapses to `epsilon_R`.
/// - **concentrated** — a Zipf stream whose head is heavy enough to fire the
///   gate, so `E_H > 0` and `P_hat_R < 1` and the full three-term bound is what
///   is evaluated.
///
/// # Trial unit
///
/// The bound is a **supremum over x**, so one sketch answers it with one
/// pass/fail: every breakpoint and every probed quantile of a single sketch is
/// part of that one statement, not a separate Bernoulli draw. Independent
/// trials come from the two places UnivMon-Q's randomness actually lives — the
/// occurrence-priority stream, keyed by `source_id`, and the CountSketch hash,
/// keyed by `config.hash_seed`. Both are varied per trial.
#[test]
fn univmonq_ordered_queries_satisfy_the_full_documented_bound() {
    use common::specs::occurrence_sample_epsilon;

    const DELTA: f64 = 0.01;
    const TRIALS: usize = 12;
    const QS: [f64; 7] = [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99];

    // Two regimes: one where the adaptive gate cannot fire, and one where it
    // must. Covering only the first is what left `E_H` untested.
    for (regime, values) in [
        (
            "diffuse",
            uniform_u64(200_000, 10_000_000, 0x0DDE_0001)
                .into_iter()
                .map(|v| v as f64)
                .collect::<Vec<f64>>(),
        ),
        (
            "concentrated",
            zipf_f64(200_000, 4_096, 1.4, 1.0, 1e6, 0x0DDE_0002),
        ),
    ] {
        let truth = NumericTruth::new(values.clone());
        let mut exact_counts: HashMap<u64, u64> = HashMap::new();
        for v in &values {
            *exact_counts.entry(v.to_bits()).or_default() += 1;
        }

        let mut tally = Tally::default();
        let mut gate_fired = 0usize;
        let mut heavy_seen = 0usize;
        let mut last_context = String::new();

        for t in 0..TRIALS {
            let config = UnivMonQConfig {
                // Both knobs move the randomness the bound is stated over:
                // `hash_seed` re-draws the CountSketch hash, `source_id`
                // re-draws the occurrence priorities.
                hash_seed: 3 + t,
                ..UnivMonQConfig::default()
            };
            let mut q: UnivMonQ =
                UnivMonQ::with_hasher_and_source_id(config, 0x0DDE_1000 + t as u64)
                    .expect("config valid");
            for v in &values {
                q.update(v);
            }

            let n = q.count() as f64;
            let view = q.prepare_queries();
            let diag = view.ordered_query_diagnostics();
            let cdf = view.cdf();

            // The gate, read from public state, and the heavy set the CDF
            // actually used, read from the diagnostics. They must agree: a
            // non-empty heavy set with the gate closed would mean the two
            // disagree about which regime the sketch is in.
            let gate = view.estimate_f2() / (n * n);
            let threshold = 1.0 / config.ordered_samples as f64;
            if gate >= threshold {
                gate_fired += 1;
            } else {
                assert!(
                    diag.heavy.is_empty(),
                    "{regime} trial {t}: the adaptive gate is closed \
                     (F2_hat/N^2 = {gate:.3e} < 1/ordered_samples = {threshold:.3e}) but the \
                     CDF still used {} heavy values",
                    diag.heavy.len()
                );
            }
            if !diag.heavy.is_empty() {
                heavy_seen += 1;
            }

            // E_H: the largest normalized frequency error over the recovered
            // heavy set, against exact truth. Empty heavy set gives zero, which
            // is what makes the diffuse regime collapse to epsilon_R.
            let e_h = diag
                .heavy
                .iter()
                .map(|(value, frequency)| {
                    let exact = *exact_counts.get(&value.to_bits()).unwrap_or(&0) as f64;
                    (frequency - exact).abs() / n
                })
                .fold(0.0f64, f64::max);
            let p_hat_r = diag.residual_mass_fraction(q.count());
            let m_r = diag.residual_samples;
            let eps_r = if m_r == 0 {
                0.0
            } else {
                occurrence_sample_epsilon(m_r, DELTA)
            };
            let bound = 2.0 * e_h + p_hat_r * eps_r;

            let context = format!(
                "{regime} trial {t}: hash_seed={} source_id={:#x}, n={n}, gate \
                 F2_hat/N^2={gate:.3e} vs 1/ordered_samples={threshold:.3e}, heavy set \
                 {} values, E_H={e_h:.6}, P_hat_R={p_hat_r:.6}, m_R={m_r}, \
                 epsilon_R={eps_r:.6} at delta={DELTA} -> bound {bound:.6}",
                config.hash_seed,
                0x0DDE_1000 + t as u64,
                diag.heavy.len(),
            );
            last_context = context.clone();

            assert!(
                m_r > 0 || !diag.heavy.is_empty(),
                "{context}: the CDF was built from neither heavy values nor occurrence \
                 samples, so there is nothing for the bound to cover"
            );

            // The bound is a sup over x, so a single trial is a single
            // pass/fail over every breakpoint *and* every probed quantile.
            let mut worst = 0.0f64;
            let mut detail = String::new();
            for point in cdf {
                let (excl, incl) = truth.rank_interval(point.value);
                let dist = (point.rank - incl).max(excl - point.rank).max(0.0);
                if dist >= worst {
                    worst = dist;
                    detail = format!(
                        "cdf breakpoint value {} reported rank {:.6}, true rank interval \
                         [{excl:.6}, {incl:.6}]",
                        point.value, point.rank
                    );
                }
            }
            for &qq in &QS {
                let est = view.quantile(qq).expect("ordered_samples enabled");
                let (excl, incl) = truth.rank_interval(est);
                let dist = (qq - incl).max(excl - qq).max(0.0);
                if dist >= worst {
                    worst = dist;
                    detail = format!(
                        "quantile({qq}) returned {est}, whose true rank interval is \
                         [{excl:.6}, {incl:.6}]"
                    );
                }
            }
            tally.record(worst <= bound, || {
                format!("{context}\n      sup deviation {worst:.6} > bound; worst at {detail}")
            });

            // Monotonicity is structural: a CDF that decreases is malformed
            // whatever the sampling error.
            assert!(!cdf.is_empty(), "{context}: cdf must not be empty");
            for w in cdf.windows(2) {
                assert!(
                    w[0].rank <= w[1].rank + 1e-9,
                    "cdf ranks not monotone at {:?} -> {:?}. {context}",
                    w[0],
                    w[1]
                );
            }

            // `rank` and `quantile` must be a consistent pair: re-ranking a
            // quantile answer has to land on that value's own true rank, or one
            // of the two is reading a different CDF.
            //
            // The comparison is against `rank_incl(v)` — the exact number of
            // observations at or below `v`, which is what `rank` estimates — and
            // **not** against the requested `q`. On a tie-dense stream a single
            // value legitimately spans a wide rank band (the head of a
            // zipf(1.4) stream covers a third of it), so `q` and
            // `rank(quantile(q))` can differ by that whole band while both
            // answers are correct.
            for &qq in &[0.1f64, 0.5, 0.9] {
                let v = view.quantile(qq).expect("quantile");
                let r = view.rank(v).expect("rank") as f64 / n;
                let (_, incl) = truth.rank_interval(v);
                assert!(
                    (r - incl).abs() <= 2.0 * bound + 1e-9,
                    "rank(quantile({qq})) = {r:.6} but {v} truly occupies ranks up to \
                     {incl:.6}, off by {:.6} > 2*bound = {:.6}. {context}",
                    (r - incl).abs(),
                    2.0 * bound
                );
            }
        }

        // Each regime must actually be the regime it claims to be, across every
        // trial — otherwise the "concentrated" half would silently degenerate
        // into a second diffuse run and `E_H` would go back to being untested.
        match regime {
            "diffuse" => assert_eq!(
                gate_fired, 0,
                "the diffuse stream fired the adaptive gate on {gate_fired} of {TRIALS} \
                 trials; the regime premise is broken. {last_context}"
            ),
            _ => assert!(
                heavy_seen > 0,
                "the concentrated stream never produced a non-empty heavy set in {TRIALS} \
                 trials, so E_H was zero throughout and the full bound was never \
                 exercised. {last_context}"
            ),
        }

        tally.assert_independent_binomial(
            &format!(
                "UnivMonQ ordered queries ({regime}) / sup_x |F_hat - F| <= 2 E_H + P_hat_R eps_R"
            ),
            DELTA,
            &format!(
                "one trial = one sketch with its own hash seed and source id, scored on \
                 the supremum over every CDF breakpoint and every probed quantile; \
                 {TRIALS} trials, q grid {QS:?}. Last: {last_context}"
            ),
        );
    }
}

/// Distinct count, entropy and heavy-hitter recall for UnivMon-Q's recursive
/// sketch hierarchy.
///
/// Documented empirical regression, not a theorem. UnivMon's `g`-sum
/// recurrence composes per-layer Count Sketch errors across `levels`
/// geometrically sampled substreams; the resulting constant depends on the
/// stream's own layer occupancy and the crate publishes no closed form for it,
/// so there is no theoretical band to assert.
///
/// Band source: measured on this exact stream (40 separated heavy keys with
/// weights 60..2400, plus 20k uniform background over 800 keys, stream seed
/// 3008, default `UnivMonQConfig`). Observed relative errors are distinct
/// 1.6%, entropy 0.3%; the bands below are 10% for both, roughly six times the
/// observed movement, and heavy-hitter recall is 10/10 against a target of 8.
#[test]
fn univmonq_distinct_entropy_and_recall_stay_within_the_documented_empirical_band() {
    use common::FreqTruth;

    let mut q = UnivMonQ::new(Default::default()).expect("default config valid");
    let mut truth = FreqTruth::default();
    for i in 0..40u64 {
        for _ in 0..(i + 1) * 60 {
            q.update(&(i as f64));
            truth.observe(i as i64);
        }
    }
    for k in uniform_u64(20_000, 900, 3008) {
        let key = 100u64 + k % 800;
        q.update(&(key as f64));
        truth.observe(key as i64);
    }

    let distinct = truth.distinct() as f64;
    assert_between(
        q.estimate_distinct(),
        distinct * 0.90,
        distinct * 1.10,
        "UnivMonQ distinct (empirical band, stream seed 3008)",
    );
    let entropy = truth.entropy(false);
    assert_between(
        q.estimate_entropy(),
        entropy * 0.90,
        entropy * 1.10,
        "UnivMonQ entropy in nats (empirical band, stream seed 3008)",
    );

    // The ten heaviest separated keys carry weights 1860..2400, each at least
    // 3x the heaviest background key, so a working heavy-hitter path finds
    // essentially all of them.
    let top_true: std::collections::HashSet<u64> = (30..40u64).collect();
    let hits = q
        .heavy_hitters(10)
        .iter()
        .filter(|(v, _)| top_true.contains(&(*v as u64)))
        .count();
    assert!(
        hits >= 8,
        "UnivMonQ recovered only {hits}/10 known heavy keys (empirical band, stream seed 3008)"
    );
}

// -------------------------------------------------------- TumblingWindow<KLL>

/// Window bookkeeping is exact (which observations land in which window) while
/// the answers inside a window carry KLL's rank error. Both halves are checked
/// against their own standard: window membership by equality, quantiles by
/// `eps(k)`.
#[test]
fn tumbling_kll_windows_are_exact_and_answers_satisfy_the_rank_contract() {
    const K: i32 = 200;
    const SKETCH_SEED: u64 = 0x7717_0001;
    const N: usize = 4_000;
    const WINDOW: u64 = 400;

    let cfg = KLLConfig {
        k: K as usize,
        m: 8,
        seed: Some(SKETCH_SEED),
    };
    let mut tw: TumblingWindow<KLL> = TumblingWindow::new(WINDOW, 16, cfg, 4);
    let all: Vec<f64> = uniform_u64(N, 1_000_000, 3009)
        .iter()
        .map(|v| *v as f64)
        .collect();
    for (t, v) in all.iter().enumerate() {
        tw.insert(t as u64, &DataInput::F64(*v), 0);
    }

    // Window boundaries are arithmetic, so this is an equality, not a band.
    assert_eq!(
        tw.closed_count(),
        (N as u64 / WINDOW - 1) as usize,
        "windows [0, {}) should be closed at t={}",
        N as u64 - WINDOW,
        N - 1
    );

    // Sixteen independent compaction seeds, one trial each. Within a seed the
    // three window views (`query_all`, `query_recent`, the active sketch) are
    // built from the same compactors and are strongly dependent, so they are
    // reduced to a single worst-case rank error before the trial is scored.
    const TRIAL_SEEDS: usize = 16;
    let spec = KllRankSpec::datasketches(K as usize);
    let context = format!(
        "k={K} stream_seed=3009 n={N} window={WINDOW}, {TRIAL_SEEDS} independent \
         compaction seeds from kll_trial_seed(0x7717_0000..)"
    );
    let mut tally = Tally::default();
    for t in 0..TRIAL_SEEDS {
        let seed = kll_trial_seed(0x7717_0000 + t as u64);
        let cfg = KLLConfig {
            k: K as usize,
            m: 8,
            seed: Some(seed),
        };
        let mut w: TumblingWindow<KLL> = TumblingWindow::new(WINDOW, 16, cfg, 4);
        for (t, v) in all.iter().enumerate() {
            w.insert(t as u64, &DataInput::F64(*v), 0);
        }
        let views: [(&str, KLL<f64>, &[f64]); 3] = [
            ("query_all", w.query_all(), &all[..]),
            // query_recent(1) = the active window plus the last closed one.
            (
                "query_recent(1)",
                w.query_recent(1),
                &all[N - 2 * WINDOW as usize..],
            ),
            (
                "active_sketch",
                w.active_sketch().clone(),
                &all[N - WINDOW as usize..],
            ),
        ];
        let mut worst = 0.0f64;
        let mut detail = String::new();
        for (label, sketch, slice) in views {
            let truth = NumericTruth::new(slice.to_vec());
            let (e, d) = spec.max_rank_error(truth.sorted(), &[0.1, 0.25, 0.5, 0.75, 0.9], |q| {
                sketch.quantile(q)
            });
            if e >= worst {
                worst = e;
                detail = format!("{label}: {d}");
            }
        }
        let eps = spec.epsilon();
        tally.record(worst <= eps, || {
            format!("seed={seed:#x}: max rank error {worst:.6} > eps(k={K}) = {eps:.6}; {detail}")
        });
    }
    tally.assert_independent_binomial(
        "TumblingWindow<KLL> window queries / maximum rank error per compaction seed",
        spec.trial_failure_probability,
        &context,
    );

    // Rotation must not drop windows. `KLL::count()` is the *retained weighted
    // mass*, not an exact counter — compaction promotes half a buffer at
    // double weight, so the total is an unbiased randomized estimate of `n`
    // and single-pass sketches already report `n +- a few` before any merge.
    // The crate documents no exactness guarantee for it, so this is a
    // consistency check rather than a theorem: the estimate must stay inside
    // the same `eps(k)` band the rank contract works in, which a pipeline that
    // silently discarded a whole window (12.5% of the stream here) could not.
    let merged_count = tw.query_all().count() as f64;
    let drift = (merged_count - N as f64).abs() / N as f64;
    assert!(
        drift <= spec.epsilon(),
        "TumblingWindow<KLL>::query_all reported a weighted mass of {merged_count} for {N} \
         inserts (drift {drift:.5} > eps(k) {:.5}); a dropped or duplicated window would \
         show up here. {context}",
        spec.epsilon()
    );
}

// --------------------------------------------- Portable HydraKll per-key

/// Hydra routes each key to its own KLL cell, so each cell answers under the
/// same rank characterization as a standalone KLL over that key's observations.
///
/// Every cell of a `HydraKllSketch` is cloned from one seeded prototype, so the
/// two keys of a single grid share a compaction seed and are **not** independent
/// trials. Each grid is therefore reduced to its worst rank error across both
/// keys and the whole q grid, and the binomial runs over twelve independent
/// prototype seeds.
#[test]
fn portable_hydra_kll_per_key_medians_satisfy_the_rank_characterization() {
    const K: usize = 200;
    const TRIALS: usize = 12;
    const QS: [f64; 5] = [0.1, 0.25, 0.5, 0.75, 0.9];

    let spec = KllRankSpec::datasketches(K);
    let mut truths: Vec<(&str, NumericTruth)> = Vec::new();
    for (name, base, seed) in [("svc-a", 100.0f64, 3010u64), ("svc-b", 900.0, 3011)] {
        let vals: Vec<f64> = normal_f64(4000, base, base * 0.05, seed)
            .into_iter()
            .map(f64::abs) // HydraKll cells are KLL: positive domain
            .collect();
        truths.push((name, NumericTruth::new(vals)));
    }

    let mut tally = Tally::default();
    for t in 0..TRIALS {
        let seed = kll_trial_seed(0x5EED_0500 + t as u64);
        let mut hk = HydraKllSketch::with_seed(3, 256, K as u16, seed);
        for (name, truth) in &truths {
            for v in truth.sorted() {
                hk.update(name, *v);
            }
        }
        let mut worst = 0.0f64;
        let mut detail = String::new();
        for (name, truth) in &truths {
            let (e, d) = spec.max_rank_error(truth.sorted(), &QS, |q| hk.quantile(name, q));
            if e >= worst {
                worst = e;
                detail = format!("key {name}: {d}");
            }
        }
        let eps = spec.epsilon();
        tally.record(worst <= eps, || {
            format!("seed={seed:#x}: max rank error {worst:.6} > eps(k={K}) = {eps:.6}; {detail}")
        });
    }
    tally.assert_independent_binomial(
        "portable HydraKll per-key quantiles / maximum rank error per prototype seed",
        spec.trial_failure_probability,
        &format!(
            "k={K}, 3x256 grid, normal(100, 5) and normal(900, 45), stream seeds 3010/3011, \
             {TRIALS} independent prototype seeds from kll_trial_seed(0x5EED_0500..), \
             q grid {QS:?}"
        ),
    );
}

// ------------------------------------------- DDSketch input rejection

/// Structural guards, not accuracy: values the mapping cannot index and
/// deltas spanning an implausible bucket range must be rejected without
/// corrupting state. No error bound applies to any of it.
#[test]
fn ddsketch_rejects_untrackable_values_and_mapping_mismatches() {
    let alpha = 0.01;
    let mut core = DDSketch::new(alpha);
    let mut port = PortableDds::new(alpha);

    // Non-finite / non-positive / beyond-indexable-range values must be
    // dropped by BOTH implementations: silently, without corrupting bucket 0
    // (NaN floor-casts to 0) and without letting one sample force a distant-
    // bucket allocation (unguarded, f64::MAX mapped ~35k buckets away at
    // alpha=0.01 — ~277 KiB of amplification per sample, scaling with 1/lnγ;
    // #70 item 4).
    for v in [
        f64::NAN,
        f64::NEG_INFINITY,
        f64::INFINITY,
        -5.0,
        0.0,
        f64::MIN_POSITIVE, // below min-indexable
        5e-324,            // smallest subnormal
        f64::MAX,          // above max-indexable
        1e308,
    ] {
        core.add(&v);
        port.update(v);
    }
    assert_eq!(core.get_count(), 0, "core must drop untrackable extremes");
    assert_eq!(
        port.total_count(),
        0,
        "portable must drop untrackable extremes"
    );
    assert!(
        port.store_counts.len() < 10_000,
        "portable store grew to {} buckets from untrackable input",
        port.store_counts.len()
    );
    assert_eq!(port.quantile(0.5), None, "nothing trackable was added");

    // Normal samples still work after the rejected inputs.
    let good = [1.0f64, 2.0, 4.0, 8.0, 16.0];
    for v in good {
        core.add(&v);
        port.update(v);
    }
    assert_eq!(core.get_count(), 5);
    assert_eq!(port.total_count(), 5);
    assert_between(
        port.store_counts.len() as f64,
        1.0,
        2.0 * 256.0, // initial GROW_CHUNK seed + at most one more
        "store stays compact",
    );

    // Boundary acceptance: values just INSIDE the indexable range must still
    // be tracked, so the guards reject only genuinely unmappable extremes.
    // Bounds come from the shared production helper — core and portable MUST
    // agree because they compute from the same function.
    let (min_idx, max_idx) = asap_sketchlib::sketches::ddsketch::ddsketch_indexable_bounds(alpha);

    let mut port_boundary = PortableDds::new(alpha);
    let mut core_boundary = DDSketch::new(alpha);
    let just_inside_min = min_idx * (1.0 + 1e-9); // a hair above the floor
    let just_inside_max = max_idx * (1.0 - 1e-9); // a hair below the ceiling
    port_boundary.update(just_inside_min);
    port_boundary.update(just_inside_max);
    core_boundary.add(&just_inside_min);
    core_boundary.add(&just_inside_max);
    assert_eq!(
        port_boundary.total_count(),
        2,
        "in-range extremes near boundaries must be kept"
    );
    assert_eq!(core_boundary.get_count(), 2, "core boundary agreement");
    assert_eq!(port_boundary.total_count(), core_boundary.get_count());

    // And one step past each boundary must be rejected by both.
    port_boundary.update(min_idx * 0.5);
    port_boundary.update(max_idx * (1.0 + 1e-6));
    core_boundary.add(&(min_idx * 0.5));
    assert_eq!(
        port_boundary.total_count(),
        2,
        "out-of-range neighbors rejected"
    );
    assert_eq!(
        core_boundary.get_count(),
        2,
        "core rejects out-of-range neighbor"
    );

    // Item 2 contract: mismatched mappings are a runtime error in BOTH types,
    // not a debug-only assertion.
    let other_alpha = 0.05;
    let mut core_other = DDSketch::new(other_alpha);
    core_other.add(&1.0);
    assert!(
        core.merge(&core_other).is_err(),
        "core merge must reject alpha mismatch"
    );
    let mut port_other = PortableDds::new(other_alpha);
    port_other.update(1.0);
    assert!(
        port.merge(&port_other).is_err(),
        "portable merge must reject alpha mismatch"
    );

    // Tiny-alpha regression: at alpha=1e-9 ln(gamma) ~ 2e-9, and naive
    // reciprocal-multiplied guard formulas diverge from core's — admitting
    // v=1e-300 whose bucket index saturates i32 and overflows ensure_bucket.
    // Both implementations must drop it identically via the shared helper.
    let mut tiny = PortableDds::new(1e-9);
    let mut tiny_core = DDSketch::new(1e-9);
    tiny.update(1e-300);
    tiny_core.add(&1e-300);
    assert_eq!(
        tiny.total_count(),
        0,
        "portable drops sub-indexable value at tiny alpha"
    );
    assert_eq!(
        tiny_core.get_count(),
        0,
        "core drops sub-indexable value at tiny alpha"
    );
    assert_eq!(
        tiny.store_counts.len(),
        0,
        "no allocation may occur for rejected values"
    );
}

#[test]
fn portable_ddsketch_rejects_hostile_delta_spans() {
    use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketchDelta;

    let alpha = 0.01;
    let mut base = PortableDds::new(alpha);
    base.update(1.0);
    let (len_before, offset_before) = (base.store_counts.len(), base.store_offset);

    // A corrupt/hostile delta pointing near i32::MAX must be rejected with an
    // error BEFORE any allocation: the naive pad would be ~2e9 buckets.
    let hostile = DdSketchDelta {
        buckets: vec![(i32::MAX - 1, 1), (7, 3)],
        ..DdSketchDelta::default()
    };
    assert!(
        base.apply_delta(&hostile).is_err(),
        "hostile far-span delta must be rejected"
    );
    assert_eq!(
        base.store_counts.len(),
        len_before,
        "state untouched on rejection"
    );
    assert_eq!(
        base.store_offset, offset_before,
        "offset untouched on rejection"
    );

    // Benign deltas still apply: bucket index 6 carries count 5 afterward,
    // and its representative gamma^6*(1+alpha) becomes visible in queries.
    let benign = DdSketchDelta {
        buckets: vec![(6, 5)],
        ..DdSketchDelta::default()
    };
    base.apply_delta(&benign).expect("benign delta");
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    assert_eq!(
        base.quantile(0.9),
        Some(gamma.powf(6.0) * (1.0 + alpha)),
        "applied delta count visible through queries"
    );
}
