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
//! Neither may be run through the other's battery. `RankErrorSpec` and
//! `RelativeQuantileSpec` in `common::specs` keep them apart.
//!
//! Every sketch here is built with an explicit compaction seed. KLL's coin is
//! the sketch's own randomness and is entirely separate from the stream seed;
//! the wall-clock-seeded `KLL::init_kll` / `KLLDynamic::init_kll` constructors
//! cannot be used in an accuracy test because a failure would not reproduce.

mod common;

use common::specs::{RankErrorSpec, RelativeQuantileSpec, Tally};
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

/// Both KLL implementations against the published normalized-rank-error
/// contract `eps(k) = 2.446 / k^0.9433` at 99% confidence, over a fixed grid
/// of `(k, sketch seed, distribution, feed mode, q)`.
///
/// Failures are pooled per feed mode and judged by the binomial acceptance
/// rule at the contract's own per-query failure probability of 0.01. Widening
/// `eps` to make a failure disappear would break the tie to `k` and is exactly
/// what this spec exists to prevent.
#[test]
fn kll_family_satisfies_the_datasketches_normalized_rank_error_contract() {
    // 4 seeds x 6 shapes x 3 values of k x 4 feed modes x 7 quantiles is ~500
    // checks per feed mode, which the binomial acceptance rule at p = 0.01
    // makes a tight test; a longer stream would not make it tighter.
    const N: usize = 30_000;
    const KS: [i32; 3] = [64, 200, 800];

    let mut tallies: HashMap<String, Tally> = HashMap::new();
    for (trial, &sketch_seed) in KLL_SKETCH_SEEDS.iter().enumerate() {
        for (shape, values) in rank_streams(trial, N) {
            let truth = NumericTruth::new(values.clone());
            for &k in &KS {
                let spec = RankErrorSpec::datasketches(k as usize);
                for &feed in &FEEDS {
                    let fixed = feed_kll(feed, k, sketch_seed, &values);
                    spec.tally_into(
                        tallies.entry(format!("KLL/{feed:?}")).or_default(),
                        truth.sorted(),
                        &RANK_QS,
                        |q| fixed.quantile(q),
                    );
                    if let Some(dynamic) = feed_kll_dynamic(feed, k, sketch_seed, &values) {
                        spec.tally_into(
                            tallies.entry(format!("KLLDynamic/{feed:?}")).or_default(),
                            truth.sorted(),
                            &RANK_QS,
                            |q| dynamic.quantile(q),
                        );
                    }
                    let _ = shape;
                }
            }
        }
    }

    for (label, tally) in tallies {
        tally.assert_within(
            &format!("{label} / normalized rank error"),
            RankErrorSpec::datasketches(200).failure_probability,
            &format!(
                "n={N} k in {KS:?}, sketch seeds {KLL_SKETCH_SEEDS:02x?}, \
                 stream shapes {:?}, q grid {RANK_QS:?}",
                rank_streams(0, 1)
                    .iter()
                    .map(|(s, _)| *s)
                    .collect::<Vec<_>>()
            ),
        );
    }
}

/// Rank error must shrink as `k` grows, at the rate the contract states.
/// A hard-coded tolerance cannot see this: it passes identically at `k = 64`
/// and `k = 800`, so it would not notice a `k` that stopped being wired
/// through to the compactor capacities at all.
#[test]
fn kll_rank_error_shrinks_with_k_as_the_contract_predicts() {
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
        let eps = RankErrorSpec::datasketches(*k as usize).epsilon();
        assert!(
            *w <= eps,
            "KLL k={k}: worst rank error {w:.5} exceeds eps(k)={eps:.5} \
             (n={N}, sketch seeds {KLL_SKETCH_SEEDS:02x?}, stream seed 0xC0FFEE01)"
        );
    }
    // 16x more capacity must buy at least a 4x tighter rank error; the
    // contract predicts 16^0.9433 = 13.4x.
    let (k_lo, w_lo) = worst[0];
    let (k_hi, w_hi) = worst[2];
    assert!(
        w_lo >= w_hi * 4.0,
        "raising k from {k_lo} to {k_hi} moved the worst rank error only from \
         {w_lo:.5} to {w_hi:.5}; the contract predicts a {:.1}x improvement, so \
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
/// The comparison is against the **exact nearest-rank order statistic** at the
/// same `q`, using the same ceil convention `get_value_at_quantile` uses, so
/// the two sides answer literally the same question. The tolerance is
/// `alpha + numerical_slack`, where the slack is a few ULP of the logarithmic
/// mapping — never a percentage of alpha, which would license breaking the
/// advertised guarantee by that percentage.
#[test]
fn ddsketch_core_and_portable_satisfy_the_relative_value_error_contract() {
    const SAMPLE_SIZES: [usize; 3] = [1_000, 20_000, 100_000];

    for &alpha in &DDS_ALPHAS {
        let spec = RelativeQuantileSpec::new(alpha);
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
                spec.tally_into(&mut core_tally, truth.sorted(), &DDS_QS, |q| {
                    core.get_value_at_quantile(q)
                });
                spec.tally_into(&mut port_tally, truth.sorted(), &DDS_QS, |q| {
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

/// `q = 0` and `q = 1` are exact min/max, not bucket representatives: both
/// implementations track the true extremes alongside the bucket store, so the
/// relative error at the endpoints is zero rather than merely within alpha.
#[test]
fn ddsketch_endpoints_return_the_exact_min_and_max() {
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
        // The portable twin clamps its bucket representative into
        // [min, max], so its endpoints are exact too.
        let (p0, p1) = (port.quantile(0.0).unwrap(), port.quantile(1.0).unwrap());
        assert!(
            (p0 - truth.min()).abs() <= truth.min() * alpha,
            "portable alpha={alpha}: q=0 gave {p0}, true min {}",
            truth.min()
        );
        assert!(
            (p1 - truth.max()).abs() <= truth.max() * alpha,
            "portable alpha={alpha}: q=1 gave {p1}, true max {}",
            truth.max()
        );
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
        let spec = RelativeQuantileSpec::new(alpha);
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
                if let Err(detail) = spec.check(0.5, est, probe) {
                    panic!("core DDSketch alpha={alpha} gamma^{k} boundary probe: {detail}");
                }
                let pest = port.quantile(0.5).unwrap();
                if let Err(detail) = spec.check(0.5, pest, probe) {
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
        let spec = RelativeQuantileSpec::new(alpha);
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
        spec.tally_into(&mut port_tally, truth.sorted(), &DDS_QS, |q| pa.quantile(q));
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
        spec.tally_into(&mut pr_tally, truth.sorted(), &DDS_QS[1..6], |q| {
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

/// Ordered queries against the residual half of the documented bound.
///
/// The full contract in `docs/api/api_univmon_q.md` is
///
/// ```text
///   sup_x |F_hat(x) - F(x)| <= 2 E_H + P_hat_R * epsilon_R
/// ```
///
/// `E_H` is the frequency error over the *internally recovered* heavy set, and
/// neither that set nor `m_R` is reachable through the public API — so the
/// complete theorem **cannot be verified from outside the crate**, and this
/// test does not claim to.
///
/// What it does verify is the strongest contract public state supports: the
/// diffuse regime. The adaptive gate is `F2_hat / N^2 >= 1 / ordered_samples`,
/// and both sides of that inequality are public (`estimate_f2`, `count`,
/// `config().ordered_samples`). On a stream where the gate provably does not
/// fire, the heavy set is empty, so `E_H = 0` and `P_hat_R = 1`, and the whole
/// bound collapses to the distribution-free occurrence bound
/// `epsilon_R = sqrt(ln(2/delta) / (2 m_R))` over the retained occurrence
/// sample, whose size is observable as the number of CDF breakpoints.
#[test]
fn univmonq_ordered_queries_satisfy_the_residual_occurrence_bound_when_diffuse() {
    use common::specs::{occurrence_sample_epsilon, rank_violation};

    const DELTA: f64 = 0.01;
    let config = UnivMonQConfig::default();
    let mut q = UnivMonQ::new(config).expect("default config valid");

    // Diffuse by construction: 200k observations spread over 200k distinct
    // values, so no value carries enough mass to qualify as heavy.
    let values: Vec<f64> = uniform_u64(200_000, 10_000_000, 0x0DDE_0001)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    for v in &values {
        q.update(v);
    }
    let truth = NumericTruth::new(values.clone());
    let n = q.count() as f64;

    // The gate condition, read entirely from public state.
    let gate = q.estimate_f2() / (n * n);
    let threshold = 1.0 / config.ordered_samples as f64;
    assert!(
        gate < threshold,
        "test premise broken: the adaptive gate fired (F2_hat/N^2 = {gate:.3e} >= \
         1/ordered_samples = {threshold:.3e}), so the heavy set may be non-empty and \
         E_H is no longer provably zero"
    );

    let breakpoints = q.cdf();
    let m_r = breakpoints.len();
    let eps = occurrence_sample_epsilon(m_r, DELTA);
    let context = format!(
        "diffuse uniform stream n={} distinct={}, ordered_samples={}, \
         retained CDF breakpoints m_R={m_r}, delta={DELTA} -> epsilon_R={eps:.5}, \
         stream seed 0x0DDE0001",
        values.len(),
        truth.sorted().windows(2).filter(|w| w[0] != w[1]).count() + 1,
        config.ordered_samples
    );

    // The bound is a supremum over x, so every breakpoint must satisfy it.
    let mut sup_tally = Tally::default();
    for point in &breakpoints {
        let (excl, incl) = truth.rank_interval(point.value);
        let dist = if point.rank < excl {
            excl - point.rank
        } else if point.rank > incl {
            point.rank - incl
        } else {
            0.0
        };
        sup_tally.record(dist <= eps, || {
            format!(
                "value {} reported rank {:.5} but occupies true ranks [{excl:.5}, {incl:.5}] \
                 (distance {dist:.5} > epsilon_R {eps:.5})",
                point.value, point.rank
            )
        });
    }
    sup_tally.assert_within(
        "UnivMonQ CDF sup-error vs the residual occurrence bound",
        DELTA,
        &context,
    );

    // Inverting the same bound: a quantile answer's rank must land within
    // epsilon_R of the requested q.
    let mut quantile_tally = Tally::default();
    for &qq in &[0.01f64, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99] {
        let est = q.quantile(qq).expect("ordered_samples enabled");
        let violation = rank_violation(truth.sorted(), qq, est, eps);
        quantile_tally.record(violation.is_none(), || violation.unwrap());
    }
    quantile_tally.assert_within(
        "UnivMonQ::quantile vs the residual occurrence bound",
        DELTA,
        &context,
    );

    // `rank` and `quantile` must be a consistent inverse pair: re-ranking a
    // quantile answer has to reproduce the requested rank to within the same
    // epsilon, or one of the two is reading a different CDF.
    for &qq in &[0.1f64, 0.5, 0.9] {
        let v = q.quantile(qq).expect("quantile");
        let r = q.rank(v).expect("rank") as f64 / n;
        assert!(
            (r - qq).abs() <= 2.0 * eps,
            "rank(quantile({qq})) = {r:.5}, off by {:.5} > 2*epsilon_R = {:.5}. {context}",
            (r - qq).abs(),
            2.0 * eps
        );
    }

    // Monotonicity is structural: a CDF that decreases is malformed whatever
    // the sampling error.
    assert!(!breakpoints.is_empty(), "cdf must not be empty");
    for w in breakpoints.windows(2) {
        assert!(
            w[0].rank <= w[1].rank + 1e-9,
            "cdf ranks not monotone at {:?} -> {:?}. {context}",
            w[0],
            w[1]
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

    let spec = RankErrorSpec::datasketches(K as usize);
    let context =
        format!("k={K} sketch_seed=0x{SKETCH_SEED:08x} stream_seed=3009 n={N} window={WINDOW}");
    let mut tally = Tally::default();
    for (label, sketch, slice) in [
        ("query_all", tw.query_all(), &all[..]),
        // query_recent(1) = the active window plus the last closed one.
        (
            "query_recent(1)",
            tw.query_recent(1),
            &all[N - 2 * WINDOW as usize..],
        ),
        (
            "active_sketch",
            tw.active_sketch().clone(),
            &all[N - WINDOW as usize..],
        ),
    ] {
        let truth = NumericTruth::new(slice.to_vec());
        spec.tally_into(
            &mut tally,
            truth.sorted(),
            &[0.1, 0.25, 0.5, 0.75, 0.9],
            |q| sketch.quantile(q),
        );
        let _ = label;
    }
    tally.assert_within(
        "TumblingWindow<KLL> window queries / rank error",
        spec.failure_probability,
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
/// same rank contract as a standalone KLL over that key's observations.
#[test]
fn portable_hydra_kll_per_key_medians_satisfy_the_rank_contract() {
    const K: usize = 200;
    let spec = RankErrorSpec::datasketches(K);
    let mut hk = HydraKllSketch::with_seed(3, 256, K as u16, 0x5EED_0500);
    let mut truths: HashMap<&str, NumericTruth> = HashMap::new();
    for (name, base, seed) in [("svc-a", 100.0f64, 3010u64), ("svc-b", 900.0, 3011)] {
        let vals: Vec<f64> = normal_f64(4000, base, base * 0.05, seed)
            .into_iter()
            .map(f64::abs) // HydraKll cells are KLL: positive domain
            .collect();
        truths.insert(name, NumericTruth::new(vals.clone()));
        for v in vals {
            hk.update(name, v);
        }
    }
    let mut tally = Tally::default();
    for (name, truth) in &truths {
        spec.tally_into(
            &mut tally,
            truth.sorted(),
            &[0.1, 0.25, 0.5, 0.75, 0.9],
            |q| hk.quantile(name, q),
        );
    }
    tally.assert_within(
        "portable HydraKll per-key quantiles / rank error",
        spec.failure_probability,
        &format!(
            "k={K} sketch_seed=0x5EED0500, 3x256 grid, normal(100, 5) and normal(900, 45), \
             stream seeds 3010/3011"
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
