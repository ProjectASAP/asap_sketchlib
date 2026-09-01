//! E2E composition-layer pipelines: Hydra subpopulation queries, conformance
//! batteries and error bounds; UnivMon; Nitro; ExponentialHistogram;
//! TumblingWindow; HashSketchEnsemble.

mod common;
#[path = "e2e_frameworks/hydra_data.rs"]
mod hydra_data;

use std::collections::HashMap;

use common::specs::KllRankSpec;
use common::streams::{ZipfConfig, ZipfGenerator, normal_f64, uniform_u64, zipf_i64, zipf_u64};
use common::truth::freq_truth;
use common::{FreqTruth, assert_between};
use hydra_data::{ENDPOINTS, H2_REGIONS, H2_SERVICES, REGIONS, Record, SCHEMA, STATUSES, h2_keys};

use asap_sketchlib::input::{HydraCounter, HydraQuery};
use asap_sketchlib::{
    Count, CountMin, DataInput, EHSketchList, ExponentialHistogram, FastPath, FoldCMS,
    FoldCMSConfig, Hydra, KLL, TumblingWindow, UnivMon, UnivMonPyramid, UnivSketchPool, Vector2D,
};

// ------------------------------------------------------------------- Hydra

/// `k` every Hydra KLL cell in this file is built at.
const HYDRA_KLL_K: usize = 200;

/// Accepted band for a UnivMon `L2Norm` reading, derived from the AMS
/// second-moment bound rather than written down.
///
/// The reported value is `sqrt(F2_hat)` where `F2_hat` is the row-median AMS
/// estimate over the sketch's own counters, so `SecondMomentSpec`'s relative
/// bound `b = sqrt(2*kappa/w)` on `F2` becomes, on the norm,
///
/// ```text
///   L2 * sqrt(1 - b)  <=  L2_hat  <=  L2 * sqrt(1 + b)
/// ```
///
/// Both endpoints are computed from the sketch's own `(rows, cols)`, so a
/// narrower sketch is held to a wider band automatically.
fn univmon_l2_band(exact_l2: f64, rows: usize, cols: usize) -> (f64, f64) {
    let b = common::specs::SecondMomentSpec::new(rows, cols).relative_bound();
    (
        exact_l2 * (1.0 - b).max(0.0).sqrt(),
        exact_l2 * (1.0 + b).sqrt(),
    )
}

#[test]
fn hydra_cm_multilabel_frequencies() {
    // Sparse dims keep full-key counts collision-free => near-exact.
    let mut hydra = Hydra::with_schema(
        4,
        4096,
        ["region", "user"],
        HydraCounter::CM(
            CountMin::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(4, 4096),
        ),
    )
    .expect("schema");

    let regions = ["eu", "us"];
    let users = ["alice", "bob"];
    for r in regions {
        for u in users {
            for _ in 0..25 {
                hydra
                    .update(&[r, u], &DataInput::Str("event"), None)
                    .expect("update");
            }
        }
    }

    let mut failures: Vec<String> = Vec::new();
    // Wildcards are expressed with None at the trailing positions; truncated
    // slices are an arity error. Generalized (wildcard) keys accumulate
    // sibling traffic through the fan-out, so they get one-sided slack.
    for (key, expected, exact) in [
        (vec![Some("eu"), Some("alice")], 25i64, true),
        (vec![Some("us"), Some("bob")], 25, true),
        (vec![Some("eu"), None], 50, false),
        (vec![None, Some("bob")], 50, false),
        (vec![Some("apac"), None], 0, false),
    ] {
        let v = hydra
            .query_key(&key, &HydraQuery::Frequency(DataInput::Str("event")))
            .expect("CM frequency query");
        let ok = if exact {
            (v - expected as f64).abs() <= 2.0
        } else {
            v >= expected as f64 && v <= expected as f64 * 1.2 + 1.0
        };
        if !ok {
            failures.push(format!("{key:?} expected {expected} exact={exact} got {v}"));
        }
    }
    assert!(
        failures.is_empty(),
        "hydra CM frequencies off: {failures:?}"
    );
}

#[test]
fn hydra_kll_head_quantile_and_cdf() {
    let mut hydra = Hydra::with_schema(
        4,
        512,
        ["shard"],
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 0x5EED_0600)),
    )
    .expect("schema");
    let values: Vec<f64> = uniform_u64(20_000, 1_000_000, 4001)
        .iter()
        .map(|v| *v as f64)
        .collect();
    let truth = common::NumericTruth::new(values.clone());
    for v in &values {
        hydra
            .update(&["s0"], &DataInput::F64(*v), None)
            .expect("update");
    }

    let med = hydra
        .query_key(&[Some("s0")], &HydraQuery::Quantile(0.5))
        .expect("quantile");
    assert_between(
        med,
        truth.quantile(0.47),
        truth.quantile(0.53),
        "Hydra KLL median",
    );

    // Cdf(x) returns the empirical fraction <= x.
    let x = 500_000.0;
    let cdf = hydra
        .query_key(&[Some("s0")], &HydraQuery::Cdf(x))
        .expect("cdf");
    // The CDF band is KLL's own rank error at the cell's `k`, not a written
    // constant: a Hydra cell is a KLL over that subpopulation, and `cdf(x)` is
    // a rank read, so `eps(k)` is exactly the right half-width.
    let kll_eps = KllRankSpec::datasketches(HYDRA_KLL_K).epsilon();
    assert_between(
        cdf,
        truth.cdf(x) - kll_eps,
        truth.cdf(x) + kll_eps,
        "Hydra KLL CDF",
    );

    // Unseen population reports zero rather than erroring.
    let none = hydra
        .query_key(&[Some("ghost")], &HydraQuery::Cdf(x))
        .expect("empty cell");
    assert_eq!(none, 0.0, "empty subpopulation must be 0");
}

#[test]
fn hydra_hll_head_cardinality() {
    let mut hydra = Hydra::with_schema(4, 512, ["tenant"], HydraCounter::HLL(Default::default()))
        .expect("schema");
    for t in ["t1", "t2"] {
        for i in 0..500u32 {
            hydra
                .update(&[t], &DataInput::U32(i), None)
                .expect("update");
        }
    }
    for t in ["t1", "t2"] {
        let card = hydra
            .query_key(&[Some(t)], &HydraQuery::Cardinality)
            .expect("card");
        assert_between(card, 450.0, 550.0, &format!("tenant {t} cardinality"));
    }
}

/// Sole member of the measure domain, which makes each per-cell Count-Min
/// exact and leaves the grid as the only error source.
const MEASURE: &str = "hit";
const DEFAULT_QUANTILE_QS: [f64; 5] = [0.1, 0.25, 0.5, 0.75, 0.9];

/// Per-cell counter for grid-focused tests, sized to a value domain of a few
/// members rather than to a standalone deployment.
fn cell_cm() -> HydraCounter {
    HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 64))
}

fn cell_cs() -> HydraCounter {
    HydraCounter::CS(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 64))
}

#[test]
fn hydra_cm_passes_frequency_and_merge_conformance() {
    let stream = zipf_i64(40_000, 256, 1.1, 5_101);
    let truth = freq_truth(&stream);
    let mut sketch = Hydra::with_schema(5, 8192, ["key"], cell_cm()).expect("schema");
    let mut left = Hydra::with_schema(5, 8192, ["key"], cell_cm()).expect("schema");
    let mut right = Hydra::with_schema(5, 8192, ["key"], cell_cm()).expect("schema");
    for (index, key) in stream.iter().enumerate() {
        let key = key.to_string();
        sketch
            .update(&[key.as_str()], &DataInput::Str(MEASURE), None)
            .expect("update");
        let shard = if index % 2 == 0 {
            &mut left
        } else {
            &mut right
        };
        shard
            .update(&[key.as_str()], &DataInput::Str(MEASURE), None)
            .expect("update");
    }
    left.merge(&right).expect("merge");
    for (key, exact) in truth.pairs() {
        let key = key.to_string();
        let query = HydraQuery::Frequency(DataInput::Str(MEASURE));
        let estimate = sketch
            .query_key(&[Some(key.as_str())], &query)
            .expect("query");
        assert!(
            estimate >= exact as f64 && estimate <= exact as f64 * 1.01 + 4.0,
            "Hydra CM key {key}: estimate {estimate}, exact {exact}"
        );
        assert_eq!(left.query_key(&[Some(key.as_str())], &query), Ok(estimate));
    }
}

#[test]
fn hydra_cs_passes_signed_frequency_conformance() {
    let stream = zipf_i64(40_000, 256, 1.1, 5_101);
    let truth = freq_truth(&stream);
    let mut sketch = Hydra::with_schema(5, 8192, ["key"], cell_cs()).expect("schema");
    let mut left = Hydra::with_schema(5, 8192, ["key"], cell_cs()).expect("schema");
    let mut right = Hydra::with_schema(5, 8192, ["key"], cell_cs()).expect("schema");
    for (index, key) in stream.iter().enumerate() {
        let key = key.to_string();
        sketch
            .update(&[key.as_str()], &DataInput::Str(MEASURE), None)
            .expect("update");
        let shard = if index % 2 == 0 {
            &mut left
        } else {
            &mut right
        };
        shard
            .update(&[key.as_str()], &DataInput::Str(MEASURE), None)
            .expect("update");
    }
    left.merge(&right).expect("merge");
    for (key, exact) in truth.pairs() {
        let key = key.to_string();
        let query = HydraQuery::Frequency(DataInput::Str(MEASURE));
        let estimate = sketch
            .query_key(&[Some(key.as_str())], &query)
            .expect("query");
        assert!(
            (estimate - exact as f64).abs() <= exact as f64 * 0.06 + 25.0,
            "Hydra CS key {key}: estimate {estimate}, exact {exact}"
        );
        assert_eq!(left.query_key(&[Some(key.as_str())], &query), Ok(estimate));
    }
    let key = "42";
    sketch
        .update(&[key], &DataInput::Str(MEASURE), Some(500))
        .expect("increment");
    sketch
        .update(&[key], &DataInput::Str(MEASURE), Some(-500))
        .expect("decrement");
}

// ---------------------------------------------------------------------------
// Hydra: the subpopulation lattice
// ---------------------------------------------------------------------------

type LatticeKey = (Vec<Option<&'static str>>, &'static str);

/// Exact record counts for every `(subpopulation, endpoint)` pair over the
/// whole `2^D - 1` lattice.
fn lattice_truth(stream: &[Record]) -> HashMap<LatticeKey, i64> {
    let mut truth: HashMap<LatticeKey, i64> = HashMap::new();
    for rec in stream {
        for mask in 1u32..(1u32 << SCHEMA.len()) {
            let key: Vec<Option<&'static str>> = (0..SCHEMA.len())
                .map(|col| ((mask >> col) & 1 == 1).then_some(rec.key[col]))
                .collect();
            *truth.entry((key, rec.endpoint)).or_insert(0) += 1;
        }
    }
    truth
}

fn ingest(hydra: &mut Hydra, stream: &[Record]) {
    for rec in stream {
        hydra
            .update(&rec.key, &DataInput::Str(rec.endpoint), None)
            .expect("arity 3");
    }
}

fn freq(hydra: &Hydra, key: &[Option<&str>], endpoint: &str) -> f64 {
    hydra
        .query_key(key, &HydraQuery::Frequency(DataInput::Str(endpoint)))
        .expect("well-formed frequency query")
}

/// Grid over a schema producing at most 99 distinct subkeys: 11 singles,
/// 40 pairs, 48 triples.
fn lattice_hydra(cols: usize) -> Hydra {
    Hydra::with_schema(5, cols, SCHEMA, cell_cm()).expect("three-column schema")
}

#[test]
fn hydra_subpopulation_counts_are_exact_on_a_sparse_grid() {
    // 99 subkeys over 4096 columns: median-of-5 needs three rows to collide
    // before an answer moves, so every dense subpopulation is exact.
    let stream = {
        let count = 30_000;
        let seed = 4_200;
        let src = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.8,
            seed,
        });
        let dst = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.5,
            seed: seed + 1,
        });
        let statuses = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: STATUSES.len(),
            exponent: 1.2,
            seed: seed + 2,
        });
        let endpoints = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: ENDPOINTS.len(),
            exponent: 0.4,
            seed: seed + 3,
        });
        (0..count)
            .map(|i| Record {
                key: [
                    REGIONS[src[i] as usize],
                    REGIONS[dst[i] as usize],
                    STATUSES[statuses[i] as usize],
                ],
                endpoint: ENDPOINTS[endpoints[i] as usize],
            })
            .collect::<Vec<_>>()
    };
    let truth = lattice_truth(&stream);
    let mut hydra = lattice_hydra(4096);
    ingest(&mut hydra, &stream);

    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for ((key, endpoint), count) in &truth {
        if *count < 25 {
            continue;
        }
        checked += 1;
        let est = freq(&hydra, key, endpoint);
        if est != *count as f64 {
            failures.push(format!("{key:?} x {endpoint}: true {count}, est {est}"));
        }
    }
    failures.sort();
    assert!(checked > 200, "lattice too sparse to be meaningful");
    assert!(
        failures.is_empty(),
        "{} of {checked} dense subpopulations inexact:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// `Hydra::update`'s `count` reaches the per-cell counter, so a
/// subpopulation's frequency is its weighted total, not its record count.
fn assert_weighted_updates_reach_the_cell(counter: HydraCounter, slack: impl Fn(i64) -> f64) {
    let stream = {
        let count = 12_000;
        let seed = 4_240;
        let src = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.8,
            seed,
        });
        let dst = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.5,
            seed: seed + 1,
        });
        let statuses = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: STATUSES.len(),
            exponent: 1.2,
            seed: seed + 2,
        });
        let endpoints = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: ENDPOINTS.len(),
            exponent: 0.4,
            seed: seed + 3,
        });
        (0..count)
            .map(|i| Record {
                key: [
                    REGIONS[src[i] as usize],
                    REGIONS[dst[i] as usize],
                    STATUSES[statuses[i] as usize],
                ],
                endpoint: ENDPOINTS[endpoints[i] as usize],
            })
            .collect::<Vec<_>>()
    };
    let mut hydra = Hydra::with_schema(5, 4096, SCHEMA, counter).expect("three-column schema");
    let mut truth: HashMap<LatticeKey, i64> = HashMap::new();

    for (i, rec) in stream.iter().enumerate() {
        let weight = 1 + (i % 7) as i64;
        hydra
            .update(&rec.key, &DataInput::Str(rec.endpoint), Some(weight as i32))
            .expect("arity 3");
        for mask in 1u32..(1u32 << SCHEMA.len()) {
            let key: Vec<Option<&'static str>> = (0..SCHEMA.len())
                .map(|col| ((mask >> col) & 1 == 1).then_some(rec.key[col]))
                .collect();
            *truth.entry((key, rec.endpoint)).or_insert(0) += weight;
        }
    }

    let mut checked = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for ((key, endpoint), total) in &truth {
        if *total < 100 {
            continue;
        }
        checked += 1;
        let est = freq(&hydra, key, endpoint);
        if (est - *total as f64).abs() > slack(*total) {
            failures.push(format!(
                "{key:?} x {endpoint}: weighted total {total}, est {est}"
            ));
        }
    }
    failures.sort();
    assert!(checked > 200, "lattice too sparse to be meaningful");
    assert!(
        failures.is_empty(),
        "{} of {checked} weighted subpopulations wrong:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

#[test]
fn hydra_cm_weighted_updates_reach_the_cell_counter() {
    assert_weighted_updates_reach_the_cell(cell_cm(), |_| 0.0);
}

#[test]
fn hydra_cs_weighted_updates_reach_the_cell_counter() {
    // The kit's Count spec.
    assert_weighted_updates_reach_the_cell(cell_cs(), |t| t as f64 * 0.06 + 25.0);
}

/// KLL treats `count` as multiplicity, so a weighted stream must answer the
/// quantiles of the stream with each value repeated `count` times.
#[test]
fn hydra_kll_weighted_updates_repeat_the_value() {
    let n = 20_000usize;
    let keys = h2_keys(n, 4_760);
    let values = normal_f64(n, 500.0, 80.0, 4_765);
    let mut hydra = Hydra::with_schema(
        5,
        128,
        ["region", "service"],
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 4_766)),
    )
    .expect("two-column schema");
    let mut truth: HashMap<Vec<Option<&str>>, Vec<f64>> = HashMap::new();

    for (i, (region, service)) in keys.iter().enumerate() {
        let weight = 1 + (i % 4) as i64;
        hydra
            .update(
                &[region, service],
                &DataInput::F64(values[i]),
                Some(weight as i32),
            )
            .expect("arity 2");
        for key in h2_masks(region, service) {
            let slot = truth.entry(key.to_vec()).or_default();
            for _ in 0..weight {
                slot.push(values[i]);
            }
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (key, vals) in truth {
        let exact = common::NumericTruth::new(vals);
        for q in DEFAULT_QUANTILE_QS {
            let est = hydra
                .query_key(&key, &HydraQuery::Quantile(q))
                .expect("quantile");
            let (lo, hi) = exact.quantile_band(q, KllRankSpec::datasketches(HYDRA_KLL_K).epsilon());
            if est < lo || est > hi {
                failures.push(format!(
                    "{key:?} q={q}: {est:.3} outside [{lo:.3}, {hi:.3}]"
                ));
            }
        }
    }
    failures.sort();
    assert!(
        failures.is_empty(),
        "weighted KLL quantiles out of rank band:\n  {}",
        failures.join("\n  ")
    );
}

/// HLL cells answer per-subpopulation distinct counts across the lattice.
#[test]
fn hydra_hll_head_subpopulation_cardinalities() {
    let n = 40_000usize;
    let keys = h2_keys(n, 4_730);
    let ids = zipf_u64(n, 20_000, 0.2, 4_735);
    let mut hydra = Hydra::with_schema(
        5,
        128,
        ["region", "service"],
        HydraCounter::HLL(Default::default()),
    )
    .expect("two-column schema");
    let mut truth: HashMap<Vec<Option<&str>>, std::collections::HashSet<u64>> = HashMap::new();

    for (i, (region, service)) in keys.iter().enumerate() {
        hydra
            .update(&[region, service], &DataInput::U32(ids[i] as u32), None)
            .expect("arity 2");
        for key in h2_masks(region, service) {
            truth.entry(key.to_vec()).or_default().insert(ids[i]);
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (key, distinct) in &truth {
        let t = distinct.len() as f64;
        let est = hydra
            .query_key(key, &HydraQuery::Cardinality)
            .expect("cardinality");
        // CardinalitySpec::default() from the conformance kit.
        if est < t * 0.97 || est > t * 1.03 {
            failures.push(format!("{key:?}: distinct {t}, est {est:.0}"));
        }
    }
    failures.sort();
    assert_eq!(truth.len(), 8, "the 2^2-1 lattice over 2x2 domains");
    assert!(
        failures.is_empty(),
        "HLL subpopulation cardinalities out of band:\n  {}",
        failures.join("\n  ")
    );
}

/// KLL cells answer per-subpopulation quantiles across the lattice.
#[test]
fn hydra_kll_head_subpopulation_quantiles() {
    let n = 40_000usize;
    let keys = h2_keys(n, 4_740);
    let values = normal_f64(n, 500.0, 80.0, 4_745);
    let mut hydra = Hydra::with_schema(
        5,
        128,
        ["region", "service"],
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 4_746)),
    )
    .expect("two-column schema");
    let mut truth: HashMap<Vec<Option<&str>>, Vec<f64>> = HashMap::new();

    for (i, (region, service)) in keys.iter().enumerate() {
        hydra
            .update(&[region, service], &DataInput::F64(values[i]), None)
            .expect("arity 2");
        for key in h2_masks(region, service) {
            truth.entry(key.to_vec()).or_default().push(values[i]);
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (key, vals) in truth {
        let exact = common::NumericTruth::new(vals);
        for q in DEFAULT_QUANTILE_QS {
            let est = hydra
                .query_key(&key, &HydraQuery::Quantile(q))
                .expect("quantile");
            // QuantileSpec::default() from the conformance kit.
            let (lo, hi) = exact.quantile_band(q, KllRankSpec::datasketches(HYDRA_KLL_K).epsilon());
            if est < lo || est > hi {
                failures.push(format!(
                    "{key:?} q={q}: {est:.3} outside [{lo:.3}, {hi:.3}]"
                ));
            }
        }
    }
    failures.sort();
    assert!(
        failures.is_empty(),
        "KLL subpopulation quantiles out of rank band:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn hydra_marginals_agree_with_the_sum_of_their_children() {
    // A wildcard query reads its own subkey, written by the fan-out on every
    // matching record, not a roll-up of the cells beneath it.
    let stream = {
        let count = 30_000;
        let seed = 5_210;
        let src = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.8,
            seed,
        });
        let dst = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.5,
            seed: seed + 1,
        });
        let statuses = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: STATUSES.len(),
            exponent: 1.2,
            seed: seed + 2,
        });
        let endpoints = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: ENDPOINTS.len(),
            exponent: 0.4,
            seed: seed + 3,
        });
        (0..count)
            .map(|i| Record {
                key: [
                    REGIONS[src[i] as usize],
                    REGIONS[dst[i] as usize],
                    STATUSES[statuses[i] as usize],
                ],
                endpoint: ENDPOINTS[endpoints[i] as usize],
            })
            .collect::<Vec<_>>()
    };
    let mut hydra = lattice_hydra(4096);
    ingest(&mut hydra, &stream);

    let mut failures: Vec<String> = Vec::new();
    for region in REGIONS {
        for endpoint in ENDPOINTS {
            let parent = freq(&hydra, &[Some(region), None, None], endpoint);
            let children: f64 = REGIONS
                .iter()
                .flat_map(|dst| {
                    STATUSES
                        .iter()
                        .map(move |st| [Some(region), Some(*dst), Some(*st)])
                })
                .map(|key| freq(&hydra, &key, endpoint))
                .sum();
            if parent != children {
                failures.push(format!(
                    "src {region} x {endpoint}: marginal {parent} vs children {children}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "marginals disagree with their children:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn hydra_shard_merge_is_exactly_single_pass() {
    // Cell-wise merge of an additive counter is exact, so the comparison
    // carries no tolerance.
    const SHARDS: usize = 4;
    let stream = {
        let count = 24_000;
        let seed = 5_220;
        let src = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.8,
            seed,
        });
        let dst = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.5,
            seed: seed + 1,
        });
        let statuses = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: STATUSES.len(),
            exponent: 1.2,
            seed: seed + 2,
        });
        let endpoints = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: ENDPOINTS.len(),
            exponent: 0.4,
            seed: seed + 3,
        });
        (0..count)
            .map(|i| Record {
                key: [
                    REGIONS[src[i] as usize],
                    REGIONS[dst[i] as usize],
                    STATUSES[statuses[i] as usize],
                ],
                endpoint: ENDPOINTS[endpoints[i] as usize],
            })
            .collect::<Vec<_>>()
    };

    let mut single = lattice_hydra(2048);
    ingest(&mut single, &stream);

    let mut shards: Vec<Hydra> = (0..SHARDS).map(|_| lattice_hydra(2048)).collect();
    for (i, rec) in stream.iter().enumerate() {
        shards[i % SHARDS]
            .update(&rec.key, &DataInput::Str(rec.endpoint), None)
            .expect("arity 3");
    }
    let mut merged = shards.remove(0);
    for shard in &shards {
        merged.merge(shard).expect("identical dims and schema");
    }

    let truth = lattice_truth(&stream);
    let mut compared = 0usize;
    for (key, endpoint) in truth.keys() {
        assert_eq!(
            freq(&single, key, endpoint),
            freq(&merged, key, endpoint),
            "{key:?} x {endpoint}: merged shards diverged from the single pass"
        );
        compared += 1;
    }
    assert!(compared > 200, "lattice too sparse to be meaningful");
}

/// Two columns over one value domain: `{a = x}` and `{b = x}` are distinct
/// subpopulations that share a value.
#[test]
fn hydra_columns_sharing_a_value_do_not_alias() {
    let mut hydra = Hydra::with_schema(5, 1024, ["a", "b"], cell_cm()).expect("two-column schema");
    for (key, times) in [(["x", "y"], 300), (["y", "x"], 120)] {
        for _ in 0..times {
            hydra
                .update(&key, &DataInput::Str(MEASURE), None)
                .expect("arity 2");
        }
    }

    assert_eq!(freq(&hydra, &[Some("x"), None], MEASURE), 300.0);
    assert_eq!(freq(&hydra, &[None, Some("x")], MEASURE), 120.0);
    assert_eq!(freq(&hydra, &[Some("y"), None], MEASURE), 120.0);
    assert_eq!(freq(&hydra, &[None, Some("y")], MEASURE), 300.0);
}

// ---------------------------------------------------------------------------
// Hydra: wire round trips
// ---------------------------------------------------------------------------

fn round_trip(hydra: &Hydra) -> Hydra {
    let bytes = hydra
        .serialize_to_bytes()
        .expect("serialize to MessagePack");
    assert!(!bytes.is_empty());
    Hydra::deserialize_from_bytes(&bytes).expect("deserialize from MessagePack")
}

/// Answers a decoded Hydra must reproduce exactly, one query set per counter
/// family.
fn probe_answers(hydra: &Hydra, queries: &[(&[Option<&str>], HydraQuery)]) -> Vec<f64> {
    queries
        .iter()
        .map(|(key, q)| hydra.query_key(key, q).expect("supported query"))
        .collect()
}

#[test]
fn hydra_serde_round_trip_preserves_answers_for_every_counter() {
    let keys: [[&str; 2]; 3] = [["eu", "auth"], ["eu", "cart"], ["us", "auth"]];

    // --- Count-Min and Count Sketch: frequency over a small value domain.
    for (name, counter) in [("CM", cell_cm()), ("CS", cell_cs())] {
        let mut hydra =
            Hydra::with_schema(3, 512, ["region", "service"], counter).expect("two-column schema");
        for (i, key) in keys.iter().enumerate() {
            for j in 0..(40 * (i + 1)) {
                hydra
                    .update(key, &DataInput::Str(ENDPOINTS[j % ENDPOINTS.len()]), None)
                    .expect("arity 2");
            }
        }
        let probes: Vec<(&[Option<&str>], HydraQuery)> = vec![
            (
                &[Some("eu"), Some("auth")],
                HydraQuery::Frequency(DataInput::Str("/login")),
            ),
            (
                &[Some("eu"), None],
                HydraQuery::Frequency(DataInput::Str("/checkout")),
            ),
            (
                &[None, Some("auth")],
                HydraQuery::Frequency(DataInput::Str("/query")),
            ),
        ];
        let decoded = round_trip(&hydra);
        assert_eq!(decoded.schema(), hydra.schema(), "{name}: schema lost");
        assert_eq!(
            probe_answers(&decoded, &probes),
            probe_answers(&hydra, &probes),
            "{name}: answers changed across the wire"
        );
    }

    // --- HyperLogLog: cardinality per subpopulation.
    let mut hll = Hydra::with_schema(
        3,
        64,
        ["region", "service"],
        HydraCounter::HLL(Default::default()),
    )
    .expect("two-column schema");
    for key in &keys {
        for i in 0..2_000u32 {
            hll.update(key, &DataInput::U32(i), None).expect("arity 2");
        }
    }
    let hll_probes: Vec<(&[Option<&str>], HydraQuery)> = vec![
        (&[Some("eu"), Some("auth")], HydraQuery::Cardinality),
        (&[Some("eu"), None], HydraQuery::Cardinality),
    ];
    let decoded_hll = round_trip(&hll);
    assert_eq!(
        probe_answers(&decoded_hll, &hll_probes),
        probe_answers(&hll, &hll_probes),
        "HLL: answers changed across the wire"
    );

    // --- KLL: quantile and CDF per subpopulation.
    let mut kll = Hydra::with_schema(
        3,
        64,
        ["region", "service"],
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 5_301)),
    )
    .expect("two-column schema");
    for (i, v) in uniform_u64(6_000, 100_000, 5_302).iter().enumerate() {
        kll.update(&keys[i % keys.len()], &DataInput::F64(*v as f64), None)
            .expect("arity 2");
    }
    let kll_probes: Vec<(&[Option<&str>], HydraQuery)> = vec![
        (&[Some("eu"), Some("auth")], HydraQuery::Quantile(0.5)),
        (&[Some("eu"), None], HydraQuery::Quantile(0.9)),
        (&[Some("eu"), None], HydraQuery::Cdf(50_000.0)),
    ];
    let decoded_kll = round_trip(&kll);
    assert_eq!(
        probe_answers(&decoded_kll, &kll_probes),
        probe_answers(&kll, &kll_probes),
        "KLL: answers changed across the wire"
    );

    // --- UnivMon: the universal-metric head.
    let mut um = Hydra::with_schema(
        3,
        16,
        ["region", "service"],
        HydraCounter::UNIVERSAL(UnivMon::init_univmon(16, 3, 256, 4)),
    )
    .expect("two-column schema");
    for (i, k) in zipf_u64(6_000, 400, 1.2, 5_303).iter().enumerate() {
        um.update(&keys[i % keys.len()], &DataInput::U32(*k as u32), None)
            .expect("arity 2");
    }
    let um_probes: Vec<(&[Option<&str>], HydraQuery)> = vec![
        (&[Some("eu"), Some("auth")], HydraQuery::L1Norm),
        (&[Some("eu"), Some("auth")], HydraQuery::L2Norm),
        (&[Some("eu"), None], HydraQuery::Entropy),
        (&[Some("eu"), None], HydraQuery::Cardinality),
    ];
    let decoded_um = round_trip(&um);
    assert_eq!(
        probe_answers(&decoded_um, &um_probes),
        probe_answers(&um, &um_probes),
        "UnivMon: answers changed across the wire"
    );
}

// ---------------------------------------------------------------------------
// Hydra: error surface
// ---------------------------------------------------------------------------

#[test]
fn hydra_rejects_malformed_keys_and_queries() {
    let mut hydra = lattice_hydra(256);
    hydra
        .update(&["eu-west", "auth", "200"], &DataInput::Str(MEASURE), None)
        .expect("arity 3");

    // Keys are positional and full width; a short or long key is an arity
    // error. The query below is one the counter supports, so only arity can
    // make it fail.
    let hit = HydraQuery::Frequency(DataInput::Str(MEASURE));
    assert!(
        hydra
            .update(&["eu-west", "us-east"], &DataInput::Str(MEASURE), None)
            .is_err()
    );
    assert!(
        hydra
            .update(
                &["eu-west", "us-east", "200", "extra"],
                &DataInput::Str(MEASURE),
                None
            )
            .is_err()
    );
    assert!(hydra.query_key(&[Some("eu-west")], &hit).is_err());
    assert!(
        hydra
            .query_key(&[Some("eu-west"), None, None, None], &hit)
            .is_err()
    );
    // An all-wildcard query names no subpopulation.
    assert!(hydra.query_key(&[None, None, None], &hit).is_err());
    // Counter/query mismatches surface as errors, not zeros.
    assert!(
        hydra
            .query_key(&[Some("eu-west"), None, None], &HydraQuery::Quantile(0.5))
            .is_err()
    );
    // Merges only join identically shaped grids.
    let other =
        Hydra::with_schema(5, 256, ["region", "service", "code"], cell_cm()).expect("schema");
    assert!(hydra.merge(&other).is_err(), "schema labels must match");
    let wider = lattice_hydra(512);
    assert!(hydra.merge(&wider).is_err(), "dimensions must match");

    // An empty subpopulation answers zero; only malformed queries error.
    let empty = freq(&hydra, &[Some("nowhere"), None, None], MEASURE);
    assert_eq!(empty, 0.0);
    assert_between(
        freq(
            &hydra,
            &[Some("eu-west"), Some("auth"), Some("200")],
            MEASURE,
        ),
        1.0,
        1.0,
        "the single ingested record",
    );
}

// ---------------------------------------------------------------------------
// Hydra: Theorem 2 under load
// ---------------------------------------------------------------------------

/// Exact probability that a *median* of `rows` row-estimates violates the
/// bound at per-row failure probability `p_row`: a strict majority must fail.
fn median_failure_probability(rows: usize, p_row: f64) -> f64 {
    let need = rows / 2 + 1;
    (need..=rows)
        .map(|k| {
            let mut binom = 1.0_f64;
            for t in 0..k {
                binom = binom * (rows - t) as f64 / (t + 1) as f64;
            }
            binom * p_row.powi(k as i32) * (1.0 - p_row).powi((rows - k) as i32)
        })
        .sum()
}

const BOUND_REGIONS: usize = 8;
const BOUND_DEVICES: usize = 6;
const BOUND_OSES: usize = 4;
const BOUND_N: usize = 120_000;

/// Manousis et al., VLDB 2022, Theorem 2: over `w = O(1/eps)` columns and `r`
/// rows combined by median, every subpopulation satisfies, with probability
/// `1 - delta`, `G_i*(1-eps_us) <= Ghat_i <= G_i*(1+eps_us) + eps*G_s`.
fn hydra_additive_bound_config(rows: usize, cols: usize) {
    const D: usize = 3;
    const FANOUT: usize = (1 << D) - 1;
    let combos = BOUND_REGIONS * BOUND_DEVICES * BOUND_OSES;

    let regions: Vec<String> = (0..BOUND_REGIONS).map(|i| format!("region-{i}")).collect();
    let devices: Vec<String> = (0..BOUND_DEVICES).map(|i| format!("device-{i}")).collect();
    let oses: Vec<String> = (0..BOUND_OSES).map(|i| format!("os-{i}")).collect();

    let mut hydra =
        Hydra::with_schema(rows, cols, ["region", "device", "os"], cell_cm()).expect("schema");
    let measure = DataInput::Str(MEASURE);
    let mut truth: HashMap<[usize; D], u64> = HashMap::new();

    for combo in zipf_u64(BOUND_N, combos, 1.1, 4_501) {
        let combo = combo as usize;
        let idx = [
            combo % BOUND_REGIONS,
            (combo / BOUND_REGIONS) % BOUND_DEVICES,
            combo / (BOUND_REGIONS * BOUND_DEVICES),
        ];
        hydra
            .update(
                &[
                    regions[idx[0]].as_str(),
                    devices[idx[1]].as_str(),
                    oses[idx[2]].as_str(),
                ],
                &measure,
                None,
            )
            .expect("arity 3");
        for mask in 1..=FANOUT {
            let mut projected = [usize::MAX; D];
            for (col, slot) in projected.iter_mut().enumerate() {
                if (mask >> col) & 1 == 1 {
                    *slot = idx[col];
                }
            }
            *truth.entry(projected).or_insert(0) += 1;
        }
    }

    let g_s = (BOUND_N * FANOUT) as f64;
    assert_eq!(
        truth.values().sum::<u64>(),
        (BOUND_N * FANOUT) as u64,
        "ground truth must account for exactly the mass written to the grid"
    );

    let epsilon = 4.0 / cols as f64;
    let p_row = 1.0 / (epsilon * cols as f64);
    let delta = median_failure_probability(rows, p_row);
    let error_bound = epsilon * g_s;

    let pick = |slot: usize, values: &Vec<String>| -> Option<String> {
        (slot != usize::MAX).then(|| values[slot].clone())
    };

    let mut within = 0usize;
    let mut max_over = 0.0f64;
    let mut sum_over = 0.0f64;
    for (projected, &g_i) in &truth {
        let owned = [
            pick(projected[0], &regions),
            pick(projected[1], &devices),
            pick(projected[2], &oses),
        ];
        let key: Vec<Option<&str>> = owned.iter().map(|v| v.as_deref()).collect();
        let est = hydra
            .query_key(&key, &HydraQuery::Frequency(measure.clone()))
            .expect("well-formed query");

        assert!(
            est >= g_i as f64,
            "lower bound violated for {key:?}: est {est} < truth {g_i} \
             (with eps_us = 0 an estimate can only over-count)"
        );
        let over = est - g_i as f64;
        max_over = max_over.max(over);
        sum_over += over;
        if over <= error_bound {
            within += 1;
        }
    }

    let total = truth.len();
    let required = total as f64 * (1.0 - delta);
    eprintln!(
        "[hydra additive bound] rows={rows} cols={cols} G_s={g_s} subpops={total} \
         eps={epsilon:.6} bound={error_bound:.1} | within={within}/{total} \
         (required>{required:.1}) max_overshoot={max_over:.1} mean_overshoot={:.1} \
         delta={delta:.6}",
        sum_over / total as f64
    );
    assert!(
        within as f64 > required,
        "in-bound subpopulations {within} of {total} not above {required} \
         (rows={rows}, cols={cols}, eps={epsilon}, delta={delta}, \
         bound={error_bound}, max_overshoot={max_over})"
    );
}

#[test]
fn hydra_subpopulation_error_stays_within_the_additive_grid_bound() {
    // 5x4096 is the sparse deployment regime; 5x256 puts 840k units of
    // post-fan-out mass over 256 cells.
    hydra_additive_bound_config(5, 4096);
    hydra_additive_bound_config(5, 256);
}

// ---------------------------------------------------------------------------
// Hydra: every counter family on a multi-column subpopulation lattice
// ---------------------------------------------------------------------------

#[test]
fn hydra_cs_head_subpopulation_frequencies() {
    // Count Sketch carries signed per-cell noise, so the band is symmetric.
    let stream = {
        let count = 30_000;
        let seed = 4_300;
        let src = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.8,
            seed,
        });
        let dst = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: REGIONS.len(),
            exponent: 0.5,
            seed: seed + 1,
        });
        let statuses = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: STATUSES.len(),
            exponent: 1.2,
            seed: seed + 2,
        });
        let endpoints = ZipfGenerator::generate::<u64>(&ZipfConfig {
            count,
            domain: ENDPOINTS.len(),
            exponent: 0.4,
            seed: seed + 3,
        });
        (0..count)
            .map(|i| Record {
                key: [
                    REGIONS[src[i] as usize],
                    REGIONS[dst[i] as usize],
                    STATUSES[statuses[i] as usize],
                ],
                endpoint: ENDPOINTS[endpoints[i] as usize],
            })
            .collect::<Vec<_>>()
    };
    let truth = lattice_truth(&stream);
    let mut hydra = Hydra::with_schema(5, 4096, SCHEMA, cell_cs()).expect("three-column schema");
    ingest(&mut hydra, &stream);

    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for ((key, endpoint), count) in &truth {
        if *count < 25 {
            continue;
        }
        checked += 1;
        let est = freq(&hydra, key, endpoint);
        let slack = *count as f64 * 0.06 + 25.0; // the kit's Count spec
        if (est - *count as f64).abs() > slack {
            failures.push(format!(
                "{key:?} x {endpoint}: true {count}, est {est} (slack {slack:.1})"
            ));
        }
    }
    failures.sort();
    assert!(checked > 200, "lattice too sparse to be meaningful");
    assert!(
        failures.is_empty(),
        "{} of {checked} dense subpopulations out of band:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

fn h2_masks(region: &'static str, service: &'static str) -> [[Option<&'static str>; 2]; 3] {
    [
        [Some(region), None],
        [None, Some(service)],
        [Some(region), Some(service)],
    ]
}

/// Every subpopulation of the 2x2 lattice, in `[region, service]` form.
fn h2_lattice() -> Vec<[Option<&'static str>; 2]> {
    let mut keys: Vec<[Option<&'static str>; 2]> = Vec::new();
    for r in H2_REGIONS {
        keys.push([Some(r), None]);
    }
    for sv in H2_SERVICES {
        keys.push([None, Some(sv)]);
    }
    for r in H2_REGIONS {
        for sv in H2_SERVICES {
            keys.push([Some(r), Some(sv)]);
        }
    }
    keys
}

fn h2_matches(key: &[Option<&str>; 2], region: &str, service: &str) -> bool {
    key[0].is_none_or(|v| v == region) && key[1].is_none_or(|v| v == service)
}

/// Drives one counter family through the 2x2 lattice, requiring each
/// subpopulation's answer to equal a standalone counter over its own records.
/// Both sides share a query implementation, so this constrains routing only.
fn assert_head_routes_to_the_right_cell<F>(
    label: &str,
    counter: HydraCounter,
    n: usize,
    seed: u64,
    value: F,
    queries: &[HydraQuery],
) where
    F: Fn(usize) -> DataInput<'static>,
{
    let keys = h2_keys(n, seed);
    let mut hydra = Hydra::with_schema(5, 256, ["region", "service"], counter.clone())
        .expect("two-column schema");
    let lattice = h2_lattice();
    let mut reference: Vec<HydraCounter> = lattice.iter().map(|_| counter.clone()).collect();

    for (i, (region, service)) in keys.iter().enumerate() {
        let v = value(i);
        hydra.update(&[region, service], &v, None).expect("arity 2");
        for (slot, key) in lattice.iter().enumerate() {
            if h2_matches(key, region, service) {
                reference[slot].insert(&v, None);
            }
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (slot, key) in lattice.iter().enumerate() {
        for query in queries {
            let through_grid = hydra.query_key(key, query).expect("supported query");
            let standalone = reference[slot].query(query).expect("supported query");
            if through_grid != standalone {
                failures.push(format!(
                    "{key:?} {query}: grid {through_grid} vs standalone {standalone}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{label} head misroutes subpopulations:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn hydra_cs_head_routes_records_to_the_right_cell() {
    let values = zipf_u64(30_000, ENDPOINTS.len(), 0.5, 4_615);
    assert_head_routes_to_the_right_cell(
        "CS",
        cell_cs(),
        30_000,
        4_612,
        move |i| DataInput::Str(ENDPOINTS[values[i] as usize]),
        &[HydraQuery::Frequency(DataInput::Str(ENDPOINTS[0]))],
    );
}

#[test]
fn hydra_hll_head_routes_records_to_the_right_cell() {
    assert_head_routes_to_the_right_cell(
        "HLL",
        HydraCounter::HLL(Default::default()),
        30_000,
        4_610,
        |i| DataInput::U32((i as u32).wrapping_mul(2_654_435_761) % 12_000),
        &[HydraQuery::Cardinality],
    );
}

#[test]
fn hydra_kll_head_routes_records_to_the_right_cell() {
    let values = normal_f64(30_000, 500.0, 80.0, 4_640);
    assert_head_routes_to_the_right_cell(
        "KLL",
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 4_641)),
        30_000,
        4_630,
        move |i| DataInput::F64(values[i]),
        &[HydraQuery::Quantile(0.5)],
    );
}

#[test]
fn hydra_univmon_head_routes_records_to_the_right_cell() {
    let items = zipf_u64(30_000, 1000, 1.2, 4_660);
    assert_head_routes_to_the_right_cell(
        "UnivMon",
        HydraCounter::UNIVERSAL(UnivMon::init_univmon(32, 5, 256, 8)),
        30_000,
        4_650,
        move |i| DataInput::U32(items[i] as u32),
        &[HydraQuery::L1Norm, HydraQuery::Cardinality],
    );
}

/// L1 is a weighted record count, preserved exactly by the fan-out and the
/// per-cell UnivMon.
/// UnivMon cells answer L1, L2 and entropy per subpopulation across the
/// lattice, over a weighted stream.
#[test]
fn hydra_univmon_head_subpopulation_metrics() {
    let n = 40_000usize;
    let keys = h2_keys(n, 4_770);
    let items = zipf_u64(n, 1000, 1.2, 4_775);
    let mut hydra = Hydra::with_schema(
        5,
        128,
        ["region", "service"],
        HydraCounter::UNIVERSAL(UnivMon::init_univmon(32, 5, 256, 8)),
    )
    .expect("two-column schema");
    let mut truth: HashMap<Vec<Option<&str>>, FreqTruth> = HashMap::new();

    for (i, (region, service)) in keys.iter().enumerate() {
        let weight = 1 + (i % 7) as i64;
        hydra
            .update(
                &[region, service],
                &DataInput::U32(items[i] as u32),
                Some(weight as i32),
            )
            .expect("arity 2");
        for key in h2_masks(region, service) {
            truth
                .entry(key.to_vec())
                .or_default()
                .observe_weighted(items[i] as i64, weight);
        }
    }

    let mut failures: Vec<String> = Vec::new();
    for (key, exact) in &truth {
        let l1 = hydra.query_key(key, &HydraQuery::L1Norm).expect("L1");
        if l1 != exact.total() as f64 {
            failures.push(format!("{key:?}: L1 {l1} != exact {}", exact.total()));
        }
        // Bands from `univmon_weighted_metrics_and_fast_insert_parity`.
        for (label, est, want, tol) in [
            (
                "L2",
                hydra.query_key(key, &HydraQuery::L2Norm).expect("L2"),
                exact.l2_norm(),
                0.05,
            ),
            (
                "entropy",
                hydra.query_key(key, &HydraQuery::Entropy).expect("entropy"),
                exact.entropy(true),
                0.12,
            ),
        ] {
            if est < want * (1.0 - tol) || est > want * (1.0 + tol) {
                failures.push(format!(
                    "{key:?}: {label} {est:.3} vs exact {want:.3} (tol {:.0}%)",
                    tol * 100.0
                ));
            }
        }
    }
    failures.sort();
    assert_eq!(truth.len(), 8, "the 2^2-1 lattice over 2x2 domains");
    assert!(
        failures.is_empty(),
        "UnivMon subpopulation metrics out of band:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn hydra_univmon_head_l1_is_exact_per_subpopulation() {
    let n = 30_000usize;
    let keys = h2_keys(n, 4_650);
    let items = zipf_u64(n, 1000, 1.2, 4_660);
    let mut hydra = Hydra::with_schema(
        5,
        256,
        ["region", "service"],
        HydraCounter::UNIVERSAL(UnivMon::init_univmon(32, 5, 256, 8)),
    )
    .expect("two-column schema");
    let mut truth: HashMap<Vec<Option<&str>>, FreqTruth> = HashMap::new();

    for (i, (region, service)) in keys.iter().enumerate() {
        let weight = 1 + (i % 7) as i64;
        hydra
            .update(
                &[region, service],
                &DataInput::U32(items[i] as u32),
                Some(weight as i32),
            )
            .expect("arity 2");
        for key in h2_masks(region, service) {
            truth
                .entry(key.to_vec())
                .or_default()
                .observe_weighted(items[i] as i64, weight);
        }
    }

    assert_eq!(truth.len(), 8, "the 2^2-1 lattice over 2x2 domains");
    for (key, exact) in &truth {
        assert_eq!(
            hydra.query_key(key, &HydraQuery::L1Norm).expect("L1"),
            exact.total() as f64,
            "{key:?}: weighted L1 must survive the fan-out exactly"
        );
    }
}

#[test]
fn hydra_shard_merge_preserves_answers_for_every_counter() {
    // CM is covered over the full D=3 lattice by
    // `hydra_shard_merge_is_exactly_single_pass`; this is the other four.
    let n = 24_000usize;
    let keys = h2_keys(n, 4_670);
    let probe: Vec<[Option<&str>; 2]> = h2_masks("eu-west", "auth").to_vec();
    // Shard assignment is drawn independently of the value, so both shards see
    // every value and the merge has to combine two non-zero counters.
    let shards = uniform_u64(n, 2, 4_675);

    let run = |counter: HydraCounter, value: &dyn Fn(usize) -> DataInput<'static>| {
        let mut single = Hydra::with_schema(5, 256, ["region", "service"], counter.clone())
            .expect("two-column schema");
        let mut left = Hydra::with_schema(5, 256, ["region", "service"], counter.clone())
            .expect("two-column schema");
        let mut right =
            Hydra::with_schema(5, 256, ["region", "service"], counter).expect("two-column schema");
        for (i, (region, service)) in keys.iter().enumerate() {
            let v = value(i);
            single
                .update(&[region, service], &v, None)
                .expect("arity 2");
            let shard = if shards[i] == 0 {
                &mut left
            } else {
                &mut right
            };
            shard.update(&[region, service], &v, None).expect("arity 2");
        }
        left.merge(&right).expect("identical dims and schema");
        (single, left)
    };

    // CS is additive: cell-wise merge reproduces the single pass exactly.
    let cs_values = zipf_u64(n, ENDPOINTS.len(), 0.5, 4_676);
    let (single, merged) = run(cell_cs(), &|i| {
        DataInput::Str(ENDPOINTS[cs_values[i] as usize])
    });
    for key in &probe {
        for ep in ENDPOINTS {
            assert_eq!(
                freq(&single, key, ep),
                freq(&merged, key, ep),
                "CS merge diverged at {key:?} x {ep}"
            );
        }
    }

    // HLL merges register-wise by max, so likewise exact.
    let hll_values = uniform_u64(n, 9_000, 4_677);
    let (single, merged) = run(HydraCounter::HLL(Default::default()), &|i| {
        DataInput::U32(hll_values[i] as u32)
    });
    for key in &probe {
        assert_eq!(
            single
                .query_key(key, &HydraQuery::Cardinality)
                .expect("cardinality"),
            merged
                .query_key(key, &HydraQuery::Cardinality)
                .expect("cardinality"),
            "HLL merge diverged at {key:?}"
        );
    }

    // KLL compaction is randomized, so the merged sketch is held to the rank
    // band around exact truth. The two shards draw from separated modes, so a
    // merge that keeps only one side lands outside every band.
    let low = normal_f64(n, 300.0, 20.0, 4_680);
    let high = normal_f64(n, 700.0, 20.0, 4_685);
    let kll_values: Vec<f64> = (0..n)
        .map(|i| if shards[i] == 0 { low[i] } else { high[i] })
        .collect();
    let (_, merged) = run(
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 4_681)),
        &|i| DataInput::F64(kll_values[i]),
    );
    let mut region_values: Vec<f64> = Vec::new();
    for (i, (region, _)) in keys.iter().enumerate() {
        if *region == "eu-west" {
            region_values.push(kll_values[i]);
        }
    }
    let exact = common::NumericTruth::new(region_values);
    for q in DEFAULT_QUANTILE_QS {
        let est = merged
            .query_key(&[Some("eu-west"), None], &HydraQuery::Quantile(q))
            .expect("quantile");
        let (lo, hi) = exact.quantile_band(q, KllRankSpec::datasketches(HYDRA_KLL_K).epsilon());
        assert!(
            est >= lo && est <= hi,
            "merged KLL q={q}: {est:.3} outside [{lo:.3}, {hi:.3}]"
        );
    }

    // UnivMon L1 is a weighted record count, exact through the merge and equal
    // to the single pass.
    let um_items = zipf_u64(n, 800, 1.2, 4_690);
    let (single, merged) = run(
        HydraCounter::UNIVERSAL(UnivMon::init_univmon(32, 5, 256, 8)),
        &|i| DataInput::U32(um_items[i] as u32),
    );
    let mut region_truth = FreqTruth::default();
    for (i, (region, _)) in keys.iter().enumerate() {
        if *region == "eu-west" {
            region_truth.observe(um_items[i] as i64);
        }
    }
    let key = [Some("eu-west"), None];
    let merged_l1 = merged.query_key(&key, &HydraQuery::L1Norm).expect("L1");
    assert_eq!(
        merged_l1,
        region_truth.total() as f64,
        "merged UnivMon L1 must stay exact"
    );
    assert_eq!(
        merged_l1,
        single.query_key(&key, &HydraQuery::L1Norm).expect("L1"),
        "merged UnivMon L1 must equal the single pass"
    );
    // `L2Norm` is `sqrt(F2_hat)` over the sketch's own counters, so the band
    // is the AMS row-median bound on F2 carried through the square root:
    // `|F2_hat/F2 - 1| <= b` gives `|L2_hat/L2 - 1| <= sqrt(1 + b) - 1` above
    // and `1 - sqrt(1 - b)` below. Derived from the cell's own (rows, cols),
    // never a flat 5%.
    let l2 = merged.query_key(&key, &HydraQuery::L2Norm).expect("L2");
    let (lo, hi) = univmon_l2_band(region_truth.l2_norm(), 5, 256);
    assert_between(l2, lo, hi, "merged UnivMon L2");
}

// ----------------------------------------------------------------- UnivMon

#[test]
fn univmon_weighted_metrics_and_fast_insert_parity() {
    let build = || UnivMon::init_univmon(32, 5, 2048, 8);
    let mut um = build();
    let mut fast = build();
    let mut truth = common::FreqTruth::default();

    let stream = zipf_u64(20_000, 1000, 1.2, 4003);
    for (i, k) in stream.iter().enumerate() {
        let w = 1 + (i % 7) as i64;
        um.insert(&DataInput::U32(*k as u32), w);
        fast.fast_insert(&DataInput::U32(*k as u32), w);
        truth.observe_weighted(*k as i64, w);
    }

    let total = truth.total();
    assert_eq!(um.calc_l1(), total as f64, "L1 must be exact");
    assert_eq!(fast.calc_l1(), total as f64, "fast-insert L1 must be exact");

    let l2_truth = truth.l2_norm();
    let (lo, hi) = univmon_l2_band(l2_truth, 5, 2048);
    assert_between(um.calc_l2(), lo, hi, "L2");
    let h_truth = truth.entropy(true);
    // UnivMon entropy is a ~10%-accurate estimator (repo tests allow 15%).
    assert_between(
        um.calc_entropy(),
        h_truth * 0.88,
        h_truth * 1.12,
        "entropy (bits)",
    );
    let card_truth = truth.distinct() as f64;
    assert_between(
        um.calc_card(),
        card_truth * 0.94,
        card_truth * 1.06,
        "cardinality",
    );

    // fast_insert path tracks the standard estimator within loose bounds.
    assert_between(
        fast.calc_l2(),
        l2_truth * 0.85,
        l2_truth * 1.15,
        "fast L2 parity",
    );
}

#[test]
fn univmon_pyramid_weighted_metrics() {
    let mut up = UnivMonPyramid::with_defaults();
    let mut truth = common::FreqTruth::default();
    let stream = zipf_u64(15_000, 800, 1.3, 4004);
    for (i, k) in stream.iter().enumerate() {
        let w = 1 + (i % 3) as i64;
        up.insert(&DataInput::U32(*k as u32), w);
        truth.observe_weighted(*k as i64, w);
    }
    assert_eq!(
        up.calc_l1(),
        truth.total() as f64,
        "pyramid L1 must be exact"
    );
    let l2_truth = truth.l2_norm();
    assert_between(up.calc_l2(), l2_truth * 0.85, l2_truth * 1.15, "pyramid L2");
    let card_truth = truth.distinct() as f64;
    assert_between(
        up.calc_card(),
        card_truth * 0.80,
        card_truth * 1.20,
        "pyramid cardinality",
    );
}

// ------------------------------------------------------------------- Nitro
//
// `NitroBatch` moved to `tests/e2e_composition.rs`. The version that lived
// here built its batches with `NitroBatch::with_target`, which seeds the
// sampling RNG from the OS — so its +-5% and +-10% bands were re-rolled on
// every run and neither reproduced a failure nor derived from the estimator's
// variance. The replacement uses the seeded constructor and the binomial
// sampling band, across rates 1.0 / 0.5 / 0.1 / 0.01 and all three targets.

// -------------------------------------------------- ExponentialHistogram

#[test]
fn exponential_histogram_sliding_window_counts() {
    // Single event type: merged-bucket count is additive and one-sided.
    let window = 100u64;
    let eh_type = EHSketchList::CM(
        CountMin::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(3, 2048),
    );
    let mut eh = ExponentialHistogram::new(8, window, eh_type);

    for t in 0..500u64 {
        if t % 3 == 0 {
            eh.update(t, &DataInput::Str("req"));
        }
    }
    // Multiples of 3 in [400, 499]: 402..498 -> 33 events. Bucket-boundary
    // snapping makes interval merges approximate in both directions; allow
    // granularity slack around the exact figure.
    let merged = eh.query_interval_merge(400, 499).expect("interval covered");
    let est = merged.query(&DataInput::Str("req")).expect("CM query");
    assert!(
        (26.0..=40.0).contains(&est),
        "window count {est} outside [26, 40]"
    );

    // Window expiry retains only recent buckets (window=100): the retained
    // span must be covered, anything past the observed max must not be.
    let retained_min = eh.get_min_time().expect("buckets present");
    let retained_max = eh.get_max_time().expect("buckets present");
    assert_eq!(retained_max, 498);
    assert!(eh.cover(retained_min, 498), "must cover retained span");
    assert!(!eh.cover(500, 600), "cannot cover beyond observed span");
}

// ------------------------------------------------------------ TumblingWindow

#[test]
fn tumbling_foldcms_weighted_windows_exact_counts() {
    let cfg = FoldCMSConfig {
        rows: 3,
        full_cols: 2048,
        fold_level: 0,
        top_k: 32,
    };
    let mut tw: TumblingWindow<FoldCMS> = TumblingWindow::new(10, 16, cfg, 4);

    for t in 0..35u64 {
        tw.insert(t, &DataInput::Str("A"), 2);
        tw.insert(t, &DataInput::Str("B"), 1);
    }
    assert_eq!(
        tw.closed_count(),
        3,
        "windows [0,10) [10,20) [20,30) closed at t=34"
    );

    let all = tw.query_all();
    assert_eq!(all.query(&DataInput::Str("A")), 70);
    assert_eq!(all.query(&DataInput::Str("B")), 35);

    // Last two windows + active cover t in [10,35): 25 inserts.
    let recent = tw.query_recent(2);
    assert_eq!(recent.query(&DataInput::Str("A")), 50);
    assert_eq!(recent.query(&DataInput::Str("B")), 25);

    // flush() closes the active window unconditionally (even if partially
    // filled or empty), so [30,40) and an empty [40,50) both land in closed.
    tw.flush(40);
    assert_eq!(
        tw.closed_count(),
        5,
        "flush closes active + opens/closes empty"
    );
    let post = tw.query_all();
    assert_eq!(post.query(&DataInput::Str("A")), 70);
    assert_eq!(post.query(&DataInput::Str("B")), 35);
}

// ------------------------------------------------------ HashSketchEnsemble
//
// `HashSketchEnsemble` moved to `tests/e2e_composition.rs`, where every member
// variant (CountMinFast, CountFast, HllErtl, HllClassic, HllHip) is compared
// against a standalone reference and held to its own family's bound. The
// version that lived here covered two members with a hand-picked 3x upper
// slack on the Count-Min cell.

#[test]
fn hydra_query_frequency_is_the_frequency_query_it_wraps() {
    let mut hydra = Hydra::with_schema(
        4,
        4_096,
        ["region", "user"],
        HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
            4, 4_096,
        )),
    )
    .expect("schema");

    for r in ["eu", "us"] {
        for u in ["alice", "bob"] {
            for _ in 0..25 {
                hydra
                    .update(&[r, u], &DataInput::Str("event"), None)
                    .expect("update");
            }
        }
    }

    let probe = DataInput::Str("event");
    for key in [
        vec![Some("eu"), Some("alice")],
        vec![Some("us"), Some("bob")],
        vec![Some("eu"), None],
        vec![None, Some("bob")],
        vec![Some("apac"), None],
    ] {
        let wrapper = hydra
            .query_frequency(&key, &probe)
            .expect("frequency convenience query");
        let explicit = hydra
            .query_key(&key, &HydraQuery::Frequency(probe.clone()))
            .expect("frequency query");
        assert_eq!(
            wrapper, explicit,
            "{key:?}: query_frequency disagreed with the query it wraps"
        );
    }

    assert!(
        hydra.query_frequency(&[Some("eu")], &probe).is_err(),
        "a key shorter than the schema arity must be an error"
    );
    assert_eq!(
        hydra
            .query_frequency(&[Some("apac"), Some("nobody")], &probe)
            .expect("absent cell"),
        0.0,
        "an unseen subpopulation carries no frequency"
    );
}

#[test]
fn hydra_query_quantile_is_the_cumulative_query_it_wraps() {
    let mut hydra = Hydra::with_schema(
        4,
        512,
        ["shard"],
        HydraCounter::KLL(KLL::init_kll_with_seed(HYDRA_KLL_K as i32, 0x5EED_0700)),
    )
    .expect("schema");
    let values: Vec<f64> = uniform_u64(20_000, 1_000_000, 4_101)
        .iter()
        .map(|v| *v as f64)
        .collect();
    let truth = common::NumericTruth::new(values.clone());
    for v in &values {
        hydra
            .update(&["s0"], &DataInput::F64(*v), None)
            .expect("update");
    }

    let kll_eps = KllRankSpec::datasketches(HYDRA_KLL_K).epsilon();
    for x in [100_000.0f64, 250_000.0, 500_000.0, 750_000.0, 900_000.0] {
        let wrapper = hydra
            .query_quantile(&[Some("s0")], x)
            .expect("quantile convenience query");
        let explicit = hydra
            .query_key(&[Some("s0")], &HydraQuery::Cdf(x))
            .expect("cdf query");
        assert_eq!(
            wrapper, explicit,
            "x={x}: query_quantile disagreed with the query it wraps"
        );
        assert_between(
            wrapper,
            truth.cdf(x) - kll_eps,
            truth.cdf(x) + kll_eps,
            &format!("Hydra query_quantile at x={x}"),
        );
    }

    assert_eq!(
        hydra
            .query_quantile(&[Some("ghost")], 500_000.0)
            .expect("absent cell"),
        0.0,
        "an unseen subpopulation reports no cumulative mass"
    );
    assert!(
        hydra.query_quantile(&[], 500_000.0).is_err(),
        "an empty key must be an arity error"
    );
}

#[test]
fn univmon_generic_g_sum_reproduces_every_named_estimator() {
    let mut um = UnivMon::init_univmon(32, 5, 2_048, 8);
    let stream = zipf_u64(20_000, 1_000, 1.2, 4_201);
    let mut truth = common::FreqTruth::default();
    for (i, k) in stream.iter().enumerate() {
        let w = 1 + (i % 7) as i64;
        um.insert(&DataInput::U32(*k as u32), w);
        truth.observe_weighted(*k as i64, w);
    }

    assert_eq!(
        um.calc_g_sum(|x| x * x, false).sqrt(),
        um.calc_l2(),
        "g(x)=x^2 under a square root is the L2 estimator"
    );
    assert_eq!(
        um.calc_g_sum(|_| 1.0, true),
        um.calc_card(),
        "g(x)=1 in cardinality mode is the distinct-count estimator"
    );
    let x_log_x = um.calc_g_sum(|x| if x > 0.0 { x * x.log2() } else { 0.0 }, false);
    assert_between(
        um.calc_entropy(),
        um.calc_l1().log2() - x_log_x / um.calc_l1() - 1e-9,
        um.calc_l1().log2() - x_log_x / um.calc_l1() + 1e-9,
        "the entropy estimator is the x*log2(x) g-sum rearranged",
    );
    assert_eq!(
        um.calc_g_sum(|x| x * x, false),
        um.calc_g_sum_heuristic(|x| x * x, false),
        "the public g-sum must be the heuristic it delegates to"
    );
    assert_eq!(
        um.calc_g_sum(|_| 0.0, false),
        0.0,
        "the zero function must sum to zero"
    );

    let identity = um.calc_g_sum(|x| x, false);
    let l1 = truth.total() as f64;
    assert_between(
        identity,
        l1 * 0.50,
        l1 * 1.50,
        "UnivMon g(x)=x tracks the exact L1",
    );
}

#[test]
fn univmon_g_sum_is_zero_on_an_empty_sketch() {
    let um = UnivMon::init_univmon(32, 5, 256, 4);
    assert_eq!(um.calc_l1(), 0.0, "an empty sketch has no mass");
    assert_eq!(um.calc_entropy(), 0.0, "an empty sketch has no entropy");
    assert_eq!(um.calc_g_sum(|x| x * x, false), 0.0, "no second moment");
    assert_eq!(um.calc_card(), 0.0, "no distinct values");
}

#[test]
fn a_univmon_pool_recycles_a_returned_sketch_and_hands_back_a_cleared_one() {
    const CAP: usize = 2;
    let mut pool = UnivSketchPool::new(CAP, 16, 3, 256, 4);
    assert_eq!(pool.available(), CAP, "a fresh pool holds its capacity");
    assert_eq!(pool.total_allocated(), CAP);

    let first = pool.take();
    let second = pool.take();
    assert_eq!(pool.available(), 0);
    assert_eq!(pool.total_allocated(), CAP);

    let third = pool.take();
    assert_eq!(
        pool.total_allocated(),
        CAP + 1,
        "an exhausted pool allocates rather than blocking"
    );

    let mut dirty = first;
    for k in zipf_u64(5_000, 512, 1.1, 4_202) {
        dirty.insert(&DataInput::U32(k as u32), 1);
    }
    assert!(dirty.calc_l1() > 0.0, "the fixture must carry mass");
    pool.put(dirty);
    assert_eq!(pool.available(), 1, "a returned sketch is available again");

    let recycled = pool.take();
    assert_eq!(
        recycled.calc_l1(),
        0.0,
        "a recycled sketch must come back cleared"
    );
    assert_eq!(
        pool.total_allocated(),
        CAP + 1,
        "taking a recycled sketch must not allocate"
    );
    drop((second, third, recycled));
}
