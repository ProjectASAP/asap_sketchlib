//! E2E composition-layer pipelines: Hydra multi-label queries across counter
//! types, MultiHeadHydra routing, UnivMon/UnivMonPyramid weighted metrics,
//! Nitro sampling, ExponentialHistogram sliding windows, and TumblingWindow
//! over FoldCMS.

mod common;

use common::{assert_between, zipf_u64};

use asap_sketchlib::input::{HydraCounter, HydraQuery};
use asap_sketchlib::sketch_framework::hydra::MultiHeadHydra;
use asap_sketchlib::{
    CountMin, DataInput, EHSketchList, ExponentialHistogram, FoldCMS, FoldCMSConfig, Hydra, KLL,
    TumblingWindow, UnivMon, UnivMonPyramid, Vector2D,
};

// ------------------------------------------------------------------- Hydra

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
    let mut hydra = Hydra::with_schema(4, 512, ["shard"], HydraCounter::KLL(KLL::init_kll(200)))
        .expect("schema");
    let values: Vec<f64> = common::uniform_u64(20_000, 1_000_000, 4001)
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
    assert_between(
        cdf,
        truth.cdf(x) - 0.03,
        truth.cdf(x) + 0.03,
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

#[test]
fn multihead_hydra_routes_values_to_named_heads() {
    let heads = vec![
        ("events".to_string(), HydraCounter::CM(Default::default())),
        ("latency".to_string(), HydraCounter::KLL(KLL::init_kll(200))),
    ];
    let mut mh = MultiHeadHydra::with_schema(4, 1024, ["svc"], heads).expect("schema");

    let latencies: Vec<f64> = common::uniform_u64(2000, 500, 4002)
        .iter()
        .map(|v| *v as f64)
        .collect();
    let latency_truth = common::NumericTruth::new(latencies.clone());
    for v in &latencies {
        mh.update(
            &["svc-a"],
            &[
                (&DataInput::F64(*v), &["latency"]),
                (&DataInput::Str("hit"), &["events"]),
            ],
            None,
        )
        .expect("update");
    }

    let freq = mh
        .query_key(
            &[Some("svc-a")],
            "events",
            &HydraQuery::Frequency(DataInput::Str("hit")),
        )
        .expect("freq");
    assert_between(freq, 1998.0, 2002.0, "events head frequency");

    let med = mh
        .query_key(&[Some("svc-a")], "latency", &HydraQuery::Quantile(0.5))
        .expect("med");
    assert_between(
        med,
        latency_truth.quantile(0.46),
        latency_truth.quantile(0.54),
        "latency head median",
    );

    // Unknown head name is an error, not a silent zero.
    assert!(
        mh.query_key(&[Some("svc-a")], "nope", &HydraQuery::Cardinality)
            .is_err()
    );
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
    assert_between(um.calc_l2(), l2_truth * 0.95, l2_truth * 1.05, "L2");
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

#[test]
fn nitro_unbiased_across_rates_cm_and_cs_targets() {
    let n = 100_000i64;
    for rate in [1.0f64, 0.5] {
        let mut cm_batch = asap_sketchlib::NitroBatch::with_target(
            rate,
            CountMin::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(5, 2048),
        );
        cm_batch.insert(&vec![42i64; n as usize]);
        let est = cm_batch.estimate_median(&DataInput::I64(42));
        assert_between(
            est,
            n as f64 * 0.95,
            n as f64 * 1.05,
            &format!("Nitro CM rate={rate}"),
        );

        let mut cs_batch = asap_sketchlib::NitroBatch::with_target(
            rate,
            asap_sketchlib::Count::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(
                5, 2048,
            ),
        );
        cs_batch.insert(&vec![42i64; n as usize]);
        let cs_est = cs_batch.estimate_median(&DataInput::I64(42));
        assert_between(
            cs_est,
            n as f64 * 0.90,
            n as f64 * 1.10,
            &format!("Nitro CS rate={rate}"),
        );
    }
}

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
