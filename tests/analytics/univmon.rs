//! UnivMon, UnivMonTwoTier and UnivMonQ.

use crate::support::*;
use asap_sketchlib::analytics::{
    FlowData, TwoTierParams, UnivMonParams, UnivMonQData, max_row_failure, p_med,
};
use asap_sketchlib::{
    DataInput, ErrorBound, ErrorMetric, SketchAnalytics, StatisticKind, UnivMon, UnivMonQ,
    UnivMonQConfig, UnivMonTwoTier,
};
use rand::Rng;
use rand::rngs::StdRng;

fn key(id: u64) -> DataInput<'static> {
    DataInput::U64(id)
}

fn distinct(d: u64) -> FlowData {
    FlowData {
        distinct: Some(d),
        key_bytes: None,
        merged: None,
    }
}

/// One pyramid of the family, driven the same way by every test.
trait Pyramid: SketchAnalytics<Data = FlowData> + Clone {
    fn build(params: &Self::Params) -> Self;
    fn add(&mut self, id: u64, weight: i64);
    fn l1(&self) -> f64;
    fn l2(&self) -> f64;
    fn cardinality(&self) -> f64;
    #[cfg_attr(not(feature = "op-count"), allow(dead_code))]
    fn entropy(&self) -> f64;
    #[cfg_attr(not(feature = "op-count"), allow(dead_code))]
    fn f2_sum(&self) -> f64;
    fn top_complete(&self) -> bool;
    fn merge(&mut self, other: &Self);
}

impl Pyramid for UnivMon {
    fn build(p: &UnivMonParams) -> Self {
        UnivMon::new(p.heap_size, p.rows, p.cols, p.layers)
    }
    fn add(&mut self, id: u64, weight: i64) {
        self.update_weighted(&key(id), weight);
    }
    fn l1(&self) -> f64 {
        self.estimate_l1()
    }
    fn l2(&self) -> f64 {
        self.estimate_l2()
    }
    fn cardinality(&self) -> f64 {
        self.estimate_cardinality()
    }
    fn entropy(&self) -> f64 {
        self.estimate_entropy()
    }
    fn f2_sum(&self) -> f64 {
        self.estimate_g_sum(|x| x * x, false)
    }
    fn top_complete(&self) -> bool {
        self.top_layer_complete()
    }
    fn merge(&mut self, other: &Self) {
        UnivMon::merge(self, other);
    }
}

impl Pyramid for UnivMonTwoTier {
    fn build(p: &TwoTierParams) -> Self {
        UnivMonTwoTier::new(
            p.heap_size,
            p.elephant_layers,
            p.elephant_rows,
            p.elephant_cols,
            p.mouse_rows,
            p.mouse_cols,
            p.layers,
        )
    }
    fn add(&mut self, id: u64, weight: i64) {
        self.update_weighted(&key(id), weight);
    }
    fn l1(&self) -> f64 {
        self.estimate_l1()
    }
    fn l2(&self) -> f64 {
        self.estimate_l2()
    }
    fn cardinality(&self) -> f64 {
        self.estimate_cardinality()
    }
    fn entropy(&self) -> f64 {
        self.estimate_entropy()
    }
    fn f2_sum(&self) -> f64 {
        self.estimate_g_sum(|x| x * x, false)
    }
    fn top_complete(&self) -> bool {
        self.top_layer_complete()
    }
    fn merge(&mut self, other: &Self) {
        UnivMonTwoTier::merge(self, other);
    }
}

const UNIVMON_SIZES: [UnivMonParams; 2] = [
    UnivMonParams {
        heap_size: 8,
        rows: 3,
        cols: 64,
        layers: 4,
    },
    UnivMonParams {
        heap_size: 32,
        rows: 5,
        cols: 512,
        layers: 7,
    },
];

const TWO_TIER_SIZES: [TwoTierParams; 2] = [
    TwoTierParams {
        heap_size: 8,
        elephant_layers: 2,
        elephant_rows: 3,
        elephant_cols: 64,
        mouse_rows: 1,
        mouse_cols: 16,
        layers: 4,
    },
    TwoTierParams {
        heap_size: 32,
        elephant_layers: 3,
        elephant_rows: 5,
        elephant_cols: 512,
        mouse_rows: 3,
        mouse_cols: 128,
        layers: 7,
    },
];

/// Distinct over trials: a trial fails when the top layer overflowed or the
/// estimate is off by more than `ε·D`, which together have probability at
/// most `δ`.
fn distinct_trials<P: Pyramid>(salt: u64, params: &P::Params, keys: usize, delta: f64) {
    let data = distinct(keys as u64);
    let bound = P::theoretical_error(StatisticKind::Distinct, params, &data, Some(delta)).unwrap();
    assert_eq!(bound.metric, ErrorMetric::Relative);
    let trials = 150;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(salt, trial);
        let stream = mixed_keys(&mut rng, 20, 64, keys - 20);
        let mut sketch = P::build(params);
        for &(id, w) in &stream {
            sketch.add(id, w as i64);
        }
        let truth = keys as f64;
        if !sketch.top_complete() || (sketch.cardinality() - truth).abs() > bound.epsilon * truth {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

/// FrequencyL2 over trials, at its fixed `δ`.
fn l2_trials<P: Pyramid>(salt: u64, params: &P::Params) {
    let bound = P::theoretical_error(
        StatisticKind::FrequencyL2,
        params,
        &FlowData::default(),
        None,
    )
    .unwrap();
    assert_eq!(bound.metric, ErrorMetric::Relative);
    let delta = bound.delta.unwrap();
    let trials = 300;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(salt, trial);
        let stream = mixed_keys(&mut rng, 50, 64, 1000);
        let mut sketch = P::build(params);
        for &(id, w) in &stream {
            sketch.add(id, w as i64);
        }
        let truth = l2(&stream);
        if (sketch.l2() - truth).abs() > bound.epsilon * truth {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

#[test]
fn univmon_distinct_is_within_relative_epsilon_with_probability_one_minus_delta() {
    let params = UnivMonParams {
        heap_size: 32,
        rows: 3,
        cols: 256,
        layers: 7,
    };
    distinct_trials::<UnivMon>(1, &params, 1000, 0.3);
}

#[test]
fn univmon_l2_is_within_relative_epsilon_with_probability_one_minus_delta() {
    let params = UnivMonParams {
        heap_size: 8,
        rows: 5,
        cols: 1024,
        layers: 2,
    };
    let bound = UnivMon::theoretical_error(
        StatisticKind::FrequencyL2,
        &params,
        &FlowData::default(),
        None,
    )
    .unwrap();
    assert_eq!(bound.delta, Some(p_med(5, 0.1)));
    l2_trials::<UnivMon>(2, &params);
}

#[test]
fn two_tier_distinct_is_within_relative_epsilon_with_probability_one_minus_delta() {
    let params = TwoTierParams {
        heap_size: 32,
        elephant_layers: 2,
        elephant_rows: 3,
        elephant_cols: 256,
        mouse_rows: 3,
        mouse_cols: 64,
        layers: 7,
    };
    distinct_trials::<UnivMonTwoTier>(3, &params, 1000, 0.3);
}

#[test]
fn two_tier_l2_reads_layer_zero_dimensions() {
    let params = TwoTierParams {
        heap_size: 8,
        elephant_layers: 1,
        elephant_rows: 5,
        elephant_cols: 1024,
        mouse_rows: 1,
        mouse_cols: 16,
        layers: 2,
    };
    let bound = UnivMonTwoTier::theoretical_error(
        StatisticKind::FrequencyL2,
        &params,
        &FlowData::default(),
        None,
    )
    .unwrap();
    assert_eq!(bound.delta, Some(p_med(5, 0.1)));
    l2_trials::<UnivMonTwoTier>(4, &params);
}

#[test]
fn univmon_total_is_exact_and_distinct_needs_d_and_room_above_psi() {
    let p = UnivMonParams::default();
    let total =
        UnivMon::theoretical_error(StatisticKind::Total, &p, &FlowData::default(), None).unwrap();
    assert_eq!(total.epsilon, 0.0);
    assert_eq!(total.contract, "univmon_unit_update_total");
    let mut sketch = UnivMon::build(&p);
    for id in 0..100 {
        sketch.add(id, id as i64 + 1);
    }
    assert_eq!(sketch.l1(), 5050.0);
    for statistic in [StatisticKind::FrequencyEntropy, StatisticKind::GSum] {
        assert_eq!(
            UnivMon::theoretical_error(statistic, &p, &distinct(10), None),
            None
        );
    }
    assert_eq!(
        UnivMon::theoretical_error(StatisticKind::Distinct, &p, &FlowData::default(), None),
        None
    );
    // k = 32 and D = 10^4 need L ≥ lg(312.5) + 2 = 11, and 8 layers leave ψ = 1.
    assert_eq!(
        UnivMon::theoretical_error(StatisticKind::Distinct, &p, &distinct(10_000), Some(0.5)),
        None
    );
    let deep = UnivMonParams { layers: 11, ..p };
    let psi = (std::f64::consts::E / 4.0).powf(16.0);
    assert_eq!(
        UnivMon::theoretical_error(StatisticKind::Distinct, &deep, &distinct(10_000), Some(psi)),
        None
    );
    let bound =
        UnivMon::theoretical_error(StatisticKind::Distinct, &deep, &distinct(10_000), Some(0.1))
            .unwrap();
    assert!((bound.epsilon - (1.35 / (32.0 * (0.1 - psi))).sqrt()).abs() < 1e-12);
    let small_heap = UnivMonParams { heap_size: 7, ..p };
    assert_eq!(
        UnivMon::theoretical_error(StatisticKind::Distinct, &small_heap, &distinct(5), None),
        None
    );
}

#[test]
fn univmon_distinct_sizing_meets_the_target_and_one_step_smaller_does_not() {
    let data = distinct(100_000);
    for (epsilon, delta) in [(0.5, 0.3), (0.2, 0.1), (0.1, 0.05)] {
        for two_tier in [false, true] {
            let (k, layers, bound, smaller_heap, fewer_layers) = if two_tier {
                let p = UnivMonTwoTier::params_for(StatisticKind::Distinct, epsilon, delta, &data)
                    .unwrap();
                let err = |q: &TwoTierParams| {
                    UnivMonTwoTier::theoretical_error(
                        StatisticKind::Distinct,
                        q,
                        &data,
                        Some(delta),
                    )
                };
                (
                    p.heap_size,
                    p.layers,
                    err(&p),
                    err(&TwoTierParams {
                        heap_size: p.heap_size - 1,
                        ..p
                    }),
                    err(&TwoTierParams {
                        layers: p.layers - 1,
                        ..p
                    }),
                )
            } else {
                let p =
                    UnivMon::params_for(StatisticKind::Distinct, epsilon, delta, &data).unwrap();
                let err = |q: &UnivMonParams| {
                    UnivMon::theoretical_error(StatisticKind::Distinct, q, &data, Some(delta))
                };
                (
                    p.heap_size,
                    p.layers,
                    err(&p),
                    err(&UnivMonParams {
                        heap_size: p.heap_size - 1,
                        ..p
                    }),
                    err(&UnivMonParams {
                        layers: p.layers - 1,
                        ..p
                    }),
                )
            };
            let bound = bound.unwrap();
            assert!(bound.epsilon <= epsilon, "k={k}, L={layers}: {bound:?}");
            assert!(
                smaller_heap.is_none_or(|b| b.epsilon > epsilon),
                "k={k} is larger than needed"
            );
            assert!(fewer_layers.is_none(), "L={layers} is deeper than needed");
        }
    }
    assert_eq!(
        UnivMon::params_for(StatisticKind::Distinct, 0.1, 0.05, &FlowData::default()),
        None
    );
    // 2^25 keys at k = 2700 need L = lg(12428) + 2 = 16; 2^40 keys need 31.
    assert!(UnivMon::params_for(StatisticKind::Distinct, 0.1, 0.05, &distinct(1 << 25)).is_some());
    assert_eq!(
        UnivMon::params_for(StatisticKind::Distinct, 0.1, 0.05, &distinct(1 << 40)),
        None
    );
}

#[test]
fn univmon_l2_sizing_meets_the_target() {
    for (epsilon, delta) in [(0.2, 0.05), (0.1, 0.01)] {
        let p = UnivMon::params_for(
            StatisticKind::FrequencyL2,
            epsilon,
            delta,
            &FlowData::default(),
        )
        .unwrap();
        let bound = UnivMon::theoretical_error(
            StatisticKind::FrequencyL2,
            &p,
            &FlowData::default(),
            Some(delta),
        )
        .unwrap();
        assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
        let narrower = UnivMonParams {
            cols: p.cols / 2,
            ..p
        };
        assert!(
            UnivMon::theoretical_error(
                StatisticKind::FrequencyL2,
                &narrower,
                &FlowData::default(),
                None
            )
            .is_none_or(|b| b.epsilon > epsilon)
        );
    }
}

fn loaded<P: Pyramid>(params: &P::Params, salt: u64, keys: usize) -> P {
    let mut rng = rng(salt, 0);
    let mut sketch = P::build(params);
    for (id, w) in mixed_keys(&mut rng, 30, 64, keys) {
        sketch.add(id, w as i64);
    }
    sketch
}

#[test]
fn pyramid_memory_covers_the_state() {
    let numeric = FlowData {
        key_bytes: Some(0),
        ..FlowData::default()
    };
    for p in UNIVMON_SIZES {
        let mut sketch: UnivMon = loaded(&p, 5, 3000);
        assert_eq!(
            <UnivMon as SketchAnalytics>::theoretical_memory_bytes(&p, &FlowData::default()),
            None
        );
        let bound = <UnivMon as SketchAnalytics>::theoretical_memory_bytes(&p, &numeric).unwrap();
        assert!(bound >= sketch.memory_bytes(), "{p:?}");
        Pyramid::merge(&mut sketch, &loaded(&p, 15, 3000));
        assert!(bound >= sketch.memory_bytes(), "{p:?} merged");
    }
    for p in TWO_TIER_SIZES {
        let mut sketch: UnivMonTwoTier = loaded(&p, 6, 3000);
        assert_eq!(
            <UnivMonTwoTier as SketchAnalytics>::theoretical_memory_bytes(&p, &FlowData::default()),
            None
        );
        let bound =
            <UnivMonTwoTier as SketchAnalytics>::theoretical_memory_bytes(&p, &numeric).unwrap();
        assert!(bound >= sketch.memory_bytes(), "{p:?}");
        Pyramid::merge(&mut sketch, &loaded(&p, 16, 3000));
        assert!(bound >= sketch.memory_bytes(), "{p:?} merged");
    }
}

#[cfg(feature = "op-count")]
mod pyramid_ops {
    use super::*;
    use asap_sketchlib::analytics::ops;

    fn pyramid_ops<P: Pyramid>(sizes: &[P::Params]) {
        for (i, p) in sizes.iter().enumerate() {
            let mut a: P = loaded(p, 7 + i as u64, 2000);
            let b: P = loaded(p, 17 + i as u64, 2000);
            let data = FlowData::default();
            let mut rng = rng(27, i as u64);
            let updates = 2000;
            let ((), counted) = ops::count(|| {
                for _ in 0..updates {
                    a.add(rng.random(), 1);
                }
            });
            let average = counted as f64 / updates as f64;
            let bound = P::theoretical_update_ops(p, &data);
            assert!(
                average <= bound,
                "update averages {average}, formula {bound}"
            );
            type Read<P> = fn(&P) -> f64;
            for (statistic, read) in [
                (StatisticKind::Total, P::l1 as Read<P>),
                (StatisticKind::FrequencyL2, P::l2),
                (StatisticKind::Distinct, P::cardinality),
                (StatisticKind::FrequencyEntropy, P::entropy),
                (StatisticKind::GSum, P::f2_sum),
            ] {
                let bound = P::theoretical_query_ops(statistic, p, &data).unwrap();
                assert_ops_within(&format!("{statistic:?}"), bound, || read(&a));
            }
            let merge = P::theoretical_merge_ops(p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }

    #[test]
    fn univmon() {
        pyramid_ops::<UnivMon>(&UNIVMON_SIZES);
    }

    #[test]
    fn univmon_two_tier() {
        pyramid_ops::<UnivMonTwoTier>(&TWO_TIER_SIZES);
    }
}

fn univmonq(config: UnivMonQConfig, rng: &mut StdRng) -> UnivMonQ {
    UnivMonQ::with_source_id(config, rng.random()).unwrap()
}

const ITEM_CONFIG: UnivMonQConfig = UnivMonQConfig {
    layers: 2,
    top_layer_cols: 16,
    cols_halving_period: 0,
    rows: 3,
    counter_bits: 32,
    heap_size: 16,
    ordered_samples: 0,
    seed_index: 5,
};

#[test]
fn univmonq_item_frequency_is_off_by_at_most_epsilon_l2_with_probability_one_minus_delta() {
    let delta = 0.05;
    let bound = UnivMonQ::<asap_sketchlib::DefaultXxHasher>::theoretical_error(
        StatisticKind::ItemFrequency,
        &ITEM_CONFIG,
        &UnivMonQData::default(),
        Some(delta),
    )
    .unwrap();
    assert_eq!(bound.metric, ErrorMetric::L2);
    assert!((bound.epsilon - (1.0 / 16.0 / max_row_failure(3, delta)).sqrt()).abs() < 1e-12);
    let trials = 150;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(8, trial);
        let values = mixed_keys(&mut rng, 30, 200, 12_000);
        let mut sketch = univmonq(ITEM_CONFIG, &mut rng);
        for &(id, count) in &values {
            let value = id as f64;
            for _ in 0..count {
                sketch.update(&value);
            }
        }
        let (target, truth) = values[(trial % 30) as usize];
        let estimate = sketch.estimate_frequency(target as f64) as f64;
        if (estimate - truth as f64).abs() > bound.epsilon * l2(&values) {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

const SAMPLE_CONFIG: UnivMonQConfig = UnivMonQConfig {
    layers: 4,
    top_layer_cols: 64,
    cols_halving_period: 0,
    rows: 3,
    counter_bits: 32,
    heap_size: 16,
    ordered_samples: 256,
    seed_index: 5,
};

/// Distinct uniform values, so no value is frequent, `F̂₂ ≤ 2n` and the
/// heavy set is empty.
fn diffuse(rng: &mut StdRng, n: usize) -> (UnivMonQ, Vec<f64>) {
    let mut sketch = univmonq(SAMPLE_CONFIG, rng);
    let values: Vec<f64> = (0..n).map(|_| rng.random::<f64>()).collect();
    for value in &values {
        sketch.update(value);
    }
    assert!(sketch.estimate_f2() <= 2.0 * n as f64);
    assert!(
        sketch
            .prepare_queries()
            .ordered_query_diagnostics()
            .heavy
            .is_empty(),
        "a diffuse stream left a heavy set"
    );
    (sketch, values)
}

fn sample_bound(statistic: StatisticKind, n: usize, delta: f64) -> ErrorBound {
    let data = UnivMonQData {
        n: Some(n as u64),
        f2_estimate: Some(2.0 * n as f64),
    };
    UnivMonQ::<asap_sketchlib::DefaultXxHasher>::theoretical_error(
        statistic,
        &SAMPLE_CONFIG,
        &data,
        Some(delta),
    )
    .unwrap()
}

#[test]
fn univmonq_quantile_rank_is_within_epsilon_n_with_probability_one_minus_delta() {
    let (n, delta) = (3000, 0.05);
    let bound = sample_bound(StatisticKind::Quantile, n, delta);
    assert_eq!(bound.metric, ErrorMetric::Rank);
    let trials = 200;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(9, trial);
        let (sketch, values) = diffuse(&mut rng, n);
        let q = (trial % 9 + 1) as f64 / 10.0;
        let estimate = sketch.quantile(q).unwrap();
        let rank = values.iter().filter(|&&v| v <= estimate).count() as f64;
        if (rank - q * n as f64).abs() > bound.epsilon * n as f64 {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

#[test]
fn univmonq_rank_is_within_epsilon_n_with_probability_one_minus_delta() {
    let (n, delta) = (3000, 0.05);
    let bound = sample_bound(StatisticKind::Rank, n, delta);
    assert_eq!(bound.metric, ErrorMetric::L1);
    let trials = 200;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(10, trial);
        let (sketch, values) = diffuse(&mut rng, n);
        let x: f64 = rng.random();
        let truth = values.iter().filter(|&&v| v <= x).count() as f64;
        let estimate = sketch.rank(x).unwrap() as f64;
        if (estimate - truth).abs() > bound.epsilon * n as f64 {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

#[test]
fn univmonq_bounds_need_their_conditions() {
    type Q = UnivMonQ;
    let c = SAMPLE_CONFIG;
    let full = UnivMonQData {
        n: Some(10_000),
        f2_estimate: Some(10_000.0),
    };
    // `F̂₂·M < n²` holds at M = 256 and fails at M = 512.
    let concentrated = UnivMonQData {
        f2_estimate: Some(300_000.0),
        ..full
    };
    let more_samples = UnivMonQConfig {
        ordered_samples: 512,
        ..c
    };
    for statistic in [StatisticKind::Quantile, StatisticKind::Rank] {
        assert!(Q::theoretical_error(statistic, &c, &full, None).is_some());
        assert!(Q::theoretical_error(statistic, &c, &concentrated, None).is_some());
        assert_eq!(
            Q::theoretical_error(statistic, &more_samples, &concentrated, None),
            None
        );
        for data in [
            UnivMonQData::default(),
            UnivMonQData {
                f2_estimate: Some(1e8 / 256.0),
                ..full
            },
            UnivMonQData {
                f2_estimate: None,
                ..full
            },
            UnivMonQData { n: None, ..full },
        ] {
            assert_eq!(Q::theoretical_error(statistic, &c, &data, None), None);
        }
        let no_samples = UnivMonQConfig {
            ordered_samples: 0,
            ..c
        };
        assert_eq!(
            Q::theoretical_error(statistic, &no_samples, &full, None),
            None
        );
    }
    // min(M, n): fewer updates than samples widen the bound.
    let few = UnivMonQData {
        n: Some(64),
        f2_estimate: Some(10.0),
    };
    assert!(
        Q::theoretical_error(StatisticKind::Rank, &c, &few, None)
            .unwrap()
            .epsilon
            > Q::theoretical_error(StatisticKind::Rank, &c, &full, None)
                .unwrap()
                .epsilon
    );
    for statistic in [
        StatisticKind::Total,
        StatisticKind::FrequencyL2,
        StatisticKind::Distinct,
        StatisticKind::GSum,
        StatisticKind::FrequencyEntropy,
        StatisticKind::TopK,
    ] {
        assert_eq!(Q::theoretical_error(statistic, &c, &full, None), None);
    }
}

#[test]
fn univmonq_sizing_meets_the_target_and_one_step_smaller_does_not() {
    type Q = UnivMonQ;
    let none = UnivMonQData::default();
    for (epsilon, delta) in [(0.1, 0.1), (0.05, 0.01), (0.01, 1e-4)] {
        let c = Q::params_for(StatisticKind::ItemFrequency, epsilon, delta, &none).unwrap();
        let bound =
            Q::theoretical_error(StatisticKind::ItemFrequency, &c, &none, Some(delta)).unwrap();
        assert!(bound.epsilon <= epsilon, "{c:?} gives {bound:?}");
        let narrower = UnivMonQConfig {
            top_layer_cols: c.top_layer_cols / 2,
            ..c
        };
        let wider =
            Q::theoretical_error(StatisticKind::ItemFrequency, &narrower, &none, Some(delta))
                .unwrap();
        assert!(wider.epsilon > epsilon, "{c:?} is wider than needed");

        for statistic in [StatisticKind::Quantile, StatisticKind::Rank] {
            let data = UnivMonQData {
                n: Some(1 << 40),
                f2_estimate: Some((1u64 << 40) as f64),
            };
            assert_eq!(Q::params_for(statistic, epsilon, delta, &none), None);
            let c = Q::params_for(statistic, epsilon, delta, &data).unwrap();
            let bound = Q::theoretical_error(statistic, &c, &data, Some(delta)).unwrap();
            assert!(bound.epsilon <= epsilon);
            let fewer = UnivMonQConfig {
                ordered_samples: c.ordered_samples - 1,
                ..c
            };
            assert!(
                Q::theoretical_error(statistic, &fewer, &data, Some(delta))
                    .unwrap()
                    .epsilon
                    > epsilon
            );
        }
    }
    // M = 150 at (0.1, 0.1): `F̂₂·M < n² = 10^8` needs `F̂₂ < 666 667`.
    let sized = |f2: f64| {
        let data = UnivMonQData {
            n: Some(10_000),
            f2_estimate: Some(f2),
        };
        let c = Q::params_for(StatisticKind::Quantile, 0.1, 0.1, &data)?;
        assert!(Q::theoretical_error(StatisticKind::Quantile, &c, &data, Some(0.1)).is_some());
        Some(c.ordered_samples)
    };
    assert_eq!(sized(600_000.0), Some(150));
    assert_eq!(sized(666_667.0), None);
}

const Q_SIZES: [UnivMonQConfig; 2] = [
    UnivMonQConfig {
        layers: 3,
        top_layer_cols: 32,
        cols_halving_period: 1,
        rows: 3,
        counter_bits: 32,
        heap_size: 8,
        ordered_samples: 16,
        seed_index: 5,
    },
    UnivMonQConfig {
        layers: 6,
        top_layer_cols: 256,
        cols_halving_period: 2,
        rows: 5,
        counter_bits: 64,
        heap_size: 64,
        ordered_samples: 300,
        seed_index: 5,
    },
];

fn loaded_q(config: UnivMonQConfig, salt: u64) -> UnivMonQ {
    let mut rng = rng(salt, 0);
    let mut sketch = univmonq(config, &mut rng);
    for (id, count) in mixed_keys(&mut rng, 30, 64, 3000) {
        for _ in 0..count {
            sketch.update(&((id % 100_000) as f64));
        }
    }
    sketch
}

#[test]
fn univmonq_memory_covers_the_state() {
    for c in Q_SIZES {
        let bound =
            <UnivMonQ as SketchAnalytics>::theoretical_memory_bytes(&c, &UnivMonQData::default())
                .unwrap();
        let fresh = UnivMonQ::new(c).unwrap();
        assert!(bound >= fresh.memory_bytes());
        let sketch = loaded_q(c, 11);
        assert!(bound >= sketch.memory_bytes(), "{c:?}");
    }
}

#[test]
fn univmonq_memory_covers_cloned_and_decoded_partial_states() {
    for c in Q_SIZES {
        let bound = UnivMonQ::<asap_sketchlib::DefaultXxHasher>::theoretical_memory_bytes(c);
        let mut rng = rng(13, c.heap_size as u64);
        let mut partial = univmonq(c, &mut rng);
        for i in 0..c.ordered_samples * 3 / 4 {
            partial.update(&(i as f64));
        }
        let decoded = UnivMonQ::from_asapv1(&partial.to_asapv1().unwrap()).unwrap();
        for mut state in [partial.clone(), decoded] {
            for i in 0..20_000 {
                state.update(&(i as f64));
            }
            assert!(
                bound >= state.memory_bytes(),
                "{c:?}: {bound} < {}",
                state.memory_bytes()
            );
        }
    }
}

#[cfg(feature = "op-count")]
#[test]
fn univmonq_ops() {
    type Q = UnivMonQ;
    let data = UnivMonQData::default();
    for (i, c) in Q_SIZES.into_iter().enumerate() {
        let mut a = loaded_q(c, 12 + i as u64);
        let b = loaded_q(c, 22 + i as u64);
        let update = Q::theoretical_update_ops(&c, &data);
        let mut rng = rng(32, i as u64);
        for _ in 0..200 {
            let value = rng.random::<f64>();
            assert_ops_within("update", update, || a.update(&value));
        }
        // Later passes rescore residents, so a new key re-queues stale entries.
        let mut fresh = univmonq(c, &mut rng);
        let mut worst = 0;
        for pass in 1..=4 {
            for j in 0..pass * c.heap_size {
                let value = j as f64;
                let ((), ops) = asap_sketchlib::analytics::ops::count(|| fresh.update(&value));
                worst = worst.max(ops);
            }
        }
        assert!(
            worst as f64 <= update,
            "update counted {worst}, formula {update}"
        );
        let query = Q::theoretical_query_ops(StatisticKind::ItemFrequency, &c, &data).unwrap();
        assert_ops_within("ItemFrequency", query, || a.estimate_frequency(7.0));
        assert_eq!(
            Q::theoretical_query_ops(StatisticKind::Quantile, &c, &data),
            None
        );
        let merge = Q::theoretical_merge_ops(&c, &data).unwrap();
        assert_ops_within("merge", merge, || a.merge(&b).unwrap());
    }
}
