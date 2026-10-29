//! HyperLogLog, HyperLogLogHip, Kmv, SetAggregator and Bloom.

use crate::support::*;
use asap_sketchlib::analytics::{
    BloomParams, DistinctData, HllData, HllParams, KmvParams, SetData,
};
use asap_sketchlib::sketches::hll::{HyperLogLogHipImpl, HyperLogLogImpl};
use asap_sketchlib::{
    Bloom, Classic, DataInput, ErrorBound, ErrorMetric, Ertl, FastPath, HllRegistersP12,
    HllRegistersP14, Kmv, RegularPath, SetAggregator, SketchAnalytics, StatisticKind,
};
use rand::Rng;

asap_sketchlib::impl_hll_registers!(HllRegistersP8, 8, 1_usize << 8);

type ClassicP8 = HyperLogLogImpl<Classic, HllRegistersP8>;
type ClassicP12 = HyperLogLogImpl<Classic, HllRegistersP12>;
type ClassicP14 = HyperLogLogImpl<Classic, HllRegistersP14>;
type ErtlP8 = HyperLogLogImpl<Ertl, HllRegistersP8>;
type ErtlP12 = HyperLogLogImpl<Ertl, HllRegistersP12>;
type HipP8 = HyperLogLogHipImpl<HllRegistersP8>;
type HipP12 = HyperLogLogHipImpl<HllRegistersP12>;
type BloomRegular = Bloom<RegularPath>;
type BloomFast = Bloom<FastPath>;
type KmvX = Kmv;

const BLOOM_SIZES: [BloomParams; 2] = [
    BloomParams {
        rows: 3,
        cols: 1024,
    },
    BloomParams {
        rows: 7,
        cols: 1 << 16,
    },
];

fn key(id: u64) -> DataInput<'static> {
    DataInput::U64(id)
}

fn distinct(d: u64) -> DistinctData {
    DistinctData { distinct: Some(d) }
}

/// `count` fresh random keys.
fn fresh_keys(rng: &mut rand::rngs::StdRng, count: usize) -> Vec<u64> {
    (0..count).map(|_| rng.random()).collect()
}

/// `sqrt(mean(((estimate − truth)/truth)²))` over `trials` fresh key sets of
/// `truth` keys each.
fn relative_rmse(salt: u64, trials: u64, truth: usize, estimate: impl Fn(&[u64]) -> f64) -> f64 {
    let sum: f64 = (0..trials)
        .map(|trial| {
            let keys = fresh_keys(&mut rng(salt, trial), truth);
            ((estimate(&keys) - truth as f64) / truth as f64).powi(2)
        })
        .sum();
    (sum / trials as f64).sqrt()
}

#[test]
fn hll_memory_is_exact() {
    let none = HllData::default();
    let p8 = HllParams { precision: 8 };
    let p14 = HllParams { precision: 14 };
    assert_eq!(
        <ClassicP8 as SketchAnalytics>::theoretical_memory_bytes(&p8, &none),
        ClassicP8::new().memory_bytes()
    );
    assert_eq!(
        <ErtlP12 as SketchAnalytics>::theoretical_memory_bytes(&HllParams { precision: 12 }, &none),
        ErtlP12::new().memory_bytes()
    );
    assert_eq!(
        <ClassicP14 as SketchAnalytics>::theoretical_memory_bytes(&p14, &none),
        ClassicP14::new().memory_bytes()
    );
    assert_eq!(
        <HipP8 as SketchAnalytics>::theoretical_memory_bytes(&p8, &()),
        HipP8::new().memory_bytes()
    );
}

#[test]
fn kmv_memory_is_exact_for_known_distinct_and_bounds_unknown() {
    for k in [64, 3000] {
        for d in [10u64, 2000, 20_000] {
            let mut sketch: Kmv = Kmv::new(k);
            for id in 0..d {
                sketch.update(&key(id));
            }
            let p = KmvParams { k };
            assert_eq!(
                <Kmv as SketchAnalytics>::theoretical_memory_bytes(&p, &distinct(d)),
                sketch.memory_bytes(),
                "k={k} D={d}"
            );
            assert!(
                <Kmv as SketchAnalytics>::theoretical_memory_bytes(&p, &DistinctData::default())
                    >= sketch.memory_bytes()
            );
        }
    }
}

#[test]
fn set_aggregator_memory_is_exact() {
    for (d, len) in [(0u64, 8usize), (5, 8), (1000, 12), (20_000, 6)] {
        let mut set = SetAggregator::new();
        let mut other = SetAggregator::new();
        for id in 0..d {
            let k = format!("{id:0len$}");
            if id % 2 == 0 {
                set.update(&k);
            } else {
                other.update(&k);
            }
        }
        set.merge(&other).unwrap();
        let data = SetData {
            distinct: Some(d),
            mean_key_bytes: Some(len as f64),
        };
        assert_eq!(
            SetAggregator::theoretical_memory_bytes(&(), &data),
            set.memory_bytes(),
            "D={d}"
        );
        assert!(
            SetAggregator::theoretical_memory_bytes(&(), &SetData::default()) >= set.memory_bytes()
        );
    }
}

#[test]
fn bloom_memory_is_exact() {
    for p in BLOOM_SIZES
        .into_iter()
        .chain([BloomParams { rows: 2, cols: 100 }])
    {
        let none = DistinctData::default();
        assert_eq!(
            <BloomRegular as SketchAnalytics>::theoretical_memory_bytes(&p, &none),
            BloomRegular::with_dimensions(p.rows, p.cols).memory_bytes()
        );
        assert_eq!(
            <BloomFast as SketchAnalytics>::theoretical_memory_bytes(&p, &none),
            BloomFast::with_dimensions(p.rows, p.cols).memory_bytes()
        );
    }
}

/// The standard error is a root-mean-square relative error, so the measured
/// one over many trials lands within 10% of `ε`.
#[test]
fn hll_standard_errors_match_the_measured_relative_rmse() {
    let p = HllParams { precision: 8 };
    let truth = 10 << 8;
    let trials = 2000;
    let check = |name: &str, bound: ErrorBound, measured: f64| {
        assert_eq!(bound.metric, ErrorMetric::Relative);
        assert_eq!(bound.delta, None);
        assert!(
            (measured / bound.epsilon - 1.0).abs() <= 0.1,
            "{name}: measured relative RMSE {measured}, standard error {}",
            bound.epsilon
        );
    };
    let none = HllData::default();
    check(
        "classic",
        ClassicP8::theoretical_error(StatisticKind::Distinct, &p, &none, None).unwrap(),
        relative_rmse(10, trials, truth, |keys| {
            let mut sketch = ClassicP8::new();
            keys.iter().for_each(|&id| sketch.update(&key(id)));
            sketch.estimate() as f64
        }),
    );
    check(
        "ertl",
        ErtlP8::theoretical_error(StatisticKind::Distinct, &p, &none, None).unwrap(),
        relative_rmse(11, trials, truth, |keys| {
            let mut sketch = ErtlP8::new();
            keys.iter().for_each(|&id| sketch.update(&key(id)));
            sketch.estimate() as f64
        }),
    );
    check(
        "hip",
        HipP8::theoretical_error(StatisticKind::Distinct, &p, &(), None).unwrap(),
        relative_rmse(12, trials, truth, |keys| {
            let mut sketch = HipP8::new();
            keys.iter().for_each(|&id| sketch.update(&key(id)));
            sketch.estimate() as f64
        }),
    );
}

#[test]
fn classic_linear_counting_is_within_epsilon_with_probability_one_minus_delta() {
    let p = HllParams { precision: 12 };
    let max_distinct = 256u64;
    let data = HllData {
        max_distinct: Some(max_distinct),
    };
    let delta = 0.05;
    let bound =
        ClassicP12::theoretical_error(StatisticKind::Distinct, &p, &data, Some(delta)).unwrap();
    assert_eq!(
        bound.contract,
        "classic_hll_linear_counting_collision_bound_v1"
    );
    assert_eq!(bound.delta, Some(delta));
    let trials = 1000;
    let mut failures = 0;
    for trial in 0..trials {
        let keys = fresh_keys(&mut rng(13, trial), max_distinct as usize);
        let mut sketch = ClassicP12::new();
        keys.iter().for_each(|&id| sketch.update(&key(id)));
        let truth = max_distinct as f64;
        if (sketch.estimate() as f64 - truth).abs() > bound.epsilon * truth {
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
fn hll_contracts_follow_the_variant_and_the_conditions() {
    let p = HllParams { precision: 12 };
    let small = HllData {
        max_distinct: Some(256),
    };
    let large = HllData {
        max_distinct: Some(4096),
    };
    let generic = "generic_hll_rse_only_no_confidence_v1";
    let rse = |b: Option<ErrorBound>| b.map(|b| (b.contract, b.delta));
    assert_eq!(
        rse(ClassicP12::theoretical_error(
            StatisticKind::Distinct,
            &p,
            &HllData::default(),
            None
        )),
        Some((generic, None))
    );
    assert_eq!(
        rse(ClassicP12::theoretical_error(
            StatisticKind::Distinct,
            &p,
            &large,
            None
        )),
        Some((generic, None))
    );
    assert_eq!(
        rse(ErtlP12::theoretical_error(
            StatisticKind::Distinct,
            &p,
            &small,
            None
        )),
        Some((generic, None))
    );
    let collision =
        ClassicP12::theoretical_error(StatisticKind::Distinct, &p, &small, None).unwrap();
    assert_eq!(collision.delta, Some(0.01));
    assert_eq!(
        HipP12::theoretical_error(StatisticKind::Distinct, &p, &(), None)
            .unwrap()
            .contract,
        "hip_hll_rse_only_no_confidence_v1"
    );
    assert_eq!(HipP12::theoretical_merge_ops(&p, &()), None);
}

#[test]
fn hll_standard_error_sizing_meets_the_target_and_one_step_smaller_does_not() {
    let none = HllData::default();
    for epsilon in [0.1, 0.02, 0.005] {
        let p = ErtlP12::params_for(StatisticKind::Distinct, epsilon, 0.01, &none).unwrap();
        let smaller = HllParams {
            precision: p.precision - 1,
        };
        let error = |p| {
            ErtlP12::theoretical_error(StatisticKind::Distinct, &p, &none, None)
                .unwrap()
                .epsilon
        };
        assert!(error(p) <= epsilon && error(smaller) > epsilon, "{p:?}");
        assert_eq!(
            ClassicP12::params_for(StatisticKind::Distinct, epsilon, 0.01, &none),
            Some(p)
        );
        let hip = HipP12::params_for(StatisticKind::Distinct, epsilon, 0.01, &()).unwrap();
        let hip_error = |p| {
            HipP12::theoretical_error(StatisticKind::Distinct, &p, &(), None)
                .unwrap()
                .epsilon
        };
        let smaller = HllParams {
            precision: hip.precision - 1,
        };
        assert!(hip_error(hip) <= epsilon && hip_error(smaller) > epsilon);
    }
}

#[test]
fn classic_collision_sizing_meets_the_target_and_one_step_smaller_does_not() {
    for (max_distinct, epsilon, delta) in
        [(1000u64, 0.05, 0.01), (100, 0.1, 0.05), (5000, 0.05, 1e-3)]
    {
        let data = HllData {
            max_distinct: Some(max_distinct),
        };
        let p = ClassicP12::params_for(StatisticKind::Distinct, epsilon, delta, &data).unwrap();
        let bound =
            ClassicP12::theoretical_error(StatisticKind::Distinct, &p, &data, Some(delta)).unwrap();
        assert_eq!(
            bound.contract,
            "classic_hll_linear_counting_collision_bound_v1"
        );
        assert!(bound.epsilon <= epsilon, "{p:?} gives {bound:?}");
        let smaller = HllParams {
            precision: p.precision - 1,
        };
        assert!(
            ClassicP12::theoretical_error(StatisticKind::Distinct, &smaller, &data, Some(delta))
                .is_none_or(|b| b.contract != bound.contract || b.epsilon > epsilon),
            "{p:?} is larger than needed"
        );
    }
}

#[test]
fn kmv_is_exact_below_k_and_within_epsilon_with_probability_one_minus_delta() {
    let k = 256;
    let p = KmvParams { k };
    let delta = 0.05;
    let exact = KmvX::theoretical_error(StatisticKind::Distinct, &p, &distinct(200), None).unwrap();
    assert_eq!((exact.epsilon, exact.delta), (0.0, Some(0.0)));
    let bound =
        KmvX::theoretical_error(StatisticKind::Distinct, &p, &distinct(5000), Some(delta)).unwrap();
    assert_eq!(bound.metric, ErrorMetric::Relative);
    let trials = 500;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(14, trial);
        for (d, epsilon) in [(200usize, 0.0), (5000, bound.epsilon)] {
            let keys = fresh_keys(&mut rng, d);
            let mut sketch: Kmv = Kmv::new(k);
            for &id in &keys {
                sketch.update(&key(id));
                sketch.update(&key(id));
            }
            let error = (sketch.estimate() - d as f64).abs();
            if epsilon == 0.0 {
                assert_eq!(error, 0.0, "below k the estimate is the count");
            } else if error > epsilon * d as f64 {
                failures += 1;
            }
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

#[test]
fn kmv_sizing_meets_the_target_and_one_smaller_k_does_not() {
    let unknown = DistinctData::default();
    let p = KmvX::params_for(StatisticKind::Distinct, 0.05, 0.01, &unknown).unwrap();
    assert_eq!(p.k, 4323);
    for (epsilon, delta) in [(0.05, 0.01), (0.2, 0.1), (0.01, 1e-4), (1.5, 0.01)] {
        let p = KmvX::params_for(StatisticKind::Distinct, epsilon, delta, &unknown).unwrap();
        let bound =
            KmvX::theoretical_error(StatisticKind::Distinct, &p, &unknown, Some(delta)).unwrap();
        assert!(bound.epsilon <= epsilon, "{p:?} gives {bound:?}");
        if p.k > 2 {
            let smaller = KmvParams { k: p.k - 1 };
            let bound =
                KmvX::theoretical_error(StatisticKind::Distinct, &smaller, &unknown, Some(delta))
                    .unwrap();
            assert!(bound.epsilon > epsilon, "{p:?} is larger than needed");
        }
    }
    let p = KmvX::params_for(StatisticKind::Distinct, 0.01, 1e-4, &distinct(500)).unwrap();
    assert_eq!(p.k, 501);
}

#[test]
fn set_aggregator_is_exact() {
    for statistic in [StatisticKind::Distinct, StatisticKind::Membership] {
        assert_eq!(
            SetAggregator::theoretical_error(statistic, &(), &SetData::default(), None),
            Some(ErrorBound::EXACT)
        );
    }
    let mut rng = rng(15, 0);
    let keys: Vec<String> = fresh_keys(&mut rng, 500)
        .iter()
        .map(|id| id.to_string())
        .collect();
    let mut set = SetAggregator::new();
    for k in keys.iter().chain(keys.iter().take(100)) {
        set.update(k);
    }
    assert_eq!(set.values.len(), 500);
    assert!(keys.iter().all(|k| set.contains(k)));
    assert!(!set.contains("not a key"));
}

/// Members always read present, and a fresh key reads present at the
/// predicted rate.
#[test]
fn bloom_false_positive_rate_is_at_most_delta() {
    let p = BloomParams {
        rows: 4,
        cols: 1024,
    };
    let d = 650u64;
    let bound =
        BloomRegular::theoretical_error(StatisticKind::Membership, &p, &distinct(d), None).unwrap();
    assert_eq!((bound.metric, bound.epsilon), (ErrorMetric::ABSOLUTE, 0.0));
    let delta = bound.delta.unwrap();
    assert!((0.03..0.08).contains(&delta), "{delta}");
    let trials = 2000;
    for fast in [false, true] {
        let mut failures = 0;
        for trial in 0..trials {
            let mut rng = rng(16 + fast as u64, trial);
            let keys = fresh_keys(&mut rng, d as usize + 1);
            let (members, probe) = keys.split_at(d as usize);
            let mut regular = BloomRegular::with_dimensions(p.rows, p.cols);
            let mut fastpath = BloomFast::with_dimensions(p.rows, p.cols);
            let contains = |regular: &BloomRegular, fastpath: &BloomFast, id| {
                if fast {
                    fastpath.contains(&key(id))
                } else {
                    regular.contains(&key(id))
                }
            };
            for &id in members {
                if fast {
                    fastpath.update(&key(id));
                } else {
                    regular.update(&key(id));
                }
            }
            assert!(
                members.iter().all(|&id| contains(&regular, &fastpath, id)),
                "fast={fast}: a member read absent"
            );
            if contains(&regular, &fastpath, probe[0]) {
                failures += 1;
            }
        }
        assert!(
            failures <= allowed_failures(trials as usize, delta),
            "fast={fast}: {failures}/{trials} false positives at delta={delta}"
        );
    }
}

#[test]
fn bloom_error_needs_d_and_a_power_of_two_width() {
    let p = BloomParams {
        rows: 4,
        cols: 1000,
    };
    assert!(
        BloomRegular::theoretical_error(StatisticKind::Membership, &p, &distinct(10), None)
            .is_none()
    );
    let p = BloomParams {
        rows: 4,
        cols: 1024,
    };
    assert!(
        BloomRegular::theoretical_error(
            StatisticKind::Membership,
            &p,
            &DistinctData::default(),
            None
        )
        .is_none()
    );
    let rate = BloomRegular::theoretical_error(StatisticKind::Membership, &p, &distinct(10), None)
        .unwrap()
        .delta
        .unwrap();
    assert!(
        BloomRegular::theoretical_error(
            StatisticKind::Membership,
            &p,
            &distinct(10),
            Some(rate / 2.0)
        )
        .is_none()
    );
}

#[test]
fn bloom_sizing_meets_delta_with_the_fewest_bits() {
    for (d, delta) in [(1000u64, 0.01), (100_000, 1e-4), (10, 0.1), (5000, 1e-6)] {
        let data = distinct(d);
        let p = BloomRegular::params_for(StatisticKind::Membership, 0.0, delta, &data).unwrap();
        let meets = |rows: usize, cols: usize| {
            BloomRegular::theoretical_error(
                StatisticKind::Membership,
                &BloomParams { rows, cols },
                &data,
                Some(delta),
            )
            .is_some()
        };
        assert!(meets(p.rows, p.cols), "{p:?}");
        assert!(
            p.cols == 1 || !meets(p.rows, p.cols / 2),
            "{p:?} is wider than needed"
        );
        for rows in 1..=asap_sketchlib::BLOOM_MAX_SLICES {
            let mut cols = 1usize;
            while rows * cols < p.rows * p.cols {
                assert!(!meets(rows, cols), "{rows}x{cols} beats {p:?}");
                cols *= 2;
            }
        }
    }
}

#[cfg(feature = "op-count")]
mod ops {
    use super::*;

    #[test]
    fn hll_classic_and_ertl() {
        let none = HllData::default();
        let p8 = HllParams { precision: 8 };
        let p12 = HllParams { precision: 12 };
        macro_rules! check {
            ($ty:ty, $p:expr) => {{
                let mut a = <$ty>::new();
                let mut b = <$ty>::new();
                for id in 0..5000 {
                    a.update(&key(id));
                    b.update(&key(id + 2500));
                }
                let update = <$ty>::theoretical_update_ops(&$p, &none);
                for id in 0..100 {
                    assert_ops_within("update", update, || a.update(&key(id * 7919)));
                }
                let query =
                    <$ty>::theoretical_query_ops(StatisticKind::Distinct, &$p, &none).unwrap();
                assert_ops_within("query", query, || a.estimate());
                let merge = <$ty>::theoretical_merge_ops(&$p, &none).unwrap();
                assert_ops_within("merge", merge, || a.merge(&b));
            }};
        }
        check!(ClassicP8, p8);
        check!(ClassicP12, p12);
        check!(ErtlP8, p8);
        check!(ErtlP12, p12);
    }

    #[test]
    fn hll_hip() {
        macro_rules! check {
            ($ty:ty, $p:expr) => {{
                let update = <$ty>::theoretical_update_ops(&$p, &());
                let mut sketch = <$ty>::new();
                for id in 0..5000 {
                    assert_ops_within("update", update, || sketch.update(&key(id)));
                }
                let query =
                    <$ty>::theoretical_query_ops(StatisticKind::Distinct, &$p, &()).unwrap();
                assert_ops_within("query", query, || sketch.estimate());
            }};
        }
        check!(HipP8, HllParams { precision: 8 });
        check!(HipP12, HllParams { precision: 12 });
    }

    #[test]
    fn kmv() {
        for k in [64usize, 1000] {
            let p = KmvParams { k };
            let data = DistinctData::default();
            let update = KmvX::theoretical_update_ops(&p, &data);
            let mut filling: Kmv = Kmv::new(k);
            for id in 0..k as u64 {
                assert_ops_within("update, filling", update, || {
                    filling.update_with_hash(u64::MAX - id)
                });
            }
            let mut a: Kmv = Kmv::new(k);
            let mut b: Kmv = Kmv::new(k);
            for id in 0..20_000 {
                a.update(&key(id));
                b.update(&key(id + 1_000_000));
            }
            for hash in [0, 1, 2, u64::MAX] {
                assert_ops_within("update, full", update, || a.update_with_hash(hash));
            }
            let query = KmvX::theoretical_query_ops(StatisticKind::Distinct, &p, &data).unwrap();
            assert_ops_within("query", query, || a.estimate());
            let merge = KmvX::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
            let mut empty: Kmv = Kmv::new(k);
            assert_ops_within("merge into empty", merge, || empty.merge(&b));
        }
    }

    #[test]
    fn set_aggregator() {
        for d in [10u64, 1000] {
            let data = SetData {
                distinct: Some(d),
                mean_key_bytes: None,
            };
            let mut a = SetAggregator::new();
            let mut b = SetAggregator::new();
            for id in 0..d {
                a.update(&id.to_string());
                b.update(&(id + d / 2).to_string());
            }
            let update = SetAggregator::theoretical_update_ops(&(), &data);
            assert_ops_within("update", update, || a.update("new"));
            let distinct =
                SetAggregator::theoretical_query_ops(StatisticKind::Distinct, &(), &data).unwrap();
            assert_ops_within("distinct", distinct, || a.values.len());
            let member =
                SetAggregator::theoretical_query_ops(StatisticKind::Membership, &(), &data)
                    .unwrap();
            assert_ops_within("membership", member, || a.contains("7"));
            let merge = SetAggregator::theoretical_merge_ops(&(), &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b).unwrap());
        }
    }

    #[test]
    fn bloom() {
        let data = DistinctData::default();
        for p in BLOOM_SIZES {
            let mut a = BloomRegular::with_dimensions(p.rows, p.cols);
            let mut f = BloomFast::with_dimensions(p.rows, p.cols);
            for id in 0..500 {
                a.update(&key(id));
                f.update(&key(id));
            }
            let (b, g) = (a.clone(), f.clone());
            let update = BloomRegular::theoretical_update_ops(&p, &data);
            assert_ops_within("update", update, || a.update(&key(7)));
            assert_ops_within("update", update, || f.update(&key(7)));
            let query =
                BloomRegular::theoretical_query_ops(StatisticKind::Membership, &p, &data).unwrap();
            assert_ops_within("query", query, || a.contains(&key(7)));
            assert_ops_within("query", query, || f.contains(&key(7)));
            let merge = BloomRegular::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
            assert_ops_within("merge", merge, || f.merge(&g));
        }
    }
}
