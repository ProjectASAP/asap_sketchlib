//! SpaceSaving, ElasticSketch and CocoSketch.

use crate::support::*;
use asap_sketchlib::analytics::{
    CocoParams, ElasticParams, FlowData, SpaceSavingData, SpaceSavingParams,
};
use asap_sketchlib::{
    CocoSketch, DataInput, ElasticSketch, ErrorMetric, SketchAnalytics, SpaceSaving, StatisticKind,
};
use rand::seq::SliceRandom;
use std::collections::HashMap;

type Ss = SpaceSaving;
type Elastic = ElasticSketch;
type Coco = CocoSketch;

const UNMERGED: FlowData = FlowData {
    distinct: None,
    key_bytes: None,
    merged: Some(false),
};

const UNMERGED_SS: SpaceSavingData = SpaceSavingData {
    key_bytes: None,
    top_k: None,
    other_capacity: None,
    merged: Some(false),
};

const SS_SIZES: [SpaceSavingParams; 2] = [
    SpaceSavingParams { capacity: 16 },
    SpaceSavingParams { capacity: 256 },
];

const ELASTIC_SIZES: [ElasticParams; 2] = [
    ElasticParams {
        bucket_count: 16,
        light_rows: 2,
        light_cols: 64,
    },
    ElasticParams {
        bucket_count: 64,
        light_rows: 4,
        light_cols: 256,
    },
];

const COCO_SIZES: [CocoParams; 2] = [
    CocoParams { rows: 2, cols: 16 },
    CocoParams { rows: 4, cols: 128 },
];

fn key(id: u64) -> DataInput<'static> {
    DataInput::U64(id)
}

/// A 16-byte flow id that owns exactly its length.
fn flow(id: u64) -> String {
    let mut id = format!("{id:016x}");
    id.shrink_to_fit();
    id
}

fn elastic(p: ElasticParams) -> Elastic {
    Elastic::with_heavy_and_light(p.bucket_count as i32, p.light_rows, p.light_cols)
}

/// `keys` as a shuffled stream of unit updates.
fn unit_stream(rng: &mut rand::rngs::StdRng, keys: &[(u64, u64)]) -> Vec<u64> {
    let mut stream: Vec<u64> = keys
        .iter()
        .flat_map(|&(id, w)| std::iter::repeat_n(id, w as usize))
        .collect();
    stream.shuffle(rng);
    stream
}

#[test]
fn space_saving_memory_covers_the_summary() {
    for p in SS_SIZES {
        let mut numbers = Ss::with_capacity(p.capacity);
        let mut strings = Ss::with_capacity(p.capacity);
        let mut rng = rng(10, p.capacity as u64);
        let keys = mixed_keys(&mut rng, 40, 64, 4000);
        for id in unit_stream(&mut rng, &keys) {
            numbers.update(&key(id));
            strings.update(&DataInput::String(flow(id)));
        }
        let other = numbers.clone();
        numbers.merge(&other);
        let unknown = <Ss as SketchAnalytics>::theoretical_memory_bytes(&p, &Default::default());
        assert_eq!(unknown, None);
        let data = SpaceSavingData {
            key_bytes: Some(0),
            top_k: None,
            ..SpaceSavingData::default()
        };
        let bound = <Ss as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
        assert!(
            bound >= numbers.memory_bytes(),
            "{bound} < {}",
            numbers.memory_bytes()
        );
        let data = SpaceSavingData {
            key_bytes: Some(16),
            top_k: None,
            ..SpaceSavingData::default()
        };
        let bound = <Ss as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
        assert!(
            bound >= strings.memory_bytes(),
            "{bound} < {}",
            strings.memory_bytes()
        );
    }
}

#[test]
fn elastic_memory_is_exact_once_every_bucket_holds_a_flow() {
    for p in ELASTIC_SIZES {
        let mut sketch = elastic(p);
        let data = FlowData {
            distinct: None,
            key_bytes: Some(16),
            merged: None,
        };
        let bound = <Elastic as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
        assert!(bound >= sketch.memory_bytes());
        for id in 0..4000 {
            sketch.update(flow(id % 700));
        }
        assert!(sketch.heavy.iter().all(|b| !b.is_vacant()));
        assert_eq!(bound, sketch.memory_bytes());
        let unknown = FlowData {
            key_bytes: None,
            ..data
        };
        assert_eq!(
            <Elastic as SketchAnalytics>::theoretical_memory_bytes(&p, &unknown),
            None
        );
    }
}

#[test]
fn elastic_memory_counts_the_capacity_of_each_flow_id() {
    let key = |id: u64| format!("flow-{id}");
    let capacity = key(7).capacity();
    assert!(capacity > key(7).len());
    assert!((0..700).all(|id| key(id).capacity() == capacity));
    for p in ELASTIC_SIZES {
        let mut sketch = elastic(p);
        for id in 0..4000 {
            sketch.update(key(id % 700));
        }
        assert!(sketch.heavy.iter().all(|b| !b.is_vacant()));
        let data = FlowData {
            distinct: None,
            key_bytes: Some(capacity),
            merged: None,
        };
        let bound = <Elastic as SketchAnalytics>::theoretical_memory_bytes(&p, &data);
        assert_eq!(bound, Some(sketch.memory_bytes()));
    }
}

#[test]
fn coco_memory_is_exact_once_every_bucket_holds_a_key() {
    for p in COCO_SIZES {
        let mut sketch = Coco::with_dimensions_and_seed(p.rows, p.cols, 3);
        for id in 0..300 {
            sketch.update_weighted(&flow(id), 1);
        }
        let data = FlowData {
            distinct: Some(300),
            key_bytes: Some(16),
            merged: None,
        };
        let bound = <Coco as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
        assert!(bound >= sketch.memory_bytes());
        for id in 0..20_000 {
            sketch.update_weighted(&flow(id), 1);
        }
        assert_eq!(sketch.recorded_flows().count(), p.rows * p.cols);
        let data = FlowData {
            distinct: None,
            key_bytes: Some(16),
            merged: None,
        };
        let bound = <Coco as SketchAnalytics>::theoretical_memory_bytes(&p, &data);
        assert_eq!(bound, Some(sketch.memory_bytes()));
        let unknown = FlowData {
            key_bytes: None,
            ..data
        };
        assert_eq!(
            <Coco as SketchAnalytics>::theoretical_memory_bytes(&p, &unknown),
            None
        );
    }
}

#[test]
fn space_saving_is_off_by_at_most_n_over_m_on_every_trial() {
    let p = SpaceSavingParams { capacity: 64 };
    let data = UNMERGED_SS;
    let bound = Ss::theoretical_error(StatisticKind::ItemFrequency, &p, &data, None).unwrap();
    assert_eq!(bound.metric, ErrorMetric::L1);
    assert_eq!(bound.delta, Some(0.0));
    let top = Ss::theoretical_error(StatisticKind::TopK, &p, &data, Some(0.01)).unwrap();
    assert_eq!(top, bound);
    for trial in 0..100 {
        let mut rng = rng(11, trial);
        let keys = mixed_keys(&mut rng, 50, 64, 3000);
        let mut sketch = Ss::with_capacity(p.capacity);
        for id in unit_stream(&mut rng, &keys) {
            sketch.update(&key(id));
        }
        let allowed = bound.epsilon * l1(&keys);
        for &(id, truth) in &keys {
            let estimate = sketch.estimate(&key(id));
            assert!(
                (estimate as f64 - truth as f64).abs() <= allowed,
                "trial {trial}: key {id} estimated {estimate}, true {truth}, allowed {allowed}"
            );
        }
        let truth: HashMap<u64, u64> = keys.iter().copied().collect();
        for (reported, count, _) in sketch.top_k(10) {
            let asap_sketchlib::OwnedDataInput::U64(id) = reported else {
                panic!("not a u64 key");
            };
            assert!((count as f64 - truth[&id] as f64).abs() <= allowed);
        }
    }
}

#[test]
fn elastic_overcounts_by_at_most_epsilon_n_with_probability_one_minus_delta() {
    let p = ElasticParams {
        bucket_count: 16,
        light_rows: 3,
        light_cols: 64,
    };
    let bound =
        Elastic::theoretical_error(StatisticKind::ItemFrequency, &p, &UNMERGED, None).unwrap();
    assert_eq!(bound.metric, ErrorMetric::L1);
    let delta = bound.delta.unwrap();
    assert!((delta - (-3.0f64).exp()).abs() < 1e-12);
    let trials = 600;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(12, trial);
        let keys = mixed_keys(&mut rng, 50, 64, 3000);
        let ids: HashMap<u64, String> = keys.iter().map(|&(id, _)| (id, flow(id))).collect();
        let mut sketch = elastic(p);
        for id in unit_stream(&mut rng, &keys) {
            sketch.update(ids[&id].clone());
        }
        let truths: HashMap<&str, u64> =
            keys.iter().map(|&(id, w)| (ids[&id].as_str(), w)).collect();
        for bucket in sketch
            .heavy
            .iter()
            .filter(|b| !b.is_vacant() && !b.eviction)
        {
            assert_eq!(
                sketch.estimate(bucket.flow_id.clone()) as u64,
                truths[bucket.flow_id.as_str()],
                "a never-evicted resident is counted exactly"
            );
        }
        let (target, truth) = if trial % 2 == 0 {
            keys[(trial / 2 % 50) as usize]
        } else {
            keys[50 + (trial as usize % 3000)]
        };
        let estimate = sketch.estimate(ids[&target].clone()) as i64;
        assert!(estimate >= truth as i64, "Elastic undercounted");
        if (estimate - truth as i64) as f64 > bound.epsilon * l1(&keys) {
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
fn elastic_never_undercounts_any_key() {
    // Few heavy buckets force evictions, and a wide light layer leaves little
    // collision mass to hide a lost vote.
    let p = ElasticParams {
        bucket_count: 4,
        light_rows: 3,
        light_cols: 1 << 14,
    };
    for trial in 0..20 {
        let mut rng = rng(14, trial);
        let keys = mixed_keys(&mut rng, 50, 64, 3000);
        let ids: HashMap<u64, String> = keys.iter().map(|&(id, _)| (id, flow(id))).collect();
        let mut sketch = elastic(p);
        for id in unit_stream(&mut rng, &keys) {
            sketch.update(ids[&id].clone());
        }
        for &(id, truth) in &keys {
            let estimate = sketch.estimate(ids[&id].clone()) as i64;
            assert!(
                estimate >= truth as i64,
                "trial {trial}: estimated {estimate}, true {truth}"
            );
        }
    }
}

#[test]
fn space_saving_sizing_meets_the_target_and_one_counter_fewer_does_not() {
    let data = UNMERGED_SS;
    for epsilon in [0.1, 0.01, 0.003] {
        let p = Ss::params_for(StatisticKind::ItemFrequency, epsilon, 0.0, &data).unwrap();
        let bound =
            Ss::theoretical_error(StatisticKind::ItemFrequency, &p, &data, Some(0.0)).unwrap();
        assert!(bound.epsilon <= epsilon);
        let fewer = SpaceSavingParams {
            capacity: p.capacity - 1,
        };
        let wider = Ss::theoretical_error(StatisticKind::ItemFrequency, &fewer, &data, None);
        assert!(
            wider.unwrap().epsilon > epsilon,
            "{p:?} is larger than needed"
        );
    }
}

#[test]
fn elastic_sizing_meets_the_target_and_one_step_smaller_does_not() {
    let data = UNMERGED;
    for (epsilon, delta) in [(0.1, 0.1), (0.01, 0.01), (0.002, 0.007)] {
        let p = Elastic::params_for(StatisticKind::ItemFrequency, epsilon, delta, &data).unwrap();
        let bound =
            Elastic::theoretical_error(StatisticKind::ItemFrequency, &p, &data, Some(delta))
                .unwrap();
        assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
        let narrower = ElasticParams {
            light_cols: p.light_cols - 1,
            ..p
        };
        let wider =
            Elastic::theoretical_error(StatisticKind::ItemFrequency, &narrower, &data, None);
        assert!(
            wider.unwrap().epsilon > epsilon,
            "{p:?} is wider than needed"
        );
        if p.light_rows > 1 {
            let shallower = ElasticParams {
                light_rows: p.light_rows - 1,
                ..p
            };
            assert!(
                Elastic::theoretical_error(
                    StatisticKind::ItemFrequency,
                    &shallower,
                    &data,
                    Some(delta)
                )
                .is_none(),
                "{p:?} is deeper than needed"
            );
        }
    }
    // Light rows past the fifth share the heavy part's seed.
    assert!(Elastic::params_for(StatisticKind::ItemFrequency, 0.01, 1e-3, &data).is_none());
    let deep = ElasticParams {
        light_rows: 8,
        ..ElasticParams::default()
    };
    let bound =
        Elastic::theoretical_error(StatisticKind::ItemFrequency, &deep, &data, None).unwrap();
    assert!((bound.delta.unwrap() - (-5.0f64).exp()).abs() < 1e-12);
    assert!(
        Elastic::theoretical_error(StatisticKind::ItemFrequency, &deep, &data, Some(1e-3))
            .is_none()
    );
}

#[test]
fn elastic_top_k_takes_delta_over_every_distinct_key() {
    assert!(
        Elastic::theoretical_error(
            StatisticKind::TopK,
            &ElasticParams::default(),
            &UNMERGED,
            None
        )
        .is_none()
    );
    let data = FlowData {
        distinct: Some(20),
        key_bytes: None,
        merged: Some(false),
    };
    let p = Elastic::params_for(StatisticKind::TopK, 0.01, 0.2, &data).unwrap();
    assert_eq!(p.light_rows, 5);
    let bound = Elastic::theoretical_error(StatisticKind::TopK, &p, &data, Some(0.2)).unwrap();
    assert!((bound.delta.unwrap() - 20.0 * (-5.0f64).exp()).abs() < 1e-12);
    assert!(bound.epsilon <= 0.01);
    assert!(Elastic::params_for(StatisticKind::TopK, 0.01, 0.1, &data).is_none());
}

#[test]
fn space_saving_and_elastic_bounds_need_an_unmerged_state() {
    let ss = SpaceSavingParams::default();
    let flows = FlowData {
        distinct: Some(20),
        ..UNMERGED
    };
    for statistic in [StatisticKind::ItemFrequency, StatisticKind::TopK] {
        let elastic = Elastic::params_for(statistic, 0.01, 0.2, &flows).unwrap();
        assert!(Ss::theoretical_error(statistic, &ss, &UNMERGED_SS, None).is_some());
        assert!(Ss::params_for(statistic, 0.01, 0.0, &UNMERGED_SS).is_some());
        assert!(Elastic::theoretical_error(statistic, &elastic, &flows, Some(0.5)).is_some());
        assert!(Elastic::params_for(statistic, 0.01, 0.2, &flows).is_some());
        for merged in [None, Some(true)] {
            let data = SpaceSavingData {
                merged,
                ..UNMERGED_SS
            };
            assert_eq!(Ss::theoretical_error(statistic, &ss, &data, None), None);
            assert_eq!(Ss::params_for(statistic, 0.01, 0.0, &data), None);
            let data = FlowData { merged, ..flows };
            assert_eq!(
                Elastic::theoretical_error(statistic, &elastic, &data, Some(0.5)),
                None
            );
            assert_eq!(Elastic::params_for(statistic, 0.01, 0.2, &data), None);
        }
    }
}

#[test]
fn unanswered_statistics_have_no_bound() {
    let ss = SpaceSavingParams::default();
    let flows = UNMERGED;
    for statistic in [StatisticKind::Total, StatisticKind::Quantile] {
        assert_eq!(
            Ss::theoretical_error(statistic, &ss, &UNMERGED_SS, None),
            None
        );
        assert_eq!(
            Ss::theoretical_query_ops(statistic, &ss, &Default::default()),
            None
        );
        assert_eq!(
            Coco::theoretical_query_ops(statistic, &CocoParams::default(), &flows),
            None
        );
    }
    for statistic in [
        StatisticKind::ItemFrequency,
        StatisticKind::PartialKeySum,
        StatisticKind::Total,
    ] {
        assert_eq!(
            Coco::theoretical_error(statistic, &CocoParams::default(), &flows, None),
            None
        );
    }
    assert_eq!(
        Elastic::theoretical_error(
            StatisticKind::Total,
            &ElasticParams::default(),
            &flows,
            None
        ),
        None
    );
}

#[cfg(feature = "op-count")]
mod ops {
    use super::*;

    #[test]
    fn space_saving_summary() {
        for p in SS_SIZES {
            let data = SpaceSavingData {
                key_bytes: None,
                top_k: Some(p.capacity / 2),
                other_capacity: Some(p.capacity),
                merged: None,
            };
            let mut rng = rng(13, p.capacity as u64);
            let mut a = Ss::with_capacity(p.capacity);
            let mut b = Ss::with_capacity(p.capacity);
            let keys = mixed_keys(&mut rng, 30, 64, 2000);
            for id in unit_stream(&mut rng, &keys) {
                a.update(&key(id));
                b.update(&key(id ^ 1));
            }
            let update = Ss::theoretical_update_ops(&p, &data);
            let held = a.top_k(1)[0].0.clone();
            let asap_sketchlib::OwnedDataInput::U64(held) = held else {
                unreachable!()
            };
            assert_ops_within("held update", update, || a.update(&key(held)));
            for id in 0..20 {
                assert_ops_within("new update", update, || a.update(&key(u64::MAX - id)));
            }
            let query = Ss::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data).unwrap();
            assert_ops_within("query", query, || a.estimate(&key(held)));
            let top = Ss::theoretical_query_ops(StatisticKind::TopK, &p, &data).unwrap();
            assert_ops_within("top_k", top, || a.top_k(p.capacity / 2));
            let all = Ss::theoretical_query_ops(StatisticKind::TopK, &p, &Default::default());
            assert_ops_within("top_k", all.unwrap(), || a.top_k(usize::MAX));
            let merge = Ss::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
        // The costliest unit update evicts a key alone in the lowest bucket
        // into a new bucket taken from the free list.
        let mut s = Ss::with_capacity(2);
        for id in [0, 1, 1, 1] {
            s.update(&key(id));
        }
        let p = SpaceSavingParams { capacity: 2 };
        let ((), ops) = asap_sketchlib::analytics::ops::count(|| s.update(&key(2)));
        assert_eq!(
            ops as f64,
            Ss::theoretical_update_ops(&p, &Default::default())
        );
    }

    /// A state of `capacity` holding `keys` keys at distinct counts.
    fn filled(capacity: usize, keys: u64, offset: u64) -> Ss {
        let mut s = Ss::with_capacity(capacity);
        for id in 0..keys {
            for _ in 0..=id {
                s.update(&key(offset + id));
            }
        }
        s
    }

    #[test]
    fn space_saving_merge_covers_unequal_capacities() {
        for (m, other) in [(16, 256), (256, 16)] {
            let p = SpaceSavingParams { capacity: m };
            let data = SpaceSavingData {
                other_capacity: Some(other),
                ..SpaceSavingData::default()
            };
            let equal = SpaceSavingData {
                other_capacity: Some(m),
                ..data
            };
            let unknown = Ss::theoretical_merge_ops(&p, &SpaceSavingData::default());
            assert_eq!(unknown, Some(f64::INFINITY));
            let mut a = filled(m, m as u64, 0);
            let b = filled(other, other as u64, 1 << 20);
            let merge = Ss::theoretical_merge_ops(&p, &data).unwrap();
            let ((), ops) = asap_sketchlib::analytics::ops::count(|| a.merge(&b));
            assert!(ops as f64 <= merge, "merge: {ops} > {merge}");
            if other > m {
                let equal = Ss::theoretical_merge_ops(&p, &equal).unwrap();
                assert!(
                    ops as f64 > equal,
                    "{ops} within the equal-capacity {equal}"
                );
            }
        }
    }

    #[test]
    fn space_saving_unit_update_after_a_merge() {
        let p = SpaceSavingParams { capacity: 256 };
        let update = Ss::theoretical_update_ops(&p, &SpaceSavingData::default());
        // An under-full state that carries the full side's minimum.
        let mut a = filled(256, 8, 0);
        a.merge(&filled(16, 16, 1 << 20));
        assert!(a.len() < a.capacity() && a.min_count() > 0);
        for id in 0..300 {
            assert_ops_within("new key", update, || a.update(&key(u64::MAX - id)));
        }
        for id in 0..8 {
            assert_ops_within("held key", update, || a.update(&key(id)));
        }
    }

    #[test]
    fn elastic_sketch() {
        for p in ELASTIC_SIZES {
            let data = FlowData::default();
            let mut a = elastic(p);
            let mut b = elastic(p);
            for id in 0..3000 {
                a.update(flow(id % 400));
                b.update(flow(id % 500 + 200));
            }
            let update = Elastic::theoretical_update_ops(&p, &data);
            for id in [3, 7, 450, 9999] {
                assert_ops_within("update", update, || a.update(flow(id)));
            }
            let query =
                Elastic::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data).unwrap();
            assert_ops_within("query", query, || a.estimate(flow(7)));
            let top = Elastic::theoretical_query_ops(StatisticKind::TopK, &p, &data).unwrap();
            assert_ops_within("top_k", top, || a.top_k(5));
            let merge = Elastic::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }

    #[test]
    fn coco_sketch() {
        for p in COCO_SIZES {
            let data = FlowData::default();
            let mut a = Coco::with_dimensions_and_seed(p.rows, p.cols, 1);
            let mut b = Coco::with_dimensions_and_seed(p.rows, p.cols, 2);
            for id in 0..2000 {
                a.update_weighted(&flow(id % 300), 1);
                b.update_weighted(&flow(id % 400 + 100), 1);
            }
            let update = Coco::theoretical_update_ops(&p, &data);
            for id in [3, 7, 999] {
                assert_ops_within("update", update, || a.update_weighted(&flow(id), 2));
            }
            let query =
                Coco::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data).unwrap();
            assert_ops_within("query", query, || a.estimate(&flow(7)));
            let partial =
                Coco::theoretical_query_ops(StatisticKind::PartialKeySum, &p, &data).unwrap();
            assert_ops_within("partial key", partial, || {
                a.estimate_projected("0000", |k| &k[..4])
            });
            let merge = Coco::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }
}
