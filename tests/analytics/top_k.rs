//! Count-Min and Count Sketch with a heap, with a Space-Saving summary, and
//! folded, and the `TopKHeap` they share.

use crate::support::*;
use asap_sketchlib::analytics::{
    FlowData, FoldData, FoldParams, MatrixHeapParams, MatrixSpaceSavingParams,
};
use asap_sketchlib::{
    CountMinSketchFold, CountMinSketchWithHeap, CountMinSketchWithSpaceSaving, CountSketchFold,
    CountSketchWithHeap, CountSketchWithSpaceSaving, DataInput, ErrorBound, ErrorMetric,
    OwnedDataInput, RegularPath, SketchAnalytics, StatisticKind, Vector2D,
};
use rand::seq::SliceRandom;
use std::collections::HashMap;

type CmHeap = CountMinSketchWithHeap<Vector2D<i64>, RegularPath>;
type CsHeap = CountSketchWithHeap<Vector2D<i64>, RegularPath>;
type CmSs = CountMinSketchWithSpaceSaving<Vector2D<i64>, RegularPath>;
type CsSs = CountSketchWithSpaceSaving<Vector2D<i64>, RegularPath>;
type CmFold = CountMinSketchFold;
type CsFold = CountSketchFold;

fn key(id: u64) -> DataInput<'static> {
    DataInput::U64(id)
}

fn id_of(key: &OwnedDataInput) -> u64 {
    match key {
        OwnedDataInput::U64(id) => *id,
        other => panic!("expected a U64 key, got {other:?}"),
    }
}

/// A 16-byte key that owns exactly its length.
fn flow(id: u64) -> String {
    let mut id = format!("{id:016x}");
    id.shrink_to_fit();
    id
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

/// One sketch of the family, driven the same way by every test.
trait TopK: SketchAnalytics + Clone {
    fn build(params: &Self::Params) -> Self;
    fn add(&mut self, key: &DataInput, weight: i64);
    fn estimate(&self, id: u64) -> f64;
    /// The reported keys with their counts.
    fn reported(&self) -> Vec<(u64, f64)>;
    fn merge(&mut self, other: &Self);
}

macro_rules! with_heap {
    ($t:ty) => {
        impl TopK for $t {
            fn build(p: &MatrixHeapParams) -> Self {
                Self::new(p.rows, p.cols, p.heap_size)
            }
            fn add(&mut self, key: &DataInput, weight: i64) {
                self.update_weighted(key, weight);
            }
            fn estimate(&self, id: u64) -> f64 {
                <$t>::estimate(self, &key(id)) as f64
            }
            fn reported(&self) -> Vec<(u64, f64)> {
                let heap = self.heap().heap();
                heap.iter()
                    .map(|e| (id_of(&e.key), e.count as f64))
                    .collect()
            }
            fn merge(&mut self, other: &Self) {
                <$t>::merge(self, other);
            }
        }
    };
}
with_heap!(CmHeap);
with_heap!(CsHeap);

macro_rules! with_space_saving {
    ($t:ty) => {
        impl TopK for $t {
            fn build(p: &MatrixSpaceSavingParams) -> Self {
                Self::new(p.rows, p.cols, p.capacity)
            }
            fn add(&mut self, key: &DataInput, weight: i64) {
                self.update_weighted(key, weight);
            }
            fn estimate(&self, id: u64) -> f64 {
                <$t>::estimate(self, &key(id)) as f64
            }
            fn reported(&self) -> Vec<(u64, f64)> {
                let top = self.top_k(usize::MAX);
                top.iter().map(|(k, c)| (id_of(k), *c as f64)).collect()
            }
            fn merge(&mut self, other: &Self) {
                <$t>::merge(self, other);
            }
        }
    };
}
with_space_saving!(CmSs);
with_space_saving!(CsSs);

macro_rules! fold {
    ($t:ty) => {
        impl TopK for $t {
            fn build(p: &FoldParams) -> Self {
                Self::new(p.rows, p.full_cols, p.fold_level, p.heap_size)
            }
            fn add(&mut self, key: &DataInput, weight: i64) {
                self.update_weighted(key, weight);
            }
            fn estimate(&self, id: u64) -> f64 {
                <$t>::estimate(self, &key(id)) as f64
            }
            fn reported(&self) -> Vec<(u64, f64)> {
                let heap = self.heap().heap();
                heap.iter()
                    .map(|e| (id_of(&e.key), e.count as f64))
                    .collect()
            }
            fn merge(&mut self, other: &Self) {
                self.merge_same_level(other);
            }
        }
    };
}
fold!(CmFold);
fold!(CsFold);

/// `E` and `S` of a fold's cells.
fn fold_shape(cells: &[asap_sketchlib::FoldCell]) -> (usize, usize) {
    let most = cells.iter().map(|c| c.entry_count()).max().unwrap_or(0);
    let collided = cells
        .iter()
        .map(|c| c.entry_count())
        .filter(|&n| n > 1)
        .sum();
    (most, collided)
}

fn fold_data(cells: &[asap_sketchlib::FoldCell], distinct: usize) -> FoldData {
    let (most, collided) = fold_shape(cells);
    FoldData {
        distinct: Some(distinct as u64),
        max_cell_entries: Some(most),
        collided_entries: Some(collided),
        key_bytes: None,
        merged: None,
    }
}

/// `N` for an `Absolute(L1)` bound and `‖f‖₂` for `Absolute(L2)`.
fn scale(bound: &ErrorBound, keys: &[(u64, u64)]) -> f64 {
    if bound.metric == ErrorMetric::L1 {
        l1(keys)
    } else {
        assert_eq!(bound.metric, ErrorMetric::L2);
        l2(keys)
    }
}

/// One ItemFrequency estimate of a head key per trial against `bound`; a
/// Count-Min base never undercounts.
fn item_frequency_trials<T: TopK>(salt: u64, params: &T::Params, bound: ErrorBound) {
    let one_sided = bound.metric == ErrorMetric::L1;
    let delta = bound.delta.unwrap();
    let trials = 800;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(salt, trial);
        let keys = mixed_keys(&mut rng, 50, 2000, 3000);
        let (target, truth) = keys[(trial % 10) as usize];
        let mut sketch = T::build(params);
        for &(id, w) in &keys {
            sketch.add(&key(id), w as i64);
        }
        let estimate = sketch.estimate(target);
        assert!(!one_sided || estimate >= truth as f64, "undercounted");
        if (estimate - truth as f64).abs() > bound.epsilon * scale(&bound, &keys) {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

/// `D` of every TopK trial: 20 heavy keys over 180 unit keys.
const TOP_K_DISTINCT: u64 = 200;

/// Every reported count of one state per trial, built from unit updates,
/// against `bound`; a Count-Min base never undercounts.
fn top_k_trials<T: TopK>(salt: u64, params: &T::Params, bound: ErrorBound) {
    let one_sided = bound.metric == ErrorMetric::L1;
    let delta = bound.delta.unwrap();
    let trials = 300;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(salt, trial);
        let keys = mixed_keys(&mut rng, 20, 300, 180);
        assert_eq!(keys.len() as u64, TOP_K_DISTINCT);
        let truth: HashMap<u64, u64> = keys.iter().copied().collect();
        let mut sketch = T::build(params);
        for id in unit_stream(&mut rng, &keys) {
            sketch.add(&key(id), 1);
        }
        let reported = sketch.reported();
        assert!(!reported.is_empty());
        let limit = bound.epsilon * scale(&bound, &keys);
        let mut failed = false;
        for (id, count) in reported {
            let truth = truth[&id] as f64;
            assert!(!one_sided || count >= truth, "undercounted");
            failed |= (count - truth).abs() > limit;
        }
        failures += failed as usize;
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} trials beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

const TOP_K_FLOWS: FlowData = FlowData {
    distinct: Some(TOP_K_DISTINCT),
    key_bytes: None,
    merged: None,
};

const TOP_K_FOLDS: FoldData = FoldData {
    distinct: Some(TOP_K_DISTINCT),
    max_cell_entries: None,
    collided_entries: None,
    key_bytes: None,
    merged: Some(false),
};

const ITEM_HEAP: MatrixHeapParams = MatrixHeapParams {
    rows: 3,
    cols: 64,
    heap_size: 16,
};
const TOP_K_HEAP: MatrixHeapParams = MatrixHeapParams {
    rows: 7,
    cols: 256,
    heap_size: 16,
};
const ITEM_SS: MatrixSpaceSavingParams = MatrixSpaceSavingParams {
    rows: 3,
    cols: 64,
    capacity: 16,
};
const TOP_K_SS: MatrixSpaceSavingParams = MatrixSpaceSavingParams {
    rows: 5,
    cols: 256,
    capacity: 16,
};
const ITEM_FOLD: FoldParams = FoldParams {
    rows: 3,
    full_cols: 64,
    fold_level: 3,
    heap_size: 16,
};
const TOP_K_FOLD: FoldParams = FoldParams {
    rows: 7,
    full_cols: 256,
    fold_level: 3,
    heap_size: 16,
};

#[test]
fn count_min_with_heap_estimates_as_count_min() {
    let none = FlowData::default();
    let bound = CmHeap::theoretical_error(StatisticKind::ItemFrequency, &ITEM_HEAP, &none, None);
    item_frequency_trials::<CmHeap>(50, &ITEM_HEAP, bound.unwrap());
}

#[test]
fn count_sketch_with_heap_estimates_as_count_sketch() {
    let p = MatrixHeapParams {
        cols: 256,
        ..ITEM_HEAP
    };
    let none = FlowData::default();
    let bound = CsHeap::theoretical_error(StatisticKind::ItemFrequency, &p, &none, Some(0.05));
    item_frequency_trials::<CsHeap>(51, &p, bound.unwrap());
}

#[test]
fn count_min_with_heap_reports_counts_within_the_top_k_bound() {
    let p = TOP_K_HEAP;
    let bound = CmHeap::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, None).unwrap();
    assert_eq!(bound.delta, Some(TOP_K_DISTINCT as f64 * (-7f64).exp()));
    top_k_trials::<CmHeap>(52, &p, bound);
}

#[test]
fn count_sketch_with_heap_reports_counts_within_the_top_k_bound() {
    let p = MatrixHeapParams {
        cols: 1024,
        ..TOP_K_HEAP
    };
    let bound = CsHeap::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(0.05));
    let bound = bound.unwrap();
    assert_eq!(bound.delta, Some(0.05));
    top_k_trials::<CsHeap>(53, &p, bound);
}

#[test]
fn count_min_with_space_saving_estimates_as_count_min() {
    let none = FlowData::default();
    let bound = CmSs::theoretical_error(StatisticKind::ItemFrequency, &ITEM_SS, &none, None);
    item_frequency_trials::<CmSs>(54, &ITEM_SS, bound.unwrap());
}

#[test]
fn count_sketch_with_space_saving_estimates_as_count_sketch() {
    let p = MatrixSpaceSavingParams {
        cols: 256,
        ..ITEM_SS
    };
    let none = FlowData::default();
    let bound = CsSs::theoretical_error(StatisticKind::ItemFrequency, &p, &none, Some(0.05));
    item_frequency_trials::<CsSs>(55, &p, bound.unwrap());
}

#[test]
fn count_min_with_space_saving_reports_counts_within_the_top_k_bound() {
    let p = TOP_K_SS;
    let bound = CmSs::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, None).unwrap();
    assert_eq!(bound.delta, Some(16.0 * (-5f64).exp()));
    top_k_trials::<CmSs>(56, &p, bound);
}

#[test]
fn count_sketch_with_space_saving_reports_counts_within_the_top_k_bound() {
    let p = MatrixSpaceSavingParams {
        cols: 1024,
        ..TOP_K_SS
    };
    let bound = CsSs::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(0.05));
    top_k_trials::<CsSs>(57, &p, bound.unwrap());
}

#[test]
fn count_min_fold_estimates_as_count_min_at_full_width() {
    let none = FoldData::default();
    let bound = CmFold::theoretical_error(StatisticKind::ItemFrequency, &ITEM_FOLD, &none, None);
    item_frequency_trials::<CmFold>(58, &ITEM_FOLD, bound.unwrap());
}

#[test]
fn count_sketch_fold_estimates_as_count_sketch_at_full_width() {
    let p = FoldParams {
        full_cols: 256,
        fold_level: 5,
        ..ITEM_FOLD
    };
    let none = FoldData::default();
    let bound = CsFold::theoretical_error(StatisticKind::ItemFrequency, &p, &none, Some(0.05));
    item_frequency_trials::<CsFold>(59, &p, bound.unwrap());
}

#[test]
fn count_min_fold_reports_counts_within_the_top_k_bound() {
    let p = TOP_K_FOLD;
    let bound = CmFold::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FOLDS, None).unwrap();
    top_k_trials::<CmFold>(60, &p, bound);
}

#[test]
fn count_sketch_fold_reports_counts_within_the_top_k_bound() {
    let p = FoldParams {
        full_cols: 1024,
        ..TOP_K_FOLD
    };
    let bound = CsFold::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FOLDS, Some(0.05));
    top_k_trials::<CsFold>(61, &p, bound.unwrap());
}

#[test]
fn top_k_needs_d_and_a_count_min_product_below_one() {
    let p = MatrixHeapParams::default();
    let none = FlowData::default();
    let top_k = StatisticKind::TopK;
    assert!(CmHeap::theoretical_error(top_k, &p, &none, None).is_none());
    assert!(CsHeap::theoretical_error(top_k, &p, &none, None).is_none());
    assert!(CmHeap::params_for(top_k, 0.01, 0.01, &none).is_none());
    // D·e^−3 ≥ 1 at D = 21.
    let many = FlowData {
        distinct: Some(21),
        key_bytes: None,
        merged: None,
    };
    assert!(CmHeap::theoretical_error(StatisticKind::TopK, &p, &many, None).is_none());
    // The Space-Saving variants multiply by min(m, D).
    let p = MatrixSpaceSavingParams {
        rows: 5,
        cols: 64,
        capacity: 8,
    };
    let bound = CmSs::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, None).unwrap();
    assert_eq!(bound.delta, Some(8.0 * (-5f64).exp()));
    // At most m keys are reported, so an unknown D counts as m.
    assert_eq!(
        CmSs::theoretical_error(StatisticKind::TopK, &p, &none, None),
        Some(bound)
    );
    assert_eq!(
        CsSs::theoretical_error(top_k, &p, &none, Some(0.05)),
        CsSs::theoretical_error(top_k, &p, &TOP_K_FLOWS, Some(0.05))
    );
    assert!(CsSs::theoretical_error(top_k, &p, &none, Some(0.05)).is_some());
    let at_m = FlowData {
        distinct: Some(MatrixSpaceSavingParams::default().capacity as u64),
        ..none
    };
    assert_eq!(
        CmSs::params_for(top_k, 0.05, 0.01, &none),
        CmSs::params_for(top_k, 0.05, 0.01, &at_m)
    );
    assert!(CmSs::params_for(top_k, 0.05, 0.01, &none).is_some());
}

#[test]
fn fold_top_k_needs_a_state_merge_same_level_has_not_merged_into() {
    let p = TOP_K_FOLD;
    let top_k = StatisticKind::TopK;
    assert!(CmFold::theoretical_error(top_k, &p, &TOP_K_FOLDS, None).is_some());
    assert!(CsFold::params_for(top_k, 0.05, 0.05, &TOP_K_FOLDS).is_some());
    for merged in [None, Some(true)] {
        let data = FoldData {
            merged,
            ..TOP_K_FOLDS
        };
        assert_eq!(CmFold::theoretical_error(top_k, &p, &data, None), None);
        assert_eq!(
            CsFold::theoretical_error(top_k, &p, &data, Some(0.05)),
            None
        );
        assert_eq!(CmFold::params_for(top_k, 0.05, 0.05, &data), None);
        assert_eq!(CsFold::params_for(top_k, 0.05, 0.05, &data), None);
        let item = StatisticKind::ItemFrequency;
        assert_eq!(
            CmFold::theoretical_error(item, &p, &data, None),
            CmFold::theoretical_error(item, &p, &TOP_K_FOLDS, None)
        );
        assert!(CmFold::params_for(item, 0.05, 0.05, &data).is_some());
    }
}

#[test]
fn count_min_sizing_meets_the_target_and_one_step_smaller_does_not() {
    let none = FlowData::default();
    for (epsilon, delta) in [(0.1, 0.1), (0.01, 0.01), (0.001, 1e-4)] {
        let p = CmHeap::params_for(StatisticKind::ItemFrequency, epsilon, delta, &none).unwrap();
        assert_eq!(p.heap_size, MatrixHeapParams::default().heap_size);
        let bound = CmHeap::theoretical_error(StatisticKind::ItemFrequency, &p, &none, Some(delta));
        let bound = bound.unwrap();
        assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
        let narrower = MatrixHeapParams {
            cols: p.cols - 1,
            ..p
        };
        let wider = CmHeap::theoretical_error(StatisticKind::ItemFrequency, &narrower, &none, None);
        assert!(
            wider.unwrap().epsilon > epsilon,
            "{p:?} is wider than needed"
        );
        if p.rows > 1 {
            let shallower = MatrixHeapParams {
                rows: p.rows - 1,
                ..p
            };
            let shallow = CmHeap::theoretical_error(
                StatisticKind::ItemFrequency,
                &shallower,
                &none,
                Some(delta),
            );
            assert!(shallow.is_none(), "{p:?} is deeper than needed");
        }

        let p = CmHeap::params_for(StatisticKind::TopK, epsilon, delta, &TOP_K_FLOWS).unwrap();
        let bound = CmHeap::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(delta));
        assert!(bound.unwrap().epsilon <= epsilon);
        let shallower = MatrixHeapParams {
            rows: p.rows - 1,
            ..p
        };
        assert!(
            CmHeap::theoretical_error(StatisticKind::TopK, &shallower, &TOP_K_FLOWS, Some(delta))
                .is_none()
        );

        let p = CmSs::params_for(StatisticKind::TopK, epsilon, delta, &TOP_K_FLOWS).unwrap();
        let bound = CmSs::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(delta));
        assert!(bound.unwrap().epsilon <= epsilon);

        let p = CmFold::params_for(
            StatisticKind::ItemFrequency,
            epsilon,
            delta,
            &FoldData::default(),
        )
        .unwrap();
        assert!(p.full_cols.is_power_of_two());
        let bound = CmFold::theoretical_error(
            StatisticKind::ItemFrequency,
            &p,
            &FoldData::default(),
            Some(delta),
        );
        let bound = bound.unwrap();
        assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
        let narrower = FoldParams {
            full_cols: p.full_cols / 2,
            ..p
        };
        let wider = CmFold::theoretical_error(
            StatisticKind::ItemFrequency,
            &narrower,
            &FoldData::default(),
            None,
        );
        assert!(
            wider.unwrap().epsilon > epsilon,
            "{p:?} is wider than needed"
        );
    }
    // W is at most 65536.
    assert!(
        CmFold::params_for(
            StatisticKind::ItemFrequency,
            1e-5,
            0.01,
            &FoldData::default()
        )
        .is_none()
    );
}

/// `F = min(3, log2 W)`, so `new` accepts every sized shape.
#[test]
fn fold_sizing_returns_parameters_new_accepts() {
    let none = FoldData::default();
    for epsilon in [0.01, 0.5, 1.0, 3.0, 1e6, 1e200, f64::INFINITY] {
        for p in [
            CmFold::params_for(StatisticKind::ItemFrequency, epsilon, 0.01, &none),
            CsFold::params_for(StatisticKind::ItemFrequency, epsilon, 0.01, &none),
        ] {
            let p = p.unwrap_or_else(|| panic!("{epsilon}"));
            assert_eq!(p.fold_level, p.full_cols.trailing_zeros().min(3), "{p:?}");
            CmFold::new(p.rows, p.full_cols, p.fold_level, p.heap_size);
            CsFold::new(p.rows, p.full_cols, p.fold_level, p.heap_size);
        }
    }
}

#[test]
fn count_sketch_sizing_meets_the_target() {
    let none = FlowData::default();
    for (epsilon, delta) in [(0.1, 0.1), (0.05, 0.01), (0.02, 1e-3)] {
        let p = CsHeap::params_for(StatisticKind::ItemFrequency, epsilon, delta, &none).unwrap();
        let bound = CsHeap::theoretical_error(StatisticKind::ItemFrequency, &p, &none, Some(delta));
        assert!(bound.unwrap().epsilon <= epsilon, "{p:?}");
        let p = CsHeap::params_for(StatisticKind::TopK, epsilon, delta, &TOP_K_FLOWS).unwrap();
        let bound = CsHeap::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(delta));
        assert!(bound.unwrap().epsilon <= epsilon, "{p:?}");
        let p = CsSs::params_for(StatisticKind::TopK, epsilon, delta, &TOP_K_FLOWS).unwrap();
        let bound = CsSs::theoretical_error(StatisticKind::TopK, &p, &TOP_K_FLOWS, Some(delta));
        assert!(bound.unwrap().epsilon <= epsilon, "{p:?}");
        for statistic in [StatisticKind::ItemFrequency, StatisticKind::TopK] {
            let p = CsFold::params_for(statistic, epsilon, delta, &TOP_K_FOLDS).unwrap();
            assert!(p.full_cols.is_power_of_two() && p.rows % 2 == 1, "{p:?}");
            let bound = CsFold::theoretical_error(statistic, &p, &TOP_K_FOLDS, Some(delta));
            assert!(bound.unwrap().epsilon <= epsilon, "{p:?}");
        }
    }
    assert!(
        CsFold::params_for(
            StatisticKind::ItemFrequency,
            1e-3,
            0.01,
            &FoldData::default()
        )
        .is_none()
    );
}

#[test]
fn heap_variant_memory_covers_the_state() {
    for p in [
        MatrixHeapParams {
            rows: 3,
            cols: 64,
            heap_size: 8,
        },
        MatrixHeapParams {
            rows: 5,
            cols: 1024,
            heap_size: 64,
        },
    ] {
        let mut rng = rng(62, p.heap_size as u64);
        let keys = mixed_keys(&mut rng, 40, 64, 3000);
        let stream = unit_stream(&mut rng, &keys);
        let strings = FlowData {
            distinct: None,
            key_bytes: Some(16),
            merged: None,
        };
        let mut cm = CmHeap::build(&p);
        let mut cs = CsHeap::build(&p);
        let cm_bound = <CmHeap as SketchAnalytics>::theoretical_memory_bytes(&p, &strings).unwrap();
        let cs_bound = <CsHeap as SketchAnalytics>::theoretical_memory_bytes(&p, &strings).unwrap();
        for &id in &stream {
            let flow = flow(id);
            cm.add(&DataInput::Str(&flow), 1);
            cs.add(&DataInput::Str(&flow), 1);
            assert!(cm_bound >= cm.memory_bytes());
            assert!(cs_bound >= cs.memory_bytes());
        }
        let mut numbers = CmHeap::build(&p);
        for &id in &stream {
            numbers.add(&key(id), 1);
        }
        let other = numbers.clone();
        numbers.merge(&other);
        let unknown = FlowData::default();
        assert_eq!(
            <CmHeap as SketchAnalytics>::theoretical_memory_bytes(&p, &unknown),
            None
        );
        let numeric = FlowData {
            key_bytes: Some(0),
            ..unknown
        };
        let bound = <CmHeap as SketchAnalytics>::theoretical_memory_bytes(&p, &numeric).unwrap();
        assert!(bound >= numbers.memory_bytes());
    }
}

#[test]
fn space_saving_variant_memory_covers_the_state() {
    for p in [
        MatrixSpaceSavingParams {
            rows: 3,
            cols: 64,
            capacity: 8,
        },
        MatrixSpaceSavingParams {
            rows: 5,
            cols: 1024,
            capacity: 128,
        },
    ] {
        let mut rng = rng(63, p.capacity as u64);
        let keys = mixed_keys(&mut rng, 40, 64, 3000);
        let strings = FlowData {
            distinct: None,
            key_bytes: Some(16),
            merged: None,
        };
        let mut cm = CmSs::build(&p);
        let mut cs = CsSs::build(&p);
        for id in unit_stream(&mut rng, &keys) {
            let flow = flow(id);
            cm.add(&DataInput::Str(&flow), 1);
            cs.add(&DataInput::Str(&flow), 1);
        }
        let other = cm.clone();
        cm.merge(&other);
        assert_eq!(
            <CmSs as SketchAnalytics>::theoretical_memory_bytes(&p, &FlowData::default()),
            None
        );
        let bound = <CmSs as SketchAnalytics>::theoretical_memory_bytes(&p, &strings).unwrap();
        assert!(
            bound >= cm.memory_bytes(),
            "{bound} < {}",
            cm.memory_bytes()
        );
        let bound = <CsSs as SketchAnalytics>::theoretical_memory_bytes(&p, &strings).unwrap();
        assert!(
            bound >= cs.memory_bytes(),
            "{bound} < {}",
            cs.memory_bytes()
        );
    }
}

#[test]
fn fold_memory_covers_the_state() {
    for p in [
        FoldParams {
            rows: 3,
            full_cols: 1024,
            fold_level: 3,
            heap_size: 8,
        },
        FoldParams {
            rows: 5,
            full_cols: 4096,
            fold_level: 5,
            heap_size: 32,
        },
    ] {
        let mut rng = rng(64, p.full_cols as u64);
        let keys = mixed_keys(&mut rng, 40, 64, 2000);
        let mut cm = CmFold::build(&p);
        let mut cs = CsFold::build(&p);
        for id in unit_stream(&mut rng, &keys) {
            let flow = flow(id);
            cm.add(&DataInput::Str(&flow), 1);
            cs.add(&DataInput::Str(&flow), 1);
        }
        let other = cm.clone();
        cm.merge(&other);
        let unknown = FoldData {
            key_bytes: Some(16),
            ..FoldData::default()
        };
        assert_eq!(
            <CmFold as SketchAnalytics>::theoretical_memory_bytes(&p, &FoldData::default()),
            None
        );
        let measured = FoldData {
            key_bytes: Some(16),
            ..fold_data(cm.cells(), keys.len())
        };
        for data in [measured, unknown] {
            let bound = <CmFold as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
            assert!(
                bound >= cm.memory_bytes(),
                "{data:?}: {bound} < {}",
                cm.memory_bytes()
            );
        }
        let measured = FoldData {
            key_bytes: Some(16),
            ..fold_data(cs.cells(), keys.len())
        };
        for data in [measured, unknown] {
            let bound = <CsFold as SketchAnalytics>::theoretical_memory_bytes(&p, &data).unwrap();
            assert!(
                bound >= cs.memory_bytes(),
                "{data:?}: {bound} < {}",
                cs.memory_bytes()
            );
        }
    }
}

#[cfg(feature = "op-count")]
mod ops {
    use super::*;
    use asap_sketchlib::TopKHeap;
    use asap_sketchlib::analytics::{est_count_min, est_count_sketch, heap_update_ops, ops};
    use rand::Rng;

    #[test]
    fn heap_update_costs_at_most_h_k() {
        for k in [1, 2, 3, 8, 15, 32, 100] {
            let mut heap = TopKHeap::<i64>::new(k);
            let mut rng = rng(70, k as u64);
            let mut worst = 0;
            for _ in 0..20_000 {
                let id = rng.random_range(0..3 * k as u64 + 5);
                let count = rng.random_range(0..1000i64);
                let ((), ops) = ops::count(|| {
                    heap.update(&key(id), count);
                });
                worst = worst.max(ops);
            }
            assert!(worst as f64 <= heap_update_ops(k), "k={k}: {worst}");
        }
    }

    /// In a full heap of `2^j − 1` keys every leaf sits `j − 1` levels down,
    /// so a new key above every count displaces the root and sinks to a leaf.
    #[test]
    fn a_full_descent_costs_exactly_h_k() {
        for j in 1..=7 {
            let k = (1usize << j) - 1;
            let mut heap = TopKHeap::<i64>::new(k);
            for id in 0..k as u64 {
                heap.update(&key(id), id as i64 + 1);
            }
            for id in k as u64..k as u64 + 10 {
                let ((), ops) = ops::count(|| {
                    heap.update(&key(id), 1_000_000 + id as i64);
                });
                assert_eq!(ops as f64, heap_update_ops(k), "k={k}");
            }
        }
    }

    #[test]
    fn a_clear_costs_two_per_resident() {
        for k in [1, 5, 64] {
            let mut heap = TopKHeap::<i64>::new(k);
            for id in 0..2 * k as u64 {
                heap.update(&key(id), id as i64 + 1);
            }
            let ((), ops) = ops::count(|| heap.clear());
            assert_eq!(ops, 2 * k as u64, "k={k}");
        }
    }

    const HEAP_SIZES: [MatrixHeapParams; 2] = [
        MatrixHeapParams {
            rows: 3,
            cols: 64,
            heap_size: 8,
        },
        MatrixHeapParams {
            rows: 5,
            cols: 1024,
            heap_size: 32,
        },
    ];

    /// Two loaded states and the ids streamed into the first.
    fn loaded<T: TopK>(salt: u64, p: &T::Params) -> (T, T, Vec<u64>) {
        let mut rng = rng(salt, 0);
        let keys = mixed_keys(&mut rng, 40, 64, 1000);
        let stream = unit_stream(&mut rng, &keys);
        let (mut a, mut b) = (T::build(p), T::build(p));
        for &id in &stream {
            a.add(&key(id), 1);
            b.add(&key(id ^ 1), 1);
        }
        (a, b, stream)
    }

    fn heap_variant<T: TopK<Params = MatrixHeapParams, Data = FlowData>>(
        salt: u64,
        est: fn(usize) -> f64,
    ) {
        for p in HEAP_SIZES {
            let none = FlowData::default();
            assert_eq!(
                T::theoretical_update_ops(&p, &none),
                p.rows as f64 + est(p.rows) + heap_update_ops(p.heap_size)
            );
            let (mut a, b, stream) = loaded::<T>(salt, &p);
            let update = T::theoretical_update_ops(&p, &none);
            for &id in stream.iter().take(500) {
                assert_ops_within("update", update, || a.add(&key(id), 1));
            }
            for id in 0..50 {
                assert_ops_within("new key", update, || a.add(&key(u64::MAX - id), 1_000));
            }
            let query = T::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &none).unwrap();
            assert_ops_within("query", query, || a.estimate(stream[0]));
            let top = T::theoretical_query_ops(StatisticKind::TopK, &p, &none).unwrap();
            assert_ops_within("top_k", top, || a.reported());
            let merge = T::theoretical_merge_ops(&p, &none).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }

    #[test]
    fn count_min_with_heap() {
        heap_variant::<CmHeap>(71, est_count_min);
    }

    #[test]
    fn count_sketch_with_heap() {
        heap_variant::<CsHeap>(72, est_count_sketch);
    }

    fn space_saving_variant<T: TopK<Params = MatrixSpaceSavingParams, Data = FlowData>>(salt: u64) {
        for p in [
            MatrixSpaceSavingParams {
                rows: 3,
                cols: 64,
                capacity: 16,
            },
            MatrixSpaceSavingParams {
                rows: 5,
                cols: 1024,
                capacity: 64,
            },
        ] {
            let none = FlowData::default();
            let (mut a, b, stream) = loaded::<T>(salt, &p);
            let update = T::theoretical_update_ops(&p, &none);
            for &id in stream.iter().take(500) {
                assert_ops_within("update", update, || a.add(&key(id), 1));
            }
            for id in 0..50 {
                assert_ops_within("new key", update, || a.add(&key(u64::MAX - id), 1));
            }
            let query = T::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &none).unwrap();
            assert_ops_within("query", query, || a.estimate(stream[0]));
            let top = T::theoretical_query_ops(StatisticKind::TopK, &p, &none).unwrap();
            assert_ops_within("top_k", top, || a.reported());
            let few = T::build(&p);
            let data = FlowData {
                distinct: Some(0),
                key_bytes: None,
                merged: None,
            };
            let top = T::theoretical_query_ops(StatisticKind::TopK, &p, &data).unwrap();
            assert_ops_within("empty top_k", top, || few.reported());
            let merge = T::theoretical_merge_ops(&p, &none).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }

    #[test]
    fn count_min_with_space_saving() {
        space_saving_variant::<CmSs>(73);
    }

    #[test]
    fn count_sketch_with_space_saving() {
        space_saving_variant::<CsSs>(74);
    }

    trait Fold: TopK<Params = FoldParams, Data = FoldData> {
        fn cells(&self) -> &[asap_sketchlib::FoldCell];
        fn hierarchical(states: &[Self]) -> Self;
    }

    impl Fold for CmFold {
        fn cells(&self) -> &[asap_sketchlib::FoldCell] {
            CmFold::cells(self)
        }
        fn hierarchical(states: &[Self]) -> Self {
            CmFold::hierarchical_merge(states)
        }
    }

    impl Fold for CsFold {
        fn cells(&self) -> &[asap_sketchlib::FoldCell] {
            CsFold::cells(self)
        }
        fn hierarchical(states: &[Self]) -> Self {
            CsFold::hierarchical_merge(states)
        }
    }

    fn fold_variant<T: Fold>(salt: u64, est: fn(usize) -> f64) {
        for p in [
            FoldParams {
                rows: 3,
                full_cols: 1024,
                fold_level: 3,
                heap_size: 8,
            },
            FoldParams {
                rows: 5,
                full_cols: 4096,
                fold_level: 6,
                heap_size: 32,
            },
        ] {
            let (mut a, b, stream) = loaded::<T>(salt, &p);
            let distinct = stream
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                + 50;
            let mut finished = a.clone();
            for &id in stream.iter().take(500) {
                finished.add(&key(id), 1);
            }
            for id in 0..50 {
                finished.add(&key(u64::MAX - id), 1);
            }
            let data = fold_data(finished.cells(), distinct);
            let update = T::theoretical_update_ops(&p, &data);
            for &id in stream.iter().take(500) {
                assert_ops_within("update", update, || a.add(&key(id), 1));
            }
            for id in 0..50 {
                assert_ops_within("new key", update, || a.add(&key(u64::MAX - id), 1));
            }
            let unknown = T::theoretical_update_ops(&p, &FoldData::default());
            assert!(unknown >= update);
            let query = T::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data).unwrap();
            for &id in stream.iter().take(50) {
                assert_ops_within("query", query, || a.estimate(id));
            }
            let top = T::theoretical_query_ops(StatisticKind::TopK, &p, &data).unwrap();
            assert_ops_within("top_k", top, || a.reported());

            let mut merged = a.clone();
            merged.merge(&b);
            let both = distinct * 2;
            let data = fold_data(merged.cells(), both);
            let merge = T::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
            let unknown = T::theoretical_merge_ops(&p, &FoldData::default()).unwrap();
            assert!(unknown >= merge);

            // hierarchical_merge: dW + Σ (dW/2^F_i + 2e_i + k_i·(1 + est(d) + h(k))).
            let states = [a.clone(), b.clone(), T::build(&p)];
            let d = p.rows as f64;
            let w = p.full_cols as f64;
            let mut bound = d * w;
            for s in &states {
                let entries: usize = s.cells().iter().map(|c| c.entry_count()).sum();
                let keys = s.reported().len() as f64;
                bound += s.cells().len() as f64 + 2.0 * entries as f64;
                bound += keys * (1.0 + est(p.rows) + heap_update_ops(p.heap_size));
            }
            assert_ops_within("hierarchical merge", bound, || T::hierarchical(&states));
        }
    }

    #[test]
    fn count_min_fold() {
        fold_variant::<CmFold>(75, est_count_min);
    }

    #[test]
    fn count_sketch_fold() {
        fold_variant::<CsFold>(76, est_count_sketch);
    }
}
