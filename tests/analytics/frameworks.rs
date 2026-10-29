//! Hydra, ExponentialHistogram, NitroBatch, TumblingWindow,
//! HashSketchEnsemble, and the experimental ExponentialHistogramUnivMon.

use crate::support::*;
use asap_sketchlib::analytics::{
    CellData, EhCell, EhData, EhParams, EnsembleCell, EnsembleParams, FoldData, FoldParams,
    HllParams, HydraCell, HydraData, HydraParams, KllParams, MatrixParams, NitroData, NitroParams,
    StreamData, TumblingData, TumblingParams,
};
use asap_sketchlib::input::HydraCounter;
use asap_sketchlib::{
    CountMinSketch, CountMinSketchFold, CountMinSketchFoldConfig, CountSketchWithL2Norm, DataInput,
    EhBucketSketch, EnsembleSketch, ErrorMetric, Ertl, ExponentialHistogram, FastPath,
    HashSketchEnsemble, Hydra, HyperLogLog, Kll, NitroBatch, SketchAnalytics, StatisticKind,
    TumblingWindow, Vector2D,
};
use rand::Rng;

type Cm = CountMinSketch<Vector2D<i32>, FastPath>;
type Nitro = NitroBatch<Cm>;
type FoldWindow = TumblingWindow<CountMinSketchFold>;
type Ensemble = HashSketchEnsemble;
type Fold = CountMinSketchFold;

/// Hydra grid sizes, with Count-Min cells.
const HYDRA_SIZES: [(usize, usize, usize, MatrixParams); 2] = [
    (3, 64, 2, MatrixParams { rows: 2, cols: 32 }),
    (5, 256, 3, MatrixParams { rows: 3, cols: 128 }),
];

fn schema(arity: usize) -> Vec<String> {
    (0..arity)
        .map(|i| String::from(["region", "os", "app"][i]))
        .collect()
}

fn hydra(rows: usize, cols: usize, arity: usize, cell: MatrixParams) -> Hydra {
    let prototype = HydraCounter::CountMinSketch(Cm::with_dimensions(cell.rows, cell.cols));
    Hydra::with_schema(rows, cols, schema(arity), prototype).unwrap()
}

/// The schema values of flow `id`: one per column, all distinct.
fn flow_values(id: u64, arity: usize) -> Vec<String> {
    (0..arity)
        .map(|i| format!("{:x}", id ^ (i as u64) << 60))
        .collect()
}

fn hydra_update(h: &mut Hydra, id: u64, arity: usize, value: &DataInput, weight: i32) {
    let values = flow_values(id, arity);
    let key: Vec<&str> = values.iter().map(String::as_str).collect();
    h.update(&key, value, Some(weight)).unwrap();
}

fn hydra_params(rows: usize, cols: usize, arity: usize, cell: MatrixParams) -> HydraParams {
    HydraParams {
        rows,
        cols,
        arity,
        cell: HydraCell::CountMin(cell),
    }
}

fn label_bytes(arity: usize) -> usize {
    schema(arity).iter().map(String::len).sum()
}

#[test]
fn hydra_memory_is_exact_for_count_min_cells() {
    for (rows, cols, arity, cell) in HYDRA_SIZES {
        let p = hydra_params(rows, cols, arity, cell);
        let data = HydraData {
            label_bytes: Some(label_bytes(arity)),
            ..HydraData::default()
        };
        let mut h = hydra(rows, cols, arity, cell);
        hydra_update(&mut h, 7, arity, &DataInput::U64(1), 3);
        assert_eq!(Hydra::theoretical_memory_bytes(&p, &data), h.memory_bytes());
    }
}

#[test]
fn hydra_count_min_frequency_is_within_epsilon_n_with_probability_one_minus_delta() {
    let (rows, cols, arity) = (3, 64, 2);
    let cell = MatrixParams { rows: 4, cols: 32 };
    let p = hydra_params(rows, cols, arity, cell);
    let bound = Hydra::theoretical_error(
        StatisticKind::ItemFrequency,
        &p,
        &HydraData::default(),
        None,
    )
    .unwrap();
    assert_eq!(bound.metric, ErrorMetric::L1);
    let delta = bound.delta.unwrap();
    let value = DataInput::U64(1);
    let trials = 300;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(101, trial);
        let flows = mixed_keys(&mut rng, 30, 40, 1500);
        let mut h = hydra(rows, cols, arity, cell);
        for &(id, w) in &flows {
            hydra_update(&mut h, id, arity, &value, w as i32);
        }
        let (target, truth) = flows[(trial % 30) as usize];
        let values = flow_values(target, arity);
        let estimate = h
            .query_frequency(&[Some(values[0].as_str()), None], &value)
            .unwrap();
        if (estimate - truth as f64).abs() > bound.epsilon * l1(&flows) {
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
fn hydra_sizing_meets_the_target() {
    let data = HydraData {
        arity: Some(2),
        ..HydraData::default()
    };
    for (epsilon, delta) in [(0.2, 0.2), (0.05, 0.01), (0.01, 1e-4)] {
        let p = Hydra::params_for(StatisticKind::ItemFrequency, epsilon, delta, &data).unwrap();
        assert_eq!(p.arity, 2);
        let bound =
            Hydra::theoretical_error(StatisticKind::ItemFrequency, &p, &data, Some(delta)).unwrap();
        assert!(
            bound.epsilon <= epsilon && bound.delta.unwrap() <= delta,
            "{p:?} gives {bound:?} for ({epsilon}, {delta})"
        );
        let narrower = HydraParams {
            cols: p.cols / 2,
            ..p
        };
        let narrower =
            Hydra::theoretical_error(StatisticKind::ItemFrequency, &narrower, &data, Some(delta))
                .unwrap();
        assert!(narrower.epsilon > epsilon / 2.0);
    }
}

#[test]
fn hydra_sizing_meets_the_target_for_every_cell_kind() {
    let data = HydraData {
        arity: Some(3),
        n: Some(100_000),
        total_weight: Some(100_000),
        group_weight: Some(20_000),
        group_distinct: Some(5_000),
        group_l2: Some(8_000.0),
        ..HydraData::default()
    };
    for statistic in [
        StatisticKind::Quantile,
        StatisticKind::Rank,
        StatisticKind::Distinct,
        StatisticKind::FrequencyL2,
    ] {
        for (epsilon, delta) in [(0.3, 0.1), (0.2, 0.05)] {
            let p = Hydra::params_for(statistic, epsilon, delta, &data)
                .unwrap_or_else(|| panic!("{statistic:?} at ({epsilon}, {delta})"));
            let bound = Hydra::theoretical_error(statistic, &p, &data, Some(delta)).unwrap();
            assert!(
                bound.epsilon <= epsilon && bound.delta.is_none_or(|d| d <= delta),
                "{statistic:?}: {p:?} gives {bound:?} for ({epsilon}, {delta})"
            );
        }
    }
}

#[cfg(feature = "op-count")]
#[test]
fn hydra_ops_are_within_their_formulas() {
    let data = HydraData::default();
    for (i, (rows, cols, arity, cell)) in HYDRA_SIZES.into_iter().enumerate() {
        let p = hydra_params(rows, cols, arity, cell);
        let mut a = hydra(rows, cols, arity, cell);
        let mut b = hydra(rows, cols, arity, cell);
        let mut rng = rng(102, i as u64);
        for _ in 0..500 {
            hydra_update(
                &mut a,
                rng.random(),
                arity,
                &DataInput::U64(rng.random_range(0..9)),
                1,
            );
            hydra_update(
                &mut b,
                rng.random(),
                arity,
                &DataInput::U64(rng.random_range(0..9)),
                1,
            );
        }
        let update = Hydra::theoretical_update_ops(&p, &data);
        let values = flow_values(rng.random(), arity);
        let key: Vec<&str> = values.iter().map(String::as_str).collect();
        assert_ops_within("update", update, || {
            a.update(&key, &DataInput::U64(3), None).unwrap()
        });
        let query: Vec<Option<&str>> = std::iter::once(Some(key[0]))
            .chain((1..arity).map(|_| None))
            .collect();
        let frequency = Hydra::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data);
        assert_ops_within("frequency", frequency.unwrap(), || {
            a.query_frequency(&query, &DataInput::U64(3)).unwrap()
        });
        let total = Hydra::theoretical_query_ops(StatisticKind::Total, &p, &data);
        assert_ops_within("group total", total.unwrap(), || {
            a.query_group_total(&query).unwrap()
        });
        let merge = Hydra::theoretical_merge_ops(&p, &data).unwrap();
        assert_ops_within("merge", merge, || a.merge(&b).unwrap());
    }
}

// ---------------------------------------------------------------------------
// ExponentialHistogram
// ---------------------------------------------------------------------------

fn eh_count_min(k: usize, window: u64, cell: MatrixParams) -> ExponentialHistogram {
    ExponentialHistogram::new(
        k,
        window,
        EhBucketSketch::CountMinSketch(Cm::with_dimensions(cell.rows, cell.cols)),
    )
}

fn eh_l2(k: usize, window: u64, cell: MatrixParams) -> ExponentialHistogram {
    ExponentialHistogram::new(
        k,
        window,
        EhBucketSketch::CountSketchWithL2Norm(CountSketchWithL2Norm::with_dimensions(
            cell.rows, cell.cols,
        )),
    )
}

#[test]
fn eh_memory_bounds_memory_bytes() {
    for (k, cell) in [
        (4, MatrixParams { rows: 2, cols: 32 }),
        (16, MatrixParams { rows: 3, cols: 64 }),
    ] {
        for by_mass in [false, true] {
            let mut eh = if by_mass {
                eh_l2(k, 300, cell)
            } else {
                eh_count_min(k, 300, cell)
            };
            let mut rng = rng(103, k as u64);
            let mut most = 0;
            for t in 0..1000 {
                eh.update(t, &DataInput::U64(rng.random_range(0..50)));
                most = most.max(eh.bucket_count());
            }
            let params = EhParams {
                k,
                window: 300,
                cell: if by_mass {
                    EhCell::CountSketchWithL2Norm(cell)
                } else {
                    EhCell::CountMin(cell)
                },
            };
            let data = EhData {
                buckets: Some(most),
                ..EhData::default()
            };
            let theory = ExponentialHistogram::theoretical_memory_bytes(&params, &data);
            assert!(
                theory >= eh.memory_bytes(),
                "{theory} < {}",
                eh.memory_bytes()
            );
            if !by_mass {
                let from_arrivals = EhData {
                    window_arrivals: Some(301),
                    ..EhData::default()
                };
                let bound = ExponentialHistogram::theoretical_memory_bytes(&params, &from_arrivals);
                assert!(bound >= eh.memory_bytes());
            }
        }
    }
}

#[test]
fn eh_count_min_frequency_by_count_is_within_epsilon_n_w_with_probability_one_minus_delta() {
    let (k, window) = (64, 1000u64);
    let cell = MatrixParams { rows: 2, cols: 64 };
    let params = EhParams {
        k,
        window,
        cell: EhCell::CountMin(cell),
    };
    let bound = ExponentialHistogram::theoretical_error(
        StatisticKind::ItemFrequency,
        &params,
        &EhData::default(),
        None,
    )
    .unwrap();
    assert_eq!(bound.metric, ErrorMetric::L1);
    let delta = bound.delta.unwrap();
    let trials = 200;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(104, trial);
        let target: u64 = rng.random();
        let end = window + rng.random_range(0..600);
        let mut eh = eh_count_min(k, window, cell);
        let mut keys = Vec::with_capacity(end as usize + 1);
        for t in 0..=end {
            let key = if rng.random_bool(0.5) {
                target
            } else {
                rng.random()
            };
            keys.push(key);
            eh.update(t, &DataInput::U64(key));
        }
        let start = end - window;
        let truth = keys[start as usize..]
            .iter()
            .filter(|&&k| k == target)
            .count();
        let merged = eh.query_interval(start, end).unwrap();
        let estimate = merged.query(&DataInput::U64(target)).unwrap();
        if (estimate - truth as f64).abs() > bound.epsilon * (window + 1) as f64 {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={} at delta={delta}",
        bound.epsilon
    );
}

#[cfg(feature = "op-count")]
#[test]
fn eh_ops_are_within_their_formulas() {
    use asap_sketchlib::analytics::ops;
    for (i, (k, cell)) in [
        (4, MatrixParams { rows: 2, cols: 32 }),
        (16, MatrixParams { rows: 3, cols: 64 }),
    ]
    .into_iter()
    .enumerate()
    {
        for by_mass in [false, true] {
            let window = 400;
            let mut eh = if by_mass {
                eh_l2(k, window, cell)
            } else {
                eh_count_min(k, window, cell)
            };
            let mut rng = rng(105, i as u64);
            let updates = 1500;
            let mut most = 0;
            let ((), counted) = ops::count(|| {
                for t in 0..updates {
                    eh.update(t, &DataInput::U64(rng.random_range(0..40)));
                    most = most.max(eh.bucket_count());
                }
            });
            let params = EhParams {
                k,
                window,
                cell: if by_mass {
                    EhCell::CountSketchWithL2Norm(cell)
                } else {
                    EhCell::CountMin(cell)
                },
            };
            let data = EhData {
                buckets: Some(most),
                ..EhData::default()
            };
            let average = counted as f64 / updates as f64;
            let bound = ExponentialHistogram::theoretical_update_ops(&params, &data);
            assert!(
                average <= bound,
                "by_mass={by_mass}: update averages {average}, formula {bound}"
            );
            let statistic = if by_mass {
                StatisticKind::FrequencyL2
            } else {
                StatisticKind::ItemFrequency
            };
            let query =
                ExponentialHistogram::theoretical_query_ops(statistic, &params, &data).unwrap();
            assert_ops_within("query", query, || {
                let merged = eh.query_interval(updates - window, updates - 1).unwrap();
                match &merged {
                    EhBucketSketch::CountSketchWithL2Norm(s) => s.l2(),
                    other => other.query(&DataInput::U64(3)).unwrap(),
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// NitroBatch
// ---------------------------------------------------------------------------

#[test]
fn nitro_memory_is_exact() {
    for cell in [
        MatrixParams { rows: 3, cols: 64 },
        MatrixParams { rows: 5, cols: 512 },
    ] {
        let p = NitroParams {
            rate: 0.25,
            inner: cell,
        };
        let nitro = Nitro::with_target_and_seed(0.25, Cm::with_dimensions(cell.rows, cell.cols), 1);
        assert_eq!(
            Nitro::theoretical_memory_bytes(&p, &NitroData::default()),
            nitro.memory_bytes()
        );
    }
}

/// A unit-update stream of `keys`, each repeated its weight times.
fn stream(keys: &[(u64, u64)]) -> Vec<i64> {
    keys.iter()
        .flat_map(|&(id, w)| std::iter::repeat_n(id as i64, w as usize))
        .collect()
}

#[test]
fn nitro_count_min_frequency_is_within_epsilon_n_with_probability_one_minus_delta() {
    let cell = MatrixParams { rows: 3, cols: 64 };
    let rate = 0.25;
    let delta = 0.5;
    let trials = 300;
    let mut failures = 0;
    let mut checked = None;
    for trial in 0..trials {
        let mut rng = rng(106, trial);
        let mut keys = vec![(rng.random::<u64>() >> 1, 4000)];
        keys.extend(mixed_keys(&mut rng, 20, 400, 6000));
        let data = stream(&keys);
        let p = NitroParams { rate, inner: cell };
        let n = NitroData {
            total_weight: Some(data.len() as u64),
        };
        let bound =
            Nitro::theoretical_error(StatisticKind::ItemFrequency, &p, &n, Some(delta)).unwrap();
        assert_eq!(bound.metric, ErrorMetric::L1);
        checked.get_or_insert(bound.epsilon);
        let mut nitro =
            Nitro::with_target_and_seed(rate, Cm::with_dimensions(cell.rows, cell.cols), trial);
        nitro.update_batch(&data);
        let (target, truth) = keys[0];
        let estimate = nitro.estimate_median(&DataInput::I64(target as i64));
        if (estimate - truth as f64).abs() > bound.epsilon * data.len() as f64 {
            failures += 1;
        }
    }
    assert!(
        failures <= allowed_failures(trials as usize, delta),
        "{failures}/{trials} beyond eps={:?} at delta={delta}",
        checked
    );
}

#[test]
fn nitro_has_no_count_sketch_bound_and_needs_n() {
    type NitroCs = NitroBatch<asap_sketchlib::CountSketch<Vector2D<i32>, FastPath>>;
    let p = NitroParams::default();
    let n = NitroData {
        total_weight: Some(1000),
    };
    assert!(NitroCs::theoretical_error(StatisticKind::ItemFrequency, &p, &n, None).is_none());
    assert!(
        Nitro::theoretical_error(
            StatisticKind::ItemFrequency,
            &p,
            &NitroData::default(),
            None
        )
        .is_none()
    );
    // δ at or below P_med(d, q·w/e) leaves no room for sampling.
    assert!(Nitro::theoretical_error(StatisticKind::ItemFrequency, &p, &n, Some(0.01)).is_none());
}

#[cfg(feature = "op-count")]
#[test]
fn nitro_ops_are_within_their_formulas() {
    use asap_sketchlib::analytics::ops;
    for (i, cell) in [
        MatrixParams { rows: 3, cols: 64 },
        MatrixParams { rows: 5, cols: 512 },
    ]
    .into_iter()
    .enumerate()
    {
        let rate = 0.1;
        let p = NitroParams { rate, inner: cell };
        let data = NitroData::default();
        let mut a = Nitro::with_target_and_seed(rate, Cm::with_dimensions(cell.rows, cell.cols), 3);
        let mut b = Nitro::with_target_and_seed(rate, Cm::with_dimensions(cell.rows, cell.cols), 4);
        let mut rng = rng(107, i as u64);
        let batch: Vec<i64> = (0..200_000).map(|_| rng.random_range(0..5000)).collect();
        let ((), counted) = ops::count(|| a.update_batch(&batch));
        b.update_batch(&batch[..1000]);
        let n = batch.len() as f64;
        let mean = Nitro::theoretical_update_ops(&p, &data) * n;
        // The admitted count is binomial: allow four standard deviations.
        let slack = 4.0 * cell.rows as f64 * (n * rate * (1.0 - rate)).sqrt();
        assert!(
            counted as f64 <= mean + slack,
            "updates counted {counted}, formula {mean} + {slack}"
        );
        let query = Nitro::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data);
        assert_ops_within("query", query.unwrap(), || {
            a.estimate_median(&DataInput::I64(7))
        });
        let merge = Nitro::theoretical_merge_ops(&p, &data).unwrap();
        assert_ops_within("merge", merge, || a.merge(&b));
    }
}

// ---------------------------------------------------------------------------
// TumblingWindow
// ---------------------------------------------------------------------------

const FOLD_SIZES: [FoldParams; 2] = [
    FoldParams {
        rows: 2,
        full_cols: 64,
        fold_level: 2,
        heap_size: 8,
    },
    FoldParams {
        rows: 3,
        full_cols: 256,
        fold_level: 3,
        heap_size: 16,
    },
];

fn fold_window(p: FoldParams, max_windows: usize, pool: usize) -> FoldWindow {
    let config = CountMinSketchFoldConfig {
        rows: p.rows,
        full_cols: p.full_cols,
        fold_level: p.fold_level,
        heap_size: p.heap_size,
    };
    TumblingWindow::new(10, max_windows, config, pool)
}

#[test]
fn tumbling_memory_bounds_memory_bytes() {
    for (i, inner) in FOLD_SIZES.into_iter().enumerate() {
        for (max_windows, pool) in [(4, 2), (3, 8), (0, 0)] {
            let mut w = fold_window(inner, max_windows, pool);
            let mut rng = rng(108, i as u64);
            for t in 0..400 {
                w.update_weighted(t, &DataInput::U64(rng.random_range(0..100)), 1);
            }
            let p = TumblingParams {
                window_size: 10,
                max_windows,
                pool_initial_size: pool,
                inner,
            };
            let data = TumblingData {
                inner: FoldData {
                    distinct: Some(100),
                    ..FoldData::default()
                },
                ..TumblingData::default()
            };
            let theory = FoldWindow::theoretical_memory_bytes(&p, &data);
            assert!(
                theory >= w.memory_bytes(),
                "{theory} < {}",
                w.memory_bytes()
            );
        }
    }
}

#[test]
fn tumbling_error_is_the_inner_bound_and_kll_windows_have_none() {
    let p = TumblingParams::<FoldParams>::default();
    let data = TumblingData::<FoldData>::default();
    assert_eq!(
        FoldWindow::theoretical_error(StatisticKind::ItemFrequency, &p, &data, None),
        Fold::theoretical_error(StatisticKind::ItemFrequency, &p.inner, &data.inner, None)
    );
    let kll = TumblingParams::<KllParams>::default();
    let stream = TumblingData::<StreamData>::default();
    assert!(
        TumblingWindow::<Kll>::theoretical_error(StatisticKind::Quantile, &kll, &stream, None)
            .is_none()
    );
}

#[cfg(feature = "op-count")]
#[test]
fn tumbling_ops_are_within_their_formulas() {
    for (i, inner) in FOLD_SIZES.into_iter().enumerate() {
        let max_windows = 4;
        let mut w = fold_window(inner, max_windows, 2);
        let p = TumblingParams {
            window_size: 10,
            max_windows,
            pool_initial_size: 2,
            inner,
        };
        let data = TumblingData {
            inner: FoldData {
                distinct: Some(200),
                ..FoldData::default()
            },
            ..TumblingData::default()
        };
        let update = FoldWindow::theoretical_update_ops(&p, &data);
        let mut rng = rng(109, i as u64);
        for t in 0..120 {
            let key = DataInput::U64(rng.random_range(0..200));
            // Each update opens the next window, crossing one boundary.
            assert_ops_within("update", update, || w.update_weighted(t * 10, &key, 1));
        }
        for t in [1, 3, 9] {
            let windows = TumblingData {
                windows: Some(t),
                ..data
            };
            let query =
                FoldWindow::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &windows)
                    .unwrap();
            assert_ops_within("query_recent", query, || w.query_recent(t));
        }
    }
}

// ---------------------------------------------------------------------------
// HashSketchEnsemble
// ---------------------------------------------------------------------------

fn ensemble(cell: MatrixParams) -> HashSketchEnsemble {
    HashSketchEnsemble::new(vec![
        EnsembleSketch::from(Cm::with_dimensions(cell.rows, cell.cols)),
        EnsembleSketch::from(Cm::with_dimensions(cell.rows, cell.cols)),
        EnsembleSketch::from(HyperLogLog::<Ertl>::new()),
    ])
    .unwrap()
}

fn ensemble_params(cell: MatrixParams, index: usize) -> EnsembleParams {
    EnsembleParams {
        sketches: vec![
            EnsembleCell::CountMin(cell),
            EnsembleCell::CountMin(cell),
            EnsembleCell::HllErtl(HllParams::default()),
        ],
        index,
    }
}

#[test]
fn ensemble_memory_is_exact_and_each_sketch_keeps_its_bound() {
    for cell in [
        MatrixParams { rows: 3, cols: 64 },
        MatrixParams {
            rows: 4,
            cols: 1024,
        },
    ] {
        let p = ensemble_params(cell, 0);
        let e = ensemble(cell);
        assert_eq!(
            Ensemble::theoretical_memory_bytes(&p, &CellData::default()),
            e.memory_bytes()
        );
        assert_eq!(
            Ensemble::theoretical_error(
                StatisticKind::ItemFrequency,
                &p,
                &CellData::default(),
                None
            ),
            Cm::theoretical_error(StatisticKind::ItemFrequency, &cell, &(), None)
        );
    }
}

#[cfg(feature = "op-count")]
#[test]
fn ensemble_ops_are_within_their_formulas() {
    for (i, cell) in [
        MatrixParams { rows: 3, cols: 64 },
        MatrixParams {
            rows: 4,
            cols: 1024,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut e = ensemble(cell);
        let data = CellData::default();
        let update = Ensemble::theoretical_update_ops(&ensemble_params(cell, 0), &data);
        let mut rng = rng(110, i as u64);
        for _ in 0..300 {
            let key = DataInput::U64(rng.random());
            assert_ops_within("update", update, || e.update(&key));
        }
        let frequency = Ensemble::theoretical_query_ops(
            StatisticKind::ItemFrequency,
            &ensemble_params(cell, 1),
            &data,
        );
        assert_ops_within("frequency", frequency.unwrap(), || {
            e.estimate(1, &DataInput::U64(5)).unwrap()
        });
        let distinct = Ensemble::theoretical_query_ops(
            StatisticKind::Distinct,
            &ensemble_params(cell, 2),
            &data,
        );
        assert_ops_within("distinct", distinct.unwrap(), || e.cardinality(2).unwrap());
    }
}

// ---------------------------------------------------------------------------
// ExponentialHistogramUnivMon
// ---------------------------------------------------------------------------

#[cfg(feature = "experimental")]
mod eh_univmon {
    use super::*;
    use asap_sketchlib::ExponentialHistogramUnivMon;
    use asap_sketchlib::analytics::{EhUnivMonData, EhUnivMonParams, UnivMonParams};

    type Ehu = ExponentialHistogramUnivMon;

    fn params(k: usize, window: u64, rows: usize, cols: usize, layers: usize) -> EhUnivMonParams {
        EhUnivMonParams {
            k,
            window,
            univmon: UnivMonParams {
                heap_size: 8,
                rows,
                cols,
                layers,
            },
            pool_initial_size: 2,
        }
    }

    fn build(p: &EhUnivMonParams) -> Ehu {
        let u = p.univmon;
        Ehu::with_pool_initial_size(
            p.k,
            p.window,
            u.heap_size,
            u.rows,
            u.cols,
            u.layers,
            p.pool_initial_size,
        )
    }

    /// The most UnivMon buckets, exact buckets and exact keys seen.
    fn observe(eh: &Ehu, most: &mut EhUnivMonData) {
        let keys: usize = eh.exact_buckets.iter().map(|b| b.counts.len()).sum();
        let max = |a: Option<usize>, b: usize| Some(a.unwrap_or(0).max(b));
        most.sketch_buckets = max(most.sketch_buckets, eh.sketch_buckets.len());
        most.exact_buckets = max(most.exact_buckets, eh.exact_buckets.len());
        most.exact_keys = max(most.exact_keys, keys);
    }

    const SIZES: [(usize, usize, usize, usize); 2] = [(4, 2, 8, 2), (8, 2, 16, 2)];

    #[test]
    fn eh_univmon_memory_bounds_memory_bytes() {
        for (i, (k, rows, cols, layers)) in SIZES.into_iter().enumerate() {
            let p = params(k, 1500, rows, cols, layers);
            let mut eh = build(&p);
            let mut most = EhUnivMonData::default();
            let mut rng = rng(111, i as u64);
            for t in 0..3000 {
                eh.update_weighted(t, &DataInput::U64(rng.random_range(0..400)), 1);
                observe(&eh, &mut most);
            }
            let theory = Ehu::theoretical_memory_bytes(&p, &most);
            assert!(
                theory >= eh.memory_bytes(),
                "{theory} < {}",
                eh.memory_bytes()
            );
            assert!(
                most.sketch_buckets.unwrap() > 0,
                "no bucket reached UnivMon"
            );
        }
    }

    #[test]
    fn eh_univmon_exact_l2_is_within_relative_epsilon_on_every_trial() {
        let p = params(200, 500, 3, 1024, 4);
        let bound = Ehu::theoretical_error(
            StatisticKind::FrequencyL2,
            &p,
            &EhUnivMonData {
                exact_query: Some(true),
                ..EhUnivMonData::default()
            },
            None,
        )
        .unwrap();
        assert_eq!(bound.delta, Some(0.0));
        for trial in 0..60 {
            let mut rng = rng(112, trial);
            let mut eh = build(&p);
            let end = p.window + rng.random_range(0..800);
            let mut keys = Vec::new();
            for t in 0..=end {
                let key = rng.random_range(0..50u64);
                keys.push(key);
                eh.update_weighted(t, &DataInput::U64(key), 1);
            }
            assert!(eh.sketch_buckets.is_empty());
            let start = end - p.window;
            let mut counts = std::collections::HashMap::new();
            for &key in &keys[start as usize..] {
                *counts.entry(key).or_insert(0u64) += 1;
            }
            let truth = counts.values().map(|&c| (c * c) as f64).sum::<f64>().sqrt();
            let estimate = eh.query_interval(start, end).unwrap().estimate_l2();
            assert!(
                (estimate - truth).abs() <= bound.epsilon * truth,
                "trial {trial}: {estimate} vs {truth}, eps={}",
                bound.epsilon
            );
        }
    }

    #[cfg(feature = "op-count")]
    #[test]
    fn eh_univmon_ops_are_within_their_formulas() {
        use asap_sketchlib::analytics::ops;
        for (i, (k, rows, cols, layers)) in SIZES.into_iter().enumerate() {
            let p = params(k, 1500, rows, cols, layers);
            let mut eh = build(&p);
            let mut most = EhUnivMonData::default();
            let mut rng = rng(113, i as u64);
            let updates = 3000;
            let mut counted = 0;
            for t in 0..updates {
                let key = DataInput::U64(rng.random_range(0..400));
                let ((), ops) = ops::count(|| eh.update_weighted(t, &key, 1));
                counted += ops;
                observe(&eh, &mut most);
            }
            let average = counted as f64 / updates as f64;
            let bound = Ehu::theoretical_update_ops(&p, &most);
            assert!(
                average <= bound,
                "update averages {average}, formula {bound}"
            );
            assert!(
                most.sketch_buckets.unwrap() > 0,
                "no bucket reached UnivMon"
            );
            let query = Ehu::theoretical_query_ops(StatisticKind::FrequencyL2, &p, &most).unwrap();
            for start in [0, updates / 2, updates - 5] {
                assert_ops_within("query", query, || {
                    eh.query_interval(start, updates - 1).unwrap().estimate_l2()
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Octo
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
mod octo {
    use super::*;
    use asap_sketchlib::analytics::{OctoData, OctoParent, frameworks};
    use asap_sketchlib::{
        CountMinSketchOctoPlan, OctoAggregator, OctoConfig, OctoPartition, OctoPlan, OctoWorker,
        hash64_seeded,
    };

    fn config(workers: usize, tau: u32, partition: OctoPartition) -> OctoConfig {
        OctoConfig {
            num_workers: workers,
            threshold: asap_sketchlib::OctoThreshold::new(tau),
            partition,
            ..OctoConfig::default()
        }
    }

    /// Workers and parent run on this thread, each delta applied as it is
    /// emitted: the state after the queues drain, before a flush.
    #[test]
    fn octo_count_min_residual_is_within_epsilon_with_probability_one_minus_delta() {
        let cell = MatrixParams { rows: 3, cols: 256 };
        let (workers, tau) = (4, 60);
        let cfg = config(workers, tau, OctoPartition::HashByKey);
        let trials = 200;
        let mut failures = 0;
        let mut delta = 0.0;
        for trial in 0..trials {
            let mut rng = rng(114, trial);
            let keys = mixed_keys(&mut rng, 20, 2000, 4000);
            let data = OctoData {
                total_weight: Some(l1(&keys) as u64),
                ..OctoData::default()
            };
            let bound = frameworks::octo_error(
                StatisticKind::ItemFrequency,
                &OctoParent::CountMin(cell),
                &cfg,
                &data,
                None,
            )
            .unwrap();
            delta = bound.delta.unwrap();
            let plan =
                CountMinSketchOctoPlan::with_threshold(cell.rows, cell.cols, cfg.threshold.clone());
            let mut fleet: Vec<_> = (0..workers).map(|id| plan.worker(id)).collect();
            let mut parent = plan.aggregator();
            for &(id, w) in &keys {
                let input = DataInput::U64(id);
                let worker = (hash64_seeded(0, &input) % workers as u64) as usize;
                let payload = plan.prepare(&input);
                for _ in 0..w {
                    fleet[worker].process(&payload, &mut |d| parent.apply(d));
                }
            }
            let (target, truth) = keys[(trial % 3) as usize];
            let estimate = parent.sketch.estimate(&DataInput::U64(target)) as f64;
            if (estimate - truth as f64).abs() > bound.epsilon {
                failures += 1;
            }
        }
        assert!(
            failures <= allowed_failures(trials as usize, delta),
            "{failures}/{trials} beyond the residual bound at delta={delta}"
        );
    }

    #[test]
    fn octo_bounds_follow_the_partition_and_the_flush() {
        let cell = MatrixParams { rows: 3, cols: 256 };
        let data = OctoData {
            total_weight: Some(10_000),
            l2: Some(500.0),
            ..OctoData::default()
        };
        let read = |parent: OctoParent, cfg: &OctoConfig, data: &OctoData| {
            frameworks::octo_error(StatisticKind::ItemFrequency, &parent, cfg, data, None)
        };
        let by_key = config(4, 60, OctoPartition::HashByKey);
        let round_robin = config(4, 60, OctoPartition::RoundRobin);
        let cm = read(OctoParent::CountMin(cell), &by_key, &data).unwrap();
        let cm_rr = read(OctoParent::CountMin(cell), &round_robin, &data).unwrap();
        assert!((cm_rr.epsilon - cm.epsilon - 3.0 * 59.0).abs() < 1e-9);
        let cs = read(OctoParent::CountSketch(cell), &by_key, &data).unwrap();
        let cs_rr = read(OctoParent::CountSketch(cell), &round_robin, &data).unwrap();
        assert_eq!(cs.epsilon, cs_rr.epsilon);
        let flushed = OctoData {
            flushed: true,
            ..data
        };
        assert_eq!(
            read(OctoParent::CountMin(cell), &by_key, &flushed).unwrap().epsilon,
            asap_sketchlib::CountMinSketch::<Vector2D<i32>, asap_sketchlib::RegularPath>::theoretical_error(
                StatisticKind::ItemFrequency,
                &cell,
                &(),
                None
            )
            .unwrap()
            .epsilon
        );
        let heap = read(OctoParent::CountMinWithHeap(cell), &by_key, &flushed).unwrap();
        assert_eq!(heap.epsilon, cm.epsilon);
        assert!(read(OctoParent::Other, &by_key, &flushed).is_none());
        let memory = frameworks::octo_memory_bytes::<CountMinSketchOctoPlan>(&by_key, 1000, 768);
        assert!(memory > 1000 + 4 * 768);
    }
}
