//! Count-Min, Count Sketch, Count Sketch with row L2 norms, CountMinHll and
//! MicroCm.

use crate::support::*;
use asap_sketchlib::analytics::{MatrixParams, p_med};
use asap_sketchlib::{
    CountMinSketch, CountSketch, CountSketchWithL2Norm, DataInput, ErrorMetric, FastPath,
    RegularPath, SketchAnalytics, StatisticKind, Vector2D,
};
use rand::Rng;

type CmRegular = CountMinSketch<Vector2D<i64>, RegularPath>;
type CmFast = CountMinSketch<Vector2D<i64>, FastPath>;
type CsRegular = CountSketch<Vector2D<i64>, RegularPath>;
type CsFast = CountSketch<Vector2D<i64>, FastPath>;
type CsL2 = CountSketchWithL2Norm;

const SIZES: [MatrixParams; 2] = [
    MatrixParams { rows: 3, cols: 64 },
    MatrixParams {
        rows: 7,
        cols: 1024,
    },
];

fn key(id: u64) -> DataInput<'static> {
    DataInput::U64(id)
}

#[test]
fn memory_is_exact_for_vector_storage() {
    for p in SIZES {
        let cm = CmRegular::with_dimensions(p.rows, p.cols);
        assert_eq!(
            <CmRegular as SketchAnalytics>::theoretical_memory_bytes(&p, &()),
            Some(cm.memory_bytes())
        );
        let cs = CsFast::with_dimensions(p.rows, p.cols);
        assert_eq!(
            <CsFast as SketchAnalytics>::theoretical_memory_bytes(&p, &()),
            Some(cs.memory_bytes())
        );
        let l2 = CsL2::with_dimensions(p.rows, p.cols);
        assert_eq!(
            <CsL2 as SketchAnalytics>::theoretical_memory_bytes(&p, &()),
            Some(l2.memory_bytes())
        );
    }
}

#[test]
fn count_min_overcounts_by_at_most_epsilon_n_with_probability_one_minus_delta() {
    let p = MatrixParams { rows: 3, cols: 64 };
    for fast in [false, true] {
        let bound = if fast {
            CmFast::theoretical_error(StatisticKind::ItemFrequency, &p, &(), None)
        } else {
            CmRegular::theoretical_error(StatisticKind::ItemFrequency, &p, &(), None)
        }
        .unwrap();
        assert_eq!(bound.metric, ErrorMetric::L1);
        let delta = bound.delta.unwrap();
        let trials = 800;
        let mut failures = 0;
        for trial in 0..trials {
            let mut rng = rng(1, trial);
            let keys = mixed_keys(&mut rng, 50, 64, 3000);
            let (target, truth) = keys[(trial % 50) as usize];
            let mut regular = CmRegular::with_dimensions(p.rows, p.cols);
            let mut fastpath = CmFast::with_dimensions(p.rows, p.cols);
            for &(id, w) in &keys {
                if fast {
                    fastpath.update_weighted(&key(id), w as i64);
                } else {
                    regular.update_weighted(&key(id), w as i64);
                }
            }
            let estimate = if fast {
                fastpath.estimate(&key(target))
            } else {
                regular.estimate(&key(target))
            };
            assert!(estimate >= truth as i64, "Count-Min undercounted");
            if (estimate - truth as i64) as f64 > bound.epsilon * l1(&keys) {
                failures += 1;
            }
        }
        assert!(
            failures <= allowed_failures(trials as usize, delta),
            "fast={fast}: {failures}/{trials} beyond eps={} at delta={delta}",
            bound.epsilon
        );
    }
}

#[test]
fn count_sketch_is_off_by_at_most_epsilon_l2_with_probability_one_minus_delta() {
    let p = MatrixParams { rows: 3, cols: 256 };
    let delta = 0.05;
    for fast in [false, true] {
        let bound = if fast {
            CsFast::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(delta))
        } else {
            CsRegular::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(delta))
        }
        .unwrap();
        assert_eq!(bound.metric, ErrorMetric::L2);
        assert_eq!(bound.delta, Some(delta));
        let trials = 800;
        let mut failures = 0;
        for trial in 0..trials {
            let mut rng = rng(2, trial);
            let keys = mixed_keys(&mut rng, 50, 64, 3000);
            let (target, truth) = keys[(trial % 50) as usize];
            let mut regular = CsRegular::with_dimensions(p.rows, p.cols);
            let mut fastpath = CsFast::with_dimensions(p.rows, p.cols);
            for &(id, w) in &keys {
                if fast {
                    fastpath.update_weighted(&key(id), w as i64);
                } else {
                    regular.update_weighted(&key(id), w as i64);
                }
            }
            let estimate = if fast {
                fastpath.estimate(&key(target))
            } else {
                regular.estimate(&key(target))
            };
            if (estimate - truth as f64).abs() > bound.epsilon * l2(&keys) {
                failures += 1;
            }
        }
        assert!(
            failures <= allowed_failures(trials as usize, delta),
            "fast={fast}: {failures}/{trials} beyond eps={} at delta={delta}",
            bound.epsilon
        );
    }
}

#[test]
fn l2_norm_is_within_relative_epsilon_with_probability_one_minus_delta() {
    let p = MatrixParams {
        rows: 5,
        cols: 1024,
    };
    let bound = CsL2::theoretical_error(StatisticKind::FrequencyL2, &p, &(), None).unwrap();
    assert_eq!(bound.metric, ErrorMetric::Relative);
    let delta = bound.delta.unwrap();
    assert_eq!(delta, p_med(5, 0.1));
    let trials = 800;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(3, trial);
        let keys = mixed_keys(&mut rng, 50, 64, 3000);
        let mut sketch = CsL2::with_dimensions(p.rows, p.cols);
        for &(id, w) in &keys {
            sketch.update_weighted(&key(id), w as i64);
        }
        let truth = l2(&keys);
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

/// The fraction of key pairs that share a column of a one-row `FastPath`
/// Count-Min of `cols` columns: `Σ c_j(c_j − 1)` over `K(K − 1)`, where the
/// estimates of the `K` unit keys sum to `Σ c_j²`.
fn fast_path_pair_collision_rate(cols: usize, salt: u64) -> f64 {
    let keys = 20_000u64;
    let mut rng = rng(salt, cols as u64);
    let ids: Vec<u64> = (0..keys).map(|_| rng.random()).collect();
    let mut sketch = CmFast::with_dimensions(1, cols);
    for &id in &ids {
        sketch.update_weighted(&key(id), 1);
    }
    let squares: i64 = ids.iter().map(|&id| sketch.estimate(&key(id))).sum();
    (squares as f64 - keys as f64) / (keys as f64 * (keys - 1) as f64)
}

#[test]
fn fast_path_non_power_of_two_widths_collide_more_than_one_over_w() {
    // At w = 48 a 6-bit slice modulo 48 puts 16 columns at twice the rate,
    // so the pair collision rate is (w + 3r)/(w + r)² = 9/(8w), above 1/w.
    for cols in [48usize, 40, 100] {
        let w = cols as f64;
        let r = (cols.next_power_of_two() - cols) as f64;
        let rate = fast_path_pair_collision_rate(cols, 4);
        let exact = (w + 3.0 * r) / ((w + r) * (w + r));
        assert!(
            (rate / exact - 1.0).abs() < 0.03,
            "w={cols}: {rate} vs {exact}"
        );
        let q = asap_sketchlib::analytics::count_matrix::collision_probability(cols, true);
        assert!(rate <= q * 1.03, "w={cols}: {rate} above q={q}");
        let p = MatrixParams { rows: 3, cols };
        let bound = CmFast::theoretical_error(StatisticKind::ItemFrequency, &p, &(), None).unwrap();
        assert!(
            (bound.epsilon - std::f64::consts::E * exact).abs() < 1e-12,
            "w={cols}: {bound:?}"
        );
        assert!(bound.epsilon >= std::f64::consts::E * rate / 1.03);
    }
    let rate = fast_path_pair_collision_rate(48, 5);
    assert!(rate > 1.08 / 48.0, "{rate}");
    assert_eq!(
        asap_sketchlib::analytics::count_matrix::collision_probability(48, true),
        9.0 / (8.0 * 48.0)
    );
}

#[test]
fn fast_path_count_min_at_a_non_power_of_two_width_overcounts_by_at_most_epsilon_n() {
    let p = MatrixParams { rows: 2, cols: 48 };
    let bound = CmFast::theoretical_error(StatisticKind::ItemFrequency, &p, &(), None).unwrap();
    let delta = bound.delta.unwrap();
    let trials = 600;
    let mut failures = 0;
    for trial in 0..trials {
        let mut rng = rng(6, trial);
        let keys = mixed_keys(&mut rng, 50, 64, 3000);
        let (target, truth) = keys[(trial % 50) as usize];
        let mut sketch = CmFast::with_dimensions(p.rows, p.cols);
        for &(id, w) in &keys {
            sketch.update_weighted(&key(id), w as i64);
        }
        let estimate = sketch.estimate(&key(target));
        assert!(estimate >= truth as i64, "Count-Min undercounted");
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
fn count_min_sizing_meets_the_target_and_one_step_smaller_does_not() {
    for (epsilon, delta) in [(0.1, 0.1), (0.01, 0.01), (0.001, 1e-4)] {
        for fast in [false, true] {
            let (p, bound) = if fast {
                let p =
                    CmFast::params_for(StatisticKind::ItemFrequency, epsilon, delta, &()).unwrap();
                (
                    p,
                    CmFast::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(delta)),
                )
            } else {
                let p = CmRegular::params_for(StatisticKind::ItemFrequency, epsilon, delta, &())
                    .unwrap();
                (
                    p,
                    CmRegular::theoretical_error(
                        StatisticKind::ItemFrequency,
                        &p,
                        &(),
                        Some(delta),
                    ),
                )
            };
            let bound = bound.unwrap();
            assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
            let narrower = MatrixParams {
                cols: if fast { p.cols / 2 } else { p.cols - 1 },
                ..p
            };
            let error = |q: &MatrixParams, delta| {
                if fast {
                    CmFast::theoretical_error(StatisticKind::ItemFrequency, q, &(), delta)
                } else {
                    CmRegular::theoretical_error(StatisticKind::ItemFrequency, q, &(), delta)
                }
            };
            let wider_error = error(&narrower, None).unwrap();
            assert!(wider_error.epsilon > epsilon, "{p:?} is wider than needed");
            if p.rows > 1 {
                let shallower = MatrixParams {
                    rows: p.rows - 1,
                    ..p
                };
                assert!(
                    error(&shallower, Some(delta)).is_none(),
                    "{p:?} is deeper than needed"
                );
            }
        }
    }
}

#[test]
fn count_sketch_sizing_meets_the_target_within_the_row_cap() {
    for (epsilon, delta) in [(0.1, 0.1), (0.05, 0.01), (0.01, 1e-4)] {
        let p = CsRegular::params_for(StatisticKind::ItemFrequency, epsilon, delta, &()).unwrap();
        assert!(p.rows % 2 == 1 && p.rows <= 19, "{p:?}");
        let bound =
            CsRegular::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(delta))
                .unwrap();
        assert!(bound.epsilon <= epsilon, "{p:?} gives {bound:?}");
        let fast = CsFast::params_for(StatisticKind::ItemFrequency, epsilon, delta, &()).unwrap();
        assert!(fast.cols.is_power_of_two());
        let bound =
            CsFast::theoretical_error(StatisticKind::ItemFrequency, &fast, &(), Some(delta))
                .unwrap();
        assert!(bound.epsilon <= epsilon, "{fast:?} gives {bound:?}");
    }
    // At δ = 0.01 the fewest counters take 5 rows of about 9.5/ε² columns.
    let p = CsRegular::params_for(StatisticKind::ItemFrequency, 0.01, 0.01, &()).unwrap();
    assert_eq!(p.rows, 5);
    assert!((p.cols as f64 * 1e-4 - 9.5).abs() < 0.1, "{p:?}");
}

#[test]
fn l2_norm_sizing_meets_the_target_and_one_step_smaller_does_not() {
    for (epsilon, delta) in [(0.2, 0.05), (0.1, 0.01), (0.05, 1e-3)] {
        let p = CsL2::params_for(StatisticKind::FrequencyL2, epsilon, delta, &()).unwrap();
        let bound =
            CsL2::theoretical_error(StatisticKind::FrequencyL2, &p, &(), Some(delta)).unwrap();
        assert!(bound.epsilon <= epsilon && bound.delta.unwrap() <= delta);
        let narrower = MatrixParams {
            cols: p.cols / 2,
            ..p
        };
        assert!(
            CsL2::theoretical_error(StatisticKind::FrequencyL2, &narrower, &(), None)
                .is_none_or(|b| b.epsilon > epsilon)
        );
        if p.rows > 1 {
            let shallower = MatrixParams {
                rows: p.rows - 1,
                ..p
            };
            assert!(
                CsL2::theoretical_error(StatisticKind::FrequencyL2, &shallower, &(), Some(delta))
                    .is_none()
            );
        }
    }
}

/// At (0.02, 0.01) the fewest counters take 9 rows of 16384, 135 hash bits,
/// and a shape with more counters fits the 128.
#[test]
fn l2_norm_item_sizing_takes_the_fewest_counters_that_fit_the_hash() {
    let (epsilon, delta) = (0.02, 0.01);
    let fewest = CsFast::params_for(StatisticKind::ItemFrequency, epsilon, delta, &()).unwrap();
    assert!(
        fewest.rows * (fewest.cols.ilog2() as usize + 1) > 128,
        "{fewest:?}"
    );
    let p = CsL2::params_for(StatisticKind::ItemFrequency, epsilon, delta, &()).unwrap();
    assert!(p.rows * (p.cols.ilog2() as usize + 1) <= 128, "{p:?}");
    let bound =
        CsL2::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(delta)).unwrap();
    assert!(bound.epsilon <= epsilon);
}

#[test]
fn count_sketch_sizing_gives_a_column_for_huge_epsilon() {
    for epsilon in [1e100, 1e200, f64::INFINITY] {
        let p = CsRegular::params_for(StatisticKind::ItemFrequency, epsilon, 0.01, &()).unwrap();
        assert!(p.cols >= 1, "{epsilon}: {p:?}");
        let bound = CsRegular::theoretical_error(StatisticKind::ItemFrequency, &p, &(), Some(0.01));
        assert!(bound.unwrap().epsilon <= epsilon);
    }
}

#[test]
fn total_is_exact_and_unanswered_statistics_have_no_bound() {
    let p = MatrixParams::default();
    assert_eq!(
        CmRegular::theoretical_error(StatisticKind::Total, &p, &(), None),
        Some(asap_sketchlib::ErrorBound::EXACT)
    );
    assert_eq!(
        CsRegular::theoretical_error(StatisticKind::Total, &p, &(), None),
        None
    );
    assert_eq!(
        CmRegular::theoretical_query_ops(StatisticKind::Quantile, &p, &()),
        None
    );
}

#[cfg(feature = "op-count")]
mod ops {
    use super::*;

    fn loaded<S>(mut sketch: S, update: impl Fn(&mut S, u64)) -> S {
        for id in 0..500 {
            update(&mut sketch, id);
        }
        sketch
    }

    macro_rules! matrix_ops {
        ($name:ident, $ty:ty, $stats:expr) => {
            #[test]
            fn $name() {
                for p in SIZES {
                    let mut a = loaded(<$ty>::with_dimensions(p.rows, p.cols), |s, id| {
                        s.update(&key(id))
                    });
                    let b = a.clone();
                    let update = <$ty>::theoretical_update_ops(&p, &());
                    assert_ops_within("update", update, || a.update(&key(7)));
                    let query = <$ty>::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &())
                        .unwrap();
                    assert_ops_within("query", query, || a.estimate(&key(7)));
                    for (statistic, read) in $stats {
                        let bound = <$ty>::theoretical_query_ops(statistic, &p, &()).unwrap();
                        assert_ops_within("query", bound, || read(&a));
                    }
                    let merge = <$ty>::theoretical_merge_ops(&p, &()).unwrap();
                    assert_ops_within("merge", merge, || a.merge(&b));
                }
            }
        };
    }

    type Read<S> = fn(&S) -> f64;
    matrix_ops!(
        count_min_regular,
        CmRegular,
        [(StatisticKind::Total, (|s| s.total()) as Read<CmRegular>)]
    );
    matrix_ops!(
        count_min_fast,
        CmFast,
        [(StatisticKind::Total, (|s| s.total()) as Read<CmFast>)]
    );
    matrix_ops!(
        count_sketch_regular,
        CsRegular,
        [] as [(StatisticKind, Read<CsRegular>); 0]
    );
    matrix_ops!(
        count_sketch_fast,
        CsFast,
        [] as [(StatisticKind, Read<CsFast>); 0]
    );

    #[test]
    fn count_sketch_with_l2_norm() {
        for p in SIZES {
            let mut a = loaded(CsL2::with_dimensions(p.rows, p.cols), |s, id| {
                s.update_weighted(&key(id), 1)
            });
            let b = a.clone();
            let update = CsL2::theoretical_update_ops(&p, &());
            assert_ops_within("update", update, || a.update_weighted(&key(7), 1));
            for (statistic, read) in [
                (
                    StatisticKind::ItemFrequency,
                    (|s: &CsL2| s.estimate(&key(7))) as Read<CsL2>,
                ),
                (StatisticKind::FrequencyL2, |s: &CsL2| s.l2()),
            ] {
                let bound = CsL2::theoretical_query_ops(statistic, &p, &()).unwrap();
                assert_ops_within("query", bound, || read(&a));
            }
            let merge = CsL2::theoretical_merge_ops(&p, &()).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b));
        }
    }
}

#[cfg(feature = "experimental")]
mod experimental {
    use super::*;
    use asap_sketchlib::analytics::{CountMinHllParams, MicroCmData, MicroCmParams};
    use asap_sketchlib::sketches::microscope::{MicroParams, SubWindowClock};
    use asap_sketchlib::{CountMinHll, MicroCm};

    type Hll = CountMinHll;
    type Micro = MicroCm;

    const HLL_SIZES: [CountMinHllParams; 2] = [
        CountMinHllParams {
            rows: 2,
            cols: 16,
            precision: 4,
        },
        CountMinHllParams {
            rows: 3,
            cols: 64,
            precision: 8,
        },
    ];

    const MICRO_SIZES: [(MicroCmParams, u64); 2] = [
        (
            MicroCmParams {
                rows: 2,
                cols: 16,
                t: 4,
                c: 2,
            },
            32,
        ),
        (
            MicroCmParams {
                rows: 4,
                cols: 64,
                t: 12,
                c: 3,
            },
            256,
        ),
    ];

    fn micro(p: MicroCmParams, per_sub_window: u64) -> MicroCm {
        MicroCm::with_dimensions(
            p.rows,
            p.cols,
            MicroParams::new(p.t, p.c),
            SubWindowClock::count_based(per_sub_window),
            7,
        )
    }

    #[test]
    fn memory_matches_the_storage() {
        for p in HLL_SIZES {
            let sketch = Hll::with_dimensions(p.rows, p.cols, p.precision);
            assert_eq!(
                <Hll as SketchAnalytics>::theoretical_memory_bytes(&p, &()),
                Some(sketch.memory_bytes())
            );
        }
        for (p, per) in MICRO_SIZES {
            let sketch = micro(p, per);
            let data = MicroCmData {
                items_per_sub_window: Some(per),
            };
            assert_eq!(
                <Micro as SketchAnalytics>::theoretical_memory_bytes(&p, &data),
                Some(sketch.memory_bytes())
            );
        }
    }

    #[cfg(feature = "op-count")]
    #[test]
    fn count_min_hll_ops() {
        for p in HLL_SIZES {
            let mut a = Hll::with_dimensions(p.rows, p.cols, p.precision);
            for id in 0..300 {
                a.update(&key(id % 17), &key(id));
            }
            let b = a.clone();
            let update = Hll::theoretical_update_ops(&p, &());
            assert_ops_within("update", update, || a.update(&key(1), &key(99)));
            let query = Hll::theoretical_query_ops(StatisticKind::KeyDistinct, &p, &()).unwrap();
            assert_ops_within("query", query, || a.estimate(&key(1)));
            let merge = Hll::theoretical_merge_ops(&p, &()).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b).unwrap());
        }
    }

    #[cfg(feature = "op-count")]
    #[test]
    fn micro_cm_ops() {
        for (p, per) in MICRO_SIZES {
            let data = MicroCmData {
                items_per_sub_window: Some(per),
            };
            let mut a = micro(p, per);
            let mut b = micro(p, per);
            let updates = per * (p.t as u64 + 2);
            let ((), ops) = asap_sketchlib::analytics::ops::count(|| {
                for id in 0..updates {
                    a.update(&key(id % 23));
                    b.update(&key(id % 29));
                }
            });
            let average = ops as f64 / (2 * updates) as f64;
            let bound = Micro::theoretical_update_ops(&p, &data);
            assert!(
                average <= bound,
                "update averages {average}, formula {bound}"
            );
            let query =
                Micro::theoretical_query_ops(StatisticKind::ItemFrequency, &p, &data).unwrap();
            assert_ops_within("query", query, || a.estimate(&key(3)));
            let merge = Micro::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b).unwrap());
        }
    }
}
