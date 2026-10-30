//! Kll, KllDynamic, DdSketch and UniformSampling.

use crate::support::*;
use asap_sketchlib::analytics::{DdData, DdParams, KllParams, StreamData, normal_quantile};
use asap_sketchlib::{
    DdSketch, ErrorBound, ErrorMetric, Kll, KllDynamic, SketchAnalytics, StatisticKind,
};
use rand::Rng;
use rand::rngs::StdRng;

/// What the tests need from Kll and KllDynamic.
trait KllLike: SketchAnalytics<Params = KllParams, Data = StreamData> + Sized {
    fn seeded(p: KllParams, seed: u64) -> Self;
    fn add(&mut self, value: f64);
    fn absorb(&mut self, other: &Self);
    fn quantile_at(&self, q: f64) -> f64;
    fn rank_of(&self, x: f64) -> usize;
    /// `memory_bytes()`, after caching a CDF where the sketch keeps one.
    fn bytes(&mut self) -> usize;
}

impl KllLike for Kll {
    fn seeded(p: KllParams, seed: u64) -> Self {
        Kll::with_seed(p.k, p.m, seed)
    }
    fn add(&mut self, value: f64) {
        self.update(&value);
    }
    fn absorb(&mut self, other: &Self) {
        self.merge(other);
    }
    fn quantile_at(&self, q: f64) -> f64 {
        self.quantile(q)
    }
    fn rank_of(&self, x: f64) -> usize {
        self.rank(x)
    }
    fn bytes(&mut self) -> usize {
        self.cdf_cached();
        self.memory_bytes()
    }
}

impl KllLike for KllDynamic {
    fn seeded(p: KllParams, seed: u64) -> Self {
        KllDynamic::with_seed(p.k, p.m, seed)
    }
    fn add(&mut self, value: f64) {
        self.update(&value);
    }
    fn absorb(&mut self, other: &Self) {
        self.merge(other);
    }
    fn quantile_at(&self, q: f64) -> f64 {
        self.quantile(q)
    }
    fn rank_of(&self, x: f64) -> usize {
        self.rank(x)
    }
    fn bytes(&mut self) -> usize {
        self.memory_bytes()
    }
}

fn stream(n: usize) -> StreamData {
    StreamData { n: Some(n as u64) }
}

/// `n` fresh values in [0, 1), from a splitmix64 stream seeded by `rng`.
fn fresh_values(rng: &mut StdRng, n: usize) -> Vec<f64> {
    let mut state: u64 = rng.random();
    (0..n)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            ((z ^ (z >> 31)) >> 11) as f64 / (1u64 << 53) as f64
        })
        .collect()
}

/// A state over `values`: one sketch, or `parts` independently seeded
/// sketches over disjoint slices merged into the first.
fn kll_state<S: KllLike>(p: KllParams, rng: &mut StdRng, values: &[f64], parts: usize) -> S {
    let mut states = values.chunks(values.len().div_ceil(parts)).map(|chunk| {
        let mut sketch = S::seeded(p, rng.random());
        chunk.iter().for_each(|&v| sketch.add(v));
        sketch
    });
    let mut merged = states.next().unwrap();
    states.for_each(|other| merged.absorb(&other));
    merged
}

fn kll_memory_bounds_memory_bytes<S: KllLike>() {
    for p in [
        KllParams { k: 64, m: 8 },
        KllParams { k: 200, m: 2 },
        KllParams { k: 30, m: 20 },
    ] {
        for n in [0, 10, 300, 5000, 100_000] {
            for parts in [1, 3] {
                let values = fresh_values(&mut rng(30, n as u64), n);
                let mut sketch: S = if n == 0 {
                    S::seeded(p, 1)
                } else {
                    kll_state(p, &mut rng(31, n as u64), &values, parts)
                };
                let held = sketch.bytes();
                assert!(
                    S::theoretical_memory_bytes(&p, &stream(n)).unwrap() >= held,
                    "{p:?} n={n} parts={parts}"
                );
                assert!(S::theoretical_memory_bytes(&p, &StreamData::default()).unwrap() >= held);
            }
        }
    }
}

#[test]
fn kll_memory_bounds_memory_bytes_through_merges() {
    kll_memory_bounds_memory_bytes::<Kll>();
}

#[test]
fn kll_dynamic_memory_bounds_memory_bytes_through_merges() {
    kll_memory_bounds_memory_bytes::<KllDynamic>();
}

/// Over `trials` fresh streams of `n` values, half of them merges of four
/// independently seeded states, counts the trials whose quantile at a
/// varying `φ` and whose rank of a varying `x` miss their bounds.
fn kll_failures<S: KllLike>(salt: u64, p: KllParams, n: usize, trials: u64) -> (usize, usize) {
    let none = StreamData::default();
    let quantile = S::theoretical_error(StatisticKind::Quantile, &p, &none, None).unwrap();
    let rank = S::theoretical_error(StatisticKind::Rank, &p, &none, None).unwrap();
    assert_eq!(quantile.metric, ErrorMetric::Rank);
    assert_eq!(rank.metric, ErrorMetric::L1);
    let (mut quantile_failures, mut rank_failures) = (0, 0);
    for trial in 0..trials {
        let mut rng = rng(salt, trial);
        let values = fresh_values(&mut rng, n);
        let phi = rng.random_range(0.05..0.95);
        let x: f64 = rng.random();
        let sketch: S = kll_state(p, &mut rng, &values, if trial % 2 == 0 { 1 } else { 4 });
        let at_or_below = |v: f64| values.iter().filter(|&&y| y <= v).count() as f64;
        let returned = sketch.quantile_at(phi);
        if (at_or_below(returned) / n as f64 - phi).abs() > quantile.epsilon {
            quantile_failures += 1;
        }
        if (sketch.rank_of(x) as f64 - at_or_below(x)).abs() > rank.epsilon * n as f64 {
            rank_failures += 1;
        }
    }
    (quantile_failures, rank_failures)
}

fn kll_is_within_epsilon_with_probability_one_minus_delta<S: KllLike>(salt: u64, k: usize) {
    let trials = 1000;
    let p = KllParams { k, m: 8 };
    let (quantile, rank) = kll_failures::<S>(salt, p, 100_000, trials);
    let allowed = allowed_failures(trials as usize, 0.01);
    assert!(
        quantile <= allowed,
        "k={k}: {quantile} quantile failures, allowed {allowed}"
    );
    assert!(
        rank <= allowed,
        "k={k}: {rank} rank failures, allowed {allowed}"
    );
}

#[test]
fn kll_quantile_and_rank_at_k_64_are_within_epsilon_with_probability_one_minus_delta() {
    kll_is_within_epsilon_with_probability_one_minus_delta::<Kll>(40, 64);
}

#[test]
fn kll_quantile_and_rank_at_k_200_are_within_epsilon_with_probability_one_minus_delta() {
    kll_is_within_epsilon_with_probability_one_minus_delta::<Kll>(41, 200);
}

#[test]
fn kll_dynamic_quantile_and_rank_at_k_64_are_within_epsilon_with_probability_one_minus_delta() {
    kll_is_within_epsilon_with_probability_one_minus_delta::<KllDynamic>(50, 64);
}

#[test]
fn kll_dynamic_quantile_and_rank_at_k_200_are_within_epsilon_with_probability_one_minus_delta() {
    kll_is_within_epsilon_with_probability_one_minus_delta::<KllDynamic>(51, 200);
}

fn kll_bound(statistic: StatisticKind, p: KllParams, delta: f64) -> Option<ErrorBound> {
    <Kll as SketchAnalytics>::theoretical_error(statistic, &p, &StreamData::default(), Some(delta))
}

#[test]
fn kll_bounds_follow_the_fits_and_need_m_at_least_8() {
    let none = StreamData::default();
    for (statistic, constant, power, contract, scaled) in [
        (
            StatisticKind::Quantile,
            2.296,
            0.9723,
            "kll_quantile_empirical_99_v1",
            "kll_quantile_empirical_99_delta_scaled_v1",
        ),
        (
            StatisticKind::Rank,
            2.07,
            0.9646,
            "kll_rank_measured_99_v1",
            "kll_rank_measured_99_delta_scaled_v1",
        ),
    ] {
        let p = KllParams { k: 200, m: 8 };
        let at = |delta| kll_bound(statistic, p, delta).unwrap();
        let fit = constant / 200f64.powf(power);
        let default =
            <Kll as SketchAnalytics>::theoretical_error(statistic, &p, &none, None).unwrap();
        assert_eq!(default, at(0.01));
        assert!((default.epsilon - fit).abs() < 1e-12);
        assert_eq!(default.contract, contract);
        assert_eq!(at(0.05).epsilon, default.epsilon);
        assert_eq!(at(0.05).delta, Some(0.05));
        let tight = at(0.001);
        assert_eq!(tight.contract, scaled);
        let s = normal_quantile(1.0 - 0.0005) / normal_quantile(0.995);
        assert!((tight.epsilon - s * fit).abs() < 1e-12);
        assert_eq!(
            <KllDynamic as SketchAnalytics>::theoretical_error(statistic, &p, &none, Some(0.001)),
            Some(tight)
        );
        for m in [1, 2, 7] {
            assert_eq!(kll_bound(statistic, KllParams { k: 200, m }, 0.01), None);
        }
        assert!(kll_bound(statistic, KllParams { k: 3, m: 9 }, 0.01).is_some());
        assert!(kll_bound(statistic, KllParams { k: 5, m: 40_000 }, 0.01).is_some());
        assert_eq!(kll_bound(statistic, p, 0.0), None);
    }
    assert_eq!(
        kll_bound(StatisticKind::Distinct, KllParams::default(), 0.01),
        None
    );
}

#[test]
fn kll_sizing_meets_the_target_and_one_smaller_k_does_not() {
    let none = StreamData::default();
    for statistic in [StatisticKind::Quantile, StatisticKind::Rank] {
        for epsilon in [0.05, 0.01, 0.002] {
            for delta in [0.05, 0.01, 1e-4] {
                let p =
                    <Kll as SketchAnalytics>::params_for(statistic, epsilon, delta, &none).unwrap();
                assert_eq!(p.m, 8);
                assert!(kll_bound(statistic, p, delta).unwrap().epsilon <= epsilon);
                let smaller = KllParams { k: p.k - 1, ..p };
                assert!(
                    kll_bound(statistic, smaller, delta).unwrap().epsilon > epsilon,
                    "{statistic:?} eps={epsilon} delta={delta} k={}",
                    p.k
                );
                assert_eq!(
                    <KllDynamic as SketchAnalytics>::params_for(statistic, epsilon, delta, &none),
                    Some(p)
                );
            }
        }
        assert_eq!(
            <Kll as SketchAnalytics>::params_for(statistic, 1e-5, 0.01, &none),
            None
        );
    }
}

/// `n` values with magnitudes log-uniform in `[10^lo, 10^hi)`, each negative
/// with probability `negative_share`.
fn dd_values(rng: &mut StdRng, n: usize, (lo, hi): (f64, f64), negative_share: f64) -> Vec<f64> {
    (0..n)
        .map(|_| {
            let magnitude = 10f64.powf(rng.random_range(lo..hi));
            if rng.random_bool(negative_share) {
                -magnitude
            } else {
                magnitude
            }
        })
        .collect()
}

const WIDE: (f64, f64) = (-3.0, 6.0);

fn dd_state(alpha: f64, values: &[f64], merged: bool) -> DdSketch {
    let (left, right) = values.split_at(if merged {
        values.len() / 3
    } else {
        values.len()
    });
    let mut sketch = DdSketch::new(alpha);
    left.iter().for_each(|v| sketch.add(v));
    let mut other = DdSketch::new(alpha);
    right.iter().for_each(|v| other.add(v));
    sketch.merge(&other).unwrap();
    sketch
}

fn dd_data(values: &[f64]) -> DdData {
    let magnitudes = || values.iter().map(|v| v.abs());
    DdData {
        n: Some(values.len() as u64),
        min_magnitude: Some(magnitudes().fold(f64::INFINITY, f64::min)),
        max_magnitude: Some(magnitudes().fold(0.0, f64::max)),
        negatives: Some(values.iter().any(|&v| v < 0.0)),
    }
}

const PHIS: [f64; 9] = [0.0, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0];

/// δ = 0: every quantile of every trial is within `α` relative of the true
/// `⌈φ·n⌉`-th value.
#[test]
fn ddsketch_quantile_is_within_alpha_relative_on_every_trial() {
    for trial in 0..200u64 {
        let mut rng = rng(60, trial);
        let alpha = if trial % 2 == 0 { 0.01 } else { 0.05 };
        let values = dd_values(&mut rng, 2000, WIDE, if trial % 4 < 2 { 0.0 } else { 0.3 });
        let p = DdParams { alpha };
        let bound =
            DdSketch::theoretical_error(StatisticKind::Quantile, &p, &dd_data(&values), None)
                .unwrap();
        assert_eq!(
            (bound.metric, bound.delta),
            (ErrorMetric::Relative, Some(0.0))
        );
        assert_eq!(bound.contract, "ddsketch_relative_error_alpha_v1");
        let sketch = dd_state(alpha, &values, trial % 3 == 0);
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        let n = sorted.len();
        for phi in PHIS.into_iter().chain([rng.random()]) {
            let truth = sorted[((phi * n as f64).ceil() as usize).clamp(1, n) - 1];
            let estimate = sketch.get_value_at_quantile(phi).unwrap();
            assert!(
                (estimate - truth).abs() <= bound.epsilon * truth.abs() * (1.0 + 1e-9),
                "trial {trial} phi {phi}: {estimate} vs {truth}"
            );
        }
    }
}

/// δ = 0: every interpolated quantile is within `α` relative when the values
/// share a sign, and within `α·A` otherwise.
#[test]
fn ddsketch_interpolated_quantile_is_within_its_bound_on_every_trial() {
    for trial in 0..200u64 {
        let mut rng = rng(61, trial);
        let alpha = if trial % 2 == 0 { 0.01 } else { 0.05 };
        let signed = trial % 4 >= 2;
        // Signed magnitudes sit near `A`, where `α·A` is tight.
        let values = if signed {
            dd_values(&mut rng, 2000, (5.98, 6.0), 0.4)
        } else {
            dd_values(&mut rng, 2000, WIDE, 0.0)
        };
        let data = dd_data(&values);
        let bound = DdSketch::theoretical_error(
            StatisticKind::QuantileInterpolated,
            &DdParams { alpha },
            &data,
            None,
        )
        .unwrap();
        assert_eq!(bound.delta, Some(0.0));
        if signed {
            assert_eq!(bound.metric, ErrorMetric::ABSOLUTE);
            assert_eq!(bound.contract, "ddsketch_interpolated_absolute_alpha_v1");
            assert_eq!(bound.epsilon, alpha * data.max_magnitude.unwrap());
        } else {
            assert_eq!(bound.metric, ErrorMetric::Relative);
            assert_eq!(bound.contract, "ddsketch_interpolated_relative_alpha_v1");
        }
        let sketch = dd_state(alpha, &values, trial % 3 == 0);
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        let last = (sorted.len() - 1) as f64;
        for phi in PHIS.into_iter().chain([rng.random()]) {
            let rank = phi * last;
            let (lower, upper) = (sorted[rank.floor() as usize], sorted[rank.ceil() as usize]);
            let fraction = rank - rank.floor();
            let truth = lower * (1.0 - fraction) + upper * fraction;
            let estimate = sketch.quantile_interpolated(phi).unwrap().unwrap();
            let allowed = if signed {
                bound.epsilon
            } else {
                bound.epsilon * truth.abs()
            };
            assert!(
                (estimate - truth).abs() <= allowed * (1.0 + 1e-9),
                "trial {trial} phi {phi}: {estimate} vs {truth}"
            );
        }
    }
}

#[test]
fn ddsketch_interpolated_bound_needs_a_for_signed_or_unknown_values() {
    let p = DdParams { alpha: 0.02 };
    let interpolated = |data: &DdData| {
        DdSketch::theoretical_error(StatisticKind::QuantileInterpolated, &p, data, None)
    };
    assert_eq!(interpolated(&DdData::default()), None);
    let signed = DdData {
        negatives: Some(true),
        max_magnitude: Some(50.0),
        ..DdData::default()
    };
    assert_eq!(interpolated(&signed).unwrap().epsilon, 1.0);
    assert_eq!(
        DdSketch::theoretical_error(StatisticKind::Quantile, &p, &DdData::default(), Some(0.1))
            .unwrap()
            .delta,
        Some(0.0)
    );
    for statistic in [
        StatisticKind::Total,
        StatisticKind::Rank,
        StatisticKind::Sample,
    ] {
        assert_eq!(
            DdSketch::theoretical_error(statistic, &p, &signed, None),
            None
        );
    }
}

#[test]
fn ddsketch_sizing_meets_the_target() {
    let positive = DdData {
        negatives: Some(false),
        ..DdData::default()
    };
    let signed = DdData {
        negatives: Some(true),
        max_magnitude: Some(200.0),
        ..DdData::default()
    };
    for (statistic, data) in [
        (StatisticKind::Quantile, DdData::default()),
        (StatisticKind::QuantileInterpolated, positive),
        (StatisticKind::QuantileInterpolated, signed),
    ] {
        for epsilon in [0.05, 0.01, 0.001] {
            let p = DdSketch::params_for(statistic, epsilon, 0.0, &data).unwrap();
            let bound = DdSketch::theoretical_error(statistic, &p, &data, None).unwrap();
            assert!(bound.epsilon <= epsilon * (1.0 + 1e-12));
            assert!(bound.epsilon >= epsilon * (1.0 - 1e-12));
        }
    }
    assert_eq!(
        DdSketch::params_for(StatisticKind::Quantile, 1.0, 0.0, &DdData::default()),
        None
    );
    assert_eq!(
        DdSketch::params_for(
            StatisticKind::QuantileInterpolated,
            0.01,
            0.0,
            &DdData::default()
        ),
        None
    );
}

#[test]
fn ddsketch_memory_bounds_memory_bytes() {
    for alpha in [0.01, 0.05] {
        for (n, negative_share) in [(1, 0.0), (500, 0.0), (5000, 0.3)] {
            let values = dd_values(&mut rng(62, n as u64), n, WIDE, negative_share);
            for merged in [false, true] {
                let sketch = dd_state(alpha, &values, merged);
                let p = DdParams { alpha };
                let known =
                    <DdSketch as SketchAnalytics>::theoretical_memory_bytes(&p, &dd_data(&values));
                assert!(known.unwrap() >= sketch.memory_bytes());
                let unknown =
                    <DdSketch as SketchAnalytics>::theoretical_memory_bytes(&p, &DdData::default());
                assert!(unknown.unwrap() >= sketch.memory_bytes());
            }
        }
    }
}

#[cfg(feature = "op-count")]
mod ops {
    use super::*;

    fn kll_ops<S: KllLike>() {
        for p in [
            KllParams { k: 64, m: 8 },
            KllParams { k: 200, m: 8 },
            KllParams { k: 32, m: 2 },
        ] {
            for n in [p.k / 2, 10 * p.k, 50_000] {
                let data = stream(n);
                let values = fresh_values(&mut rng(70, n as u64), n);
                let mut a = S::seeded(p, 1);
                let average = S::theoretical_update_ops(&p, &data);
                assert_ops_within("updates", average * n as f64, || {
                    values.iter().for_each(|&v| a.add(v))
                });
                let unknown = S::theoretical_update_ops(&p, &StreamData::default());
                assert!(unknown >= average);
                for statistic in [StatisticKind::Quantile, StatisticKind::Rank] {
                    let bound = S::theoretical_query_ops(statistic, &p, &data).unwrap();
                    assert!(
                        S::theoretical_query_ops(statistic, &p, &StreamData::default()).unwrap()
                            >= bound
                    );
                    assert_ops_within("query", bound, || match statistic {
                        StatisticKind::Quantile => a.quantile_at(0.3),
                        _ => a.rank_of(0.3) as f64,
                    });
                }
                let mut b = S::seeded(p, 2);
                values.iter().for_each(|&v| b.add(1.0 - v));
                let merge = S::theoretical_merge_ops(&p, &data).unwrap();
                assert_ops_within("merge", merge, || a.absorb(&b));
            }
        }
    }

    #[test]
    fn kll() {
        kll_ops::<Kll>();
    }

    #[test]
    fn kll_dynamic() {
        kll_ops::<KllDynamic>();
    }

    #[test]
    fn ddsketch() {
        for alpha in [0.01, 0.05] {
            let p = DdParams { alpha };
            let values = dd_values(&mut rng(71, 0), 5000, WIDE, 0.3);
            let data = dd_data(&values);
            let mut a = dd_state(alpha, &values[..2500], false);
            let b = dd_state(alpha, &values[2500..], false);
            let update = DdSketch::theoretical_update_ops(&p, &data);
            for v in [1.0, -2.5, 0.0, 1e-300, 1e9, -1e-4] {
                assert_ops_within("update", update, || a.add(&v));
            }
            let mut a = dd_state(alpha, &values[..2500], false);
            let quantile = DdSketch::theoretical_query_ops(StatisticKind::Quantile, &p, &data);
            assert_ops_within("quantile", quantile.unwrap(), || {
                a.get_value_at_quantile(1.0)
            });
            let interpolated =
                DdSketch::theoretical_query_ops(StatisticKind::QuantileInterpolated, &p, &data);
            assert_ops_within("interpolated", interpolated.unwrap(), || {
                a.quantile_interpolated(0.999_9).unwrap()
            });
            let merge = DdSketch::theoretical_merge_ops(&p, &data).unwrap();
            assert_ops_within("merge", merge, || a.merge(&b).unwrap());
            let mut empty = DdSketch::new(alpha);
            assert_ops_within("merge into empty", merge, || empty.merge(&b).unwrap());
        }
    }
}

#[cfg(feature = "experimental")]
mod uniform {
    use super::*;
    use asap_sketchlib::UniformSampling;
    use asap_sketchlib::analytics::UniformParams;

    #[test]
    fn memory_bounds_memory_bytes_through_merges() {
        for sample_rate in [0.1, 0.5, 1.0] {
            let p = UniformParams { sample_rate };
            for n in [1u64, 37, 1000, 20_000] {
                let mut sampler = UniformSampling::with_seed(sample_rate, 3);
                let mut other = UniformSampling::with_seed(sample_rate, 4);
                for i in 0..n {
                    if i % 3 == 0 {
                        other.update(i as f64);
                    } else {
                        sampler.update(i as f64);
                    }
                }
                let alone = sampler.clone();
                sampler.merge(&other).unwrap();
                for (sampler, n) in [(sampler, n), (alone, n - n.div_ceil(3))] {
                    let bound =
                        UniformSampling::theoretical_memory_bytes(&p, &StreamData { n: Some(n) });
                    assert!(
                        bound.unwrap() >= sampler.memory_bytes(),
                        "rho={sample_rate} n={n}"
                    );
                }
            }
            let unknown = UniformSampling::theoretical_memory_bytes(&p, &StreamData::default());
            assert_eq!(unknown, None);
        }
    }

    #[test]
    fn sample_has_no_bound_and_total_is_not_a_readout() {
        let p = UniformParams::default();
        let data = StreamData { n: Some(100) };
        for statistic in [StatisticKind::Sample, StatisticKind::Total] {
            assert_eq!(
                UniformSampling::theoretical_error(statistic, &p, &data, None),
                None
            );
            assert_eq!(
                UniformSampling::params_for(statistic, 0.1, 0.01, &data),
                None
            );
        }
        assert!(UniformSampling::theoretical_query_ops(StatisticKind::Sample, &p, &data).is_some());
        assert_eq!(
            UniformSampling::theoretical_query_ops(StatisticKind::Total, &p, &data),
            None
        );
    }

    #[cfg(feature = "op-count")]
    #[test]
    fn ops() {
        for sample_rate in [0.1, 0.5] {
            let p = UniformParams { sample_rate };
            for n in [100u64, 3000] {
                let mut a = UniformSampling::with_seed(sample_rate, 5);
                let mut b = UniformSampling::with_seed(sample_rate, 6);
                for i in 0..n {
                    let update =
                        UniformSampling::theoretical_update_ops(&p, &StreamData { n: Some(i) });
                    assert_ops_within("update", update, || a.update(i as f64));
                    b.update(-(i as f64));
                }
                let data = StreamData { n: Some(n) };
                let sample =
                    UniformSampling::theoretical_query_ops(StatisticKind::Sample, &p, &data);
                assert_ops_within("sample", sample.unwrap(), || a.samples());
                let merge = UniformSampling::theoretical_merge_ops(&p, &data).unwrap();
                assert_ops_within("merge", merge, || a.merge(&b).unwrap());
            }
        }
    }
}
