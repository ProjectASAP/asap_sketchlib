//! Nitro sampling, end to end: the row-level accelerator behind
//! `CountMin::fast_insert_nitro` / `Count::fast_insert_nitro`, and the
//! `NitroBatch` wrapper's two ingestion paths.
//!
//! # What Nitro promises, and what that makes testable
//!
//! Nitro admits each unit of work with probability `p` and compensates by
//! writing a weight of `1/p`. Counters are integers, so the weight is rounded
//! **stochastically** — `floor(1/p) + Bernoulli(frac(1/p))` — which makes the
//! estimator unbiased at every rate rather than only at rates whose reciprocal
//! is an integer. With `r = frac(1/p)` and `f` units of true work:
//!
//! ```text
//!   E[estimate]   = f
//!   Var[estimate] = f * ( (1-p)/p  +  p * r * (1-r) )
//!                       \_________/    \___________/
//!                        admission       rounding
//! ```
//!
//! Every band below is `z` standard deviations of that, computed per rate from
//! the sketch's own configuration. Nothing here is a percentage.
//!
//! # The unit of work differs between the two APIs
//!
//! - **Row-level** (`fast_insert_nitro`): NitroSketch's per-row sampling. A
//!   `d`-row sketch turns `f` updates of a key into `f * d` *row slots*, each
//!   admitted independently. So row `r`'s counter is an estimate of `f` from
//!   `f` slots, and the whole sketch's admitted mass is an estimate of `f * d`
//!   from `f * d` slots.
//! - **`NitroBatch`**: samples per *update*, and an admitted update writes its
//!   weight into every row. So the unit is `f`, and every row carries the same
//!   total.
//!
//! Getting this wrong by a factor of `d` is exactly the kind of error these
//! tests exist to catch, so the true value is written out per API rather than
//! shared.
//!
//! # Trial units
//!
//! The randomness is the sampling schedule. One `(rate, seed)` pair is one
//! draw; the rows *inside* one sketch share an interleaved skip stream and are
//! not independent, so a trial's rows are reduced to a single pass/fail before
//! the binomial runs over seeds.
//!
//! Seeds are spaced so that no two trials read overlapping stretches of the
//! shared skip table — see `TRIAL_STRIDE`.

mod common;

use common::specs::{CountMinSpec, SIMULTANEOUS_LEVEL, SamplingConfidenceSpec, Tally};
use common::{FreqTruth, zipf_u64};

use asap_sketchlib::{Count, CountMin, DataInput, FastPath, Nitro, NitroBatch, Vector2D};

/// Every rate the public constructors accept that the suite covers.
///
/// `0.3` and `0.07` are the load-bearing ones: their reciprocals are not
/// integers, so they are the only rates where the rounding correction is
/// observable at all. `1.0` is the degenerate full-sampling case.
const RATES: [f64; 6] = [1.0, 0.5, 0.3, 0.1, 0.07, 0.01];

/// Gaussian quantile for the sampling bands. Two-sided 6.3e-5 per trial under
/// the normal approximation to a sum of `f` i.i.d. weighted admissions.
const Z: f64 = 4.0;

const TRIALS: usize = 12;

/// Distance between two trials' starting offsets in the shared skip table.
///
/// A trial consumes one table entry per admission. `WORK_PER_TRIAL` fixes the
/// admission count at roughly 1000 per row-level trial regardless of rate, so a
/// 5000-entry stride leaves every trial reading a disjoint stretch and the
/// twelve of them stay inside the 65536-entry table without wrapping into each
/// other.
const TRIAL_STRIDE: u64 = 5_000;

fn trial_seed(trial: usize) -> u64 {
    TRIAL_STRIDE * trial as u64 + 1
}

/// Admissions each trial should make, held constant across rates.
///
/// The true count is `WORK_PER_TRIAL / p`, so `f * p` — the admission count,
/// and therefore the table consumption — does not move with the rate, while the
/// relative width of the band stays usable at every rate (about 9% at `p = 0.5`
/// and 13% at `p = 0.01`). Holding `f` fixed instead would make the band at
/// `p = 0.01` wider than 100%, which would test nothing.
const WORK_PER_TRIAL: f64 = 1_000.0;

fn true_count_for(rate: f64) -> usize {
    (WORK_PER_TRIAL / rate).ceil() as usize
}

const ROWS: usize = 3;
const COLS: usize = 4_096;

/// A single key, so every counter it touches holds its mass and nothing else.
const SOLO_KEY: u64 = 0xC0FF_EE01;

// ---------------------------------------------------------------------------
// The skip schedule itself
// ---------------------------------------------------------------------------

/// The skip-table cursor must advance and wrap at the table's real length.
///
/// It used to be advanced with `(idx + 1) & 0x10000` — the table's *length*
/// used as a mask instead of `length - 1`. `x & 0x10000` is zero for every
/// `x` below `0xFFFF`, so the cursor never left entry 0: every skip distance
/// was the same number, and the stochastic-rounding draw derived from the
/// cursor was the same bit forever. This pins both ends of the fix.
#[test]
fn the_skip_cursor_advances_and_wraps_at_the_table_length() {
    let len = Nitro::skip_table_len();
    assert!(len > 1, "the skip table must be non-trivial");

    let mut nitro = Nitro::init_nitro(0.5);
    assert_eq!(
        nitro.table_cursor(),
        0,
        "an unseeded sampler starts at entry 0"
    );

    // One draw must move it. Under the old mask it did not.
    nitro.draw_geometric();
    assert_eq!(
        nitro.table_cursor(),
        1,
        "one draw must advance the cursor by exactly one entry"
    );

    // Over a few thousand draws it must visit a large number of distinct
    // entries, not sit on one.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..4_096 {
        seen.insert(nitro.table_cursor());
        nitro.draw_geometric();
    }
    assert_eq!(
        seen.len(),
        4_096,
        "4096 draws must touch 4096 distinct table entries; the cursor is stuck \
         or aliasing"
    );

    // And it must wrap exactly at the table length, not before or after.
    let mut wrapper = Nitro::init_nitro(0.5);
    for _ in 0..len {
        wrapper.draw_geometric();
    }
    assert_eq!(
        wrapper.table_cursor(),
        0,
        "after exactly {len} draws the cursor must be back at entry 0"
    );

    // A cursor decoded from an old payload can be anywhere, including past the
    // table. Reading must stay in bounds and rejoin the cycle.
    let mut hostile = Nitro::init_nitro(0.5);
    hostile.commit_ctx(usize::MAX, 0);
    assert!(
        hostile.table_cursor() < len,
        "an out-of-range cursor must be folded back into the table"
    );
    hostile.draw_geometric(); // must not panic
}

/// The geometric skip must be drawn at the **configured** rate.
///
/// `draw_geometric` used to read a table whose entries are already divided by
/// `ln(0.99)`, so every rate got `p = 0.01`'s schedule: at `p = 0.5` the
/// sampler skipped about 99 slots between admissions instead of 1, admitting
/// roughly 1% of the stream while weighting each admission as if it were 50%.
///
/// The inverse-CDF construction `floor(ln(1-u) / ln(1-p))` is exactly
/// `Geometric(p)` on `{0, 1, ...}`, so:
///
/// ```text
///   E[skip]   = (1-p)/p
///   Var[skip] = (1-p)/p^2
/// ```
///
/// The table is fixed, so this is a deterministic check; the threshold is not.
/// It is `z` standard errors of the sample mean of `n` Geometric(p) draws,
/// `sqrt(Var/n)`, which is what the table would have to satisfy if it really
/// were such a sample.
#[test]
fn the_geometric_skip_mean_matches_the_configured_rate() {
    const DRAWS: usize = 20_000;

    for &rate in &RATES {
        if rate >= 1.0 {
            let mut nitro = Nitro::init_nitro(rate);
            for _ in 0..64 {
                nitro.draw_geometric();
                assert_eq!(
                    nitro.get_ctx().2,
                    0,
                    "full sampling must never skip anything"
                );
            }
            continue;
        }

        let mut nitro = Nitro::init_nitro(rate);
        let mut total = 0f64;
        for _ in 0..DRAWS {
            nitro.draw_geometric();
            total += nitro.get_ctx().2 as f64;
        }
        let observed = total / DRAWS as f64;
        let expected = (1.0 - rate) / rate;
        let standard_error = ((1.0 - rate) / (rate * rate) / DRAWS as f64).sqrt();
        let allowed = Z * standard_error;
        assert!(
            (observed - expected).abs() <= allowed,
            "rate={rate}: mean skip {observed:.4} vs (1-p)/p = {expected:.4}, off by \
             {:.4} > z*se = {Z}*{standard_error:.4} = {allowed:.4}. A schedule locked \
             to a different rate shows up here first.",
            (observed - expected).abs()
        );
    }
}

/// A reciprocal-integer rate must not consume a rounding draw, and a
/// non-integer one must.
///
/// This is what keeps `p ∈ {1, 1/2, 1/10, 1/100}` emitting exactly the weights
/// they always did: the early return in `admitted_delta` means their rounding
/// stream never advances, so the correction is invisible where it is not
/// needed. It also pins the weights themselves, which is the whole bug.
#[test]
fn stochastic_rounding_only_draws_where_the_reciprocal_is_not_an_integer() {
    for &rate in &[1.0f64, 0.5, 0.1, 0.01] {
        let mut nitro = Nitro::init_nitro(rate);
        let expected = if rate >= 1.0 {
            1
        } else {
            (1.0 / rate).round() as u64
        };
        let weights: Vec<u64> = (0..64).map(|_| nitro.admitted_delta()).collect();
        assert!(
            weights.iter().all(|w| *w == expected),
            "rate={rate}: 1/p is an integer, so every admitted weight must be \
             exactly {expected}; got {:?}",
            &weights[..8]
        );
    }

    // Non-integer reciprocals must produce both weights, in the right
    // proportion. `E[W] = 1/p` is the property the whole correction exists for.
    for &rate in &[0.3f64, 0.07] {
        let mut nitro = Nitro::init_nitro(rate);
        let exact = 1.0 / rate;
        let floor = exact.floor() as u64;
        let frac = exact - exact.floor();
        const DRAWS: usize = 50_000;
        let weights: Vec<u64> = (0..DRAWS).map(|_| nitro.admitted_delta()).collect();
        assert!(
            weights.contains(&floor) && weights.contains(&(floor + 1)),
            "rate={rate}: both {floor} and {} must occur; a frozen dither emits one \
             of them forever, which is the +20% bias at p=0.3",
            floor + 1
        );
        let mean = weights.iter().map(|w| *w as f64).sum::<f64>() / DRAWS as f64;
        // sd of a Bernoulli(frac) mean over DRAWS draws.
        let se = (frac * (1.0 - frac) / DRAWS as f64).sqrt();
        assert!(
            (mean - exact).abs() <= Z * se,
            "rate={rate}: mean weight {mean:.6} vs 1/p = {exact:.6}, off by {:.6} > \
             z*se = {:.6}",
            (mean - exact).abs(),
            Z * se
        );
    }
}

// ---------------------------------------------------------------------------
// Row-level Nitro: CountMin and Count Sketch
// ---------------------------------------------------------------------------

/// Total mass a `Vector2D<i32>` row holds.
fn row_mass(storage: &Vector2D<i32>, row: usize) -> f64 {
    (0..storage.cols())
        .map(|c| storage.query_one_counter(row, c) as f64)
        .sum()
}

/// `CountMin::fast_insert_nitro` on a single key, at every rate.
///
/// One key means no collisions, so row `r`'s whole mass is that key's admitted
/// mass in row `r` — an estimate of `f` from `f` independently admitted slots,
/// with exactly the variance in the module docs.
///
/// Two assertions, and the second is the one the rounding fix is for:
///
/// 1. every row of every trial lands in the `z`-sigma band (rows within a trial
///    share an interleaved skip stream, so a trial passes only if *all* its
///    rows do, and the binomial runs over seeds);
/// 2. the **mean across independent seeds** is unbiased. A single-trial band
///    cannot see a systematic error: at `p = 0.3` the old `ceil` weight put
///    every estimate 20% high, which averaging exposes and a band the width of
///    one trial's noise does not.
#[test]
fn row_level_countmin_nitro_is_unbiased_at_every_rate() {
    for &rate in &RATES {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let f = true_count_for(rate);
        let key = DataInput::U64(SOLO_KEY);
        let mut tally = Tally::default();
        let mut row0 = Vec::new();

        for trial in 0..TRIALS {
            let seed = trial_seed(trial);
            let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
            cm.enable_nitro_with_seed(rate, seed);
            for _ in 0..f {
                cm.fast_insert_nitro(&key);
            }
            let storage = cm.as_storage();
            let masses: Vec<f64> = (0..ROWS).map(|r| row_mass(storage, r)).collect();
            row0.push(masses[0]);

            let failures: Vec<String> = masses
                .iter()
                .enumerate()
                .filter_map(|(r, m)| {
                    spec.check(*m, f as f64, 0.0)
                        .err()
                        .map(|d| format!("row {r}: {d}"))
                })
                .collect();
            tally.record(failures.is_empty(), || {
                format!("seed={seed}: {}", failures.join("; "))
            });
        }

        let (q, r) = spec.weight_parts();
        let context = format!(
            "CountMin::fast_insert_nitro rate={rate} rows={ROWS} cols={COLS} f={f} \
             (f*p={:.0} admissions per row), weight {q}+Bernoulli({r:.4}), \
             sigma={:.2}, z={Z}; one trial per seed, all {ROWS} rows must pass",
            f as f64 * rate,
            spec.sigma(f as f64)
        );
        tally.assert_independent_binomial(
            &format!("row-level CountMin Nitro rate={rate} / sampling band"),
            spec.per_check_failure(),
            &context,
        );
        assert_unbiased_across_trials(
            "row-level CountMin Nitro",
            rate,
            &row0,
            f as f64,
            &spec,
            &context,
        );
    }
}

/// `Count::fast_insert_nitro` on a single key, at every rate.
///
/// Count Sketch applies a per-row sign taken from the row's own hash bit, so a
/// single key's row mass is `±` its admitted mass; the magnitude carries the
/// same distribution as the Count-Min path and is what the band applies to.
#[test]
fn row_level_countsketch_nitro_is_unbiased_at_every_rate() {
    for &rate in &RATES {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let f = true_count_for(rate);
        let key = DataInput::U64(SOLO_KEY);
        let mut tally = Tally::default();
        let mut row0 = Vec::new();

        for trial in 0..TRIALS {
            let seed = trial_seed(trial);
            let mut cs = Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
            cs.enable_nitro_with_seed(rate, seed);
            for _ in 0..f {
                cs.fast_insert_nitro(&key);
            }
            let storage = cs.as_storage();
            let masses: Vec<f64> = (0..ROWS).map(|r| row_mass(storage, r).abs()).collect();
            row0.push(masses[0]);

            let failures: Vec<String> = masses
                .iter()
                .enumerate()
                .filter_map(|(r, m)| {
                    spec.check(*m, f as f64, 0.0)
                        .err()
                        .map(|d| format!("row {r}: {d}"))
                })
                .collect();
            tally.record(failures.is_empty(), || {
                format!("seed={seed}: {}", failures.join("; "))
            });
        }

        let context = format!(
            "Count::fast_insert_nitro rate={rate} rows={ROWS} cols={COLS} f={f}, \
             sigma={:.2}, z={Z}; signed rows, magnitude checked",
            spec.sigma(f as f64)
        );
        tally.assert_independent_binomial(
            &format!("row-level Count Sketch Nitro rate={rate} / sampling band"),
            spec.per_check_failure(),
            &context,
        );
        assert_unbiased_across_trials(
            "row-level Count Sketch Nitro",
            rate,
            &row0,
            f as f64,
            &spec,
            &context,
        );
    }
}

/// Averaging independent trials drives the sampling noise down by
/// `sqrt(trials)` and leaves any systematic rounding bias standing.
fn assert_unbiased_across_trials(
    label: &str,
    rate: f64,
    observations: &[f64],
    truth: f64,
    spec: &SamplingConfidenceSpec,
    context: &str,
) {
    let n = observations.len() as f64;
    let mean = observations.iter().sum::<f64>() / n;
    let allowed = Z * spec.sigma(truth) / n.sqrt();
    assert!(
        (mean - truth).abs() <= allowed,
        "{label} rate={rate}: the mean over {n} independent seeds is {mean:.1} against a \
         true {truth:.0} — relative bias {:.4}, outside the mean's band \
         z*sigma/sqrt(trials) = {allowed:.1}. A weight whose expectation is not 1/p \
         shows up here even when every single trial sits inside its own band. \
         {context}",
        (mean - truth) / truth,
    );
}

/// A Zipf stream through the row-level Count-Min path, per key.
///
/// With collisions the two error sources compose, and the band is the sum of
/// the two the sketch actually earns — no third constant:
///
/// - **below:** a cell only ever *gains* mass from collisions, so the minimum
///   over rows is at least the key's own admitted mass in some row, and that is
///   within `z*sigma(f)` of `f`. The union over `d` rows is what the failure
///   probability is quoted at.
/// - **above:** the minimum is at most row 0, which holds the key's own
///   admitted mass plus the colliding keys' — bounded by Count-Min's own
///   simultaneous budget, with `z*sigma(N)` for the noise, `sigma` being
///   increasing in the mass and a cell holding at most the whole stream.
#[test]
fn row_level_countmin_nitro_tracks_a_zipf_stream_within_the_combined_band() {
    const N: usize = 200_000;
    const DOMAIN: usize = 512;
    const STREAM_SEED: u64 = 0x0117_2199;

    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
    }
    let total = truth.total() as f64;
    let distinct = truth.distinct();
    let cm_spec = CountMinSpec::new(ROWS, COLS);

    // Rates whose admission count keeps the whole stream inside one disjoint
    // stretch of the skip table.
    for &rate in &[1.0f64, 0.3, 0.1, 0.07] {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
        cm.enable_nitro_with_seed(rate, trial_seed(0));
        for k in &stream {
            cm.fast_insert_nitro(&DataInput::U64(*k));
        }

        let collision_budget =
            |f: f64| cm_spec.simultaneous_bound(total, f, distinct, SIMULTANEOUS_LEVEL);
        let context = format!(
            "row-level CountMin Nitro over zipf(1.1) domain={DOMAIN} n={N} \
             seed={STREAM_SEED:#x}, rate={rate}, rows={ROWS} cols={COLS}"
        );

        let mut low = Tally::default();
        let mut high = Tally::default();
        for (k, c) in truth.pairs() {
            let f = c as f64;
            let est = cm.nitro_estimate(&DataInput::U64(k as u64));
            let floor = f - Z * spec.sigma(f);
            let ceiling = f + collision_budget(f) + Z * spec.sigma(total);
            low.record(est >= floor, || {
                format!("key {k}: true {f}, est {est:.1} below f - z*sigma(f) = {floor:.1}")
            });
            high.record(est <= ceiling, || {
                format!(
                    "key {k}: true {f}, est {est:.1} above f + collisions + z*sigma(N) = \
                     {ceiling:.1}"
                )
            });
        }
        // Both halves are union-bounded over the probed keys, so neither
        // tolerates a violation.
        low.assert_none(
            &format!("row-level CountMin Nitro rate={rate} / lower band"),
            &context,
        );
        high.assert_none(
            &format!("row-level CountMin Nitro rate={rate} / upper band"),
            &context,
        );
    }
}

/// Several keys of comparable frequency, so no single key dominates the cells
/// it shares and the per-key estimates cannot be carried by one hot flow.
#[test]
fn row_level_countmin_nitro_separates_keys_of_similar_frequency() {
    const KEYS: usize = 16;
    const PER_KEY: usize = 20_000;

    for &rate in &[1.0f64, 0.5, 0.3, 0.1] {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
        cm.enable_nitro_with_seed(rate, trial_seed(1));
        let keys: Vec<u64> = (0..KEYS as u64).map(|i| 0x5EED_0000 + i * 7919).collect();
        for _ in 0..PER_KEY {
            for k in &keys {
                cm.fast_insert_nitro(&DataInput::U64(*k));
            }
        }

        let total = (KEYS * PER_KEY) as f64;
        let f = PER_KEY as f64;
        let cm_spec = CountMinSpec::new(ROWS, COLS);
        let ceiling = f
            + cm_spec.simultaneous_bound(total, f, KEYS, SIMULTANEOUS_LEVEL)
            + Z * spec.sigma(total);
        let floor = f - Z * spec.sigma(f);
        let mut tally = Tally::default();
        for k in &keys {
            let est = cm.nitro_estimate(&DataInput::U64(*k));
            tally.record(est >= floor && est <= ceiling, || {
                format!("key {k:#x}: est {est:.1} outside [{floor:.1}, {ceiling:.1}]")
            });
        }
        tally.assert_none(
            &format!("row-level CountMin Nitro rate={rate} / {KEYS} equal-frequency keys"),
            &format!("each key inserted {PER_KEY} times, rows={ROWS} cols={COLS}"),
        );
    }
}

// ---------------------------------------------------------------------------
// NitroBatch: both ingestion paths
// ---------------------------------------------------------------------------

/// `NitroBatch::insert` and `NitroBatch::insert_cached_step` under the same
/// band, at every rate.
///
/// The two paths differ only in where the skip distances come from — a live
/// `SmallRng` for `insert`, the shared precomputed table for
/// `insert_cached_step` — so they answer to the same model and the same
/// variance. `insert_cached_step` is the path that read a table hard-wired to
/// `p = 0.01` regardless of rate, and whose cursor never advanced.
#[test]
fn nitro_batch_both_ingestion_paths_stay_inside_the_sampling_band() {
    for &rate in &RATES {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let f = true_count_for(rate);
        let data = vec![SOLO_KEY as i64; f];

        for path in ["insert", "insert_cached_step"] {
            let mut tally = Tally::default();
            let mut estimates = Vec::new();
            for trial in 0..TRIALS {
                let seed = trial_seed(trial);
                let mut nitro = NitroBatch::with_target_and_seed(
                    rate,
                    CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS),
                    seed,
                );
                match path {
                    "insert" => nitro.insert(&data),
                    _ => nitro.insert_cached_step(&data),
                }
                let est = nitro.estimate_median(&DataInput::I64(SOLO_KEY as i64));
                estimates.push(est);
                let outcome = spec.check(est, f as f64, 0.0);
                tally.record(outcome.is_ok(), || {
                    format!("seed={seed}: {}", outcome.unwrap_err())
                });
            }

            let context = format!(
                "NitroBatch::{path} rate={rate} f={f}, sigma={:.2}, z={Z}, \
                 E[W]={:.4}; one trial per sampling seed",
                spec.sigma(f as f64),
                spec.expected_weight()
            );
            tally.assert_independent_binomial(
                &format!("NitroBatch::{path} rate={rate} / sampling band"),
                spec.per_check_failure(),
                &context,
            );
            assert_unbiased_across_trials(
                &format!("NitroBatch::{path}"),
                rate,
                &estimates,
                f as f64,
                &spec,
                &context,
            );
        }
    }
}

/// `insert_cached_step` must actually walk the table.
///
/// With the old mask the cursor stayed at entry 0, so every skip was the same
/// distance and the admitted positions were an arithmetic progression. Two
/// things follow that are cheap to assert and impossible under that bug: the
/// cursor ends far from where it started, and two different seeds admit
/// different subsets.
#[test]
fn nitro_batch_cached_step_walks_the_table_and_respects_its_seed() {
    const RATE: f64 = 0.1;
    let f = true_count_for(RATE);
    let data = vec![SOLO_KEY as i64; f];

    let run = |seed: u64| {
        let mut nitro = NitroBatch::with_target_and_seed(
            RATE,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS),
            seed,
        );
        let start = nitro.table_cursor();
        nitro.insert_cached_step(&data);
        (
            start,
            nitro.table_cursor(),
            nitro.estimate_median(&DataInput::I64(SOLO_KEY as i64)),
        )
    };

    let (start, end, est) = run(trial_seed(0));
    let advanced = (end + NitroBatch::<Vector2D<u32>>::skip_table_len() - start)
        % NitroBatch::<Vector2D<u32>>::skip_table_len();
    assert!(
        advanced >= f / 20,
        "the cached path made about {} admissions but the cursor only advanced \
         {advanced} entries; it is not walking the table",
        (f as f64 * RATE) as usize
    );

    // The same seed reproduces exactly; a different one does not.
    assert_eq!(run(trial_seed(0)).2, est, "same seed must reproduce");
    assert_ne!(
        run(trial_seed(5)).2,
        est,
        "different seeds must read different stretches of the table, or the \
         cached path is not seeded at all"
    );
}

/// Full sampling is not approximate on any path.
#[test]
fn full_sampling_is_exact_on_every_nitro_path() {
    const F: usize = 5_000;
    let key = DataInput::U64(SOLO_KEY);

    let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
    cm.enable_nitro_with_seed(1.0, trial_seed(0));
    for _ in 0..F {
        cm.fast_insert_nitro(&key);
    }
    for r in 0..ROWS {
        assert_eq!(
            row_mass(cm.as_storage(), r),
            F as f64,
            "rate=1.0: CountMin row {r} must hold every update at unit weight"
        );
    }

    let mut cs = Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
    cs.enable_nitro_with_seed(1.0, trial_seed(0));
    for _ in 0..F {
        cs.fast_insert_nitro(&key);
    }
    for r in 0..ROWS {
        assert_eq!(
            row_mass(cs.as_storage(), r).abs(),
            F as f64,
            "rate=1.0: Count Sketch row {r} must hold every update at unit weight"
        );
    }

    let data = vec![SOLO_KEY as i64; F];
    for path in ["insert", "insert_cached_step"] {
        let mut nitro = NitroBatch::with_target_and_seed(
            1.0,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS),
            trial_seed(0),
        );
        match path {
            "insert" => nitro.insert(&data),
            _ => nitro.insert_cached_step(&data),
        }
        assert_eq!(
            nitro.estimate_median(&DataInput::I64(SOLO_KEY as i64)),
            F as f64,
            "rate=1.0: NitroBatch::{path} must admit every update at unit weight"
        );
    }
}

/// Every path reproduces from its seed, and different seeds genuinely differ.
#[test]
fn every_nitro_path_is_reproducible_from_its_seed() {
    const RATE: f64 = 0.3;
    let f = true_count_for(RATE);
    let key = DataInput::U64(SOLO_KEY);
    let data = vec![SOLO_KEY as i64; f];

    let cm_run = |seed: u64| {
        let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
        cm.enable_nitro_with_seed(RATE, seed);
        for _ in 0..f {
            cm.fast_insert_nitro(&key);
        }
        row_mass(cm.as_storage(), 0)
    };
    let cs_run = |seed: u64| {
        let mut cs = Count::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
        cs.enable_nitro_with_seed(RATE, seed);
        for _ in 0..f {
            cs.fast_insert_nitro(&key);
        }
        row_mass(cs.as_storage(), 0).abs()
    };
    let batch_run = |seed: u64, cached: bool| {
        let mut nitro = NitroBatch::with_target_and_seed(
            RATE,
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS),
            seed,
        );
        if cached {
            nitro.insert_cached_step(&data);
        } else {
            nitro.insert(&data);
        }
        nitro.estimate_median(&DataInput::I64(SOLO_KEY as i64))
    };

    /// One ingestion path, reduced to "seed in, a single number out".
    type SeededRun<'a> = Box<dyn Fn(u64) -> f64 + 'a>;

    let cases: [(&str, SeededRun<'_>); 4] = [
        ("CountMin::fast_insert_nitro", Box::new(cm_run)),
        ("Count::fast_insert_nitro", Box::new(cs_run)),
        ("NitroBatch::insert", Box::new(move |s| batch_run(s, false))),
        (
            "NitroBatch::insert_cached_step",
            Box::new(move |s| batch_run(s, true)),
        ),
    ];

    for (label, run) in cases {
        let a = run(trial_seed(0));
        assert_eq!(a, run(trial_seed(0)), "{label}: same seed must reproduce");
        let others: std::collections::HashSet<u64> =
            (1..TRIALS).map(|t| run(trial_seed(t)).to_bits()).collect();
        assert!(
            others.len() > 1,
            "{label}: {} distinct seeds produced {} distinct results; the seed is not \
             reaching the sampler",
            TRIALS - 1,
            others.len()
        );
    }
}

/// Merging two row-level Nitro sketches sums their admitted mass, so a key
/// split across both must land in the band for the combined work.
#[test]
fn row_level_nitro_merge_lands_in_the_combined_band() {
    for &rate in &RATES {
        let spec = SamplingConfidenceSpec::new(rate, Z);
        let half = true_count_for(rate);
        let key = DataInput::U64(SOLO_KEY);
        let mut tally = Tally::default();

        for pair in 0..TRIALS / 2 {
            let (sa, sb) = (trial_seed(2 * pair), trial_seed(2 * pair + 1));
            let build = |seed: u64| {
                let mut cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
                cm.enable_nitro_with_seed(rate, seed);
                for _ in 0..half {
                    cm.fast_insert_nitro(&key);
                }
                cm
            };
            let mut a = build(sa);
            let b = build(sb);
            a.merge(&b);

            let combined = (2 * half) as f64;
            let mass = row_mass(a.as_storage(), 0);
            let outcome = spec.check(mass, combined, 0.0);
            tally.record(outcome.is_ok(), || {
                format!("seeds {sa}/{sb}: {}", outcome.unwrap_err())
            });
        }
        tally.assert_independent_binomial(
            &format!("row-level Nitro merge rate={rate} / combined band"),
            spec.per_check_failure(),
            &format!(
                "two shards of {half} updates each, merged; sigma over the combined \
                 work = {:.2}, z={Z}",
                spec.sigma((2 * half) as f64)
            ),
        );
    }
}
