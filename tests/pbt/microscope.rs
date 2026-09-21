//! Property tests for `MicroCM` (MicroscopeSketch): sliding-window frequency
//! estimation with adaptive zoom.
//!
//! Zhao, Wang, Li, Dong, Yang, Chen, Zhang, Uhlig, KDD '23.
//!
//! The oracle is an exact sliding window — a `VecDeque` of raw arrivals, each
//! tagged with the sub-window it was recorded in — and knows nothing of
//! pixels, shutters or exponents. Everything the sketch may legitimately
//! differ by is derived from the two mechanisms separately: Count-Min
//! collisions, which only inflate and are computed per sub-window because
//! that is where the minimum across rows is taken, and zoom rounding, whose
//! slack is `c^Z` per pixel read.
//!
//! The generators exist to make the second mechanism fire. A load that never
//! saturates a pixel leaves `max_zoom()` at zero and tests the sketch as a
//! plain windowed Count-Min, which is the one shape these laws are not for;
//! `a_hot_key_saturates_a_pixel_and_forces_a_zoom` pins the generator to a
//! load that zooms, and every law below re-asserts it on the run it drew.

use crate::support::close;
use asap_sketchlib::sketches::microscope::{
    DeltaStrategy, MicroLayout, MicroParams, SubWindowClock,
};
use asap_sketchlib::{DataInput, DefaultXxHasher, MatrixFastHash, MicroCM, SketchHasher, Vector3D};
use proptest::prelude::*;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

/// Key domain the streams draw from. Narrow enough that a handful of columns
/// forces collisions on every row.
const DOMAIN: u64 = 12;

/// The default-hasher instantiation, so the turbofish stays out of the laws.
type Micro = MicroCM<DefaultXxHasher>;

fn key(k: u64) -> DataInput<'static> {
    DataInput::U64(k)
}

/// The column `k` lands in on each row, through the hash entry point
/// `MicroCM` routes with.
fn columns_of(rows: usize, cols: usize, k: u64) -> Vec<usize> {
    let packed = DefaultXxHasher::hash128_seeded(0, &key(k));
    (0..rows)
        .map(|row| MatrixFastHash::col_for_row(&packed, row, cols))
        .collect()
}

// ===== The oracle =====

/// The exact sliding window: every arrival still inside it, in order, tagged
/// with the sub-window it was recorded in.
///
/// `Over` charges sub-windows `n-t ..= n`, so that is what is kept; anything
/// older has left the span under every strategy.
struct Window {
    arrivals: VecDeque<(u64, u64)>,
    t: u64,
}

impl Window {
    fn new(t: usize) -> Self {
        Self {
            arrivals: VecDeque::new(),
            t: t as u64,
        }
    }

    fn record(&mut self, sub_window: u64, k: u64) {
        self.arrivals.push_back((sub_window, k));
        let oldest = sub_window.saturating_sub(self.t);
        while self.arrivals.front().is_some_and(|(s, _)| *s < oldest) {
            self.arrivals.pop_front();
        }
    }

    /// How many times each key arrived in each sub-window still in the span.
    fn by_sub_window(&self) -> HashMap<(u64, u64), i64> {
        let mut counts = HashMap::new();
        for (sub_window, k) in &self.arrivals {
            *counts.entry((*sub_window, *k)).or_default() += 1;
        }
        counts
    }

    fn present_keys(&self) -> Vec<u64> {
        let mut seen: Vec<u64> = self.arrivals.iter().map(|(_, k)| *k).collect();
        seen.sort_unstable();
        seen.dedup();
        seen
    }
}

/// What `DeltaStrategy::Over` would answer for `k` with no rounding: the true
/// window count, and the widest value collisions can inflate it to.
///
/// The sketch reduces across rows **per sub-window** and sums the minima, so
/// the inflated end does the same: each sub-window contributes the lightest
/// row's share of the cell `k` sits in, which is `k`'s own arrivals plus
/// everything that landed in the same column on that row.
fn over_band(window: &Window, rows: usize, cols: usize, n: u64, t: u64, k: u64) -> (f64, f64) {
    let counts = window.by_sub_window();
    let present = window.present_keys();
    let mine = columns_of(rows, cols, k);
    let columns: HashMap<u64, Vec<usize>> = present
        .iter()
        .map(|j| (*j, columns_of(rows, cols, *j)))
        .collect();

    let mut truth = 0i64;
    let mut inflated = 0i64;
    for sub_window in n.saturating_sub(t)..=n {
        truth += counts.get(&(sub_window, k)).copied().unwrap_or(0);
        inflated += (0..rows)
            .map(|row| {
                present
                    .iter()
                    .filter(|j| columns[j][row] == mine[row])
                    .map(|j| counts.get(&(sub_window, *j)).copied().unwrap_or(0))
                    .sum::<i64>()
            })
            .min()
            .expect("a sketch has at least one row");
    }
    (truth as f64, inflated as f64)
}

/// How far a zoomed estimate may sit outside the collision band.
///
/// A cell that reached exponent `Z` divided its pixels at most `Z` times,
/// each division rounding by less than the unit it produced, so the
/// compounded error of one pixel is under `c + c^2 + ... + c^Z`, i.e. under
/// `c^Z * c/(c-1) <= 2 * c^Z`. A sub-window boundary adds one more: the
/// shutter it closes out is rounded into that sub-window's pixel, worth
/// under one further unit. The estimate reads `t + 1` sub-windows, so the
/// slack is `3 * c^Z * (t + 1)`.
///
/// At `Z = 0` nothing has ever been divided and no shutter has ever held a
/// partial unit, so the arithmetic is exact and the slack is zero.
fn zoom_slack(peak: u8, c: u32, t: usize) -> f64 {
    if peak == 0 {
        return 0.0;
    }
    3.0 * (c as f64).powi(peak as i32) * (t + 1) as f64
}

// ===== The load =====

/// One drawn run: a grid, a window shape, and the stream to push through it.
#[derive(Clone, Debug)]
struct Run {
    rows: usize,
    cols: usize,
    t: usize,
    c: u32,
    per_sub_window: u64,
    seed: u64,
    stream: Vec<u64>,
}

impl Run {
    fn empty(&self) -> Micro {
        MicroCM::with_dimensions(
            self.rows,
            self.cols,
            MicroParams::new(self.t, self.c),
            SubWindowClock::count_based(self.per_sub_window),
            self.seed,
        )
    }

    /// Pushes the stream through a sketch and the exact window side by side,
    /// returning the highest `max_zoom()` seen along the way.
    ///
    /// The peak is sampled rather than taken at the end because
    /// `enter_sub_window` reclaims resolution: a cell that zoomed out and
    /// later zoomed back in still carries the rounding of the division, and
    /// the slack has to be measured against the exponent it reached.
    fn drive(&self, sketch: &mut Micro, window: &mut Window) -> u8 {
        let mut peak = 0;
        for (i, k) in self.stream.iter().enumerate() {
            window.record(i as u64 / self.per_sub_window, *k);
            sketch.insert(&key(*k));
            peak = peak.max(sketch.max_zoom());
        }
        peak
    }
}

/// A stream built from a few drawn parameters instead of one draw per item:
/// every `spacing`-th arrival is a cold key and the rest are `hot`.
///
/// At `spacing >= 3` the hot key takes over two thirds of each sub-window,
/// and a sub-window is at least 500 items, so more than 255 of them land on
/// one pixel — which is the increment that has to zoom the cell out. The
/// pixel it leaves behind is above the `255 / c` ceiling a zoom-in needs, so
/// the exponent stays up for as long as that pixel is in the ring.
fn stream_of(len: usize, hot: u64, cold: &[u64], spacing: usize) -> Vec<u64> {
    (0..len)
        .map(|i| {
            if i % spacing == spacing - 1 {
                cold[(i / spacing) % cold.len()]
            } else {
                hot
            }
        })
        .collect()
}

fn zooming_run() -> impl Strategy<Value = Run> {
    (
        1usize..4,
        prop_oneof![Just(2usize), Just(4), Just(8)],
        1usize..5,
        prop_oneof![Just(2u32), Just(3), Just(4)],
        500u64..800,
        3usize..8,
        0u64..DOMAIN,
        prop::collection::vec(0u64..DOMAIN, 1..5),
        any::<u64>(),
    )
        .prop_map(
            |(rows, cols, t, c, per_sub_window, spacing, hot, cold, seed)| {
                let len = (per_sub_window * (t as u64 + 3)) as usize;
                Run {
                    rows,
                    cols,
                    t,
                    c,
                    per_sub_window,
                    seed,
                    stream: stream_of(len, hot, &cold, spacing),
                }
            },
        )
}

// ===== The generator guard =====

/// The load every property below draws from must saturate a pixel.
///
/// Pinned at both ends of each drawn axis, because a generator that quietly
/// stopped zooming would leave the laws holding over a windowed Count-Min
/// and say nothing about the mechanism this sketch exists for. Each law
/// re-asserts it on its own run; this test is what fails first, and names
/// the axis, when the shape stops reaching a zoom.
#[test]
fn a_hot_key_saturates_a_pixel_and_forces_a_zoom() {
    for rows in [1usize, 3] {
        for cols in [2usize, 8] {
            for t in [1usize, 4] {
                for c in [2u32, 4] {
                    for (per_sub_window, spacing) in [(500u64, 7usize), (799, 3)] {
                        let run = Run {
                            rows,
                            cols,
                            t,
                            c,
                            per_sub_window,
                            seed: 0xABCD,
                            stream: stream_of(
                                (per_sub_window * (t as u64 + 3)) as usize,
                                5,
                                &[1, 2, 3],
                                spacing,
                            ),
                        };
                        let mut sketch = run.empty();
                        let mut window = Window::new(t);
                        let peak = run.drive(&mut sketch, &mut window);
                        assert!(
                            peak > 0,
                            "rows={rows} cols={cols} t={t} c={c} \
                             per_sub_window={per_sub_window} spacing={spacing}: \
                             the load never saturated a pixel"
                        );
                        assert!(
                            sketch.max_zoom() > 0,
                            "rows={rows} cols={cols} t={t} c={c} \
                             per_sub_window={per_sub_window} spacing={spacing}: \
                             the zoom was reclaimed before the run ended"
                        );
                    }
                }
            }
        }
    }
}

/// `max_zoom()` is not monotone over a stream, and the property below states
/// the weaker law that is true instead. This is the witness: a burst that
/// zooms, then a spread load that lets `enter_sub_window` reclaim every
/// exponent it raised.
#[test]
fn a_zoom_is_reclaimed_once_the_hot_key_leaves_the_window() {
    let mut sketch: Micro = Micro::with_dimensions(
        2,
        256,
        MicroParams::new(2, 2),
        SubWindowClock::count_based(600),
        0xABCD,
    );
    for _ in 0..2_400 {
        sketch.insert(&key(1));
    }
    assert!(sketch.max_zoom() > 0, "the burst must have zoomed");
    for i in 0..3_600u64 {
        sketch.insert(&key(1_000 + i));
    }
    assert_eq!(
        sketch.max_zoom(),
        0,
        "a spread load leaves every pixel under the zoom-in ceiling"
    );
}

/// A zoom-out rescales what the cell is already holding, and the reclaim that
/// follows scales it back — neither losing nor inventing counts.
///
/// The properties below only ever bound the answer by a slack that grows with
/// the exponent, which is the wrong instrument for this: a zoom that divided
/// the wrong pixels, or a reclaim that dropped `Z` without multiplying, moves
/// the answer by a factor of `c` and a slack of `c^Z` per pixel absorbs it.
/// So this is an equality instead, and everything is arranged to keep it one:
/// a time clock sets the load per sub-window rather than having it follow the
/// item count, which lets a single key own the grid and removes collisions,
/// and `c = 2` over even counts makes every division exact.
#[test]
fn a_zoom_out_and_its_reclaim_preserve_the_window_count() {
    const T: usize = 4;
    const LEN: u64 = 10;
    const STEADY: u64 = 100;
    const BURST: u64 = 400;

    fn push(sketch: &mut Micro, sub_window: u64, items: u64) {
        for _ in 0..items {
            sketch.insert_at(&key(1), sub_window * LEN);
        }
    }

    let mut sketch: Micro = Micro::with_dimensions(
        2,
        8,
        MicroParams::new(T, 2),
        SubWindowClock::time_based(LEN, 0),
        0xABCD,
    );
    push(&mut sketch, 0, STEADY);
    push(&mut sketch, 1, STEADY);
    push(&mut sketch, 2, BURST);

    // Sub-windows 0 and 1 were sitting in the cell when the burst zoomed it
    // out, so their pixels went through the division that zoom performed.
    assert!(sketch.max_zoom() > 0, "the burst must have zoomed the cell");
    assert_eq!(
        sketch.estimate_with(&key(1), DeltaStrategy::Over),
        (2 * STEADY + BURST) as f64,
        "a zoom-out must rescale the sub-windows already in the cell"
    );

    // The burst ages out at sub-window 8, which is the boundary that brings
    // every pixel back under the zoom-in ceiling.
    //
    // Sub-window 4 is twice the others, and 4 is exactly the oldest one the
    // window still reaches at 8. Under a flat load a span shifted by one
    // sub-window adds up to the same total, so the two strategies would pin
    // how many sub-windows are charged without pinning which.
    for n in 3..=8u64 {
        push(&mut sketch, n, if n == 4 { 2 * STEADY } else { STEADY });
    }
    assert_eq!(
        sketch.max_zoom(),
        0,
        "the burst has left the ring, so the resolution is back"
    );
    assert_eq!(
        sketch.estimate_with(&key(1), DeltaStrategy::Over),
        (6 * STEADY) as f64,
        "a reclaim must multiply the pixels back up, over sub-windows 4..=8"
    );
    assert_eq!(
        sketch.estimate_with(&key(1), DeltaStrategy::Under),
        (4 * STEADY) as f64,
        "a reclaim must multiply the pixels back up, over sub-windows 5..=8"
    );
}

/// The rounding seed reaches the rounding.
///
/// Every division a zoom performs draws from the sketch's own generator, so a
/// constructor that dropped its seed would leave every sketch rounding
/// identically and the reproducibility `with_dimensions` offers would be an
/// accident of there being one stream rather than a choice. `c = 3` is what
/// makes the divisions inexact, so draws are taken at all.
#[test]
fn the_rounding_seed_reaches_the_zoom() {
    let grid_for = |seed: u64| {
        let mut sketch: Micro = Micro::with_dimensions(
            2,
            8,
            MicroParams::new(4, 3),
            SubWindowClock::count_based(1_000),
            seed,
        );
        for i in 0..8_000u64 {
            sketch.insert(&key(i % 3));
        }
        assert!(sketch.max_zoom() > 0, "the load must have zoomed");
        sketch.as_storage().as_slice().to_vec()
    };
    assert_ne!(
        grid_for(1),
        grid_for(2),
        "two seeds rounded a zoom identically, so the seed is not reaching it"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    // ===== A live cell holds what the wire refuses to decode without =====
    //
    // `deserialize` rejects a record whose shutter has reached one `c^Z`
    // unit, because that is the assumption bounding the carry a merge
    // computes. A payload cannot be the only way in: the running sketch has
    // to keep it too, through every zoom-out, boundary and reclaim.

    #[test]
    fn a_live_cell_never_holds_a_whole_unit_in_its_shutter(run in zooming_run()) {
        let layout = MicroLayout::new(MicroParams::new(run.t, run.c));
        let mut sketch = run.empty();
        let mut peak = 0;
        for (step, k) in run.stream.iter().enumerate() {
            sketch.insert(&key(*k));
            peak = peak.max(sketch.max_zoom());
            for (index, record) in sketch
                .as_storage()
                .as_slice()
                .chunks_exact(layout.depth())
                .enumerate()
            {
                let at = layout.pixels();
                let z = record[at];
                let shutter = u32::from_le_bytes([
                    record[at + 1], record[at + 2], record[at + 3], record[at + 4],
                ]) as u64;
                let unit = (run.c as u64).pow(z as u32);
                prop_assert!(
                    shutter < unit,
                    "insert {} of key {} left cell {} holding a shutter of {} at \
                     zoom {}, which is a whole unit of {} or more",
                    step, k, index, shutter, z, unit
                );
            }
        }
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);
    }

    // ===== A zoomed estimate stays inside the collision band =====
    //
    // Two mechanisms move the answer off the truth and they are bounded
    // separately: collisions inflate it, by the lightest row's share per
    // sub-window, and zoom rounding moves it either way by `c^Z` per pixel
    // read. `a_zoomed_cell_tracks_the_window_count` in `src` holds the
    // second alone on one cell; this is the pair of them on a grid.

    #[test]
    fn a_zoomed_estimate_stays_within_the_rounding_slack_of_the_collision_band(
        run in zooming_run(),
    ) {
        let mut sketch = run.empty();
        let mut window = Window::new(run.t);
        let peak = run.drive(&mut sketch, &mut window);
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        let slack = zoom_slack(peak, run.c, run.t);
        let n = sketch.sub_window();
        for k in 0..DOMAIN {
            let (truth, inflated) =
                over_band(&window, run.rows, run.cols, n, run.t as u64, k);
            let est = sketch.estimate_with(&key(k), DeltaStrategy::Over);
            prop_assert!(
                est >= truth - slack,
                "key {}: estimate {} is more than the {} slack below the true \
                 window count {} at zoom {} (n={}, {:?})",
                k, est, slack, truth, peak, n, run
            );
            prop_assert!(
                est <= inflated + slack,
                "key {}: estimate {} is more than the {} slack above the {} the \
                 lightest row's collisions allow at zoom {} (n={}, {:?})",
                k, est, slack, inflated, peak, n, run
            );
        }
    }

    // ===== The exponent is no higher than the load asks for =====
    //
    // A cell reaches `Z` only because a pixel holding 255 units of `c^(Z-1)`
    // took one more, so the cell had attributed `255 * c^(Z-1)` items to a
    // single sub-window. A sub-window is `per_sub_window` items and each row
    // routes every one of them to exactly one cell, so no cell can hold more
    // than that; the factor of two is the room the rounding of a division has
    // to inflate a pixel above what arrived.
    //
    // This is also what keeps the law above from being self-defeating: its
    // slack is `c^Z` scaled, read off the run's own peak, so a bug that drove
    // the exponent towards its cap would widen the band until it admitted
    // anything. Bounding the exponent bounds the band.

    #[test]
    fn the_zoom_exponent_stays_within_what_one_sub_window_can_fill(run in zooming_run()) {
        let mut sketch = run.empty();
        let peak = run.drive(&mut sketch, &mut Window::new(run.t));
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        let filled = 255u64 * (run.c as u64).pow(peak as u32 - 1);
        prop_assert!(
            filled <= 2 * run.per_sub_window,
            "zoom {} says a pixel worth {} items filled up inside a sub-window of \
             {} ({:?})",
            peak, filled, run.per_sub_window, run
        );
    }

    // ===== The strategies are ordered by how much of the oldest sub-window
    // they charge =====
    //
    // `Over` and `Under` are the two ends of `Linear`, so this is also what
    // pins them to it: an arm that stopped agreeing with its fraction would
    // break the chain rather than go unnoticed.

    #[test]
    fn the_delta_strategies_are_ordered_by_the_fraction_they_charge(
        run in zooming_run(),
        fractions in prop::collection::vec(0.0f64..=1.0, 1..6),
    ) {
        let mut sketch = run.empty();
        let mut window = Window::new(run.t);
        let peak = run.drive(&mut sketch, &mut window);
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        let mut ordered = fractions.clone();
        ordered.sort_by(f64::total_cmp);
        for k in 0..DOMAIN {
            let under = sketch.estimate_with(&key(k), DeltaStrategy::Under);
            let over = sketch.estimate_with(&key(k), DeltaStrategy::Over);
            prop_assert!(under <= over, "key {}: Under {} above Over {}", k, under, over);
            prop_assert_eq!(
                sketch.estimate_with(&key(k), DeltaStrategy::Linear(0.0)),
                under,
                "key {}: Linear(0.0) is not Under", k
            );
            prop_assert_eq!(
                sketch.estimate_with(&key(k), DeltaStrategy::Linear(1.0)),
                over,
                "key {}: Linear(1.0) is not Over", k
            );

            let mut previous = under;
            for f in &ordered {
                let now = sketch.estimate_with(&key(k), DeltaStrategy::Linear(*f));
                prop_assert!(
                    (under..=over).contains(&now),
                    "key {}: Linear({}) = {} outside [{}, {}]", k, f, now, under, over
                );
                prop_assert!(
                    now >= previous,
                    "key {}: Linear({}) = {} fell below the smaller fraction's {}",
                    k, f, now, previous
                );
                previous = now;
            }

            prop_assert_eq!(
                sketch.estimate(&key(k)),
                sketch.estimate_with(
                    &key(k),
                    DeltaStrategy::Linear(sketch.residual_fraction()),
                ),
                "key {}: estimate() is not Linear(residual_fraction())", k
            );
        }
    }

    // ===== A count-based window is driven by the item count and nothing else
    // =====

    #[test]
    fn a_count_based_window_advances_on_the_item_count_alone(
        per_sub_window in 1u64..40,
        t in 1usize..5,
        left in prop::collection::vec(0u64..DOMAIN, 0..200),
    ) {
        let right: Vec<u64> = left.iter().map(|k| (k + 7) % DOMAIN).collect();
        let build = || Micro::with_dimensions(
            2, 8, MicroParams::new(t, 2), SubWindowClock::count_based(per_sub_window), 9,
        );
        let mut first = build();
        let mut second = build();

        for (seen, (a, b)) in left.iter().zip(&right).enumerate() {
            let seen = seen as u64 + 1;
            first.insert(&key(*a));
            second.insert(&key(*b));

            prop_assert_eq!(
                first.sub_window(), seen / per_sub_window,
                "after {} items of {} per sub-window", seen, per_sub_window
            );
            prop_assert_eq!(
                first.residual_fraction(),
                1.0 - (seen % per_sub_window) as f64 / per_sub_window as f64,
                "after {} items of {} per sub-window", seen, per_sub_window
            );
            prop_assert_eq!(
                (first.sub_window(), first.residual_fraction()),
                (second.sub_window(), second.residual_fraction()),
                "the clock moved with the keys, not the count, at item {}", seen
            );
        }
    }

    // ===== A time-based window is driven by the timestamps, and never
    // backwards =====

    #[test]
    fn a_time_based_window_follows_the_high_water_timestamp(
        sub_window_len in 1u64..50,
        epoch in 0u64..100,
        t in 1usize..5,
        arrivals in prop::collection::vec((0u64..DOMAIN, 0u64..600), 0..160),
    ) {
        let mut sketch: Micro = Micro::with_dimensions(
            2, 8, MicroParams::new(t, 2), SubWindowClock::time_based(sub_window_len, epoch), 9,
        );
        let mut now = epoch;
        for (step, (k, timestamp)) in arrivals.iter().enumerate() {
            sketch.insert_at(&key(*k), *timestamp);
            now = now.max(*timestamp);
            prop_assert_eq!(
                sketch.sub_window(),
                now.saturating_sub(epoch) / sub_window_len,
                "arrival {} at t={} (high water {}) put the window at the wrong \
                 sub-window", step, timestamp, now
            );
        }
    }

    // ===== The two clocks are one clock =====
    //
    // Fed item `i` at timestamp `i`, a time-based sketch does the same work
    // in the same order as a count-based one of the same sub-window length.
    // `insert` records against the sub-window it is already in and ticks
    // afterwards; `insert_at` advances first and then records. So the
    // boundary between item `mL - 1` and item `mL` falls in the same place in
    // the sequence either way, and with it every clear, shutter close-out and
    // reclaim the boundary owes the cells. The rounding stream is driven by
    // that sequence, which makes the agreement byte-for-byte.
    //
    // The stream deliberately does not end on a boundary: there the count
    // clock has ticked past its last item and the time clock has not yet been
    // given the timestamp that would, so one has run a boundary the other has
    // not. It also runs past `T + 2` sub-windows, so the ring wraps and the
    // clearing a boundary does is visible in the bytes.

    #[test]
    fn a_time_clock_fed_the_matching_timestamps_is_the_count_clock(
        per_sub_window in 2u64..40,
        t in 1usize..5,
        keys in prop::collection::vec(0u64..DOMAIN, 1..40),
    ) {
        let build = |clock| Micro::with_dimensions(2, 8, MicroParams::new(t, 2), clock, 0x5EED);
        let mut counted = build(SubWindowClock::count_based(per_sub_window));
        let mut timed = build(SubWindowClock::time_based(per_sub_window, 0));

        for i in 0..per_sub_window * (t as u64 + 3) + 1 {
            let k = key(keys[i as usize % keys.len()]);
            counted.insert(&k);
            timed.insert_at(&k, i);
        }
        prop_assert_eq!(counted.sub_window(), timed.sub_window());
        prop_assert_eq!(
            timed.as_storage().as_slice(),
            counted.as_storage().as_slice(),
            "the two clocks disagree about what the stream left behind"
        );
    }

    // ===== Merging =====

    #[test]
    fn a_merge_needs_a_shared_frame_and_leaves_a_refused_target_alone(
        run in zooming_run(),
        other_len in 1u64..40,
        trim in 0usize..2,
    ) {
        let mut left = run.empty();
        let mut window = Window::new(run.t);
        let peak = run.drive(&mut left, &mut window);
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        // Same frame, same sub-window: the only combination `merge` accepts.
        let mut twin = run.empty();
        run.drive(&mut twin, &mut Window::new(run.t));
        prop_assert!(left.clone().merge(&twin).is_ok());

        // Every candidate below is brought to `left`'s own sub-window, so
        // each is refused for its own reason rather than because the two
        // clocks happen to have drifted apart — which is why the reason is
        // asserted and not just the refusal. A count clock's `n` is its item
        // count over its sub-window length, so filling `n` whole sub-windows
        // of the candidate's own length lands it there exactly.
        let at_the_same_sub_window = |sketch: &mut Micro, length: u64| {
            for i in 0..left.sub_window() * length {
                sketch.insert(&key(run.stream[i as usize % run.stream.len()]));
            }
        };

        // A different sub-window length numbers the ring differently.
        let other_length = run.per_sub_window + other_len;
        let mut other_frame = Micro::with_dimensions(
            run.rows, run.cols, MicroParams::new(run.t, run.c),
            SubWindowClock::count_based(other_length), run.seed,
        );
        at_the_same_sub_window(&mut other_frame, other_length);
        prop_assert_eq!(other_frame.sub_window(), left.sub_window());

        // A time clock never shares a frame with a count clock.
        let time_frame: Micro = Micro::with_dimensions(
            run.rows, run.cols, MicroParams::new(run.t, run.c),
            SubWindowClock::time_based(run.per_sub_window, 0), run.seed,
        );

        // The same frame and the same sub-window, on a wider grid: the cells
        // line up in neither number nor meaning.
        let mut wider = Micro::with_dimensions(
            run.rows, run.cols * 2, MicroParams::new(run.t, run.c),
            SubWindowClock::count_based(run.per_sub_window), run.seed,
        );
        at_the_same_sub_window(&mut wider, run.per_sub_window);
        prop_assert_eq!(wider.sub_window(), left.sub_window());

        // The same frame, held back far enough to sit in an earlier
        // sub-window than `left`.
        let mut behind = run.empty();
        let kept = run.stream.len() - (trim + 1) * run.per_sub_window as usize;
        for k in run.stream.iter().take(kept) {
            behind.insert(&key(*k));
        }
        prop_assert!(behind.sub_window() < left.sub_window());

        for (label, candidate, reason) in [
            ("a different sub-window length", &other_frame, "number sub-windows differently"),
            ("a time-based clock", &time_frame, "number sub-windows differently"),
            ("a wider grid", &wider, "different shape"),
            ("a different sub-window", &behind, "at different sub-windows"),
        ] {
            let before = left.as_storage().as_slice().to_vec();
            let err = left.merge(candidate);
            prop_assert!(
                err.as_ref().is_err_and(|detail| detail.contains(reason)),
                "{} should have been refused for {:?}, got {:?}", label, reason, err
            );
            prop_assert_eq!(
                left.as_storage().as_slice(),
                before.as_slice(),
                "{} was refused but the target changed", label
            );
        }
    }

    #[test]
    fn a_merge_never_lowers_either_sides_estimate_beyond_its_own_rounding(
        run in zooming_run(),
        offset in 1u64..DOMAIN,
    ) {
        let mut left = run.empty();
        let peak = run.drive(&mut left, &mut Window::new(run.t));
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        let shifted = Run {
            stream: run.stream.iter().map(|k| (k + offset) % DOMAIN).collect(),
            ..run.clone()
        };
        let mut right = shifted.empty();
        shifted.drive(&mut right, &mut Window::new(run.t));
        prop_assert_eq!(left.sub_window(), right.sub_window());

        let mut merged = left.clone();
        merged.merge(&right).expect("same shape, same frame, same sub-window");

        // Bringing the two sides to a common exponent divides each side's
        // pixels, and so does zooming the sum back into a byte; the estimate
        // reads `t + 1` of them.
        let slack = zoom_slack(merged.max_zoom(), run.c, run.t);
        for k in 0..DOMAIN {
            let after = merged.estimate_with(&key(k), DeltaStrategy::Over);
            for (label, side) in [("left", &left), ("right", &right)] {
                let before = side.estimate_with(&key(k), DeltaStrategy::Over);
                prop_assert!(
                    after >= before - slack,
                    "key {}: the merge put {} more than the {} slack below the {} \
                     side's {} at zoom {}",
                    k, after, slack, label, before, merged.max_zoom()
                );
            }
        }
    }

    // ===== A grid that was sized but never filled =====
    //
    // `Vector3D::init` leaves the storage empty and every bucket accessor
    // panics until `fill` has run, so a `MicroCM` around one would panic on
    // its first insert or query. No public constructor produces that state,
    // and the wire must not either: the geometry a payload declares has to
    // match the bytes it carries.

    #[test]
    fn a_grid_that_was_sized_but_never_filled_is_refused_on_the_wire(
        rows in 1usize..4,
        cols in prop_oneof![Just(2usize), Just(4), Just(8)],
        t in 1usize..5,
        c in prop_oneof![Just(2u32), Just(3), Just(4)],
        per_sub_window in 1u64..1000,
    ) {
        #[derive(Serialize)]
        struct Forged {
            cells: Vector3D<u8>,
            params: MicroParams,
            clock: SubWindowClock,
            rounding: u64,
        }

        let params = MicroParams::new(t, c);
        let depth = Micro::with_dimensions(
            rows, cols, params, SubWindowClock::count_based(per_sub_window), 0,
        ).cell_bytes();

        let forge = |cells: Vector3D<u8>| {
            let bytes = rmp_serde::to_vec_named(&Forged {
                cells,
                params,
                clock: SubWindowClock::count_based(per_sub_window),
                rounding: 1,
            }).expect("encode");
            Micro::deserialize_from_bytes(&bytes).map_err(|e| e.to_string())
        };

        // The same geometry, filled, is the control: the refusal below is
        // about the missing bytes and nothing else.
        let mut filled = Vector3D::init(rows, cols, depth);
        filled.fill(0u8);
        prop_assert!(forge(filled).is_ok(), "the control payload must decode");

        let refused = forge(Vector3D::init(rows, cols, depth));
        prop_assert!(refused.is_err(), "a grid with no bytes must be refused");
        let err = refused.err().unwrap_or_default();
        prop_assert!(
            err.contains("does not match rows * cols * depth"),
            "unexpected refusal for an unfilled {}x{}x{} grid: {}",
            rows, cols, depth, err
        );
    }

    // ===== The wire =====

    #[test]
    fn a_zoomed_sketch_round_trips_to_the_same_answers(run in zooming_run()) {
        let mut sketch = run.empty();
        let peak = run.drive(&mut sketch, &mut Window::new(run.t));
        prop_assert!(peak > 0, "the run never zoomed: {:?}", run);

        let bytes = sketch.serialize_to_bytes().expect("encode");
        let decoded = Micro::deserialize_from_bytes(&bytes).expect("decode");
        prop_assert_eq!(decoded.max_zoom(), sketch.max_zoom());
        prop_assert_eq!(decoded.sub_window(), sketch.sub_window());
        for k in 0..DOMAIN {
            prop_assert!(
                close(
                    decoded.estimate_with(&key(k), DeltaStrategy::Over),
                    sketch.estimate_with(&key(k), DeltaStrategy::Over),
                ),
                "key {} answers differently after a round trip", k
            );
        }
    }
}
