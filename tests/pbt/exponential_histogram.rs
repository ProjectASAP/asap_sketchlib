//! Property tests for the sliding-window structures: `ExponentialHistogram`,
//! `EHSketchList` and the tumbling-window wrappers.
//!
//! Datar, Gionis, Indyk, Motwani, SODA '02.
//!
//! The oracle is the raw arrival stream itself: a slice of `(time, key)`
//! pairs the test keeps beside the histogram, from which the exact window
//! content and the run of arrivals behind every bucket are recomputed. The
//! bucket laws are the paper's invariants, not a reading of
//! `merge_volumes_l1`. The tumbling oracle likewise derives the retained
//! windows from the stream, the window width and `max_windows`, never from
//! the manager's own `closed_count`.
//!
//! One Count-Min row totals to the number of inserts that reached it: an
//! insert raises exactly one cell per row by one. That makes the arrival
//! count of any bucket or merge readable without trusting the `size` field,
//! and it is exact whatever the hash does.

use crate::support::{QUANTILES, grid};
use asap_sketchlib::{
    CountMin, DataInput, EHSketchList, ExponentialHistogram, FastPath, FoldCMS, FoldCMSConfig,
    FoldCS, FoldCSConfig, KLL, KLLConfig, SketchPool, TumblingWindow, UnivMon, UnivMonQ,
    UnivMonQConfig, UnivSketchPool, Vector2D,
};
use proptest::prelude::*;
use std::collections::HashSet;

type Cm = CountMin<Vector2D<i32>, FastPath>;

const ROWS: usize = 3;
const COLS: usize = 64;

// ---------------------------------------------------------------------------
// Exponential histogram
// ---------------------------------------------------------------------------

fn proto() -> EHSketchList {
    EHSketchList::CM(Cm::with_dimensions(ROWS, COLS))
}

fn cm(list: &EHSketchList) -> &Cm {
    match list {
        EHSketchList::CM(sketch) => sketch,
        other => panic!("the histogram under test carries Count-Min, found {other:?}"),
    }
}

fn cells(list: &EHSketchList) -> Vec<i32> {
    let sketch = cm(list);
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

/// Arrivals the sketch absorbed, read off row zero.
fn arrivals_in(list: &EHSketchList) -> i64 {
    let sketch = cm(list);
    (0..sketch.cols())
        .map(|c| i64::from(sketch.as_storage().query_one_counter(0, c)))
        .sum()
}

fn cm_over(stream: &[(u64, u64)]) -> EHSketchList {
    let mut sketch = Cm::with_dimensions(ROWS, COLS);
    for (_, key) in stream {
        sketch.insert(&DataInput::U64(*key));
    }
    EHSketchList::CM(sketch)
}

fn merge_all(eh: &ExponentialHistogram) -> Option<EHSketchList> {
    let mut buckets = eh.payload.iter();
    let mut merged = buckets.next()?.bucket.clone();
    for b in buckets {
        merged.merge(&b.bucket).expect("Count-Min buckets merge");
    }
    Some(merged)
}

fn feed(eh: &mut ExponentialHistogram, stream: &[(u64, u64)]) {
    for (time, key) in stream {
        eh.update(*time, &DataInput::U64(*key));
    }
}

fn retained_count(eh: &ExponentialHistogram) -> usize {
    eh.payload.iter().map(|b| b.size).sum()
}

/// Non-decreasing timestamps over a narrow key domain, ties included. The
/// stream is far longer than any window drawn beside it, so buckets merge,
/// the window rolls and old buckets expire many times over.
fn stream(max: usize) -> impl Strategy<Value = Vec<(u64, u64)>> {
    timestamped(max, 0)
}

/// The same, with no two arrivals sharing a timestamp. A shared timestamp
/// leaves `query_interval_merge` no way to tell which of the two buckets
/// meeting at it an endpoint names, so the interval law draws its stream
/// from here.
fn distinct_stream(max: usize) -> impl Strategy<Value = Vec<(u64, u64)>> {
    timestamped(max, 1)
}

fn timestamped(max: usize, min_gap: u64) -> impl Strategy<Value = Vec<(u64, u64)>> {
    prop::collection::vec((min_gap..4, 0u64..16), 40..max).prop_map(|steps| {
        let mut t = 0u64;
        steps
            .into_iter()
            .map(|(dt, key)| {
                t += dt;
                (t, key)
            })
            .collect()
    })
}

proptest! {
    // ===== Bucket invariants =====
    //
    // The L1 rule keeps a bounded number of buckets at each size level, so a
    // window of n arrivals costs O(k log n) buckets rather than O(n).

    #[test]
    fn exponential_histogram_holds_a_bounded_bucket_count_at_every_size(
        k in 1usize..9,
        window in 8u64..48,
        stream in stream(400),
    ) {
        let mut eh = ExponentialHistogram::new(k, window, proto());
        let bound = k / 2 + 2;

        for (step, (time, key)) in stream.iter().enumerate() {
            eh.update(*time, &DataInput::U64(*key));

            let sizes: Vec<usize> = eh.payload.iter().map(|b| b.size).collect();
            for size in &sizes {
                let at_this_size = sizes.iter().filter(|s| *s == size).count();
                prop_assert!(
                    at_this_size <= bound,
                    "step {}: {} buckets of size {} with k = {}, over the bound {}: {:?}",
                    step, at_this_size, size, k, bound, sizes
                );
            }
        }
    }

    #[test]
    fn exponential_histogram_bucket_sizes_form_a_power_of_two_ladder(
        k in 1usize..9,
        window in 8u64..48,
        stream in stream(400),
    ) {
        let mut eh = ExponentialHistogram::new(k, window, proto());

        for (step, (time, key)) in stream.iter().enumerate() {
            eh.update(*time, &DataInput::U64(*key));

            for (i, b) in eh.payload.iter().enumerate() {
                prop_assert!(
                    b.size.is_power_of_two(),
                    "step {}: bucket {} has size {}, which no run of equal-size merges reaches",
                    step, i, b.size
                );
            }
            for (i, pair) in eh.payload.windows(2).enumerate() {
                prop_assert!(
                    pair[0].size >= pair[1].size,
                    "step {}: bucket {} of size {} is older than bucket {} of size {}",
                    step, i, pair[0].size, i + 1, pair[1].size
                );
            }
        }
    }

    // ===== The standard EH error bound =====
    //
    // Expiry is bucket-granular, so what the histogram retains is a superset
    // of the window: every arrival at or after the cutoff survives, and only
    // the one bucket straddling the cutoff carries arrivals from before it.
    // The overshoot is therefore at most that bucket's size.

    #[test]
    fn exponential_histogram_overcounts_the_window_by_at_most_the_oldest_bucket(
        k in 1usize..9,
        window in 8u64..48,
        stream in stream(400),
    ) {
        let mut eh = ExponentialHistogram::new(k, window, proto());

        for (step, (time, key)) in stream.iter().enumerate() {
            eh.update(*time, &DataInput::U64(*key));

            let cutoff = time.saturating_sub(window);
            let in_window = stream[..=step].iter().filter(|(t, _)| *t >= cutoff).count() as i64;
            let held = merge_all(&eh).map(|m| arrivals_in(&m)).unwrap_or(0);
            let oldest = eh.payload.first().map(|b| b.size).unwrap_or(0) as i64;

            prop_assert!(
                held >= in_window,
                "step {}: {} arrivals held, {} still inside the window [{}, {}]",
                step, held, in_window, cutoff, time
            );
            prop_assert!(
                held - in_window <= oldest,
                "step {}: {} held over {} in window, past the oldest bucket's size {}",
                step, held, in_window, oldest
            );
        }
    }

    // ===== The buckets tile the retained suffix =====
    //
    // Stronger than a claim about totals: each bucket holds one consecutive
    // run of arrivals, the runs abut, and together they are exactly the tail
    // of the stream. A bucket's declared size, its sketch's contents and its
    // time range all have to name the same run, so expiry can neither reach
    // an arrival newer than the ones it drops nor lose one it kept, and a
    // merge cannot forget where its older half began.

    #[test]
    fn exponential_histogram_buckets_tile_the_retained_suffix(
        k in 1usize..9,
        window in 8u64..48,
        stream in stream(300),
    ) {
        let mut eh = ExponentialHistogram::new(k, window, proto());

        for (step, (time, key)) in stream.iter().enumerate() {
            eh.update(*time, &DataInput::U64(*key));

            let held = retained_count(&eh);
            prop_assert!(held <= step + 1, "step {}: {} arrivals retained", step, held);
            let start = step + 1 - held;

            let mut from = start;
            for (i, b) in eh.payload.iter().enumerate() {
                let to = from + b.size;
                prop_assert_eq!(
                    arrivals_in(&b.bucket), b.size as i64,
                    "step {}: bucket {} claims size {}", step, i, b.size
                );
                prop_assert_eq!(
                    (b.min_time, b.max_time), (stream[from].0, stream[to - 1].0),
                    "step {}: bucket {} of size {} should span arrivals {}..{}",
                    step, i, b.size, from, to
                );
                from = to;
            }

            prop_assert_eq!(
                cells(&merge_all(&eh).expect("a fed histogram has buckets")),
                cells(&cm_over(&stream[start..=step])),
                "step {}: the {} retained arrivals are not the last {}", step, held, held
            );
        }
    }

    // ===== A query spanning whole buckets answers exactly =====
    //
    // Nothing inside the retained range is approximated: a query whose ends
    // land on bucket boundaries returns precisely the arrivals of those
    // buckets. The oracle is the matching slice of the raw stream.

    #[test]
    fn exponential_histogram_queries_spanning_whole_buckets_are_exact(
        k in 1usize..9,
        window in 8u64..48,
        stream in distinct_stream(300),
        lo in any::<prop::sample::Index>(),
        hi in any::<prop::sample::Index>(),
    ) {
        let mut eh = ExponentialHistogram::new(k, window, proto());
        feed(&mut eh, &stream);

        let sizes: Vec<usize> = eh.payload.iter().map(|b| b.size).collect();
        let held: usize = sizes.iter().sum();
        let start = stream.len() - held;

        let a = lo.index(sizes.len());
        let b = hi.index(sizes.len());
        let (a, b) = if a <= b { (a, b) } else { (b, a) };

        let from = start + sizes[..a].iter().sum::<usize>();
        let to = start + sizes[..=b].iter().sum::<usize>();

        let answer = eh
            .query_interval_merge(eh.payload[a].min_time, eh.payload[b].max_time)
            .expect("a fed histogram answers");

        prop_assert_eq!(
            cells(&answer),
            cells(&cm_over(&stream[from..to])),
            "buckets {}..={} of {} cover arrivals {}..{}", a, b, sizes.len(), from, to
        );
    }
}

// ---------------------------------------------------------------------------
// Tumbling windows
// ---------------------------------------------------------------------------

/// Arrivals a manager still retains after the stream. Each arrival belongs to
/// window `time / size`; the open window is the last arrival's, and eviction
/// keeps the `max_windows` closed windows immediately before it. Derived from
/// the stream and the configuration alone, never from the manager.
fn retained_after(stream: &[(u64, f64)], size: u64, max_windows: usize) -> Vec<usize> {
    let open = stream.last().map(|(t, _)| t / size).unwrap_or(0);
    let first = open.saturating_sub(max_windows as u64);
    (0..stream.len())
        .filter(|i| stream[*i].0 / size >= first)
        .collect()
}

fn timed_stream(max: usize) -> impl Strategy<Value = Vec<(u64, f64)>> {
    prop::collection::vec((1u64..5, 0u64..32), 20..max).prop_map(|steps| {
        let mut t = 0u64;
        steps
            .into_iter()
            .map(|(dt, key)| {
                t += dt;
                (t, key as f64)
            })
            .collect()
    })
}

const FOLD_ROWS: usize = 3;
const FOLD_COLS: usize = 64;
const FOLD_LEVEL: u32 = 2;
const FOLD_TOP_K: usize = 8;

fn fold_cms_config() -> FoldCMSConfig {
    FoldCMSConfig {
        rows: FOLD_ROWS,
        full_cols: FOLD_COLS,
        fold_level: FOLD_LEVEL,
        top_k: FOLD_TOP_K,
    }
}

fn fold_cs_config() -> FoldCSConfig {
    FoldCSConfig {
        rows: FOLD_ROWS,
        full_cols: FOLD_COLS,
        fold_level: FOLD_LEVEL,
        top_k: FOLD_TOP_K,
    }
}

fn roomy_kll_config() -> KLLConfig {
    KLLConfig {
        k: 512,
        m: 8,
        seed: Some(0x5eed_1234_5678_9abc),
    }
}

fn fresh_fold_cms() -> FoldCMS {
    FoldCMS::new(FOLD_ROWS, FOLD_COLS, FOLD_LEVEL, FOLD_TOP_K)
}

fn fresh_fold_cs() -> FoldCS {
    FoldCS::new(FOLD_ROWS, FOLD_COLS, FOLD_LEVEL, FOLD_TOP_K)
}

fn kll_config() -> KLLConfig {
    KLLConfig {
        k: 32,
        m: 8,
        seed: Some(0x5eed_1234_5678_9abc),
    }
}

fn univ_q_config() -> UnivMonQConfig {
    UnivMonQConfig {
        levels: 4,
        width: 256,
        depth: 3,
        candidates: 64,
        ordered_samples: 128,
        ..UnivMonQConfig::default()
    }
}

proptest! {
    // ===== Windowing partitions the stream =====
    //
    // Windows carve the stream into disjoint pieces; merging the retained
    // pieces has to rebuild what a single sketch fed those same arrivals
    // holds. `merge_same_level` and `insert` are separate code paths, and the
    // retained set is chosen by eviction rather than by the test, so neither
    // side is the other by construction.

    /// Every window between the first and the last arrival is opened and
    /// closed in turn, empty ones included, so the number of closed windows
    /// is fixed by the last timestamp and the retention limit.
    #[test]
    fn tumbling_closes_one_window_per_elapsed_period_and_keeps_the_last_few(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(200),
    ) {
        let mut tw = TumblingWindow::<FoldCMS>::new(size, max_windows, fold_cms_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let elapsed = stream.last().map(|(t, _)| t / size).unwrap_or(0) as usize;
        prop_assert_eq!(
            tw.closed_count(), elapsed.min(max_windows),
            "{} periods of width {} elapsed, retaining at most {}",
            elapsed, size, max_windows
        );
    }

    #[test]
    fn tumbling_fold_cms_windows_merge_back_into_the_single_pass_sketch(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(200),
    ) {
        let mut tw = TumblingWindow::<FoldCMS>::new(size, max_windows, fold_cms_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let kept = retained_after(&stream, size, max_windows);
        let mut single = fresh_fold_cms();
        for i in &kept {
            single.insert(&DataInput::F64(stream[*i].1), 1);
        }

        prop_assert_eq!(
            tw.query_all().to_flat_counters(),
            single.to_flat_counters(),
            "{} closed windows of width {} over {} arrivals, {} retained",
            tw.closed_count(), size, stream.len(), kept.len()
        );
    }

    #[test]
    fn tumbling_fold_cs_windows_merge_back_into_the_single_pass_sketch(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(200),
    ) {
        let mut tw = TumblingWindow::<FoldCS>::new(size, max_windows, fold_cs_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let kept = retained_after(&stream, size, max_windows);
        let mut single = fresh_fold_cs();
        for i in &kept {
            single.insert(&DataInput::F64(stream[*i].1), 1);
        }

        prop_assert_eq!(
            tw.query_all().to_flat_counters(),
            single.to_flat_counters(),
            "{} closed windows of width {} over {} arrivals, {} retained",
            tw.closed_count(), size, stream.len(), kept.len()
        );
    }

    /// Below the first compaction a KLL is an exact sample buffer, so merging
    /// the retained windows must reproduce the retained observations
    /// themselves. `merge` and `update` are separate code paths into the same
    /// buffer, and the retained set is chosen by eviction, not by the test.
    #[test]
    fn tumbling_kll_windows_merge_back_to_the_retained_observations(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(200),
    ) {
        let mut tw = TumblingWindow::<KLL>::new(size, max_windows, roomy_kll_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let kept = retained_after(&stream, size, max_windows);
        let merged = tw.query_all();

        prop_assert_eq!(
            merged.wire_num_levels(), 1,
            "the capacity was meant to hold {} observations uncompacted", kept.len()
        );

        let mut held = merged.wire_items();
        held.sort_by(f64::total_cmp);
        let mut expected: Vec<f64> = kept.iter().map(|i| stream[*i].1).collect();
        expected.sort_by(f64::total_cmp);

        prop_assert_eq!(
            held, expected,
            "{} closed windows of width {} over {} arrivals",
            tw.closed_count(), size, stream.len()
        );
    }

    /// Once compaction starts a KLL merge is randomized and the buffer cannot
    /// be compared item for item. Two weaker things still hold exactly.
    /// Compaction only ever drops and re-weights values it was handed, so no
    /// answer can fall outside the retained range. And it halves a level into
    /// survivors carrying twice the weight, rounding a level of odd size up:
    /// the levels therefore never total less than the observations the
    /// retained windows saw.
    #[test]
    fn tumbling_kll_merges_report_only_arrived_values_and_never_lose_weight(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(200),
    ) {
        let mut tw = TumblingWindow::<KLL>::new(size, max_windows, kll_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let kept = retained_after(&stream, size, max_windows);
        prop_assume!(!kept.is_empty());
        let merged = tw.query_all();

        prop_assert!(
            merged.count() >= kept.len(),
            "the merge totals {} over {} retained observations, {} levels in use",
            merged.count(), kept.len(), merged.wire_num_levels()
        );

        let arrived: HashSet<u64> = kept.iter().map(|i| stream[*i].1.to_bits()).collect();
        for item in merged.wire_items() {
            prop_assert!(
                arrived.contains(&item.to_bits()),
                "the merge holds {}, which no retained window saw", item
            );
        }

        let values: Vec<f64> = kept.iter().map(|i| stream[*i].1).collect();
        let low = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let high = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        for q in QUANTILES {
            let answer = merged.quantile(q);
            prop_assert!(
                (low..=high).contains(&answer),
                "quantile {} answered {}, outside the retained range [{}, {}]",
                q, answer, low, high
            );
        }
    }

    /// UnivMon-Q's counters are signed and randomized, but its stream
    /// cardinality and its extremes are tracked exactly through a merge.
    #[test]
    fn tumbling_univmon_q_windows_merge_back_to_the_retained_weight(
        size in 2u64..16,
        max_windows in 1usize..6,
        stream in timed_stream(150),
    ) {
        let mut tw = TumblingWindow::<UnivMonQ>::new(size, max_windows, univ_q_config(), 2);
        for (t, key) in &stream {
            tw.insert(*t, &DataInput::F64(*key), 1);
        }

        let kept = retained_after(&stream, size, max_windows);
        let merged = tw.query_all();

        prop_assert_eq!(
            merged.count(), kept.len() as u64,
            "{} closed windows of width {} over {} arrivals",
            tw.closed_count(), size, stream.len()
        );

        let values: Vec<f64> = kept.iter().map(|i| stream[*i].1).collect();
        let low = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let high = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        if !values.is_empty() {
            prop_assert_eq!(merged.min(), Some(low), "merged minimum");
            prop_assert_eq!(merged.max(), Some(high), "merged maximum");
        }
    }

    // ===== Recycling is indistinguishable from allocating =====
    //
    // The pool hands a used sketch back to the next window. A recycled
    // instance must match a fresh one when it arrives, and must go on
    // matching it once both are fed the same second half of the stream:
    // anything `tumbling_clear` forgets to reset is only visible through the
    // answers the next window gives, not through its empty state.

    #[test]
    fn sketch_pool_hands_back_a_fold_cms_matching_a_fresh_one(
        stream in timed_stream(120),
    ) {
        let (first, second) = stream.split_at(stream.len() / 2);
        let mut pool = SketchPool::<FoldCMS>::new(0, fold_cms_config());
        let mut used = pool.take();
        for (_, key) in first {
            used.insert(&DataInput::F64(*key), 1);
        }
        pool.put(used);

        let mut recycled = pool.take();
        let mut fresh = fresh_fold_cms();

        prop_assert_eq!(recycled.to_flat_counters(), fresh.to_flat_counters(), "counters handed back");
        prop_assert_eq!(recycled.total_entries(), fresh.total_entries(), "entries handed back");
        prop_assert_eq!(recycled.heap().heap().len(), fresh.heap().heap().len(), "heap handed back");

        for (_, key) in second {
            recycled.insert(&DataInput::F64(*key), 1);
            fresh.insert(&DataInput::F64(*key), 1);
        }

        prop_assert_eq!(recycled.to_flat_counters(), fresh.to_flat_counters(), "counters reused");
        prop_assert_eq!(recycled.total_entries(), fresh.total_entries(), "entries reused");
        prop_assert_eq!(recycled.heap().heap().len(), fresh.heap().heap().len(), "heap reused");
    }

    #[test]
    fn sketch_pool_hands_back_a_fold_cs_matching_a_fresh_one(
        stream in timed_stream(120),
    ) {
        let (first, second) = stream.split_at(stream.len() / 2);
        let mut pool = SketchPool::<FoldCS>::new(0, fold_cs_config());
        let mut used = pool.take();
        for (_, key) in first {
            used.insert(&DataInput::F64(*key), 1);
        }
        pool.put(used);

        let mut recycled = pool.take();
        let mut fresh = fresh_fold_cs();

        prop_assert_eq!(recycled.to_flat_counters(), fresh.to_flat_counters(), "counters handed back");
        prop_assert_eq!(recycled.total_entries(), fresh.total_entries(), "entries handed back");
        prop_assert_eq!(recycled.heap().heap().len(), fresh.heap().heap().len(), "heap handed back");

        for (_, key) in second {
            recycled.insert(&DataInput::F64(*key), 1);
            fresh.insert(&DataInput::F64(*key), 1);
        }

        prop_assert_eq!(recycled.to_flat_counters(), fresh.to_flat_counters(), "counters reused");
        prop_assert_eq!(recycled.total_entries(), fresh.total_entries(), "entries reused");
        prop_assert_eq!(recycled.heap().heap().len(), fresh.heap().heap().len(), "heap reused");
    }

    /// A seeded pool re-seeds on clear, so a recycled KLL is the fresh sketch
    /// down to the compaction coin, and stays byte-identical to it under a
    /// second stream that compacts both.
    #[test]
    fn sketch_pool_hands_back_a_kll_matching_a_fresh_one(
        stream in timed_stream(120),
    ) {
        let (first, second) = stream.split_at(stream.len() / 2);
        let config = kll_config();
        let mut pool = SketchPool::<KLL>::new(0, config.clone());
        let mut used = pool.take();
        for (_, key) in first {
            let _ = used.update_data_input(&DataInput::F64(*key));
        }
        pool.put(used);

        let mut recycled = pool.take();
        let mut fresh = KLL::init_with_seed(config.k, config.m, config.seed.expect("seeded"));

        prop_assert_eq!(recycled.count(), fresh.count(), "count handed back");
        prop_assert_eq!(recycled.wire_items(), fresh.wire_items(), "items handed back");
        prop_assert_eq!(recycled.wire_levels(), fresh.wire_levels(), "levels handed back");
        prop_assert_eq!(recycled.wire_coin(), fresh.wire_coin(), "coin handed back");

        for (_, key) in second {
            let _ = recycled.update_data_input(&DataInput::F64(*key));
            let _ = fresh.update_data_input(&DataInput::F64(*key));
        }

        prop_assert_eq!(recycled.count(), fresh.count(), "count reused");
        prop_assert_eq!(recycled.wire_items(), fresh.wire_items(), "items reused");
        prop_assert_eq!(recycled.wire_levels(), fresh.wire_levels(), "levels reused");
        prop_assert_eq!(recycled.wire_num_levels(), fresh.wire_num_levels(), "levels in use reused");
        prop_assert_eq!(recycled.wire_coin(), fresh.wire_coin(), "coin reused");
    }

    #[test]
    fn sketch_pool_hands_back_a_univmon_q_matching_a_fresh_one(
        stream in timed_stream(120),
    ) {
        let (first, second) = stream.split_at(stream.len() / 2);
        let mut pool = SketchPool::<UnivMonQ>::new(0, univ_q_config());
        let mut used = pool.take();
        for (_, key) in first {
            let _ = used.update_data_input(&DataInput::F64(*key));
        }
        pool.put(used);

        let mut recycled = pool.take();
        let mut fresh = UnivMonQ::new(univ_q_config()).expect("config");

        prop_assert_eq!(recycled.count(), fresh.count(), "count handed back");
        prop_assert_eq!(recycled.cdf(), fresh.cdf(), "cdf handed back");

        for (_, key) in second {
            let _ = recycled.update_data_input(&DataInput::F64(*key));
            let _ = fresh.update_data_input(&DataInput::F64(*key));
        }

        prop_assert_eq!(recycled.count(), fresh.count(), "count reused");
        prop_assert_eq!(recycled.min(), fresh.min(), "minimum reused");
        prop_assert_eq!(recycled.max(), fresh.max(), "maximum reused");
        prop_assert_eq!(recycled.estimate_l1(), fresh.estimate_l1(), "L1 reused");
        prop_assert_eq!(recycled.estimate_f2(), fresh.estimate_f2(), "F2 reused");
        prop_assert_eq!(recycled.cdf(), fresh.cdf(), "cdf reused");
    }

    #[test]
    fn univ_sketch_pool_hands_back_a_univmon_matching_a_fresh_one(
        stream in timed_stream(120),
    ) {
        let (first, second) = stream.split_at(stream.len() / 2);
        let mut pool = UnivSketchPool::new(0, 8, 3, 256, 4);
        let mut used = pool.take();
        for (_, key) in first {
            used.insert(&DataInput::F64(*key), 1);
        }
        pool.put(used);

        let mut recycled = pool.take();
        let mut fresh = UnivMon::init_univmon(8, 3, 256, 4);

        prop_assert_eq!(recycled.calc_l1(), fresh.calc_l1(), "L1 handed back");
        prop_assert_eq!(recycled.calc_card(), fresh.calc_card(), "cardinality handed back");

        for (_, key) in second {
            recycled.insert(&DataInput::F64(*key), 1);
            fresh.insert(&DataInput::F64(*key), 1);
        }

        prop_assert_eq!(recycled.calc_l1(), fresh.calc_l1(), "L1 reused");
        prop_assert_eq!(recycled.calc_l2(), fresh.calc_l2(), "L2 reused");
        prop_assert_eq!(recycled.calc_card(), fresh.calc_card(), "cardinality reused");
        prop_assert_eq!(
            recycled.candidates_complete(), fresh.candidates_complete(), "candidate flags reused"
        );
    }
}
