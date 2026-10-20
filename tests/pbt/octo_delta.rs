//! Property tests for the OctoSketch delta protocol.
//!
//! The oracle is a single-threaded sketch over the same stream: whatever the
//! workers promoted plus whatever they still hold must reconstruct it cell for
//! cell, and a flush must close the gap exactly.
//!
//! The reconstruction is stated per delta type, because the types do not merge
//! alike. Count-Min, Count, DDSketch and the UnivMon layers apply by addition,
//! so promoted plus residual is an exact cell-for-cell equality. HyperLogLog
//! applies by maximum and never clears a register, so the parent trails the
//! worker by a bounded gain rather than by a held counter. CocoSketch and
//! Elastic replay a promoted `<key, counter>` through the parent's own
//! insertion logic, which picks its own victim and may evict a different flow,
//! so what survives is the conservation of the stream's total mass rather than
//! any per-bucket equality.

use std::collections::{HashMap, HashSet};

use asap_sketchlib::octo_delta::DdSign;
use asap_sketchlib::sketches::hll::HyperLogLogImpl;
use asap_sketchlib::{
    Classic, CocoDelta, CocoOctoAggregator, CocoSketch, CocoSketchWorker, CountMinSketch,
    CountMinSketchCellDelta, CountMinSketchOctoAggregator, CountMinSketchWithHeapOctoAggregator,
    CountMinSketchWithHeapOctoPlan, CountMinSketchWorker, CountSketch, CountSketchCellDelta,
    CountSketchOctoAggregator, CountSketchWithHeapOctoAggregator, CountSketchWithL2Norm,
    CountSketchWithL2NormWorker, CountSketchWorker, DataInput, DdOctoAggregator, DdSketch,
    DdSketchBucketDelta, DdSketchWorker, ElasticDelta, ElasticOctoAggregator, ElasticSketch,
    ElasticSketchWorker, HllOctoAggregator, HllOctoPlan, HllRegisterDelta, HyperLogLog,
    KeyedCountSketchCellDelta, OCTO_COCO_DEFAULT_THRESHOLD, OCTO_COUNT_MIN_DEFAULT_THRESHOLD,
    OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, OCTO_DDSKETCH_DEFAULT_THRESHOLD,
    OCTO_ELASTIC_DEFAULT_THRESHOLD, OCTO_HLL_DEFAULT_THRESHOLD, OCTO_MAX_PROMOTION_THRESHOLD,
    OCTO_UNIVMON_DEFAULT_THRESHOLD, OctoAggregator, OctoPlan, OctoThreshold, OctoWorker,
    RegularPath, UnivMonCellDelta, UnivMonOctoPlan, Vector2D, max_hll_threshold,
    univmon_layer_threshold,
};
use proptest::prelude::*;

asap_sketchlib::impl_hll_registers!(BucketsP4, 4, 1_usize << 4);

type Cm = CountMinSketch<Vector2D<i32>, RegularPath>;
type Cs = CountSketch<Vector2D<i32>, RegularPath>;
type Hll = HyperLogLog<Classic>;
/// Sixteen registers, so a register is improved many times over one stream and
/// two shards land on the same one. At the default precision a register is
/// touched once and `max` is indistinguishable from overwrite.
type SmallHll = HyperLogLogImpl<Classic, BucketsP4>;

fn stream(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(0u64..256, 0..max)
}

fn cm_cells(sketch: &Cm) -> Vec<i32> {
    let (rows, cols) = (sketch.rows(), sketch.cols());
    (0..rows)
        .flat_map(|r| (0..cols).map(move |c| (r, c)))
        .map(|(r, c)| sketch.as_storage().query_one_counter(r, c))
        .collect()
}

fn cs_cells(sketch: &Cs) -> Vec<i32> {
    let (rows, cols) = (sketch.rows(), sketch.cols());
    (0..rows)
        .flat_map(|r| (0..cols).map(move |c| (r, c)))
        .map(|(r, c)| sketch.as_storage().query_one_counter(r, c))
        .collect()
}

fn cm_reference(rows: usize, cols: usize, keys: &[u64]) -> Cm {
    let mut s = Cm::with_dimensions(rows, cols);
    for k in keys {
        s.update(&DataInput::U64(*k));
    }
    s
}

fn cs_reference(rows: usize, cols: usize, keys: &[u64]) -> Cs {
    let mut s = Cs::with_dimensions(rows, cols);
    for k in keys {
        s.update(&DataInput::U64(*k));
    }
    s
}

/// A stream of samples a DDSketch of any alpha here can index, drawn from a
/// domain narrow enough that buckets repeat and promote. Zero is in it, so the
/// zero store is exercised alongside the two signed ones.
fn dd_stream(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(-8i32..=8, 0..max)
        .prop_map(|v| v.into_iter().map(f64::from).collect::<Vec<f64>>())
}

fn dd_alpha() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.01f64), Just(0.05), Just(0.2)]
}

/// Orders the three stores so a failure names one bucket the same way twice.
fn dd_store_tag(store: DdSign) -> u8 {
    match store {
        DdSign::Negative => 0,
        DdSign::Zero => 1,
        DdSign::Positive => 2,
    }
}

/// Every non-empty bucket, keyed the way a `DdSketchBucketDelta` addresses one.
fn dd_buckets(sketch: &DdSketch) -> HashMap<(DdSign, i32), u64> {
    let mut buckets = HashMap::new();
    for (i, count) in sketch.positive_store_counts().iter().enumerate() {
        if *count != 0 {
            buckets.insert(
                (DdSign::Positive, sketch.positive_store_offset() + i as i32),
                *count,
            );
        }
    }
    for (i, count) in sketch.negative_store_counts().iter().enumerate() {
        if *count != 0 {
            buckets.insert(
                (DdSign::Negative, sketch.negative_store_offset() + i as i32),
                *count,
            );
        }
    }
    if sketch.zero_count() != 0 {
        buckets.insert((DdSign::Zero, 0), sketch.zero_count());
    }
    buckets
}

fn dd_sorted_buckets(sketch: &DdSketch) -> Vec<((DdSign, i32), u64)> {
    let mut all: Vec<((DdSign, i32), u64)> = dd_buckets(sketch).into_iter().collect();
    all.sort_by_key(|((store, index), _)| (dd_store_tag(*store), *index));
    all
}

fn dd_reference(alpha: f64, values: &[f64]) -> DdSketch {
    let mut s = DdSketch::new(alpha);
    for v in values {
        s.add(v);
    }
    s
}

/// `2^exp`, saturating, so a register gain can be compared against a threshold
/// without overflowing.
fn pow2(exp: u8) -> u128 {
    if exp >= 127 { u128::MAX } else { 1u128 << exp }
}

fn hll_reference(keys: &[u64]) -> Hll {
    let mut s = Hll::default();
    for k in keys {
        s.update(&DataInput::U64(*k));
    }
    s
}

fn small_hll_reference(keys: &[u64]) -> SmallHll {
    let mut s = SmallHll::new();
    for k in keys {
        s.update(&DataInput::U64(*k));
    }
    s
}

/// The flow key a keyed sketch is driven with. Rendered here rather than by
/// `flow_key_string`, so the laws do not inherit the crate's own rendering.
fn flow(key: u64) -> String {
    key.to_string()
}

/// Total mass the parent table holds, over every bucket.
fn coco_mass(sketch: &CocoSketch) -> u64 {
    (0..sketch.rows)
        .flat_map(|i| (0..sketch.cols).map(move |j| (i, j)))
        .map(|(i, j)| sketch.table[i][j].val)
        .sum()
}

/// Mass an Elastic parent holds: resident votes, plus the light layer's cell
/// total divided by the rows each light insert writes to.
fn elastic_mass(sketch: &ElasticSketch) -> (i64, i64) {
    let heavy: i64 = sketch.heavy.iter().map(|b| i64::from(b.vote_pos)).sum();
    let light: i64 = cm_cells(&sketch.light).iter().map(|c| i64::from(*c)).sum();
    (heavy, light)
}

fn l2hh_cells(sketch: &CountSketchWithL2Norm) -> Vec<i64> {
    let (rows, cols) = (sketch.rows(), sketch.cols());
    (0..rows)
        .flat_map(|r| (0..cols).map(move |c| (r, c)))
        .map(|(r, c)| sketch.as_storage().query_one_counter(r, c))
        .collect()
}

fn l2hh_reference(rows: usize, cols: usize, layer: usize, keys: &[u64]) -> CountSketchWithL2Norm {
    let mut s = CountSketchWithL2Norm::with_dimensions_and_seed_index(rows, cols, layer);
    for k in keys {
        s.update_weighted(&DataInput::U64(*k), 1);
    }
    s
}

/// Fisher-Yates over a xorshift stream, so a reordering is reproducible from
/// the drawn seed.
fn shuffled<T: Clone>(items: &[T], seed: u64) -> Vec<T> {
    let mut out = items.to_vec();
    let mut state = seed | 1;
    for i in (1..out.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.swap(i, (state % (i as u64 + 1)) as usize);
    }
    out
}

proptest! {
    // ===== Count-Min, full-precision worker =====

    #[test]
    fn cm_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        keys in stream(400),
    ) {
        let mut child = Cm::with_dimensions(rows, cols);
        let mut parent = Cm::with_dimensions(rows, cols);
        for k in &keys {
            child.update_emit_delta_with_threshold(&DataInput::U64(*k), tau, &mut |d| {
                parent.apply_delta(d)
            });
        }

        let reference = cm_reference(rows, cols, &keys);
        for (i, ((c, p), r)) in cm_cells(&child)
            .into_iter()
            .zip(cm_cells(&parent))
            .zip(cm_cells(&reference))
            .enumerate()
        {
            prop_assert_eq!(p + c, r, "cell {}: promoted {} plus residual {} != {}", i, p, c, r);
            prop_assert!(c < tau as i32, "cell {}: residual {} reached the threshold", i, c);
        }
    }

    // ===== Count-Min, compact worker against the full-precision parent =====

    #[test]
    fn cm_worker_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        keys in stream(400),
    ) {
        let mut worker = CountMinSketchWorker::new(rows, cols);
        let mut parent = CountMinSketchOctoAggregator::new(rows, cols);
        for k in &keys {
            worker.update_emit_delta(&DataInput::U64(*k), tau, &mut |d| parent.apply(d));
        }

        let residual = worker.residual().to_vec();
        let reference = cm_reference(rows, cols, &keys);
        for (i, (p, r)) in cm_cells(&parent.sketch).into_iter().zip(cm_cells(&reference)).enumerate() {
            prop_assert_eq!(
                p + i32::from(residual[i]), r,
                "cell {}: promoted {} plus residual {} != {}", i, p, residual[i], r
            );
            prop_assert!(
                u32::from(residual[i]) < tau,
                "cell {}: residual {} reached the threshold", i, residual[i]
            );
        }
    }

    #[test]
    fn cm_worker_flush_leaves_the_parent_exact(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        shards in 1usize..5,
        keys in stream(400),
    ) {
        let mut workers: Vec<CountMinSketchWorker> = (0..shards).map(|_| CountMinSketchWorker::new(rows, cols)).collect();
        let mut parent = CountMinSketchOctoAggregator::new(rows, cols);
        for (i, k) in keys.iter().enumerate() {
            workers[i % shards].update_emit_delta(&DataInput::U64(*k), tau, &mut |d| parent.apply(d));
        }
        for worker in &mut workers {
            worker.flush(&mut |d| parent.apply(d));
        }

        prop_assert_eq!(cm_cells(&parent.sketch), cm_cells(&cm_reference(rows, cols, &keys)));
        for worker in &workers {
            prop_assert!(
                worker.residual().iter().all(|c| *c == 0),
                "a flush left a counter held back"
            );
        }
    }

    // ===== Count sketch =====

    #[test]
    fn count_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        keys in stream(400),
    ) {
        let mut child = Cs::with_dimensions(rows, cols);
        let mut parent = Cs::with_dimensions(rows, cols);
        for k in &keys {
            child.update_emit_delta_with_threshold(&DataInput::U64(*k), tau, &mut |d| {
                parent.apply_delta(d)
            });
        }

        let reference = cs_reference(rows, cols, &keys);
        for (i, ((c, p), r)) in cs_cells(&child)
            .into_iter()
            .zip(cs_cells(&parent))
            .zip(cs_cells(&reference))
            .enumerate()
        {
            prop_assert_eq!(p + c, r, "cell {}: promoted {} plus residual {} != {}", i, p, c, r);
            prop_assert!(c.abs() < tau as i32, "cell {}: residual {} reached the threshold", i, c);
        }
    }

    #[test]
    fn count_worker_flush_leaves_the_parent_exact(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        shards in 1usize..5,
        keys in stream(400),
    ) {
        let mut workers: Vec<CountSketchWorker> =
            (0..shards).map(|_| CountSketchWorker::new(rows, cols)).collect();
        let mut parent = CountSketchOctoAggregator::new(rows, cols);
        for (i, k) in keys.iter().enumerate() {
            workers[i % shards].update_emit_delta(&DataInput::U64(*k), tau, &mut |d| parent.apply(d));
        }
        for worker in &mut workers {
            worker.flush(&mut |d| parent.apply(d));
        }

        prop_assert_eq!(cs_cells(&parent.sketch), cs_cells(&cs_reference(rows, cols, &keys)));
        for worker in &workers {
            prop_assert!(
                worker.residual().iter().all(|c| *c == 0),
                "a flush left a counter held back"
            );
        }
    }

    #[test]
    fn count_worker_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        tau in 1u32..=OCTO_MAX_PROMOTION_THRESHOLD,
        keys in stream(400),
    ) {
        let mut worker = CountSketchWorker::new(rows, cols);
        let mut parent = CountSketchOctoAggregator::new(rows, cols);
        for k in &keys {
            worker.update_emit_delta(&DataInput::U64(*k), tau, &mut |d| parent.apply(d));
        }

        let residual = worker.residual().to_vec();
        let reference = cs_reference(rows, cols, &keys);
        for (i, (p, r)) in cs_cells(&parent.sketch).into_iter().zip(cs_cells(&reference)).enumerate() {
            prop_assert_eq!(
                p + i32::from(residual[i]), r,
                "cell {}: promoted {} plus residual {} != {}", i, p, residual[i], r
            );
            prop_assert!(
                residual[i].unsigned_abs() < tau as u8,
                "cell {}: residual {} reached the threshold", i, residual[i]
            );
        }
    }

    // ===== DDSketch: a sparse bucket store, applied by addition =====

    #[test]
    fn dd_promotions_and_residual_reconstruct_a_single_pass(
        alpha in dd_alpha(),
        tau in 1u32..=8,
        values in dd_stream(400),
    ) {
        let mut worker = DdSketchWorker::new(alpha);
        let mut parent = DdOctoAggregator::new(alpha);
        for v in &values {
            worker.add_emit_delta(*v, tau, &mut |d| parent.apply(d));
        }

        let reference = dd_reference(alpha, &values);
        let promoted = dd_buckets(&parent.sketch);
        for (bucket, r) in dd_sorted_buckets(&reference) {
            let p = promoted.get(&bucket).copied().unwrap_or(0);
            let held = u64::from(worker.residual().get(&bucket).copied().unwrap_or(0));
            prop_assert_eq!(
                p + held, r,
                "bucket {:?}: promoted {} plus residual {} != {}", bucket, p, held, r
            );
        }
        for (bucket, p) in dd_sorted_buckets(&parent.sketch) {
            prop_assert!(
                dd_buckets(&reference).contains_key(&bucket),
                "bucket {:?} holds {} the stream never put there", bucket, p
            );
        }
        prop_assert_eq!(
            parent.sketch.get_count() + worker.held_back(),
            reference.get_count()
        );
        for (bucket, held) in worker.residual() {
            prop_assert!(
                u32::from(*held) < tau,
                "bucket {:?}: residual {} reached the threshold", bucket, held
            );
        }
    }

    #[test]
    fn dd_worker_flush_leaves_the_parent_exact(
        alpha in dd_alpha(),
        tau in 1u32..=8,
        shards in 1usize..5,
        values in dd_stream(400),
    ) {
        let mut workers: Vec<DdSketchWorker> = (0..shards).map(|_| DdSketchWorker::new(alpha)).collect();
        let mut parent = DdOctoAggregator::new(alpha);
        for (i, v) in values.iter().enumerate() {
            workers[i % shards].add_emit_delta(*v, tau, &mut |d| parent.apply(d));
        }
        for worker in &mut workers {
            worker.flush(&mut |d| parent.apply(d));
        }

        let reference = dd_reference(alpha, &values);
        prop_assert_eq!(dd_sorted_buckets(&parent.sketch), dd_sorted_buckets(&reference));
        prop_assert_eq!(parent.sketch.get_count(), reference.get_count());
        for worker in &workers {
            prop_assert!(worker.held_back() == 0, "a flush left a bucket held back");
        }
    }

    // ===== HyperLogLog: registers applied by maximum, never cleared =====

    #[test]
    fn hll_promotions_leave_the_parent_within_one_threshold_gain(
        tau in 0u8..=6,
        keys in stream(400),
    ) {
        let mut worker = SmallHll::new();
        let mut parent = SmallHll::new();
        for k in &keys {
            worker.update_emit_delta_with_threshold(&DataInput::U64(*k), tau, &mut |d| {
                parent.apply_delta(d)
            });
        }

        let reference = small_hll_reference(&keys);
        for (i, (p, r)) in parent
            .registers_as_slice()
            .iter()
            .copied()
            .zip(reference.registers_as_slice().iter().copied())
            .enumerate()
        {
            prop_assert!(p <= r, "register {}: the parent holds {} above the stream's {}", i, p, r);
            prop_assert!(
                pow2(r) - pow2(p) < pow2(tau),
                "register {}: {} behind {} is a gain of at least 2^{}", i, p, r, tau
            );
        }
    }

    #[test]
    fn hll_workers_flush_to_a_single_pass(
        tau in 0u8..=6,
        shards in 1usize..5,
        keys in stream(400),
    ) {
        let plan = HllOctoPlan::with_threshold(tau);
        let mut workers: Vec<_> = (0..shards).map(|i| plan.worker(i)).collect();
        let mut parent = HllOctoAggregator::new();
        for (i, k) in keys.iter().enumerate() {
            let payload = plan.prepare(&DataInput::U64(*k));
            workers[i % shards].process(&payload, &mut |d| parent.apply(d));
        }
        for w in &mut workers {
            w.flush(&mut |d| parent.apply(d));
        }

        let reference = hll_reference(&keys);
        prop_assert_eq!(
            parent.sketch.registers_as_slice(),
            reference.registers_as_slice()
        );
    }

    /// At a threshold of 0 a worker holds nothing back, so the shards need no
    /// flush and the parent is the register-wise maximum outright.
    #[test]
    fn hll_deltas_merge_to_the_register_wise_maximum(
        shards in 1usize..5,
        keys in stream(400),
    ) {
        let mut workers: Vec<SmallHll> = (0..shards).map(|_| SmallHll::new()).collect();
        let mut parent = SmallHll::new();
        for (i, k) in keys.iter().enumerate() {
            workers[i % shards].update_emit_delta_with_threshold(
                &DataInput::U64(*k),
                0,
                &mut |d| parent.apply_delta(d),
            );
        }

        let shard_references: Vec<SmallHll> = (0..shards)
            .map(|s| {
                let shard: Vec<u64> = keys
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i % shards == s)
                    .map(|(_, k)| *k)
                    .collect();
                small_hll_reference(&shard)
            })
            .collect();
        for (i, p) in parent.registers_as_slice().iter().copied().enumerate() {
            let highest = shard_references
                .iter()
                .map(|r| r.registers_as_slice()[i])
                .max()
                .unwrap_or(0);
            prop_assert_eq!(p, highest, "register {}: {} is not the shards' maximum {}", i, p, highest);
        }
        let reference = small_hll_reference(&keys);
        prop_assert_eq!(parent.registers_as_slice(), reference.registers_as_slice());
    }

    #[test]
    fn hll_delta_application_is_order_independent_and_idempotent(
        tau in 0u8..=6,
        keys in stream(400),
        seed in any::<u64>(),
    ) {
        let mut worker = SmallHll::new();
        let mut deltas: Vec<HllRegisterDelta> = Vec::new();
        for k in &keys {
            worker.update_emit_delta_with_threshold(&DataInput::U64(*k), tau, &mut |d| deltas.push(d));
        }

        let mut ordered = SmallHll::new();
        for d in &deltas {
            ordered.apply_delta(*d);
        }
        let mut reordered = SmallHll::new();
        for d in &shuffled(&deltas, seed) {
            reordered.apply_delta(*d);
        }
        let mut twice = SmallHll::new();
        for d in deltas.iter().chain(deltas.iter()) {
            twice.apply_delta(*d);
        }

        prop_assert_eq!(ordered.registers_as_slice(), reordered.registers_as_slice());
        prop_assert_eq!(ordered.registers_as_slice(), twice.registers_as_slice());
    }

    // ===== CocoSketch: a keyed bucket replayed through the parent's election =====

    #[test]
    fn coco_promotions_and_residual_conserve_the_stream_mass(
        w in 1usize..8,
        d in 1usize..4,
        tau in 1u32..=8,
        seed in any::<u64>(),
        keys in stream(300),
    ) {
        let mut worker = CocoSketchWorker::with_seed(d, w, seed);
        let mut deltas: Vec<CocoDelta> = Vec::new();
        for k in &keys {
            worker.update_emit_delta(&flow(*k), tau, &mut |delta| deltas.push(delta));
        }

        let promoted: u64 = deltas.iter().map(|delta| delta.count).sum();
        let held: u64 = worker.residual().iter().map(|c| u64::from(*c)).sum();
        prop_assert_eq!(
            promoted + held, keys.len() as u64,
            "promoted {} plus residual {} != {} inserts", promoted, held, keys.len()
        );

        let seen: HashSet<String> = keys.iter().map(|k| flow(*k)).collect();
        for delta in &deltas {
            prop_assert_eq!(delta.count, u64::from(tau), "a promotion carried {}", delta.count);
            prop_assert!(seen.contains(&delta.key), "promoted key {} never arrived", delta.key);
        }
        for (i, held) in worker.residual().iter().enumerate() {
            prop_assert!(
                u32::from(*held) < tau,
                "bucket {}: residual {} reached the threshold", i, held
            );
        }
    }

    #[test]
    fn coco_workers_flush_to_the_parents_whole_mass(
        w in 1usize..8,
        d in 1usize..4,
        tau in 1u32..=8,
        shards in 1usize..5,
        seed in any::<u64>(),
        keys in stream(300),
    ) {
        let mut workers: Vec<CocoSketchWorker> = (0..shards)
            .map(|i| CocoSketchWorker::with_seed(d, w, seed.wrapping_add(i as u64 + 1)))
            .collect();
        let mut parent = CocoOctoAggregator::with_seed(w, d, seed);
        for (i, k) in keys.iter().enumerate() {
            workers[i % shards].update_emit_delta(&flow(*k), tau, &mut |delta| parent.apply(delta));
        }
        for worker in &mut workers {
            worker.flush(&mut |delta| parent.apply(delta));
        }

        prop_assert_eq!(
            coco_mass(&parent.sketch), keys.len() as u64,
            "the parent table holds {} of {} inserts", coco_mass(&parent.sketch), keys.len()
        );
        for worker in &workers {
            prop_assert!(
                worker.residual().iter().all(|c| *c == 0),
                "a flush left a bucket held back"
            );
        }
    }

    /// The victim bucket is the least loaded of the ones the key maps to. One
    /// bucket per array puts every key on the same `d` of them, so a stream of
    /// distinct keys that never reaches the threshold fills them evenly.
    #[test]
    fn coco_worker_fills_the_least_loaded_bucket(
        d in 2usize..5,
        n in 2usize..120,
        seed in any::<u64>(),
    ) {
        let mut worker = CocoSketchWorker::with_seed(d, 1, seed);
        let mut promoted = 0usize;
        for k in 0..n as u64 {
            worker.update_emit_delta(&flow(k), OCTO_MAX_PROMOTION_THRESHOLD, &mut |_| promoted += 1);
        }

        prop_assert_eq!(promoted, 0, "{} inserts promoted at a threshold of {}", n, OCTO_MAX_PROMOTION_THRESHOLD);
        let counters = worker.residual();
        let lowest = counters.iter().copied().min().unwrap();
        let highest = counters.iter().copied().max().unwrap();
        prop_assert!(
            highest - lowest <= 1,
            "{} inserts left the buckets at {:?}, not filled least-loaded first", n, counters
        );
        prop_assert_eq!(counters.iter().map(|c| u64::from(*c)).sum::<u64>(), n as u64);
    }

    /// At a threshold of 1 every bucket is empty when an insert arrives, so the
    /// arrival always wins the election and the promotion names it rather than
    /// the key the bucket held before.
    #[test]
    fn coco_at_threshold_one_promotes_the_arriving_key(
        w in 1usize..8,
        d in 1usize..4,
        seed in any::<u64>(),
        keys in stream(300),
    ) {
        let mut worker = CocoSketchWorker::with_seed(d, w, seed);
        for k in &keys {
            let arrival = flow(*k);
            let mut promoted: Vec<CocoDelta> = Vec::new();
            worker.update_emit_delta(&arrival, 1, &mut |delta| promoted.push(delta));

            prop_assert_eq!(
                promoted.len(), 1,
                "an insert at a threshold of 1 promoted {} buckets", promoted.len()
            );
            prop_assert_eq!(
                &promoted[0].key, &arrival,
                "the promotion named {} rather than the arriving {}", promoted[0].key, arrival
            );
            prop_assert_eq!(promoted[0].count, 1);
        }
        prop_assert!(worker.residual().iter().all(|c| *c == 0));
    }

    // ===== Elastic sketch: a heavy half with replacement, a light half by addition =====

    #[test]
    fn elastic_promotions_and_flush_conserve_the_stream_mass(
        buckets in 1usize..8,
        light_rows in 1usize..4,
        light_cols in 1usize..16,
        tau in 1u32..=8,
        keys in stream(300),
    ) {
        let mut worker = ElasticSketchWorker::new(buckets, light_rows, light_cols);
        let mut deltas: Vec<ElasticDelta> = Vec::new();
        for k in &keys {
            worker.update_emit_delta(&flow(*k), tau, &mut |delta| deltas.push(delta));
        }
        let promoted_during_inserts = deltas.len();
        worker.flush(&mut |delta| deltas.push(delta));

        let rows = worker.light().rows() as u64;
        let (mut heavy, mut evicted, mut light) = (0u64, 0u64, 0u64);
        for delta in &deltas {
            match delta {
                ElasticDelta::Heavy { votes, .. } => heavy += u64::from(*votes),
                ElasticDelta::Evicted { votes, .. } => evicted += u64::from(*votes),
                ElasticDelta::Light(cell) => light += cell.count as u64,
            }
        }
        prop_assert_eq!(
            light % rows, 0,
            "a light total of {} is not a whole number of {}-row inserts", light, rows
        );
        prop_assert_eq!(
            heavy + evicted + light / rows, keys.len() as u64,
            "heavy {} plus evicted {} plus light {} over {} rows != {} inserts",
            heavy, evicted, light, rows, keys.len()
        );

        let seen: HashSet<String> = keys.iter().map(|k| flow(*k)).collect();
        for delta in &deltas[..promoted_during_inserts] {
            match delta {
                ElasticDelta::Heavy { key, votes, .. } => {
                    prop_assert_eq!(*votes, tau, "a heavy promotion carried {}", votes);
                    prop_assert!(seen.contains(key), "promoted key {} never arrived", key);
                }
                ElasticDelta::Evicted { key, votes } => {
                    prop_assert!(*votes < tau, "an eviction carried {}, at or above the threshold", votes);
                    prop_assert!(seen.contains(key), "evicted key {} never arrived", key);
                }
                ElasticDelta::Light(cell) => {
                    prop_assert_eq!(cell.count, tau, "a light promotion carried {}", cell.count);
                }
            }
        }
    }

    #[test]
    fn elastic_workers_flush_to_the_parents_whole_mass(
        buckets in 1usize..8,
        light_rows in 1usize..4,
        light_cols in 1usize..16,
        tau in 1u32..=8,
        shards in 1usize..5,
        keys in stream(300),
    ) {
        let mut workers: Vec<ElasticSketchWorker> = (0..shards)
            .map(|_| ElasticSketchWorker::new(buckets, light_rows, light_cols))
            .collect();
        let mut parent = ElasticOctoAggregator::new(buckets as i32, light_rows, light_cols);
        for (i, k) in keys.iter().enumerate() {
            workers[i % shards].update_emit_delta(&flow(*k), tau, &mut |delta| parent.apply(delta));
        }
        for worker in &mut workers {
            worker.flush(&mut |delta| parent.apply(delta));
        }

        let rows = parent.sketch.light.rows() as i64;
        let (heavy, light) = elastic_mass(&parent.sketch);
        prop_assert_eq!(
            light % rows, 0,
            "a light total of {} is not a whole number of {}-row inserts", light, rows
        );
        prop_assert_eq!(
            heavy + light / rows, keys.len() as i64,
            "heavy {} plus light {} over {} rows != {} inserts", heavy, light, rows, keys.len()
        );
    }

    /// A heavy bucket takes a negative vote per rival arrival and hands the
    /// resident over the moment those reach `LAMBDA` times its positive ones -
    /// the eighth rival against a resident holding a single vote.
    #[test]
    fn elastic_evicts_the_resident_at_the_lambda_boundary(
        resident in 0u64..64,
        rival in 64u64..128,
        light_rows in 1usize..4,
        light_cols in 2usize..16,
    ) {
        let mut worker = ElasticSketchWorker::new(1, light_rows, light_cols);
        worker.update_emit_delta(&flow(resident), OCTO_MAX_PROMOTION_THRESHOLD, &mut |_| {});

        let mut evictions: Vec<(usize, String, u32)> = Vec::new();
        for arrival in 1..=16usize {
            worker.update_emit_delta(&flow(rival), OCTO_MAX_PROMOTION_THRESHOLD, &mut |delta| {
                if let ElasticDelta::Evicted { key, votes } = delta {
                    evictions.push((arrival, key, votes));
                }
            });
        }

        prop_assert_eq!(evictions.len(), 1, "16 rival arrivals evicted {:?}", evictions);
        prop_assert_eq!(
            evictions[0].0, 8,
            "the resident was handed over on rival arrival {}", evictions[0].0
        );
        prop_assert_eq!(&evictions[0].1, &flow(resident));
        prop_assert_eq!(evictions[0].2, 1, "the eviction carried {} votes", evictions[0].2);
    }

    // ===== UnivMon: one Count layer per level, tagged with layer and weight =====

    #[test]
    fn univmon_layer_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..5,
        cols in 1usize..32,
        layer in 0usize..4,
        tau in 1u32..=16,
        keys in stream(400),
    ) {
        let mut worker = CountSketchWithL2NormWorker::new(rows, cols, layer);
        let mut parent = CountSketchWithL2Norm::with_dimensions_and_seed_index(rows, cols, layer);
        for k in &keys {
            worker.update_emit_delta(&DataInput::U64(*k), tau, &mut |d| parent.apply_delta(d));
        }

        let residual = worker.residual().to_vec();
        let reference = l2hh_reference(rows, cols, layer, &keys);
        for (i, (p, r)) in l2hh_cells(&parent).into_iter().zip(l2hh_cells(&reference)).enumerate() {
            prop_assert_eq!(
                p + i64::from(residual[i]), r,
                "cell {}: promoted {} plus residual {} != {}", i, p, residual[i], r
            );
            prop_assert!(
                residual[i].unsigned_abs() < tau as u8,
                "cell {}: residual {} reached the threshold", i, residual[i]
            );
        }

        worker.flush(&mut |d| parent.apply_delta(d));
        prop_assert_eq!(l2hh_cells(&parent), l2hh_cells(&reference));
        prop_assert!(
            worker.residual().iter().all(|c| *c == 0),
            "a flush left a counter held back"
        );
        prop_assert_eq!(
            parent.l2(), reference.l2(),
            "a parent fed only deltas reports a different L2"
        );
    }

    #[test]
    fn univmon_deltas_carry_their_layer_threshold_and_the_workers_running_weight(
        rows in 1usize..4,
        cols in 2usize..16,
        layers in 1usize..5,
        worker_id in 0usize..4,
        tau in 1u32..=16,
        keys in stream(300),
    ) {
        let plan = UnivMonOctoPlan::with_threshold(rows, cols, layers, OctoThreshold::new(tau));
        let mut worker = plan.worker(worker_id);
        let mut promotions: Vec<(usize, UnivMonCellDelta)> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let payload = plan.prepare(&DataInput::U64(*k));
            worker.process(&payload, &mut |d| promotions.push((i, d)));
        }

        for (i, d) in &promotions {
            let layer = d.layer as usize;
            prop_assert!(layer < layers, "layer {} outside {}", layer, layers);
            // Halving is stated here rather than read back from the crate, so a
            // pyramid that stopped halving cannot satisfy both sides at once.
            prop_assert_eq!(
                univmon_layer_threshold(tau, layer), (tau >> layer).max(1),
                "layer {}'s threshold is not tau halved {} times", layer, layer
            );
            prop_assert_eq!(
                d.cell.count.unsigned_abs(),
                (tau >> layer).max(1),
                "layer {} promoted {} rather than its own threshold", layer, d.cell.count
            );
            prop_assert_eq!(d.worker_id as usize, worker_id);
            prop_assert_eq!(
                d.total_weight, *i as u64 + 1,
                "insert {} reported a running weight of {}", i, d.total_weight
            );
            prop_assert_eq!(&d.key, &DataInput::U64(keys[*i]).to_owned_input());
        }
    }

    // ===== Keyed deltas: the counter path, beside the aggregator's only heap =====

    #[test]
    fn cm_keyed_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        top_k in 1usize..8,
        tau in 1u32..=8,
        keys in stream(400),
    ) {
        let plan = CountMinSketchWithHeapOctoPlan::with_threshold(rows, cols, OctoThreshold::new(tau));
        let mut worker = plan.worker(0);
        let mut parent = CountMinSketchWithHeapOctoAggregator::new(rows, cols, top_k);
        let mut promoted: HashSet<u64> = HashSet::new();
        for k in &keys {
            let payload = plan.prepare(&DataInput::U64(*k));
            worker.process(&payload, &mut |d| {
                promoted.insert(*k);
                parent.apply(d);
            });
        }

        let residual = worker.sketch().residual().to_vec();
        let reference = cm_reference(rows, cols, &keys);
        for (i, (p, r)) in cm_cells(parent.sketch.count_min_sketch()).into_iter().zip(cm_cells(&reference)).enumerate() {
            prop_assert_eq!(
                p + i32::from(residual[i]), r,
                "cell {}: promoted {} plus residual {} != {}", i, p, residual[i], r
            );
        }

        let seen: HashSet<u64> = keys.iter().copied().collect();
        prop_assert_eq!(
            parent.sketch.heap().len(), promoted.len().min(top_k),
            "the heap seated {} of the {} keys that promoted, at a capacity of {}",
            parent.sketch.heap().len(), promoted.len(), top_k
        );
        for item in parent.sketch.heap().heap() {
            let key = item.key.as_data_input();
            let estimate = i64::from(parent.sketch.count_min_sketch().estimate(&key));
            prop_assert!(
                matches!(key, DataInput::U64(k) if seen.contains(&k)),
                "the heap holds {:?}, which never arrived", item.key
            );
            prop_assert!(
                item.count <= estimate,
                "the heap holds {} for {:?}, above the parent's own {}", item.count, item.key, estimate
            );
        }
    }

    #[test]
    fn count_keyed_promotions_and_residual_reconstruct_a_single_pass(
        rows in 1usize..6,
        cols in 1usize..64,
        top_k in 1usize..8,
        tau in 1u32..=8,
        keys in stream(400),
    ) {
        let mut worker = CountSketchWorker::new(rows, cols);
        let mut parent = CountSketchWithHeapOctoAggregator::new(rows, cols, top_k);
        let mut deltas: Vec<KeyedCountSketchCellDelta> = Vec::new();
        let mut promoted: HashSet<u64> = HashSet::new();
        for k in &keys {
            let input = DataInput::U64(*k);
            let hashes = CountMinSketchWorker::hashes(rows, &input);
            let key = input.to_owned_input();
            worker.update_emit_keyed_delta_with_row_hashes(&hashes, &key, tau, &mut |d| {
                deltas.push(d.clone());
                promoted.insert(*k);
                parent.apply(d);
            });
        }

        let residual = worker.residual().to_vec();
        let reference = cs_reference(rows, cols, &keys);
        for (i, (p, r)) in cs_cells(parent.sketch.count_sketch()).into_iter().zip(cs_cells(&reference)).enumerate() {
            prop_assert_eq!(
                p + i32::from(residual[i]), r,
                "cell {}: promoted {} plus residual {} != {}", i, p, residual[i], r
            );
        }

        for d in &deltas {
            prop_assert_eq!(d.delta.count.unsigned_abs(), tau, "a promotion carried {}", d.delta.count);
        }
        let seen: HashSet<u64> = keys.iter().copied().collect();
        prop_assert_eq!(
            parent.sketch.heap().len(), promoted.len().min(top_k),
            "the heap seated {} of the {} keys that promoted, at a capacity of {}",
            parent.sketch.heap().len(), promoted.len(), top_k
        );
        for item in parent.sketch.heap().heap() {
            let key = item.key.as_data_input();
            prop_assert!(
                matches!(key, DataInput::U64(k) if seen.contains(&k)),
                "the heap holds {:?}, which never arrived", item.key
            );
        }
    }

    // ===== The default thresholds =====

    #[test]
    fn every_promask_is_the_promotion_step_of_its_own_delta_type(
        seed in any::<u64>(),
        key in 0u64..1024,
        n in 600usize..800,
    ) {
        let keys = vec![key; n];
        for tau in [OCTO_COUNT_MIN_DEFAULT_THRESHOLD, OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, OCTO_DDSKETCH_DEFAULT_THRESHOLD, OCTO_COCO_DEFAULT_THRESHOLD, OCTO_ELASTIC_DEFAULT_THRESHOLD, OCTO_UNIVMON_DEFAULT_THRESHOLD] {
            prop_assert_eq!(
                OctoThreshold::new(tau).get(), tau,
                "{} is outside the 1..={} a worker will run at", tau, OCTO_MAX_PROMOTION_THRESHOLD
            );
        }

        let mut cm = CountMinSketchWorker::new(4, 16);
        let mut cm_deltas: Vec<CountMinSketchCellDelta> = Vec::new();
        for k in &keys {
            cm.update_emit_delta(&DataInput::U64(*k), OCTO_COUNT_MIN_DEFAULT_THRESHOLD, &mut |d| cm_deltas.push(d));
        }
        prop_assert!(!cm_deltas.is_empty(), "OCTO_COUNT_MIN_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for d in &cm_deltas {
            prop_assert_eq!(d.count, OCTO_COUNT_MIN_DEFAULT_THRESHOLD, "a Count-Min promotion carried {}", d.count);
        }

        let mut cs = CountSketchWorker::new(4, 16);
        let mut cs_deltas: Vec<CountSketchCellDelta> = Vec::new();
        for k in &keys {
            cs.update_emit_delta(&DataInput::U64(*k), OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, &mut |d| cs_deltas.push(d));
        }
        prop_assert!(!cs_deltas.is_empty(), "OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for d in &cs_deltas {
            prop_assert_eq!(d.count.unsigned_abs(), OCTO_COUNT_SKETCH_DEFAULT_THRESHOLD, "a CountSketch promotion carried {}", d.count);
        }

        let mut dd = DdSketchWorker::new(0.05);
        let mut dd_deltas: Vec<DdSketchBucketDelta> = Vec::new();
        for k in &keys {
            dd.add_emit_delta(f64::from((*k % 8) as u32), OCTO_DDSKETCH_DEFAULT_THRESHOLD, &mut |d| dd_deltas.push(d));
        }
        prop_assert!(!dd_deltas.is_empty(), "OCTO_DDSKETCH_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for d in &dd_deltas {
            prop_assert_eq!(d.count, u64::from(OCTO_DDSKETCH_DEFAULT_THRESHOLD), "a DDSketch promotion carried {}", d.count);
        }

        let mut coco = CocoSketchWorker::with_seed(2, 8, seed);
        let mut coco_deltas: Vec<CocoDelta> = Vec::new();
        for k in &keys {
            coco.update_emit_delta(&flow(*k), OCTO_COCO_DEFAULT_THRESHOLD, &mut |d| coco_deltas.push(d));
        }
        prop_assert!(!coco_deltas.is_empty(), "OCTO_COCO_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for d in &coco_deltas {
            prop_assert_eq!(d.count, u64::from(OCTO_COCO_DEFAULT_THRESHOLD), "a Coco promotion carried {}", d.count);
        }

        let mut elastic = ElasticSketchWorker::new(8, 2, 8);
        let mut elastic_deltas: Vec<ElasticDelta> = Vec::new();
        for k in &keys {
            elastic.update_emit_delta(&flow(*k), OCTO_ELASTIC_DEFAULT_THRESHOLD, &mut |d| elastic_deltas.push(d));
        }
        let heavy: Vec<u32> = elastic_deltas
            .iter()
            .filter_map(|d| match d {
                ElasticDelta::Heavy { votes, .. } => Some(*votes),
                _ => None,
            })
            .collect();
        prop_assert!(!heavy.is_empty(), "OCTO_ELASTIC_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for value in &heavy {
            prop_assert_eq!(*value, OCTO_ELASTIC_DEFAULT_THRESHOLD, "an Elastic promotion carried {}", value);
        }

        let plan = UnivMonOctoPlan::with_threshold(2, 8, 4, OctoThreshold::new(OCTO_UNIVMON_DEFAULT_THRESHOLD));
        let mut univmon = plan.worker(0);
        let mut layered: Vec<UnivMonCellDelta> = Vec::new();
        for k in &keys {
            let payload = plan.prepare(&DataInput::U64(*k));
            univmon.process(&payload, &mut |d| layered.push(d));
        }
        prop_assert_eq!(univmon_layer_threshold(OCTO_UNIVMON_DEFAULT_THRESHOLD, 0), OCTO_UNIVMON_DEFAULT_THRESHOLD);
        let top_layer: Vec<i32> = layered
            .iter()
            .filter(|d| d.layer == 0)
            .map(|d| d.cell.count)
            .collect();
        prop_assert!(!top_layer.is_empty(), "OCTO_UNIVMON_DEFAULT_THRESHOLD promoted nothing over {} inserts", n);
        for value in &top_layer {
            prop_assert_eq!(value.unsigned_abs(), OCTO_UNIVMON_DEFAULT_THRESHOLD, "a UnivMon promotion carried {}", value);
        }

        let mut hll_worker = Hll::default();
        let mut hll_parent = Hll::default();
        for k in &keys {
            hll_worker.update_emit_delta_with_threshold(&DataInput::U64(*k), OCTO_HLL_DEFAULT_THRESHOLD, &mut |d| {
                hll_parent.apply_delta(d)
            });
        }
        let precision = hll_worker.registers_as_slice().len().ilog2() as u8;
        prop_assert_eq!(
            max_hll_threshold(precision), 64 - precision,
            "the HLL threshold ceiling is not 64 minus the precision"
        );
        prop_assert!(OCTO_HLL_DEFAULT_THRESHOLD < 64 - precision, "OCTO_HLL_DEFAULT_THRESHOLD can never fire");
        prop_assert_eq!(
            hll_parent.registers_as_slice(),
            hll_worker.registers_as_slice(),
            "OCTO_HLL_DEFAULT_THRESHOLD held a register improvement back"
        );
    }

    #[test]
    fn max_promask_is_the_widest_threshold_a_one_byte_counter_survives(
        key in 0u64..1024,
        n in 300usize..500,
    ) {
        prop_assert_eq!(OCTO_MAX_PROMOTION_THRESHOLD, i8::MAX as u32);
        prop_assert_eq!(OctoThreshold::new(0).get(), 1);
        prop_assert_eq!(OctoThreshold::new(OCTO_MAX_PROMOTION_THRESHOLD + 1).get(), OCTO_MAX_PROMOTION_THRESHOLD);
        prop_assert_eq!(OctoThreshold::new(u32::MAX).get(), OCTO_MAX_PROMOTION_THRESHOLD);

        let keys = vec![key; n];
        let mut worker = CountSketchWorker::new(4, 16);
        let mut deltas: Vec<CountSketchCellDelta> = Vec::new();
        for k in &keys {
            worker.update_emit_delta(&DataInput::U64(*k), OCTO_MAX_PROMOTION_THRESHOLD, &mut |d| deltas.push(d));
        }
        prop_assert!(!deltas.is_empty(), "OCTO_MAX_PROMOTION_THRESHOLD promoted nothing over {} inserts", n);
        for d in &deltas {
            prop_assert_eq!(d.count.unsigned_abs(), OCTO_MAX_PROMOTION_THRESHOLD, "a promotion carried {}", d.count);
            prop_assert!(i8::try_from(d.count).is_ok(), "{} does not fit the worker's counter", d.count);
        }
    }
}
