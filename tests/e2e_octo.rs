//! E2E OctoSketch suite: delta-promotion conservation invariants for the
//! CountMin, Count and HyperLogLog child/parent protocol, and the
//! `octo-runtime` multi-threaded pipeline checked against single-threaded
//! replays of the same round-robin partition.
//!
//! The protocol is fully deterministic: `run_octo` dispatches round-robin from
//! the calling thread, and the aggregator only ever applies commutative
//! updates (`+=` for the counter sketches, `max` for HLL registers). Thread
//! interleaving therefore cannot change the parent, so every runtime test here
//! asserts *exact* equality against a replay instead of a statistical band.
//!
//! The second half checks the implementation against the OctoSketch paper
//! itself (NSDI '24, Zhang/Chen/Liu): whether estimates land inside Theorems
//! 1, 3 and 4, and whether delta promotion actually beats the periodic
//! sketch-merge baseline the paper measures against (Theorem 2, Figure 6).
//! The runtime defaults to `OctoPartition::HashByKey`, so a flow lands on one
//! worker and the paper's k' - the number of workers a flow may pass by - is 1.
//! The bound tests sweep `RoundRobin` as well, where k' = k. Note that k' is
//! about a *flow*: a counter is shared by whatever flows hash into it, and each
//! worker may hold back up to tau of its own share, so the provable per-counter
//! gap is k*tau under either partition.

mod common;

use asap_sketchlib::common::BOTTOM_LAYER_FINDER;
use asap_sketchlib::{
    CM_PROMASK, COUNT_PROMASK, Classic, CmDelta, Count, CountDelta, CountMin, DD_PROMASK, DDSketch,
    DataInput, DdWorkerSketch, ErtlMLE, FastPath, HLL_PROMASK, HllDelta, HyperLogLog,
    HyperLogLogP12, HyperLogLogP16, L2HH, L2hhWorkerSketch, LayeredCountDelta, OctoAggregator,
    OctoPlan, OctoThreshold, OctoWorker, RegularPath, UnivMon, UnivMonDeltaFidelity,
    UnivMonOctoAggregator, UnivMonOctoPlan, UnivMonOctoWorker, Vector2D, bottom_layer_for_hash,
    hash64_seeded, hash128_seeded, input_to_owned, univmon_layer_threshold,
};
use common::{FreqTruth, zipf_u64};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

type Cm = CountMin<Vector2D<i32>, RegularPath>;
type CmFast = CountMin<Vector2D<i32>, FastPath>;
type Cs = Count<Vector2D<i32>, RegularPath>;
type CsFast = Count<Vector2D<i32>, FastPath>;

const ROWS: usize = 5;
const COLS: usize = 2048;

/// τ in the paper's notation (NSDI '24, §4.1): the worker promotion threshold.
const TAU: i32 = CM_PROMASK as i32;

/// Largest amount a single cell can lag its exact count: promotion fires at a
/// multiple of τ, so at most `τ - 1` un-promoted increments remain.
const MAX_CELL_RESIDUAL: i32 = TAU - 1;

fn keys(n: usize, domain: usize, seed: u64) -> Vec<u64> {
    zipf_u64(n, domain, 1.1, seed)
}

fn inputs_from(keys: &[u64]) -> Vec<DataInput<'static>> {
    keys.iter().copied().map(DataInput::U64).collect()
}

// ---------------------------------------------------------------------------
// Child-run helpers: drive one child sketch over a stream, capture its deltas
// ---------------------------------------------------------------------------

fn cm_child_run(stream: &[u64], rows: usize, cols: usize) -> (Cm, Vec<CmDelta>) {
    let mut child = Cm::with_dimensions(rows, cols);
    let mut deltas = Vec::new();
    for k in stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| deltas.push(d));
    }
    (child, deltas)
}

fn cs_child_run(stream: &[u64], rows: usize, cols: usize) -> (Cs, Vec<CountDelta>) {
    let mut child = Cs::with_dimensions(rows, cols);
    let mut deltas = Vec::new();
    for k in stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| deltas.push(d));
    }
    (child, deltas)
}

fn hll_child_run(stream: &[u64]) -> (HyperLogLog<Classic>, Vec<HllDelta>) {
    let mut child = HyperLogLog::<Classic>::default();
    let mut deltas = Vec::new();
    for k in stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| deltas.push(d));
    }
    (child, deltas)
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

// ---------------------------------------------------------------------------
// CountMin delta protocol
// ---------------------------------------------------------------------------

#[test]
fn cm_promotion_is_lossless_modulo_the_child_residual() {
    let stream = keys(20_000, 512, 9_101);
    let (child, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }
    let mut reference = Cm::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    // Algorithm 1 clears the counter it promotes, so what the child still
    // holds plus what the parent received must reconstruct a single pass.
    let (child_cells, parent_cells, ref_cells) =
        (cm_cells(&child), cm_cells(&parent), cm_cells(&reference));
    for (i, ((&c, &p), &r)) in child_cells
        .iter()
        .zip(parent_cells.iter())
        .zip(ref_cells.iter())
        .enumerate()
    {
        assert_eq!(p + c, r, "cell {i}: promoted {p} plus residual {c} != {r}");
        assert!(c <= MAX_CELL_RESIDUAL, "cell {i}: residual {c} un-promoted");
    }
}

#[test]
fn cm_deltas_are_well_formed_and_carry_exactly_one_promotion() {
    let stream = keys(20_000, 512, 9_102);
    let (_, deltas) = cm_child_run(&stream, ROWS, COLS);

    assert!(!deltas.is_empty(), "a 20k stream must promote something");
    for d in &deltas {
        assert_eq!(d.value, CM_PROMASK, "CM promotes exactly the mask value");
        assert!((d.row as usize) < ROWS, "row {} out of range", d.row);
        assert!((d.col as usize) < COLS, "col {} out of range", d.col);
    }
}

#[test]
fn cm_parent_holds_every_completed_promotion() {
    let stream = keys(30_000, 512, 9_103);
    let (_, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }

    let mut reference = Cm::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    let mask = CM_PROMASK as i32;
    let (parent_cells, ref_cells) = (cm_cells(&parent), cm_cells(&reference));
    let mut worst_residual = 0;
    for (i, (&p, &r)) in parent_cells.iter().zip(ref_cells.iter()).enumerate() {
        assert_eq!(
            p,
            mask * (r / mask),
            "cell {i}: parent must hold every completed promotion of {r}"
        );
        worst_residual = worst_residual.max(r - p);
    }
    assert!(worst_residual <= MAX_CELL_RESIDUAL);
    assert!(
        worst_residual > 0,
        "a skewed stream should leave some un-promoted remainder"
    );
}

#[test]
fn cm_octo_estimate_trails_the_single_thread_estimate_by_under_one_promotion() {
    let stream = keys(40_000, 256, 9_104);
    let (_, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }
    let mut reference = Cm::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    // Per row the parent lags by `count mod tau`, so the row-wise minimum
    // lags by strictly less than one promotion window and never overshoots.
    for k in 0u64..256 {
        let key = DataInput::U64(k);
        let single = reference.estimate(&key);
        let octo = parent.estimate(&key);
        assert!(
            octo <= single,
            "key {k}: octo {octo} must not exceed single-thread {single}"
        );
        assert!(
            single - octo <= MAX_CELL_RESIDUAL,
            "key {k}: deficit {} exceeds one promotion window",
            single - octo
        );
    }
}

#[test]
fn cm_fast_path_promotes_on_the_same_schedule_as_the_regular_path() {
    let stream = keys(20_000, 512, 9_105);

    let mut fast_child = CmFast::with_dimensions(ROWS, COLS);
    let mut fast_parent = CmFast::with_dimensions(ROWS, COLS);
    let mut fast_deltas = 0usize;
    for k in &stream {
        fast_child.insert_emit_delta(&DataInput::U64(*k), &mut |d| {
            assert_eq!(d.value, CM_PROMASK);
            fast_parent.apply_delta(d);
            fast_deltas += 1;
        });
    }

    assert!(fast_deltas > 0, "a 20k stream must promote something");

    // The fast path derives all columns from one hash, so its collision
    // pattern - and therefore its promotion count - differs from the regular
    // path. Only the per-cell promotion rule is shared.
    let mut reference = CmFast::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }
    for r in 0..ROWS {
        for c in 0..COLS {
            assert_eq!(
                fast_parent.as_storage().query_one_counter(r, c)
                    + fast_child.as_storage().query_one_counter(r, c),
                reference.as_storage().query_one_counter(r, c),
                "cell ({r},{c})"
            );
        }
    }
}

#[test]
fn cm_delta_addressing_survives_columns_past_the_old_u16_ceiling() {
    // Regression: `CmDelta` used to address cells with u16 row/col, so any
    // geometry wider than 65_536 columns silently wrapped deltas onto the
    // wrong cell. The fields are u32, so a wider sketch must stay exact.
    let rows = 3;
    let cols = 100_000;
    let stream = keys(60_000, 4_096, 9_108);

    let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
    let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
    for k in &stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| parent.apply_delta(d));
    }

    let mut reference = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }
    for r in 0..rows {
        for c in 0..cols {
            assert_eq!(
                parent.as_storage().query_one_counter(r, c)
                    + child.as_storage().query_one_counter(r, c),
                reference.as_storage().query_one_counter(r, c),
                "cell ({r},{c}) mis-addressed past the old u16 ceiling"
            );
        }
    }
}

#[test]
fn cm_delta_application_is_order_independent() {
    let stream = keys(15_000, 512, 9_106);
    let (_, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut in_order = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        in_order.apply_delta(*d);
    }

    let mut shuffled = deltas.clone();
    shuffled.shuffle(&mut StdRng::seed_from_u64(9_106));
    let mut out_of_order = Cm::with_dimensions(ROWS, COLS);
    for d in &shuffled {
        out_of_order.apply_delta(*d);
    }

    assert_eq!(
        cm_cells(&in_order),
        cm_cells(&out_of_order),
        "the aggregator applies commutative updates, so delivery order is free"
    );
}

#[test]
fn cm_sharded_children_conserve_counts_against_a_single_pass() {
    let stream = keys(40_000, 512, 9_107);
    let shards = 4;

    let mut children: Vec<Cm> = (0..shards)
        .map(|_| Cm::with_dimensions(ROWS, COLS))
        .collect();
    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for (i, k) in stream.iter().enumerate() {
        let mut out = Vec::new();
        children[i % shards].insert_emit_delta(&DataInput::U64(*k), &mut |d| out.push(d));
        for d in out {
            parent.apply_delta(d);
        }
    }

    let mut reference = Cm::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    // parent + Σ per-shard residuals == the single-pass sketch, cell for cell.
    for r in 0..ROWS {
        for c in 0..COLS {
            let promoted = parent.as_storage().query_one_counter(r, c);
            let residual: i32 = children
                .iter()
                .map(|ch| ch.as_storage().query_one_counter(r, c))
                .sum();
            assert_eq!(
                promoted + residual,
                reference.as_storage().query_one_counter(r, c),
                "cell ({r},{c}) lost or duplicated counts across {shards} shards"
            );
            assert!(
                residual <= shards as i32 * MAX_CELL_RESIDUAL,
                "cell ({r},{c}) residual {residual} exceeds one window per shard"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Count sketch delta protocol
// ---------------------------------------------------------------------------

#[test]
fn count_deltas_carry_exactly_the_signed_threshold() {
    let stream = keys(30_000, 256, 9_201);
    let (_, deltas) = cs_child_run(&stream, ROWS, COLS);

    assert!(!deltas.is_empty(), "a 30k stream must promote something");
    for d in &deltas {
        assert_eq!(
            d.value.unsigned_abs(),
            COUNT_PROMASK,
            "Count resets on promotion, so a delta is always ±the mask"
        );
        assert!((d.row as usize) < ROWS, "row {} out of range", d.row);
        assert!((d.col as usize) < COLS, "col {} out of range", d.col);
    }
}

#[test]
fn count_promotion_is_lossless_modulo_the_child_residual() {
    let stream = keys(30_000, 256, 9_202);
    let (child, deltas) = cs_child_run(&stream, ROWS, COLS);

    let mut parent = Cs::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }

    let mut reference = Cs::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    let (child_cells, parent_cells, ref_cells) =
        (cs_cells(&child), cs_cells(&parent), cs_cells(&reference));
    for (i, ((&c, &p), &r)) in child_cells
        .iter()
        .zip(parent_cells.iter())
        .zip(ref_cells.iter())
        .enumerate()
    {
        assert_eq!(
            p + c,
            r,
            "cell {i}: promoted {p} plus residual {c} must reconstruct {r}"
        );
        assert!(
            c.abs() <= MAX_CELL_RESIDUAL,
            "cell {i}: residual {c} should have been promoted"
        );
    }
}

#[test]
fn count_octo_estimate_stays_within_one_residual_of_the_single_thread_estimate() {
    let stream = keys(40_000, 256, 9_203);
    let (_, deltas) = cs_child_run(&stream, ROWS, COLS);

    let mut parent = Cs::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }
    let mut reference = Cs::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    // Every row cell is within `MAX_CELL_RESIDUAL` of the reference, and the
    // estimate is an order statistic over those rows, so the same bound holds.
    for k in 0u64..256 {
        let key = DataInput::U64(k);
        let gap = (parent.estimate(&key) - reference.estimate(&key)).abs();
        assert!(
            gap <= MAX_CELL_RESIDUAL as f64,
            "key {k}: estimate gap {gap} exceeds one promotion window"
        );
    }
}

#[test]
fn count_fast_path_conserves_counts_like_the_regular_path() {
    let stream = keys(20_000, 256, 9_204);

    let mut child = CsFast::with_dimensions(ROWS, COLS);
    let mut parent = CsFast::with_dimensions(ROWS, COLS);
    for k in &stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| {
            assert_eq!(d.value.unsigned_abs(), COUNT_PROMASK);
            parent.apply_delta(d);
        });
    }

    let mut reference = CsFast::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    for r in 0..ROWS {
        for c in 0..COLS {
            assert_eq!(
                parent.as_storage().query_one_counter(r, c)
                    + child.as_storage().query_one_counter(r, c),
                reference.as_storage().query_one_counter(r, c),
                "cell ({r},{c})"
            );
        }
    }
}

#[test]
fn count_delta_application_is_order_independent() {
    let stream = keys(15_000, 256, 9_205);
    let (_, deltas) = cs_child_run(&stream, ROWS, COLS);

    let mut in_order = Cs::with_dimensions(ROWS, COLS);
    for d in &deltas {
        in_order.apply_delta(*d);
    }
    let mut shuffled = deltas.clone();
    shuffled.shuffle(&mut StdRng::seed_from_u64(9_205));
    let mut out_of_order = Cs::with_dimensions(ROWS, COLS);
    for d in &shuffled {
        out_of_order.apply_delta(*d);
    }

    assert_eq!(cs_cells(&in_order), cs_cells(&out_of_order));
}

#[test]
fn count_sharded_children_conserve_signed_counts() {
    let stream = keys(40_000, 256, 9_206);
    let shards = 3;

    let mut children: Vec<Cs> = (0..shards)
        .map(|_| Cs::with_dimensions(ROWS, COLS))
        .collect();
    let mut parent = Cs::with_dimensions(ROWS, COLS);
    for (i, k) in stream.iter().enumerate() {
        let mut out = Vec::new();
        children[i % shards].insert_emit_delta(&DataInput::U64(*k), &mut |d| out.push(d));
        for d in out {
            parent.apply_delta(d);
        }
    }

    let mut reference = Cs::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    for r in 0..ROWS {
        for c in 0..COLS {
            let residual: i32 = children
                .iter()
                .map(|ch| ch.as_storage().query_one_counter(r, c))
                .sum();
            assert_eq!(
                parent.as_storage().query_one_counter(r, c) + residual,
                reference.as_storage().query_one_counter(r, c),
                "cell ({r},{c})"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// HyperLogLog delta protocol
// ---------------------------------------------------------------------------

#[test]
fn hll_promotion_reproduces_the_single_thread_registers_exactly() {
    let stream: Vec<u64> = (0..60_000u64).collect();
    let (child, deltas) = hll_child_run(&stream);

    let mut parent = HyperLogLog::<Classic>::default();
    for d in &deltas {
        parent.apply_delta(*d);
    }

    let mut reference = HyperLogLog::<Classic>::default();
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    assert_eq!(
        HLL_PROMASK, 0,
        "the exactness argument relies on every improvement being promoted"
    );
    assert_eq!(
        parent.registers_as_slice(),
        reference.registers_as_slice(),
        "max-register promotion is lossless"
    );
    assert_eq!(
        child.registers_as_slice(),
        reference.registers_as_slice(),
        "the emit path must leave the child identical to a plain insert"
    );
    assert_eq!(parent.estimate(), reference.estimate());
}

#[test]
fn hll_promotion_is_exact_for_every_variant_and_precision() {
    let stream: Vec<u64> = (0..40_000u64)
        .map(|i| i.wrapping_mul(2_654_435_761))
        .collect();

    macro_rules! check {
        ($ty:ty, $label:literal) => {{
            let mut child = <$ty>::default();
            let mut parent = <$ty>::default();
            for k in &stream {
                child.insert_emit_delta(&DataInput::U64(*k), &mut |d| parent.apply_delta(d));
            }
            let mut reference = <$ty>::default();
            for k in &stream {
                reference.insert(&DataInput::U64(*k));
            }
            assert_eq!(
                parent.registers_as_slice(),
                reference.registers_as_slice(),
                "{} registers diverged",
                $label
            );
            assert_eq!(
                child.registers_as_slice(),
                reference.registers_as_slice(),
                "{} child perturbed by the emit path",
                $label
            );
        }};
    }

    check!(HyperLogLogP12<Classic>, "P12/Classic");
    check!(HyperLogLog<Classic>, "P14/Classic");
    check!(HyperLogLogP16<Classic>, "P16/Classic");
    check!(HyperLogLogP12<ErtlMLE>, "P12/ErtlMLE");
    check!(HyperLogLog<ErtlMLE>, "P14/ErtlMLE");
    check!(HyperLogLogP16<ErtlMLE>, "P16/ErtlMLE");
}

#[test]
fn hll_deltas_are_strictly_increasing_per_register_and_never_repeat() {
    let stream: Vec<u64> = (0..30_000u64).collect();
    let (child, deltas) = hll_child_run(&stream);

    let register_count = child.registers_as_slice().len();
    let mut best = vec![0u8; register_count];
    for d in &deltas {
        let pos = d.pos as usize;
        assert!(pos < register_count, "register {pos} out of range");
        assert!(
            d.value > best[pos],
            "register {pos} re-promoted a non-improving value {}",
            d.value
        );
        best[pos] = d.value;
    }
    assert_eq!(
        best,
        child.registers_as_slice(),
        "the delta stream must replay the child's final register state"
    );
}

#[test]
fn hll_duplicate_keys_promote_nothing_after_the_first_improvement() {
    let mut child = HyperLogLog::<Classic>::default();
    let mut deltas = 0usize;
    for _ in 0..1_000 {
        child.insert_emit_delta(&DataInput::U64(7), &mut |_| deltas += 1);
    }
    assert_eq!(deltas, 1, "only the first insert can improve a register");
}

#[test]
fn hll_delta_application_is_order_independent() {
    let stream: Vec<u64> = (0..30_000u64).collect();
    let (_, deltas) = hll_child_run(&stream);

    let mut in_order = HyperLogLog::<Classic>::default();
    for d in &deltas {
        in_order.apply_delta(*d);
    }
    let mut shuffled = deltas.clone();
    shuffled.shuffle(&mut StdRng::seed_from_u64(9_301));
    let mut out_of_order = HyperLogLog::<Classic>::default();
    for d in &shuffled {
        out_of_order.apply_delta(*d);
    }

    assert_eq!(
        in_order.registers_as_slice(),
        out_of_order.registers_as_slice(),
        "max is commutative, so delivery order is free"
    );
}

#[test]
fn hll_sharded_children_match_a_single_pass_exactly() {
    let stream: Vec<u64> = (0..50_000u64).collect();
    let shards = 4;

    let mut children: Vec<HyperLogLog<Classic>> =
        (0..shards).map(|_| HyperLogLog::default()).collect();
    let mut parent = HyperLogLog::<Classic>::default();
    for (i, k) in stream.iter().enumerate() {
        let mut out = Vec::new();
        children[i % shards].insert_emit_delta(&DataInput::U64(*k), &mut |d| out.push(d));
        for d in out {
            parent.apply_delta(d);
        }
    }

    let mut reference = HyperLogLog::<Classic>::default();
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    assert_eq!(
        parent.registers_as_slice(),
        reference.registers_as_slice(),
        "sharded HLL promotion carries no partition penalty"
    );
}

#[test]
fn hll_cardinality_survives_the_delta_round_trip_exactly() {
    let truth = 100_000u64;
    let stream: Vec<u64> = (0..truth).collect();
    let (_, deltas) = hll_child_run(&stream);
    let mut parent = HyperLogLog::<Classic>::default();
    for d in &deltas {
        parent.apply_delta(*d);
    }
    let mut reference = HyperLogLog::<Classic>::default();
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    // At HLL_PROMASK = 0 every improvement is promoted, so this is an equality,
    // not a tolerance: a band would pass on an implementation that dropped a
    // slice of the register improvements.
    assert_eq!(
        parent.registers_as_slice(),
        reference.registers_as_slice(),
        "delta-fed registers must match a single pass byte for byte"
    );
    assert_eq!(parent.estimate(), reference.estimate());
    // The sketch's own accuracy, for context: P14 standard error is
    // 1.04/sqrt(2^14) ~ 0.81%.
    let error = (parent.estimate() as f64 - truth as f64).abs() / truth as f64;
    assert!(error < 0.025, "P14 error {error:.4} exceeds 3 sigma");
}

// ---------------------------------------------------------------------------
// Runtime: `run_octo` / `OctoRuntime`
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
mod runtime {
    use super::*;
    use asap_sketchlib::{
        CmOctoAggregator, CmOctoPlan, CmTopKOctoAggregator, CmTopKOctoPlan, CountOctoAggregator,
        CountOctoPlan, DdOctoAggregator, DdOctoPlan, HllOctoAggregator, HllOctoPlan,
        OctoAggregator, OctoConfig, OctoPartition, OctoPlan, OctoRuntime, OctoWorker, run_octo,
    };

    fn config(num_workers: usize) -> OctoConfig {
        OctoConfig {
            num_workers,
            // CI runners have fewer cores than the widest configuration here.
            pin_cores: false,
            queue_capacity: 4096,
            ..OctoConfig::default()
        }
    }

    fn cm_runtime(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cm {
        run_octo(
            inputs,
            &config(workers),
            CmOctoPlan::new(rows, cols),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(rows, cols),
            },
        )
        .parent
        .sketch
    }

    fn cm_replay(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cm {
        let mut replay = OctoCm::new(workers, rows, cols, Route::HashByKey);
        for (i, input) in inputs.iter().enumerate() {
            replay.insert(i, input);
        }
        // `run_octo` flushes when the stream ends, so the replay must too.
        replay.flush();
        replay.parent
    }

    fn cs_replay(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cs {
        let mut replay = OctoCs::new(workers, rows, cols, Route::HashByKey);
        for (i, input) in inputs.iter().enumerate() {
            replay.insert(i, input);
        }
        replay.flush();
        replay.parent
    }

    #[test]
    fn run_octo_cm_matches_a_single_threaded_replay_of_the_same_partition() {
        let inputs = inputs_from(&keys(40_000, 512, 9_401));
        for workers in [1usize, 2, 3, 4, 7] {
            assert_eq!(
                cm_cells(&cm_runtime(&inputs, workers, ROWS, COLS)),
                cm_cells(&cm_replay(&inputs, workers, ROWS, COLS)),
                "{workers} workers: runtime diverged from the deterministic replay"
            );
        }
    }

    #[test]
    fn run_octo_count_matches_a_single_threaded_replay_of_the_same_partition() {
        let inputs = inputs_from(&keys(40_000, 256, 9_402));
        for workers in [1usize, 2, 4] {
            let got = run_octo(
                &inputs,
                &config(workers),
                CountOctoPlan::new(ROWS, COLS),
                || CountOctoAggregator {
                    sketch: Cs::with_dimensions(ROWS, COLS),
                },
            )
            .parent
            .sketch;
            assert_eq!(
                cs_cells(&got),
                cs_cells(&cs_replay(&inputs, workers, ROWS, COLS)),
                "{workers} workers: runtime diverged from the deterministic replay"
            );
        }
    }

    #[test]
    fn run_octo_hll_is_bit_exact_and_worker_count_invariant() {
        let inputs = inputs_from(&(0..80_000u64).collect::<Vec<_>>());
        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }

        for workers in [1usize, 2, 3, 4, 8] {
            let got = run_octo(&inputs, &config(workers), HllOctoPlan::new(), || {
                HllOctoAggregator {
                    sketch: HyperLogLog::<Classic>::default(),
                }
            })
            .parent
            .sketch;
            assert_eq!(
                got.registers_as_slice(),
                reference.registers_as_slice(),
                "{workers} workers: HLL promotion must carry no partition penalty"
            );
        }
    }

    #[test]
    fn run_octo_ddsketch_matches_a_single_threaded_replay() {
        // Bucket deltas are additive, so the parent is a pure function of the
        // partition and thread interleaving cannot change it.
        let alpha = 0.01;
        let values: Vec<f64> = (1..=40_000u64)
            .map(|i| 1.0 + (i as f64 * 7.0) % 999.0)
            .collect();
        let inputs: Vec<DataInput<'_>> = values.iter().map(|v| DataInput::F64(*v)).collect();
        let workers = 4usize;

        let got = run_octo(&inputs, &config(workers), DdOctoPlan::new(alpha), || {
            DdOctoAggregator::new(alpha)
        })
        .parent
        .sketch;

        let mut replay = OctoDd::new(workers, alpha);
        for (i, value) in values.iter().enumerate() {
            replay.insert(i, *value, DD_PROMASK);
        }
        // `run_octo` flushes when the stream ends, so the replay must too.
        let mut children = std::mem::take(&mut replay.children);
        for child in children.iter_mut() {
            child.flush(&mut |d| replay.parent.apply_delta(d));
        }

        // The dense store grows in chunks anchored on whichever bucket is
        // touched first, so its offset and array length depend on delta arrival
        // order. What must match is the logical bucket-to-count mapping.
        let buckets = |sketch: &DDSketch| -> Vec<(i32, u64)> {
            sketch
                .store_counts()
                .iter()
                .enumerate()
                .filter(|(_, count)| **count != 0)
                .map(|(i, count)| (sketch.store_offset() + i as i32, *count))
                .collect()
        };
        assert_eq!(got.get_count(), replay.parent.get_count());
        assert_eq!(buckets(&got), buckets(&replay.parent));
    }

    #[test]
    fn run_octo_is_deterministic_across_repeated_runs() {
        let inputs = inputs_from(&keys(30_000, 512, 9_403));
        let first = cm_runtime(&inputs, 4, ROWS, COLS);
        let second = cm_runtime(&inputs, 4, ROWS, COLS);
        assert_eq!(
            cm_cells(&first),
            cm_cells(&second),
            "commutative aggregation makes the parent independent of interleaving"
        );
    }

    #[test]
    fn streaming_runtime_matches_the_batch_helper() {
        let inputs = inputs_from(&keys(30_000, 512, 9_404));
        let batch = cm_runtime(&inputs, 4, ROWS, COLS);

        let mut runtime = OctoRuntime::new(&config(4), CmOctoPlan::new(ROWS, COLS), || {
            CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            }
        });
        for input in &inputs {
            runtime.insert(input.clone());
        }
        let streamed = runtime.finish().parent.sketch;

        assert_eq!(cm_cells(&batch), cm_cells(&streamed));
    }

    #[test]
    fn insert_batch_matches_element_wise_inserts() {
        let inputs = inputs_from(&keys(20_000, 512, 9_405));

        let mut one_by_one = OctoRuntime::new(&config(3), CmOctoPlan::new(ROWS, COLS), || {
            CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            }
        });
        for input in &inputs {
            one_by_one.insert(input.clone());
        }

        let mut batched = OctoRuntime::new(&config(3), CmOctoPlan::new(ROWS, COLS), || {
            CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            }
        });
        batched.insert_batch(&inputs);

        assert_eq!(
            cm_cells(&one_by_one.finish().parent.sketch),
            cm_cells(&batched.finish().parent.sketch)
        );
    }

    #[test]
    fn degenerate_config_is_clamped_rather_than_rejected() {
        let inputs = inputs_from(&(0..5_000u64).collect::<Vec<_>>());
        let cfg = OctoConfig {
            num_workers: 0,
            pin_cores: false,
            queue_capacity: 0,
            ..OctoConfig::default()
        };
        let got = run_octo(&inputs, &cfg, HllOctoPlan::new(), || HllOctoAggregator {
            sketch: HyperLogLog::<Classic>::default(),
        })
        .parent
        .sketch;

        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }
        assert_eq!(
            got.registers_as_slice(),
            reference.registers_as_slice(),
            "zero workers/capacity must clamp to one, not drop data"
        );
    }

    #[test]
    fn a_one_slot_queue_applies_backpressure_without_deadlocking() {
        let inputs = inputs_from(&(0..20_000u64).collect::<Vec<_>>());
        let cfg = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 1,
            ..OctoConfig::default()
        };
        let got = run_octo(&inputs, &cfg, HllOctoPlan::new(), || HllOctoAggregator {
            sketch: HyperLogLog::<Classic>::default(),
        })
        .parent
        .sketch;

        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }
        assert_eq!(got.registers_as_slice(), reference.registers_as_slice());
    }

    #[test]
    fn a_borrowed_key_may_be_dropped_the_moment_insert_returns() {
        // The point of a borrowed key is that its owner does not have to
        // outlive the sketch. `insert` hashes for partitioning and prepares the
        // payload on this thread, so nothing borrowed crosses to a worker and
        // the string below is free the instant the call returns.
        //
        // This used to be a use-after-free: `insert` transmuted the input to
        // 'static and queued it, so workers hashed freed heap.
        let n = 20_000u64;
        let mut runtime = OctoRuntime::new(&config(4), HllOctoPlan::new(), HllOctoAggregator::new);
        for i in 0..n {
            let owned = format!("session-{i:020}");
            runtime.insert(DataInput::Str(&owned));
            drop(owned);
        }
        let got = runtime.finish().parent.sketch;

        let mut reference = HyperLogLog::<Classic>::default();
        for i in 0..n {
            reference.insert(&DataInput::Str(&format!("session-{i:020}")));
        }
        assert_eq!(
            got.registers_as_slice(),
            reference.registers_as_slice(),
            "a key freed right after insert must still have been hashed correctly"
        );
    }

    #[test]
    fn a_borrowed_key_reaches_a_keyed_aggregator_by_value() {
        // A worker that stores keys copies at preparation, which is the one
        // place a copy is unavoidable - and it happens on this thread, so the
        // borrow still ends at the call.
        let (rows, cols, top_k) = (4usize, 2048usize, 8usize);
        let mut runtime = OctoRuntime::new(&config(4), CmTopKOctoPlan::new(rows, cols), || {
            CmTopKOctoAggregator::new(rows, cols, top_k)
        });
        for _ in 0..4_000 {
            for hot in 0..3usize {
                let owned = format!("hot-{hot}");
                runtime.insert(DataInput::Str(&owned));
                drop(owned);
            }
        }
        let result = runtime.finish();
        for hot in 0..3usize {
            let key = format!("hot-{hot}");
            assert!(
                result
                    .parent
                    .sketch
                    .heap()
                    .find(&DataInput::Str(&key))
                    .is_some(),
                "heavy hitter {key} missing from the aggregator heap"
            );
        }
    }

    #[test]
    fn an_empty_stream_finishes_with_a_pristine_parent() {
        let got = run_octo(&[], &config(4), CmOctoPlan::new(ROWS, COLS), || {
            CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            }
        })
        .parent
        .sketch;
        assert!(cm_cells(&got).iter().all(|&c| c == 0));
    }

    // -- custom worker/aggregator: the public trait surface must be usable
    //    from outside the crate, not just by the bundled adapters.

    struct SumWorker {
        worker_id: usize,
        seen: u64,
    }

    #[derive(Clone, Copy)]
    struct SumDelta {
        worker_id: usize,
        keys_seen: u64,
    }

    impl OctoWorker for SumWorker {
        type Delta = SumDelta;
        type Payload = ();

        fn process<F>(&mut self, _payload: &(), emit: &mut F)
        where
            F: FnMut(Self::Delta),
        {
            self.seen += 1;
            emit(SumDelta {
                worker_id: self.worker_id,
                keys_seen: 1,
            });
        }
    }

    struct SumPlan;

    impl OctoPlan for SumPlan {
        type Worker = SumWorker;

        fn worker(&self, worker_id: usize) -> Self::Worker {
            SumWorker { worker_id, seen: 0 }
        }

        fn prepare(&self, _input: &DataInput<'_>) {}
    }

    struct SumAggregator {
        per_worker: Vec<u64>,
    }

    impl OctoAggregator for SumAggregator {
        type Delta = SumDelta;

        fn apply(&mut self, delta: SumDelta) {
            self.per_worker[delta.worker_id] += delta.keys_seen;
        }
    }

    #[test]
    fn a_user_defined_worker_and_aggregator_round_trip_every_input() {
        let workers = 3;
        let n = 10_001u64;
        let inputs = inputs_from(&(0..n).collect::<Vec<_>>());

        let cfg = OctoConfig {
            partition: OctoPartition::RoundRobin,
            ..config(workers)
        };
        let result = run_octo(&inputs, &cfg, SumPlan, || SumAggregator {
            per_worker: vec![0; workers],
        });

        assert_eq!(
            result.parent.per_worker.iter().sum::<u64>(),
            n,
            "no input may be dropped"
        );
        // Round-robin from a single dispatching thread: worker w takes every
        // index ≡ w (mod workers).
        for (w, &load) in result.parent.per_worker.iter().enumerate() {
            let expected = (0..n as usize).filter(|i| i % workers == w).count() as u64;
            assert_eq!(load, expected, "worker {w} received the wrong share");
        }
    }

    #[test]
    fn the_read_handle_observes_a_monotone_prefix_of_the_final_state() {
        let workers = 2;
        let n = 4_000u64;
        let mut runtime = OctoRuntime::new(&config(workers), SumPlan, || SumAggregator {
            per_worker: vec![0; workers],
        });
        let reader = runtime.read_handle();

        assert_eq!(
            reader.with_parent(|p| p.per_worker.iter().sum::<u64>()),
            0,
            "nothing inserted yet"
        );

        let mut last = 0u64;
        for i in 0..n {
            runtime.insert(DataInput::U64(i));
            if i % 500 == 0 {
                let observed = reader.with_parent(|p| p.per_worker.iter().sum::<u64>());
                assert!(
                    observed >= last,
                    "live total went backwards: {last} -> {observed}"
                );
                assert!(observed <= n, "live total {observed} exceeded the stream");
                last = observed;
            }
        }

        // Monotonicity alone is satisfied by a handle stuck at zero, so wait
        // for it to actually reach the live total while the runtime is still
        // open. That is the property the handle exists for.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let observed = reader.with_parent(|p| p.per_worker.iter().sum::<u64>());
            assert!(observed >= last, "live total went backwards");
            last = observed;
            if observed == n {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "read handle stalled at {observed}/{n}"
            );
            std::hint::spin_loop();
        }

        assert_eq!(runtime.finish().parent.per_worker.iter().sum::<u64>(), n);
    }

    #[test]
    #[should_panic(expected = "Octo runtime has been finished")]
    fn reading_through_a_stale_handle_panics() {
        let workers = 2;
        let runtime = OctoRuntime::new(&config(workers), SumPlan, || SumAggregator {
            per_worker: vec![0; workers],
        });
        let reader = runtime.read_handle();
        let _ = runtime.finish();
        reader.with_parent(|p| p.per_worker.len());
    }

    #[test]
    fn close_is_idempotent_and_preserves_already_queued_work() {
        let workers = 4;
        let n = 3_001u64;
        let mut runtime = OctoRuntime::new(&config(workers), SumPlan, || SumAggregator {
            per_worker: vec![0; workers],
        });
        for i in 0..n {
            runtime.insert(DataInput::U64(i));
        }
        runtime.close();
        runtime.close();

        let result = runtime.finish();
        assert_eq!(
            result.parent.per_worker.iter().sum::<u64>(),
            n,
            "close must drain what was already queued"
        );
    }

    #[test]
    #[should_panic(expected = "cannot insert after runtime has been closed")]
    fn inserting_after_close_panics() {
        let workers = 2;
        let mut runtime = OctoRuntime::new(&config(workers), SumPlan, || SumAggregator {
            per_worker: vec![0; workers],
        });
        runtime.close();
        runtime.insert(DataInput::U64(1));
    }

    // -- accuracy end to end -------------------------------------------------

    #[test]
    fn a_finished_run_carries_only_count_mins_own_one_sided_error() {
        let rows = 5;
        let cols = 4096;
        let stream = keys(200_000, 4_096, 9_501);
        let inputs = inputs_from(&stream);

        let mut truth = FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }

        let parent = cm_runtime(&inputs, 4, rows, cols);

        // `run_octo` flushes when the stream ends, so no promotion residue
        // survives and the protocol contributes nothing: what is left is
        // Count-Min's own one-sided error, never below truth and at most eps*N
        // above it with eps = e/cols. Between flushes the parent would instead
        // sit up to k*tau low - that band is what the theorem tests measure.
        let epsilon_n = (std::f64::consts::E / cols as f64) * truth.total() as f64;
        for k in 0u64..4_096 {
            let exact = truth.get(k as i64) as f64;
            let est = parent.estimate(&DataInput::U64(k)) as f64;
            assert!(
                est >= exact,
                "key {k}: a flushed Count-Min must never underestimate: {est} < {exact}"
            );
            assert!(
                est <= exact + epsilon_n,
                "key {k}: octo estimate {est} exceeded truth {exact} + {epsilon_n:.1}"
            );
        }
    }

    #[test]
    fn run_octo_hll_cardinality_error_stays_within_three_sigma() {
        let truth = 200_000u64;
        let inputs = inputs_from(&(0..truth).collect::<Vec<_>>());
        let got = run_octo(&inputs, &config(4), HllOctoPlan::new(), || {
            HllOctoAggregator {
                sketch: HyperLogLog::<Classic>::default(),
            }
        })
        .parent
        .sketch;

        // Max-register promotion is lossless, so the runtime's parent must be
        // the single-threaded sketch exactly, not merely within 3 sigma of it.
        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }
        assert_eq!(got.registers_as_slice(), reference.registers_as_slice());
        let estimate = got.estimate() as f64;
        let error = (estimate - truth as f64).abs() / truth as f64;
        assert!(
            error < 0.025,
            "octo HLL error {error:.4} exceeds 3σ (estimate {estimate}, truth {truth})"
        );
    }

    #[test]
    fn run_octo_count_frequencies_track_a_zipf_stream() {
        let rows = 5;
        let cols = 4096;
        let stream = keys(200_000, 2_048, 9_502);
        let inputs = inputs_from(&stream);

        let mut truth = FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }

        let parent = run_octo(&inputs, &config(4), CountOctoPlan::new(rows, cols), || {
            CountOctoAggregator {
                sketch: Cs::with_dimensions(rows, cols),
            }
        })
        .parent
        .sketch;

        // A finished run is flushed, so the promotion protocol adds nothing
        // and what is left is the Count-Sketch bound itself: +/- ||f||_2/sqrt(cols) w.h.p.
        let tolerance = 3.0 * truth.l2_norm() / (cols as f64).sqrt();
        let mut violations = Vec::new();
        for (key, exact) in truth.top_k(64) {
            let est = parent.estimate(&DataInput::U64(key as u64));
            if (est - exact as f64).abs() > tolerance {
                violations.push(format!("key {key}: est {est} vs exact {exact}"));
            }
        }
        assert!(
            violations.is_empty(),
            "count-sketch estimates outside ±{tolerance:.1}: {violations:?}"
        );
    }
}

// ===========================================================================
// Paper conformance: OctoSketch (NSDI '24) Theorems 1-4 and its baseline
// ===========================================================================

// ---------------------------------------------------------------------------
// Replays: OctoSketch delta promotion vs. the periodic sketch-merge baseline
// ---------------------------------------------------------------------------

/// How a replay routes an input to a worker, mirroring `OctoPartition`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    /// One flow, one worker: the paper's setting, k' = 1.
    HashByKey,
    /// Every flow reaches every worker: k' = k, the worst case of each bound.
    RoundRobin,
}

impl Route {
    fn worker(self, index: usize, key: &DataInput, workers: usize) -> usize {
        match self {
            Route::RoundRobin => index % workers,
            Route::HashByKey => (hash64_seeded(0, key) % workers as u64) as usize,
        }
    }

    /// k' in the paper's bounds: the number of workers one flow may reach.
    fn k_prime(self, workers: usize) -> usize {
        match self {
            Route::RoundRobin => workers,
            Route::HashByKey => 1,
        }
    }
}

/// OctoSketch: workers promote single counters once they cross τ.
struct OctoCm {
    children: Vec<Cm>,
    parent: Cm,
    route: Route,
    /// Counters shipped to the aggregator, the unit Theorem 2 counts in.
    sent_counters: usize,
}

impl OctoCm {
    fn new(workers: usize, rows: usize, cols: usize, route: Route) -> Self {
        Self {
            children: (0..workers)
                .map(|_| Cm::with_dimensions(rows, cols))
                .collect(),
            parent: Cm::with_dimensions(rows, cols),
            route,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = self.route.worker(index, key, self.children.len());
        let mut promoted = Vec::new();
        self.children[worker].insert_emit_delta(key, &mut |d| promoted.push(d));
        self.sent_counters += promoted.len();
        for d in promoted {
            self.parent.apply_delta(d);
        }
    }

    /// Hands over every residual counter, mirroring what the runtime does when
    /// a stream is finished or handed over for querying. The bound and
    /// conservation tests deliberately do not call this: the residue is what
    /// they are measuring.
    fn flush(&mut self) {
        let (rows, cols) = (self.parent.rows(), self.parent.cols());
        for child in self.children.iter_mut() {
            for row in 0..rows {
                for col in 0..cols {
                    let held = child.as_storage().query_one_counter(row, col);
                    if held != 0 {
                        self.parent.apply_delta(CmDelta {
                            row: row as u32,
                            col: col as u32,
                            value: held as u32,
                        });
                        child
                            .as_storage_mut()
                            .update_one_counter(row, col, |c, _| *c = 0, ());
                        self.sent_counters += 1;
                    }
                }
            }
        }
    }
}

/// Sketch-merge: each worker keeps a full sketch and ships all `rows*cols`
/// counters every `period` items it sees, then restarts from zero.
struct MergeCm {
    children: Vec<Cm>,
    seen: Vec<usize>,
    parent: Cm,
    period: usize,
    rows: usize,
    cols: usize,
    route: Route,
    sent_counters: usize,
}

impl MergeCm {
    fn new(workers: usize, rows: usize, cols: usize, period: usize, route: Route) -> Self {
        Self {
            children: (0..workers)
                .map(|_| Cm::with_dimensions(rows, cols))
                .collect(),
            seen: vec![0; workers],
            parent: Cm::with_dimensions(rows, cols),
            period,
            rows,
            cols,
            route,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = self.route.worker(index, key, self.children.len());
        self.children[worker].insert(key);
        self.seen[worker] += 1;
        if self.seen[worker] % self.period == 0 {
            self.parent.merge(&self.children[worker]);
            self.children[worker] = Cm::with_dimensions(self.rows, self.cols);
            self.sent_counters += self.rows * self.cols;
        }
    }
}

/// Count-sketch flavour of `OctoCm`.
struct OctoCs {
    children: Vec<Cs>,
    parent: Cs,
    route: Route,
    sent_counters: usize,
}

impl OctoCs {
    fn new(workers: usize, rows: usize, cols: usize, route: Route) -> Self {
        Self {
            children: (0..workers)
                .map(|_| Cs::with_dimensions(rows, cols))
                .collect(),
            parent: Cs::with_dimensions(rows, cols),
            route,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = self.route.worker(index, key, self.children.len());
        let mut promoted = Vec::new();
        self.children[worker].insert_emit_delta(key, &mut |d| promoted.push(d));
        self.sent_counters += promoted.len();
        for d in promoted {
            self.parent.apply_delta(d);
        }
    }

    /// See `OctoCm::flush`.
    fn flush(&mut self) {
        let (rows, cols) = (self.parent.rows(), self.parent.cols());
        for child in self.children.iter_mut() {
            for row in 0..rows {
                for col in 0..cols {
                    let held = child.as_storage().query_one_counter(row, col);
                    if held != 0 {
                        self.parent.apply_delta(CountDelta {
                            row: row as u32,
                            col: col as u32,
                            value: held,
                        });
                        child
                            .as_storage_mut()
                            .update_one_counter(row, col, |c, _| *c = 0, ());
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Theorem 1: Count-Min error bound
// ---------------------------------------------------------------------------

#[test]
fn theorem_1_bounds_the_count_min_error() {
    // "let d = log2 δ⁻¹ and l = 2ε⁻¹. For any flow e and any traffic whose
    //  L1 > ε⁻¹k'τ, Pr[|f̂(e) − f(e)| > εL1] < δ"
    let d = 5usize;
    let l = 4096usize;
    let workers = 4usize;
    let epsilon = 2.0 / l as f64;
    let delta = 2f64.powi(-(d as i32));

    let stream = zipf_u64(400_000, 8_192, 1.1, 11_001);
    let mut truth = FreqTruth::default();
    for key in &stream {
        truth.observe(*key as i64);
    }
    let mut measured: Vec<(Route, f64)> = Vec::new();
    for route in [Route::HashByKey, Route::RoundRobin] {
        let k_prime_tau = (route.k_prime(workers) as i32 * TAU) as f64;
        // A counter is shared by every flow that hashes into it, and each worker
        // may hold back up to tau of its own share, so the provable per-counter
        // gap is k*tau whatever the partition. The paper's k'*tau bounds only the
        // queried flow's own held-back count.
        let counter_ceiling = (workers as i32 * TAU) as f64;
        let mut octo = OctoCm::new(workers, d, l, route);
        for (i, key) in stream.iter().enumerate() {
            octo.insert(i, &DataInput::U64(*key));
        }

        let l1 = truth.total() as f64;
        assert!(
            l1 > k_prime_tau / epsilon,
            "theorem precondition L1 > ε⁻¹k'τ unmet: L1 = {l1}, ε⁻¹k'τ = {}",
            k_prime_tau / epsilon
        );

        let band = epsilon * l1;
        let mut violations = 0usize;
        let mut worst_deficit = 0.0f64;
        let mut worst_excess = 0.0f64;
        let pairs = truth.pairs();
        for (key, exact) in &pairs {
            let est = octo.parent.estimate(&DataInput::U64(*key as u64)) as f64;
            let exact = *exact as f64;
            if (est - exact).abs() > band {
                violations += 1;
            }
            worst_deficit = worst_deficit.max(exact - est);
            worst_excess = worst_excess.max(est - exact);
        }

        // The proof's deterministic half: f̂ ≤ f̂' and |f̂' − f̂| < k'τ, and Count-Min
        // never underestimates, so f(e) − k'τ < f̂(e) for *every* key, not merely
        // with probability 1 − δ.
        assert!(
            worst_deficit < counter_ceiling,
            "worst underestimate {worst_deficit} reached the k*tau = {counter_ceiling} ceiling"
        );
        measured.push((route, worst_deficit));

        let rate = violations as f64 / pairs.len() as f64;
        println!(
            "Theorem 1 [{route:?}]: eps={epsilon:.2e} delta={delta:.4} eps*L1={band:.1} \
         k'tau={k_prime_tau} violations={rate:.5} worst_deficit={worst_deficit} \
         worst_excess={worst_excess}"
        );
        assert!(
            rate < delta,
            "{route:?}: violation rate {rate:.5} exceeded δ = {delta:.5} over {} keys",
            pairs.len()
        );
    }

    // Hash partitioning keeps a flow's whole count on one worker, so it should
    // never trail further behind than round-robin does.
    let hashed = measured[0].1;
    let round_robin = measured[1].1;
    assert!(
        hashed <= round_robin,
        "hash partitioning lagged more than round-robin: {hashed} vs {round_robin}"
    );
}

// ---------------------------------------------------------------------------
// Theorem 3: Count sketch error bound
// ---------------------------------------------------------------------------

#[test]
fn theorem_3_bounds_the_count_sketch_error() {
    // "let d = O(log2 δ⁻¹) and l = 8ε⁻². For any flow e and any traffic whose
    //  L2 > 2ε⁻¹k'τ, Pr[|f̂(e) − f(e)| > εL2] < δ"
    assert_eq!(
        COUNT_PROMASK, CM_PROMASK,
        "both sketches share one τ in this implementation"
    );
    let d = 5usize;
    let l = 4096usize;
    let workers = 4usize;
    let epsilon = (8.0 / l as f64).sqrt();
    let delta = 2f64.powi(-(d as i32));

    let stream = zipf_u64(400_000, 8_192, 1.1, 11_003);
    let mut truth = FreqTruth::default();
    let mut single_core = Cs::with_dimensions(d, l);
    for key in &stream {
        truth.observe(*key as i64);
        single_core.insert(&DataInput::U64(*key));
    }
    let mut measured: Vec<(Route, f64)> = Vec::new();
    for route in [Route::HashByKey, Route::RoundRobin] {
        let k_prime_tau = (route.k_prime(workers) as i32 * TAU) as f64;
        // See Theorem 1: the per-counter gap ceiling is k*tau, not k'*tau.
        let counter_ceiling = (workers as i32 * TAU) as f64;
        let mut octo = OctoCs::new(workers, d, l, route);
        for (i, key) in stream.iter().enumerate() {
            octo.insert(i, &DataInput::U64(*key));
        }

        let l2 = truth.l2_norm();
        assert!(
            l2 > 2.0 * k_prime_tau / epsilon,
            "theorem precondition L2 > 2ε⁻¹k'τ unmet: L2 = {l2}, 2ε⁻¹k'τ = {}",
            2.0 * k_prime_tau / epsilon
        );

        let band = epsilon * l2;
        let mut violations = 0usize;
        let mut worst_octo_gap = 0.0f64;
        let pairs = truth.pairs();
        for (key, exact) in &pairs {
            let input = DataInput::U64(*key as u64);
            let est = octo.parent.estimate(&input);
            if (est - *exact as f64).abs() > band {
                violations += 1;
            }
            // The proof's deterministic half: |f̂'(e) − f̂(e)| < k'τ.
            worst_octo_gap = worst_octo_gap.max((single_core.estimate(&input) - est).abs());
        }

        assert!(
            worst_octo_gap < counter_ceiling,
            "gap to the single-core sketch {worst_octo_gap} reached the k*tau = {counter_ceiling} ceiling"
        );
        measured.push((route, worst_octo_gap));

        let rate = violations as f64 / pairs.len() as f64;
        println!(
            "Theorem 3 [{route:?}]: eps={epsilon:.4} delta={delta:.4} eps*L2={band:.1} \
         k'tau={k_prime_tau} violations={rate:.5} worst_gap_to_single_core={worst_octo_gap}"
        );
        assert!(
            rate < delta,
            "{route:?}: violation rate {rate:.5} exceeded δ = {delta:.5} over {} keys",
            pairs.len()
        );
    }

    let hashed = measured[0].1;
    let round_robin = measured[1].1;
    assert!(
        hashed <= round_robin,
        "hash partitioning lagged more than round-robin: {hashed} vs {round_robin}"
    );
}

// ---------------------------------------------------------------------------
// Theorem 4: HyperLogLog exactness
// ---------------------------------------------------------------------------

#[test]
fn theorem_4_holds_unconditionally_because_this_implementation_pins_tau_to_zero() {
    // Theorem 4 only promises Ẑ = Ẑ' once Ẑ > 2·α_m·m²·2^(τ−2), because a
    // worker holds back register improvements below τ−1. This implementation
    // sets HLL_PROMASK = 0, so C'[i] = C[i] for every i and the equality holds
    // at any cardinality - including ones orders of magnitude below the
    // precondition the paper would otherwise need.
    assert_eq!(HLL_PROMASK, 0);

    let m = HyperLogLog::<Classic>::default().registers_as_slice().len() as f64;
    let alpha_m = 0.7213 / (1.0 + 1.079 / m);
    // The precondition evaluated at the smallest threshold the paper suggests
    // for HLL (Appendix B: "the threshold τ = 2 is often enough").
    let precondition = 2.0 * alpha_m * m * m * 2f64.powi(2 - 2);

    for n in [10u64, 100, 1_000, 10_000] {
        let mut child = HyperLogLog::<Classic>::default();
        let mut parent = HyperLogLog::<Classic>::default();
        for i in 0..n {
            child.insert_emit_delta(&DataInput::U64(i), &mut |d| parent.apply_delta(d));
        }
        let mut ideal = HyperLogLog::<Classic>::default();
        for i in 0..n {
            ideal.insert(&DataInput::U64(i));
        }

        assert!(
            (n as f64) < precondition,
            "n = {n} is above the paper's precondition {precondition:.3e}, \
             so this case would not demonstrate the unconditional guarantee"
        );
        assert_eq!(
            parent.registers_as_slice(),
            ideal.registers_as_slice(),
            "n = {n}: Ẑ' must equal Ẑ even far below the τ = 2 precondition"
        );
        assert_eq!(parent.estimate(), ideal.estimate(), "n = {n}");
    }
    println!(
        "Theorem 4: tau=0 gives Z' = Z unconditionally; the tau=2 precondition \
         would only bind above Z > {precondition:.3e}"
    );
}

// ---------------------------------------------------------------------------
// Theorem 2: communication cost against sketch-merge
// ---------------------------------------------------------------------------

#[test]
fn theorem_2_delta_promotion_sends_l_times_fewer_counters_for_the_same_goal() {
    // "To achieve the accuracy goal, sketch-merge needs to send O(Δ⁻¹·k·N·d·l)
    //  counters, while OctoSketch needs to send O(Δ⁻¹·k·N·d) counters."
    //
    // The proof holds both sides to the same accuracy goal Δ: sketch-merge
    // ships the whole sketch every Δ/k items per worker, and OctoSketch sets
    // its threshold to τ = Δ/k. This implementation pins τ = 31, so the goal
    // it is solving for is Δ = k·τ.
    let d = 3usize;
    let l = 1_024usize;
    let workers = 4usize;
    // A counter is shared across workers whatever the partition, so the goal a
    // shipped tau solves for is k*tau, and sketch-merge must match it by
    // shipping every tau items per worker.
    let accuracy_goal = (workers as i32 * TAU) as f64;
    let merge_period = TAU as usize;

    let stream = zipf_u64(40_000, 2_048, 1.1, 11_005);
    let mut octo = OctoCm::new(workers, d, l, Route::HashByKey);
    let mut merge = MergeCm::new(workers, d, l, merge_period, Route::HashByKey);
    let mut single_core = Cm::with_dimensions(d, l);
    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        octo.insert(i, &input);
        merge.insert(i, &input);
        single_core.insert(&input);
    }

    // Both configurations must actually meet the goal before their costs are
    // comparable - that is the premise of the theorem, not a side check.
    let mut worst_octo = 0.0f64;
    let mut worst_merge = 0.0f64;
    for key in 0u64..2_048 {
        let input = DataInput::U64(key);
        let ideal = single_core.estimate(&input) as f64;
        worst_octo = worst_octo.max(ideal - octo.parent.estimate(&input) as f64);
        worst_merge = worst_merge.max(ideal - merge.parent.estimate(&input) as f64);
    }
    assert!(
        worst_octo <= accuracy_goal,
        "octo missed the accuracy goal: {worst_octo} > {accuracy_goal}"
    );
    assert!(
        worst_merge <= accuracy_goal,
        "sketch-merge missed the accuracy goal: {worst_merge} > {accuracy_goal}"
    );

    let ratio = merge.sent_counters as f64 / octo.sent_counters as f64;
    println!(
        "Theorem 2: goal={accuracy_goal} octo_counters={} merge_counters={} ratio={ratio:.0}x (l={l})",
        octo.sent_counters, merge.sent_counters
    );
    assert!(
        ratio > l as f64 / 4.0,
        "expected roughly an l = {l} fold saving, measured {ratio:.0}x"
    );

    // Counting bytes rather than counters keeps the claim honest: an Octo
    // message carries indices as well as a value, a merged counter does not.
    let octo_bytes = octo.sent_counters * std::mem::size_of::<CmDelta>();
    let merge_bytes = merge.sent_counters * std::mem::size_of::<i32>();
    let byte_ratio = merge_bytes as f64 / octo_bytes as f64;
    println!("Theorem 2 (bytes): octo={octo_bytes} merge={merge_bytes} ratio={byte_ratio:.0}x");
    assert!(
        byte_ratio > l as f64 / 16.0,
        "byte-level saving {byte_ratio:.0}x is smaller than expected"
    );
}

// ---------------------------------------------------------------------------
// Figure 6 / Table 1: online accuracy against sketch-merge
// ---------------------------------------------------------------------------

#[test]
fn delta_promotion_beats_sketch_merge_on_online_accuracy() {
    // The paper's headline claim is *online* accuracy: the error a query sees
    // while the stream is still running. Sketch-merge is stale by up to one
    // merge period; delta promotion is stale by at most k'τ whatever the
    // arrival rate. Sweeping the baseline's communication budget from parity
    // up to 10x Octo's shows the gap is a property of the mechanism, not of a
    // budget chosen to flatter it.
    let d = 3usize;
    let l = 1_024usize;
    let workers = 4usize;
    let n = 60_000usize;
    let checkpoints = 8usize;
    let budget_multiples = [1usize, 10];

    let stream = zipf_u64(n, 4_096, 1.1, 11_007);
    let mut final_truth = FreqTruth::default();
    for key in &stream {
        final_truth.observe(*key as i64);
    }
    let watched: Vec<i64> = final_truth.top_k(32).into_iter().map(|(k, _)| k).collect();

    // Sizing pass: what does delta promotion spend over this stream?
    let mut probe = OctoCm::new(workers, d, l, Route::HashByKey);
    for (i, key) in stream.iter().enumerate() {
        probe.insert(i, &DataInput::U64(*key));
    }
    let mut merges: Vec<MergeCm> = budget_multiples
        .iter()
        .map(|mult| {
            let merges_per_worker = ((mult * probe.sent_counters) as f64 / (d * l * workers) as f64)
                .ceil()
                .max(1.0) as usize;
            // Divide by one more than the merge count so boundaries land
            // mid-stream: a period equal to a worker's whole share would make
            // it a coin flip whether that worker ever merges at all.
            let period = ((n / workers) / (merges_per_worker + 1)).max(1);
            MergeCm::new(workers, d, l, period, Route::HashByKey)
        })
        .collect();

    let mut truth = FreqTruth::default();
    let mut octo = OctoCm::new(workers, d, l, Route::HashByKey);
    let mut single_core = Cm::with_dimensions(d, l);
    let mut octo_error = 0.0f64;
    let mut ideal_error = 0.0f64;
    let mut merge_error = vec![0.0f64; merges.len()];
    let mut samples = 0usize;
    let stride = n / checkpoints;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        truth.observe(*key as i64);
        octo.insert(i, &input);
        single_core.insert(&input);
        for merge in merges.iter_mut() {
            merge.insert(i, &input);
        }

        // Query at points that deliberately do not line up with a merge
        // boundary: an online query arrives whenever it arrives.
        if i > 0 && i % stride == stride / 3 {
            for watched_key in &watched {
                let probe_input = DataInput::U64(*watched_key as u64);
                let exact = truth.get(*watched_key) as f64;
                octo_error += (octo.parent.estimate(&probe_input) as f64 - exact).abs();
                ideal_error += (single_core.estimate(&probe_input) as f64 - exact).abs();
                for (slot, merge) in merge_error.iter_mut().zip(merges.iter()) {
                    *slot += (merge.parent.estimate(&probe_input) as f64 - exact).abs();
                }
                samples += 1;
            }
        }
    }

    let octo_mae = octo_error / samples as f64;
    let ideal_mae = ideal_error / samples as f64;
    println!("Online accuracy over {samples} queries: ideal={ideal_mae:.1} octo={octo_mae:.1}");
    for ((mult, merge), error) in budget_multiples
        .iter()
        .zip(merges.iter())
        .zip(merge_error.iter())
    {
        let merge_mae = error / samples as f64;
        let spend = merge.sent_counters as f64 / octo.sent_counters as f64;
        println!(
            "  merge at {mult}x budget (period={}, actual spend {spend:.1}x): \
             mae={merge_mae:.1} = {:.1}x octo",
            merge.period,
            merge_mae / octo_mae
        );
        assert!(
            merge.sent_counters > octo.sent_counters,
            "the baseline must not be starved of budget for this comparison to mean anything"
        );
        // Empirical and deliberately loose: measured 14.0x at parity and 5.4x
        // when the baseline is handed ten times the budget.
        assert!(
            merge_mae > octo_mae * 2.0,
            "delta promotion should stay well ahead at {mult}x budget: \
             octo={octo_mae:.1} vs merge={merge_mae:.1}"
        );
    }

    let parity_mae = merge_error[0] / samples as f64;
    assert!(
        parity_mae > octo_mae * 8.0,
        "at equal communication budget the gap should be large: \
         octo={octo_mae:.1} vs merge={parity_mae:.1}"
    );
    assert!(
        octo_mae <= ideal_mae + TAU as f64,
        "octo online error {octo_mae:.1} drifted more than tau from ideal {ideal_mae:.1}"
    );
}

#[test]
fn sketch_merge_staleness_grows_with_the_merge_period_while_promotion_does_not() {
    // The mechanism behind the previous test, isolated: sketch-merge error is
    // proportional to its period, delta promotion error is capped at k'τ no
    // matter how much traffic arrives between queries. The deficit is averaged
    // over many query points - sampling only at the end of the stream would
    // land on a merge boundary whenever the period divides the per-worker
    // item count, and report a staleness of zero.
    let d = 3usize;
    let l = 1_024usize;
    let workers = 4usize;
    let n = 40_000usize;
    let query_points = 40usize;
    let stream = zipf_u64(n, 2_048, 1.1, 11_009);

    let mut whole_stream_truth = FreqTruth::default();
    for key in &stream {
        whole_stream_truth.observe(*key as i64);
    }
    let hottest = whole_stream_truth.top_k(1)[0].0;
    let probe = DataInput::U64(hottest as u64);

    let periods = [500usize, 1_000, 2_000, 4_000];
    let mut truth = FreqTruth::default();
    let mut octo = OctoCm::new(workers, d, l, Route::HashByKey);
    let mut merges: Vec<MergeCm> = periods
        .iter()
        .map(|p| MergeCm::new(workers, d, l, *p, Route::HashByKey))
        .collect();
    let mut octo_deficit = 0.0f64;
    let mut merge_deficit = vec![0.0f64; periods.len()];
    let mut samples = 0usize;
    let stride = n / query_points;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        truth.observe(*key as i64);
        octo.insert(i, &input);
        for merge in merges.iter_mut() {
            merge.insert(i, &input);
        }
        if i > 0 && i % stride == stride / 3 {
            let exact = truth.get(hottest) as f64;
            octo_deficit += exact - octo.parent.estimate(&probe) as f64;
            for (slot, merge) in merge_deficit.iter_mut().zip(merges.iter()) {
                *slot += exact - merge.parent.estimate(&probe) as f64;
            }
            samples += 1;
        }
    }

    let octo_mean = octo_deficit / samples as f64;
    let merge_means: Vec<f64> = merge_deficit.iter().map(|d| d / samples as f64).collect();
    println!(
        "hottest key: octo mean deficit={octo_mean:.1}, merge by period {periods:?} = {:?}",
        merge_means
            .iter()
            .map(|m| format!("{m:.1}"))
            .collect::<Vec<_>>()
    );

    assert!(
        octo_mean <= TAU as f64,
        "octo mean deficit {octo_mean:.1} exceeded tau"
    );
    for (window, pair) in merge_means.windows(2).zip(periods.windows(2)) {
        assert!(
            window[1] > window[0],
            "doubling the merge period from {} to {} should increase staleness: \
             {:.1} -> {:.1}",
            pair[0],
            pair[1],
            window[0],
            window[1]
        );
    }
    assert!(
        merge_means[periods.len() - 1] > octo_mean * 3.0,
        "expected the widest merge period to be far staler than delta promotion"
    );
}

// ===========================================================================
// Online accuracy per sketch: delta promotion vs periodic sketch-merge
// ===========================================================================
//
// Correctness is settled by the conservation identities above. What these
// measure is the paper's actual claim: the error a query sees *while the
// stream is still running*. Every comparison hands sketch-merge at least as
// much communication as delta promotion spent, then queries both at points
// that do not line up with a merge boundary.

/// One row of the comparison: mean error at query time under each scheme.
struct OnlineComparison {
    ideal: f64,
    octo: f64,
    merge_at_parity: f64,
    merge_at_ten_x: f64,
    octo_counters: usize,
    parity_counters: usize,
    ten_x_counters: usize,
}

impl OnlineComparison {
    fn report(&self, label: &str) {
        println!(
            "{label:<10} ideal={:.4} octo={:.4} merge@{:.1}x={:.4} merge@{:.1}x={:.4} \
             (octo sent {} counters)",
            self.ideal,
            self.octo,
            self.parity_counters as f64 / self.octo_counters.max(1) as f64,
            self.merge_at_parity,
            self.ten_x_counters as f64 / self.octo_counters.max(1) as f64,
            self.merge_at_ten_x,
            self.octo_counters,
        );
    }

    /// Harness self-check, not evidence: `merge_periods` already rounds the
    /// baseline's spend up past its budget. This catches a sizing bug that
    /// would starve the baseline and make the comparison meaningless; the
    /// printed spend multiples are what say how well it was funded.
    fn assert_baselines_were_funded(&self) {
        assert!(
            self.parity_counters >= self.octo_counters,
            "parity baseline was starved: {} < {}",
            self.parity_counters,
            self.octo_counters
        );
        assert!(self.ten_x_counters > self.parity_counters);
    }
}

/// Merge periods that spend roughly `1x` and `10x` the given counter budget.
fn merge_periods(
    octo_counters: usize,
    sketch_counters: usize,
    per_worker: usize,
) -> (usize, usize) {
    let period_for = |budget: usize| {
        let merges = (budget as f64 / sketch_counters as f64).ceil().max(1.0) as usize;
        (per_worker / (merges + 1)).max(1)
    };
    (period_for(octo_counters), period_for(10 * octo_counters))
}

// --------------------------------------------------------------- Count sketch

struct MergeCs {
    children: Vec<Cs>,
    seen: Vec<usize>,
    parent: Cs,
    period: usize,
    rows: usize,
    cols: usize,
    sent_counters: usize,
}

impl MergeCs {
    fn new(workers: usize, rows: usize, cols: usize, period: usize) -> Self {
        Self {
            children: (0..workers)
                .map(|_| Cs::with_dimensions(rows, cols))
                .collect(),
            seen: vec![0; workers],
            parent: Cs::with_dimensions(rows, cols),
            period,
            rows,
            cols,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = Route::HashByKey.worker(index, key, self.children.len());
        self.children[worker].insert(key);
        self.seen[worker] += 1;
        if self.seen[worker] % self.period == 0 {
            self.parent.merge(&self.children[worker]);
            self.children[worker] = Cs::with_dimensions(self.rows, self.cols);
            self.sent_counters += self.rows * self.cols;
        }
    }
}

#[test]
fn count_sketch_online_accuracy_beats_sketch_merge() {
    let (d, l, workers, n) = (5usize, 1_024usize, 4usize, 60_000usize);
    let stream = zipf_u64(n, 4_096, 1.1, 12_001);
    let mut truth = FreqTruth::default();
    for key in &stream {
        truth.observe(*key as i64);
    }
    let watched: Vec<i64> = truth.top_k(32).into_iter().map(|(k, _)| k).collect();

    // Sizing pass: what does delta promotion spend over this stream?
    let mut probe = OctoCs::new(workers, d, l, Route::HashByKey);
    for (i, key) in stream.iter().enumerate() {
        probe.insert(i, &DataInput::U64(*key));
    }
    let octo_counters = probe.sent_counters;
    let (parity_period, ten_x_period) = merge_periods(octo_counters, d * l, n / workers);

    let mut running = FreqTruth::default();
    let mut octo = OctoCs::new(workers, d, l, Route::HashByKey);
    let mut ideal = Cs::with_dimensions(d, l);
    let mut parity = MergeCs::new(workers, d, l, parity_period);
    let mut ten_x = MergeCs::new(workers, d, l, ten_x_period);
    let (mut e_ideal, mut e_octo, mut e_parity, mut e_ten) = (0.0, 0.0, 0.0, 0.0);
    let mut samples = 0usize;
    let stride = n / 8;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        running.observe(*key as i64);
        octo.insert(i, &input);
        ideal.insert(&input);
        parity.insert(i, &input);
        ten_x.insert(i, &input);
        if i > 0 && i % stride == stride / 3 {
            for probe_key in &watched {
                let probe_input = DataInput::U64(*probe_key as u64);
                let exact = running.get(*probe_key) as f64;
                e_ideal += (ideal.estimate(&probe_input) - exact).abs();
                e_octo += (octo.parent.estimate(&probe_input) - exact).abs();
                e_parity += (parity.parent.estimate(&probe_input) - exact).abs();
                e_ten += (ten_x.parent.estimate(&probe_input) - exact).abs();
                samples += 1;
            }
        }
    }

    let scale = samples as f64;
    let comparison = OnlineComparison {
        ideal: e_ideal / scale,
        octo: e_octo / scale,
        merge_at_parity: e_parity / scale,
        merge_at_ten_x: e_ten / scale,
        octo_counters,
        parity_counters: parity.sent_counters,
        ten_x_counters: ten_x.sent_counters,
    };
    comparison.report("Count");
    comparison.assert_baselines_were_funded();
    assert!(comparison.octo < comparison.merge_at_parity / 2.0);
    assert!(comparison.octo < comparison.merge_at_ten_x);
}

// ---------------------------------------------------------------- HyperLogLog

struct OctoHll {
    children: Vec<HyperLogLog<Classic>>,
    parent: HyperLogLog<Classic>,
    sent_counters: usize,
}

impl OctoHll {
    fn new(workers: usize) -> Self {
        Self {
            children: (0..workers).map(|_| HyperLogLog::default()).collect(),
            parent: HyperLogLog::default(),
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = Route::HashByKey.worker(index, key, self.children.len());
        let mut promoted = Vec::new();
        self.children[worker].insert_emit_delta(key, &mut |d| promoted.push(d));
        self.sent_counters += promoted.len();
        for d in promoted {
            self.parent.apply_delta(d);
        }
    }
}

struct MergeHll {
    children: Vec<HyperLogLog<Classic>>,
    seen: Vec<usize>,
    parent: HyperLogLog<Classic>,
    period: usize,
    sent_counters: usize,
}

impl MergeHll {
    fn new(workers: usize, period: usize) -> Self {
        Self {
            children: (0..workers).map(|_| HyperLogLog::default()).collect(),
            seen: vec![0; workers],
            parent: HyperLogLog::default(),
            period,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = Route::HashByKey.worker(index, key, self.children.len());
        self.children[worker].insert(key);
        self.seen[worker] += 1;
        if self.seen[worker] % self.period == 0 {
            // A max-merge does not clear the worker: its registers stay valid.
            self.parent.merge(&self.children[worker]);
            self.sent_counters += self.children[worker].registers_as_slice().len();
        }
    }
}

#[test]
fn hyperloglog_online_accuracy_beats_sketch_merge() {
    let (workers, n) = (4usize, 200_000usize);
    let stream: Vec<u64> = (0..n as u64).collect();
    let registers = HyperLogLog::<Classic>::default().registers_as_slice().len();

    let mut probe = OctoHll::new(workers);
    for (i, key) in stream.iter().enumerate() {
        probe.insert(i, &DataInput::U64(*key));
    }
    let octo_counters = probe.sent_counters;
    let (parity_period, ten_x_period) = merge_periods(octo_counters, registers, n / workers);

    let mut octo = OctoHll::new(workers);
    let mut ideal = HyperLogLog::<Classic>::default();
    let mut parity = MergeHll::new(workers, parity_period);
    let mut ten_x = MergeHll::new(workers, ten_x_period);
    let (mut e_ideal, mut e_octo, mut e_parity, mut e_ten) = (0.0, 0.0, 0.0, 0.0);
    let mut samples = 0usize;
    let stride = n / 8;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        octo.insert(i, &input);
        ideal.insert(&input);
        parity.insert(i, &input);
        ten_x.insert(i, &input);
        if i > 0 && i % stride == stride / 3 {
            // Every key so far is distinct, so the exact cardinality is i + 1.
            let exact = (i + 1) as f64;
            let rel = |estimate: usize| (estimate as f64 - exact).abs() / exact;
            e_ideal += rel(ideal.estimate());
            e_octo += rel(octo.parent.estimate());
            e_parity += rel(parity.parent.estimate());
            e_ten += rel(ten_x.parent.estimate());
            samples += 1;
        }
    }

    let scale = samples as f64;
    let comparison = OnlineComparison {
        ideal: e_ideal / scale,
        octo: e_octo / scale,
        merge_at_parity: e_parity / scale,
        merge_at_ten_x: e_ten / scale,
        octo_counters,
        parity_counters: parity.sent_counters,
        ten_x_counters: ten_x.sent_counters,
    };
    comparison.report("HLL");
    comparison.assert_baselines_were_funded();
    // Max-register promotion loses nothing, so the parent is the ideal sketch
    // at every query point - not merely close to it.
    assert_eq!(
        comparison.octo, comparison.ideal,
        "delta promotion must leave HLL exactly ideal online"
    );
    assert!(comparison.octo < comparison.merge_at_parity);
    assert!(comparison.octo < comparison.merge_at_ten_x);
}

// -------------------------------------------------------------------- DDSketch

struct OctoDd {
    children: Vec<DdWorkerSketch>,
    parent: DDSketch,
    sent_counters: usize,
}

impl OctoDd {
    fn new(workers: usize, alpha: f64) -> Self {
        Self {
            children: (0..workers).map(|_| DdWorkerSketch::new(alpha)).collect(),
            parent: DDSketch::new(alpha),
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, value: f64, threshold: u32) {
        let key = DataInput::F64(value);
        let worker = Route::HashByKey.worker(index, &key, self.children.len());
        let mut promoted = Vec::new();
        self.children[worker].add_emit_delta(value, threshold, &mut |d| promoted.push(d));
        self.sent_counters += promoted.len();
        for d in promoted {
            self.parent.apply_delta(d);
        }
    }
}

struct MergeDd {
    children: Vec<DDSketch>,
    seen: Vec<usize>,
    parent: DDSketch,
    period: usize,
    alpha: f64,
    sent_counters: usize,
}

impl MergeDd {
    fn new(workers: usize, alpha: f64, period: usize) -> Self {
        Self {
            children: (0..workers).map(|_| DDSketch::new(alpha)).collect(),
            seen: vec![0; workers],
            parent: DDSketch::new(alpha),
            period,
            alpha,
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, value: f64) {
        let worker = Route::HashByKey.worker(index, &DataInput::F64(value), self.children.len());
        self.children[worker].add(&value);
        self.seen[worker] += 1;
        if self.seen[worker] % self.period == 0 {
            self.sent_counters += self.children[worker].store_counts().len();
            self.parent
                .merge(&self.children[worker])
                .expect("same alpha");
            self.children[worker] = DDSketch::new(self.alpha);
        }
    }
}

/// Runs the DDSketch comparison over one value stream at one threshold.
fn ddsketch_online_comparison(
    label: &str,
    values: &[f64],
    alpha: f64,
    threshold: u32,
) -> OnlineComparison {
    let workers = 4usize;
    let n = values.len();

    let mut sizing = DDSketch::new(alpha);
    for v in values {
        sizing.add(v);
    }
    let sketch_counters = sizing.store_counts().len();

    let mut probe = OctoDd::new(workers, alpha);
    for (i, v) in values.iter().enumerate() {
        probe.insert(i, *v, threshold);
    }
    let octo_counters = probe.sent_counters;
    let (parity_period, ten_x_period) = merge_periods(octo_counters, sketch_counters, n / workers);

    let mut octo = OctoDd::new(workers, alpha);
    let mut ideal = DDSketch::new(alpha);
    let mut parity = MergeDd::new(workers, alpha, parity_period);
    let mut ten_x = MergeDd::new(workers, alpha, ten_x_period);
    let mut seen: Vec<f64> = Vec::with_capacity(n);
    let (mut e_ideal, mut e_octo, mut e_parity, mut e_ten) = (0.0, 0.0, 0.0, 0.0);
    let mut samples = 0usize;
    let stride = n / 8;

    for (i, v) in values.iter().enumerate() {
        octo.insert(i, *v, threshold);
        ideal.add(v);
        parity.insert(i, *v);
        ten_x.insert(i, *v);
        seen.push(*v);
        if i > 0 && i % stride == stride / 3 {
            let mut sorted = seen.clone();
            sorted.sort_by(f64::total_cmp);
            for q in [0.1, 0.5, 0.9] {
                let exact = sorted[((sorted.len() - 1) as f64 * q) as usize];
                let rel = |sketch: &DDSketch| {
                    sketch
                        .get_value_at_quantile(q)
                        .map(|got| (got - exact).abs() / exact)
                        .unwrap_or(1.0)
                };
                e_ideal += rel(&ideal);
                e_octo += rel(&octo.parent);
                e_parity += rel(&parity.parent);
                e_ten += rel(&ten_x.parent);
                samples += 1;
            }
        }
    }

    let scale = samples as f64;
    let comparison = OnlineComparison {
        ideal: e_ideal / scale,
        octo: e_octo / scale,
        merge_at_parity: e_parity / scale,
        merge_at_ten_x: e_ten / scale,
        octo_counters,
        parity_counters: parity.sent_counters,
        ten_x_counters: ten_x.sent_counters,
    };
    comparison.report(label);
    comparison
}

#[test]
fn ddsketch_delta_promotion_trades_quantile_accuracy_for_messages() {
    // DDSketch is the one integrated sketch where delta promotion does not pay
    // off, and this pins the reason. A Count-Min counter that lags is still
    // counted, just low; a DDSketch bucket that never reaches the threshold is
    // absent from the parent entirely, and a quantile is exactly a statement
    // about where the mass sits. Sparse buckets are also the common case: a
    // logarithmic histogram over a skewed stream has many of them.
    let (n, alpha) = (200_000usize, 0.01f64);
    let values = common::exponential_f64(n, 0.05, 12_101)
        .into_iter()
        .map(|v| v.max(1e-3))
        .collect::<Vec<f64>>();

    let mut sweep: Vec<(u32, f64, usize)> = Vec::new();
    for threshold in [1u32, 2, 4, 8] {
        let result =
            ddsketch_online_comparison(&format!("DDS/tau={threshold}"), &values, alpha, threshold);
        sweep.push((threshold, result.octo, result.octo_counters));
    }

    // A threshold of 1 promotes every sample, so the parent is the ideal
    // sketch - at the cost of one message per sample.
    let (_, exact_error, exact_counters) = sweep[0];
    assert_eq!(exact_counters, n, "tau=1 must promote every sample");
    let ideal_error = ddsketch_online_comparison("DDS/ideal-ref", &values, alpha, 1).ideal;
    assert!(
        (exact_error - ideal_error).abs() < 1e-12,
        "tau=1 must leave the parent exactly ideal: {exact_error} vs {ideal_error}"
    );

    // Above that, every step up buys fewer messages and costs accuracy.
    for pair in sweep.windows(2) {
        let (lo_tau, lo_error, lo_counters) = pair[0];
        let (hi_tau, hi_error, hi_counters) = pair[1];
        assert!(
            hi_counters < lo_counters,
            "tau {hi_tau} should send fewer counters than {lo_tau}: {hi_counters} vs {lo_counters}"
        );
        assert!(
            hi_error >= lo_error,
            "tau {hi_tau} should not be more accurate than {lo_tau}: {hi_error} vs {lo_error}"
        );
    }
}

// --------------------------------------------------------------------- UnivMon

struct OctoUniv {
    plan: UnivMonOctoPlan,
    workers: Vec<UnivMonOctoWorker>,
    aggregator: UnivMonOctoAggregator,
    sent_counters: usize,
}

impl OctoUniv {
    fn new(workers: usize, heap: usize, rows: usize, cols: usize, layers: usize, tau: u32) -> Self {
        let threshold = OctoThreshold::new(tau);
        let plan = UnivMonOctoPlan::with_threshold(rows, cols, layers, threshold.clone());
        Self {
            workers: (0..workers).map(|id| plan.worker(id)).collect(),
            plan,
            aggregator: UnivMonOctoAggregator::with_threshold(heap, rows, cols, layers, threshold),
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = Route::HashByKey.worker(index, key, self.workers.len());
        let payload = self.plan.prepare(key);
        let mut promoted = Vec::new();
        self.workers[worker].process(&payload, &mut |d| promoted.push(d));
        self.sent_counters += promoted.len();
        for delta in promoted {
            self.aggregator.apply(delta);
        }
    }
}

struct MergeUniv {
    children: Vec<UnivMon>,
    seen: Vec<usize>,
    parent: UnivMon,
    period: usize,
    dims: (usize, usize, usize, usize),
    sent_counters: usize,
}

impl MergeUniv {
    fn new(
        workers: usize,
        heap: usize,
        rows: usize,
        cols: usize,
        layers: usize,
        period: usize,
    ) -> Self {
        Self {
            children: (0..workers)
                .map(|_| UnivMon::init_univmon(heap, rows, cols, layers))
                .collect(),
            seen: vec![0; workers],
            parent: UnivMon::init_univmon(heap, rows, cols, layers),
            period,
            dims: (heap, rows, cols, layers),
            sent_counters: 0,
        }
    }

    fn insert(&mut self, index: usize, key: &DataInput) {
        let worker = Route::HashByKey.worker(index, key, self.children.len());
        self.children[worker].insert(key, 1);
        self.seen[worker] += 1;
        if self.seen[worker] % self.period == 0 {
            let (heap, rows, cols, layers) = self.dims;
            self.parent.merge(&self.children[worker]);
            self.children[worker] = UnivMon::init_univmon(heap, rows, cols, layers);
            self.sent_counters += rows * cols * layers;
        }
    }
}

#[test]
fn univmon_online_accuracy_beats_sketch_merge() {
    // UnivMon's recursive estimator needs about log2(distinct) sampling layers
    // before the deepest sampled substream fits the heap. This stream carries
    // ~3.5k distinct keys, so 12 layers; too few and the sketch's own error
    // swamps anything a distribution scheme does to it.
    let (workers, heap, rows, cols, layers) = (4usize, 64usize, 5usize, 1_024usize, 12usize);
    let n = 60_000usize;
    let tau = COUNT_PROMASK;
    let stream = zipf_u64(n, 4_096, 1.1, 12_201);

    let mut probe = OctoUniv::new(workers, heap, rows, cols, layers, tau);
    for (i, key) in stream.iter().enumerate() {
        probe.insert(i, &DataInput::U64(*key));
    }
    let octo_counters = probe.sent_counters;
    let (parity_period, ten_x_period) =
        merge_periods(octo_counters, rows * cols * layers, n / workers);

    let mut truth = FreqTruth::default();
    let mut octo = OctoUniv::new(workers, heap, rows, cols, layers, tau);
    let mut ideal = UnivMon::init_univmon(heap, rows, cols, layers);
    let mut parity = MergeUniv::new(workers, heap, rows, cols, layers, parity_period);
    let mut ten_x = MergeUniv::new(workers, heap, rows, cols, layers, ten_x_period);

    let (mut e_ideal, mut e_octo, mut e_parity, mut e_ten) = (0.0, 0.0, 0.0, 0.0);
    let mut samples = 0usize;
    let stride = n / 8;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        truth.observe(*key as i64);
        octo.insert(i, &input);
        ideal.insert(&input, 1);
        parity.insert(i, &input);
        ten_x.insert(i, &input);
        if i > 0 && i % stride == stride / 3 {
            // UnivMon's headline queries are g-sums over the whole stream:
            // distinct-count and Shannon entropy. Both are compared against the
            // single-core sketch rather than against exact truth, which is the
            // paper's Definition 1: what is being measured here is the gap a
            // distributed scheme opens up, not UnivMon's own approximation
            // error, which at these dimensions dwarfs it.
            let (ideal_card, ideal_entropy) = (ideal.calc_card(), ideal.calc_entropy());
            let gap_to_ideal = |sketch: &UnivMon| {
                let card = (sketch.calc_card() - ideal_card).abs() / ideal_card.abs().max(1.0);
                let entropy =
                    (sketch.calc_entropy() - ideal_entropy).abs() / ideal_entropy.abs().max(1e-9);
                (card + entropy) / 2.0
            };
            let exact_card = truth.distinct() as f64;
            let exact_entropy = truth.entropy(true);
            e_ideal += ((ideal_card - exact_card).abs() / exact_card
                + (ideal_entropy - exact_entropy).abs() / exact_entropy.abs())
                / 2.0;
            e_octo += gap_to_ideal(&octo.aggregator.sketch);
            e_parity += gap_to_ideal(&parity.parent);
            e_ten += gap_to_ideal(&ten_x.parent);
            samples += 1;
        }
    }

    let scale = samples as f64;
    let comparison = OnlineComparison {
        ideal: e_ideal / scale,
        octo: e_octo / scale,
        merge_at_parity: e_parity / scale,
        merge_at_ten_x: e_ten / scale,
        octo_counters,
        parity_counters: parity.sent_counters,
        ten_x_counters: ten_x.sent_counters,
    };
    // `ideal` here is UnivMon's own error against exact truth, for context;
    // the other three are gaps to that ideal, per Definition 1.
    comparison.report("UnivMon");
    comparison.assert_baselines_were_funded();
    assert!(
        comparison.octo < comparison.merge_at_parity / 2.0,
        "delta promotion should track the single-core answer at least twice as closely \
         at comparable budget: octo={:.4} merge={:.4}",
        comparison.octo,
        comparison.merge_at_parity
    );
    // Sketch-merge does close the gap - it just has to buy its way there. On
    // this workload it needs an order of magnitude more traffic before it
    // matches delta promotion, which is Theorem 2 showing up as a measurement.
    assert!(
        comparison.merge_at_ten_x < comparison.merge_at_parity,
        "more budget must help the baseline"
    );
    assert!(
        comparison.ten_x_counters as f64 / comparison.octo_counters as f64 > 10.0,
        "the catch-up budget should be an order of magnitude larger"
    );
}

#[test]
fn univmon_heavy_hitter_recall_beats_sketch_merge() {
    let (workers, heap, rows, cols, layers) = (4usize, 64usize, 5usize, 1_024usize, 12usize);
    let n = 60_000usize;
    let tau = COUNT_PROMASK;
    let stream = zipf_u64(n, 4_096, 1.1, 12_203);

    let mut truth = FreqTruth::default();
    for key in &stream {
        truth.observe(*key as i64);
    }
    let hottest: Vec<i64> = truth.top_k(16).into_iter().map(|(k, _)| k).collect();

    let mut probe = OctoUniv::new(workers, heap, rows, cols, layers, tau);
    for (i, key) in stream.iter().enumerate() {
        probe.insert(i, &DataInput::U64(*key));
    }
    let (parity_period, _) = merge_periods(probe.sent_counters, rows * cols * layers, n / workers);

    let mut octo = OctoUniv::new(workers, heap, rows, cols, layers, tau);
    let mut parity = MergeUniv::new(workers, heap, rows, cols, layers, parity_period);
    let (mut octo_recall, mut merge_recall) = (0.0, 0.0);
    let mut samples = 0usize;
    let stride = n / 8;

    for (i, key) in stream.iter().enumerate() {
        let input = DataInput::U64(*key);
        octo.insert(i, &input);
        parity.insert(i, &input);
        if i > 0 && i % stride == stride / 3 {
            let found = |sketch: &UnivMon| {
                hottest
                    .iter()
                    .filter(|k| {
                        sketch.hh_layers[0]
                            .find(&DataInput::U64(**k as u64))
                            .is_some()
                    })
                    .count() as f64
                    / hottest.len() as f64
            };
            octo_recall += found(&octo.aggregator.sketch);
            merge_recall += found(&parity.parent);
            samples += 1;
        }
    }

    let (octo_recall, merge_recall) = (octo_recall / samples as f64, merge_recall / samples as f64);
    println!(
        "UnivMon top-16 recall at layer 0: octo={octo_recall:.3} merge={merge_recall:.3} \
         (merge spent {}x octo's counters)",
        parity.sent_counters / probe.sent_counters.max(1)
    );
    assert!(
        octo_recall >= merge_recall,
        "the aggregator's heap should not trail sketch-merge: {octo_recall:.3} vs {merge_recall:.3}"
    );
    assert!(
        octo_recall > 0.9,
        "heavy hitters should be found nearly always, got {octo_recall:.3}"
    );
}

// ---------------------------------------------------------------------------
// Why UnivMon scales its threshold per layer
// ---------------------------------------------------------------------------

/// Drives a UnivMon pyramid through the delta path with a caller-chosen
/// per-layer threshold, so a flat threshold can be compared against the scaled
/// one the shipped worker uses.
fn univmon_through_deltas(
    stream: &[u64],
    heap: usize,
    rows: usize,
    cols: usize,
    layers: usize,
    layer_threshold: impl Fn(usize) -> u32,
) -> UnivMon {
    let mut workers: Vec<L2hhWorkerSketch> = (0..layers)
        .map(|layer| L2hhWorkerSketch::new(rows, cols, layer))
        .collect();
    let mut parent = UnivMon::init_univmon(heap, rows, cols, layers);
    let mut weight = 0u64;

    for key in stream {
        let input = DataInput::U64(*key);
        weight += 1;
        let bottom = bottom_layer_for_hash(hash64_seeded(BOTTOM_LAYER_FINDER, &input), layers);
        let owned = input_to_owned(&input);
        for (layer, worker) in workers.iter_mut().enumerate().take(bottom + 1) {
            let hashed = hash128_seeded(layer, &input);
            let mut promoted = Vec::new();
            worker
                .insert_hash_emit_delta(hashed, layer_threshold(layer), &mut |d| promoted.push(d));
            for delta in promoted {
                parent.apply_layered_delta(
                    &LayeredCountDelta {
                        layer: layer as u32,
                        key: owned.clone(),
                        delta,
                        worker_id: 0,
                        weight_total: weight,
                    },
                    if layer_threshold(layer) <= 1 {
                        UnivMonDeltaFidelity::EveryInsert
                    } else {
                        UnivMonDeltaFidelity::PromotedOnly
                    },
                );
            }
        }
        parent.set_total_weight(weight as usize);
    }
    parent
}

#[test]
fn a_flat_threshold_starves_the_deep_univmon_layers() {
    // A UnivMon layer only receives the keys that survive L coin flips, so
    // layer L carries about n / 2^L of the stream. One threshold across the
    // whole pyramid is sized for layer 0 and leaves the deep layers - the ones
    // the recursive estimator leans on for cardinality - with nothing. This is
    // the measurement `univmon_layer_threshold` exists for.
    let (heap, rows, cols, layers) = (64usize, 5usize, 1_024usize, 12usize);
    let base = 31u32;
    let stream = zipf_u64(60_000, 4_096, 1.1, 12_201);

    let mut ideal = UnivMon::init_univmon(heap, rows, cols, layers);
    for key in &stream {
        ideal.insert(&DataInput::U64(*key), 1);
    }

    let flat = univmon_through_deltas(&stream, heap, rows, cols, layers, |_| base);
    let scaled = univmon_through_deltas(&stream, heap, rows, cols, layers, |layer| {
        univmon_layer_threshold(base, layer)
    });

    let nonzero = |sketch: &UnivMon, layer: usize| -> usize {
        let L2HH::COUNT(inner) = &sketch.l2_sketch_layers[layer];
        (0..inner.rows())
            .flat_map(|r| (0..inner.cols()).map(move |c| (r, c)))
            .filter(|(r, c)| inner.as_storage().query_one_counter(*r, *c) != 0)
            .count()
    };
    let deepest = layers - 1;
    println!(
        "UnivMon card: ideal={:.0} flat-tau={:.0} scaled-tau={:.0} | deepest layer non-zero cells: \
         ideal={} flat={} scaled={}",
        ideal.calc_card(),
        flat.calc_card(),
        scaled.calc_card(),
        nonzero(&ideal, deepest),
        nonzero(&flat, deepest),
        nonzero(&scaled, deepest),
    );

    // The deep layers are what a flat threshold empties.
    assert!(
        nonzero(&ideal, deepest) > 0,
        "the ideal sketch reaches this layer"
    );
    assert_eq!(
        nonzero(&flat, deepest),
        0,
        "a flat threshold leaves the deepest layer empty"
    );
    assert_eq!(
        nonzero(&scaled, deepest),
        nonzero(&ideal, deepest),
        "scaling the threshold per layer keeps the deep layers exact"
    );

    // And that is what wrecks the estimate the deep layers feed.
    let relative = |sketch: &UnivMon| {
        (sketch.calc_card() - ideal.calc_card()).abs() / ideal.calc_card().abs().max(1.0)
    };
    assert!(
        relative(&flat) > 0.5,
        "a flat threshold should lose most of the cardinality, got {:.3}",
        relative(&flat)
    );
    assert!(
        relative(&scaled) < 0.1,
        "the scaled threshold should track it, got {:.3}",
        relative(&scaled)
    );
}
