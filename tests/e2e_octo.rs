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

mod common;

use asap_sketchlib::{
    CM_PROMASK, COUNT_PROMASK, Classic, CmDelta, Count, CountDelta, CountMin, DataInput, ErtlMLE,
    FastPath, HLL_PROMASK, HllDelta, HyperLogLog, HyperLogLogP12, HyperLogLogP16, RegularPath,
    Vector2D,
};
use common::zipf_u64;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

type Cm = CountMin<Vector2D<i32>, RegularPath>;
type CmFast = CountMin<Vector2D<i32>, FastPath>;
type Cs = Count<Vector2D<i32>, RegularPath>;
type CsFast = Count<Vector2D<i32>, FastPath>;

const ROWS: usize = 5;
const COLS: usize = 2048;

/// Largest amount a single cell can lag its exact count: promotion fires at a
/// multiple of the mask, so at most `mask - 1` un-promoted increments remain.
/// This is τ in the OctoSketch paper's notation (NSDI '24, §4.1).
const MAX_CELL_RESIDUAL: i32 = CM_PROMASK as i32 - 1;

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
fn cm_emit_path_leaves_the_child_identical_to_a_plain_insert() {
    let stream = keys(20_000, 512, 9_101);
    let (child, _) = cm_child_run(&stream, ROWS, COLS);

    let mut reference = Cm::with_dimensions(ROWS, COLS);
    for k in &stream {
        reference.insert(&DataInput::U64(*k));
    }

    assert_eq!(
        cm_cells(&child),
        cm_cells(&reference),
        "emitting deltas must not perturb the child's own counters"
    );
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
fn cm_parent_holds_the_promoted_multiple_of_every_child_cell() {
    let stream = keys(30_000, 512, 9_103);
    let (child, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }

    let (child_cells, parent_cells) = (cm_cells(&child), cm_cells(&parent));
    let mask = CM_PROMASK as i32;
    let mut worst_residual = 0;
    for (i, (&c, &p)) in child_cells.iter().zip(parent_cells.iter()).enumerate() {
        assert_eq!(
            p,
            mask * (c / mask),
            "cell {i}: parent must hold every completed promotion of child count {c}"
        );
        worst_residual = worst_residual.max(c - p);
    }
    assert!(
        worst_residual <= MAX_CELL_RESIDUAL,
        "residual {worst_residual} exceeds one promotion window"
    );
    assert!(
        worst_residual > 0,
        "a skewed stream should leave some un-promoted remainder"
    );
}

#[test]
fn cm_octo_estimate_trails_the_single_thread_estimate_by_under_one_promotion() {
    let stream = keys(40_000, 256, 9_104);
    let (child, deltas) = cm_child_run(&stream, ROWS, COLS);

    let mut parent = Cm::with_dimensions(ROWS, COLS);
    for d in &deltas {
        parent.apply_delta(*d);
    }

    // Per row the parent lags by `count mod mask`, so the row-wise minimum
    // lags by strictly less than one promotion window and never overshoots.
    for k in 0u64..256 {
        let key = DataInput::U64(k);
        let single = child.estimate(&key);
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
    let mask = CM_PROMASK as i32;
    for r in 0..ROWS {
        for c in 0..COLS {
            let child_cell = fast_child.as_storage().query_one_counter(r, c);
            let parent_cell = fast_parent.as_storage().query_one_counter(r, c);
            assert_eq!(parent_cell, mask * (child_cell / mask), "cell ({r},{c})");
        }
    }
}

#[test]
fn cm_delta_addressing_holds_at_the_widest_supported_geometry() {
    // `CmDelta` addresses cells with u16 row/col, so 65_536 columns is the
    // widest geometry the promotion protocol can carry (max index 65_535).
    let rows = 3;
    let cols = u16::MAX as usize + 1;
    let stream = keys(60_000, 4_096, 9_108);

    let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
    let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
    for k in &stream {
        child.insert_emit_delta(&DataInput::U64(*k), &mut |d| parent.apply_delta(d));
    }

    let mask = CM_PROMASK as i32;
    for r in 0..rows {
        for c in 0..cols {
            let child_cell = child.as_storage().query_one_counter(r, c);
            assert_eq!(
                parent.as_storage().query_one_counter(r, c),
                mask * (child_cell / mask),
                "cell ({r},{c}) mis-addressed at the u16 column boundary"
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
            let mask = CM_PROMASK as i32;
            let residual: i32 = children
                .iter()
                .map(|ch| ch.as_storage().query_one_counter(r, c) % mask)
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
fn hll_cardinality_error_survives_the_delta_round_trip() {
    let truth = 100_000u64;
    let stream: Vec<u64> = (0..truth).collect();
    let (_, deltas) = hll_child_run(&stream);
    let mut parent = HyperLogLog::<Classic>::default();
    for d in &deltas {
        parent.apply_delta(*d);
    }

    // P14 standard error is 1.04/sqrt(2^14) ≈ 0.81%; allow 3σ.
    let estimate = parent.estimate() as f64;
    let error = (estimate - truth as f64).abs() / truth as f64;
    assert!(
        error < 0.025,
        "delta-fed HLL error {error:.4} exceeds 3σ (estimate {estimate}, truth {truth})"
    );
}

// ---------------------------------------------------------------------------
// Runtime: `run_octo` / `OctoRuntime`
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
mod runtime {
    use super::*;
    use asap_sketchlib::{
        CmOctoAggregator, CmOctoWorker, CountOctoAggregator, CountOctoWorker, HllOctoAggregator,
        HllOctoWorker, OctoAggregator, OctoConfig, OctoRuntime, OctoWorker, run_octo,
    };

    fn config(num_workers: usize) -> OctoConfig {
        OctoConfig {
            num_workers,
            // CI runners have fewer cores than the widest configuration here.
            pin_cores: false,
            queue_capacity: 4096,
        }
    }

    fn cm_runtime(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cm {
        run_octo(
            inputs,
            &config(workers),
            |_| CmOctoWorker::new(rows, cols),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(rows, cols),
            },
        )
        .parent
        .sketch
    }

    /// Single-threaded replay of the runtime's round-robin dispatch.
    fn cm_replay(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cm {
        let mut children: Vec<Cm> = (0..workers)
            .map(|_| Cm::with_dimensions(rows, cols))
            .collect();
        let mut parent = Cm::with_dimensions(rows, cols);
        for (i, input) in inputs.iter().enumerate() {
            let mut out = Vec::new();
            children[i % workers].insert_emit_delta(input, &mut |d| out.push(d));
            for d in out {
                parent.apply_delta(d);
            }
        }
        parent
    }

    fn cs_replay(inputs: &[DataInput<'_>], workers: usize, rows: usize, cols: usize) -> Cs {
        let mut children: Vec<Cs> = (0..workers)
            .map(|_| Cs::with_dimensions(rows, cols))
            .collect();
        let mut parent = Cs::with_dimensions(rows, cols);
        for (i, input) in inputs.iter().enumerate() {
            let mut out = Vec::new();
            children[i % workers].insert_emit_delta(input, &mut |d| out.push(d));
            for d in out {
                parent.apply_delta(d);
            }
        }
        parent
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
                |_| CountOctoWorker::new(ROWS, COLS),
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
            let got = run_octo(
                &inputs,
                &config(workers),
                |_| HllOctoWorker::new(),
                || HllOctoAggregator {
                    sketch: HyperLogLog::<Classic>::default(),
                },
            )
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

        let mut runtime = OctoRuntime::new(
            &config(4),
            |_| CmOctoWorker::new(ROWS, COLS),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            },
        );
        for input in &inputs {
            runtime.insert(input.clone());
        }
        let streamed = runtime.finish().parent.sketch;

        assert_eq!(cm_cells(&batch), cm_cells(&streamed));
    }

    #[test]
    fn insert_batch_matches_element_wise_inserts() {
        let inputs = inputs_from(&keys(20_000, 512, 9_405));

        let mut one_by_one = OctoRuntime::new(
            &config(3),
            |_| CmOctoWorker::new(ROWS, COLS),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            },
        );
        for input in &inputs {
            one_by_one.insert(input.clone());
        }

        let mut batched = OctoRuntime::new(
            &config(3),
            |_| CmOctoWorker::new(ROWS, COLS),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            },
        );
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
        };
        let got = run_octo(
            &inputs,
            &cfg,
            |_| HllOctoWorker::new(),
            || HllOctoAggregator {
                sketch: HyperLogLog::<Classic>::default(),
            },
        )
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
        };
        let got = run_octo(
            &inputs,
            &cfg,
            |_| HllOctoWorker::new(),
            || HllOctoAggregator {
                sketch: HyperLogLog::<Classic>::default(),
            },
        )
        .parent
        .sketch;

        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }
        assert_eq!(got.registers_as_slice(), reference.registers_as_slice());
    }

    #[test]
    fn borrowed_string_inputs_survive_the_cross_thread_transport() {
        let owned: Vec<String> = (0..5_000).map(|i| format!("session-{i:05}")).collect();
        let inputs: Vec<DataInput<'_>> = owned.iter().map(|s| DataInput::Str(s)).collect();

        let got = run_octo(
            &inputs,
            &config(4),
            |_| HllOctoWorker::new(),
            || HllOctoAggregator {
                sketch: HyperLogLog::<Classic>::default(),
            },
        )
        .parent
        .sketch;

        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }
        assert_eq!(
            got.registers_as_slice(),
            reference.registers_as_slice(),
            "borrowed &str payloads must reach the workers intact"
        );
    }

    #[test]
    fn an_empty_stream_finishes_with_a_pristine_parent() {
        let got = run_octo(
            &[],
            &config(4),
            |_| CmOctoWorker::new(ROWS, COLS),
            || CmOctoAggregator {
                sketch: Cm::with_dimensions(ROWS, COLS),
            },
        )
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

        fn process<F>(&mut self, _input: &DataInput, emit: &mut F)
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

        let result = run_octo(
            &inputs,
            &config(workers),
            |worker_id| SumWorker { worker_id, seen: 0 },
            || SumAggregator {
                per_worker: vec![0; workers],
            },
        );

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
        let mut runtime = OctoRuntime::new(
            &config(workers),
            |worker_id| SumWorker { worker_id, seen: 0 },
            || SumAggregator {
                per_worker: vec![0; workers],
            },
        );
        let reader = runtime.read_handle();

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

        let result = runtime.finish();
        let total = result.parent.per_worker.iter().sum::<u64>();
        assert_eq!(total, n);
        assert!(total >= last, "final total must dominate every snapshot");
    }

    #[test]
    #[should_panic(expected = "Octo runtime has been finished")]
    fn reading_through_a_stale_handle_panics() {
        let workers = 2;
        let runtime = OctoRuntime::new(
            &config(workers),
            |worker_id| SumWorker { worker_id, seen: 0 },
            || SumAggregator {
                per_worker: vec![0; workers],
            },
        );
        let reader = runtime.read_handle();
        let _ = runtime.finish();
        reader.with_parent(|p| p.per_worker.len());
    }

    #[test]
    fn close_is_idempotent_and_preserves_already_queued_work() {
        let workers = 4;
        let n = 3_001u64;
        let mut runtime = OctoRuntime::new(
            &config(workers),
            |worker_id| SumWorker { worker_id, seen: 0 },
            || SumAggregator {
                per_worker: vec![0; workers],
            },
        );
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
        let mut runtime = OctoRuntime::new(
            &config(workers),
            |worker_id| SumWorker { worker_id, seen: 0 },
            || SumAggregator {
                per_worker: vec![0; workers],
            },
        );
        runtime.close();
        runtime.insert(DataInput::U64(1));
    }

    // -- accuracy end to end -------------------------------------------------

    #[test]
    fn run_octo_cm_zipf_error_stays_within_the_cms_bound_plus_the_residual() {
        let rows = 5;
        let cols = 4096;
        let stream = keys(200_000, 4_096, 9_501);
        let inputs = inputs_from(&stream);

        let mut truth = common::FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }

        let parent = cm_runtime(&inputs, 4, rows, cols);

        // Upper side: the usual one-sided CMS bound ε·N with ε = e/cols.
        // Lower side: OctoSketch (NSDI '24) Theorem 1 charges the promotion
        // protocol an additive k'·τ, where k' is the number of workers a key
        // may reach. `run_octo` dispatches round-robin, so every key reaches
        // every worker and k' = num_workers - the worst case of that bound.
        let epsilon_n = (std::f64::consts::E / cols as f64) * truth.total() as f64;
        let floor = 4.0 * MAX_CELL_RESIDUAL as f64;
        for k in 0u64..4_096 {
            let exact = truth.get(k as i64) as f64;
            let est = parent.estimate(&DataInput::U64(k)) as f64;
            assert!(
                est >= exact - floor,
                "key {k}: octo estimate {est} fell more than {floor} below truth {exact}"
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
        let got = run_octo(
            &inputs,
            &config(4),
            |_| HllOctoWorker::new(),
            || HllOctoAggregator {
                sketch: HyperLogLog::<Classic>::default(),
            },
        )
        .parent
        .sketch;

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

        let mut truth = common::FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }

        let parent = run_octo(
            &inputs,
            &config(4),
            |_| CountOctoWorker::new(rows, cols),
            || CountOctoAggregator {
                sketch: Cs::with_dimensions(rows, cols),
            },
        )
        .parent
        .sketch;

        // Count-Sketch error is ±‖f‖₂/√cols w.h.p.; per OctoSketch Theorem 1
        // the promotion protocol adds k'·τ, and round-robin makes k' = workers.
        let tolerance =
            3.0 * truth.l2_norm() / (cols as f64).sqrt() + 4.0 * MAX_CELL_RESIDUAL as f64;
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
