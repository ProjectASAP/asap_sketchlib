//! Regression tests for wrong-query-results bugs found by the
//! ground-truth accuracy probe (examples/accuracy_probe.rs).
//!
//! Each test feeds fully deterministic synthetic data with an exactly known
//! answer and asserts the theory-correct behavior.
//!
//! The two Nitro defects the probe found — the insert and query paths reading
//! different hash domains, and each sampled record reaching only one row — are
//! pinned by `full_sampling_is_exact_and_the_query_reads_the_cells_the_insert_wrote`
//! in `tests/e2e/nitro.rs`, which names both failure modes on the same run
//! that already exercises every ingestion path.

use asap_sketchlib::{CountL2HH, DataInput, DefaultXxHasher};

// ---------------------------------------------------------------------------
// Bug 4: CountL2HH's hot-path L2 accumulation ran in plain i64
// (countsketch_topk.rs:1028: old + new^2 - old^2) and wrapped silently once
// sum(count^2) exceeded i64::MAX (panicking in debug builds). The hot path
// now saturates in i128, matching the merge path.
//
// Synthetic stream: two keys with count 3e9 each => true F2 = 1.8e19 > i64::MAX.
// Pre-fix behavior: 0 in release, overflow panic in debug. Post-fix: saturates.
// ---------------------------------------------------------------------------
#[test]
fn countl2hh_f2_survives_beyond_i64_max() {
    let c = 3_000_000_000i64;
    let mut sk = CountL2HH::<DefaultXxHasher>::with_dimensions_and_seed(4, 2048, 7);
    sk.fast_insert_with_count(&DataInput::U32(1), c);

    // Single key: true F2 = 9e18 < i64::MAX => must stay exact.
    let single_truth = (c as f64) * (c as f64);
    let got_single = sk.get_l2_sqr();
    assert!(
        (got_single - single_truth).abs() / single_truth < 1e-12,
        "exact-range F2 drifted: got {got_single}, expected {single_truth:.3e}"
    );

    // Second key pushes the true F2 to 1.8e19 — beyond any i64 counter.
    sk.fast_insert_with_count(&DataInput::U32(2), c);
    let truth = 2.0f64 * (c as f64) * (c as f64); // 1.8e19 > i64::MAX
    let got = sk.get_l2_sqr();

    // Must saturate at i64::MAX rather than wrap to garbage or panic.
    let i64_max = i64::MAX as f64;
    assert!(
        (got - i64_max).abs() / i64_max < 1e-12,
        "F2 neither exact nor saturated: got {got}, expected saturation at \
         {i64_max:.3e} (true value {truth:.3e})"
    );
}
