//! Regression tests for wrong-query-results bugs found by the
//! ground-truth accuracy probe (examples/accuracy_probe.rs).
//!
//! Each test feeds fully deterministic synthetic data with an exactly known
//! answer and asserts the theory-correct behavior. These tests FAIL against
//! the current implementation; each documents one confirmed defect.

use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketch as PortableDds;
use asap_sketchlib::{
    CountL2HH, CountMin, DDSketch, DataInput, DefaultXxHasher, NitroBatch, Vector2D,
};

// ---------------------------------------------------------------------------
// Bug 1: NitroBatch::estimate_median reads a different hash domain than the
// insert path writes, so it returns ~0 for every key.
//
// Insert:  hash128_seeded(0, key), bit-sliced per row   (nitro.rs:259)
// Estimate: CountMin fast path -> Packed64(hash64(...)) (hash.rs:329)
//
// Synthetic stream: one key inserted 100_000 times at rate 1.0.
// Truth: 100_000. Current behavior: 0.
// ---------------------------------------------------------------------------
#[test]
fn nitro_estimate_median_matches_insert_hash_domain() {
    let n = 100_000i64;
    let mut nb = NitroBatch::with_target(
        1.0,
        CountMin::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(5, 2048),
    );
    nb.insert(&vec![7i64; n as usize]);

    let est = nb.estimate_median(&DataInput::I64(7));
    assert!(
        est > 0.5 * n as f64,
        "public estimate_median returned {est} for a key inserted {n} times \
         (insert and estimate use different hash derivations)"
    );
}

// ---------------------------------------------------------------------------
// Bug 2 (regression): NitroBatch must not assign each sampled item to a
// single row (position % rows), which drove every per-row counter to
// truth/rows. After the fix each sampled record updates ALL rows, so the
// estimate converges to the true frequency at ANY sampling rate.
//
// Synthetic stream: one key inserted 10_000 times, 5 rows, rates {1.0, 0.5}.
// Truth: 10_000. Pre-fix behavior: 2_000 (= n / rows) or 0 (hash mismatch).
// ---------------------------------------------------------------------------
#[test]
fn nitro_estimate_is_not_divided_by_rows() {
    let n = 100_000i64;
    let rows = 5usize;
    for rate in [1.0f64, 0.5, 0.25] {
        let mut nb = NitroBatch::with_target(
            rate,
            CountMin::<Vector2D<i32>, asap_sketchlib::FastPath>::with_dimensions(rows, 2048),
        );
        nb.insert(&vec![7i64; n as usize]);
        let est = nb.estimate_median(&DataInput::I64(7));
        assert!(
            (est - n as f64).abs() <= 0.05 * n as f64,
            "rate={rate}: estimate_median returned {est} for {n} inserts \
             across {rows} rows (expected ~{n})"
        );
    }
}

// ---------------------------------------------------------------------------
// Bug 3: portable DdSketch reports gamma^(k+0.5) (bucket log-midpoint) as the
// representative (portable/ddsketch.rs:426). For values sitting at the lower
// edge of bucket k the relative error is sqrt(gamma)-1 ~= alpha + alpha^2/2,
// violating the advertised relative-accuracy guarantee. Core DDSketch already
// uses gamma^k * (1 + alpha) (ddsketch.rs:383-392), so the two disagree on
// identical data.
//
// Synthetic data: value gamma^k * (1 + 1e-6) repeated 10_000x, alpha = 0.05.
// Truth for every quantile: the value itself. Current behavior: 5.13% error.
// ---------------------------------------------------------------------------
#[test]
fn portable_ddsketch_respects_alpha_at_bucket_edges() {
    let alpha = 0.05;
    let gamma = (1.0f64 + alpha) / (1.0 - alpha);
    let v = gamma.powi(20) * (1.0 + 1e-6);

    let mut port = PortableDds::new(alpha);
    let mut core = DDSketch::new(alpha);
    for _ in 0..10_000 {
        port.update(v);
        core.add(&v);
    }

    let q_port = port.quantile(0.5).unwrap();
    let rel_port = ((q_port - v) / v).abs();
    assert!(
        rel_port <= alpha * (1.0 + 1e-6),
        "portable DdSketch median off by {rel_port:.5} (> alpha={alpha}) at a \
         bucket-edge value"
    );

    // Both implementations must sit within alpha of the truth on identical
    // data. (Exact equality is impossible: core clamps representatives to its
    // observed min/max, which the portable wire format does not carry.)
    let q_core = core.get_value_at_quantile(0.5).unwrap();
    let rel_core = ((q_core - v) / v).abs();
    assert!(
        rel_core <= alpha * (1.0 + 1e-6),
        "core DDSketch median off by {rel_core:.5} (> alpha={alpha})"
    );
}

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
