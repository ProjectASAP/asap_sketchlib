//! E2E suite for the Bloom filter: the properties a membership sketch is
//! bought for, which the shared batteries do not model.
//!
//! `tests/conformance_kit.rs` runs the standard `membership_battery`. Here the
//! filter is pushed on the three things it promises exactly - no false
//! negative, a union that is the filter of the concatenated stream, and a
//! delivered false-positive rate its own sizing predicts - plus the two hash
//! paths, serialization, and the degenerate geometries.

use asap_sketchlib::{Bloom, DataInput, FastPath, RegularPath};

const MEMBERS: i64 = 20_000;
const PROBES: i64 = 200_000;

fn members() -> Vec<i64> {
    (0..MEMBERS).collect()
}

fn probes() -> Vec<i64> {
    (10_000_000..10_000_000 + PROBES).collect()
}

fn filled_regular(target: f64) -> Bloom<RegularPath> {
    let mut filter = Bloom::<RegularPath>::with_capacity(MEMBERS as usize, target);
    for key in members() {
        filter.insert(&DataInput::I64(key));
    }
    filter
}

fn false_positive_rate<F: Fn(i64) -> bool>(contains: F) -> f64 {
    let probes = probes();
    let hits = probes.iter().filter(|k| contains(**k)).count();
    hits as f64 / probes.len() as f64
}

/// The one exact guarantee: a key that was inserted is never reported absent.
/// Asserted on both hash paths, since they decode columns differently.
#[test]
fn an_inserted_key_is_never_reported_absent() {
    let regular = filled_regular(0.01);
    let absent: Vec<i64> = members()
        .into_iter()
        .filter(|k| !regular.contains(&DataInput::I64(*k)))
        .collect();
    assert!(
        absent.is_empty(),
        "regular path lost {} of {MEMBERS} members, first {:?}",
        absent.len(),
        absent.first()
    );

    let mut fast = Bloom::<FastPath>::with_capacity(MEMBERS as usize, 0.01);
    for key in members() {
        fast.insert(&DataInput::I64(key));
    }
    let absent_fast = members()
        .into_iter()
        .filter(|k| !fast.contains(&DataInput::I64(*k)))
        .count();
    assert_eq!(absent_fast, 0, "fast path lost {absent_fast} of {MEMBERS}");
}

/// A filter with nothing in it rejects everything, on both paths.
#[test]
fn an_empty_filter_rejects_every_probe() {
    let regular = Bloom::<RegularPath>::with_capacity(MEMBERS as usize, 0.01);
    let fast = Bloom::<FastPath>::with_capacity(MEMBERS as usize, 0.01);
    assert!(regular.is_empty());
    assert_eq!(regular.inserted(), 0);
    for key in probes().into_iter().take(10_000) {
        assert!(!regular.contains(&DataInput::I64(key)));
        assert!(!fast.contains(&DataInput::I64(key)));
    }
}

/// The delivered rate lands at or under the target, and `predicted_fpp`
/// describes it. Sizing rounds each slice up to a power of two, so the filter
/// is larger than the formula's minimum and the rate correspondingly lower.
#[test]
fn the_measured_false_positive_rate_matches_what_the_sizing_predicts() {
    for target in [0.1, 0.01, 0.001] {
        let filter = filled_regular(target);
        let measured = false_positive_rate(|k| filter.contains(&DataInput::I64(k)));
        let predicted = filter.predicted_fpp(MEMBERS as usize);

        assert!(
            measured <= target,
            "target {target}: measured {measured:.5} above the target"
        );
        assert!(
            predicted <= target,
            "target {target}: predicted {predicted:.5} above the target"
        );
        // The prediction is the model; allow a quarter of it either way for the
        // finite probe set rather than pinning an exact rate.
        assert!(
            measured >= predicted * 0.75 && measured <= predicted * 1.25,
            "target {target}: measured {measured:.5} outside 0.75-1.25x of predicted {predicted:.5}"
        );
    }
}

/// `estimated_fpp` reads the bits actually set rather than the insert count,
/// so it tracks the measured rate on a filter that saw duplicates.
#[test]
fn the_fill_based_estimate_tracks_the_measured_rate() {
    let mut filter = Bloom::<RegularPath>::with_capacity(MEMBERS as usize, 0.01);
    for _ in 0..3 {
        for key in members() {
            filter.insert(&DataInput::I64(key));
        }
    }
    assert_eq!(filter.inserted(), 3 * MEMBERS as u64);

    let measured = false_positive_rate(|k| filter.contains(&DataInput::I64(k)));
    let estimated = filter.estimated_fpp();
    assert!(
        measured >= estimated * 0.75 && measured <= estimated * 1.25,
        "measured {measured:.5} outside 0.75-1.25x of estimated {estimated:.5}"
    );
}

/// Inserting a key twice sets no bit the first insert did not, so the filter's
/// fill is a function of the distinct keys alone.
#[test]
fn repeated_inserts_leave_the_bits_unchanged() {
    let mut once = Bloom::<RegularPath>::with_capacity(1_000, 0.01);
    let mut twice = Bloom::<RegularPath>::with_capacity(1_000, 0.01);
    for key in 0..1_000i64 {
        once.insert(&DataInput::I64(key));
        twice.insert(&DataInput::I64(key));
        twice.insert(&DataInput::I64(key));
    }
    assert_eq!(once.as_bits().count_ones(), twice.as_bits().count_ones());
    assert_eq!(once.fill_ratio(), twice.fill_ratio());
}

/// A union is exact: merging two filters gives the same bits as one filter
/// over both streams. This is the property that makes the filter shardable.
#[test]
fn a_union_equals_the_filter_of_the_concatenated_stream() {
    let mut left = Bloom::<RegularPath>::with_dimensions(7, 1 << 14);
    let mut right = Bloom::<RegularPath>::with_dimensions(7, 1 << 14);
    let mut whole = Bloom::<RegularPath>::with_dimensions(7, 1 << 14);

    for key in 0..10_000i64 {
        if key % 2 == 0 {
            left.insert(&DataInput::I64(key));
        } else {
            right.insert(&DataInput::I64(key));
        }
        whole.insert(&DataInput::I64(key));
    }

    left.merge_from(&right);
    assert_eq!(left.as_bits().count_ones(), whole.as_bits().count_ones());
    assert_eq!(left.inserted(), whole.inserted());

    for key in 0..10_000i64 {
        assert!(left.contains(&DataInput::I64(key)), "union lost key {key}");
    }
    // Same bits means the same answers on non-members too, not just members.
    for key in probes().into_iter().take(20_000) {
        assert_eq!(
            left.contains(&DataInput::I64(key)),
            whole.contains(&DataInput::I64(key)),
            "union and single-pass disagree on non-member {key}"
        );
    }
}

/// Sizing follows the standard formula, with each slice rounded up to a power
/// of two so the column fold carries no modulo bias.
#[test]
fn sizing_follows_the_formula_and_rounds_slices_to_a_power_of_two() {
    for (n, p) in [(1_000usize, 0.01), (20_000, 0.001), (1, 0.5)] {
        let (rows, cols) = Bloom::<RegularPath>::dimensions_for(n, p);
        assert!(rows >= 1 && cols >= 1, "n={n} p={p} gave {rows}x{cols}");
        assert!(
            cols.is_power_of_two(),
            "n={n} p={p} gave a non-power-of-two slice of {cols}"
        );

        let ln2 = std::f64::consts::LN_2;
        let m = -(n as f64) * p.ln() / (ln2 * ln2);
        let ideal_rows = ((m / n as f64) * ln2).round().max(1.0) as usize;
        assert_eq!(rows, ideal_rows, "n={n} p={p} row count");
        assert!(
            rows * cols >= m.ceil() as usize,
            "n={n} p={p}: {rows}x{cols} is below the {m:.0} bits the formula asks for"
        );
    }
}

/// A single-bit filter is degenerate but must still answer, and a single row
/// must still hold its members.
#[test]
fn degenerate_geometries_still_answer() {
    let mut one = Bloom::<RegularPath>::with_dimensions(1, 1);
    assert!(!one.contains(&DataInput::I64(7)));
    one.insert(&DataInput::I64(7));
    assert!(one.contains(&DataInput::I64(7)));
    // One bit, so everything now reads present.
    assert!(one.contains(&DataInput::I64(8)));
    assert_eq!(one.estimated_fpp(), 1.0);

    let mut row = Bloom::<RegularPath>::with_dimensions(1, 4096);
    for key in 0..500i64 {
        row.insert(&DataInput::I64(key));
    }
    for key in 0..500i64 {
        assert!(row.contains(&DataInput::I64(key)));
    }
}

/// `clear` returns the filter to its constructed state, dimensions kept.
#[test]
fn clearing_restores_an_empty_filter() {
    let mut filter = filled_regular(0.01);
    let (rows, cols) = (filter.rows(), filter.cols());
    filter.clear();
    assert!(filter.is_empty());
    assert_eq!(filter.inserted(), 0);
    assert_eq!((filter.rows(), filter.cols()), (rows, cols));
    for key in members().into_iter().take(1_000) {
        assert!(!filter.contains(&DataInput::I64(key)));
    }
}

/// A round-trip through serde preserves every answer, members and probes
/// alike.
#[test]
fn a_serde_round_trip_preserves_every_answer() {
    let filter = filled_regular(0.01);
    let bytes = rmp_serde::to_vec(&filter).expect("serialize");
    let decoded: Bloom<RegularPath> = rmp_serde::from_slice(&bytes).expect("deserialize");

    assert_eq!(decoded.rows(), filter.rows());
    assert_eq!(decoded.cols(), filter.cols());
    assert_eq!(decoded.inserted(), filter.inserted());
    assert_eq!(
        decoded.as_bits().count_ones(),
        filter.as_bits().count_ones()
    );
    for key in members() {
        assert!(decoded.contains(&DataInput::I64(key)));
    }
    for key in probes().into_iter().take(20_000) {
        assert_eq!(
            decoded.contains(&DataInput::I64(key)),
            filter.contains(&DataInput::I64(key))
        );
    }
}

/// Packed storage is one bit per cell, not one byte.
#[test]
fn storage_is_one_bit_per_cell() {
    let filter = Bloom::<RegularPath>::with_dimensions(8, 4096);
    assert_eq!(filter.bit_capacity(), 8 * 4096);
    assert_eq!(filter.size_in_bytes(), 8 * 4096 / 8);
}
