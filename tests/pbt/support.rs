//! Shared generators, probes and the round-trip law.
//!
//! One home for what more than one sketch needs: the adversarial generators
//! the wire laws aim at the envelope, a reader that flattens a grid into
//! something `prop_assert_eq!` can compare, and the round-trip law itself,
//! stated once and applied by every sketch that has an envelope.

#![allow(dead_code)]

use proptest::prelude::*;

/// Encode, decode, and hold the decoded sketch to the original's answers and
/// to a byte-identical re-encode. A refused encode passes: several payloads
/// cannot represent every state their constructor accepts. Silent corruption
/// is not allowed.
macro_rules! round_trip {
    ($ty:ty, $sketch:expr, $($probe:expr),+ $(,)?) => {{
        let original = &$sketch;
        if let Ok(bytes) = original.serialize_to_bytes() {
            let decoded = <$ty>::deserialize_from_bytes(&bytes)
                .expect("bytes the encoder produced must decode");
            $(
                prop_assert_eq!($probe(&decoded), $probe(original));
            )+
            prop_assert_eq!(
                decoded.serialize_to_bytes().expect("a decoded sketch must re-encode"),
                bytes,
                "the re-encode is not byte-identical"
            );
        }
    }};
}

/// Quantiles probed on both sides of a round trip.
pub const QUANTILES: [f64; 5] = [0.0, 0.1, 0.5, 0.9, 1.0];

/// How many keys a round-trip probe asks a frequency sketch about.
pub const PROBE_KEYS: usize = 64;

/// Relative tolerance absorbing f64 summation-order differences between a
/// merge and the equivalent update stream.
pub const REL_TOL: f64 = 1e-12;

pub fn close(a: f64, b: f64) -> bool {
    let scale = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() <= REL_TOL * scale
}

pub fn matrices_close(a: &[Vec<f64>], b: &[Vec<f64>]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(ra, rb)| ra.len() == rb.len() && ra.iter().zip(rb).all(|(x, y)| close(*x, *y)))
}

/// Every cell of a `rows` by `cols` grid in row-major order.
pub fn grid<T>(rows: usize, cols: usize, mut read: impl FnMut(usize, usize) -> T) -> Vec<T> {
    let mut cells = Vec::with_capacity(rows * cols);
    for r in 0..rows {
        for c in 0..cols {
            cells.push(read(r, c));
        }
    }
    cells
}

pub fn sorted(values: &[i64]) -> Vec<i64> {
    let mut v = values.to_vec();
    v.sort_unstable();
    v
}

/// NaN, the infinities, the signed zeros and the f64 extremes, mixed into a
/// draw that is mostly ordinary values.
pub fn extreme_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        6 => any::<f64>(),
        1 => Just(f64::NAN),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
        1 => Just(0.0f64),
        1 => Just(-0.0f64),
        1 => Just(f64::MIN_POSITIVE),
        1 => Just(f64::MAX),
        1 => Just(f64::MIN),
        1 => Just(f64::EPSILON),
    ]
}

pub fn extreme_values(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(extreme_f64(), 0..max)
}

/// One, a prime, and a power of two, at both ends of the useful range.
pub fn edge_dimension() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(1usize),
        Just(2),
        Just(3),
        Just(7),
        Just(8),
        Just(64),
        Just(127),
        Just(128),
    ]
}

/// Row counts the ASAPv1 matrix envelope accepts: it refuses anything above
/// `MATRIX_MAX_ROWS`, so a wider draw on this axis would be thrown away
/// before a round trip could run.
pub fn edge_rows() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(1usize),
        Just(2),
        Just(3),
        Just(7),
        Just(8),
        Just(16),
        Just(19),
        Just(20),
    ]
}

/// Slice counts a Bloom filter accepts: `with_dimensions` panics above
/// `BLOOM_MAX_SLICES`, so the geometry is refused at construction rather than
/// at serialization.
pub fn bloom_rows() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(1usize),
        Just(2),
        Just(3),
        Just(7),
        Just(8),
        Just(16),
        Just(20),
    ]
}

/// A key domain narrow enough that collisions and repeats happen by
/// construction rather than by luck.
pub fn keys(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(0u64..512, 0..max)
}

pub fn wide_keys(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(any::<u64>(), 0..max)
}

pub fn keyed_updates(max: usize) -> impl Strategy<Value = Vec<(String, f64)>> {
    prop::collection::vec(("[a-z]{1,8}", 1.0f64..10_000.0), 0..max)
}
