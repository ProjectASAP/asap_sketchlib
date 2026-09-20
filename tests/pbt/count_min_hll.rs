//! Property tests for `CountMinHll`: a Count-Min-shaped grid whose buckets
//! hold HyperLogLog registers instead of counters.
//!
//! The oracle for what a bucket holds is a standalone
//! `HyperLogLogImpl<Classic, _>` of the same precision, fed the same value
//! hashes through its own register and rank code: two separate derivations of
//! the same published construction, held to byte equality.
//!
//! Column routing is never recomputed here. It is read back from a sketch that
//! has seen one key and nothing else, so the laws constrain the grid without
//! restating its hashing.
//!
//! Repeats and arrival order answer to an exact set model: the registers are a
//! function of the distinct `(key, value)` pairs, and of nothing else.
//!
//! Cormode and Muthukrishnan, J. Algorithms 55(1), 2005.
//! Flajolet, Fusy, Gandouet and Meunier, AofA '07.

use asap_sketchlib::sketches::hll::HyperLogLogImpl;
use asap_sketchlib::{Classic, CountMinHll, DataInput, DefaultXxHasher, SketchHasher};
use proptest::prelude::*;
use std::collections::HashSet;

asap_sketchlib::impl_hll_bucket_list!(BucketsP4, 4, 1_usize << 4);
asap_sketchlib::impl_hll_bucket_list!(BucketsP6, 6, 1_usize << 6);
asap_sketchlib::impl_hll_bucket_list!(BucketsP8, 8, 1_usize << 8);

type Sketch = CountMinHll<DefaultXxHasher>;

/// The seed a value is hashed under before it reaches a bucket's registers.
/// Part of the wire form: registers written under another seed name a
/// different distinct set.
const HLL_SEED: usize = 1;

fn value_hash(value: u64) -> u64 {
    DefaultXxHasher::hash64_seeded(HLL_SEED, &DataInput::U64(value))
}

/// Keys carrying a stream of values each, repeats included. Keys are unique
/// within a draw, so "another key reaches this bucket" means a collision.
fn key_groups(
    max_keys: usize,
    key_space: u64,
    max_values: usize,
) -> impl Strategy<Value = Vec<(u64, Vec<u64>)>> {
    prop::collection::hash_map(
        0u64..key_space,
        prop::collection::vec(0u64..64, 1..max_values),
        1..=max_keys,
    )
    .prop_map(|map| {
        let mut groups: Vec<(u64, Vec<u64>)> = map.into_iter().collect();
        groups.sort_unstable();
        groups
    })
}

fn pair_stream(max_pairs: usize) -> impl Strategy<Value = Vec<(u64, u64)>> {
    prop::collection::vec((0u64..24, 0u64..24), 0..max_pairs)
}

/// Grid shapes the constructor accepts: `cols` a power of two at least 2, and
/// `rows * log2(cols)` inside the 128-bit packed column hash.
fn shape() -> impl Strategy<Value = (usize, usize, u32)> {
    (
        1usize..=4,
        prop::sample::select(vec![2usize, 4, 8, 16, 32]),
        1u32..=8,
    )
}

macro_rules! shape_laws {
    ($name:ident, $rows:expr, $cols:expr, $precision:expr, $buckets:ty, $keys:expr, $space:expr) => {
        mod $name {
            use super::*;

            const ROWS: usize = $rows;
            const COLS: usize = $cols;
            const P: u32 = $precision;

            type Reference = HyperLogLogImpl<Classic, $buckets>;

            fn empty() -> Sketch {
                Sketch::with_dimensions(ROWS, COLS, P)
            }

            fn sketch_of(groups: &[(u64, Vec<u64>)]) -> Sketch {
                let mut sketch = empty();
                for (key, values) in groups {
                    for value in values {
                        sketch.insert(&DataInput::U64(*key), &DataInput::U64(*value));
                    }
                }
                sketch
            }

            fn sketch_of_pairs(pairs: &[(u64, u64)]) -> Sketch {
                let mut sketch = empty();
                for (key, value) in pairs {
                    sketch.insert(&DataInput::U64(*key), &DataInput::U64(*value));
                }
                sketch
            }

            fn reference_of(values: &[u64]) -> Reference {
                let mut reference = Reference::new();
                for value in values {
                    reference.insert_with_hash(value_hash(*value));
                }
                reference
            }

            /// The column each row routes `key` to, read back instead of
            /// recomputed: one insert leaves a rank of at least one in exactly
            /// one bucket per row, so the bucket that is not all zeros names
            /// that row's column.
            fn route(key: u64) -> Vec<usize> {
                let mut probe = empty();
                probe.insert(&DataInput::U64(key), &DataInput::U64(0));
                (0..ROWS)
                    .map(|row| {
                        let touched: Vec<usize> = (0..COLS)
                            .filter(|&col| {
                                probe
                                    .as_storage()
                                    .bucket_slice(row, col)
                                    .iter()
                                    .any(|&register| register != 0)
                            })
                            .collect();
                        assert_eq!(
                            touched.len(),
                            1,
                            "key {key} touched {touched:?} in row {row}, expected one bucket"
                        );
                        touched[0]
                    })
                    .collect()
            }

            fn routes_of(groups: &[(u64, Vec<u64>)]) -> Vec<Vec<usize>> {
                groups.iter().map(|(key, _)| route(*key)).collect()
            }

            fn groups() -> impl Strategy<Value = Vec<(u64, Vec<u64>)>> {
                key_groups($keys, $space, 40)
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(64))]

                // One value hash serves every row, so a key with the grid to
                // itself writes the same register array into each row it
                // reaches. This is what makes the minimum across rows a bound
                // on one side only: the rows differ by what collided into
                // them, never by what the key itself wrote.
                #[test]
                fn every_row_holds_the_same_registers_for_a_key_that_has_the_grid_to_itself(
                    key in 0u64..4096,
                    values in prop::collection::vec(0u64..64, 1..40),
                ) {
                    let sketch = sketch_of(&[(key, values)]);
                    let storage = sketch.as_storage();

                    let touched: Vec<(usize, usize)> = (0..ROWS)
                        .flat_map(|row| (0..COLS).map(move |col| (row, col)))
                        .filter(|&(row, col)| {
                            storage.bucket_slice(row, col).iter().any(|&r| r != 0)
                        })
                        .collect();
                    prop_assert_eq!(
                        touched.len(),
                        ROWS,
                        "key {} wrote into {:?}, expected one bucket per row",
                        key,
                        touched
                    );

                    let (first_row, first_col) = touched[0];
                    let first = storage.bucket_slice(first_row, first_col);
                    for &(row, col) in &touched[1..] {
                        prop_assert_eq!(
                            storage.bucket_slice(row, col),
                            first,
                            "key {}: bucket ({}, {}) differs from ({}, {})",
                            key,
                            row,
                            col,
                            first_row,
                            first_col
                        );
                    }
                }

                // A bucket no other key routes to holds this key's values and
                // nothing else, so it must equal a standalone HyperLogLog of
                // the same precision fed the same hashes - register for
                // register, not merely to within the estimator's band.
                #[test]
                fn a_bucket_no_other_key_reaches_holds_the_standalone_hyperloglog(
                    groups in groups(),
                ) {
                    let sketch = sketch_of(&groups);
                    let routes = routes_of(&groups);

                    for (i, (key, values)) in groups.iter().enumerate() {
                        let reference = reference_of(values);
                        for row in 0..ROWS {
                            let col = routes[i][row];
                            let shared = routes
                                .iter()
                                .enumerate()
                                .any(|(j, other)| j != i && other[row] == col);
                            if shared {
                                continue;
                            }
                            prop_assert_eq!(
                                sketch.as_storage().bucket_slice(row, col),
                                reference.registers_as_slice(),
                                "key {} alone in bucket ({}, {})",
                                key,
                                row,
                                col
                            );
                        }
                    }
                }

                // A bucket carries the union of the distinct sets routed to
                // it, and a union only grows. Every register therefore sits at
                // or above what the key wrote on its own, in every row.
                #[test]
                fn no_register_falls_below_what_the_key_wrote_on_its_own(groups in groups()) {
                    let sketch = sketch_of(&groups);
                    let routes = routes_of(&groups);

                    for (i, (key, values)) in groups.iter().enumerate() {
                        let reference = reference_of(values);
                        for row in 0..ROWS {
                            let col = routes[i][row];
                            let bucket = sketch.as_storage().bucket_slice(row, col);
                            for (j, (held, own)) in bucket
                                .iter()
                                .zip(reference.registers_as_slice())
                                .enumerate()
                            {
                                prop_assert!(
                                    held >= own,
                                    "key {} bucket ({}, {}) register {}: {} below the key's own {}",
                                    key, row, col, j, held, own
                                );
                            }
                        }
                    }
                }

                // The error is one-sided at the answer as well as at the
                // registers: no collision can talk the estimate down below
                // what the key's own HyperLogLog reports.
                #[test]
                fn an_estimate_never_falls_below_the_keys_own_hyperloglog(groups in groups()) {
                    let sketch = sketch_of(&groups);

                    for (key, values) in &groups {
                        let alone = reference_of(values).estimate() as f64;
                        let estimate = sketch.estimate(&DataInput::U64(*key));
                        prop_assert!(
                            estimate >= alone,
                            "key {}: estimate {} below its own HyperLogLog's {}",
                            key, estimate, alone
                        );
                    }
                }

                // Registers are a function of the distinct pairs: neither how
                // often a pair arrives nor the order the stream interleaves in
                // can be read back out of the grid.
                #[test]
                fn only_the_distinct_pairs_reach_the_registers(
                    stream in pair_stream(200).prop_flat_map(|s| (Just(s.clone()), Just(s).prop_shuffle())),
                ) {
                    let (stream, shuffled) = stream;
                    let mut seen: Vec<(u64, u64)> =
                        stream.iter().copied().collect::<HashSet<_>>().into_iter().collect();
                    seen.sort_unstable();

                    let streamed = sketch_of_pairs(&stream);
                    let distinct = sketch_of_pairs(&seen);
                    let reordered = sketch_of_pairs(&shuffled);
                    prop_assert_eq!(
                        streamed.as_storage().as_slice(),
                        distinct.as_storage().as_slice(),
                        "repeats moved the registers"
                    );
                    prop_assert_eq!(
                        streamed.as_storage().as_slice(),
                        reordered.as_storage().as_slice(),
                        "arrival order moved the registers"
                    );
                }

                // Merging two grids must answer as if one had seen both
                // streams. Registers hold a maximum, the one combination that
                // cannot count twice what both sides already held.
                #[test]
                fn merge_equals_streaming_the_concatenation(a in groups(), b in groups()) {
                    let mut merged = sketch_of(&a);
                    merged.merge(&sketch_of(&b)).expect("one shape merges");

                    let concatenated: Vec<(u64, Vec<u64>)> =
                        a.iter().chain(&b).cloned().collect();
                    let streamed = sketch_of(&concatenated);
                    prop_assert_eq!(
                        merged.as_storage().as_slice(),
                        streamed.as_storage().as_slice()
                    );
                }

                #[test]
                fn merge_is_commutative(a in groups(), b in groups()) {
                    let mut ab = sketch_of(&a);
                    ab.merge(&sketch_of(&b)).expect("one shape merges");
                    let mut ba = sketch_of(&b);
                    ba.merge(&sketch_of(&a)).expect("one shape merges");

                    prop_assert_eq!(
                        ab.as_storage().as_slice(),
                        ba.as_storage().as_slice()
                    );
                }

                #[test]
                fn merge_is_associative(a in groups(), b in groups(), c in groups()) {
                    let mut left = sketch_of(&a);
                    left.merge(&sketch_of(&b)).expect("one shape merges");
                    left.merge(&sketch_of(&c)).expect("one shape merges");

                    let mut right = sketch_of(&b);
                    right.merge(&sketch_of(&c)).expect("one shape merges");
                    let mut whole = sketch_of(&a);
                    whole.merge(&right).expect("one shape merges");

                    prop_assert_eq!(
                        left.as_storage().as_slice(),
                        whole.as_storage().as_slice()
                    );
                }

                #[test]
                fn merge_is_idempotent_and_the_empty_grid_is_its_identity(a in groups()) {
                    let sketch = sketch_of(&a);
                    let before = sketch.as_storage().as_slice().to_vec();

                    let mut twice = sketch.clone();
                    twice.merge(&sketch).expect("one shape merges");
                    prop_assert_eq!(twice.as_storage().as_slice(), &before[..]);

                    let mut with_empty = sketch.clone();
                    with_empty.merge(&empty()).expect("one shape merges");
                    prop_assert_eq!(with_empty.as_storage().as_slice(), &before[..]);
                }

                #[test]
                fn a_round_trip_preserves_the_grid_and_its_answers(groups in groups()) {
                    let sketch = sketch_of(&groups);
                    let probed: Vec<u64> = groups.iter().map(|(key, _)| *key).collect();

                    round_trip!(
                        Sketch,
                        sketch,
                        |s: &Sketch| s.as_storage().as_slice().to_vec(),
                        |s: &Sketch| (s.rows(), s.cols(), s.precision()),
                        |s: &Sketch| probed
                            .iter()
                            .map(|k| s.estimate(&DataInput::U64(*k)).to_bits())
                            .collect::<Vec<u64>>(),
                    );
                }
            }
        }
    };
}

// A wide grid where a key usually has every row to itself, and two narrow ones
// where collisions are the rule rather than the exception.
shape_laws!(clean_p4, 4, 512, 4, BucketsP4, 6, 4096);
shape_laws!(colliding_p6, 4, 32, 6, BucketsP6, 20, 64);
shape_laws!(colliding_p8, 3, 32, 8, BucketsP8, 12, 64);

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Registers only line up when both grids route a pair to the same bucket
    // and the same register, so a merge across shapes has no meaning to
    // salvage. It must refuse rather than mix, and leave the target as it was.
    #[test]
    fn a_merge_across_shapes_is_refused_and_leaves_the_target_alone(
        target in shape(),
        other in shape(),
        stream in pair_stream(80),
    ) {
        prop_assume!(target != other);

        let mut sketch = Sketch::with_dimensions(target.0, target.1, target.2);
        for (key, value) in &stream {
            sketch.insert(&DataInput::U64(*key), &DataInput::U64(*value));
        }
        let before = sketch.as_storage().as_slice().to_vec();

        let err = sketch
            .merge(&Sketch::with_dimensions(other.0, other.1, other.2))
            .expect_err("a different shape must not merge");
        prop_assert!(
            err.contains("different shape"),
            "shape {:?} against {:?}: unexpected error {}",
            target, other, err
        );
        prop_assert_eq!(
            sketch.as_storage().as_slice(),
            &before[..],
            "shape {:?} against {:?}: a refused merge touched the target",
            target, other
        );
    }
}
