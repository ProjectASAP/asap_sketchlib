//! ASAPv1 wire serialization for [`CountSketchWithL2Norm`], plus the sub-payload the
//! UnivMon family reuses.
//!
//! Child submodule of [`crate::sketches::countsketch_l2_norm`]: it holds the
//! metadata/payload DTOs and the `to_asapv1` / `from_asapv1` impls, while the
//! algorithm lives in the parent module file. Being a descendant module, it
//! reads the private `counts`, `row_l2_squared`, `rows`, `cols` and
//! `seed_index` fields directly without widening any field visibility. See
//! `docs/asapv1_wire_format.md`.
//!
//! CountSketchWithL2Norm is one algorithm, kind_id `0x19 0x01`. The dimensions
//! (`rows` / `cols`) and the seed-list index the sketch hashes with
//! (`seed_index`) are construction config and live in the metadata, so the
//! payload is `[counts]`.
//!
//! ## The shared sub-payload
//!
//! [`CountSketchWithL2Norm`] is also the per-layer counter of `UnivMon` and
//! `UnivMonTwoTier`. Those sketches inline the same `counts` into their own
//! positional arrays instead of nesting an envelope per layer, so
//! [`layer_state`] and [`rebuild_layer`] are the one place that state is read
//! and rebuilt.
//!
//! ## `l2` is recomputed, not carried
//!
//! A row's L2 accumulator always equals
//! [`row_capped_square_sum`](super::row_capped_square_sum) of its counters
//! (read clamped at `i64::MAX`, it is the row's sum of squares), so the
//! decoder recomputes it and the encoder rejects a cached value that disagrees.
//! The retired kind `0x19 0x00` also carried it as `[counts, l2]`; it still
//! decodes, its `l2` read and dropped, and is never encoded.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::asapv1::envelope;
use crate::asapv1::kind;
use crate::asapv1::kind::{COUNT_SKETCH_WITH_L2_NORM, COUNT_SKETCH_WITH_L2_NORM_CARRYING_L2};
use crate::common::hash::check_matrix_rows;
use crate::{HashProfile, SketchHasher, Vector1D, Vector2D};

use super::{CountSketchWithL2Norm, clamp_to_i64, row_capped_square_sum};

/// CountSketchWithL2Norm descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it). Hash-spec fields first, then the
/// structural params `seed_index` / `rows` / `cols`.
///
/// There is no `counter_type` and no `mode`: the counters are `i64` and the
/// column derivation is fixed by the algorithm, so `kind_id` already
/// determines both.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CountSketchWithL2NormMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) seed_list: Vec<u64>,
    pub(crate) seed_index: u32,
    pub(crate) rows: u32,
    pub(crate) cols: u32,
}

/// Builds the CountSketchWithL2Norm descriptor metadata from the hasher's [`HashProfile`],
/// so the wire bytes truthfully describe how the sketch was hashed.
/// `seed_index` is the sketch's own seed offset, a construction parameter
/// rather than a profile constant, so it is carried as a structural param.
pub(crate) fn count_sketch_with_l2_norm_metadata<H: HashProfile>(
    seed_index: u32,
    rows: u32,
    cols: u32,
) -> CountSketchWithL2NormMetadata {
    CountSketchWithL2NormMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        seed_index,
        rows,
        cols,
    }
}

/// CountSketchWithL2Norm payload (ASAPv1 §3.19), a msgpack **array**
/// (`to_vec`, positional): `[counts]`, packed row-major over `rows*cols`
/// signed cells.
#[derive(Debug, Serialize, Deserialize)]
struct CountSketchWithL2NormPayload {
    counts: Vec<i64>,
}

/// The retired `0x19 0x00` payload, `[counts, l2]`: decoded only, its `l2`
/// dropped.
#[derive(Debug, Serialize, Deserialize)]
struct CountSketchWithL2NormPayloadCarryingL2 {
    counts: Vec<i64>,
    l2: RetiredL2,
}

/// A retired payload's `l2` array. Decoding reads every element as an `i64`,
/// so anything but an integer array is still rejected, and keeps none of
/// them: the decoded value is always empty and allocates nothing. Encoding
/// writes the held values, which only tests use to craft retired bytes.
#[derive(Debug, Default)]
pub(crate) struct RetiredL2(pub(crate) Vec<i64>);

impl Serialize for RetiredL2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RetiredL2 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SkipI64s;
        impl<'de> serde::de::Visitor<'de> for SkipI64s {
            type Value = RetiredL2;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of i64 l2 accumulators")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<RetiredL2, A::Error> {
                while seq.next_element::<i64>()?.is_some() {}
                Ok(RetiredL2::default())
            }
        }
        deserializer.deserialize_seq(SkipI64s)
    }
}

/// Rejects a zero dimension, a row count past `MATRIX_MAX_ROWS`, and a
/// geometry whose per-row hash fields do not fit in one 128-bit key hash.
///
/// `Vector2D` derives its column mask via `cols.ilog2()`, which panics on
/// `cols == 0`.
///
/// The hash-bit budget is [`CountSketchWithL2Norm::check_hash_bits`], the same check the
/// constructor panics on, returned here as a `Result` so both wire doors fail
/// closed instead of panicking.
pub(crate) fn check_dimensions(rows: usize, cols: usize) -> Result<(), String> {
    if rows == 0 || cols == 0 {
        return Err(format!(
            "CountSketchWithL2Norm dimensions must be non-zero: rows={rows}, cols={cols}"
        ));
    }
    check_matrix_rows("CountSketchWithL2Norm", rows)?;
    CountSketchWithL2Norm::check_hash_bits(rows, cols)
}

/// Rejects a seed index the hasher's seed list does not hold: the wire
/// carries seed indexes below the seed list's length.
pub(crate) fn check_seed_index(seed_index: usize, seed_list_len: usize) -> Result<(), String> {
    if seed_index >= seed_list_len {
        return Err(format!(
            "CountSketchWithL2Norm seed_index {seed_index} is past the seed list's {seed_list_len} entries"
        ));
    }
    Ok(())
}

/// The `counts` sub-payload one CountSketchWithL2Norm contributes, in the
/// order the wire emits it. Rejects a matrix whose cell count disagrees with
/// its own dimensions, and a cached accumulator that is not its row's
/// [`row_capped_square_sum`], the value the decoder recomputes; the error
/// names the first such row.
pub(crate) fn layer_state<H: SketchHasher>(
    sketch: &CountSketchWithL2Norm<H>,
) -> Result<&[i64], RmpEncodeError> {
    check_dimensions(sketch.rows, sketch.cols).map_err(RmpEncodeError::Syntax)?;
    let cells = sketch.rows.saturating_mul(sketch.cols);
    let counts = sketch.counts.as_slice();
    if counts.len() != cells {
        return Err(RmpEncodeError::Syntax(format!(
            "ASAPv1 CountSketchWithL2Norm state: counts length {} != rows*cols {cells}",
            counts.len()
        )));
    }
    let cached = sketch.row_l2_squared.as_slice();
    if cached.len() != sketch.rows {
        return Err(RmpEncodeError::Syntax(format!(
            "ASAPv1 CountSketchWithL2Norm state: {} cached l2 accumulators over {} rows",
            cached.len(),
            sketch.rows
        )));
    }
    for (row, (counters, &held)) in counts.chunks_exact(sketch.cols).zip(cached).enumerate() {
        let recomputed = row_capped_square_sum(counters);
        if held != recomputed {
            return Err(RmpEncodeError::Syntax(format!(
                "ASAPv1 CountSketchWithL2Norm state: row {row}'s cached l2 accumulator {held} (l2 squared {}) is not the {recomputed} its counters give (l2 squared {})",
                clamp_to_i64(held),
                clamp_to_i64(recomputed)
            )));
        }
    }
    Ok(counts)
}

/// Rebuilds one CountSketchWithL2Norm from a validated `counts` sub-payload,
/// recomputing each row's L2 accumulator. The length is checked against the
/// declared geometry before the matrix is built, so a crafted `rows`/`cols`
/// never drives an allocation.
pub(crate) fn rebuild_layer<H: SketchHasher>(
    rows: usize,
    cols: usize,
    seed_index: usize,
    counts: &[i64],
) -> Result<CountSketchWithL2Norm<H>, RmpDecodeError> {
    check_dimensions(rows, cols).map_err(RmpDecodeError::Uncategorized)?;
    let cells = rows.saturating_mul(cols);
    if counts.len() != cells {
        return Err(RmpDecodeError::Uncategorized(format!(
            "CountSketchWithL2Norm counts length {} != rows*cols {cells}",
            counts.len()
        )));
    }
    Ok(CountSketchWithL2Norm {
        counts: Vector2D::from_fn(rows, cols, |r, c| counts[r * cols + c]),
        row_l2_squared: Vector1D::from_vec(
            counts
                .chunks_exact(cols)
                .map(row_capped_square_sum)
                .collect(),
        ),
        rows,
        cols,
        seed_index,
        _hasher: PhantomData,
    })
}

// Wire serialization for CountSketchWithL2Norm. This module is a descendant of the sketch
// module, so this impl reads the private fields directly.
impl<H: SketchHasher + HashProfile> CountSketchWithL2Norm<H> {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x19 0x01`). The metadata is derived from the hasher's
    /// [`HashProfile`], so it truthfully describes how the sketch was hashed.
    ///
    /// Fails when the state disagrees with its own dimensions, when a cached
    /// L2 accumulator is not its row's sum of squares, when the geometry needs more hash bits than
    /// the 128-bit key hash carries, when the seed index is past the hasher's
    /// seed list, or when a dimension or the seed index overflows its `u32`
    /// metadata field.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        check_dimensions(self.rows, self.cols).map_err(RmpEncodeError::Syntax)?;
        check_seed_index(self.seed_index, H::seed_list().len()).map_err(RmpEncodeError::Syntax)?;
        let counts = layer_state(self)?;
        let field = |name: &str, value: usize| {
            u32::try_from(value).map_err(|_| {
                RmpEncodeError::Syntax(format!(
                    "ASAPv1 CountSketchWithL2Norm envelope: {name} {value} exceeds the u32 metadata field"
                ))
            })
        };
        let metadata = rmp_serde::to_vec_named(&count_sketch_with_l2_norm_metadata::<H>(
            field("seed_index", self.seed_index)?,
            field("rows", self.rows)?,
            field("cols", self.cols)?,
        ))?;
        let payload = rmp_serde::to_vec(&CountSketchWithL2NormPayload {
            counts: counts.to_vec(),
        })?;
        Ok(envelope::encode(
            kind::COUNT_SKETCH_WITH_L2_NORM,
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. The
    /// dimensions and the seed index are structural (they are properties of
    /// the stored sketch, not of the target), so they are echoed back into the
    /// expected metadata; the hash spec is pinned against this target.
    ///
    /// Reads the current kind `0x19 0x01` and the retired `0x19 0x00`, whose
    /// `l2` is read and dropped; either way each row's accumulator is
    /// recomputed from its counters.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        let carries_l2 = match kind_id {
            kind::COUNT_SKETCH_WITH_L2_NORM => false,
            kind::COUNT_SKETCH_WITH_L2_NORM_CARRYING_L2 => true,
            _ => {
                return Err(RmpDecodeError::Uncategorized(format!(
                    "CountSketchWithL2Norm kind_id mismatch: stored {kind_id:?}, expected {COUNT_SKETCH_WITH_L2_NORM:?} or the retired {COUNT_SKETCH_WITH_L2_NORM_CARRYING_L2:?}"
                )));
            }
        };
        let meta: CountSketchWithL2NormMetadata = envelope::decode_block(metadata)?;
        if meta != count_sketch_with_l2_norm_metadata::<H>(meta.seed_index, meta.rows, meta.cols) {
            return Err(RmpDecodeError::Uncategorized(
                "ASAPv1 CountSketchWithL2Norm envelope: metadata mismatch".to_string(),
            ));
        }
        check_seed_index(meta.seed_index as usize, meta.seed_list.len())
            .map_err(RmpDecodeError::Uncategorized)?;
        let counts = if carries_l2 {
            envelope::decode_block::<CountSketchWithL2NormPayloadCarryingL2>(payload)?.counts
        } else {
            envelope::decode_block::<CountSketchWithL2NormPayload>(payload)?.counts
        };
        rebuild_layer::<H>(
            meta.rows as usize,
            meta.cols as usize,
            meta.seed_index as usize,
            &counts,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, MATRIX_MAX_ROWS, OwnedDataInput,
        RegularPath,
    };
    use rmp_serde::from_slice;

    /// The standard-profile sketch, named so decode calls can pick a hasher.
    type Std = CountSketchWithL2Norm<DefaultXxHasher>;

    fn populated() -> CountSketchWithL2Norm {
        let mut sketch = CountSketchWithL2Norm::with_dimensions_and_seed_index(3, 32, 7);
        for (key, weight) in [("alpha", 9i64), ("beta", 5), ("gamma", -4)] {
            sketch.update_weighted(&DataInput::Str(key), weight);
        }
        sketch
    }

    fn metadata_of(bytes: &[u8]) -> CountSketchWithL2NormMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    fn crafted(meta: &CountSketchWithL2NormMetadata, counts: Vec<i64>) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(meta).expect("metadata");
        let payload = rmp_serde::to_vec(&CountSketchWithL2NormPayload { counts }).expect("payload");
        envelope::encode(kind::COUNT_SKETCH_WITH_L2_NORM, &metadata, &payload)
    }

    /// `sketch`'s bytes under the retired kind `0x19 0x00`, carrying `l2`.
    fn carrying_l2(sketch: &CountSketchWithL2Norm, l2: Vec<i64>) -> Vec<u8> {
        let bytes = sketch.to_asapv1().expect("serialize");
        let (_, metadata, _) = envelope::split(&bytes).expect("split");
        let payload = rmp_serde::to_vec(&CountSketchWithL2NormPayloadCarryingL2 {
            counts: sketch.as_storage().as_slice().to_vec(),
            l2: RetiredL2(l2),
        })
        .expect("payload");
        envelope::encode(
            kind::COUNT_SKETCH_WITH_L2_NORM_CARRYING_L2,
            metadata,
            &payload,
        )
    }

    /// The payload is `[counts]` alone: no `l2` rides the wire.
    #[test]
    fn count_sketch_with_l2_norm_payload_carries_no_l2() {
        let encoded = populated().to_asapv1().expect("serialize");
        let (_, _, payload) = envelope::split(&encoded).expect("split");
        let fields: Vec<serde::de::IgnoredAny> = from_slice(payload).expect("payload array");
        assert_eq!(fields.len(), 1, "payload must be [counts]");
    }

    /// The retired `0x19 0x00` still decodes: its `l2` is read and dropped,
    /// each row's accumulator recomputed from the counters, and the sketch
    /// re-encodes under `0x19 0x01`. A carried `l2` that disagrees with the
    /// counters is not trusted.
    #[test]
    fn count_sketch_with_l2_norm_retired_kind_decodes_and_recomputes_l2() {
        let sketch = populated();
        let true_l2: Vec<i64> = sketch
            .row_l2_squared
            .as_slice()
            .iter()
            .map(|&v| clamp_to_i64(v))
            .collect();
        for l2 in [true_l2, vec![0, 1, 2], vec![-5]] {
            let decoded = Std::from_asapv1(&carrying_l2(&sketch, l2)).expect("decode retired");
            assert_eq!(
                decoded.as_storage().as_slice(),
                sketch.as_storage().as_slice()
            );
            assert_eq!(
                decoded.row_l2_squared.as_slice(),
                sketch.row_l2_squared.as_slice()
            );
            assert_eq!(
                decoded.to_asapv1().expect("re-encode"),
                sketch.to_asapv1().expect("encode")
            );
        }
    }

    /// The encoder rejects a cached accumulator the decoder would not
    /// recompute.
    #[test]
    fn count_sketch_with_l2_norm_rejects_a_stale_l2_cache() {
        let mut sketch = populated();
        sketch.row_l2_squared[0] += 1;
        let problem = sketch
            .to_asapv1()
            .expect_err("a stale cache must not serialize")
            .to_string();
        assert!(problem.contains("row 0"), "got {problem}");
    }

    /// A retired `l2` that is not an integer array is still rejected.
    #[test]
    fn count_sketch_with_l2_norm_retired_kind_rejects_a_non_integer_l2() {
        #[derive(Serialize)]
        struct WithStringL2 {
            counts: Vec<i64>,
            l2: Vec<String>,
        }
        let sketch = populated();
        let bytes = sketch.to_asapv1().expect("serialize");
        let (_, metadata, _) = envelope::split(&bytes).expect("split");
        let payload = rmp_serde::to_vec(&WithStringL2 {
            counts: sketch.as_storage().as_slice().to_vec(),
            l2: vec!["x".to_string(); 3],
        })
        .expect("payload");
        let forged = envelope::encode(
            kind::COUNT_SKETCH_WITH_L2_NORM_CARRYING_L2,
            metadata,
            &payload,
        );
        assert!(Std::from_asapv1(&forged).is_err());
    }

    /// A row whose squares saturate the accumulator keeps it equal to the
    /// recomputed value once a decrement brings the sum back below `i64::MAX`:
    /// the capped-square accumulator never loses the exact sum.
    #[test]
    fn count_sketch_with_l2_norm_l2_stays_derivable_past_saturation() {
        let mut sketch: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(1, 2);
        let delta =
            |col: u32, count: i32| crate::octo_delta::CountSketchCellDelta { row: 0, col, count };
        // Column 0 reaches 2^32, whose square 2^64 saturates the row.
        sketch.apply_delta(delta(0, 1 << 30));
        for _ in 0..3 {
            sketch.apply_delta(delta(0, 1 << 30));
        }
        sketch.apply_delta(delta(1, 1));
        assert_eq!(sketch.l2_squared(), i64::MAX as f64);
        // Back to 2^31: the row's sum of squares is 2^62 + 1.
        sketch.apply_delta(delta(0, -(1 << 30)));
        sketch.apply_delta(delta(0, -(1 << 30)));
        assert_eq!(sketch.row_l2_squared.as_slice(), [(1_i128 << 62) + 1]);
        let decoded = Std::from_asapv1(&sketch.to_asapv1().expect("serialize")).expect("decode");
        assert_eq!(
            decoded.row_l2_squared.as_slice(),
            sketch.row_l2_squared.as_slice()
        );
    }

    /// More rows than the seed list has seeds is outside the wire-eligible
    /// subset, on both sides.
    #[test]
    fn count_sketch_with_l2_norm_rejects_too_many_rows() {
        let rows = MATRIX_MAX_ROWS + 1;
        assert!(
            Std::with_dimensions(rows, 32).to_asapv1().is_err(),
            "a matrix past MATRIX_MAX_ROWS must not serialize"
        );

        let bytes = crafted(
            &count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(0, rows as u32, 32),
            vec![0; rows * 32],
        );
        let problem = Std::from_asapv1(&bytes)
            .expect_err("rows past MATRIX_MAX_ROWS must be rejected")
            .to_string();
        assert!(problem.contains("MATRIX_MAX_ROWS"), "got {problem}");

        // The boundary itself is eligible.
        assert!(
            Std::with_dimensions(MATRIX_MAX_ROWS, 32)
                .to_asapv1()
                .is_ok()
        );
    }

    #[test]
    fn count_sketch_with_l2_norm_round_trip_serialization() {
        let sketch = populated();
        let encoded = sketch.to_asapv1().expect("serialize CountSketchWithL2Norm");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x19, 0x01]); // kind_id_len=2, kind_id=[0x19,0x01]

        let meta = metadata_of(&encoded);
        assert_eq!(meta.metadata_version, 1);
        assert_eq!((meta.seed_index, meta.rows, meta.cols), (7, 3, 32));

        let decoded = Std::from_asapv1(&encoded).expect("deserialize CountSketchWithL2Norm");
        assert_eq!(decoded.rows(), 3);
        assert_eq!(decoded.cols(), 32);
        assert_eq!(decoded.seed_index(), 7);
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
        assert_eq!(
            sketch.row_l2_squared.as_slice(),
            decoded.row_l2_squared.as_slice()
        );
        assert_eq!(sketch.l2(), decoded.l2());
    }

    /// A decoded sketch re-serializes byte-identically.
    #[test]
    fn count_sketch_with_l2_norm_decoded_re_serializes_identically() {
        let sketch = populated();
        let encoded = sketch.to_asapv1().expect("serialize");
        let decoded = Std::from_asapv1(&encoded).expect("decode");
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// Count Sketch cells are signed, so a CountSketchWithL2Norm carries negatives through
    /// unchanged.
    #[test]
    fn count_sketch_with_l2_norm_negative_counters_round_trip() {
        let mut sketch: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(2, 8);
        sketch.update_weighted(&DataInput::U64(11), -25);
        sketch.update_weighted(&DataInput::U64(12), -7);
        assert!(sketch.as_storage().as_slice().iter().any(|&v| v < 0));

        let encoded = sketch.to_asapv1().expect("serialize");
        let decoded = Std::from_asapv1(&encoded).expect("decode");
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
        assert!(decoded.as_storage().as_slice().iter().any(|&v| v < 0));
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// The seed index is state, not a profile constant: two sketches that
    /// differ only in it do not share bytes and do not decode into each other.
    #[test]
    fn count_sketch_with_l2_norm_seed_index_travels_with_the_sketch() {
        let zero: CountSketchWithL2Norm =
            CountSketchWithL2Norm::with_dimensions_and_seed_index(2, 8, 0);
        let nine: CountSketchWithL2Norm =
            CountSketchWithL2Norm::with_dimensions_and_seed_index(2, 8, 9);
        let zero_bytes = zero.to_asapv1().expect("serialize");
        let nine_bytes = nine.to_asapv1().expect("serialize");
        assert_ne!(zero_bytes, nine_bytes);
        assert_eq!(
            Std::from_asapv1(&nine_bytes).expect("decode").seed_index(),
            9
        );
    }

    // A test-only custom hasher: hashes exactly like `DefaultXxHasher` but
    // declares a DIFFERENT `HashProfile`.
    #[derive(Clone, Debug)]
    struct AltHasher;

    impl SketchHasher for AltHasher {
        type HashType = <DefaultXxHasher as SketchHasher>::HashType;

        fn hash64_seeded(seed_index: usize, key: &DataInput) -> u64 {
            DefaultXxHasher::hash64_seeded(seed_index, key)
        }
        fn hash128_seeded(seed_index: usize, key: &DataInput) -> u128 {
            DefaultXxHasher::hash128_seeded(seed_index, key)
        }
        fn hash64_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u64 {
            DefaultXxHasher::hash64_seeded_owned(seed_index, key)
        }
        fn hash128_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u128 {
            DefaultXxHasher::hash128_seeded_owned(seed_index, key)
        }
        fn hash_for_matrix_seeded(
            seed_index: usize,
            rows: usize,
            cols: usize,
            key: &DataInput,
        ) -> Self::HashType {
            DefaultXxHasher::hash_for_matrix_seeded(seed_index, rows, cols, key)
        }
    }

    impl HashProfile for AltHasher {
        const PROFILE_ID: &'static str = "test.alt.profile.v1";
        const ALGORITHM: &'static str = "xxh3_64_128";
        const SEED_DERIVATION: &'static str = "seed_list_index_scaled";
        const INPUT_ENCODING: &'static str = "projectasap.input.v1";
        fn seed_list() -> Vec<u64> {
            vec![1, 2, 3, 4, 5]
        }
        const CANONICAL_SEED_INDEX: u32 = DEFAULT_CANONICAL_SEED_INDEX as u32;
        const MATRIX_SEED_INDEX: u32 = 0;
    }

    #[test]
    fn count_sketch_with_l2_norm_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) A CountSketchWithL2Norm built with a custom-profile hasher round-trips.
        let mut alt = CountSketchWithL2Norm::<AltHasher>::with_dimensions(3, 32);
        let mut std: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        alt.update_weighted(&DataInput::U64(42), 5);
        std.update_weighted(&DataInput::U64(42), 5);

        let alt_bytes = alt.to_asapv1().expect("alt serialize");
        let decoded =
            CountSketchWithL2Norm::<AltHasher>::from_asapv1(&alt_bytes).expect("alt decode");
        assert_eq!(alt.as_storage().as_slice(), decoded.as_storage().as_slice());

        // (b) Bytes differ from the standard-profile sketch.
        let std_bytes = std.to_asapv1().expect("std serialize");
        assert_ne!(alt_bytes, std_bytes);

        // (c) Standard-profile decode fails closed on custom-profile bytes.
        assert!(
            Std::from_asapv1(&alt_bytes).is_err(),
            "standard-profile decode must reject custom-profile bytes"
        );
    }

    /// Each family's envelope is rejected by the other three, and by a plain
    /// Count Sketch envelope.
    #[test]
    fn count_sketch_with_l2_norm_rejects_foreign_kind_ids() {
        let count_sketch = crate::CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8)
            .to_asapv1()
            .expect("serialize Count Sketch");
        let univmon = crate::UnivMon::new(4, 2, 8, 2)
            .to_asapv1()
            .expect("serialize UnivMon");
        let pyramid = crate::UnivMonTwoTier::new(4, 1, 2, 8, 2, 4, 2)
            .to_asapv1()
            .expect("serialize UnivMonTwoTier");
        let univmon_q = crate::UnivMonQ::new(crate::UnivMonQConfig {
            layers: 2,
            top_layer_cols: 8,
            rows: 3,
            heap_size: 4,
            ordered_samples: 4,
            ..Default::default()
        })
        .expect("config")
        .to_asapv1()
        .expect("serialize UnivMonQ");

        for foreign in [count_sketch, univmon, pyramid, univmon_q] {
            assert!(
                Std::from_asapv1(&foreign).is_err(),
                "a foreign envelope must not decode as a CountSketchWithL2Norm"
            );
        }
    }

    /// Fail closed (not panic) on a crafted envelope carrying a zero
    /// dimension, and on one whose declared geometry the payload does not
    /// carry. Both checks precede any allocation.
    #[test]
    fn count_sketch_with_l2_norm_rejects_crafted_geometry() {
        let meta = count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(0, 4, 0);
        assert!(Std::from_asapv1(&crafted(&meta, Vec::new())).is_err());

        let huge = count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(0, 1, 1 << 24);
        assert!(
            Std::from_asapv1(&crafted(&huge, vec![1, 2, 3])).is_err(),
            "counts length must match rows*cols"
        );
    }

    /// An unpopulated matrix carries dimensions its cells do not match.
    /// Serializing it must fail rather than emit bytes the decoder refuses.
    #[test]
    fn count_sketch_with_l2_norm_rejects_serializing_an_unfilled_matrix() {
        let mut sketch: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(2, 4);
        sketch.counts = Vector2D::unfilled(2, 4);
        assert!(
            sketch.to_asapv1().is_err(),
            "a matrix whose cell count disagrees with its dimensions must not serialize"
        );
    }

    /// Fail closed on an unexpected metadata key, and on a missing required
    /// one.
    #[test]
    fn count_sketch_with_l2_norm_metadata_rejects_unknown_and_missing_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            seed_index: u32,
            rows: u32,
            cols: u32,
            bogus_field: u8, // key not in CountSketchWithL2NormMetadata
        }
        #[derive(Serialize)]
        struct WithoutCols {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            seed_index: u32,
            rows: u32,
        }
        let m = count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(0, 2, 4);
        let extra = WithExtra {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            seed_index: m.seed_index,
            rows: m.rows,
            cols: m.cols,
            bogus_field: 7,
        };
        let without = WithoutCols {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            seed_index: m.seed_index,
            rows: m.rows,
        };
        assert!(
            from_slice::<CountSketchWithL2NormMetadata>(&rmp_serde::to_vec_named(&extra).unwrap())
                .is_err(),
            "an unknown metadata key must be rejected"
        );
        assert!(
            from_slice::<CountSketchWithL2NormMetadata>(
                &rmp_serde::to_vec_named(&without).unwrap()
            )
            .is_err(),
            "a missing required key must be rejected"
        );
    }

    /// A sketch past the hash budget, built without the constructor's check,
    /// as a struct deserialized by another route could be.
    fn unchecked(rows: usize, cols: usize) -> Std {
        CountSketchWithL2Norm {
            counts: Vector2D::unfilled(rows, cols),
            row_l2_squared: Vector1D::filled(rows, 0),
            rows,
            cols,
            seed_index: 0,
            _hasher: PhantomData,
        }
    }

    /// One 128-bit key hash gives each row `mask_bits + 1` bits, so a geometry
    /// needing more than 128 of them is not a sketch either door may pass.
    /// `rows = 12, cols = 4096` and `rows = 13, cols = 2048` both need 156:
    /// their last row shifts the hash by 132, past the 127-bit ceiling.
    #[test]
    fn count_sketch_with_l2_norm_rejects_a_geometry_past_the_hash_budget() {
        for (rows, cols) in [(12usize, 4096usize), (13, 2048)] {
            let problem = unchecked(rows, cols)
                .to_asapv1()
                .expect_err("a geometry past the hash budget must not serialize")
                .to_string();
            assert!(problem.contains("hash bits"), "got {problem}");

            let bytes = crafted(
                &count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(0, rows as u32, cols as u32),
                vec![0; rows * cols],
            );
            let problem = Std::from_asapv1(&bytes)
                .expect_err("a geometry past the hash budget must not decode")
                .to_string();
            assert!(problem.contains("hash bits"), "got {problem}");
        }
    }

    /// The same budget covers the case that does not panic: 10 rows of 4096
    /// columns fill bits 0..119 with column fields while the sign bits start at
    /// 118, so two rows would read a column out of bits that are already signs.
    #[test]
    fn count_sketch_with_l2_norm_rejects_column_fields_overlapping_the_sign_bits() {
        assert!(
            unchecked(10, 4096).to_asapv1().is_err(),
            "column fields must not reach into the sign bits"
        );
        // The widest geometry each column count still admits.
        assert!(Std::with_dimensions(9, 4096).to_asapv1().is_ok());
        assert!(Std::with_dimensions(10, 2048).to_asapv1().is_ok());
        assert!(
            unchecked(11, 2048).to_asapv1().is_err(),
            "column fields must not reach into the sign bits"
        );
    }

    /// The wire carries seed indexes below the seed list's length; neither
    /// door accepts one past it.
    #[test]
    fn count_sketch_with_l2_norm_rejects_a_seed_index_past_the_seed_list() {
        let seeds = <DefaultXxHasher as HashProfile>::seed_list().len();
        let past = seeds + 5;

        let problem =
            CountSketchWithL2Norm::<DefaultXxHasher>::with_dimensions_and_seed_index(2, 8, past)
                .to_asapv1()
                .expect_err("a seed index past the seed list must not serialize")
                .to_string();
        assert!(problem.contains("seed_index"), "got {problem}");

        let bytes = crafted(
            &count_sketch_with_l2_norm_metadata::<DefaultXxHasher>(past as u32, 2, 8),
            vec![0; 16],
        );
        let problem = Std::from_asapv1(&bytes)
            .expect_err("a seed index past the seed list must not decode")
            .to_string();
        assert!(problem.contains("seed_index"), "got {problem}");

        // The last in-range index is eligible.
        assert!(
            CountSketchWithL2Norm::<DefaultXxHasher>::with_dimensions_and_seed_index(
                2,
                8,
                seeds - 1
            )
            .to_asapv1()
            .is_ok()
        );
    }

    /// An empty sketch has exactly one encoding.
    #[test]
    fn count_sketch_with_l2_norm_empty_has_one_encoding() {
        let left: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        let mut right: CountSketchWithL2Norm = CountSketchWithL2Norm::with_dimensions(3, 32);
        right.update_weighted(&DataInput::U64(5), 4);
        right.clear();
        assert_eq!(
            left.to_asapv1().expect("serialize"),
            right.to_asapv1().expect("serialize")
        );
    }

    /// Known counts and seed index serialize to the `asapv1_golden/` bytes.
    /// Row 0's squares sum to 5368610562; row 1's saturate at `i64::MAX`.
    #[test]
    fn known_state_matches_golden() {
        use crate::asapv1::envelope::hex;
        use std::marker::PhantomData;

        const COUNTS: [[i64; 4]; 2] =
            [[127, 128, 65535, -32768], [-32, -33, -2147483648, i64::MIN]];
        let sketch: CountSketchWithL2Norm = CountSketchWithL2Norm {
            counts: Vector2D::from_fn(2, 4, |r, c| COUNTS[r][c]),
            row_l2_squared: Vector1D::from_vec(
                COUNTS
                    .iter()
                    .map(|row| row_capped_square_sum(row))
                    .collect(),
            ),
            rows: 2,
            cols: 4,
            seed_index: 7,
            _hasher: PhantomData,
        };
        let golden = hex(include_str!(
            "../../../asapv1_golden/count_l2hh_2x4_seed7_no_l2.hex"
        ));
        assert_eq!(sketch.to_asapv1().unwrap(), golden);

        // The retired fixture carries `l2 = [65536, i64::MAX]`, which is not
        // row 0's sum of squares; decoding drops it and recomputes.
        let retired = hex(include_str!(
            "../../../asapv1_golden/count_l2hh_2x4_seed7.hex"
        ));
        let decoded = Std::from_asapv1(&retired).expect("decode retired");
        assert_eq!(
            decoded.row_l2_squared.as_slice(),
            sketch.row_l2_squared.as_slice()
        );
        let l2: Vec<i64> = decoded
            .row_l2_squared
            .as_slice()
            .iter()
            .map(|&v| clamp_to_i64(v))
            .collect();
        assert_eq!(l2, [5_368_610_562, i64::MAX]);
        assert_eq!(decoded.to_asapv1().unwrap(), golden);
    }
}
