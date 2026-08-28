//! ASAPv1 wire serialization for the Count Sketch.
//!
//! Child submodule of [`crate::sketches::countsketch`]: it holds ALL of Count
//! Sketch's serialization (the metadata/payload DTOs, the kind_id constant, the
//! [`CsWireMode`] marker trait, and the `serialize_to_bytes` /
//! `deserialize_from_bytes` impls) while the algorithm lives in the parent
//! module file. Being a descendant module, it reads the sketch's private
//! `counts` field directly without widening any field visibility. See
//! `docs/asapv1_wire_format.md` §3.6.
//!
//! Count Sketch is one algorithm — a single kind_id `0x04 0x00`. The structural
//! parameters — the matrix dimensions (`rows` / `cols`) and the
//! column-derivation **mode** (fast/regular) — live in the metadata, so the
//! payload itself is just `[counts]` (a 1-element array mirroring Count-Min's).
//!
//! Unlike Count-Min, there is no `counter_type` metadata key: Count Sketch
//! counters must be signed and negatable ([`CountSketchCounter`] requires
//! `Neg` + `From<i32>`), which leaves `i64` as the only wire-eligible type, so
//! the kind_id already implies it. Exotic in-memory counters (i32/i128/…) must
//! be converted to `i64` first.
//!
//! [`CountSketchCounter`]: crate::sketches::countsketch::CountSketchCounter

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::{FastPath, HashProfile, RegularPath, SketchHasher, Vector2D};

use super::Count;

/// Count Sketch kind_id: family `0x04`, single algorithm variant `0x00`.
const CS_KIND: &[u8] = &[0x04, 0x00];

/// Names the wire column-derivation mode carried in the metadata (`mode`).
pub trait CsWireMode {
    /// Metadata `mode` string — `"fast"` or `"regular"`.
    const MODE: &'static str;
}
impl CsWireMode for RegularPath {
    const MODE: &'static str = "regular";
}
impl CsWireMode for FastPath {
    const MODE: &'static str = "fast";
}

/// Count Sketch descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it). Hash-spec fields first, then the
/// structural params `rows` / `cols` / `mode`. Per the spec's config→metadata
/// rule, the matrix dimensions are configuration (like HLL's `precision`) and so
/// live here rather than in the payload.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsMetadata {
    metadata_version: u8,
    hash_profile_id: String,
    hash_algorithm: String,
    seed_derivation: String,
    input_encoding: String,
    seed_list: Vec<u64>,
    matrix_seed_index: u32,
    rows: u32,
    cols: u32,
    mode: String,
}

/// Builds the Count Sketch descriptor metadata from the hasher's
/// [`HashProfile`], so the wire bytes truthfully describe how the sketch was
/// hashed (rather than hardcoding the standard profile). `matrix_seed_index` is
/// the profile's own row seed index; `rows` / `cols` are the sketch's structural
/// dimensions.
fn cs_metadata<H: HashProfile>(rows: u32, cols: u32, mode: &str) -> CsMetadata {
    CsMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        matrix_seed_index: H::MATRIX_SEED_INDEX,
        rows,
        cols,
        mode: mode.to_string(),
    }
}

/// Count Sketch payload (ASAPv1 §3.6), a msgpack **array** (`to_vec`,
/// positional): `[counts]` — a 1-element array. The dimensions live in the
/// metadata; `counts` is packed row-major and its elements are `i64`. Cells are
/// signed: Count Sketch adds `±weight`, so a counter may be negative.
#[derive(Debug, Serialize, Deserialize)]
struct CsPayload {
    counts: Vec<i64>,
}

// Wire serialization for the canonical Count Sketch configs only. `wire` is a
// descendant of the sketch module, so this impl reads the private `counts`
// field directly.
impl<Mode, H> Count<Vector2D<i64>, Mode, H>
where
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Serializes the sketch into an ASAPv1 MessagePack envelope. The metadata is
    /// derived from the hasher's [`HashProfile`], so it truthfully describes how
    /// the sketch was hashed.
    ///
    /// A matrix whose cell count disagrees with its own dimensions is an error
    /// rather than bytes that would be refused on decode.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let rows = self.counts.rows();
        let cols = self.counts.cols();
        let counts = self.counts.as_slice();
        if counts.len() != rows.saturating_mul(cols) {
            return Err(RmpEncodeError::Syntax(format!(
                "ASAPv1 Count Sketch envelope: counts length {} != rows*cols {}",
                counts.len(),
                rows.saturating_mul(cols)
            )));
        }
        let metadata =
            rmp_serde::to_vec_named(&cs_metadata::<H>(rows as u32, cols as u32, Mode::MODE))?;
        let payload = rmp_serde::to_vec(&CsPayload {
            counts: counts.to_vec(),
        })?;
        Ok(envelope::encode(CS_KIND, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. The matrix
    /// dimensions are read from the (validated) metadata; the payload carries
    /// only the counts.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != CS_KIND {
            return Err(RmpDecodeError::Uncategorized(format!(
                "Count Sketch kind_id mismatch: stored {kind_id:?}, expected {CS_KIND:?}"
            )));
        }
        let meta: CsMetadata = from_slice(metadata)?;
        // Validate the hash spec + mode against this target; `rows`/`cols` are
        // structural (the sketch is dynamically sized), so they are echoed back
        // into the expected block rather than known a priori.
        if meta != cs_metadata::<H>(meta.rows, meta.cols, Mode::MODE) {
            return Err(RmpDecodeError::Uncategorized(
                "ASAPv1 Count Sketch envelope: metadata mismatch".to_string(),
            ));
        }
        let (rows, cols) = (meta.rows as usize, meta.cols as usize);
        let p: CsPayload = from_slice(payload)?;
        // Reject zero dimensions before building the matrix: `Vector2D::from_fn`
        // derives its mask via `cols.ilog2()`, which panics on `cols == 0`. Fail
        // closed with an error rather than panicking on crafted bytes.
        if rows == 0 || cols == 0 {
            return Err(RmpDecodeError::Uncategorized(format!(
                "Count Sketch dimensions must be non-zero: rows={rows}, cols={cols}"
            )));
        }
        // Length check precedes the allocation, so crafted dimensions cannot
        // drive `from_fn` into a huge reserve: the payload must actually carry
        // `rows*cols` decoded counters.
        if p.counts.len() != rows.saturating_mul(cols) {
            return Err(RmpDecodeError::Uncategorized(format!(
                "Count Sketch counts length {} != rows*cols {}",
                p.counts.len(),
                rows.saturating_mul(cols)
            )));
        }
        let storage = Vector2D::from_fn(rows, cols, |r, c| p.counts[r * cols + c]);
        Ok(Count::from_storage(storage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CANONICAL_HASH_SEED, DataInput, DefaultXxHasher};

    #[test]
    fn count_sketch_round_trip_serialization() {
        let mut sketch = Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);
        sketch.insert(&DataInput::U64(42));
        sketch.insert(&DataInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize Count");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x04, 0x00]); // kind_id_len=2, kind_id=[0x04,0x00]

        let decoded = Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&encoded)
            .expect("deserialize Count");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }

    /// Count Sketch cells are signed — the payload must carry negatives through
    /// unchanged (the one shape difference from Count-Min's monotonic counters).
    #[test]
    fn count_sketch_negative_counters_round_trip() {
        let sketch =
            Count::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
                let v = (r * 4 + c) as i64;
                if v % 2 == 0 { v } else { -v }
            }));
        let encoded = sketch.serialize_to_bytes().expect("serialize");
        let decoded =
            Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&encoded).expect("decode");
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
        assert!(decoded.as_storage().as_slice().iter().any(|&v| v < 0));
    }

    // A test-only custom hasher: hashes exactly like `DefaultXxHasher` but
    // declares a DIFFERENT `HashProfile`. Count Sketch metadata is derived from
    // the profile, so an `AltHasher` sketch serializes truthfully. (An
    // *unprofiled* hasher cannot serialize at all — that is a compile-time
    // guarantee, since the wire methods are bounded on `H: HashProfile`.)
    #[derive(Clone, Debug)]
    struct AltHasher;

    impl SketchHasher for AltHasher {
        type HashType = <DefaultXxHasher as SketchHasher>::HashType;

        fn hash64_seeded(d: usize, key: &DataInput) -> u64 {
            DefaultXxHasher::hash64_seeded(d, key)
        }
        fn hash128_seeded(d: usize, key: &DataInput) -> u128 {
            DefaultXxHasher::hash128_seeded(d, key)
        }
        fn hash_item64_seeded(d: usize, key: &crate::HeapItem) -> u64 {
            DefaultXxHasher::hash_item64_seeded(d, key)
        }
        fn hash_item128_seeded(d: usize, key: &crate::HeapItem) -> u128 {
            DefaultXxHasher::hash_item128_seeded(d, key)
        }
        fn hash_for_matrix_seeded(
            seed_idx: usize,
            rows: usize,
            cols: usize,
            key: &DataInput,
        ) -> Self::HashType {
            DefaultXxHasher::hash_for_matrix_seeded(seed_idx, rows, cols, key)
        }
    }

    impl HashProfile for AltHasher {
        const PROFILE_ID: &'static str = "test.alt.profile.v1";
        const ALGORITHM: &'static str = "xxh3_64_128";
        const SEED_DERIVATION: &'static str = "seed_list_index_wrap";
        const INPUT_ENCODING: &'static str = "projectasap.input.v1";
        fn seed_list() -> Vec<u64> {
            vec![1, 2, 3, 4, 5]
        }
        const CANONICAL_SEED_INDEX: u32 = CANONICAL_HASH_SEED as u32;
        const MATRIX_SEED_INDEX: u32 = 0;
    }

    #[test]
    fn count_sketch_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) A Count Sketch built with a custom-profile hasher round-trips.
        let mut alt = Count::<Vector2D<i64>, RegularPath, AltHasher>::with_dimensions(3, 8);
        let mut std = Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);
        alt.insert(&DataInput::U64(42));
        alt.insert(&DataInput::U64(7));
        std.insert(&DataInput::U64(42));
        std.insert(&DataInput::U64(7));

        let alt_bytes = alt.serialize_to_bytes().expect("alt serialize");
        let decoded =
            Count::<Vector2D<i64>, RegularPath, AltHasher>::deserialize_from_bytes(&alt_bytes)
                .expect("alt decode");
        assert_eq!(alt.as_storage().as_slice(), decoded.as_storage().as_slice());

        // (b) Bytes differ from the standard-profile sketch (metadata derived
        // from the different profile).
        let std_bytes = std.serialize_to_bytes().expect("std serialize");
        assert_ne!(alt_bytes, std_bytes);

        // (c) Standard-profile decode fails closed on custom-profile bytes.
        assert!(
            Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&alt_bytes).is_err(),
            "standard-profile decode must reject custom-profile bytes"
        );
    }

    #[test]
    fn count_sketch_mode_in_metadata_round_trips() {
        let mut sketch = Count::<Vector2D<i64>, FastPath>::with_dimensions(4, 16);
        sketch.insert_many(&DataInput::U64(1), 5);
        sketch.insert_many(&DataInput::U64(2), 3);

        let encoded = sketch.serialize_to_bytes().expect("serialize");
        let decoded = Count::<Vector2D<i64>, FastPath>::deserialize_from_bytes(&encoded)
            .expect("deserialize");
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );

        // Mode is pinned by the target: a fast payload must not decode into a
        // regular sketch (metadata mismatch).
        assert!(Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&encoded).is_err());
    }

    /// Count Sketch and Count-Min share the payload shape but not the kind_id;
    /// a CMS envelope must not decode as a Count Sketch.
    #[test]
    fn count_sketch_rejects_foreign_kind_id() {
        let cms = crate::CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);
        let cms_bytes = cms.serialize_to_bytes().expect("serialize CMS");
        assert!(
            Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&cms_bytes).is_err(),
            "CMS bytes must not decode as a Count Sketch"
        );
    }

    /// Fail closed (not panic) on a crafted envelope with a zero dimension:
    /// valid envelope + valid metadata that carries `cols == 0`, with an empty
    /// `[counts]` payload. `Vector2D::from_fn` derives its mask via
    /// `cols.ilog2()`, which panics on `cols == 0`.
    #[test]
    fn count_sketch_rejects_zero_dimension_payload() {
        let metadata =
            rmp_serde::to_vec_named(&cs_metadata::<DefaultXxHasher>(4, 0, "regular")).unwrap();
        let payload = rmp_serde::to_vec(&CsPayload { counts: Vec::new() }).unwrap();
        let bytes = envelope::encode(CS_KIND, &metadata, &payload);
        assert!(
            Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).is_err(),
            "zero-dimension metadata must be rejected, not panic"
        );
    }

    /// Crafted dimensions must not drive a huge allocation: the length check
    /// runs before `Vector2D::from_fn`.
    #[test]
    fn count_sketch_rejects_dimension_length_mismatch() {
        let metadata =
            rmp_serde::to_vec_named(&cs_metadata::<DefaultXxHasher>(1024, 1024, "regular"))
                .unwrap();
        let payload = rmp_serde::to_vec(&CsPayload {
            counts: vec![1, 2, 3],
        })
        .unwrap();
        let bytes = envelope::encode(CS_KIND, &metadata, &payload);
        assert!(
            Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).is_err(),
            "counts length must match rows*cols"
        );
    }

    /// An unpopulated matrix carries dimensions its cells do not match.
    /// Serializing it must fail rather than emit bytes the decoder refuses.
    #[test]
    fn count_sketch_rejects_serializing_an_unfilled_matrix() {
        let sketch = Count::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::<i64>::init(2, 4));
        assert!(
            sketch.serialize_to_bytes().is_err(),
            "a matrix whose cell count disagrees with its dimensions must not serialize"
        );
    }

    /// Fail closed on an unexpected metadata key (mirrors the CMS/HLL tests).
    #[test]
    fn cs_metadata_rejects_unknown_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            matrix_seed_index: u32,
            rows: u32,
            cols: u32,
            mode: String,
            bogus_field: u8, // key not in CsMetadata
        }
        let m = cs_metadata::<DefaultXxHasher>(2, 3, "regular");
        let extra = WithExtra {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            matrix_seed_index: m.matrix_seed_index,
            rows: m.rows,
            cols: m.cols,
            mode: m.mode.clone(),
            bogus_field: 7,
        };
        let bytes = rmp_serde::to_vec_named(&extra).unwrap();
        assert!(rmp_serde::from_slice::<CsMetadata>(&bytes).is_err());
    }

    /// A Count Sketch metadata map without the CMS-only `counter_type` key is
    /// the contract; adding it back must be rejected.
    #[test]
    fn cs_metadata_rejects_counter_type_key() {
        #[derive(Serialize)]
        struct WithCounterType {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            matrix_seed_index: u32,
            rows: u32,
            cols: u32,
            counter_type: String, // CMS-only; Count Sketch's kind_id implies i64
            mode: String,
        }
        let m = cs_metadata::<DefaultXxHasher>(2, 3, "regular");
        let extra = WithCounterType {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            matrix_seed_index: m.matrix_seed_index,
            rows: m.rows,
            cols: m.cols,
            counter_type: "i64".to_string(),
            mode: m.mode.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&extra).unwrap();
        assert!(rmp_serde::from_slice::<CsMetadata>(&bytes).is_err());
    }
}
