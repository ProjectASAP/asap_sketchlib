//! ASAPv1 wire serialization for CountMinSketchWithHeap.
//!
//! Child submodule of [`crate::sketches::countminsketch_heap`]: it holds the
//! kind_id constant and the `to_asapv1` / `from_asapv1`
//! impls while the algorithm lives in the parent module file. Being a
//! descendant module, it reads the sketch's private `cms` / `heap` fields
//! directly without widening any field visibility. The metadata DTO, the
//! payload DTO and the heap encoding are shared with CountSketchWithHeap and live in
//! [`super::heap_wire`].
//!
//! CountMinSketchWithHeap is one algorithm — a single kind_id `0x03 0x00`. The structural
//! parameters — the matrix dimensions (`rows` / `cols`), the base **counter
//! type** (i32/i64/f64), the column-derivation **mode** (fast/regular), the heap
//! capacity `heap_size` and the heap's `key_type` — all live in the metadata, so
//! the payload is `[counts, keys, heap_counts]`: the base matrix packed row-major
//! followed by the heap's entries, whose counts are `f64` under `f64` counters
//! and `i64` otherwise.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::message_pack_format::envelope;
use crate::message_pack_format::kind;
use crate::message_pack_format::kind::COUNT_MIN_WITH_HEAP;
use crate::sketches::countminsketch::{CmsWireCounter, CmsWireMode};
use crate::{CountMinSketch, HashProfile, HeapCounter, SketchHasher, Vector2D};

use super::CountMinSketchWithHeap;
use super::heap_wire::{
    decode_metadata, decode_payload, encode_metadata, encode_payload, heap_entries, rebuild_heap,
    wire_key_type,
};

// Wire serialization for the canonical CountMinSketchWithHeap configs only. `wire` is a
// descendant of the sketch module, so this impl reads the private fields
// directly.
impl<T, Mode, H> CountMinSketchWithHeap<Vector2D<T>, Mode, H>
where
    // `AddAssign` is required for `Vector2D<T>: MatrixStorage` (the struct's
    // bound), not by the bodies below.
    T: CmsWireCounter + HeapCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x03 0x00`). The metadata is derived from the hasher's
    /// [`HashProfile`], so it truthfully describes how the sketch was hashed.
    ///
    /// Fails when the matrix has a zero dimension, more than `MATRIX_MAX_ROWS`
    /// rows or a cell count that disagrees with its own dimensions, when the
    /// heap's keys mix `OwnedDataInput` variants or hold a 128-bit key, or when
    /// `heap_size` or `cols` overflows the metadata's `u32` field.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        admission_rate::check_admits_all(self.count_min_sketch.admission_rate())
            .map_err(|e| RmpEncodeError::Syntax(format!("ASAPv1 CMSHeap envelope: {e}")))?;
        let rows = self.count_min_sketch.rows();
        let cols = self.count_min_sketch.cols();
        let counts = self.count_min_sketch.as_storage().as_slice();
        let entries = heap_entries(&self.heap);
        let key_type = wire_key_type(&entries)?;
        let metadata = encode_metadata::<H>(
            "CMSHeap",
            rows,
            cols,
            T::COUNTER_TYPE,
            Mode::MODE,
            self.heap.capacity(),
            key_type,
        )?;
        if counts.len() != rows.saturating_mul(cols) {
            return Err(RmpEncodeError::Syntax(format!(
                "ASAPv1 CMSHeap envelope: counts length {} != rows*cols {}",
                counts.len(),
                rows.saturating_mul(cols)
            )));
        }
        let payload = encode_payload(key_type, counts.to_vec(), &entries)?;
        Ok(envelope::encode(
            kind::COUNT_MIN_WITH_HEAP,
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. The matrix
    /// dimensions, `heap_size` and `key_type` are structural (they are
    /// properties of the stored sketch, not of the target), so they are echoed
    /// back into the expected metadata; the hash spec, counter type and mode are
    /// pinned against this target.
    ///
    /// Every state the algorithm could not have produced is rejected with an
    /// error rather than a panic, and neither the declared geometry nor
    /// `heap_size` sizes an allocation before the payload is measured against it.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::COUNT_MIN_WITH_HEAP {
            return Err(RmpDecodeError::Uncategorized(format!(
                "CMSHeap kind_id mismatch: stored {kind_id:?}, expected {COUNT_MIN_WITH_HEAP:?}"
            )));
        }
        let shape = decode_metadata::<H>("CMSHeap", metadata, T::COUNTER_TYPE, Mode::MODE)?;
        let (rows, cols) = (shape.rows, shape.cols);
        let (counts, entries) = decode_payload::<Vec<T>, _>(&shape.key_type, payload)?;
        // Length check precedes the allocation, so crafted dimensions cannot
        // drive `from_fn` into a huge reserve.
        if counts.len() != rows.saturating_mul(cols) {
            return Err(RmpDecodeError::Uncategorized(format!(
                "CMSHeap counts length {} != rows*cols {}",
                counts.len(),
                rows.saturating_mul(cols)
            )));
        }
        let heap = rebuild_heap(shape.heap_size, entries)?;
        let storage = Vector2D::from_fn(rows, cols, |r, c| counts[r * cols + c]);
        Ok(CountMinSketchWithHeap {
            count_min_sketch: CountMinSketch::from_storage(storage),
            heap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::countminsketch_heap::heap_wire::{
        HeapMetadata, HeapPayload, heap_metadata,
    };
    use crate::{
        DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, FastPath, MATRIX_MAX_ROWS,
        RegularPath,
    };
    use rmp_serde::from_slice;

    fn populated() -> CountMinSketchWithHeap<Vector2D<i64>, RegularPath> {
        let mut sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 8, 4);
        for (key, weight) in [(1u64, 9i64), (2, 5), (3, 7)] {
            sketch.update_weighted(&DataInput::U64(key), weight);
        }
        sketch
    }

    /// The complaint a crafted envelope draws, with no `Debug` bound on the
    /// sketch.
    fn decode_error(bytes: &[u8]) -> String {
        match CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(bytes) {
            Ok(_) => panic!("a crafted envelope must be rejected, not decoded"),
            Err(err) => err.to_string(),
        }
    }

    fn metadata_of(bytes: &[u8]) -> HeapMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    #[test]
    fn cms_heap_round_trip_serialization() {
        let sketch = populated();
        let encoded = sketch.to_asapv1().expect("serialize CMSHeap");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x03, 0x00]); // kind_id_len=2, kind_id=[0x03,0x00]

        let meta = metadata_of(&encoded);
        assert_eq!(meta.metadata_version, 1);
        assert_eq!((meta.rows, meta.cols), (3, 8));
        assert_eq!(meta.counter_type, "i64");
        assert_eq!(meta.mode, "regular");
        assert_eq!(meta.heap_size, 4);
        assert_eq!(meta.key_type, "u64");

        let decoded = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&encoded)
            .expect("deserialize CMSHeap");
        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.count_min_sketch().as_storage().as_slice(),
            decoded.count_min_sketch().as_storage().as_slice()
        );
        assert_eq!(decoded.heap().len(), 3);
        assert_eq!(decoded.heap().capacity(), 4);
        for key in 1..=3u64 {
            let probe = DataInput::U64(key);
            assert_eq!(decoded.estimate(&probe), sketch.estimate(&probe));
            let seat = decoded.heap().find(&probe).expect("heap key");
            assert_eq!(
                decoded.heap().heap()[seat].count,
                sketch.heap().heap()[sketch.heap().find(&probe).expect("heap key")].count
            );
        }
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// The `f64` base counter is a Count-Min wire type, so a float-counter
    /// CountMinSketchWithHeap serializes as well.
    #[test]
    fn cms_heap_f64_counters_round_trip() {
        let mut sketch = CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 4, |r, c| (r * 4 + c) as f64 + 0.5),
            4,
        );
        sketch.heap_mut().update(&DataInput::Str("flow"), 11.0);

        let encoded = sketch.to_asapv1().expect("serialize");
        assert_eq!(metadata_of(&encoded).counter_type, "f64");
        assert_eq!(metadata_of(&encoded).key_type, "string");

        let decoded = CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::from_asapv1(&encoded)
            .expect("decode");
        assert_eq!(
            sketch.count_min_sketch().as_storage().as_slice(),
            decoded.count_min_sketch().as_storage().as_slice()
        );
        assert!(decoded.heap().find(&DataInput::Str("flow")).is_some());
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// A heap key that is a raw byte array reaches the wire as msgpack `bin`,
    /// so a key that is not UTF-8 round trips and still answers its own
    /// `DataInput::Bytes`.
    #[test]
    fn cms_heap_byte_keys_round_trip() {
        let raw: &[u8] = &[0xff, 0x00, 0xfe];
        let mut sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 4, |r, c| (r * 4 + c) as i64),
            4,
        );
        sketch.heap_mut().update(&DataInput::Bytes(raw), 11);

        let encoded = sketch.to_asapv1().expect("serialize");
        assert_eq!(metadata_of(&encoded).key_type, "bytes");

        let decoded = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&encoded)
            .expect("decode");
        let seat = decoded
            .heap()
            .find(&DataInput::Bytes(raw))
            .expect("the byte key");
        assert_eq!(decoded.heap().heap()[seat].count, 11);
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// The base counter type is pinned by the target: i64 bytes must not decode
    /// into an f64 sketch or the reverse.
    #[test]
    fn cms_heap_counter_type_is_pinned_by_the_target() {
        let wide = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 4, |r, c| (r * 4 + c) as i64),
            4,
        );
        let floating = CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 4, |r, c| (r * 4 + c) as f64),
            4,
        );
        let wide_bytes = wide.to_asapv1().expect("serialize i64");
        let floating_bytes = floating.to_asapv1().expect("serialize f64");
        assert_ne!(wide_bytes, floating_bytes);
        assert!(
            CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::from_asapv1(&wide_bytes).is_err(),
            "i64 bytes must not decode as an f64 sketch"
        );
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&floating_bytes)
                .is_err(),
            "f64 bytes must not decode as an i64 sketch"
        );
    }

    #[test]
    fn cms_heap_mode_in_metadata_round_trips() {
        let mut sketch = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(4, 16, 8);
        sketch.update_weighted(&DataInput::U64(1), 5);
        sketch.update_weighted(&DataInput::U64(2), 3);

        let encoded = sketch.to_asapv1().expect("serialize");
        assert_eq!(metadata_of(&encoded).mode, "fast");
        let decoded = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::from_asapv1(&encoded)
            .expect("deserialize");
        assert_eq!(
            sketch.count_min_sketch().as_storage().as_slice(),
            decoded.count_min_sketch().as_storage().as_slice()
        );

        // Mode is pinned by the target: a fast payload must not decode into a
        // regular sketch.
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&encoded).is_err()
        );
    }

    /// CountMinSketchWithHeap, CountSketchWithHeap and the two base sketches all carry different kind_ids;
    /// a foreign envelope must not decode as a CountMinSketchWithHeap.
    #[test]
    fn cms_heap_rejects_foreign_kind_ids() {
        let cs_heap = crate::CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 8, 4);
        let cms = crate::CountMinSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);
        let count = crate::CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);

        for (bytes, what) in [
            (cs_heap.to_asapv1().expect("CSHeap"), "CSHeap"),
            (cms.to_asapv1().expect("CMS"), "Count-Min"),
            (count.to_asapv1().expect("Count"), "Count Sketch"),
        ] {
            assert!(
                CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&bytes).is_err(),
                "{what} bytes must not decode as a CMSHeap"
            );
        }
    }

    /// Builds a well-framed envelope around crafted metadata and a `u64`-keyed
    /// payload.
    fn crafted(rows: u32, cols: u32, heap_size: u32, counts: Vec<i64>) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(&heap_metadata::<DefaultXxHasher>(
            rows, cols, "i64", "regular", heap_size, "u64",
        ))
        .expect("metadata");
        let payload = rmp_serde::to_vec(&HeapPayload::<i64, u64, i64> {
            counts,
            keys: Vec::new(),
            heap_counts: Vec::new(),
        })
        .expect("payload");
        envelope::encode(kind::COUNT_MIN_WITH_HEAP, &metadata, &payload)
    }

    /// Fail closed (not panic) on a crafted envelope with a zero dimension:
    /// `Vector2D::from_fn` derives its mask via `cols.ilog2()`, which panics on
    /// `cols == 0`.
    #[test]
    fn cms_heap_rejects_zero_dimension_payload() {
        let bytes = crafted(4, 0, 4, Vec::new());
        let problem = decode_error(&bytes);
        assert!(problem.contains("must be non-zero"), "got {problem}");
    }

    /// Crafted dimensions must not drive a huge allocation: the length check
    /// runs before `Vector2D::from_fn`.
    #[test]
    fn cms_heap_rejects_dimension_length_mismatch() {
        let bytes = crafted(MATRIX_MAX_ROWS as u32, 1 << 24, 4, vec![1, 2, 3]);
        let problem = decode_error(&bytes);
        assert!(problem.contains("!= rows*cols"), "got {problem}");
    }

    /// More rows than the seed list has seeds is outside the wire-eligible
    /// subset, on both sides.
    #[test]
    fn cms_heap_rejects_too_many_rows() {
        let rows = MATRIX_MAX_ROWS + 1;
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(rows, 8, 4)
                .to_asapv1()
                .is_err(),
            "a matrix past MATRIX_MAX_ROWS must not serialize"
        );

        let problem = decode_error(&crafted(rows as u32, 8, 4, vec![0; rows * 8]));
        assert!(problem.contains("MATRIX_MAX_ROWS"), "got {problem}");

        // The boundary itself is eligible.
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(MATRIX_MAX_ROWS, 8, 4)
                .to_asapv1()
                .is_ok()
        );
    }

    /// A `rows == 0` matrix satisfies the length check trivially but is refused
    /// on decode, so serializing it must fail.
    #[test]
    fn cms_heap_rejects_serializing_zero_rows() {
        let sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(0, 4, 4);
        let problem = sketch
            .to_asapv1()
            .expect_err("a zero-row matrix must not serialize")
            .to_string();
        assert!(problem.contains("non-zero"), "got {problem}");
    }

    /// An unpopulated matrix carries dimensions its cells do not match.
    /// Serializing it must fail rather than emit bytes the decoder refuses.
    #[test]
    fn cms_heap_rejects_serializing_an_unfilled_matrix() {
        let sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(
            Vector2D::<i64>::unfilled(2, 4),
            4,
        );
        assert!(
            sketch.to_asapv1().is_err(),
            "a matrix whose cell count disagrees with its dimensions must not serialize"
        );
    }

    /// Fail closed on an unexpected metadata key.
    #[test]
    fn cms_heap_metadata_rejects_unknown_keys() {
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
            counter_type: String,
            mode: String,
            heap_size: u32,
            key_type: String,
            bogus_field: u8, // key not in HeapMetadata
        }
        let m = heap_metadata::<DefaultXxHasher>(2, 4, "i64", "regular", 4, "u64");
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
            counter_type: m.counter_type.clone(),
            mode: m.mode.clone(),
            heap_size: m.heap_size,
            key_type: m.key_type.clone(),
            bogus_field: 7,
        };
        let bytes = rmp_serde::to_vec_named(&extra).expect("encode");
        assert!(
            from_slice::<HeapMetadata>(&bytes).is_err(),
            "an unexpected metadata key must be rejected"
        );
    }

    /// `heap_size` is required: a metadata map missing it does not decode, so
    /// the heap capacity can never be silently defaulted.
    #[test]
    fn cms_heap_metadata_rejects_a_missing_heap_size_key() {
        #[derive(Serialize)]
        struct WithoutHeapSize {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            matrix_seed_index: u32,
            rows: u32,
            cols: u32,
            counter_type: String,
            mode: String,
            key_type: String,
        }
        let m = heap_metadata::<DefaultXxHasher>(2, 4, "i64", "regular", 4, "u64");
        let without = WithoutHeapSize {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            matrix_seed_index: m.matrix_seed_index,
            rows: m.rows,
            cols: m.cols,
            counter_type: m.counter_type.clone(),
            mode: m.mode.clone(),
            key_type: m.key_type.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&without).expect("encode");
        assert!(
            from_slice::<HeapMetadata>(&bytes).is_err(),
            "a missing heap_size must be rejected"
        );
    }

    /// `counter_type` is pinned by the target: `i32`, `i64` and `f64` are all
    /// Count-Min wire counters, so an `i32`-labelled envelope is well-formed —
    /// but it names a counter this `i64` sketch does not hold, and must not
    /// decode into it.
    #[test]
    fn cms_heap_rejects_a_foreign_counter_type_name() {
        let metadata = rmp_serde::to_vec_named(&heap_metadata::<DefaultXxHasher>(
            2, 4, "i32", "regular", 4, "u64",
        ))
        .expect("metadata");
        let payload = rmp_serde::to_vec(&HeapPayload::<i64, u64, i64> {
            counts: vec![0; 8],
            keys: Vec::new(),
            heap_counts: Vec::new(),
        })
        .expect("payload");
        let bytes = envelope::encode(kind::COUNT_MIN_WITH_HEAP, &metadata, &payload);
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&bytes).is_err(),
            "an i64 sketch must reject an i32-labelled envelope"
        );
    }

    // A test-only custom hasher: hashes exactly like `DefaultXxHasher` but
    // declares a DIFFERENT `HashProfile`. CountMinSketchWithHeap metadata is derived from the
    // profile, so an `AltHasher` sketch serializes truthfully. (An *unprofiled*
    // hasher cannot serialize at all — that is a compile-time guarantee, since
    // the wire methods are bounded on `H: HashProfile`.)
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
        fn hash64_seeded_owned(seed_index: usize, key: &crate::OwnedDataInput) -> u64 {
            DefaultXxHasher::hash64_seeded_owned(seed_index, key)
        }
        fn hash128_seeded_owned(seed_index: usize, key: &crate::OwnedDataInput) -> u128 {
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
        const SEED_DERIVATION: &'static str = "seed_list_index_wrap";
        const INPUT_ENCODING: &'static str = "projectasap.input.v1";
        fn seed_list() -> Vec<u64> {
            vec![1, 2, 3, 4, 5]
        }
        const CANONICAL_SEED_INDEX: u32 = DEFAULT_CANONICAL_SEED_INDEX as u32;
        const MATRIX_SEED_INDEX: u32 = 0;
    }

    #[test]
    fn cms_heap_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) A CountMinSketchWithHeap built with a custom-profile hasher round-trips.
        let mut alt = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath, AltHasher>::new(3, 8, 4);
        let mut std = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 8, 4);
        for key in [42u64, 7] {
            alt.update(&DataInput::U64(key));
            std.update(&DataInput::U64(key));
        }

        let alt_bytes = alt.to_asapv1().expect("alt serialize");
        let decoded = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath, AltHasher>::from_asapv1(
            &alt_bytes,
        )
        .expect("alt decode");
        assert_eq!(
            alt.count_min_sketch().as_storage().as_slice(),
            decoded.count_min_sketch().as_storage().as_slice()
        );
        assert_eq!(decoded.heap().len(), 2);

        // (b) Bytes differ from the standard-profile sketch (metadata derived
        // from the different profile).
        let std_bytes = std.to_asapv1().expect("std serialize");
        assert_ne!(alt_bytes, std_bytes);

        // (c) Standard-profile decode fails closed on custom-profile bytes.
        assert!(
            CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&alt_bytes).is_err(),
            "standard-profile decode must reject custom-profile bytes"
        );
    }

    /// A `heap_size` past the metadata's `u32` field fails the encode rather than
    /// emitting a truncated capacity.
    #[test]
    fn cms_heap_refuses_a_heap_size_the_metadata_cannot_carry() {
        let sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 4, |_, _| 0i64),
            1usize << 40,
        );
        let problem = sketch
            .to_asapv1()
            .expect_err("an oversized heap_size must not serialize")
            .to_string();
        assert!(
            problem.contains("exceeds the u32 metadata field"),
            "got {problem}"
        );
    }
}
