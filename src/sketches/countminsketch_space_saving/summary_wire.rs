//! ASAPv1 metadata and payload shared by CountMinSketchWithSpaceSaving (`0x1e 0x00`) and
//! CountSketchWithSpaceSaving (`0x1f 0x00`): `[counts, keys, summary_counts, errors, total,
//! discarded_max]` (`docs/asapv1_wire_format.md` §3.24, §3.25).

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::message_pack_format::wire_key::{WireBytes, WireString, exact_vec};
use crate::sketches::countminsketch_heap::heap_wire::{
    EMPTY_KEY_TYPE, check_distinct_keys, check_empty_key_type, check_geometry, key_type_of,
};
use crate::sketches::space_saving::parts::emitted_order;
use crate::{HashProfile, OwnedDataInput, SketchHasher, SpaceSaving};

/// Descriptor metadata, a msgpack **map** with keys in declaration order: the
/// hash-spec group, the matrix's `rows` / `cols` / `counter_type` / `mode`, then
/// the summary's `capacity` / `key_type`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SummaryTopKMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) seed_list: Vec<u64>,
    pub(crate) matrix_seed_index: u32,
    pub(crate) rows: u32,
    pub(crate) cols: u32,
    pub(crate) counter_type: String,
    pub(crate) mode: String,
    pub(crate) capacity: u32,
    pub(crate) key_type: String,
}

/// The metadata block `H`'s [`HashProfile`] gives these structural params.
pub(crate) fn summary_topk_metadata<H: HashProfile>(
    rows: u32,
    cols: u32,
    counter_type: &str,
    mode: &str,
    capacity: u32,
    key_type: &str,
) -> SummaryTopKMetadata {
    SummaryTopKMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        matrix_seed_index: H::MATRIX_SEED_INDEX,
        rows,
        cols,
        counter_type: counter_type.to_string(),
        mode: mode.to_string(),
        capacity,
        key_type: key_type.to_string(),
    }
}

/// The positional payload `[counts, keys, summary_counts, errors, total,
/// discarded_max]`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SummaryTopKPayload<C, K> {
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "C: Deserialize<'de>")
    )]
    pub(crate) counts: Vec<C>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "K: Deserialize<'de>")
    )]
    pub(crate) keys: Vec<K>,
    pub(crate) summary_counts: Vec<u64>,
    pub(crate) errors: Vec<u64>,
    pub(crate) total: u64,
    pub(crate) discarded_max: u64,
}

/// Packs the payload with `keys` typed by `key_type`; a key of another variant
/// fails the encode.
fn encode_payload<C: Serialize + Clone>(
    sketch: &str,
    key_type: &str,
    counts: &[C],
    entries: &[(&OwnedDataInput, u64, u64)],
    total: u64,
    discarded_max: u64,
) -> Result<Vec<u8>, RmpEncodeError> {
    fn pack<C: Serialize + Clone, K: Serialize>(
        sketch: &str,
        key_type: &str,
        counts: &[C],
        entries: &[(&OwnedDataInput, u64, u64)],
        total: u64,
        discarded_max: u64,
        wire: impl Fn(&OwnedDataInput) -> Option<K>,
    ) -> Result<Vec<u8>, RmpEncodeError> {
        let mut keys = Vec::with_capacity(entries.len());
        for (key, _, _) in entries {
            keys.push(wire(key).ok_or_else(|| {
                RmpEncodeError::Syntax(format!(
                    "ASAPv1 {sketch} envelope: keys mix variants — key_type is {key_type}, but a {} key is held",
                    key_type_of(key).unwrap_or("128-bit")
                ))
            })?);
        }
        rmp_serde::to_vec(&SummaryTopKPayload {
            counts: counts.to_vec(),
            keys,
            summary_counts: entries.iter().map(|entry| entry.1).collect(),
            errors: entries.iter().map(|entry| entry.2).collect(),
            total,
            discarded_max,
        })
    }

    macro_rules! pack {
        ($variant:ident, $value:ident => $wire:expr) => {
            pack(
                sketch,
                key_type,
                counts,
                entries,
                total,
                discarded_max,
                |key| match key {
                    OwnedDataInput::$variant($value) => Some($wire),
                    _ => None,
                },
            )
        };
    }

    match key_type {
        "i8" => pack!(I8, v => *v),
        "i16" => pack!(I16, v => *v),
        "i32" => pack!(I32, v => *v),
        "i64" => pack!(I64, v => *v),
        "isize" => pack!(Isize, v => *v),
        "u8" => pack!(U8, v => *v),
        "u16" => pack!(U16, v => *v),
        "u32" => pack!(U32, v => *v),
        "u64" => pack!(U64, v => *v),
        "usize" => pack!(Usize, v => *v),
        "f32" => pack!(F32, v => *v),
        "f64" => pack!(F64, v => *v),
        "string" => pack!(String, v => WireString(v.clone())),
        "bytes" => pack!(Bytes, v => WireBytes(v.clone())),
        other => Err(RmpEncodeError::Syntax(format!(
            "ASAPv1 {sketch} envelope: key_type {other:?} is not a wire key type"
        ))),
    }
}

/// One kind's identity on the wire: its name in errors, its kind_id, and the
/// `counter_type` and `mode` its target type pins.
pub(crate) struct SummaryKind {
    pub(crate) name: &'static str,
    pub(crate) kind_id: &'static [u8],
    pub(crate) counter_type: &'static str,
    pub(crate) mode: &'static str,
}

/// Encodes a row-major `rows x cols` matrix and `summary` as `kind`. Fails
/// where §3.24's decode rules would reject the bytes, and on mixed-variant or
/// 128-bit keys.
pub(crate) fn encode<C: Serialize + Clone, H: SketchHasher + HashProfile>(
    kind: &SummaryKind,
    rows: usize,
    cols: usize,
    counts: &[C],
    summary: &SpaceSaving<H>,
) -> Result<Vec<u8>, RmpEncodeError> {
    let sketch = kind.name;
    let complain = |e: String| RmpEncodeError::Syntax(format!("ASAPv1 {sketch} envelope: {e}"));
    check_geometry(sketch, rows, cols).map_err(complain)?;
    if counts.len() != rows.saturating_mul(cols) {
        return Err(complain(format!(
            "counts length {} != rows*cols {}",
            counts.len(),
            rows.saturating_mul(cols)
        )));
    }
    let capacity = u32::try_from(summary.capacity()).map_err(|_| {
        complain(format!(
            "capacity {} exceeds the u32 metadata field",
            summary.capacity()
        ))
    })?;
    let entries = summary.emitted_entries();
    check_distinct_keys(entries.iter().map(|(key, _, _)| *key))
        .map_err(|problem| complain(problem.to_string()))?;
    let key_type = match entries.first() {
        None => EMPTY_KEY_TYPE,
        Some((key, _, _)) => key_type_of(key)
            .ok_or_else(|| complain("128-bit keys are not wire types".to_string()))?,
    };
    let metadata = rmp_serde::to_vec_named(&summary_topk_metadata::<H>(
        rows as u32,
        cols as u32,
        kind.counter_type,
        kind.mode,
        capacity,
        key_type,
    ))?;
    let payload = encode_payload(
        sketch,
        key_type,
        counts,
        &entries,
        summary.total(),
        summary.discarded_max(),
    )?;
    Ok(envelope::encode(kind.kind_id, &metadata, &payload))
}

/// What a decoded envelope holds: the matrix geometry, its row-major cells and
/// the rebuilt summary.
pub(crate) struct DecodedSummaryTopK<C, H: SketchHasher> {
    pub(crate) rows: usize,
    pub(crate) cols: usize,
    pub(crate) counts: Vec<C>,
    pub(crate) summary: SpaceSaving<H>,
}

/// Decodes a `kind` envelope under §3.24's decode rules. Neither the geometry
/// nor `capacity` sizes an allocation.
pub(crate) fn decode<C: DeserializeOwned, H: SketchHasher + HashProfile>(
    kind: &SummaryKind,
    bytes: &[u8],
) -> Result<DecodedSummaryTopK<C, H>, RmpDecodeError> {
    let sketch = kind.name;
    let complain = |e: String| RmpDecodeError::Uncategorized(format!("ASAPv1 {sketch}: {e}"));
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != kind.kind_id {
        return Err(complain(format!(
            "kind_id mismatch: stored {kind_id:?}, expected {:?}",
            kind.kind_id
        )));
    }
    let meta: SummaryTopKMetadata = envelope::decode_block(metadata)?;
    if meta
        != summary_topk_metadata::<H>(
            meta.rows,
            meta.cols,
            kind.counter_type,
            kind.mode,
            meta.capacity,
            &meta.key_type,
        )
    {
        return Err(complain("metadata mismatch".to_string()));
    }
    let (rows, cols) = (meta.rows as usize, meta.cols as usize);
    check_geometry(sketch, rows, cols).map_err(complain)?;

    macro_rules! unpack {
        ($ty:ty, $into:expr) => {{
            let p: SummaryTopKPayload<C, $ty> = envelope::decode_block(payload)?;
            let keys: Vec<OwnedDataInput> = p.keys.into_iter().map($into).collect();
            (
                p.counts,
                keys,
                p.summary_counts,
                p.errors,
                p.total,
                p.discarded_max,
            )
        }};
    }

    let (counts, keys, summary_counts, errors, total, discarded_max) = match meta.key_type.as_str()
    {
        "i8" => unpack!(i8, OwnedDataInput::I8),
        "i16" => unpack!(i16, OwnedDataInput::I16),
        "i32" => unpack!(i32, OwnedDataInput::I32),
        "i64" => unpack!(i64, OwnedDataInput::I64),
        "isize" => unpack!(isize, OwnedDataInput::Isize),
        "u8" => unpack!(u8, OwnedDataInput::U8),
        "u16" => unpack!(u16, OwnedDataInput::U16),
        "u32" => unpack!(u32, OwnedDataInput::U32),
        "u64" => unpack!(u64, OwnedDataInput::U64),
        "usize" => unpack!(usize, OwnedDataInput::Usize),
        "f32" => unpack!(f32, OwnedDataInput::F32),
        "f64" => unpack!(f64, OwnedDataInput::F64),
        "string" => unpack!(WireString, |key: WireString| OwnedDataInput::String(
            key.into_string()
        )),
        "bytes" => unpack!(WireBytes, |key: WireBytes| OwnedDataInput::Bytes(
            key.into_vec()
        )),
        other => {
            return Err(complain(format!(
                "key_type {other:?} is not a wire key type"
            )));
        }
    };

    check_empty_key_type(&meta.key_type, keys.is_empty()).map_err(complain)?;
    if keys.len() != summary_counts.len() || keys.len() != errors.len() {
        return Err(complain(format!(
            "{} keys against {} summary_counts and {} errors",
            keys.len(),
            summary_counts.len(),
            errors.len()
        )));
    }
    if counts.len() != rows.saturating_mul(cols) {
        return Err(complain(format!(
            "counts length {} != rows*cols {}",
            counts.len(),
            rows.saturating_mul(cols)
        )));
    }
    check_distinct_keys(keys.iter()).map_err(|problem| complain(problem.to_string()))?;
    if keys
        .windows(2)
        .zip(summary_counts.windows(2))
        .any(|(k, c)| emitted_order((&k[0], c[0]), (&k[1], c[1])).is_ge())
    {
        return Err(complain(
            "entries are not in descending count, ties by key".to_string(),
        ));
    }
    let entries = keys
        .into_iter()
        .zip(summary_counts)
        .zip(errors)
        .map(|((key, count), error)| (key, count, error))
        .collect();
    let summary = SpaceSaving::from_parts(meta.capacity as usize, total, discarded_max, entries)
        .map_err(complain)?;
    Ok(DecodedSummaryTopK {
        rows,
        cols,
        counts,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use crate::message_pack_format::kind;
    use super::*;
    use crate::{
        CountMinSketch, CountMinSketchWithHeap, CountMinSketchWithSpaceSaving,
        CountSketchWithSpaceSaving, DataInput, DefaultXxHasher, FastPath, MATRIX_MAX_ROWS,
        RegularPath, Vector2D,
    };
    use rmp_serde::from_slice;

    type Target = CountMinSketchWithSpaceSaving<Vector2D<i64>, RegularPath>;

    /// A 2x8 sketch whose 3-counter summary evicted key `2` (count 5) for key
    /// `4`, which carries that 5 as error: entries `1:9`, `3:7`, `4:7/5`.
    fn populated() -> Target {
        let mut sketch = Target::new(2, 8, 3);
        for (key, weight) in [(1u64, 9i64), (2, 5), (3, 7), (4, 2)] {
            sketch.update_weighted(&DataInput::U64(key), weight);
        }
        sketch
    }

    fn decode_error(bytes: &[u8]) -> String {
        match Target::from_asapv1(bytes) {
            Ok(_) => panic!("a crafted envelope must be rejected, not decoded"),
            Err(err) => err.to_string(),
        }
    }

    fn metadata_of(bytes: &[u8]) -> SummaryTopKMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    fn entries_of<H: SketchHasher>(summary: &SpaceSaving<H>) -> Vec<(OwnedDataInput, u64, u64)> {
        summary
            .emitted_entries()
            .into_iter()
            .map(|(key, count, error)| (key.clone(), count, error))
            .collect()
    }

    /// A well-formed `u64`-keyed envelope over a 2x3 matrix; each test breaks
    /// one field.
    struct Craft<K> {
        rows: u32,
        cols: u32,
        capacity: u32,
        key_type: &'static str,
        counts: Vec<i64>,
        keys: Vec<K>,
        summary_counts: Vec<u64>,
        errors: Vec<u64>,
        total: u64,
        discarded_max: u64,
    }

    fn craft() -> Craft<u64> {
        Craft {
            rows: 2,
            cols: 3,
            capacity: 4,
            key_type: "u64",
            counts: vec![0, 1, 2, 3, 4, 5],
            keys: vec![7, 8, 9],
            summary_counts: vec![9, 4, 4],
            errors: vec![0, 1, 0],
            total: 16,
            discarded_max: 0,
        }
    }

    impl<K: Serialize> Craft<K> {
        fn bytes(&self) -> Vec<u8> {
            let metadata = rmp_serde::to_vec_named(&summary_topk_metadata::<DefaultXxHasher>(
                self.rows,
                self.cols,
                "i64",
                "regular",
                self.capacity,
                self.key_type,
            ))
            .expect("metadata");
            let payload = rmp_serde::to_vec(&SummaryTopKPayload {
                counts: self.counts.clone(),
                keys: self.keys.iter().collect::<Vec<&K>>(),
                summary_counts: self.summary_counts.clone(),
                errors: self.errors.clone(),
                total: self.total,
                discarded_max: self.discarded_max,
            })
            .expect("payload");
            envelope::encode(kind::COUNT_MIN_WITH_SPACE_SAVING, &metadata, &payload)
        }
    }

    #[test]
    fn round_trip_keeps_matrix_summary_and_bytes() {
        let sketch = populated();
        let encoded = sketch.to_asapv1().expect("serialize");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x1e, 0x00]);

        let meta = metadata_of(&encoded);
        assert_eq!(meta.metadata_version, 1);
        assert_eq!((meta.rows, meta.cols, meta.capacity), (2, 8, 3));
        assert_eq!(
            (meta.counter_type.as_str(), meta.mode.as_str()),
            ("i64", "regular")
        );
        assert_eq!(meta.key_type, "u64");

        let decoded = Target::from_asapv1(&encoded).expect("decode");
        assert_eq!(
            decoded.cms().as_storage().as_slice(),
            sketch.cms().as_storage().as_slice()
        );
        assert_eq!(
            entries_of(decoded.space_saving()),
            vec![
                (OwnedDataInput::U64(1), 9, 0),
                (OwnedDataInput::U64(3), 7, 0),
                (OwnedDataInput::U64(4), 7, 5),
            ]
        );
        assert_eq!(decoded.space_saving().total(), 23);
        assert_eq!(decoded.space_saving().discarded_max(), 5);
        assert_eq!(decoded.capacity(), 3);
        assert_eq!(decoded.top_k(3), sketch.top_k(3));
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    #[test]
    fn every_wire_counter_type_and_mode_round_trips() {
        let mut fast = CountMinSketchWithSpaceSaving::<Vector2D<i32>, FastPath>::new(3, 16, 2);
        fast.update_weighted(&DataInput::Str("a"), 4);
        fast.update_weighted(&DataInput::Str("b"), 1);
        fast.update_weighted(&DataInput::Str("c"), 2);
        let bytes = fast.to_asapv1().expect("i32");
        let meta = metadata_of(&bytes);
        assert_eq!(
            (meta.counter_type.as_str(), meta.mode.as_str()),
            ("i32", "fast")
        );
        assert_eq!(meta.key_type, "string");
        let back = CountMinSketchWithSpaceSaving::<Vector2D<i32>, FastPath>::from_asapv1(&bytes)
            .expect("i32");
        assert_eq!(back.top_k(2), fast.top_k(2));
        assert_eq!(back.to_asapv1().expect("i32"), bytes);

        let mut floating =
            CountMinSketchWithSpaceSaving::<Vector2D<f64>, RegularPath>::new(2, 4, 5);
        floating.update_weighted(&DataInput::Bytes(&[0xff, 0x00]), 2.5);
        let bytes = floating.to_asapv1().expect("f64");
        assert_eq!(metadata_of(&bytes).key_type, "bytes");
        let back = CountMinSketchWithSpaceSaving::<Vector2D<f64>, RegularPath>::from_asapv1(&bytes)
            .expect("f64");
        assert_eq!(back.estimate(&DataInput::Bytes(&[0xff, 0x00])), 2.5);
        assert_eq!(back.to_asapv1().expect("f64"), bytes);
    }

    #[test]
    fn an_empty_summary_has_one_encoding() {
        let bytes = Target::new(2, 3, 4).to_asapv1().expect("empty");
        assert_eq!(metadata_of(&bytes).key_type, "u64");
        let back = Target::from_asapv1(&bytes).expect("empty");
        assert!(back.space_saving().is_empty());
        assert_eq!(back.capacity(), 4);
    }

    /// An empty summary carries `key_type` `"u64"`; any other name is rejected,
    /// for both kinds.
    #[test]
    fn an_empty_summary_must_carry_the_u64_key_type() {
        use crate::sketches::countminsketch_heap::heap_wire::relabel_u64_key_type;
        let cms = Target::new(2, 3, 4).to_asapv1().expect("empty");
        let cs = CountSketchWithSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 8, 3)
            .to_asapv1()
            .expect("empty");
        for to in ["string", "f64"] {
            let problem = decode_error(&relabel_u64_key_type(&cms, "key_type", to));
            assert!(problem.contains("empty key set"), "{to}: {problem}");
            let problem = CountSketchWithSpaceSaving::<Vector2D<i64>, RegularPath>::from_asapv1(
                &relabel_u64_key_type(&cs, "key_type", to),
            )
            .map(drop)
            .expect_err("an empty CSSpaceSaving under another key_type must be rejected")
            .to_string();
            assert!(problem.contains("empty key set"), "{to}: {problem}");
        }
    }

    #[test]
    fn foreign_kind_ids_are_rejected() {
        let cms_heap = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(2, 8, 3);
        let cs_ss = CountSketchWithSpaceSaving::<Vector2D<i64>, RegularPath>::new(2, 8, 3);
        let cms = CountMinSketch::<Vector2D<i64>, RegularPath>::with_dimensions(2, 8);
        let summary = SpaceSaving::<DefaultXxHasher>::with_capacity(3);
        for (bytes, what) in [
            (cms_heap.to_asapv1().expect("CMSHeap"), "CMSHeap"),
            (cs_ss.to_asapv1().expect("CSSpaceSaving"), "CSSpaceSaving"),
            (cms.to_asapv1().expect("Count-Min"), "Count-Min"),
            (summary.to_asapv1().expect("Space-Saving"), "Space-Saving"),
        ] {
            assert!(
                decode_error(&bytes).contains("kind_id mismatch"),
                "{what} bytes must not decode as a CMSSpaceSaving"
            );
        }
    }

    #[test]
    fn counter_type_mode_and_profile_are_pinned() {
        let bytes = populated().to_asapv1().expect("serialize");
        assert!(
            CountMinSketchWithSpaceSaving::<Vector2D<f64>, RegularPath>::from_asapv1(&bytes)
                .is_err()
        );
        assert!(
            CountMinSketchWithSpaceSaving::<Vector2D<i32>, RegularPath>::from_asapv1(&bytes)
                .is_err()
        );
        assert!(
            CountMinSketchWithSpaceSaving::<Vector2D<i64>, FastPath>::from_asapv1(&bytes).is_err()
        );

        let mut meta = metadata_of(&bytes);
        meta.hash_profile_id = "test.alt.profile.v1".to_string();
        let (_, _, payload) = envelope::split(&bytes).expect("split");
        let relabelled = envelope::encode(
            kind::COUNT_MIN_WITH_SPACE_SAVING,
            &rmp_serde::to_vec_named(&meta).expect("metadata"),
            payload,
        );
        assert!(decode_error(&relabelled).contains("metadata mismatch"));
    }

    #[test]
    fn metadata_keys_are_exactly_the_schema() {
        #[derive(Serialize)]
        struct Extra {
            #[serde(flatten)]
            meta: SummaryTopKMetadata,
            admission_rate: f64,
        }
        let meta = summary_topk_metadata::<DefaultXxHasher>(2, 3, "i64", "regular", 4, "u64");
        let extra = rmp_serde::to_vec_named(&Extra {
            meta,
            admission_rate: 0.5,
        })
        .expect("encode");
        assert!(from_slice::<SummaryTopKMetadata>(&extra).is_err());

        #[derive(Serialize)]
        struct WithoutCapacity<'a> {
            metadata_version: u8,
            hash_profile_id: &'a str,
            hash_algorithm: &'a str,
            seed_derivation: &'a str,
            input_encoding: &'a str,
            seed_list: &'a [u64],
            matrix_seed_index: u32,
            rows: u32,
            cols: u32,
            counter_type: &'a str,
            mode: &'a str,
            key_type: &'a str,
        }
        let m = summary_topk_metadata::<DefaultXxHasher>(2, 3, "i64", "regular", 4, "u64");
        let without = rmp_serde::to_vec_named(&WithoutCapacity {
            metadata_version: m.metadata_version,
            hash_profile_id: &m.hash_profile_id,
            hash_algorithm: &m.hash_algorithm,
            seed_derivation: &m.seed_derivation,
            input_encoding: &m.input_encoding,
            seed_list: &m.seed_list,
            matrix_seed_index: m.matrix_seed_index,
            rows: m.rows,
            cols: m.cols,
            counter_type: &m.counter_type,
            mode: &m.mode,
            key_type: &m.key_type,
        })
        .expect("encode");
        assert!(from_slice::<SummaryTopKMetadata>(&without).is_err());
    }

    #[test]
    fn the_crafted_baseline_decodes() {
        let decoded = Target::from_asapv1(&craft().bytes()).expect("baseline");
        assert_eq!(decoded.space_saving().len(), 3);
        assert_eq!(decoded.cms().as_storage().as_slice(), &[0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn geometry_is_checked_before_anything_is_sized() {
        let mut c = craft();
        c.cols = 0;
        c.counts.clear();
        assert!(decode_error(&c.bytes()).contains("must be non-zero"));

        let mut c = craft();
        c.rows = MATRIX_MAX_ROWS as u32 + 1;
        c.counts = vec![0; (MATRIX_MAX_ROWS + 1) * 3];
        assert!(decode_error(&c.bytes()).contains("MATRIX_MAX_ROWS"));
        assert!(Target::new(MATRIX_MAX_ROWS + 1, 3, 4).to_asapv1().is_err());

        let mut c = craft();
        c.rows = MATRIX_MAX_ROWS as u32;
        c.cols = 1 << 24;
        assert!(decode_error(&c.bytes()).contains("!= rows*cols"));
    }

    #[test]
    fn key_type_names_the_keys_exactly() {
        let mut c = craft();
        c.key_type = "u128";
        assert!(decode_error(&c.bytes()).contains("not a wire key type"));

        let mut c = craft();
        c.key_type = "string";
        assert!(Target::from_asapv1(&c.bytes()).is_err());

        let strings = Craft {
            key_type: "bytes",
            keys: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ..craft_with_keys()
        };
        assert!(Target::from_asapv1(&strings.bytes()).is_err());
    }

    fn craft_with_keys<K>() -> Craft<K> {
        let c = craft();
        Craft {
            rows: c.rows,
            cols: c.cols,
            capacity: c.capacity,
            key_type: c.key_type,
            counts: c.counts,
            keys: Vec::new(),
            summary_counts: c.summary_counts,
            errors: c.errors,
            total: c.total,
            discarded_max: c.discarded_max,
        }
    }

    #[test]
    fn parallel_arrays_have_one_length() {
        let mut c = craft();
        c.errors.pop();
        assert!(decode_error(&c.bytes()).contains("summary_counts"));
        let mut c = craft();
        c.keys.pop();
        assert!(decode_error(&c.bytes()).contains("summary_counts"));
    }

    #[test]
    fn capacity_bounds_the_entries() {
        let mut c = craft();
        c.capacity = 0;
        assert!(decode_error(&c.bytes()).contains("capacity is zero"));
        let mut c = craft();
        c.capacity = 2;
        assert!(decode_error(&c.bytes()).contains("over a capacity"));
    }

    #[test]
    fn counts_and_errors_describe_a_reachable_counter() {
        let mut c = craft();
        c.summary_counts = vec![9, 4, 0];
        assert!(decode_error(&c.bytes()).contains("at zero"));
        let mut c = craft();
        c.errors = vec![0, 5, 0];
        assert!(decode_error(&c.bytes()).contains("error of 5"));
    }

    #[test]
    fn a_key_appears_once() {
        let mut c = craft();
        c.keys = vec![7, 8, 7];
        assert!(decode_error(&c.bytes()).contains("twice"));

        let nan = Craft {
            key_type: "f64",
            keys: vec![f64::NAN, f64::NAN, 1.0],
            summary_counts: vec![9, 9, 4],
            errors: vec![0, 0, 0],
            total: 22,
            ..craft_with_keys()
        };
        assert!(decode_error(&nan.bytes()).contains("twice"));

        let zeros = Craft {
            key_type: "f64",
            keys: vec![0.0f64, -0.0, 1.0],
            summary_counts: vec![9, 9, 4],
            errors: vec![0, 0, 0],
            total: 22,
            ..craft_with_keys()
        };
        assert!(Target::from_asapv1(&zeros.bytes()).is_ok());
    }

    #[test]
    fn discarded_max_sits_at_or_under_the_smallest_count() {
        let mut c = craft();
        c.discarded_max = 5;
        assert!(decode_error(&c.bytes()).contains("ceiling of 5"));
        let mut c = craft();
        c.discarded_max = 4;
        assert!(Target::from_asapv1(&c.bytes()).is_ok());

        let mut empty = craft();
        empty.keys.clear();
        empty.summary_counts.clear();
        empty.errors.clear();
        empty.discarded_max = 1;
        assert!(decode_error(&empty.bytes()).contains("ceiling of 1"));
    }

    #[test]
    fn total_covers_the_recorded_weight_saturating() {
        let mut c = craft();
        c.total = 15;
        assert!(decode_error(&c.bytes()).contains("total of 15"));

        let mut c = craft();
        c.summary_counts = vec![u64::MAX, u64::MAX, 4];
        c.total = u64::MAX;
        assert!(Target::from_asapv1(&c.bytes()).is_ok());
    }

    #[test]
    fn entries_are_in_emitted_order() {
        let mut c = craft();
        c.keys = vec![8, 7, 9];
        c.summary_counts = vec![4, 9, 4];
        c.errors = vec![1, 0, 0];
        assert!(decode_error(&c.bytes()).contains("descending count"));

        let mut c = craft();
        c.keys = vec![7, 9, 8];
        assert!(decode_error(&c.bytes()).contains("descending count"));

        let signed = Craft {
            key_type: "i64",
            keys: vec![5i64, -1, 0],
            summary_counts: vec![9, 4, 4],
            ..craft_with_keys()
        };
        assert!(decode_error(&signed.bytes()).contains("descending count"));
        let signed = Craft {
            key_type: "i64",
            keys: vec![5i64, 0, -1],
            summary_counts: vec![9, 4, 4],
            ..craft_with_keys()
        };
        assert!(Target::from_asapv1(&signed.bytes()).is_ok());
    }

    #[test]
    fn a_declared_capacity_never_sizes_an_allocation() {
        let mut c = craft();
        c.capacity = u32::MAX;
        let decoded = Target::from_asapv1(&c.bytes()).expect("decode");
        assert_eq!(decoded.capacity(), u32::MAX as usize);
        assert_eq!(decoded.space_saving().len(), 3);
        assert_eq!(decoded.to_asapv1().expect("re-encode"), c.bytes());
    }

    #[test]
    fn states_with_no_encoding_fail_to_serialize() {
        let sampled = Target::from_parts(
            CountMinSketch::with_dimensions(2, 3).with_admission_rate(0.5),
            SpaceSaving::with_capacity(4),
        );
        let problem = sampled.to_asapv1().expect_err("sampled").to_string();
        assert!(problem.contains("CMSSpaceSaving"), "got {problem}");

        let mut mixed = Target::new(2, 3, 4);
        mixed.update(&DataInput::U64(1));
        mixed.update(&DataInput::I64(2));
        assert!(
            mixed
                .to_asapv1()
                .expect_err("mixed")
                .to_string()
                .contains("mix variants")
        );

        let mut wide = Target::new(2, 3, 4);
        wide.update(&DataInput::I128(1));
        assert!(
            wide.to_asapv1()
                .expect_err("128-bit")
                .to_string()
                .contains("128-bit")
        );

        let mut nan = Target::new(2, 3, 4);
        nan.update(&DataInput::F64(f64::NAN));
        nan.update(&DataInput::F64(f64::NAN));
        assert_eq!(nan.space_saving().len(), 2);
        assert!(
            nan.to_asapv1()
                .expect_err("NaN twice")
                .to_string()
                .contains("twice")
        );

        let huge = Target::from_parts(
            CountMinSketch::with_dimensions(2, 3),
            SpaceSaving::from_parts(1 << 40, 3, 0, vec![(OwnedDataInput::U64(1), 3, 0)])
                .expect("sparse"),
        );
        assert!(
            huge.to_asapv1()
                .expect_err("capacity")
                .to_string()
                .contains("exceeds the u32 metadata field")
        );

        let unfilled = Target::from_parts(
            CountMinSketch::from_storage(Vector2D::<i64>::init(2, 3)),
            SpaceSaving::with_capacity(4),
        );
        assert!(unfilled.to_asapv1().is_err());
    }
}
