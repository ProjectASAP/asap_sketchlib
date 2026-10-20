//! ASAPv1 wire serialization for [`EhBucketSketch`], plus the sub-payload
//! [`ExponentialHistogram`] reuses.
//!
//! Child submodule of [`crate::sketch_framework::eh_bucket_sketch`]: it holds the
//! metadata/payload DTOs, the kind_id constants and the `to_asapv1` /
//! `from_asapv1` impls.
//!
//! EhBucketSketch is one kind_id, `0x14 0x00`. It is a tagged union over ten
//! sketch algorithms, so the encoding is the triple
//! `[kind_id, descriptor, state]`.
//!
//! ## One encoding, used in both places
//!
//! [`sketch_state`] and [`rebuild_sketch`] are the only place an EhBucketSketch is
//! read or rebuilt. The standalone `0x14 0x00` payload is one triple; an
//! [`ExponentialHistogram`] bucket inlines the same triple.
//!
//! ## A nested variant is its own envelope minus the framing
//!
//! The triple carries the variant's own kind_id, metadata block and payload
//! block. The magic, version and length fields are stripped, so the variant's
//! own decoder validates the blocks it wrote.
//!
//! ## The kind_id namespace is fixed in every build
//!
//! All ten ids dispatch whatever features are on. A decoder built without
//! `experimental` rejects `0x0d 0x00` by name with the feature named, and its
//! encoder can never emit it.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::message_pack_format::envelope;
use crate::message_pack_format::kind;
use crate::message_pack_format::kind::EH_BUCKET_SKETCH;

use super::EhBucketSketch;

/// The registry name of a nested kind_id. Error messages only: encoding and
/// decoding dispatch on the bytes.
pub(crate) fn variant_name(kind_id: &[u8]) -> Option<&'static str> {
    match kind_id {
        kind::COUNT_MIN => Some("CountMin"),
        kind::COCO => Some("Coco"),
        kind::COUNT_SKETCH_WITH_L2_NORM => Some("CountL2HH"),
        kind::COUNT_SKETCH => Some("CountSketch"),
        kind::DDSKETCH => Some("DDSketch"),
        kind::ELASTIC => Some("Elastic"),
        kind::HLL_ERTL => Some("HLL"),
        kind::KLL => Some("KLL"),
        kind::UNIFORM_SAMPLING => Some("UniformSampling"),
        kind::UNIVMON => Some("UnivMon"),
        _ => None,
    }
}

/// EhBucketSketch descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`). The union has no construction config of its own: the
/// kind_id belongs to the triple, which travels whole.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EhSketchListMetadata {
    pub(crate) metadata_version: u8,
}

/// Builds the EhBucketSketch descriptor metadata.
pub(crate) fn eh_sketch_list_metadata() -> EhSketchListMetadata {
    EhSketchListMetadata {
        metadata_version: 1,
    }
}

/// One EhBucketSketch as a msgpack **array** (`to_vec`, positional):
/// `[kind_id, descriptor, state]`. All three are msgpack `bin`: the variant's
/// own kind_id bytes, its ASAPv1 metadata block and its ASAPv1 payload block.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SketchState {
    #[serde(with = "serde_bytes")]
    pub(crate) kind_id: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub(crate) descriptor: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub(crate) state: Vec<u8>,
}

/// Rejects a kind_id outside the ten.
pub(crate) fn unknown_variant(kind_id: &[u8]) -> RmpDecodeError {
    RmpDecodeError::Uncategorized(format!(
        "ASAPv1 EHSketchList: kind_id {kind_id:02x?} is not a wire variant"
    ))
}

/// Rejects the variant a build without `experimental` does not carry.
#[cfg(not(feature = "experimental"))]
fn check_variant_available(kind_id: &[u8]) -> Result<(), RmpDecodeError> {
    match kind_id {
        kind::UNIFORM_SAMPLING => Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 EHSketchList: variant {} (kind_id {kind_id:02x?}) needs the experimental feature",
            variant_name(kind_id).unwrap_or("unnamed")
        ))),
        _ => Ok(()),
    }
}

/// Every variant is carried in an `experimental` build.
#[cfg(feature = "experimental")]
fn check_variant_available(_kind_id: &[u8]) -> Result<(), RmpDecodeError> {
    Ok(())
}

/// Splits one variant's own envelope into the triple, pinning its kind_id.
fn triple(kind: &'static [u8], bytes: &[u8]) -> Result<SketchState, RmpEncodeError> {
    let (kind_id, descriptor, state) = envelope::split(bytes).map_err(RmpEncodeError::Syntax)?;
    if kind_id != kind {
        return Err(RmpEncodeError::Syntax(format!(
            "ASAPv1 EHSketchList: {} produced kind_id {kind_id:02x?}, expected {kind:02x?}",
            variant_name(kind).unwrap_or("variant")
        )));
    }
    Ok(SketchState {
        kind_id: kind.to_vec(),
        descriptor: descriptor.to_vec(),
        state: state.to_vec(),
    })
}

/// Rejects a variant whose admission rate is not `1`.
fn check_admits_all(sketch: &EhBucketSketch) -> Result<(), String> {
    let rate = match sketch {
        EhBucketSketch::CountMinSketch(s) => s.admission_rate(),
        EhBucketSketch::CountSketch(s) => s.admission_rate(),
        EhBucketSketch::DdSketch(s) => s.admission_rate(),
        EhBucketSketch::Hll(s) => s.admission_rate(),
        _ => return Ok(()),
    };
    admission_rate::check_admits_all(rate).map_err(|e| format!("ASAPv1 EHSketchList: {e}"))
}

/// The triple one EhBucketSketch contributes, in the order the wire emits it.
/// Every state the variant's own encoder refuses is refused here too.
pub(crate) fn sketch_state(sketch: &EhBucketSketch) -> Result<SketchState, RmpEncodeError> {
    check_admits_all(sketch).map_err(RmpEncodeError::Syntax)?;
    match sketch {
        EhBucketSketch::CountMinSketch(s) => triple(kind::COUNT_MIN, &s.to_asapv1()?),
        EhBucketSketch::CocoSketch(s) => triple(kind::COCO, &s.to_asapv1()?),
        EhBucketSketch::CountSketchWithL2Norm(s) => {
            triple(kind::COUNT_SKETCH_WITH_L2_NORM, &s.to_asapv1()?)
        }
        EhBucketSketch::CountSketch(s) => triple(kind::COUNT_SKETCH, &s.to_asapv1()?),
        EhBucketSketch::DdSketch(s) => triple(kind::DDSKETCH, &s.to_asapv1()?),
        EhBucketSketch::ElasticSketch(s) => triple(kind::ELASTIC, &s.to_asapv1()?),
        EhBucketSketch::Hll(s) => triple(kind::HLL_ERTL, &s.to_asapv1()?),
        EhBucketSketch::Kll(s) => triple(kind::KLL, &s.to_asapv1()?),
        #[cfg(feature = "experimental")]
        EhBucketSketch::Uniform(s) => triple(kind::UNIFORM_SAMPLING, &s.to_asapv1()?),
        EhBucketSketch::UnivMon(s) => triple(kind::UNIVMON, &s.to_asapv1()?),
    }
}

/// Rebuilds one EhBucketSketch from a triple, through the variant's own decoder.
/// An unknown kind_id, and one this build does not carry, are rejected before
/// any block is assembled. A descriptor carrying `admission_rate` is rejected.
pub(crate) fn rebuild_sketch(triple: &SketchState) -> Result<EhBucketSketch, RmpDecodeError> {
    let sketch = rebuild_variant(triple)?;
    check_admits_all(&sketch).map_err(RmpDecodeError::Uncategorized)?;
    Ok(sketch)
}

/// [`rebuild_sketch`] before the `admission_rate` check.
fn rebuild_variant(triple: &SketchState) -> Result<EhBucketSketch, RmpDecodeError> {
    let kind_id = triple.kind_id.as_slice();
    check_variant_available(kind_id)?;
    if variant_name(kind_id).is_none() {
        return Err(unknown_variant(kind_id));
    }
    let bytes = envelope::encode(kind_id, &triple.descriptor, &triple.state);
    match kind_id {
        kind::COUNT_MIN => Ok(EhBucketSketch::CountMinSketch(
            crate::CountMinSketch::from_asapv1(&bytes)?,
        )),
        kind::COCO => Ok(EhBucketSketch::CocoSketch(crate::CocoSketch::from_asapv1(
            &bytes,
        )?)),
        kind::COUNT_SKETCH_WITH_L2_NORM => Ok(EhBucketSketch::CountSketchWithL2Norm(
            crate::CountSketchWithL2Norm::from_asapv1(&bytes)?,
        )),
        kind::COUNT_SKETCH => Ok(EhBucketSketch::CountSketch(
            crate::CountSketch::from_asapv1(&bytes)?,
        )),
        kind::DDSKETCH => Ok(EhBucketSketch::DdSketch(crate::DdSketch::from_asapv1(
            &bytes,
        )?)),
        kind::ELASTIC => Ok(EhBucketSketch::ElasticSketch(
            crate::ElasticSketch::from_asapv1(&bytes)?,
        )),
        kind::HLL_ERTL => Ok(EhBucketSketch::Hll(
            crate::HyperLogLog::<crate::Ertl>::from_asapv1(&bytes)?,
        )),
        kind::KLL => Ok(EhBucketSketch::Kll(crate::Kll::from_asapv1(&bytes)?)),
        #[cfg(feature = "experimental")]
        kind::UNIFORM_SAMPLING => Ok(EhBucketSketch::Uniform(
            crate::UniformSampling::from_asapv1(&bytes)?,
        )),
        kind::UNIVMON => Ok(EhBucketSketch::UnivMon(crate::UnivMon::from_asapv1(
            &bytes,
        )?)),
        other => Err(unknown_variant(other)),
    }
}

// Wire serialization for EhBucketSketch. `wire` is a descendant of the union's
// module, so this impl matches its variants directly.
impl EhBucketSketch {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x14 0x00`). The payload is the triple
    /// `[kind_id, descriptor, state]`.
    ///
    /// A state the variant's own encoder refuses is an error rather than bytes
    /// that would be refused on decode.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = rmp_serde::to_vec_named(&eh_sketch_list_metadata())?;
        let payload = rmp_serde::to_vec(&sketch_state(self)?)?;
        Ok(envelope::encode(
            kind::EH_BUCKET_SKETCH,
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. The nested
    /// kind_id selects the decoder, which validates its own blocks.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::EH_BUCKET_SKETCH {
            return Err(RmpDecodeError::Uncategorized(format!(
                "EHSketchList kind_id mismatch: stored {kind_id:?}, expected {EH_BUCKET_SKETCH:?}"
            )));
        }
        let meta: EhSketchListMetadata = envelope::decode_block(metadata)?;
        if meta != eh_sketch_list_metadata() {
            return Err(RmpDecodeError::Uncategorized(
                "ASAPv1 EHSketchList envelope: metadata mismatch".to_string(),
            ));
        }
        let triple: SketchState = envelope::decode_block(payload)?;
        rebuild_sketch(&triple)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::sketch_framework::univmon::UnivMon;
    use crate::{
        CountMinSketch, CountSketch, CountSketchWithL2Norm, DEFAULT_CANONICAL_SEED_INDEX,
        DataInput, DdSketch, DefaultXxHasher, Ertl, FastPath, HashProfile, HyperLogLog, Kll,
        SketchHasher, Vector2D,
    };

    /// An input the named variant accepts. UnivMon's heaps hold one `OwnedDataInput`
    /// variant at a time, so its keys stay strings.
    pub(crate) fn sample_input(sketch_type: &str) -> DataInput<'static> {
        match sketch_type {
            "Coco" | "Elastic" | "UnivMon" => DataInput::Str("flow::a"),
            _ => DataInput::F64(7.5),
        }
    }

    /// One populated sketch per variant this build carries, in kind_id order.
    pub(crate) fn populated_variants() -> Vec<EhBucketSketch> {
        let mut out = vec![EhBucketSketch::CountMinSketch(CountMinSketch::<
            Vector2D<i32>,
            FastPath,
        >::with_dimensions(
            3, 8
        ))];
        out.push(EhBucketSketch::CocoSketch(
            crate::CocoSketch::init_with_size(16, 2),
        ));
        out.push(EhBucketSketch::CountSketchWithL2Norm(
            CountSketchWithL2Norm::with_dimensions(3, 256),
        ));
        out.push(EhBucketSketch::CountSketch(CountSketch::<
            Vector2D<i32>,
            FastPath,
        >::with_dimensions(3, 8)));
        out.push(EhBucketSketch::DdSketch(DdSketch::new(0.01)));
        out.push(EhBucketSketch::ElasticSketch(
            crate::ElasticSketch::with_heavy_and_light(8, 2, 256),
        ));
        out.push(EhBucketSketch::Hll(HyperLogLog::<Ertl>::default()));
        out.push(EhBucketSketch::Kll(Kll::with_k(200)));
        #[cfg(feature = "experimental")]
        out.push(EhBucketSketch::Uniform(crate::UniformSampling::new(0.5)));
        out.push(EhBucketSketch::UnivMon(UnivMon::default()));
        for sketch in &mut out {
            let key = sample_input(sketch.sketch_type());
            for _ in 0..5 {
                sketch.update(&key);
            }
        }
        out
    }

    /// A test-only custom hasher: hashes exactly like `DefaultXxHasher` but
    /// declares a DIFFERENT `HashProfile`.
    #[derive(Clone, Debug)]
    pub(crate) struct AltHasher;

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

    /// A Count-Min triple whose descriptor names a custom hash profile.
    pub(crate) fn alt_profile_triple() -> SketchState {
        let sketch = CountMinSketch::<Vector2D<i32>, FastPath, AltHasher>::with_dimensions(3, 8);
        let bytes = sketch.to_asapv1().expect("serialize alt CM");
        let (_, descriptor, state) = envelope::split(&bytes).expect("split alt CM");
        SketchState {
            kind_id: kind::COUNT_MIN.to_vec(),
            descriptor: descriptor.to_vec(),
            state: state.to_vec(),
        }
    }

    /// The triple a variant contributes, with the nested kind_id replaced.
    pub(crate) fn relabelled(sketch: &EhBucketSketch, kind_id: &[u8]) -> SketchState {
        let mut triple = sketch_state(sketch).expect("state");
        triple.kind_id = kind_id.to_vec();
        triple
    }

    /// Wraps a triple in a `0x14 0x00` envelope.
    pub(crate) fn envelope_for(triple: &SketchState) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(&eh_sketch_list_metadata()).unwrap();
        let payload = rmp_serde::to_vec(triple).unwrap();
        envelope::encode(kind::EH_BUCKET_SKETCH, &metadata, &payload)
    }

    #[test]
    fn eh_sketch_list_round_trip_serialization() {
        let mut sketch = EhBucketSketch::CountMinSketch(
            CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(3, 8),
        );
        sketch.update(&DataInput::U64(42));

        let encoded = sketch.to_asapv1().expect("serialize EHSketchList");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x14, 0x00]); // kind_id_len=2, kind_id=[0x14,0x00]

        let decoded = EhBucketSketch::from_asapv1(&encoded).expect("deserialize");
        assert_eq!(decoded.sketch_type(), "CountMin");
        assert_eq!(
            decoded.query(&DataInput::U64(42)),
            sketch.query(&DataInput::U64(42))
        );
    }

    /// Every variant this build carries round-trips, keeps its type, and
    /// re-serializes byte-identically.
    #[test]
    fn eh_sketch_list_every_variant_round_trips() {
        for sketch in populated_variants() {
            let encoded = sketch
                .to_asapv1()
                .unwrap_or_else(|e| panic!("serialize {}: {e}", sketch.sketch_type()));
            let decoded = EhBucketSketch::from_asapv1(&encoded)
                .unwrap_or_else(|e| panic!("deserialize {}: {e}", sketch.sketch_type()));
            assert_eq!(decoded.sketch_type(), sketch.sketch_type());
            let again = decoded.to_asapv1().expect("re-serialize");
            assert_eq!(
                encoded,
                again,
                "{} is not byte-stable",
                sketch.sketch_type()
            );
        }
    }

    /// The ten nested kind_ids and their registry names are the same in every
    /// build, and every variant emits the id the table gives it.
    #[test]
    fn eh_sketch_list_kind_ids_are_build_independent() {
        let table: [(&[u8], &str); 10] = [
            (kind::COUNT_MIN, "CountMin"),
            (kind::COCO, "Coco"),
            (kind::COUNT_SKETCH_WITH_L2_NORM, "CountL2HH"),
            (kind::COUNT_SKETCH, "CountSketch"),
            (kind::DDSKETCH, "DDSketch"),
            (kind::ELASTIC, "Elastic"),
            (kind::HLL_ERTL, "HLL"),
            (kind::KLL, "KLL"),
            (kind::UNIFORM_SAMPLING, "UniformSampling"),
            (kind::UNIVMON, "UnivMon"),
        ];
        for (kind_id, name) in table {
            assert_eq!(variant_name(kind_id), Some(name), "{kind_id:02x?}");
        }
        assert_eq!(variant_name(&[0xff, 0xff]), None);

        for sketch in populated_variants() {
            let emitted = sketch_state(&sketch).expect("state").kind_id;
            let expected = table
                .iter()
                .find(|(_, name)| *name == sketch.sketch_type())
                .expect("variant in table")
                .0;
            assert_eq!(emitted, expected, "{}", sketch.sketch_type());
        }
    }

    /// The experimental kind_id is rejected without the feature, with an error
    /// naming the variant. Crafted bytes, so the test runs in both builds.
    #[test]
    fn eh_sketch_list_rejects_an_experimental_kind_id() {
        let triple = SketchState {
            kind_id: kind::UNIFORM_SAMPLING.to_vec(),
            descriptor: Vec::new(),
            state: Vec::new(),
        };
        let message = EhBucketSketch::from_asapv1(&envelope_for(&triple))
            .expect_err("an empty variant state must not decode")
            .to_string();
        assert!(!message.is_empty());
        #[cfg(not(feature = "experimental"))]
        {
            assert!(message.contains("UniformSampling"), "{message}");
            assert!(message.contains("experimental"), "{message}");
        }
    }

    #[test]
    fn eh_sketch_list_rejects_an_unknown_kind_id() {
        let sketch = EhBucketSketch::CountMinSketch(
            CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(3, 8),
        );
        let triple = relabelled(&sketch, &[0xff, 0xff]);
        let message = EhBucketSketch::from_asapv1(&envelope_for(&triple))
            .expect_err("an unknown kind_id must not decode")
            .to_string();
        assert!(message.contains("not a wire variant"), "{message}");
    }

    /// A nested kind_id longer than the envelope's one-byte length field is
    /// rejected as unknown, before any block is assembled.
    #[test]
    fn eh_sketch_list_rejects_an_over_long_kind_id() {
        for len in [256usize, 4096] {
            let triple = SketchState {
                kind_id: vec![0x14; len],
                descriptor: Vec::new(),
                state: Vec::new(),
            };
            let message = EhBucketSketch::from_asapv1(&envelope_for(&triple))
                .expect_err("an over-long kind_id must not decode")
                .to_string();
            assert!(message.contains("not a wire variant"), "{message}");
        }
    }

    /// The nested ids are pinned to one algorithm each: HLL is Ertl-MLE and KLL
    /// is compact, so their siblings' ids are unknown here.
    #[test]
    fn eh_sketch_list_rejects_sibling_algorithm_kind_ids() {
        let hll = EhBucketSketch::Hll(HyperLogLog::<Ertl>::default());
        for sibling in [[0x01u8, 0x01], [0x01, 0x03]] {
            let triple = relabelled(&hll, &sibling);
            assert!(
                EhBucketSketch::from_asapv1(&envelope_for(&triple)).is_err(),
                "{sibling:02x?} must not decode as the HLL arm"
            );
        }
        let kll = EhBucketSketch::Kll(Kll::with_k(200));
        let triple = relabelled(&kll, &[0x06, 0x01]);
        assert!(EhBucketSketch::from_asapv1(&envelope_for(&triple)).is_err());
    }

    /// A kind_id that does not match the blocks it carries is rejected by the
    /// variant's own decoder.
    #[test]
    fn eh_sketch_list_rejects_a_mismatched_kind_id_and_descriptor() {
        let sketch = EhBucketSketch::Hll(HyperLogLog::<Ertl>::default());
        let triple = relabelled(&sketch, kind::COUNT_MIN);
        assert!(EhBucketSketch::from_asapv1(&envelope_for(&triple)).is_err());
    }

    /// A Count-Min envelope and an ExponentialHistogram envelope are not
    /// EhBucketSketch envelopes.
    #[test]
    fn eh_sketch_list_rejects_foreign_kind_ids() {
        let cms = CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(3, 8);
        let cms_bytes = cms.to_asapv1().expect("serialize CMS");
        assert!(EhBucketSketch::from_asapv1(&cms_bytes).is_err());

        let eh = crate::ExponentialHistogram::new(
            2,
            100,
            EhBucketSketch::CountMinSketch(
                CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(3, 8),
            ),
        );
        let eh_bytes = eh.to_asapv1().expect("serialize EH");
        assert!(EhBucketSketch::from_asapv1(&eh_bytes).is_err());
    }

    /// Fail closed on an unexpected metadata key.
    #[test]
    fn eh_sketch_list_metadata_rejects_unknown_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            bogus_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            metadata_version: 1,
            bogus_field: 7,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<EhSketchListMetadata>(&bytes).is_err());
    }

    /// `metadata_version` is required: a map missing it does not decode.
    #[test]
    fn eh_sketch_list_metadata_rejects_a_missing_key() {
        #[derive(Serialize)]
        struct Empty {}
        let bytes = rmp_serde::to_vec_named(&Empty {}).unwrap();
        assert!(rmp_serde::from_slice::<EhSketchListMetadata>(&bytes).is_err());
    }

    /// Crafted blocks fail closed with an error, never a panic.
    #[test]
    fn eh_sketch_list_rejects_crafted_blocks() {
        let sketch = EhBucketSketch::CountMinSketch(
            CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(3, 8),
        );
        let good = sketch_state(&sketch).expect("state");

        let truncated = SketchState {
            kind_id: kind::COUNT_MIN.to_vec(),
            descriptor: good.descriptor[..good.descriptor.len() / 2].to_vec(),
            state: good.state.clone(),
        };
        assert!(EhBucketSketch::from_asapv1(&envelope_for(&truncated)).is_err());

        let garbage = SketchState {
            kind_id: kind::COUNT_MIN.to_vec(),
            descriptor: good.descriptor.clone(),
            state: vec![0xc1, 0xc1, 0xc1],
        };
        assert!(EhBucketSketch::from_asapv1(&envelope_for(&garbage)).is_err());
    }

    /// A descriptor naming a custom hash profile is rejected: the variant's
    /// decoder pins the profile of the type it rebuilds.
    #[test]
    fn eh_sketch_list_rejects_a_custom_hash_profile_descriptor() {
        let triple = alt_profile_triple();
        assert!(EhBucketSketch::from_asapv1(&envelope_for(&triple)).is_err());
    }

    /// A decoded union answers a query the way the original did.
    #[test]
    fn eh_sketch_list_query_agrees_after_decode() {
        for sketch in populated_variants() {
            let key = sample_input(sketch.sketch_type());
            let encoded = sketch.to_asapv1().expect("serialize");
            let decoded = EhBucketSketch::from_asapv1(&encoded).expect("deserialize");
            assert_eq!(
                sketch.query(&key).ok(),
                decoded.query(&key).ok(),
                "{} disagrees after decode",
                sketch.sketch_type()
            );
        }
    }
}
