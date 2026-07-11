//! ASAPv1 wire serialization for the HyperLogLog sketches.
//!
//! Child submodule of [`crate::sketches::hll`]: it holds ALL of HLL's
//! serialization (the metadata/payload DTOs, kind_id constants, the
//! [`HllWireVariant`] mapping, and the `serialize_to_bytes` /
//! `deserialize_from_bytes` impls) while the algorithm lives in the parent
//! module file (`hll.rs`). Being a descendant module, it can read the sketch structs' private
//! fields (`self.registers`) and construct them directly without widening any
//! field visibility. See `docs/asapv1_wire_format.md`.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::message_pack_format::envelope;
use crate::structures::fixed_structure::HllRegisterStorage;
use crate::{DefaultXxHasher, HashProfile, SketchHasher};

use super::{Classic, ErtlMLE, HyperLogLogHIPImpl, HyperLogLogImpl};

/// Maps an in-memory HLL estimator variant to its ASAPv1 `kind_id`.
pub trait HllWireVariant {
    /// The `[family, variant]` kind_id for this estimator.
    const WIRE_KIND_ID: &'static [u8];
}

impl HllWireVariant for Classic {
    const WIRE_KIND_ID: &'static [u8] = HLL_KIND_CLASSIC;
}

impl HllWireVariant for ErtlMLE {
    const WIRE_KIND_ID: &'static [u8] = HLL_KIND_ERTL_MLE;
}

/// HLL payload (ASAPv1 §3.1), serialized as a msgpack **array** (`to_vec`,
/// positional). `registers` is a msgpack `bin` (one byte per register, matching
/// Go's `[]byte`) via `serde_bytes` rather than serde's default `array<u8>`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadPlain {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
}

/// HIP payload: the register bin plus the three HIP running scalars.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadHip {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
    pub(crate) hip_kxq0: f64,
    pub(crate) hip_kxq1: f64,
    pub(crate) hip_est: f64,
}

const HLL_KIND_FAMILY: u8 = 0x01;
pub(crate) const HLL_KIND_CLASSIC: &[u8] = &[HLL_KIND_FAMILY, 0x01];
pub(crate) const HLL_KIND_ERTL_MLE: &[u8] = &[HLL_KIND_FAMILY, 0x02];
pub(crate) const HLL_KIND_HIP: &[u8] = &[HLL_KIND_FAMILY, 0x03];

/// Descriptor metadata for an HLL sketch (ASAPv1 §2), serialized as a msgpack
/// **map** (`to_vec_named`) with keys in this declaration order — the canonical
/// order the wire spec fixes. HLL carries the full hash spec (including the
/// inlined `seed_list`, so the bytes are self-describing) plus the seed index it
/// uses and its one structural param, `precision`. `deny_unknown_fields` makes
/// decode fail closed on any unexpected key rather than silently dropping it.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HllMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) seed_list: Vec<u64>,
    pub(crate) canonical_seed_index: u32,
    pub(crate) precision: u32,
}

/// Builds the HLL descriptor metadata from the hasher's [`HashProfile`], so the
/// wire bytes truthfully describe how the sketch was hashed (rather than
/// hardcoding the standard profile).
pub(crate) fn hll_metadata<H: HashProfile>(precision: u32) -> HllMetadata {
    HllMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        canonical_seed_index: H::CANONICAL_SEED_INDEX,
        precision,
    }
}

/// The standard ProjectASAP profile metadata (the [`DefaultXxHasher`] profile).
/// Used by the portable path, which only represents standard-profile sketches.
pub(crate) fn standard_hll_metadata(precision: u32) -> HllMetadata {
    hll_metadata::<DefaultXxHasher>(precision)
}

/// Validate the envelope for a known target and return the raw payload bytes.
/// The metadata is checked against the profile of `H`, so bytes hashed under a
/// different profile are rejected (fail closed).
fn validated_hll_payload<'a, H: HashProfile>(
    bytes: &'a [u8],
    expected_kind_id: &[u8],
    expected_precision: u32,
) -> Result<&'a [u8], RmpDecodeError> {
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != expected_kind_id {
        return Err(RmpDecodeError::Uncategorized(format!(
            "HLL kind_id mismatch: stored {kind_id:?}, expected {expected_kind_id:?}"
        )));
    }
    let meta: HllMetadata = from_slice(metadata)?;
    if meta != hll_metadata::<H>(expected_precision) {
        return Err(RmpDecodeError::Uncategorized(
            "ASAPv1 HLL envelope: metadata mismatch".to_string(),
        ));
    }
    Ok(payload)
}

/// Rebuild register storage from the payload's register bin.
fn registers_from_bytes<Registers: HllRegisterStorage>(
    registers: &[u8],
) -> Result<Registers, RmpDecodeError> {
    if registers.len() != Registers::NUM_REGISTERS {
        return Err(RmpDecodeError::Uncategorized(format!(
            "HLL register length mismatch: stored {}, expected {}",
            registers.len(),
            Registers::NUM_REGISTERS
        )));
    }
    let mut out = Registers::default();
    out.as_mut_slice().copy_from_slice(registers);
    Ok(out)
}

// Wire serialization for the generic HyperLogLog estimator family. `wire` is a
// descendant of the sketch module, so these impls read the private `registers`
// field and construct the struct directly.
impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    /// Serializes the sketch into an ASAPv1 MessagePack envelope. The metadata is
    /// derived from the hasher's [`HashProfile`], so it truthfully describes how
    /// the sketch was hashed.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
    where
        Variant: HllWireVariant,
        H: HashProfile,
    {
        let metadata = rmp_serde::to_vec_named(&hll_metadata::<H>(Registers::PRECISION as u32))?;
        let payload = rmp_serde::to_vec(&HllPayloadPlain {
            registers: self.registers.as_slice().to_vec(),
        })?;
        Ok(envelope::encode(Variant::WIRE_KIND_ID, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. Bytes whose
    /// metadata does not match this hasher's [`HashProfile`] are rejected.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
    where
        Variant: HllWireVariant,
        H: HashProfile,
    {
        let payload =
            validated_hll_payload::<H>(bytes, Variant::WIRE_KIND_ID, Registers::PRECISION as u32)?;
        let payload: HllPayloadPlain = from_slice(payload)?;
        let registers = registers_from_bytes::<Registers>(&payload.registers)?;
        Ok(Self {
            registers,
            _marker: PhantomData,
            _hasher: PhantomData,
        })
    }
}

// Wire serialization for the HIP estimator.
impl<Registers: HllRegisterStorage> HyperLogLogHIPImpl<Registers> {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata =
            rmp_serde::to_vec_named(&standard_hll_metadata(Registers::PRECISION as u32))?;
        let payload = rmp_serde::to_vec(&HllPayloadHip {
            registers: self.registers.as_slice().to_vec(),
            hip_kxq0: self.kxq0,
            hip_kxq1: self.kxq1,
            hip_est: self.est,
        })?;
        Ok(envelope::encode(HLL_KIND_HIP, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = validated_hll_payload::<DefaultXxHasher>(
            bytes,
            HLL_KIND_HIP,
            Registers::PRECISION as u32,
        )?;
        let payload: HllPayloadHip = from_slice(payload)?;
        let registers = registers_from_bytes::<Registers>(&payload.registers)?;
        Ok(Self {
            registers,
            kxq0: payload.hip_kxq0,
            kxq1: payload.hip_kxq1,
            est: payload.hip_est,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::hll::{HyperLogLog, HyperLogLogHIP, HyperLogLogHIPP12, HyperLogLogP12};
    use crate::structures::fixed_structure::{HllBucketListP12, HllBucketListP14};
    use crate::{DataInput, HllBucketList};

    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    // Minimal test harness (mirrors the algorithm-side one in `mod.rs`) so the
    // serialization round-trip tests can drive any (variant × precision) sketch
    // generically.
    trait HllEstimator: Default {
        fn push(&mut self, input: &DataInput);
        fn estimate(&self) -> f64;
    }

    trait HllSerializable: HllEstimator {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>;
        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
        where
            Self: Sized;
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.insert(input);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Classic, Registers, H>::estimate(self) as f64
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher + HashProfile> HllSerializable
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::deserialize_from_bytes(bytes)
        }
    }

    macro_rules! impl_ertl_mle_wire_test_traits {
        ($storage:ty) => {
            impl<H: SketchHasher> HllEstimator for HyperLogLogImpl<ErtlMLE, $storage, H> {
                fn push(&mut self, input: &DataInput) {
                    self.insert(input);
                }

                fn estimate(&self) -> f64 {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::estimate(self) as f64
                }
            }

            impl<H: SketchHasher + HashProfile> HllSerializable
                for HyperLogLogImpl<ErtlMLE, $storage, H>
            {
                fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::serialize_to_bytes(self)
                }

                fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::deserialize_from_bytes(bytes)
                }
            }
        };
    }

    impl_ertl_mle_wire_test_traits!(HllBucketListP12);
    impl_ertl_mle_wire_test_traits!(HllBucketListP14);

    impl<Registers: HllRegisterStorage> HllEstimator for HyperLogLogHIPImpl<Registers> {
        fn push(&mut self, input: &DataInput) {
            self.insert(input);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogHIPImpl::<Registers>::estimate(self) as f64
        }
    }

    impl<Registers: HllRegisterStorage> HllSerializable for HyperLogLogHIPImpl<Registers> {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogHIPImpl::<Registers>::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogHIPImpl::<Registers>::deserialize_from_bytes(bytes)
        }
    }

    fn assert_serialization_round_trip<S>(name: &str)
    where
        S: HllSerializable,
    {
        let mut sketch = S::default();
        for value in 0..SERDE_SAMPLE {
            let input = DataInput::U64(value as u64);
            sketch.push(&input);
        }

        let encoded = sketch
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("{name} serialize_to_bytes failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "{name} serialization output should not be empty"
        );

        let decoded = S::deserialize_from_bytes(&encoded)
            .unwrap_or_else(|err| panic!("{name} deserialize_from_bytes failed: {err}"));

        let reencoded = decoded
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("{name} re-serialize failed: {err}"));

        assert_eq!(
            encoded, reencoded,
            "{name} serialized bytes differed after round trip"
        );

        let original_est = sketch.estimate();
        let decoded_est = decoded.estimate();
        assert!(
            (original_est - decoded_est).abs() <= ERROR_TOLERANCE * original_est.max(1.0),
            "{name} estimate mismatch after round trip: before {original_est}, after {decoded_est}"
        );
    }

    #[test]
    fn hyperloglog_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLog<Classic>>("HyperLogLog");
    }

    #[test]
    fn hll_ertl_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLog<ErtlMLE>>("HllErtl");
    }

    #[test]
    fn hllds_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHIP>("HllDs");
    }

    #[test]
    fn hyperloglog_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<Classic>>("HyperLogLogP12");
    }

    #[test]
    fn hll_ertl_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<ErtlMLE>>("HllErtlP12");
    }

    #[test]
    fn hllds_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHIPP12>("HllDsP12");
    }

    #[test]
    fn hll_envelope_structure_and_kind_id_guard() {
        // ASAPv1 envelope header for an Ertl-MLE sketch: magic, version 0x01,
        // kind_id_len 2, kind_id [0x01, 0x02].
        let mut sketch = HyperLogLog::<ErtlMLE>::default();
        for value in 0..1000 {
            sketch.insert(&DataInput::U64(value));
        }
        let bytes = sketch.serialize_to_bytes().expect("serialize");

        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(bytes[6], envelope::VERSION);
        assert_eq!(bytes[7], 2, "kind_id_len");
        assert_eq!(&bytes[8..10], HLL_KIND_ERTL_MLE);

        let decoded = HyperLogLog::<ErtlMLE>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.registers_as_slice(), sketch.registers_as_slice());

        // A Classic decoder must reject Ertl-MLE bytes (kind_id mismatch).
        assert!(HyperLogLog::<Classic>::deserialize_from_bytes(&bytes).is_err());
    }

    #[test]
    fn hll_hip_round_trip_preserves_state() {
        let mut sketch = HyperLogLogHIP::default();
        for value in 0..1000 {
            sketch.insert(&DataInput::U64(value));
        }
        let bytes = sketch.serialize_to_bytes().expect("serialize");
        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(&bytes[8..10], HLL_KIND_HIP);

        let decoded = HyperLogLogHIP::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.registers.as_slice(), sketch.registers.as_slice());
        assert_eq!(decoded.kxq0, sketch.kxq0);
        assert_eq!(decoded.kxq1, sketch.kxq1);
        assert_eq!(decoded.est, sketch.est);
    }

    /// Drift guard: the native path and the (deprecated) portable path must emit
    /// byte-identical ASAPv1 envelopes. Keep until golden byte-vectors exist.
    #[test]
    fn native_and_portable_hll_bytes_match() {
        use crate::message_pack_format::MessagePackCodec;
        use crate::message_pack_format::portable::hll::{HllSketch, HllVariant};

        // Ertl-MLE / Datafusion: registers-only payload.
        let mut native = HyperLogLog::<ErtlMLE>::default();
        for v in 0..1000 {
            native.insert(&DataInput::U64(v));
        }
        let native_bytes = native.serialize_to_bytes().expect("native serialize");
        let portable = HllSketch::from_raw(
            HllVariant::Datafusion,
            HllBucketList::PRECISION as u32,
            native.registers_as_slice().to_vec(),
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(
            native_bytes,
            portable.to_msgpack().expect("portable serialize")
        );

        // HIP: registers + three running scalars.
        let mut hip = HyperLogLogHIP::default();
        for v in 0..1000 {
            hip.insert(&DataInput::U64(v));
        }
        let hip_bytes = hip.serialize_to_bytes().expect("native serialize");
        let portable_hip = HllSketch::from_raw(
            HllVariant::Hip,
            HllBucketList::PRECISION as u32,
            hip.registers.as_slice().to_vec(),
            hip.kxq0,
            hip.kxq1,
            hip.est,
        );
        assert_eq!(
            hip_bytes,
            portable_hip.to_msgpack().expect("portable serialize")
        );
    }

    // A test-only custom hasher: it hashes exactly like `DefaultXxHasher`
    // (delegation) but declares a DIFFERENT `HashProfile` (distinct profile id
    // and seed list). Serialization is derived from the profile, so an
    // `AltHasher` sketch serializes truthfully and its metadata differs from the
    // standard profile on the wire.
    //
    // Note: that an *unprofiled* hasher cannot be serialized is a compile-time
    // guarantee — `serialize_to_bytes`/`deserialize_from_bytes` are bounded on
    // `H: HashProfile`, so a hasher that impls only `SketchHasher` fails to
    // compile. There is nothing to assert at runtime.
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
            // A deliberately different seed list so the wire bytes differ.
            vec![1, 2, 3, 4, 5]
        }
        const CANONICAL_SEED_INDEX: u32 = crate::CANONICAL_HASH_SEED as u32;
        const MATRIX_SEED_INDEX: u32 = 0;
    }

    #[test]
    fn hll_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) An HLL built with a custom-profile hasher round-trips.
        let mut alt = HyperLogLogImpl::<ErtlMLE, HllBucketListP14, AltHasher>::default();
        let mut std = HyperLogLog::<ErtlMLE>::default();
        for v in 0..1000 {
            alt.insert(&DataInput::U64(v));
            std.insert(&DataInput::U64(v));
        }
        let alt_bytes = alt.serialize_to_bytes().expect("alt serialize");
        let decoded =
            HyperLogLogImpl::<ErtlMLE, HllBucketListP14, AltHasher>::deserialize_from_bytes(
                &alt_bytes,
            )
            .expect("alt decode");
        assert_eq!(decoded.registers_as_slice(), alt.registers_as_slice());

        // (b) Its bytes differ from the DefaultXxHasher sketch's bytes: the
        // metadata is derived from the (different) profile, so it is not a lie.
        let std_bytes = std.serialize_to_bytes().expect("std serialize");
        assert_ne!(
            alt_bytes, std_bytes,
            "a custom profile must serialize different metadata than the standard profile"
        );

        // (c) Decoding AltHasher bytes into a DefaultXxHasher-typed HLL fails
        // closed (profile mismatch), never silently accepted.
        assert!(
            HyperLogLog::<ErtlMLE>::deserialize_from_bytes(&alt_bytes).is_err(),
            "standard-profile decode must reject custom-profile bytes"
        );
    }

    /// Fail closed: metadata carrying an unexpected key must be rejected, not
    /// silently dropped (`deny_unknown_fields`).
    #[test]
    fn hll_metadata_rejects_unknown_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            canonical_seed_index: u32,
            precision: u32,
            bogus_field: u8, // key not in HllMetadata
        }
        let std = standard_hll_metadata(14);
        let extra = WithExtra {
            metadata_version: std.metadata_version,
            hash_profile_id: std.hash_profile_id.clone(),
            hash_algorithm: std.hash_algorithm.clone(),
            seed_derivation: std.seed_derivation.clone(),
            input_encoding: std.input_encoding.clone(),
            seed_list: std.seed_list.clone(),
            canonical_seed_index: std.canonical_seed_index,
            precision: std.precision,
            bogus_field: 7,
        };
        let bytes = rmp_serde::to_vec_named(&extra).expect("encode");
        assert!(
            rmp_serde::from_slice::<HllMetadata>(&bytes).is_err(),
            "an unexpected metadata key must be rejected"
        );
    }

    /// A decoder rejects bytes whose precision differs from the target storage:
    /// P12 bytes must not decode into a P14-typed sketch (metadata mismatch).
    #[test]
    fn hll_precision_cross_rejection() {
        let mut p12 = HyperLogLogP12::<Classic>::default();
        for v in 0..100 {
            p12.insert(&DataInput::U64(v));
        }
        let bytes = p12.serialize_to_bytes().expect("serialize");
        assert!(
            HyperLogLog::<Classic>::deserialize_from_bytes(&bytes).is_err(),
            "P12 bytes must be rejected by a P14 decoder"
        );
    }

    /// A Classic decoder rejects HIP bytes (kind_id mismatch).
    #[test]
    fn hll_hip_kind_id_rejected_by_classic() {
        let mut hip = HyperLogLogHIP::default();
        for v in 0..100 {
            hip.insert(&DataInput::U64(v));
        }
        let bytes = hip.serialize_to_bytes().expect("serialize");
        assert!(
            HyperLogLog::<Classic>::deserialize_from_bytes(&bytes).is_err(),
            "HIP bytes must be rejected by a Classic decoder"
        );
    }
}
