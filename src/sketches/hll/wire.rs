//! ASAPv1 wire serialization for the HyperLogLog sketches.
//!
//! Child submodule of [`crate::sketches::hll`]: it holds HLL's state
//! serialization (the metadata/payload DTOs, kind_id constants, the
//! [`HllWireVariant`] mapping, and the `to_asapv1` /
//! `from_asapv1` impls) while the algorithm lives in the parent
//! module file (`hll.rs`) and the delta kinds in `delta.rs`. Being a
//! descendant module, it can read the sketch structs' private fields
//! (`self.registers`) and construct them directly without widening any field
//! visibility. See `docs/asapv1_wire_format.md`.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::asapv1::admission_rate;
use crate::asapv1::envelope;
use crate::asapv1::kind;
use crate::asapv1::wire_key::exact;
use crate::structures::fixed_structure::HllRegisterStorage;
use crate::{DefaultXxHasher, HashProfile, SketchHasher};

use super::{Classic, Ertl, HyperLogLogHipImpl, HyperLogLogImpl};

/// Maps an in-memory HLL estimator variant to its ASAPv1 `kind_id`.
pub trait HllWireVariant {
    /// The `[family, variant]` kind_id for this estimator.
    const WIRE_KIND_ID: &'static [u8];
    /// The kind_id of this estimator's delta.
    const WIRE_DELTA_KIND_ID: &'static [u8];
}

impl HllWireVariant for Classic {
    const WIRE_KIND_ID: &'static [u8] = kind::HLL_CLASSIC;
    const WIRE_DELTA_KIND_ID: &'static [u8] = kind::HLL_CLASSIC_DELTA;
}

impl HllWireVariant for Ertl {
    const WIRE_KIND_ID: &'static [u8] = kind::HLL_ERTL;
    const WIRE_DELTA_KIND_ID: &'static [u8] = kind::HLL_ERTL_DELTA;
}

/// HLL payload (ASAPv1 §3.1), serialized as a msgpack **array** (`to_vec`,
/// positional). `registers` is a msgpack `bin` (one byte per register, matching
/// Go's `[]byte`) via `serde_bytes` rather than serde's default `array<u8>`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadPlain {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
}

/// HIP payload (`01 04`): the register bin plus the running HIP estimate.
/// `kxq0` / `kxq1` are derived from the registers and not carried.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadHip {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
    #[serde(deserialize_with = "exact")]
    pub(crate) hip_est: f64,
}

/// The retired HIP payload (`01 03`), decode-only: the register bin, then
/// `kxq0`, `kxq1` and the estimate. The two sums are read and dropped.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadHipWithKxq {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
    #[serde(deserialize_with = "exact")]
    pub(crate) hip_kxq0: f64,
    #[serde(deserialize_with = "exact")]
    pub(crate) hip_kxq1: f64,
    #[serde(deserialize_with = "exact")]
    pub(crate) hip_est: f64,
}

/// Descriptor metadata for an HLL sketch (ASAPv1 §2), serialized as a msgpack
/// **map** (`to_vec_named`) with keys in this declaration order — the canonical
/// order the wire spec fixes. HLL carries the full hash spec (including the
/// inlined `seed_list`, so the bytes are self-describing) plus the seed index it
/// uses and its one structural param, `precision`, then the optional
/// `admission_rate`. `deny_unknown_fields` makes decode fail closed on any
/// unexpected key rather than silently dropping it.
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "admission_rate::deserialize"
    )]
    pub(crate) admission_rate: Option<f64>,
}

/// Builds the HLL descriptor metadata from the hasher's [`HashProfile`], so the
/// wire bytes truthfully describe how the sketch was hashed (rather than
/// hardcoding the standard profile).
pub(crate) fn hll_metadata<H: HashProfile>(
    precision: u32,
    admission_rate: Option<f64>,
) -> HllMetadata {
    HllMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        canonical_seed_index: H::CANONICAL_SEED_INDEX,
        precision,
        admission_rate,
    }
}

/// The standard ProjectASAP profile metadata (the [`DefaultXxHasher`] profile).
#[cfg(test)]
fn standard_hll_metadata(precision: u32) -> HllMetadata {
    hll_metadata::<DefaultXxHasher>(precision, None)
}

/// The metadata block of a sketch with `Registers` storage hashed under `H`
/// and admitting updates at `admission_rate`.
pub(super) fn encode_metadata<Registers: HllRegisterStorage, H: HashProfile>(
    admission_rate: f64,
) -> Result<Vec<u8>, RmpEncodeError> {
    let admission_rate = admission_rate::to_key(admission_rate)
        .map_err(|e| RmpEncodeError::Syntax(format!("ASAPv1 HLL envelope: {e}")))?;
    rmp_serde::to_vec_named(&hll_metadata::<H>(
        Registers::PRECISION as u32,
        admission_rate,
    ))
}

/// Validate the envelope for a known target and return the raw payload bytes
/// and the admission rate. The metadata is checked against the profile of `H`,
/// so bytes hashed under a different profile are rejected (fail closed).
pub(super) fn validated_hll_payload<'a, H: HashProfile>(
    bytes: &'a [u8],
    expected_kind_id: &[u8],
    expected_precision: u32,
) -> Result<(&'a [u8], f64), RmpDecodeError> {
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != expected_kind_id {
        return Err(RmpDecodeError::Uncategorized(format!(
            "HLL kind_id mismatch: stored {kind_id:?}, expected {expected_kind_id:?}"
        )));
    }
    let meta: HllMetadata = envelope::decode_block(metadata)?;
    let admission_rate = admission_rate::from_key(meta.admission_rate)
        .map_err(|e| RmpDecodeError::Uncategorized(format!("ASAPv1 HLL envelope: {e}")))?;
    if meta != hll_metadata::<H>(expected_precision, meta.admission_rate) {
        return Err(RmpDecodeError::Uncategorized(
            "ASAPv1 HLL envelope: metadata mismatch".to_string(),
        ));
    }
    Ok((payload, admission_rate))
}

/// Rejects a register bin holding a rank no insert could have produced. A
/// register holds a rank of at most `RANK_HASH_BITS + 1`, so a byte above that
/// bound is an impossible state. Both directions call this, so the encode and
/// decode predicates cannot drift.
pub(super) fn check_register_range<Registers: HllRegisterStorage>(
    registers: &[u8],
) -> Result<(), String> {
    let max_rank = (Registers::RANK_HASH_BITS + 1) as u8;
    match registers.iter().find(|&&rank| rank > max_rank) {
        Some(&rank) => Err(format!(
            "HLL register value {rank} exceeds the maximum rank {max_rank} at precision {}",
            Registers::PRECISION
        )),
        None => Ok(()),
    }
}

/// The register bin to serialize, or an error when the sketch holds a rank no
/// insert could have produced — such a sketch has no encoding rather than bytes
/// its own decoder would refuse.
fn registers_to_bytes<Registers: HllRegisterStorage>(
    registers: &Registers,
) -> Result<Vec<u8>, RmpEncodeError> {
    let registers = registers.as_slice();
    check_register_range::<Registers>(registers)
        .map_err(|problem| RmpEncodeError::Syntax(format!("ASAPv1 HLL envelope: {problem}")))?;
    Ok(registers.to_vec())
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
    check_register_range::<Registers>(registers).map_err(RmpDecodeError::Uncategorized)?;
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
    ///
    /// A register above the maximum rank for this precision is an error rather
    /// than bytes that would be refused on decode.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
    where
        Variant: HllWireVariant,
        H: HashProfile,
    {
        let registers = registers_to_bytes(&self.registers)?;
        let metadata = encode_metadata::<Registers, H>(self.admission_rate)?;
        let payload = rmp_serde::to_vec(&HllPayloadPlain { registers })?;
        Ok(envelope::encode(Variant::WIRE_KIND_ID, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. Bytes whose
    /// metadata does not match this hasher's [`HashProfile`] are rejected.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
    where
        Variant: HllWireVariant,
        H: HashProfile,
    {
        let (payload, admission_rate) =
            validated_hll_payload::<H>(bytes, Variant::WIRE_KIND_ID, Registers::PRECISION as u32)?;
        let payload: HllPayloadPlain = envelope::decode_block(payload)?;
        let registers = registers_from_bytes::<Registers>(&payload.registers)?;
        Ok(Self {
            registers,
            _marker: PhantomData,
            _hasher: PhantomData,
            admission_rate,
        })
    }
}

// Wire serialization for the HIP estimator.
impl<Registers: HllRegisterStorage> HyperLogLogHipImpl<Registers> {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope.
    ///
    /// A register above the maximum rank for this precision is an error rather
    /// than bytes that would be refused on decode.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let registers = registers_to_bytes(&self.registers)?;
        let metadata = encode_metadata::<Registers, DefaultXxHasher>(self.admission_rate)?;
        let payload = rmp_serde::to_vec(&HllPayloadHip {
            registers,
            hip_est: self.est,
        })?;
        Ok(envelope::encode(kind::HLL_HIP, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope: the current
    /// `01 04`, or the retired `01 03`, whose `kxq0` / `kxq1` are dropped.
    /// Both are recomputed from the registers either way.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (stored_kind, _, _) = envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        let retired = stored_kind == kind::HLL_HIP_WITH_KXQ;
        let expected_kind = if retired {
            kind::HLL_HIP_WITH_KXQ
        } else {
            kind::HLL_HIP
        };
        let (payload, admission_rate) = validated_hll_payload::<DefaultXxHasher>(
            bytes,
            expected_kind,
            Registers::PRECISION as u32,
        )?;
        let (registers, est) = if retired {
            let payload: HllPayloadHipWithKxq = envelope::decode_block(payload)?;
            (payload.registers, payload.hip_est)
        } else {
            let payload: HllPayloadHip = envelope::decode_block(payload)?;
            (payload.registers, payload.hip_est)
        };
        let registers = registers_from_bytes::<Registers>(&registers)?;
        let (kxq0, kxq1) = super::hip_kxq(registers.as_slice());
        Ok(Self {
            registers,
            kxq0,
            kxq1,
            est,
            admission_rate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;
    use crate::sketches::hll::{HyperLogLog, HyperLogLogHip, HyperLogLogHipP12, HyperLogLogP12};
    use crate::structures::fixed_structure::{HllRegistersP12, HllRegistersP14};

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
        fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>;
        fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
        where
            Self: Sized;
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Classic, Registers, H>::estimate(self) as f64
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher + HashProfile> HllSerializable
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::to_asapv1(self)
        }

        fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::from_asapv1(bytes)
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Ertl, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Ertl, Registers, H>::estimate(self) as f64
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher + HashProfile> HllSerializable
        for HyperLogLogImpl<Ertl, Registers, H>
    {
        fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogImpl::<Ertl, Registers, H>::to_asapv1(self)
        }

        fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogImpl::<Ertl, Registers, H>::from_asapv1(bytes)
        }
    }

    impl<Registers: HllRegisterStorage> HllEstimator for HyperLogLogHipImpl<Registers> {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogHipImpl::<Registers>::estimate(self) as f64
        }
    }

    impl<Registers: HllRegisterStorage> HllSerializable for HyperLogLogHipImpl<Registers> {
        fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogHipImpl::<Registers>::to_asapv1(self)
        }

        fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogHipImpl::<Registers>::from_asapv1(bytes)
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
            .to_asapv1()
            .unwrap_or_else(|err| panic!("{name} to_asapv1 failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "{name} serialization output should not be empty"
        );

        let decoded = S::from_asapv1(&encoded)
            .unwrap_or_else(|err| panic!("{name} from_asapv1 failed: {err}"));

        let reencoded = decoded
            .to_asapv1()
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
        assert_serialization_round_trip::<HyperLogLog<Ertl>>("HllErtl");
    }

    #[test]
    fn hllds_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHip>("HllDs");
    }

    #[test]
    fn hyperloglog_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<Classic>>("HyperLogLogP12");
    }

    #[test]
    fn hll_ertl_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<Ertl>>("HllErtlP12");
    }

    #[test]
    fn hllds_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHipP12>("HllDsP12");
    }

    /// Known register and HIP state serializes to the `asapv1_golden/` bytes,
    /// and decodes back to that state.
    #[test]
    fn known_state_matches_golden() {
        use crate::asapv1::envelope::hex;

        fn plain<V: HllWireVariant, R: HllRegisterStorage>(
            regs: &[u8],
        ) -> HyperLogLogImpl<V, R, DefaultXxHasher> {
            HyperLogLogImpl {
                registers: registers_from_bytes(regs).expect("registers"),
                _marker: PhantomData,
                _hasher: PhantomData,
                admission_rate: 1.0,
            }
        }
        fn hip<R: HllRegisterStorage>(regs: &[u8], est: f64) -> HyperLogLogHipImpl<R> {
            let registers: R = registers_from_bytes(regs).expect("registers");
            let (kxq0, kxq1) = super::super::hip_kxq(registers.as_slice());
            HyperLogLogHipImpl {
                registers,
                kxq0,
                kxq1,
                est,
                admission_rate: 1.0,
            }
        }

        let mut p12 = vec![0u8; 4096];
        for (index, value) in [(0, 1), (1, 7), (100, 42), (4095, 3)] {
            p12[index] = value;
        }
        let mut p14 = vec![0u8; 16384];
        for (index, value) in [(0, 1), (1, 7), (8192, 42), (16383, 51)] {
            p14[index] = value;
        }

        let classic = hex(include_str!("../../../asapv1_golden/hll_classic_p12.hex"));
        let sketch = plain::<Classic, HllRegistersP12>(&p12);
        assert_eq!(sketch.to_asapv1().unwrap(), classic);
        let decoded = HyperLogLogImpl::<Classic, HllRegistersP12>::from_asapv1(&classic).unwrap();
        assert_eq!(decoded.registers.as_slice(), p12.as_slice());

        let ertl = hex(include_str!("../../../asapv1_golden/hll_ertl_mle_p12.hex"));
        let sketch = plain::<Ertl, HllRegistersP12>(&p12);
        assert_eq!(sketch.to_asapv1().unwrap(), ertl);
        let decoded = HyperLogLogImpl::<Ertl, HllRegistersP12>::from_asapv1(&ertl).unwrap();
        assert_eq!(decoded.registers.as_slice(), p12.as_slice());

        // The current `01 04` fixtures: the registers and the estimate.
        let hip_p12 = hex(include_str!("../../../asapv1_golden/hll_hip_est_p12.hex"));
        let hip_p14 = hex(include_str!("../../../asapv1_golden/hll_hip_est_p14.hex"));
        assert_eq!(
            hip::<HllRegistersP12>(&p12, 3.0).to_asapv1().unwrap(),
            hip_p12
        );
        assert_eq!(
            hip::<HllRegistersP14>(&p14, 4.125).to_asapv1().unwrap(),
            hip_p14
        );
        let decoded = HyperLogLogHipImpl::<HllRegistersP12>::from_asapv1(&hip_p12).unwrap();
        assert_eq!(decoded.registers.as_slice(), p12.as_slice());
        assert_eq!(decoded.est, 3.0);
        let decoded = HyperLogLogHipImpl::<HllRegistersP14>::from_asapv1(&hip_p14).unwrap();
        assert_eq!(decoded.registers.as_slice(), p14.as_slice());
        assert_eq!(decoded.est, 4.125);

        // The retired `01 03` fixtures carry `kxq0` / `kxq1` values no
        // register state produces; decoding drops them for the sums over the
        // registers and keeps the estimate.
        let retired = hex(include_str!("../../../asapv1_golden/hll_hip_p12.hex"));
        let decoded = HyperLogLogHipImpl::<HllRegistersP12>::from_asapv1(&retired).unwrap();
        assert_eq!(decoded.registers.as_slice(), p12.as_slice());
        assert_eq!(
            (decoded.kxq0, decoded.kxq1, decoded.est),
            (
                4092.0 + 0.5 + 2f64.powi(-7) + 2f64.powi(-3),
                2f64.powi(-42),
                3.0
            )
        );
        let retired = hex(include_str!("../../../asapv1_golden/hll_hip_p14.hex"));
        let decoded = HyperLogLogHipImpl::<HllRegistersP14>::from_asapv1(&retired).unwrap();
        assert_eq!(decoded.registers.as_slice(), p14.as_slice());
        assert_eq!(
            (decoded.kxq0, decoded.kxq1, decoded.est),
            (
                16380.0 + 0.5 + 2f64.powi(-7),
                2f64.powi(-42) + 2f64.powi(-51),
                4.125
            )
        );
    }

    #[test]
    fn hll_envelope_structure_and_kind_id_guard() {
        // ASAPv1 envelope header for an Ertl-MLE sketch: magic, version 0x01,
        // kind_id_len 2, kind_id [0x01, 0x02].
        let mut sketch = HyperLogLog::<Ertl>::default();
        for value in 0..1000 {
            sketch.update(&DataInput::U64(value));
        }
        let bytes = sketch.to_asapv1().expect("serialize");

        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(bytes[6], envelope::VERSION);
        assert_eq!(bytes[7], 2, "kind_id_len");
        assert_eq!(&bytes[8..10], kind::HLL_ERTL);

        let decoded = HyperLogLog::<Ertl>::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.registers_as_slice(), sketch.registers_as_slice());

        // A Classic decoder must reject Ertl-MLE bytes (kind_id mismatch).
        assert!(HyperLogLog::<Classic>::from_asapv1(&bytes).is_err());
    }

    #[test]
    fn hll_hip_round_trip_preserves_state() {
        let mut sketch = HyperLogLogHip::default();
        for value in 0..1000 {
            sketch.update(&DataInput::U64(value));
        }
        let bytes = sketch.to_asapv1().expect("serialize");
        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(&bytes[8..10], kind::HLL_HIP);

        let decoded = HyperLogLogHip::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.registers.as_slice(), sketch.registers.as_slice());
        assert_eq!(decoded.kxq0.to_bits(), sketch.kxq0.to_bits());
        assert_eq!(decoded.kxq1.to_bits(), sketch.kxq1.to_bits());
        assert_eq!(decoded.est.to_bits(), sketch.est.to_bits());
    }

    // A test-only custom hasher: it hashes exactly like `DefaultXxHasher`
    // (delegation) but declares a DIFFERENT `HashProfile` (distinct profile id
    // and seed list). Serialization is derived from the profile, so an
    // `AltHasher` sketch serializes truthfully and its metadata differs from the
    // standard profile on the wire.
    //
    // Note: that an *unprofiled* hasher cannot be serialized is a compile-time
    // guarantee — `to_asapv1`/`from_asapv1` are bounded on
    // `H: HashProfile`, so a hasher that impls only `SketchHasher` fails to
    // compile. There is nothing to assert at runtime.
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
        const SEED_DERIVATION: &'static str = "seed_list_index_scaled";
        const INPUT_ENCODING: &'static str = "projectasap.input.v1";
        fn seed_list() -> Vec<u64> {
            // A deliberately different seed list so the wire bytes differ.
            vec![1, 2, 3, 4, 5]
        }
        const CANONICAL_SEED_INDEX: u32 = crate::DEFAULT_CANONICAL_SEED_INDEX as u32;
        const MATRIX_SEED_INDEX: u32 = 0;
    }

    #[test]
    fn hll_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) An HLL built with a custom-profile hasher round-trips.
        let mut alt = HyperLogLogImpl::<Ertl, HllRegistersP14, AltHasher>::default();
        let mut std = HyperLogLog::<Ertl>::default();
        for v in 0..1000 {
            alt.update(&DataInput::U64(v));
            std.update(&DataInput::U64(v));
        }
        let alt_bytes = alt.to_asapv1().expect("alt serialize");
        let decoded = HyperLogLogImpl::<Ertl, HllRegistersP14, AltHasher>::from_asapv1(&alt_bytes)
            .expect("alt decode");
        assert_eq!(decoded.registers_as_slice(), alt.registers_as_slice());

        // (b) Its bytes differ from the DefaultXxHasher sketch's bytes: the
        // metadata is derived from the (different) profile, so it is not a lie.
        let std_bytes = std.to_asapv1().expect("std serialize");
        assert_ne!(
            alt_bytes, std_bytes,
            "a custom profile must serialize different metadata than the standard profile"
        );

        // (c) Decoding AltHasher bytes into a DefaultXxHasher-typed HLL fails
        // closed (profile mismatch), never silently accepted.
        assert!(
            HyperLogLog::<Ertl>::from_asapv1(&alt_bytes).is_err(),
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
            p12.update(&DataInput::U64(v));
        }
        let bytes = p12.to_asapv1().expect("serialize");
        assert!(
            HyperLogLog::<Classic>::from_asapv1(&bytes).is_err(),
            "P12 bytes must be rejected by a P14 decoder"
        );
    }

    /// Builds a well-framed envelope around a crafted register bin.
    fn crafted_plain(kind_id: &[u8], precision: u32, registers: Vec<u8>) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(&standard_hll_metadata(precision)).expect("meta");
        let payload = rmp_serde::to_vec(&HllPayloadPlain { registers }).expect("payload");
        envelope::encode(kind_id, &metadata, &payload)
    }

    /// A register holds the largest rank hashed into it, and a rank is
    /// `leading_zeros(...) + 1` over the `64 - precision` bits left after the
    /// index bits are shifted out — at most `64 - precision + 1`. A crafted bin
    /// carrying a larger byte is refused, for every variant and precision: it
    /// would inflate the Classic estimate, and index past the Ertl histogram or
    /// the HIP shift.
    #[test]
    fn hll_rejects_out_of_range_register_values() {
        const P14_MAX: u8 = (HllRegistersP14::RANK_HASH_BITS + 1) as u8; // 51
        const P12_MAX: u8 = (HllRegistersP12::RANK_HASH_BITS + 1) as u8; // 53

        let bin = |len: usize, rank: u8| {
            let mut registers = vec![0u8; len];
            registers[7] = rank;
            registers
        };
        let p14 = HllRegistersP14::NUM_REGISTERS;
        let p12 = HllRegistersP12::NUM_REGISTERS;

        // The bound itself decodes, one past it does not — for both estimators
        // sharing the plain payload, and at a second precision.
        let ok = crafted_plain(kind::HLL_CLASSIC, 14, bin(p14, P14_MAX));
        assert!(HyperLogLog::<Classic>::from_asapv1(&ok).is_ok());
        let ok = crafted_plain(kind::HLL_ERTL, 12, bin(p12, P12_MAX));
        assert!(HyperLogLogP12::<Ertl>::from_asapv1(&ok).is_ok());

        let over = crafted_plain(kind::HLL_CLASSIC, 14, bin(p14, P14_MAX + 1));
        let problem = HyperLogLog::<Classic>::from_asapv1(&over)
            .expect_err("a register past the maximum rank must be rejected")
            .to_string();
        assert!(
            problem.contains("exceeds the maximum rank 51"),
            "got {problem}"
        );

        let over = crafted_plain(kind::HLL_ERTL, 14, bin(p14, u8::MAX));
        assert!(
            HyperLogLog::<Ertl>::from_asapv1(&over).is_err(),
            "a register past the Ertl histogram must be rejected"
        );

        let over = crafted_plain(kind::HLL_ERTL, 12, bin(p12, P12_MAX + 1));
        let problem = HyperLogLogP12::<Ertl>::from_asapv1(&over)
            .expect_err("a register past the maximum rank must be rejected")
            .to_string();
        assert!(
            problem.contains("exceeds the maximum rank 53"),
            "got {problem}"
        );

        // The HIP payload carries the same bin and is held to the same bound.
        let metadata = rmp_serde::to_vec_named(&standard_hll_metadata(14)).expect("meta");
        let current = rmp_serde::to_vec(&HllPayloadHip {
            registers: bin(p14, P14_MAX + 1),
            hip_est: 0.0,
        })
        .expect("payload");
        let retired = rmp_serde::to_vec(&HllPayloadHipWithKxq {
            registers: bin(p14, P14_MAX + 1),
            hip_kxq0: p14 as f64,
            hip_kxq1: 0.0,
            hip_est: 0.0,
        })
        .expect("payload");
        for (kind_id, payload) in [(kind::HLL_HIP, current), (kind::HLL_HIP_WITH_KXQ, retired)] {
            let over = envelope::encode(kind_id, &metadata, &payload);
            let problem = HyperLogLogHip::from_asapv1(&over)
                .expect_err("a register past the maximum rank must be rejected")
                .to_string();
            assert!(
                problem.contains("exceeds the maximum rank 51"),
                "got {problem}"
            );
        }
    }

    /// The encode path holds the same bound: `apply_delta` takes an arbitrary
    /// value, so a sketch can be driven past the maximum rank in memory, and
    /// such a sketch has no encoding rather than bytes its own decoder refuses.
    #[test]
    fn hll_refuses_to_serialize_an_out_of_range_register() {
        use crate::octo_delta::HllRegisterDelta;
        const P14_MAX: u8 = (HllRegistersP14::RANK_HASH_BITS + 1) as u8; // 51

        let mut classic = HyperLogLog::<Classic>::default();
        classic.apply_delta(HllRegisterDelta {
            index: 7,
            register_value: P14_MAX,
        });
        assert!(
            classic.to_asapv1().is_ok(),
            "the bound itself must still serialize"
        );
        classic.apply_delta(HllRegisterDelta {
            index: 7,
            register_value: P14_MAX + 1,
        });
        let problem = classic
            .to_asapv1()
            .expect_err("a register past the maximum rank must not serialize")
            .to_string();
        assert!(
            problem.contains("exceeds the maximum rank 51"),
            "got {problem}"
        );

        let mut ertl = HyperLogLog::<Ertl>::default();
        ertl.apply_delta(HllRegisterDelta {
            index: 7,
            register_value: u8::MAX,
        });
        assert!(
            ertl.to_asapv1().is_err(),
            "a register past the Ertl histogram must not serialize"
        );

        let mut p12 = HyperLogLogP12::<Ertl>::default();
        p12.apply_delta(HllRegisterDelta {
            index: 7,
            register_value: (HllRegistersP12::RANK_HASH_BITS + 2) as u8,
        });
        let problem = p12
            .to_asapv1()
            .expect_err("a register past the maximum rank must not serialize")
            .to_string();
        assert!(
            problem.contains("exceeds the maximum rank 53"),
            "got {problem}"
        );

        // HIP carries the same bin and is held to the same bound.
        let mut hip = HyperLogLogHip::default();
        hip.registers.as_mut_slice()[7] = P14_MAX + 1;
        let problem = hip
            .to_asapv1()
            .expect_err("a register past the maximum rank must not serialize")
            .to_string();
        assert!(
            problem.contains("exceeds the maximum rank 51"),
            "got {problem}"
        );
    }

    /// A Classic decoder rejects HIP bytes (kind_id mismatch).
    #[test]
    fn hll_hip_kind_id_rejected_by_classic() {
        let mut hip = HyperLogLogHip::default();
        for v in 0..100 {
            hip.update(&DataInput::U64(v));
        }
        let bytes = hip.to_asapv1().expect("serialize");
        assert!(
            HyperLogLog::<Classic>::from_asapv1(&bytes).is_err(),
            "HIP bytes must be rejected by a Classic decoder"
        );
    }

    /// `(kxq0, kxq1)` summed from scratch over `registers`: `2^-r` for each
    /// register `r`, split at `r < 32` exactly as `update_with_hash` splits it.
    fn summed_kxq(registers: &[u8]) -> (f64, f64) {
        let low: f64 = registers
            .iter()
            .filter(|&&r| r < 32)
            .map(|&r| 1.0 / ((1_u64 << r) as f64))
            .sum();
        let high: f64 = registers
            .iter()
            .filter(|&&r| r >= 32)
            .map(|&r| 1.0 / ((1_u64 << r) as f64))
            .sum();
        (low, high)
    }

    /// The HIP payload: the register bin, then the running estimate, and
    /// nothing else. `kxq0` / `kxq1` are derived from the registers and are
    /// not carried.
    #[test]
    fn hip_encodes_registers_and_estimate_only() {
        let mut sketch = HyperLogLogHipP12::new();
        for value in 0..500 {
            sketch.update(&DataInput::U64(value));
        }
        let bytes = sketch.to_asapv1().expect("serialize");
        let (kind_id, _, payload) = envelope::split(&bytes).expect("split");
        assert_eq!(kind_id, [0x01, 0x04]);
        let (registers, est): (serde_bytes::ByteBuf, f64) =
            rmp_serde::from_slice(payload).expect("two-field payload");
        assert_eq!(registers.as_slice(), sketch.registers.as_slice());
        assert_eq!(est.to_bits(), sketch.est.to_bits());
    }

    /// The retired `01 03` payload still decodes: its `kxq0` / `kxq1` are read
    /// and dropped, recomputed from the registers, and the result re-encodes
    /// under the current kind. The golden carries `kxq0 = 1.5, kxq1 = 2.5`,
    /// which no register state produces, so a kept value would show.
    #[test]
    fn retired_hip_kind_decodes_with_kxq_recomputed() {
        let retired = envelope::hex(include_str!("../../../asapv1_golden/hll_hip_p12.hex"));
        assert_eq!(&retired[8..10], [0x01, 0x03]);
        let decoded = HyperLogLogHipP12::from_asapv1(&retired).expect("decode retired");
        let (kxq0, kxq1) = summed_kxq(decoded.registers.as_slice());
        assert_eq!(decoded.kxq0.to_bits(), kxq0.to_bits());
        assert_eq!(decoded.kxq1.to_bits(), kxq1.to_bits());
        assert_eq!(decoded.est, 3.0);
        let reencoded = decoded.to_asapv1().expect("re-encode");
        assert_eq!(&reencoded[8..10], [0x01, 0x04]);
    }

    /// Each kind id fixes its payload length: the two-field `01 04` payload
    /// under the retired id, or the four-field `01 03` payload under the
    /// current one, is rejected.
    #[test]
    fn hip_payload_must_match_its_kind_field_count() {
        let current = envelope::hex(include_str!("../../../asapv1_golden/hll_hip_est_p12.hex"));
        let retired = envelope::hex(include_str!("../../../asapv1_golden/hll_hip_p12.hex"));
        let (_, current_metadata, current_payload) = envelope::split(&current).expect("split");
        let (_, retired_metadata, retired_payload) = envelope::split(&retired).expect("split");
        let two_fields_as_retired =
            envelope::encode(kind::HLL_HIP_WITH_KXQ, current_metadata, current_payload);
        let four_fields_as_current =
            envelope::encode(kind::HLL_HIP, retired_metadata, retired_payload);
        assert!(HyperLogLogHipP12::from_asapv1(&two_fields_as_retired).is_err());
        assert!(HyperLogLogHipP12::from_asapv1(&four_fields_as_current).is_err());
    }

    /// The hash that raises register `bucket` to rank `rank` (`1..=R + 1`,
    /// `R` the rank hash bits).
    fn hash_placing<Registers: HllRegisterStorage>(bucket: usize, rank: u8) -> u64 {
        let rank_bits = Registers::RANK_HASH_BITS as u32;
        let rank_part = if u32::from(rank) > rank_bits {
            0
        } else {
            1u64 << (rank_bits - u32::from(rank))
        };
        ((bucket as u64) << rank_bits) | rank_part
    }

    /// The hashes a test stream feeds: a SplitMix64 run, so most ranks are
    /// small, with every 7th hash placed at a rank of 32 or more so the
    /// `kxq1` side fills too.
    fn hip_stream<Registers: HllRegisterStorage>(len: usize, seed: u64) -> Vec<u64> {
        let max_rank = (Registers::RANK_HASH_BITS + 1) as u64;
        let mut state = seed;
        (0..len)
            .map(|i| {
                state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^= z >> 31;
                if i % 7 == 0 {
                    let bucket = (z as usize) % Registers::NUM_REGISTERS;
                    let rank = 32 + (z >> 40) % (max_rank - 31);
                    hash_placing::<Registers>(bucket, rank as u8)
                } else {
                    z
                }
            })
            .collect()
    }

    /// Decoding recomputes `kxq0` / `kxq1` bit for bit, so a decoded sketch
    /// that keeps updating holds the same `est` as one that never went
    /// through the wire. Every term is a power of two between `2^-65` and
    /// `2^0`, and each side's sum stays within 53 bits of its smallest term,
    /// so the incremental and the summed values are the same `f64`.
    fn assert_decode_then_continue_is_exact<Registers: HllRegisterStorage>() {
        for (seed, before, after) in [(1, 0, 3_000), (2, 1_000, 3_000), (3, 40_000, 40_000)] {
            let stream = hip_stream::<Registers>(before + after, seed);
            let mut kept = HyperLogLogHipImpl::<Registers>::new();
            kept.update_batch_with_hashes(&stream[..before]);
            let bytes = kept.to_asapv1().expect("serialize");
            let mut restored =
                HyperLogLogHipImpl::<Registers>::from_asapv1(&bytes).expect("decode");
            assert_eq!(restored.kxq0.to_bits(), kept.kxq0.to_bits());
            assert_eq!(restored.kxq1.to_bits(), kept.kxq1.to_bits());
            kept.update_batch_with_hashes(&stream[before..]);
            restored.update_batch_with_hashes(&stream[before..]);
            assert!(kept.kxq1 > 0.0, "the stream reaches ranks of 32 or more");
            assert_eq!(restored.registers.as_slice(), kept.registers.as_slice());
            assert_eq!(restored.kxq0.to_bits(), kept.kxq0.to_bits());
            assert_eq!(restored.kxq1.to_bits(), kept.kxq1.to_bits());
            assert_eq!(
                restored.est.to_bits(),
                kept.est.to_bits(),
                "p = {}, {before} then {after} updates",
                Registers::PRECISION
            );
            let (kxq0, kxq1) = summed_kxq(kept.registers.as_slice());
            assert_eq!(
                (kept.kxq0.to_bits(), kept.kxq1.to_bits()),
                (kxq0.to_bits(), kxq1.to_bits())
            );
        }
    }

    #[test]
    fn hip_decode_then_continue_matches_an_unserialized_sketch() {
        use crate::structures::fixed_structure::HllRegistersP16;
        assert_decode_then_continue_is_exact::<HllRegistersP12>();
        assert_decode_then_continue_is_exact::<HllRegistersP14>();
        assert_decode_then_continue_is_exact::<HllRegistersP16>();
    }
}
