//! Shared ASAPv1 envelope framing (sketch-agnostic).
//!
//! Every serialized ASAP sketch binary is wrapped in this envelope:
//!
//! ```text
//! [ magic:6 | version:u8 | kind_id_len:u8 | kind_id:bytes
//!           | metadata_len:u32_be | payload_len:u32_be
//!           | metadata:msgpack | payload:msgpack ]
//! ```
//!
//! This module owns only the parts that are identical for every sketch: the
//! magic sentinel, the layout version, and the byte framing ([`encode`] /
//! [`split`]). The hash-profile values each sketch's metadata is built from live
//! with the hasher itself (see [`crate::HashProfile`]). The `kind_id`, the
//! metadata *contents*, and the payload are per-sketch and defined alongside
//! each sketch — see `docs/asapv1_wire_format.md`.
//!
//! [`split`] validates only the magic, version, and framing; it does **not**
//! check `kind_id` against a registry. Each sketch decoder checks that the
//! `kind_id` is one it owns, and reads each block with [`decode_block`].

use rmp_serde::decode::Error as RmpDecodeError;
use serde::de::DeserializeOwned;

/// 6-byte ASCII sentinel opening every ASAP sketch binary (`b"ASAPv1"`).
pub(crate) const MAGIC: &[u8; 6] = b"ASAPv1";

/// Envelope layout version. Bumped only when the framing itself changes.
pub(crate) const VERSION: u8 = 0x01;

/// Assemble the envelope around an already-encoded metadata block and payload.
pub(crate) fn encode(kind_id: &[u8], metadata: &[u8], payload: &[u8]) -> Vec<u8> {
    let kind_id_len = u8::try_from(kind_id.len()).expect("ASAPv1 kind_id too long (>255 bytes)");
    let metadata_len = u32::try_from(metadata.len()).expect("ASAPv1 metadata too large");
    let payload_len = u32::try_from(payload.len()).expect("ASAPv1 payload too large");
    let mut out = Vec::with_capacity(
        MAGIC.len() + 1 + 1 + kind_id.len() + 4 + 4 + metadata.len() + payload.len(),
    );
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(kind_id_len);
    out.extend_from_slice(kind_id);
    out.extend_from_slice(&metadata_len.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(metadata);
    out.extend_from_slice(payload);
    out
}

/// The three envelope slices returned by [`split`]: `(kind_id, metadata, payload)`.
pub(crate) type Parts<'a> = (&'a [u8], &'a [u8], &'a [u8]);

/// Split an envelope into `(kind_id, metadata, payload)`, validating the magic,
/// version, and that the two length-framed blocks fit. Returns an error string
/// on any structural mismatch. Does **not** validate `kind_id` — the caller does.
pub(crate) fn split(bytes: &[u8]) -> Result<Parts<'_>, String> {
    let magic_len = MAGIC.len();
    let kind_id_len_offset = magic_len + 1;
    let kind_id_offset = magic_len + 2;
    let header_min = kind_id_offset + 4 + 4;

    if bytes.len() < header_min {
        return Err(format!(
            "ASAPv1 envelope: too short ({} bytes, need at least {header_min})",
            bytes.len()
        ));
    }
    if &bytes[..magic_len] != MAGIC {
        return Err("ASAPv1 envelope: bad magic".to_string());
    }
    if bytes[magic_len] != VERSION {
        return Err(format!(
            "ASAPv1 envelope: unsupported version 0x{:02x}",
            bytes[magic_len]
        ));
    }
    let kind_id_len = bytes[kind_id_len_offset] as usize;
    let lengths_offset = kind_id_offset + kind_id_len;
    if bytes.len() < lengths_offset + 8 {
        return Err("ASAPv1 envelope: truncated kind_id / length fields".to_string());
    }
    let kind_id = &bytes[kind_id_offset..lengths_offset];

    let metadata_len = u32::from_be_bytes(
        bytes[lengths_offset..lengths_offset + 4]
            .try_into()
            .expect("four-byte length slice"),
    ) as usize;
    let payload_len = u32::from_be_bytes(
        bytes[lengths_offset + 4..lengths_offset + 8]
            .try_into()
            .expect("four-byte length slice"),
    ) as usize;
    let metadata_start = lengths_offset + 8;
    // Use checked arithmetic so crafted lengths near usize::MAX (reachable on
    // 32-bit targets) fail closed with an error instead of overflowing the
    // bounds check and panicking on the slice index.
    let payload_start = metadata_start
        .checked_add(metadata_len)
        .ok_or_else(|| "ASAPv1 envelope: length overflow".to_string())?;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| "ASAPv1 envelope: length overflow".to_string())?;
    if bytes.len() < payload_end {
        return Err("ASAPv1 envelope: truncated metadata / payload".to_string());
    }
    Ok((
        kind_id,
        &bytes[metadata_start..payload_start],
        &bytes[payload_start..payload_end],
    ))
}

/// Returns the `kind_id` of an ASAPv1 envelope after checking its magic, version,
/// and framing; the metadata and payload blocks are not parsed.
pub fn peek_kind_id(bytes: &[u8]) -> Result<&[u8], RmpDecodeError> {
    let (kind_id, _, _) = split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    Ok(kind_id)
}

/// Decodes a metadata or payload block as one msgpack value that fills the
/// block exactly; a byte left after the value is an error.
pub(crate) fn decode_block<T: DeserializeOwned>(block: &[u8]) -> Result<T, RmpDecodeError> {
    let mut deserializer = rmp_serde::Deserializer::new(block);
    let value = T::deserialize(&mut deserializer)?;
    let left = deserializer.into_inner().len();
    if left != 0 {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 envelope: {left} trailing byte(s) in a block after its msgpack value"
        )));
    }
    Ok(value)
}

/// Decodes a hex string, as the `asapv1_golden/*.hex` fixtures are written.
#[cfg(test)]
pub(crate) fn hex(text: &str) -> Vec<u8> {
    let text = text.trim();
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_frames_metadata_and_payload() {
        let kind_id = [0x01u8, 0x03];
        let metadata = b"meta-block";
        let payload = b"payload-bytes";
        let bytes = encode(&kind_id, metadata, payload);

        assert!(bytes.starts_with(MAGIC));
        assert_eq!(bytes[6], VERSION);
        assert_eq!(bytes[7], 2); // kind_id_len

        let (k, m, p) = split(&bytes).expect("split");
        assert_eq!(k, kind_id);
        assert_eq!(m, metadata);
        assert_eq!(p, payload);

        let trailing = [&bytes[..], b"after"].concat();
        assert_eq!(split(&trailing).expect("trailing bytes ignored"), (k, m, p));
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        let mut bytes = encode(&[0x01, 0x01], b"m", b"p");
        assert!(split(&bytes).is_ok());
        bytes[0] = b'X';
        assert!(split(&bytes).is_err());

        let mut bytes = encode(&[0x01, 0x01], b"m", b"p");
        bytes[6] = 0xFF; // version
        assert!(split(&bytes).is_err());
    }

    #[test]
    fn rejects_truncation() {
        let bytes = encode(&[0x01, 0x01], b"metadata", b"payload");
        assert!(split(&bytes[..bytes.len() - 1]).is_err());
        assert!(split(&bytes[..3]).is_err());
    }

    /// One encoding per decode path: a label, the bytes, and whether they decode.
    type Case = (&'static str, Vec<u8>, fn(&[u8]) -> bool);

    fn decode_paths() -> Vec<Case> {
        use crate::input::HydraCounter;
        use crate::*;

        macro_rules! case {
            ($label:expr, $bytes:expr, $ty:ty) => {
                ($label, $bytes, |b: &[u8]| {
                    <$ty>::deserialize_from_bytes(b).is_ok()
                })
            };
        }
        macro_rules! sketch {
            ($label:expr, $sketch:expr, $ty:ty) => {
                case!($label, $sketch.serialize_to_bytes().unwrap(), $ty)
            };
        }
        type Cms = CountMin<Vector2D<i64>, RegularPath>;
        type Cs = Count<Vector2D<i64>, RegularPath>;
        type CmsHeap = CMSHeap<Vector2D<i64>, RegularPath>;
        type CsHeap = CSHeap<Vector2D<i64>, RegularPath>;
        type HllClassic = HyperLogLogP12<Classic>;

        let classic = hex(include_str!("../../asapv1_golden/hll_classic_p12.hex"));
        let hip = hex(include_str!("../../asapv1_golden/hll_hip_p12.hex"));
        let classic_delta = hex(include_str!(
            "../../asapv1_golden/hll_classic_p14_delta.hex"
        ));
        let cms = hex(include_str!("../../asapv1_golden/cms_i64_regular_2x3.hex"));
        let cms_delta = hex(include_str!(
            "../../asapv1_golden/cms_i64_regular_2x3_delta.hex"
        ));
        let cs = hex(include_str!("../../asapv1_golden/cs_i64_regular_2x4.hex"));
        let cs_delta = hex(include_str!(
            "../../asapv1_golden/cs_i64_regular_2x4_delta.hex"
        ));
        let kll = hex(include_str!("../../asapv1_golden/kll_f64_k200.hex"));
        let cms_heap_delta = hex(include_str!(
            "../../asapv1_golden/cmsheap_i64_regular_2x3_delta.hex"
        ));
        let set = hex(include_str!(
            "../../asapv1_golden/set_aggregator_strings.hex"
        ));
        let delta = hex(include_str!("../../asapv1_golden/delta_result_strings.hex"));

        let str_keys = ["alpha", "beta", "gamma", "alpha", "alpha"].map(DataInput::Str);
        let bytes_keys = [b"ab", b"cd", b"ab"].map(|key| DataInput::Bytes(key));
        let mut cms_heap = CmsHeap::new(3, 8, 4);
        let mut cms_heap_bytes = CmsHeap::new(3, 8, 4);
        let mut cs_heap = CsHeap::new(3, 8, 4);
        let mut l2hh: CountL2HH = CountL2HH::with_dimensions(2, 8);
        let mut bloom = Bloom::<RegularPath>::with_dimensions(3, 64);
        let mut ss: SpaceSaving = SpaceSaving::with_capacity(4);
        let mut ss_u64: SpaceSaving = SpaceSaving::with_capacity(4);
        let mut ss_bytes: SpaceSaving = SpaceSaving::with_capacity(4);
        let mut univmon = UnivMon::init_univmon(4, 2, 16, 3);
        let mut univmon_bytes = UnivMon::init_univmon(4, 2, 16, 3);
        let mut pyramid = UnivMonPyramid::new(4, 2, 3, 16, 2, 8, 4);
        for key in &str_keys {
            cms_heap.insert(key);
            cs_heap.insert(key);
            l2hh.fast_insert_with_count(key, 2);
            bloom.insert(key);
            ss.insert(key);
            univmon.insert(key, 2);
            pyramid.insert(key, 2);
        }
        for key in &bytes_keys {
            cms_heap_bytes.insert(key);
            ss_bytes.insert(key);
            univmon_bytes.insert(key, 2);
        }
        ss_u64.insert(&DataInput::U64(9));
        let mut coco: Coco = Coco::init_with_size(8, 2);
        coco.insert("flow::a", 3);
        let mut elastic: Elastic = Elastic::init_with_dimensions(4, 2, 16);
        elastic.insert("flow::a".to_string());
        let mut kll_dynamic = KLLDynamic::<f64>::init_kll(200);
        let mut positive = DDSketch::new(0.01);
        let mut signed = DDSketch::new(0.01);
        let config = UnivMonQConfig {
            levels: 4,
            width: 64,
            width_halving_period: 0,
            depth: 3,
            counter_bits: 64,
            candidates: 16,
            ordered_samples: 16,
            hash_seed: 5,
        };
        let mut univmon_q = UnivMonQ::<DefaultXxHasher>::new_with_source_id(config, 7).unwrap();
        for value in [1.0, 2.5, 7.0] {
            kll_dynamic.update(&value);
            positive.add(&value);
            signed.add(&-value);
            univmon_q.update(&value);
        }
        let cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8);
        let mut eh = ExponentialHistogram::new(2, 1000, EHSketchList::CM(cm.clone()));
        eh.update(10, &DataInput::U64(1));
        let eh_list = EHSketchList::DDS(positive.clone());
        let hydra = |counter: HydraCounter, value: DataInput<'static>| {
            let mut hydra = Hydra::with_schema(2, 8, ["region"], counter).expect("schema");
            hydra.update(&["eu"], &value, None).expect("arity");
            hydra
        };
        let hydra_cm = hydra(HydraCounter::CM(cm), DataInput::U64(1));
        let hll = HyperLogLog::<ErtlMLE>::default();
        let hydra_hll = hydra(HydraCounter::HLL(hll), DataInput::U64(1));
        let hydra_kll = hydra(HydraCounter::KLL(KLL::default()), DataInput::F64(2.5));
        let um = UnivMon::init_univmon(4, 2, 16, 3);
        let hydra_um = hydra(HydraCounter::UNIVERSAL(um), DataInput::Str("alpha"));
        let portable = |b: &[u8]| HllSketch::from_msgpack(b).is_ok();

        let cases: Vec<Case> = vec![
            case!("HLL classic", classic.clone(), HllClassic),
            case!("HLL HIP", hip.clone(), HyperLogLogHIPP12),
            case!("HLL classic delta", classic_delta, HyperLogLogDelta<Classic, HllBucketListP14>),
            ("HLL portable classic", classic, portable),
            ("HLL portable HIP", hip, portable),
            case!("CMS", cms, Cms),
            case!("CMS delta", cms_delta, CountMinDelta<i64, RegularPath>),
            case!("CS", cs, Cs),
            case!("CS delta", cs_delta, sketches::CountSketchDelta<i64, RegularPath>),
            case!("KLL", kll, KLL<f64>),
            case!("SetAggregator", set, SetAggregator),
            case!("DeltaResult", delta, DeltaResult),
            sketch!("KLL dynamic", kll_dynamic, KLLDynamic<f64>),
            sketch!("CMSHeap", cms_heap, CmsHeap),
            sketch!("CMSHeap bytes", cms_heap_bytes, CmsHeap),
            case!("CMSHeap delta", cms_heap_delta, CMSHeapDelta<i64, RegularPath>),
            sketch!("CSHeap", cs_heap, CsHeap),
            sketch!("CountL2HH", l2hh, CountL2HH),
            sketch!("DDSketch", positive, DDSketch),
            sketch!("DDSketch signed", signed, DDSketch),
            sketch!("Bloom", bloom, Bloom<RegularPath>),
            sketch!("SpaceSaving string", ss, SpaceSaving),
            sketch!("SpaceSaving u64", ss_u64, SpaceSaving),
            sketch!("SpaceSaving bytes", ss_bytes, SpaceSaving),
            sketch!("Coco", coco, Coco),
            sketch!("Elastic", elastic, Elastic),
            sketch!("UnivMon", univmon, UnivMon),
            sketch!("UnivMon bytes", univmon_bytes, UnivMon),
            sketch!("UnivMonPyramid", pyramid, UnivMonPyramid),
            sketch!("UnivMonQ", univmon_q, UnivMonQ<DefaultXxHasher>),
            sketch!("ExponentialHistogram", eh, ExponentialHistogram),
            sketch!("EHSketchList", eh_list, EHSketchList),
            sketch!("Hydra CM", hydra_cm, Hydra),
            sketch!("Hydra HLL", hydra_hll, Hydra),
            sketch!("Hydra KLL", hydra_kll, Hydra),
            sketch!("Hydra UnivMon", hydra_um, Hydra),
        ];
        #[cfg(feature = "experimental")]
        let cases = {
            let mut cases = cases;
            let mut kmv: KMV = KMV::new(4);
            kmv.insert(&DataInput::U64(9));
            let mut uniform = UniformSampling::with_seed(1.0, 7);
            uniform.update(2.5);
            cases.push(sketch!("KMV", kmv, KMV));
            cases.push(sketch!("Uniform", uniform, UniformSampling));
            cases
        };
        cases
    }

    /// Re-frames `bytes` with one msgpack `nil` appended inside the metadata
    /// block or inside the payload block, the length prefix grown to cover it.
    fn grow_block(bytes: &[u8], metadata: bool) -> Vec<u8> {
        let (kind_id, metadata_block, payload_block) = split(bytes).expect("split");
        let nil: &[u8] = &[0xc0];
        if metadata {
            encode(kind_id, &[metadata_block, nil].concat(), payload_block)
        } else {
            encode(kind_id, metadata_block, &[payload_block, nil].concat())
        }
    }

    #[test]
    fn every_decoder_fills_both_blocks_exactly() {
        for (label, bytes, decodes) in decode_paths() {
            assert!(decodes(&bytes), "{label}: the valid encoding must decode");
            assert!(
                !decodes(&grow_block(&bytes, true)),
                "{label}: metadata block not filled"
            );
            assert!(
                !decodes(&grow_block(&bytes, false)),
                "{label}: payload block not filled"
            );
        }
    }
}
