//! ASAPv1 wire serialization for DDSketch.
//!
//! Child submodule of [`crate::sketches::ddsketch`]: it holds ALL of DDSketch's
//! ASAPv1 serialization (the metadata/payload DTOs, the kind_id constant, and
//! the `to_asapv1` / `from_asapv1` impls) while the
//! algorithm lives in the parent module file. Being a descendant module, it
//! reads the sketch's private stores and `count` directly without widening any
//! field visibility. See `docs/asapv1_wire_format.md` §3.8.
//!
//! ## Kind ids
//!
//! The sketch encodes as `0x05 0x01`: the two stores and the zero count. The
//! retired `0x05 0x00` also carried `sum` / `min` / `max`; it still decodes,
//! with those three scalars read and dropped, so bytes written before the
//! change stay readable. Nothing encodes it.
//!
//! ## DDSketch metadata has no hash spec
//!
//! DDSketch never hashes its inputs — it maps a value to a bucket index with
//! `floor(ln(v) / ln(gamma))`, and carries no hasher type parameter at all. The
//! hash-spec group has no truthful value here, so the metadata is structural
//! params only (`metadata_version`, `alpha`). This is the KLL precedent (Q-KLL).
//!
//! ## Relative accuracy
//!
//! `alpha` is the one construction parameter, so it lives in the metadata. The
//! `gamma` / `log_gamma` / `inv_log_gamma` triple is derived from it
//! (`gamma = (1 + alpha) / (1 - alpha)`) and never reaches the wire.
//!
//! ## Signed stores
//!
//! Every payload carries the positive store, the negative-magnitude store and
//! the zero count, whether or not the sketch holds a value `<= 0`.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::asapv1::admission_rate;
use crate::asapv1::envelope;
use crate::asapv1::kind;
use crate::asapv1::kind::{DDSKETCH, DDSKETCH_WITH_SCALARS};
use crate::asapv1::wire_key::exact;
use crate::common::structures::Vector1D;

use super::{Buckets, DdSketch, DdSketchState};

/// DDSketch descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it). There is no hash-spec group;
/// `alpha` is the sketch's one construction parameter and so, per the spec's
/// config→metadata rule, its only structural param. The optional
/// `admission_rate` closes the map.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DdMetadata {
    metadata_version: u8,
    #[serde(deserialize_with = "exact")]
    alpha: f64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "admission_rate::deserialize"
    )]
    admission_rate: Option<f64>,
}

/// Builds the DDSketch descriptor metadata for a relative accuracy `alpha`.
fn dd_metadata(alpha: f64, admission_rate: Option<f64>) -> DdMetadata {
    DdMetadata {
        metadata_version: 1,
        alpha,
        admission_rate,
    }
}

/// Encodes the metadata block for `alpha` and `admission_rate`.
pub(super) fn encode_metadata(alpha: f64, admission_rate: f64) -> Result<Vec<u8>, RmpEncodeError> {
    let admission_rate = admission_rate::to_key(admission_rate)
        .map_err(|e| RmpEncodeError::Syntax(format!("ASAPv1 DDSketch envelope: {e}")))?;
    rmp_serde::to_vec_named(&dd_metadata(alpha, admission_rate))
}

/// Decodes the metadata block to its `(alpha, admission_rate)`: exactly
/// `metadata_version` 1, an `alpha` inside `(0, 1)` and an optional
/// `admission_rate`.
pub(super) fn decode_metadata(metadata: &[u8]) -> Result<(f64, f64), RmpDecodeError> {
    let meta: DdMetadata = envelope::decode_block(metadata)?;
    let admission_rate = admission_rate::from_key(meta.admission_rate)
        .map_err(|e| RmpDecodeError::Uncategorized(format!("ASAPv1 DDSketch envelope: {e}")))?;
    // `alpha` is a property of the stored sketch rather than of the target
    // type, so it is echoed back into the expected block instead of pinned.
    if meta != dd_metadata(meta.alpha, meta.admission_rate) {
        return Err(RmpDecodeError::Uncategorized(
            "ASAPv1 DDSketch envelope: metadata mismatch".to_string(),
        ));
    }
    check_alpha(meta.alpha).map_err(RmpDecodeError::Uncategorized)?;
    Ok((meta.alpha, admission_rate))
}

/// DDSketch payload (ASAPv1 §3.8, kind `0x05 0x01`), a msgpack **array**
/// (`to_vec`, positional). Each store is carried verbatim including the growth
/// padding, so a decoded sketch re-serializes byte-identically; the bucket
/// index of `counts[i]` is `offset + i`. The total sample count is the sum of
/// both stores and `zero_count`, and so is not carried.
#[derive(Debug, Serialize, Deserialize)]
struct DdPayload {
    counts: Vec<u64>,
    offset: i32,
    negative_counts: Vec<u64>,
    negative_offset: i32,
    zero_count: u64,
}

/// The payload of the retired kind `0x05 0x00`: [`DdPayload`]'s fields with
/// `sum` / `min` / `max` between `offset` and `negative_counts`. Decoded only;
/// the three scalars are dropped.
#[derive(Debug, Serialize, Deserialize)]
struct DdPayloadWithScalars {
    counts: Vec<u64>,
    offset: i32,
    #[serde(deserialize_with = "exact")]
    sum: f64,
    #[serde(deserialize_with = "exact")]
    min: f64,
    #[serde(deserialize_with = "exact")]
    max: f64,
    negative_counts: Vec<u64>,
    negative_offset: i32,
    zero_count: u64,
}

impl From<DdPayloadWithScalars> for DdPayload {
    fn from(p: DdPayloadWithScalars) -> Self {
        Self {
            counts: p.counts,
            offset: p.offset,
            negative_counts: p.negative_counts,
            negative_offset: p.negative_offset,
            zero_count: p.zero_count,
        }
    }
}

/// Checks a relative accuracy against the domain [`DdSketch::new`] accepts:
/// finite and strictly inside `(0, 1)`. Outside it the `gamma` derivation is
/// non-positive or infinite and every bucket index is meaningless.
pub(super) fn check_alpha(alpha: f64) -> Result<(), String> {
    if !alpha.is_finite() || alpha <= 0.0 || alpha >= 1.0 {
        return Err(format!("DDSketch alpha {alpha} is not in (0, 1)"));
    }
    Ok(())
}

/// Checks a bucket store's index span: an empty store sits at offset `0`, and
/// a populated one's highest index `offset + len - 1` is representable as an
/// `i32`. Applied before the store is rebuilt from the declared offset.
fn check_store_span(store: &Buckets) -> Result<(), String> {
    let (offset, len) = (store.offset, store.counts.len());
    if len == 0 {
        if offset != 0 {
            return Err(format!(
                "DDSketch empty store must be at offset 0, got {offset}"
            ));
        }
        return Ok(());
    }
    let highest = i64::from(offset) + len as i64 - 1;
    if highest > i64::from(i32::MAX) {
        return Err(format!(
            "DDSketch store span past i32: offset={offset}, len={len}"
        ));
    }
    Ok(())
}

/// Sum of a store's bucket counts. `None` on overflow.
fn store_total(store: &Buckets) -> Option<u64> {
    store
        .counts
        .as_slice()
        .iter()
        .try_fold(0u64, |acc, &c| acc.checked_add(c))
}

/// Checks a state against §3.8 rules 3 through 6 and returns its total sample
/// count, the checked sum of both stores and `zero_count`.
pub(super) fn check_state(state: &DdSketchState) -> Result<u64, String> {
    check_alpha(state.alpha)?;
    check_store_span(&state.positive_store)?;
    check_store_span(&state.negative_store)?;
    let overflow = || "DDSketch bucket counts overflow the total sample count".to_string();
    let negative = store_total(&state.negative_store).ok_or_else(overflow)?;
    let count = store_total(&state.positive_store)
        .and_then(|c| c.checked_add(negative))
        .and_then(|c| c.checked_add(state.zero_count))
        .ok_or_else(overflow)?;
    Ok(count)
}

// Wire serialization for DDSketch. `wire` is a descendant of the sketch module,
// so these impls read the private fields and construct the struct directly.
impl DdSketch {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x05 0x01`). `alpha` lands in the metadata; the payload is the
    /// two bucket stores and the zero count.
    ///
    /// A sketch whose state the decoder would refuse — an out-of-range `alpha`,
    /// a store spanning past `i32`, or counts that overflow `u64` — is an error
    /// rather than bytes that would be refused on decode.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let state = DdSketchState {
            alpha: self.alpha,
            positive_store: self.positive_store.clone(),
            negative_store: self.negative_store.clone(),
            zero_count: self.zero_count,
            admission_rate: self.admission_rate,
        };
        check_state(&state)
            .map_err(|e| RmpEncodeError::Syntax(format!("ASAPv1 DDSketch envelope: {e}")))?;
        let metadata = encode_metadata(self.alpha, self.admission_rate)?;
        let payload = rmp_serde::to_vec(&DdPayload {
            counts: state.positive_store.counts.into_vec(),
            offset: state.positive_store.offset,
            negative_counts: state.negative_store.counts.into_vec(),
            negative_offset: state.negative_store.offset,
            zero_count: self.zero_count,
        })?;
        Ok(envelope::encode(kind::DDSKETCH, &metadata, &payload))
    }

    /// Deserializes a DDSketch from an ASAPv1 MessagePack envelope of kind
    /// `0x05 0x01`, or of the retired `0x05 0x00`, whose `sum` / `min` / `max`
    /// are read and dropped. The index mapping is rebuilt from the metadata
    /// `alpha` and the total sample count is summed from the stores. Every
    /// inconsistency fails closed.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != DDSKETCH && kind_id != DDSKETCH_WITH_SCALARS {
            return Err(RmpDecodeError::Uncategorized(format!(
                "DDSketch kind_id mismatch: stored {kind_id:?}, expected {DDSKETCH:?} \
                 (or the retired {DDSKETCH_WITH_SCALARS:?})"
            )));
        }
        let (alpha, admission_rate) = decode_metadata(metadata)?;
        let mut p: DdPayload = if kind_id == DDSKETCH {
            envelope::decode_block(payload)?
        } else {
            envelope::decode_block::<DdPayloadWithScalars>(payload)?.into()
        };
        p.counts.shrink_to_fit();
        p.negative_counts.shrink_to_fit();
        DdSketch::try_from(DdSketchState {
            alpha,
            positive_store: Buckets {
                counts: Vector1D::from_vec(p.counts),
                offset: p.offset,
            },
            negative_store: Buckets {
                counts: Vector1D::from_vec(p.negative_counts),
                offset: p.negative_offset,
            },
            zero_count: p.zero_count,
            admission_rate,
        })
        .map_err(RmpDecodeError::Uncategorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmp_serde::from_slice;

    const ALPHA: f64 = 0.01;

    fn populated() -> DdSketch {
        let mut sketch = DdSketch::new(ALPHA);
        for v in [0.25f64, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0] {
            sketch.add(&v);
        }
        sketch
    }

    /// A payload with only a positive store.
    fn positive(counts: Vec<u64>, offset: i32) -> DdPayload {
        DdPayload {
            counts,
            offset,
            negative_counts: Vec::new(),
            negative_offset: 0,
            zero_count: 0,
        }
    }

    /// Builds a real envelope with real metadata around a hand-built payload,
    /// so the crafted-bytes tests exercise the decoder's own rules rather than
    /// the envelope's framing.
    fn crafted_payload(alpha: f64, payload: &DdPayload) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(&dd_metadata(alpha, None)).unwrap();
        let payload = rmp_serde::to_vec(payload).unwrap();
        envelope::encode(kind::DDSKETCH, &metadata, &payload)
    }

    fn crafted(alpha: f64, counts: Vec<u64>, offset: i32) -> Vec<u8> {
        crafted_payload(alpha, &positive(counts, offset))
    }

    /// The message a rejected decode fails with, so a test can pin *which*
    /// rule fired rather than settling for any error at all.
    fn decode_error(bytes: &[u8]) -> String {
        DdSketch::from_asapv1(bytes)
            .expect_err("decode must fail")
            .to_string()
    }

    #[test]
    fn ddsketch_envelope_structure_and_round_trip() {
        let sketch = populated();
        let bytes = sketch.to_asapv1().expect("serialize");

        assert!(bytes.starts_with(b"ASAPv1"));
        assert_eq!(&bytes[7..10], &[2u8, 0x05, 0x01]); // kind_id_len=2, kind_id=[0x05,0x01]
        let (kind_id, metadata, _) = envelope::split(&bytes).expect("split");
        assert_eq!(kind_id, &[0x05, 0x01]);
        let meta: DdMetadata = from_slice(metadata).expect("metadata");
        assert_eq!(meta.metadata_version, 1);
        assert_eq!(meta.alpha, ALPHA);

        let decoded = DdSketch::from_asapv1(&bytes).expect("decode");
        assert_eq!(
            decoded.positive_store_counts(),
            sketch.positive_store_counts()
        );
        assert_eq!(
            decoded.positive_store_offset(),
            sketch.positive_store_offset()
        );
        assert_eq!(
            decoded.to_asapv1().expect("re-serialize"),
            bytes,
            "DDSketch serialized bytes differed after round trip"
        );
        for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
            assert_eq!(
                decoded.get_value_at_quantile(q),
                sketch.get_value_at_quantile(q)
            );
        }
    }

    /// The payload is the two stores and the zero count: five fields, with
    /// neither the total sample count nor any running scalar.
    #[test]
    fn ddsketch_payload_is_five_fields() {
        type Five = (Vec<u64>, i32, Vec<u64>, i32, u64);
        let sketch = populated();
        let bytes = sketch.to_asapv1().expect("serialize");
        let (_, _, payload) = envelope::split(&bytes).expect("split");

        let five: Five = envelope::decode_block(payload).expect("5-element payload");
        assert_eq!(five.0.iter().sum::<u64>(), sketch.get_count());
        assert_eq!((five.2.len(), five.3, five.4), (0, 0, 0));
        assert_eq!(
            DdSketch::from_asapv1(&bytes).expect("decode").get_count(),
            sketch.get_count()
        );
    }

    /// Bytes of the retired kind `0x05 0x00` decode to the same stores, with
    /// their `sum` / `min` / `max` dropped, and re-encode as `0x05 0x01`.
    #[test]
    fn ddsketch_decodes_the_retired_kind_with_scalars() {
        let sketch = populated();
        let current = sketch.to_asapv1().expect("serialize");
        let metadata = encode_metadata(ALPHA, 1.0).unwrap();
        let payload = rmp_serde::to_vec(&DdPayloadWithScalars {
            counts: sketch.positive_store_counts().to_vec(),
            offset: sketch.positive_store_offset(),
            sum: 1166.25,
            min: 0.25,
            max: 1000.0,
            negative_counts: Vec::new(),
            negative_offset: 0,
            zero_count: 0,
        })
        .unwrap();
        let retired = envelope::encode(kind::DDSKETCH_WITH_SCALARS, &metadata, &payload);
        let decoded = DdSketch::from_asapv1(&retired).expect("decode the retired kind");
        assert_eq!(decoded.get_count(), sketch.get_count());
        assert_eq!(decoded.to_asapv1().unwrap(), current);

        // A retired payload must still have all eight fields.
        let five = rmp_serde::to_vec(&positive(vec![3], 7)).unwrap();
        let short = envelope::encode(kind::DDSKETCH_WITH_SCALARS, &metadata, &five);
        assert!(DdSketch::from_asapv1(&short).is_err());
    }

    #[test]
    fn ddsketch_empty_round_trip() {
        let sketch = DdSketch::new(ALPHA);
        let bytes = sketch.to_asapv1().expect("serialize");
        let decoded = DdSketch::from_asapv1(&bytes).expect("decode");

        assert_eq!(decoded.get_count(), 0);
        assert_eq!(decoded.get_value_at_quantile(0.5), None);
        assert!(decoded.positive_store_counts().is_empty());
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
    }

    /// A merged sketch — a store grown on both sides — round-trips with its
    /// quantiles intact.
    #[test]
    fn ddsketch_merged_round_trip() {
        let mut left = DdSketch::new(ALPHA);
        let mut right = DdSketch::new(ALPHA);
        for i in 1..=200u32 {
            left.add(&f64::from(i));
            right.add(&(f64::from(i) * 0.001));
        }
        left.merge(&right).expect("merge");

        let bytes = left.to_asapv1().expect("serialize");
        let decoded = DdSketch::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.get_count(), left.get_count());
        for q in [0.0, 0.5, 1.0] {
            assert_eq!(
                decoded.get_value_at_quantile(q),
                left.get_value_at_quantile(q)
            );
        }
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
    }

    /// A crafted envelope carrying another sketch's kind_id must be rejected
    /// even though its metadata and payload parse cleanly.
    #[test]
    fn ddsketch_rejects_foreign_kind_id() {
        let cms =
            crate::CountMinSketch::<crate::Vector2D<i64>, crate::RegularPath>::with_dimensions(
                3, 8,
            );
        let cms_bytes = cms.to_asapv1().expect("serialize CMS");
        assert!(
            DdSketch::from_asapv1(&cms_bytes).is_err(),
            "Count-Min bytes must not decode as a DDSketch"
        );

        let mut bytes = DdSketch::new(ALPHA).to_asapv1().unwrap();
        assert!(DdSketch::from_asapv1(&bytes).is_ok());
        bytes[8] = 0x02;
        assert!(
            DdSketch::from_asapv1(&bytes).is_err(),
            "a foreign kind_id must be rejected"
        );
        // The delta kind is not a state.
        bytes[8] = 0x05;
        bytes[9] = 0x81;
        assert!(DdSketch::from_asapv1(&bytes).is_err());
    }

    /// Fail closed on an unexpected metadata key.
    #[test]
    fn dd_metadata_rejects_unknown_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            alpha: f64,
            bogus_field: u8, // key not in DdMetadata
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            metadata_version: 1,
            alpha: ALPHA,
            bogus_field: 7,
        })
        .unwrap();
        assert!(
            rmp_serde::from_slice::<DdMetadata>(&bytes).is_err(),
            "an unexpected metadata key must be rejected"
        );
    }

    /// `alpha` is required: a DDSketch metadata map missing it does not decode,
    /// so the key cannot be silently defaulted to zero.
    #[test]
    fn dd_metadata_rejects_a_missing_alpha_key() {
        #[derive(Serialize)]
        struct WithoutAlpha {
            metadata_version: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithoutAlpha {
            metadata_version: 1,
        })
        .unwrap();
        assert!(
            rmp_serde::from_slice::<DdMetadata>(&bytes).is_err(),
            "a missing alpha key must be rejected"
        );
    }

    /// `metadata_version` is `1`; any other version is refused.
    #[test]
    fn ddsketch_rejects_another_metadata_version() {
        let payload = rmp_serde::to_vec(&positive(vec![3], 7)).unwrap();
        for version in [0u8, 2] {
            let metadata = rmp_serde::to_vec_named(&DdMetadata {
                metadata_version: version,
                alpha: ALPHA,
                admission_rate: None,
            })
            .unwrap();
            let err = decode_error(&envelope::encode(kind::DDSKETCH, &metadata, &payload));
            assert!(
                err.contains("metadata mismatch"),
                "version {version}: {err}"
            );
        }
    }

    /// `alpha` outside `(0, 1)` makes every bucket index meaningless, so it is
    /// rejected rather than used to derive a nonsense mapping.
    #[test]
    fn ddsketch_rejects_alpha_outside_the_unit_interval() {
        for alpha in [0.0, 1.0, -0.5, 2.0, f64::NAN, f64::INFINITY] {
            let bytes = crafted(alpha, vec![3], 7);
            let err = decode_error(&bytes);
            assert!(
                err.contains("alpha") || err.contains("metadata mismatch"),
                "alpha={alpha} must be rejected by the alpha rule, got {err}"
            );
        }
        // The neighbouring in-range value is the control case.
        assert!(DdSketch::from_asapv1(&crafted(ALPHA, vec![3], 7)).is_ok());
    }

    /// `alpha` is checked before the payload is read: a bad `alpha` in front of
    /// an unreadable payload fails on `alpha`.
    #[test]
    fn ddsketch_checks_alpha_before_the_payload() {
        let metadata = rmp_serde::to_vec_named(&dd_metadata(2.0, None)).unwrap();
        let err = decode_error(&envelope::encode(kind::DDSKETCH, &metadata, &[0xc1]));
        assert!(err.contains("alpha"), "got {err}");
    }

    /// A store whose declared offset pushes its highest bucket index past
    /// `i32` is rejected from the offset and the array length alone, before the
    /// store is rebuilt — and never panics.
    #[test]
    fn ddsketch_rejects_a_store_span_past_i32() {
        let err = decode_error(&crafted(ALPHA, vec![1; 10], i32::MAX - 2));
        assert!(
            err.contains("store span past i32"),
            "an overflowing store span must be rejected as such, got {err}"
        );
        let mut negative = positive(vec![3], 7);
        negative.negative_counts = vec![1; 10];
        negative.negative_offset = i32::MAX - 2;
        let err = decode_error(&crafted_payload(ALPHA, &negative));
        assert!(err.contains("store span past i32"), "negative store: {err}");
        // The same payload one bucket shorter than the boundary decodes.
        let ok = crafted(ALPHA, vec![1; 3], i32::MAX - 2);
        assert!(DdSketch::from_asapv1(&ok).is_ok());
    }

    /// An empty store has exactly one encoding, so a crafted non-zero offset on
    /// one is rejected instead of decoding into a sketch that re-serializes to
    /// different bytes.
    #[test]
    fn ddsketch_rejects_a_nonzero_offset_on_an_empty_store() {
        let mut payload = positive(Vec::new(), 0);
        payload.offset = 42;
        let err = decode_error(&crafted_payload(ALPHA, &payload));
        assert!(
            err.contains("empty store must be at offset 0"),
            "a non-zero empty-store offset must be rejected as such, got {err}"
        );
    }

    /// Counts that overflow the recovered total — across both stores and the
    /// zero count — are rejected rather than wrapping.
    #[test]
    fn ddsketch_rejects_a_total_count_overflow() {
        let mut negative = positive(vec![u64::MAX], 0);
        negative.negative_counts = vec![1];
        let mut zero = positive(vec![u64::MAX], 0);
        zero.zero_count = 1;
        for payload in [positive(vec![u64::MAX, 1], 0), negative, zero] {
            let err = decode_error(&crafted_payload(ALPHA, &payload));
            assert!(
                err.contains("overflow the total sample count"),
                "an overflowing total must be rejected as such, got {err}"
            );
        }
    }

    /// Signed stores and the zero count decode and re-serialize byte for byte.
    #[test]
    fn ddsketch_signed_payload_round_trips() {
        let mut negative = positive(vec![3], 7);
        negative.negative_counts = vec![1];
        negative.negative_offset = 5;
        let mut zero = positive(vec![3], 7);
        zero.zero_count = 2;
        for payload in [negative, zero] {
            let bytes = crafted_payload(ALPHA, &payload);
            let decoded = DdSketch::from_asapv1(&bytes).expect("decode");
            assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
        }
    }

    /// A sketch whose store spans past `i32` would be refused on decode, so it
    /// must not serialize either.
    #[test]
    fn ddsketch_rejects_serializing_a_store_span_past_i32() {
        let mut sketch = DdSketch::new(ALPHA);
        sketch.add(&1.0f64);
        sketch.positive_store.counts = Vector1D::from_vec(vec![1u64; 10]);
        sketch.positive_store.offset = i32::MAX - 2;
        assert!(
            sketch.to_asapv1().is_err(),
            "a store spanning past i32 must not serialize"
        );
    }

    /// Bucket counts that overflow the total sample count must not serialize
    /// either — the encode side enforces the decoder's rule.
    #[test]
    fn ddsketch_rejects_serializing_an_overflowing_bucket_total() {
        let mut sketch = DdSketch::new(ALPHA);
        sketch.add(&1.0f64);
        sketch.positive_store.counts = Vector1D::from_vec(vec![u64::MAX, 1]);
        sketch.positive_store.offset = 0;
        assert!(
            sketch.to_asapv1().is_err(),
            "bucket counts overflowing the total must not serialize"
        );
    }

    /// Known stores serialize to the `asapv1_golden/` bytes.
    #[test]
    fn known_state_matches_golden() {
        use crate::asapv1::envelope::hex;

        let state = |positive: (&[u64], i32), negative: (&[u64], i32), zero_count| {
            let store = |(counts, offset): (&[u64], i32)| Buckets {
                counts: Vector1D::from_vec(counts.to_vec()),
                offset,
            };
            DdSketch::try_from(DdSketchState {
                alpha: 0.01,
                positive_store: store(positive),
                negative_store: store(negative),
                zero_count,
                admission_rate: 1.0,
            })
            .expect("valid state")
        };

        let positive = state(
            (&[1, 0, 127, 128, 300, 65536, 4294967296], -40),
            (&[], 0),
            0,
        );
        let golden = hex(include_str!(
            "../../../asapv1_golden/ddsketch_buckets_positive_a001.hex"
        ));
        assert_eq!(positive.to_asapv1().unwrap(), golden);

        let signed = state((&[3, 0, 2], 310), (&[5, 1], -208), 7);
        let golden = hex(include_str!(
            "../../../asapv1_golden/ddsketch_buckets_signed_a001.hex"
        ));
        assert_eq!(signed.to_asapv1().unwrap(), golden);
    }
}
