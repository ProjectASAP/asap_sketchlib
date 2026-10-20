//! ASAPv1 wire serialization for DDSketch.
//!
//! Child submodule of [`crate::sketches::ddsketch`]: it holds ALL of DDSketch's
//! ASAPv1 serialization (the metadata/payload DTOs, the kind_id constant, and
//! the `to_asapv1` / `from_asapv1` impls) while the
//! algorithm lives in the parent module file. Being a descendant module, it
//! reads the sketch's private `store` / `count` / `sum` / `min` / `max` fields
//! directly without widening any field visibility. See
//! `docs/asapv1_wire_format.md` §3.8.
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

use crate::message_pack_format::kind::DDSKETCH;
use crate::message_pack_format::kind;
use crate::common::structures::Vector1D;
use crate::message_pack_format::admission_rate;
use crate::message_pack_format::envelope;
use crate::message_pack_format::wire_key::exact;

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

/// DDSketch payload (ASAPv1 §3.8), a msgpack **array** (`to_vec`, positional).
/// Each store is carried verbatim including the growth padding, so a decoded
/// sketch re-serializes byte-identically; the bucket index of `counts[i]` is
/// `offset + i`. `sum` / `min` / `max` are the running scalars, which the
/// bucket counts do not determine. The total sample count is the sum of both
/// stores and `zero_count`, and so is not carried.
#[derive(Debug, Serialize, Deserialize)]
struct DdPayload {
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

/// Checks the running scalars against the total count. An empty sketch carries
/// `0.0` / `+inf` / `-inf`, the state [`DdSketch::new`] starts in. A populated
/// one carries finite scalars with `min <= max`; when every value is positive,
/// also `0 < min` and `sum >= min`.
fn check_scalars(
    count: u64,
    positive_only: bool,
    sum: f64,
    min: f64,
    max: f64,
) -> Result<(), String> {
    if count == 0 {
        if sum == 0.0 && min == f64::INFINITY && max == f64::NEG_INFINITY {
            return Ok(());
        }
        return Err(format!(
            "DDSketch empty store must carry sum=0, min=inf, max=-inf, got {sum}, {min}, {max}"
        ));
    }
    if !(sum.is_finite() && min.is_finite() && max.is_finite()) {
        return Err(format!(
            "DDSketch scalars must be finite for a populated store: {sum}, {min}, {max}"
        ));
    }
    if !(min <= max && (!positive_only || (min > 0.0 && sum >= min))) {
        return Err(format!(
            "DDSketch scalars out of order: sum={sum}, min={min}, max={max}"
        ));
    }
    Ok(())
}

/// Checks a state against §3.8 rules 3 through 7 and returns its total sample
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
    let positive_only = negative == 0 && state.zero_count == 0;
    check_scalars(count, positive_only, state.sum, state.min, state.max)?;
    Ok(count)
}

// Wire serialization for DDSketch. `wire` is a descendant of the sketch module,
// so these impls read the private fields and construct the struct directly.
impl DdSketch {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x05 0x00`). `alpha` lands in the metadata; the payload is the
    /// two bucket stores, the zero count and the three scalars.
    ///
    /// A sketch whose state the decoder would refuse — an out-of-range `alpha`,
    /// a store spanning past `i32`, counts that overflow `u64`, or inconsistent
    /// scalars — is an error rather than bytes that would be refused on decode.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let state = DdSketchState {
            alpha: self.alpha,
            positive_store: self.positive_store.clone(),
            sum: self.sum,
            min: self.min,
            max: self.max,
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
            sum: self.sum,
            min: self.min,
            max: self.max,
            negative_counts: state.negative_store.counts.into_vec(),
            negative_offset: state.negative_store.offset,
            zero_count: self.zero_count,
        })?;
        Ok(envelope::encode(kind::DDSKETCH, &metadata, &payload))
    }

    /// Deserializes a DDSketch from an ASAPv1 MessagePack envelope. The index
    /// mapping is rebuilt from the metadata `alpha` and the total sample count
    /// is summed from the stores. Every inconsistency fails closed.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::DDSKETCH {
            return Err(RmpDecodeError::Uncategorized(format!(
                "DDSketch kind_id mismatch: stored {kind_id:?}, expected {DDSKETCH:?}"
            )));
        }
        let (alpha, admission_rate) = decode_metadata(metadata)?;
        let p: DdPayload = envelope::decode_block(payload)?;
        DdSketch::try_from(DdSketchState {
            alpha,
            positive_store: Buckets {
                counts: Vector1D::from_vec(p.counts),
                offset: p.offset,
            },
            sum: p.sum,
            min: p.min,
            max: p.max,
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
    fn positive(counts: Vec<u64>, offset: i32, sum: f64, min: f64, max: f64) -> DdPayload {
        DdPayload {
            counts,
            offset,
            sum,
            min,
            max,
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

    fn crafted(alpha: f64, counts: Vec<u64>, offset: i32, sum: f64, min: f64, max: f64) -> Vec<u8> {
        crafted_payload(alpha, &positive(counts, offset, sum, min, max))
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
        assert_eq!(&bytes[7..10], &[2u8, 0x05, 0x00]); // kind_id_len=2, kind_id=[0x05,0x00]
        let (kind_id, metadata, _) = envelope::split(&bytes).expect("split");
        assert_eq!(kind_id, &[0x05, 0x00]);
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

    /// `sum` / `min` / `max` are not derivable from the buckets, so they are
    /// carried and come back exactly — not as the α-bounded bucket
    /// representatives a recomputation would produce.
    #[test]
    fn ddsketch_scalars_survive_exactly() {
        let sketch = populated();
        let decoded =
            DdSketch::from_asapv1(&sketch.to_asapv1().expect("serialize")).expect("decode");
        assert_eq!(decoded.sum(), sketch.sum());
        assert_eq!(decoded.min(), Some(0.25));
        assert_eq!(decoded.max(), Some(1000.0));
        assert_eq!(decoded.alpha(), sketch.alpha());
    }

    /// A positive-only sketch still carries all eight payload fields, and the
    /// total sample count is not one of them.
    #[test]
    fn ddsketch_payload_is_eight_fields_without_a_count() {
        type Eight = (Vec<u64>, i32, f64, f64, f64, Vec<u64>, i32, u64);
        let sketch = populated();
        let bytes = sketch.to_asapv1().expect("serialize");
        let (_, _, payload) = envelope::split(&bytes).expect("split");

        let eight: Eight = envelope::decode_block(payload).expect("8-element payload");
        assert_eq!(eight.0.iter().sum::<u64>(), sketch.get_count());
        assert_eq!((eight.5.len(), eight.6, eight.7), (0, 0, 0));
        assert_eq!(
            DdSketch::from_asapv1(&bytes).expect("decode").get_count(),
            sketch.get_count()
        );
    }

    #[test]
    fn ddsketch_empty_round_trip() {
        let sketch = DdSketch::new(ALPHA);
        let bytes = sketch.to_asapv1().expect("serialize");
        let decoded = DdSketch::from_asapv1(&bytes).expect("decode");

        assert_eq!(decoded.get_count(), 0);
        assert_eq!(decoded.min(), None);
        assert_eq!(decoded.max(), None);
        assert!(decoded.positive_store_counts().is_empty());
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
    }

    /// A merged sketch — a store grown on both sides — round-trips with its
    /// scalars and quantiles intact.
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
        assert_eq!(decoded.sum(), left.sum());
        assert_eq!(decoded.min(), left.min());
        assert_eq!(decoded.max(), left.max());
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
        let payload = rmp_serde::to_vec(&positive(vec![3], 7, 6.0, 1.5, 2.5)).unwrap();
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
            let bytes = crafted(alpha, vec![3], 7, 6.0, 1.5, 2.5);
            let err = decode_error(&bytes);
            assert!(
                err.contains("alpha") || err.contains("metadata mismatch"),
                "alpha={alpha} must be rejected by the alpha rule, got {err}"
            );
        }
        // The neighbouring in-range value is the control case.
        assert!(DdSketch::from_asapv1(&crafted(ALPHA, vec![3], 7, 6.0, 1.5, 2.5)).is_ok());
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
        let err = decode_error(&crafted(ALPHA, vec![1; 10], i32::MAX - 2, 5.0, 1.0, 2.0));
        assert!(
            err.contains("store span past i32"),
            "an overflowing store span must be rejected as such, got {err}"
        );
        let mut negative = positive(vec![3], 7, 6.0, -1.0, 2.5);
        negative.negative_counts = vec![1; 10];
        negative.negative_offset = i32::MAX - 2;
        let err = decode_error(&crafted_payload(ALPHA, &negative));
        assert!(err.contains("store span past i32"), "negative store: {err}");
        // The same payload one bucket shorter than the boundary decodes.
        let ok = crafted(ALPHA, vec![1; 3], i32::MAX - 2, 5.0, 1.0, 2.0);
        assert!(DdSketch::from_asapv1(&ok).is_ok());
    }

    /// An empty store has exactly one encoding, so a crafted non-zero offset on
    /// one is rejected instead of decoding into a sketch that re-serializes to
    /// different bytes.
    #[test]
    fn ddsketch_rejects_a_nonzero_offset_on_an_empty_store() {
        let empty = || positive(Vec::new(), 0, 0.0, f64::INFINITY, f64::NEG_INFINITY);
        let mut payload = empty();
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
        let mut negative = positive(vec![u64::MAX], 0, 5.0, -1.0, 2.0);
        negative.negative_counts = vec![1];
        let mut zero = positive(vec![u64::MAX], 0, 5.0, 0.0, 2.0);
        zero.zero_count = 1;
        for payload in [
            positive(vec![u64::MAX, 1], 0, 5.0, 1.0, 2.0),
            negative,
            zero,
        ] {
            let err = decode_error(&crafted_payload(ALPHA, &payload));
            assert!(
                err.contains("overflow the total sample count"),
                "an overflowing total must be rejected as such, got {err}"
            );
        }
    }

    /// The scalars the buckets do not determine are still bounded by them: an
    /// empty store carries the sentinels, a populated one finite positive
    /// bounds in order.
    #[test]
    fn ddsketch_rejects_inconsistent_scalars() {
        let cases = [
            // Populated store with the empty-store sentinels.
            (vec![3u64], 7i32, 6.0f64, f64::INFINITY, f64::NEG_INFINITY),
            // min above max.
            (vec![3], 7, 6.0, 9.0, 2.0),
            // Non-positive min.
            (vec![3], 7, 6.0, 0.0, 2.0),
            // Sum below the smallest ingested value.
            (vec![3], 7, 0.5, 1.5, 2.5),
            // Empty store carrying a non-zero sum.
            (Vec::new(), 0, 5.0, f64::INFINITY, f64::NEG_INFINITY),
            // All-zero buckets carrying populated-store scalars.
            (vec![0, 0], 7, 6.0, 1.5, 2.5),
        ];
        for (counts, offset, sum, min, max) in cases {
            let err = decode_error(&crafted(ALPHA, counts, offset, sum, min, max));
            assert!(
                err.contains("DDSketch empty store must carry")
                    || err.contains("scalars out of order")
                    || err.contains("scalars must be finite"),
                "sum={sum}, min={min}, max={max} must be rejected by a scalar rule, got {err}"
            );
        }
    }

    /// `0 < min` and `sum >= min` hold only while the negative store sums to
    /// zero and `zero_count` is zero; an all-zero negative store keeps them.
    #[test]
    fn ddsketch_positive_scalar_checks_follow_the_counts() {
        let mut zero_negative = positive(vec![3], 7, 6.0, -1.0, 2.5);
        zero_negative.negative_counts = vec![0, 0];
        zero_negative.negative_offset = 5;
        let err = decode_error(&crafted_payload(ALPHA, &zero_negative));
        assert!(err.contains("scalars out of order"), "got {err}");

        let mut negative = positive(vec![3], 7, -0.5, -1.5, 2.5);
        negative.negative_counts = vec![1];
        negative.negative_offset = 5;
        let mut zero = positive(vec![3], 7, 0.0, 0.0, 2.5);
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
}
