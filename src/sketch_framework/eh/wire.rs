//! ASAPv1 wire serialization for [`ExponentialHistogram`].
//!
//! Child submodule of [`crate::sketch_framework::eh`]: it holds the
//! metadata/payload DTOs, the kind_id constant and the `serialize_to_bytes` /
//! `deserialize_from_bytes` impls. Being a descendant module, it reads the
//! private `infer_merge_norm` / `compute_l2_mass` rules directly.
//!
//! ExponentialHistogram is one kind_id, `0x13 0x00`. `window` and `k` are
//! construction config and live in the metadata, so the payload is the buckets,
//! their time ranges and sizes, and the prototype.
//!
//! ## Buckets are inlined
//!
//! Each bucket holds one [`EHSketchList`], written into this payload as the
//! `[variant, descriptor, state]` triple
//! [`crate::sketch_framework::eh_sketch_list::wire`] defines. No bucket carries
//! an envelope, a magic or a kind_id of its own.
//!
//! ## No hash-spec group
//!
//! The histogram never hashes: its buckets' sketches do, each in its own way,
//! and three of the ten do not hash at all. Every hash spec on the wire is the
//! one inside a bucket's own `descriptor`.
//!
//! ## Emitted order (byte-stable round trips)
//!
//! Buckets are emitted oldest to newest and the parallel arrays follow that
//! order, so a decoded histogram re-serializes byte-identically.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::sketch_framework::eh_sketch_list::wire::{SketchState, rebuild_sketch, sketch_state};

use super::{EHBucket, ExponentialHistogram, compute_l2_mass, infer_merge_norm};

/// ExponentialHistogram kind_id: family `0x13`, single algorithm variant `0x00`.
const EH_KIND: &[u8] = &[0x13, 0x00];

/// ExponentialHistogram descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it).
///
/// Structural params only. The histogram does not hash, so there is no
/// hash-spec group; each bucket's `descriptor` carries its sketch's own.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EhMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) window: u64,
    pub(crate) k: u32,
}

/// Builds the ExponentialHistogram descriptor metadata.
pub(crate) fn eh_metadata(window: u64, k: u32) -> EhMetadata {
    EhMetadata {
        metadata_version: 1,
        window,
        k,
    }
}

/// ExponentialHistogram payload (ASAPv1, kind_id `0x13 0x00`), a msgpack
/// **array** (`to_vec`, positional):
/// `[buckets, sizes, min_times, max_times, prototype]`.
///
/// The first four are parallel and dense, oldest bucket first; the bucket count
/// is `len(buckets)`. `prototype` is the sketch every new bucket is cloned from.
#[derive(Debug, Serialize, Deserialize)]
struct EhPayload {
    buckets: Vec<SketchState>,
    sizes: Vec<u64>,
    min_times: Vec<u64>,
    max_times: Vec<u64>,
    prototype: SketchState,
}

/// Rejects a bucket state the algorithm never reaches: an empty bucket, an
/// inverted time range, or a cached mass that disagrees with its sketch.
fn check_bucket(index: usize, bucket: &EHBucket) -> Result<(), String> {
    if bucket.size == 0 {
        return Err(format!("bucket {index} has size 0"));
    }
    if bucket.min_time > bucket.max_time {
        return Err(format!(
            "bucket {index} spans [{}, {}]",
            bucket.min_time, bucket.max_time
        ));
    }
    let mass = compute_l2_mass(&bucket.bucket);
    if bucket.l2_mass != mass {
        return Err(format!(
            "bucket {index} caches l2_mass {} against its sketch's {mass}",
            bucket.l2_mass
        ));
    }
    Ok(())
}

// Wire serialization for ExponentialHistogram. `wire` is a descendant of the
// framework module, so this impl reads the parent's derivation rules directly.
impl ExponentialHistogram {
    /// Serializes the histogram into an ASAPv1 MessagePack envelope
    /// (kind_id `0x13 0x00`). `window` and `k` land in the metadata; the
    /// payload is the buckets, their sizes and time ranges, and the prototype.
    ///
    /// A `k` of zero, a bucket the algorithm never reaches, and a `merge_norm`
    /// that disagrees with the prototype are errors rather than bytes that
    /// would be refused on decode.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let fail = |problem: String| {
            RmpEncodeError::Syntax(format!("ASAPv1 ExponentialHistogram envelope: {problem}"))
        };
        if self.k == 0 {
            return Err(fail("k must be at least 1".to_string()));
        }
        let k = u32::try_from(self.k)
            .map_err(|_| fail(format!("k {} exceeds the u32 metadata field", self.k)))?;
        if self.merge_norm != infer_merge_norm(&self.type_to_clone) {
            return Err(fail(format!(
                "merge_norm {:?} disagrees with the prototype's",
                self.merge_norm
            )));
        }
        let mut buckets = Vec::with_capacity(self.payload.len());
        let mut sizes = Vec::with_capacity(self.payload.len());
        for (index, bucket) in self.payload.iter().enumerate() {
            check_bucket(index, bucket).map_err(fail)?;
            sizes.push(u64::try_from(bucket.size).map_err(|_| {
                fail(format!(
                    "bucket {index} size {} exceeds the u64 payload field",
                    bucket.size
                ))
            })?);
            buckets.push(sketch_state(&bucket.bucket)?);
        }
        let metadata = rmp_serde::to_vec_named(&eh_metadata(self.window, k))?;
        let payload = rmp_serde::to_vec(&EhPayload {
            buckets,
            sizes,
            min_times: self.payload.iter().map(|b| b.min_time).collect(),
            max_times: self.payload.iter().map(|b| b.max_time).collect(),
            prototype: sketch_state(&self.type_to_clone)?,
        })?;
        Ok(envelope::encode(EH_KIND, &metadata, &payload))
    }

    /// Deserializes a histogram from an ASAPv1 MessagePack envelope. The bucket
    /// count is the payload's own array length, `l2_mass` is recomputed from
    /// each decoded sketch and `merge_norm` from the decoded prototype.
    ///
    /// Every state the algorithm could not have produced is rejected with an
    /// error rather than a panic, and no declared count sizes an allocation
    /// before the payload is measured against it.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != EH_KIND {
            return Err(RmpDecodeError::Uncategorized(format!(
                "ExponentialHistogram kind_id mismatch: stored {kind_id:?}, expected {EH_KIND:?}"
            )));
        }
        let meta: EhMetadata = from_slice(metadata)?;
        // `window` and `k` are properties of the stored histogram rather than
        // of the target, so they are echoed back into the expected block and
        // bounded by range instead of being pinned.
        if meta != eh_metadata(meta.window, meta.k) {
            return Err(RmpDecodeError::Uncategorized(
                "ASAPv1 ExponentialHistogram envelope: metadata mismatch".to_string(),
            ));
        }
        if meta.k == 0 {
            return Err(RmpDecodeError::Uncategorized(
                "ExponentialHistogram k must be at least 1".to_string(),
            ));
        }
        let p: EhPayload = from_slice(payload)?;
        let count = p.buckets.len();
        if p.sizes.len() != count || p.min_times.len() != count || p.max_times.len() != count {
            return Err(RmpDecodeError::Uncategorized(format!(
                "ExponentialHistogram parallel lengths (buckets {count}, sizes {}, min_times {}, max_times {}) disagree",
                p.sizes.len(),
                p.min_times.len(),
                p.max_times.len()
            )));
        }
        let mut decoded = Vec::with_capacity(count);
        for (index, triple) in p.buckets.iter().enumerate() {
            let sketch = rebuild_sketch(triple)?;
            let bucket = EHBucket {
                l2_mass: compute_l2_mass(&sketch),
                bucket: sketch,
                size: usize::try_from(p.sizes[index]).map_err(|_| {
                    RmpDecodeError::Uncategorized(format!(
                        "ExponentialHistogram bucket {index} size {} exceeds this target's usize",
                        p.sizes[index]
                    ))
                })?,
                min_time: p.min_times[index],
                max_time: p.max_times[index],
            };
            check_bucket(index, &bucket).map_err(RmpDecodeError::Uncategorized)?;
            decoded.push(bucket);
        }
        let type_to_clone = rebuild_sketch(&p.prototype)?;
        Ok(ExponentialHistogram {
            payload: decoded,
            window: meta.window,
            k: meta.k as usize,
            merge_norm: infer_merge_norm(&type_to_clone),
            type_to_clone,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch_framework::eh_sketch_list::wire::tests::{
        alt_profile_triple, populated_variants, relabelled, sample_input,
    };
    use crate::sketch_framework::eh_sketch_list::wire::{
        CM_VARIANT, COCO_VARIANT, ELASTIC_VARIANT, UNIFORM_VARIANT,
    };
    use crate::sketch_framework::eh_sketch_list::{EHSketchList, SketchNorm};
    use crate::{CountMin, DataInput, FastPath, Vector2D};

    /// A histogram over Count-Min buckets with a few timestamped updates.
    fn populated_eh() -> ExponentialHistogram {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8)),
        );
        for i in 0..6u64 {
            eh.update(i * 10, &DataInput::U64(i % 3));
        }
        eh
    }

    /// The `(size, min_time, max_time, l2_mass)` of every bucket, oldest first.
    /// The mass is compared bit-exactly: it is recomputed on decode, not read.
    fn ranges(eh: &ExponentialHistogram) -> Vec<(usize, u64, u64, u64)> {
        eh.payload
            .iter()
            .map(|b| (b.size, b.min_time, b.max_time, b.l2_mass.to_bits()))
            .collect()
    }

    /// Wraps a crafted payload in a `0x13 0x00` envelope with valid metadata.
    fn envelope_for(payload: &EhPayload) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(&eh_metadata(1000, 2)).unwrap();
        envelope::encode(EH_KIND, &metadata, &rmp_serde::to_vec(payload).unwrap())
    }

    #[test]
    fn eh_round_trip_serialization() {
        let eh = populated_eh();
        let encoded = eh.serialize_to_bytes().expect("serialize EH");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x13, 0x00]); // kind_id_len=2, kind_id=[0x13,0x00]

        let decoded = ExponentialHistogram::deserialize_from_bytes(&encoded).expect("deserialize");
        assert_eq!(decoded.window, eh.window);
        assert_eq!(decoded.k, eh.k);
        assert_eq!(decoded.merge_norm, eh.merge_norm);
        assert_eq!(ranges(&decoded), ranges(&eh));
        assert_eq!(decoded.bucket_count(), eh.bucket_count());
    }

    /// Every variant this build carries round-trips as an EH bucket and as the
    /// prototype.
    #[test]
    fn eh_every_variant_round_trips_as_a_bucket() {
        for prototype in populated_variants() {
            let name = prototype.sketch_type();
            let key = sample_input(name);
            let mut eh = ExponentialHistogram::new(3, 1000, prototype);
            for i in 0..4u64 {
                eh.update(i * 5, &key);
            }
            let encoded = eh
                .serialize_to_bytes()
                .unwrap_or_else(|e| panic!("serialize EH<{name}>: {e}"));
            let decoded = ExponentialHistogram::deserialize_from_bytes(&encoded)
                .unwrap_or_else(|e| panic!("deserialize EH<{name}>: {e}"));
            assert_eq!(decoded.type_to_clone.sketch_type(), name);
            assert_eq!(ranges(&decoded), ranges(&eh));
            let again = decoded.serialize_to_bytes().expect("re-serialize");
            assert_eq!(encoded, again, "EH<{name}> is not byte-stable");
        }
    }

    /// An empty histogram has exactly one encoding and round-trips.
    #[test]
    fn eh_empty_has_one_encoding_and_round_trips() {
        let prototype =
            EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let a = ExponentialHistogram::new(2, 1000, prototype.clone());
        let b = ExponentialHistogram::new(2, 1000, prototype);
        let bytes = a.serialize_to_bytes().expect("serialize");
        assert_eq!(bytes, b.serialize_to_bytes().expect("serialize"));

        let decoded = ExponentialHistogram::deserialize_from_bytes(&bytes).expect("deserialize");
        assert_eq!(decoded.bucket_count(), 0);
        assert_eq!(bytes, decoded.serialize_to_bytes().expect("re-serialize"));
    }

    /// A prototype carrying state keeps it, so later buckets start from it.
    #[test]
    fn eh_carries_a_non_empty_prototype() {
        let mut prototype =
            EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        for _ in 0..7 {
            prototype.insert(&DataInput::U64(9));
        }
        let eh = ExponentialHistogram::new(2, 1000, prototype);
        let bytes = eh.serialize_to_bytes().expect("serialize");
        let decoded = ExponentialHistogram::deserialize_from_bytes(&bytes).expect("deserialize");
        assert_eq!(
            decoded.type_to_clone.query(&DataInput::U64(9)),
            eh.type_to_clone.query(&DataInput::U64(9))
        );
        assert!(decoded.type_to_clone.query(&DataInput::U64(9)).unwrap() >= 7.0);
    }

    /// A decoded histogram re-serializes byte-identically and answers an
    /// interval query the way the original did.
    #[test]
    fn eh_decoded_re_serializes_byte_identically_and_queries_agree() {
        let eh = populated_eh();
        let bytes = eh.serialize_to_bytes().expect("serialize");
        let decoded = ExponentialHistogram::deserialize_from_bytes(&bytes).expect("deserialize");
        assert_eq!(bytes, decoded.serialize_to_bytes().expect("re-serialize"));

        let key = DataInput::U64(1);
        let original = eh.query_interval_merge(0, 50).expect("query");
        let round_tripped = decoded.query_interval_merge(0, 50).expect("query");
        assert_eq!(original.query(&key).ok(), round_tripped.query(&key).ok());
    }

    /// An EHSketchList envelope and a Count-Min envelope are not
    /// ExponentialHistogram envelopes.
    #[test]
    fn eh_rejects_foreign_kind_ids() {
        let list = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let list_bytes = list.serialize_to_bytes().expect("serialize EHSketchList");
        assert!(ExponentialHistogram::deserialize_from_bytes(&list_bytes).is_err());

        let cms = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8);
        let cms_bytes = cms.serialize_to_bytes().expect("serialize CMS");
        assert!(ExponentialHistogram::deserialize_from_bytes(&cms_bytes).is_err());
    }

    /// Fail closed on an unexpected metadata key.
    #[test]
    fn eh_metadata_rejects_unknown_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            window: u64,
            k: u32,
            bogus_field: u8,
        }
        let bytes = rmp_serde::to_vec_named(&WithExtra {
            metadata_version: 1,
            window: 1000,
            k: 2,
            bogus_field: 7,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<EhMetadata>(&bytes).is_err());
    }

    /// `k` is required: a metadata map missing it does not decode, so it can
    /// never be silently defaulted.
    #[test]
    fn eh_metadata_rejects_a_missing_key() {
        #[derive(Serialize)]
        struct WithoutK {
            metadata_version: u8,
            window: u64,
        }
        let bytes = rmp_serde::to_vec_named(&WithoutK {
            metadata_version: 1,
            window: 1000,
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<EhMetadata>(&bytes).is_err());
    }

    /// `k` is at least 1 on both sides.
    #[test]
    fn eh_rejects_a_zero_k() {
        let mut eh = populated_eh();
        eh.k = 0;
        assert!(eh.serialize_to_bytes().is_err());

        let good = populated_eh().serialize_to_bytes().expect("serialize");
        let (_, _, payload) = envelope::split(&good).expect("split");
        let metadata = rmp_serde::to_vec_named(&eh_metadata(1000, 0)).unwrap();
        let bytes = envelope::encode(EH_KIND, &metadata, payload);
        assert!(ExponentialHistogram::deserialize_from_bytes(&bytes).is_err());
    }

    /// A declared array far longer than the buckets the payload carries is
    /// rejected before anything is sized from it.
    #[test]
    fn eh_rejects_parallel_arrays_of_unequal_length() {
        let sketch = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let triple = sketch_state(&sketch).expect("state");
        let payload = EhPayload {
            buckets: vec![sketch_state(&sketch).expect("state")],
            sizes: vec![1; 1_000_000],
            min_times: vec![0],
            max_times: vec![0],
            prototype: triple,
        };
        assert!(ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload)).is_err());
    }

    /// A zero size and an inverted time range are states the algorithm never
    /// reaches, rejected on both sides.
    #[test]
    fn eh_rejects_impossible_bucket_state() {
        let sketch = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let payload = EhPayload {
            buckets: vec![sketch_state(&sketch).expect("state")],
            sizes: vec![0],
            min_times: vec![0],
            max_times: vec![0],
            prototype: sketch_state(&sketch).expect("state"),
        };
        assert!(ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload)).is_err());

        let payload = EhPayload {
            buckets: vec![sketch_state(&sketch).expect("state")],
            sizes: vec![1],
            min_times: vec![9],
            max_times: vec![4],
            prototype: sketch_state(&sketch).expect("state"),
        };
        assert!(ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload)).is_err());

        let mut eh = populated_eh();
        eh.payload[0].size = 0;
        assert!(eh.serialize_to_bytes().is_err());
    }

    /// A cached `l2_mass` or `merge_norm` that disagrees with the state it is
    /// derived from has no encoding.
    #[test]
    fn eh_rejects_derived_fields_that_disagree() {
        let mut eh = populated_eh();
        eh.payload[0].l2_mass = 42.0;
        assert!(eh.serialize_to_bytes().is_err());

        let mut eh = populated_eh();
        eh.merge_norm = SketchNorm::L2;
        assert!(eh.serialize_to_bytes().is_err());
    }

    /// An experimental variant tag in a bucket is rejected without the feature.
    /// Crafted bytes, so the test runs in both builds.
    #[test]
    fn eh_rejects_an_experimental_variant_tag_in_a_bucket() {
        let sketch = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        for variant in [COCO_VARIANT, ELASTIC_VARIANT, UNIFORM_VARIANT] {
            let payload = EhPayload {
                buckets: vec![relabelled(&sketch, variant)],
                sizes: vec![1],
                min_times: vec![0],
                max_times: vec![0],
                prototype: relabelled(&sketch, CM_VARIANT),
            };
            let message = ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload))
                .expect_err("a relabelled bucket must not decode")
                .to_string();
            assert!(!message.is_empty());
            #[cfg(not(feature = "experimental"))]
            {
                assert!(message.contains(variant), "{message}");
                assert!(message.contains("experimental"), "{message}");
            }
        }
    }

    /// A bucket whose descriptor names a custom hash profile is rejected: the
    /// variant's decoder pins the profile of the type it rebuilds.
    #[test]
    fn eh_rejects_a_custom_hash_profile_bucket() {
        let sketch = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let payload = EhPayload {
            buckets: vec![alt_profile_triple()],
            sizes: vec![1],
            min_times: vec![0],
            max_times: vec![0],
            prototype: sketch_state(&sketch).expect("state"),
        };
        assert!(ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload)).is_err());
    }

    /// An unknown variant tag in a bucket is rejected by name.
    #[test]
    fn eh_rejects_an_unknown_variant_tag_in_a_bucket() {
        let sketch = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 8));
        let payload = EhPayload {
            buckets: vec![relabelled(&sketch, "Bogus")],
            sizes: vec![1],
            min_times: vec![0],
            max_times: vec![0],
            prototype: sketch_state(&sketch).expect("state"),
        };
        let message = ExponentialHistogram::deserialize_from_bytes(&envelope_for(&payload))
            .expect_err("an unknown variant must not decode")
            .to_string();
        assert!(message.contains("Bogus"), "{message}");
    }
}
