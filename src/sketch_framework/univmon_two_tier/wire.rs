//! ASAPv1 wire serialization for [`UnivMonTwoTier`].
//!
//! Child submodule of [`crate::sketch_framework::univmon_two_tier`]: it holds
//! the metadata DTO, the kind_id constant and the `to_asapv1` /
//! `from_asapv1` impls, while the algorithm lives in the parent
//! module file. Being a descendant module, it reads the private `update_mode`
//! and `candidate_complete` fields directly without widening any field
//! visibility. See `docs/asapv1_wire_format.md`.
//!
//! Two-tier UnivMon is one algorithm — a single kind_id `0x11 0x00`. It
//! shares [`UnivMon`](crate::UnivMon)'s payload
//! (`[counts, l2, heap_lens, keys, heap_counts, candidate_complete,
//! bucket_size, update_mode]`) and differs only in its metadata: the two-tier
//! layout gives layer `i` the elephant dimensions while `i < elephant_layers`
//! and the mouse dimensions after, so every per-layer geometry is derived and
//! none is stored.
//!
//! [`UnivMonPool`](super::UnivMonPool) is a free-list of scratch
//! `UnivMon`s rather than a sketch, and has no wire kind.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::message_pack_format::kind;
use crate::message_pack_format::kind::UNIVMON_TWO_TIER;
use crate::sketch_framework::univmon::wire::{
    check_layers, decode_pyramid, encode_pyramid, pyramid_key_type, pyramid_state, rebuild_layers,
    update_mode_of, update_mode_tag,
};
use crate::sketches::countminsketch_heap::heap_wire::check_empty_key_type;
use crate::{DefaultXxHasher, HashProfile};

use super::UnivMonTwoTier;

/// UnivMonTwoTier descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it). Hash-spec fields first, then the
/// two-tier layout and the heaps' `key_type`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UnivMonTwoTierMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) seed_list: Vec<u64>,
    pub(crate) layers: u32,
    pub(crate) elephant_layers: u32,
    pub(crate) elephant_rows: u32,
    pub(crate) elephant_cols: u32,
    pub(crate) mouse_rows: u32,
    pub(crate) mouse_cols: u32,
    pub(crate) heap_size: u32,
    pub(crate) key_type: String,
}

/// The five layout fields, in the order the metadata carries them.
pub(crate) struct TwoTierLayout {
    pub(crate) elephant_layers: u32,
    pub(crate) elephant_rows: u32,
    pub(crate) elephant_cols: u32,
    pub(crate) mouse_rows: u32,
    pub(crate) mouse_cols: u32,
}

/// Builds the UnivMonTwoTier descriptor metadata from the hasher's
/// [`HashProfile`], so the wire bytes truthfully describe how the sketch was
/// hashed.
pub(crate) fn univmon_two_tier_metadata<H: HashProfile>(
    layers: u32,
    layout: &TwoTierLayout,
    heap_size: u32,
    key_type: &str,
) -> UnivMonTwoTierMetadata {
    UnivMonTwoTierMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        layers,
        elephant_layers: layout.elephant_layers,
        elephant_rows: layout.elephant_rows,
        elephant_cols: layout.elephant_cols,
        mouse_rows: layout.mouse_rows,
        mouse_cols: layout.mouse_cols,
        heap_size,
        key_type: key_type.to_string(),
    }
}

/// Per-layer `(rows, cols)`: elephant while `layer < elephant_layers`, mouse
/// after.
fn geometry_of(layers: usize, layout: &TwoTierLayout) -> Vec<(usize, usize)> {
    (0..layers)
        .map(|layer| {
            if layer < layout.elephant_layers as usize {
                (layout.elephant_rows as usize, layout.elephant_cols as usize)
            } else {
                (layout.mouse_rows as usize, layout.mouse_cols as usize)
            }
        })
        .collect()
}

/// Rejects a layout the algorithm never has: `layers`, `heap_size` and
/// both tiers' dimensions are each positive. The encoder and the decoder both
/// call this, so the two doors cannot drift.
fn check_layout_dimensions(
    layers: usize,
    heap_size: usize,
    elephant_rows: usize,
    elephant_cols: usize,
    mouse_rows: usize,
    mouse_cols: usize,
) -> Result<(), String> {
    if layers == 0
        || heap_size == 0
        || elephant_rows == 0
        || elephant_cols == 0
        || mouse_rows == 0
        || mouse_cols == 0
    {
        return Err(format!(
            "UnivMonPyramid layers, heap_size and both tiers' dimensions must be non-zero: layers={layers}, elephant_rows={elephant_rows}, elephant_cols={elephant_cols}, mouse_rows={mouse_rows}, mouse_cols={mouse_cols}, heap_size={heap_size}"
        ));
    }
    Ok(())
}

/// Total accumulators the declared layout implies, one per row per layer.
fn accumulator_count(layers: usize, layout: &TwoTierLayout) -> Option<usize> {
    let elephants = layers.min(layout.elephant_layers as usize);
    let mice = layers - elephants;
    elephants
        .checked_mul(layout.elephant_rows as usize)?
        .checked_add(mice.checked_mul(layout.mouse_rows as usize)?)
}

// Wire serialization for UnivMonTwoTier. `wire` is a descendant of the sketch
// module, so this impl reads the private fields directly.
impl UnivMonTwoTier {
    /// Serializes the pyramid into an ASAPv1 MessagePack envelope
    /// (kind_id `0x11 0x00`). The metadata is derived from
    /// [`DefaultXxHasher`]'s [`HashProfile`], the hasher the pyramid is built
    /// on.
    ///
    /// Fails when a structural parameter is zero, when a layer's geometry or
    /// seed index disagrees with the declared layout, when a layer's heap holds
    /// one wire key twice, when the heaps' keys mix `OwnedDataInput` variants or hold
    /// a 128-bit key, or when a structural parameter overflows its `u32`
    /// metadata field.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        check_layout_dimensions(
            self.layers,
            self.heap_size,
            self.elephant_rows,
            self.elephant_cols,
            self.mouse_rows,
            self.mouse_cols,
        )
        .map_err(RmpEncodeError::Syntax)?;
        check_layers("UnivMonPyramid", self.layers).map_err(RmpEncodeError::Syntax)?;
        let field = |name: &str, value: usize| {
            u32::try_from(value).map_err(|_| {
                RmpEncodeError::Syntax(format!(
                    "ASAPv1 UnivMonPyramid envelope: {name} {value} exceeds the u32 metadata field"
                ))
            })
        };
        let layout = TwoTierLayout {
            elephant_layers: field("elephant_layers", self.elephant_layers)?,
            elephant_rows: field("elephant_rows", self.elephant_rows)?,
            elephant_cols: field("elephant_cols", self.elephant_cols)?,
            mouse_rows: field("mouse_rows", self.mouse_rows)?,
            mouse_cols: field("mouse_cols", self.mouse_cols)?,
        };
        let geometry = geometry_of(self.layers, &layout);
        let state = pyramid_state(
            &self.layer_sketches,
            &self.layer_heaps,
            &geometry,
            self.total_weight,
            update_mode_tag(self.update_mode),
            &self.candidate_complete,
        )?;
        let key_type = pyramid_key_type(&state.entries)?;
        let metadata = rmp_serde::to_vec_named(&univmon_two_tier_metadata::<DefaultXxHasher>(
            field("layers", self.layers)?,
            &layout,
            field("heap_size", self.heap_size)?,
            key_type,
        ))?;
        let payload = encode_pyramid(key_type, state)?;
        Ok(envelope::encode(
            kind::UNIVMON_TWO_TIER,
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a pyramid from an ASAPv1 MessagePack envelope. The layout
    /// and `key_type` are structural (they are properties of the stored
    /// sketch), so they are echoed back into the expected metadata; the hash
    /// spec is pinned against [`DefaultXxHasher`].
    ///
    /// Every state the algorithm could not have produced is rejected with an
    /// error rather than a panic, and no declared count sizes an allocation
    /// before the payload is measured against it.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::UNIVMON_TWO_TIER {
            return Err(RmpDecodeError::Uncategorized(format!(
                "UnivMonPyramid kind_id mismatch: stored {kind_id:?}, expected {UNIVMON_TWO_TIER:?}"
            )));
        }
        let meta: UnivMonTwoTierMetadata = envelope::decode_block(metadata)?;
        let layout = TwoTierLayout {
            elephant_layers: meta.elephant_layers,
            elephant_rows: meta.elephant_rows,
            elephant_cols: meta.elephant_cols,
            mouse_rows: meta.mouse_rows,
            mouse_cols: meta.mouse_cols,
        };
        if meta
            != univmon_two_tier_metadata::<DefaultXxHasher>(
                meta.layers,
                &layout,
                meta.heap_size,
                &meta.key_type,
            )
        {
            return Err(RmpDecodeError::Uncategorized(
                "ASAPv1 UnivMonPyramid envelope: metadata mismatch".to_string(),
            ));
        }
        let (layers, heap_size) = (meta.layers as usize, meta.heap_size as usize);
        let (elephant_rows, elephant_cols, mouse_rows, mouse_cols) = (
            meta.elephant_rows as usize,
            meta.elephant_cols as usize,
            meta.mouse_rows as usize,
            meta.mouse_cols as usize,
        );
        check_layout_dimensions(
            layers,
            heap_size,
            elephant_rows,
            elephant_cols,
            mouse_rows,
            mouse_cols,
        )
        .map_err(RmpDecodeError::Uncategorized)?;
        check_layers("UnivMonPyramid", layers).map_err(RmpDecodeError::Uncategorized)?;
        let decoded = decode_pyramid(&meta.key_type, payload)?;
        check_empty_key_type(&meta.key_type, decoded.entries.is_empty())
            .map_err(RmpDecodeError::Uncategorized)?;
        // The declared layout is measured against the accumulators the payload
        // actually carries before the geometry is built from it.
        if accumulator_count(layers, &layout) != Some(decoded.l2.len()) {
            return Err(RmpDecodeError::Uncategorized(format!(
                "UnivMonPyramid declares a layout the payload's {} accumulators do not match",
                decoded.l2.len()
            )));
        }
        let geometry = geometry_of(layers, &layout);
        let (layer_sketches, layer_heaps, candidate_complete, bucket_size, mode_tag) =
            rebuild_layers(&geometry, heap_size, decoded)?;
        Ok(UnivMonTwoTier {
            layer_sketches,
            layer_heaps,
            layers,
            elephant_layers: meta.elephant_layers as usize,
            elephant_rows,
            elephant_cols,
            mouse_rows,
            mouse_cols,
            heap_size,
            total_weight: bucket_size,
            update_mode: update_mode_of(mode_tag)?,
            candidate_complete,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch_framework::univmon::UNIVMON_MAX_LAYERS;
    use crate::sketch_framework::univmon::wire::PyramidPayload;
    use crate::sketches::countsketch_heap::CountSketchWithL2Norm;
    use crate::{
        DEFAULT_CANONICAL_SEED_INDEX, DataInput, L2HH, MATRIX_MAX_ROWS, OwnedDataInput,
        RegularPath, SketchHasher, Vector2D,
    };
    use rmp_serde::from_slice;

    fn populated() -> UnivMonTwoTier {
        let mut pyramid = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        for (key, weight) in [("alpha", 5i64), ("beta", 7), ("gamma", 9), ("delta", 11)] {
            pyramid.update_weighted(&DataInput::Str(key), weight);
        }
        pyramid
    }

    fn metadata_of(bytes: &[u8]) -> UnivMonTwoTierMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    fn payload_of<K: for<'de> Deserialize<'de>>(bytes: &[u8]) -> PyramidPayload<K> {
        let (_, _, payload) = envelope::split(bytes).expect("split");
        from_slice(payload).expect("payload")
    }

    fn crafted<K: Serialize>(
        meta: &UnivMonTwoTierMetadata,
        payload: &PyramidPayload<K>,
    ) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(meta).expect("metadata");
        let payload = rmp_serde::to_vec(payload).expect("payload");
        envelope::encode(kind::UNIVMON_TWO_TIER, &metadata, &payload)
    }

    #[test]
    fn pyramid_round_trip_serialization() {
        let pyramid = populated();
        let encoded = pyramid.to_asapv1().expect("serialize");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x11, 0x00]); // kind_id_len=2, kind_id=[0x11,0x00]

        let meta = metadata_of(&encoded);
        assert_eq!(meta.metadata_version, 1);
        assert_eq!(
            (
                meta.layers,
                meta.elephant_layers,
                meta.elephant_rows,
                meta.elephant_cols,
                meta.mouse_rows,
                meta.mouse_cols,
                meta.heap_size,
            ),
            (4, 2, 3, 16, 2, 8, 4)
        );
        assert_eq!(meta.key_type, "string");

        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("deserialize");
        assert_eq!(decoded.total_weight, pyramid.total_weight);
        assert_eq!(decoded.estimate_l1(), pyramid.estimate_l1());
        assert_eq!(decoded.estimate_l2(), pyramid.estimate_l2());
        assert_eq!(
            decoded.estimate_cardinality(),
            pyramid.estimate_cardinality()
        );
        assert_eq!(decoded.estimate_entropy(), pyramid.estimate_entropy());
        assert_eq!(decoded.candidate_complete, pyramid.candidate_complete);
        assert_eq!(
            decoded.to_asapv1().expect("re-serialize"),
            encoded,
            "a decoded pyramid re-serialized to different bytes"
        );
    }

    /// The two tiers are derived from the layer's position: the elephant
    /// layers keep their dimensions and the mouse layers keep theirs.
    #[test]
    fn pyramid_two_tier_geometry_survives() {
        let pyramid = populated();
        let encoded = pyramid.to_asapv1().expect("serialize");
        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("decode");
        for layer in 0..pyramid.layers {
            let L2HH::COUNT(original) = &pyramid.layer_sketches[layer];
            let L2HH::COUNT(rebuilt) = &decoded.layer_sketches[layer];
            let expected = if layer < 2 { (3, 16) } else { (2, 8) };
            assert_eq!((rebuilt.rows(), rebuilt.cols()), expected);
            assert_eq!(rebuilt.seed_index(), layer);
            assert_eq!(
                original.as_storage().as_slice(),
                rebuilt.as_storage().as_slice()
            );
        }
    }

    /// Layers hold different numbers of entries: each layer's heap contents
    /// survive the round trip.
    #[test]
    fn pyramid_layers_with_different_heap_loads_round_trip() {
        let mut pyramid = UnivMonTwoTier::new(8, 2, 3, 32, 2, 16, 4);
        for key in 0..40u64 {
            pyramid.update_weighted(&DataInput::U64(key), 1 + (key as i64 % 5));
        }
        let loads: Vec<usize> = (0..pyramid.layers)
            .map(|i| pyramid.layer_heaps[i].len())
            .collect();
        assert!(
            loads.windows(2).any(|pair| pair[0] != pair[1]),
            "expected layers of different heap loads, got {loads:?}"
        );

        let encoded = pyramid.to_asapv1().expect("serialize");
        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("decode");
        for layer in 0..pyramid.layers {
            let (original, rebuilt) = (&pyramid.layer_heaps[layer], &decoded.layer_heaps[layer]);
            assert_eq!(original.len(), rebuilt.len(), "layer {layer} lost entries");
            for item in original.heap() {
                let found = rebuilt
                    .find_owned(&item.key)
                    .unwrap_or_else(|| panic!("layer {layer} lost {:?}", item.key));
                assert_eq!(rebuilt.heap()[found].count, item.count);
            }
        }
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// A pyramid with no mouse layers is the one-tier case and still
    /// round-trips.
    #[test]
    fn pyramid_without_mouse_layers_round_trips() {
        let mut pyramid = UnivMonTwoTier::new(4, 4, 2, 16, 2, 8, 3);
        pyramid.update_weighted(&DataInput::U64(7), 3);
        let encoded = pyramid.to_asapv1().expect("serialize");
        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("decode");
        for layer in 0..3 {
            let L2HH::COUNT(rebuilt) = &decoded.layer_sketches[layer];
            assert_eq!((rebuilt.rows(), rebuilt.cols()), (2, 16));
        }
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// An empty pyramid has exactly one encoding.
    /// An empty pyramid carries `key_type` `"u64"`; any other name is rejected.
    #[test]
    fn pyramid_empty_must_carry_the_u64_key_type() {
        use crate::sketches::countminsketch_heap::heap_wire::relabel_u64_key_type;
        let bytes = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4)
            .to_asapv1()
            .expect("serialize");
        let problem =
            UnivMonTwoTier::from_asapv1(&relabel_u64_key_type(&bytes, "key_type", "string"))
                .map(drop)
                .expect_err("an empty pyramid under another key_type must be rejected")
                .to_string();
        assert!(problem.contains("empty key set"), "{problem}");
    }

    #[test]
    fn pyramid_empty_has_one_encoding() {
        let left = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        let mut right = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        right.update_weighted(&DataInput::Str("alpha"), 5);
        right.clear();
        let encoded = left.to_asapv1().expect("serialize");
        assert_eq!(right.to_asapv1().expect("serialize"), encoded);
        assert_eq!(metadata_of(&encoded).key_type, "u64");

        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("decode");
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// `update_mode` and `candidate_complete` are state, not derived.
    #[test]
    fn pyramid_carries_update_mode_and_candidate_flags() {
        let mut terminal = UnivMonTwoTier::new(2, 1, 2, 8, 2, 8, 3);
        for key in 0..20u64 {
            terminal.update_weighted_terminal(&DataInput::U64(key), 3);
        }
        assert!(
            terminal.candidate_complete.iter().any(|&flag| !flag),
            "expected an evicting layer"
        );
        let encoded = terminal.to_asapv1().expect("serialize");
        assert_eq!(payload_of::<u64>(&encoded).update_mode, 2);

        let decoded = UnivMonTwoTier::from_asapv1(&encoded).expect("decode");
        assert_eq!(decoded.candidate_complete, terminal.candidate_complete);
        assert_eq!(
            decoded.estimate_cardinality(),
            terminal.estimate_cardinality()
        );
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    // A test-only custom hasher: hashes exactly like `DefaultXxHasher` but
    // declares a DIFFERENT `HashProfile`.
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
        fn hash64_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u64 {
            DefaultXxHasher::hash64_seeded_owned(seed_index, key)
        }
        fn hash128_seeded_owned(seed_index: usize, key: &OwnedDataInput) -> u128 {
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

    /// The pyramid hashes through the crate default, so it has one truthful
    /// profile: (a) it emits that profile, (b) a custom-profile envelope is
    /// different bytes, and (c) decode fails closed on it.
    #[test]
    fn pyramid_pins_its_hash_profile() {
        let encoded = populated().to_asapv1().expect("serialize");
        let meta = metadata_of(&encoded);
        assert_eq!(meta.hash_profile_id, DefaultXxHasher::PROFILE_ID);
        assert_eq!(meta.seed_list, DefaultXxHasher::seed_list());

        let layout = TwoTierLayout {
            elephant_layers: meta.elephant_layers,
            elephant_rows: meta.elephant_rows,
            elephant_cols: meta.elephant_cols,
            mouse_rows: meta.mouse_rows,
            mouse_cols: meta.mouse_cols,
        };
        let alt = univmon_two_tier_metadata::<AltHasher>(
            meta.layers,
            &layout,
            meta.heap_size,
            &meta.key_type,
        );
        let (_, _, payload) = envelope::split(&encoded).expect("split");
        let forged = envelope::encode(
            kind::UNIVMON_TWO_TIER,
            &rmp_serde::to_vec_named(&alt).expect("metadata"),
            payload,
        );
        assert_ne!(forged, encoded);
        assert!(
            UnivMonTwoTier::from_asapv1(&forged).is_err(),
            "a custom-profile envelope must be rejected"
        );
    }

    /// Each family's envelope is rejected by the other three, and by a plain
    /// Count Sketch envelope.
    #[test]
    fn pyramid_rejects_foreign_kind_ids() {
        let count_sketch = crate::CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8)
            .to_asapv1()
            .expect("serialize Count Sketch");
        let count_l2hh = CountSketchWithL2Norm::<DefaultXxHasher>::with_dimensions(2, 8)
            .to_asapv1()
            .expect("serialize CountL2HH");
        let univmon = crate::UnivMon::new(4, 2, 8, 2)
            .to_asapv1()
            .expect("serialize UnivMon");
        let univmon_q = crate::UnivMonQ::new(crate::UnivMonQConfig {
            layers: 2,
            top_layer_cols: 8,
            rows: 3,
            heap_size: 4,
            ordered_samples: 4,
            ..Default::default()
        })
        .expect("config")
        .to_asapv1()
        .expect("serialize UnivMonQ");

        for foreign in [count_sketch, count_l2hh, univmon, univmon_q] {
            assert!(
                UnivMonTwoTier::from_asapv1(&foreign).is_err(),
                "a foreign envelope must not decode as a UnivMonPyramid"
            );
        }
    }

    /// Fail closed (not panic) on crafted layouts, layer counts and heap
    /// capacities, including a `layers` far larger than the payload
    /// carries.
    #[test]
    fn pyramid_rejects_crafted_shapes() {
        let encoded = populated().to_asapv1().expect("serialize");
        let base = metadata_of(&encoded);
        let payload = payload_of::<String>(&encoded);

        let shaped = |layers, layout: TwoTierLayout, heap_size| {
            univmon_two_tier_metadata::<DefaultXxHasher>(layers, &layout, heap_size, &base.key_type)
        };
        let layout = || TwoTierLayout {
            elephant_layers: 2,
            elephant_rows: 3,
            elephant_cols: 16,
            mouse_rows: 2,
            mouse_cols: 8,
        };
        let cases = [
            shaped(u32::MAX, layout(), 4),
            shaped(0, layout(), 4),
            shaped(4, layout(), 0),
            shaped(
                4,
                TwoTierLayout {
                    elephant_cols: 0,
                    ..layout()
                },
                4,
            ),
            shaped(
                4,
                TwoTierLayout {
                    elephant_rows: 0,
                    ..layout()
                },
                4,
            ),
            shaped(
                4,
                TwoTierLayout {
                    mouse_rows: 0,
                    ..layout()
                },
                4,
            ),
            shaped(
                4,
                TwoTierLayout {
                    mouse_cols: 0,
                    ..layout()
                },
                4,
            ),
            shaped(
                4,
                TwoTierLayout {
                    mouse_rows: MATRIX_MAX_ROWS as u32,
                    mouse_cols: 4096,
                    ..layout()
                },
                4,
            ),
            shaped(4, layout(), 1),
        ];
        for meta in cases {
            assert!(
                UnivMonTwoTier::from_asapv1(&crafted(&meta, &payload)).is_err(),
                "a crafted layout must be rejected, not decoded"
            );
        }

        // A zero row count against an empty payload: the accumulator check
        // alone passes (0 rows over any layer count names 0 accumulators), so
        // the tiers' dimensions must be refused before the geometry is sized.
        let mut empty = payload_of::<String>(&encoded);
        empty.counts.clear();
        empty.l2.clear();
        empty.keys.clear();
        empty.heap_counts.clear();
        let zero_rows = TwoTierLayout {
            elephant_rows: 0,
            mouse_rows: 0,
            ..layout()
        };
        let problem =
            UnivMonTwoTier::from_asapv1(&crafted(&shaped(u32::MAX, zero_rows, 4), &empty))
                .expect_err("a zero tier row count must be rejected, not sized from")
                .to_string();
        assert!(problem.contains("must be non-zero"), "got {problem}");

        let mut short = payload_of::<String>(&encoded);
        short.heap_lens.pop();
        assert!(UnivMonTwoTier::from_asapv1(&crafted(&base, &short)).is_err());

        let mut mode = payload_of::<String>(&encoded);
        mode.update_mode = 9;
        assert!(UnivMonTwoTier::from_asapv1(&crafted(&base, &mode)).is_err());
    }

    /// The constructor asserts all four tier dimensions are positive, so a
    /// layout whose mouse layers never materialize must still declare theirs:
    /// a pyramid decoded with zero mouse dimensions panics on any merge
    /// against a normally built one.
    #[test]
    fn pyramid_rejects_zero_dimensions_of_a_tier_it_never_materializes() {
        let mut one_tier = UnivMonTwoTier::new(4, 4, 2, 16, 2, 8, 3);
        one_tier.update_weighted(&DataInput::U64(7), 3);
        let encoded = one_tier.to_asapv1().expect("serialize");
        let mut meta = metadata_of(&encoded);
        meta.mouse_rows = 0;
        meta.mouse_cols = 0;
        let payload = payload_of::<u64>(&encoded);
        let problem = UnivMonTwoTier::from_asapv1(&crafted(&meta, &payload))
            .expect_err("a zero mouse dimension must be rejected")
            .to_string();
        assert!(problem.contains("must be non-zero"), "got {problem}");
    }

    /// A layer's heap can seat one key twice — `update` compares residents with
    /// `OwnedDataInput`'s equality and `NaN != NaN`, so a second `NaN` takes its own
    /// seat — and `rebuild_heap` refuses that payload. The shared encode path
    /// runs the same `check_distinct_keys` per layer, so the pyramid never
    /// emits it.
    #[test]
    fn pyramid_refuses_to_serialize_a_layer_holding_a_key_twice() {
        let mut nans = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        nans.layer_heaps[2].update(&DataInput::F64(f64::NAN), 5);
        nans.layer_heaps[2].update(&DataInput::F64(f64::NAN), 4);
        assert_eq!(
            nans.layer_heaps[2].len(),
            2,
            "the two NaNs did not both seat"
        );
        let problem = nans
            .to_asapv1()
            .expect_err("a layer holding one key twice must not serialize")
            .to_string();
        assert!(
            problem.contains("the same key appears twice"),
            "got {problem}"
        );
        assert!(problem.contains("layer 2"), "got {problem}");
    }

    /// The layout fields are public, so a caller can zero one after
    /// construction (the constructor asserts them positive). The decoder
    /// refuses a zero dimension, so the encoder must too. Each case below is
    /// built by writing the public field directly.
    #[test]
    fn pyramid_refuses_to_serialize_a_zero_dimension() {
        for zero in [
            "layers",
            "heap_size",
            "elephant_rows",
            "elephant_cols",
            "mouse_rows",
            "mouse_cols",
        ] {
            let mut pyramid = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
            match zero {
                "layers" => pyramid.layers = 0,
                "heap_size" => pyramid.heap_size = 0,
                "elephant_rows" => pyramid.elephant_rows = 0,
                "elephant_cols" => pyramid.elephant_cols = 0,
                "mouse_rows" => pyramid.mouse_rows = 0,
                _ => pyramid.mouse_cols = 0,
            }
            let problem = match pyramid.to_asapv1() {
                Ok(_) => panic!("a zero {zero} must not serialize"),
                Err(err) => err.to_string(),
            };
            assert!(problem.contains("must be non-zero"), "got {problem}");
        }
    }

    /// The layer finder shifts a 64-bit hash by up to `layers - 1`, so
    /// [`UNIVMON_MAX_LAYERS`] layers is the ceiling: the boundary round-trips and a
    /// deeper pyramid is refused at the decode door.
    #[test]
    fn pyramid_rejects_layers_past_the_shift_bound() {
        let deepest = UnivMonTwoTier::new(4, 2, 2, 16, 2, 8, UNIVMON_MAX_LAYERS);
        let encoded = deepest.to_asapv1().expect("serialize");
        assert!(UnivMonTwoTier::from_asapv1(&encoded).is_ok());

        let layers = UNIVMON_MAX_LAYERS + 1;
        let mut meta = metadata_of(&encoded);
        meta.layers = layers as u32;
        let mut payload: PyramidPayload<u64> = payload_of(&encoded);
        payload.counts = vec![0; 2 * (2 * 16) + (layers - 2) * (2 * 8)];
        payload.l2 = vec![0; 2 * layers];
        payload.heap_lens = vec![0; layers];
        payload.candidate_complete = vec![true; layers];
        let problem = UnivMonTwoTier::from_asapv1(&crafted(&meta, &payload))
            .expect_err("a pyramid past the shift bound must be rejected")
            .to_string();
        assert!(problem.contains("MAX_LAYER_SIZE"), "got {problem}");
    }

    #[test]
    #[should_panic(expected = "at most MAX_LAYER_SIZE")]
    fn pyramid_refuses_constructing_past_the_shift_bound() {
        UnivMonTwoTier::new(4, 2, 2, 16, 2, 8, UNIVMON_MAX_LAYERS + 1);
    }

    /// A pyramid whose layers disagree with their declared tier or seed index
    /// must not serialize.
    #[test]
    fn pyramid_rejects_serializing_an_inconsistent_layout() {
        let mut wrong_tier = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        wrong_tier.layer_sketches[3] = L2HH::COUNT(
            CountSketchWithL2Norm::with_dimensions_and_seed_index(3, 16, 3),
        );
        assert!(
            wrong_tier.to_asapv1().is_err(),
            "a mouse layer holding elephant dimensions must not serialize"
        );

        let mut wrong_seed = UnivMonTwoTier::new(4, 2, 3, 16, 2, 8, 4);
        wrong_seed.layer_sketches[3] = L2HH::COUNT(
            CountSketchWithL2Norm::with_dimensions_and_seed_index(2, 8, 0),
        );
        assert!(
            wrong_seed.to_asapv1().is_err(),
            "a layer hashing at another layer's seed index must not serialize"
        );
    }
    /// Fail closed on an unexpected metadata key, and on a missing required
    /// one.
    #[test]
    fn pyramid_metadata_rejects_unknown_and_missing_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            layers: u32,
            elephant_layers: u32,
            elephant_rows: u32,
            elephant_cols: u32,
            mouse_rows: u32,
            mouse_cols: u32,
            heap_size: u32,
            key_type: String,
            bogus_field: u8, // key not in UnivMonTwoTierMetadata
        }
        #[derive(Serialize)]
        struct WithoutMouseCol {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            layers: u32,
            elephant_layers: u32,
            elephant_rows: u32,
            elephant_cols: u32,
            mouse_rows: u32,
            heap_size: u32,
            key_type: String,
        }
        let layout = TwoTierLayout {
            elephant_layers: 2,
            elephant_rows: 3,
            elephant_cols: 16,
            mouse_rows: 2,
            mouse_cols: 8,
        };
        let m = univmon_two_tier_metadata::<DefaultXxHasher>(4, &layout, 4, "u64");
        let extra = WithExtra {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            layers: m.layers,
            elephant_layers: m.elephant_layers,
            elephant_rows: m.elephant_rows,
            elephant_cols: m.elephant_cols,
            mouse_rows: m.mouse_rows,
            mouse_cols: m.mouse_cols,
            heap_size: m.heap_size,
            key_type: m.key_type.clone(),
            bogus_field: 7,
        };
        let without = WithoutMouseCol {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            layers: m.layers,
            elephant_layers: m.elephant_layers,
            elephant_rows: m.elephant_rows,
            elephant_cols: m.elephant_cols,
            mouse_rows: m.mouse_rows,
            heap_size: m.heap_size,
            key_type: m.key_type.clone(),
        };
        assert!(
            from_slice::<UnivMonTwoTierMetadata>(&rmp_serde::to_vec_named(&extra).unwrap())
                .is_err(),
            "an unknown metadata key must be rejected"
        );
        assert!(
            from_slice::<UnivMonTwoTierMetadata>(&rmp_serde::to_vec_named(&without).unwrap())
                .is_err(),
            "a missing required key must be rejected"
        );
    }
}
