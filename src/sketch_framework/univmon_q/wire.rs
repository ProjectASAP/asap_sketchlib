//! ASAPv1 wire serialization for [`UnivMonQ`].
//!
//! Child submodule of [`crate::sketch_framework::univmon_q`]: it holds the
//! metadata/payload DTOs, the kind_id constant and the `to_asapv1` /
//! `from_asapv1` impls, while the algorithm lives in the parent
//! module file. Being a descendant module, it reads the private `layers`,
//! `count`, `min`, `max`, `source_id`, `next_sequence` and `ordered_heap`
//! fields directly without widening any field visibility. See
//! `docs/asapv1_wire_format.md`.
//!
//! UnivMon-Q is one algorithm — a single kind_id `0x1a 0x00`. The whole
//! [`UnivMonQConfig`](super::UnivMonQConfig) is construction config and lives
//! in the metadata, so every per-level width, the hash layout and each level's
//! candidate capacity are derived and none is stored.
//!
//! ## Rebuilt, not carried
//!
//! A level's candidate min-heap and the ordered sample's `BinaryHeap` are
//! array orders that do not survive a rebuild, so the payload is
//! **order-defined** — candidates ascending by key, occurrences ascending by
//! `(priority_high, priority_low, key)` — and both heaps are rebuilt on
//! decode. No heap index reaches the wire.
//!
//! ## Sequence state
//!
//! `source_id` and `next_sequence` are the identity the coordinated bottom-k
//! sample draws its priorities from, so a decoded sketch continues the same
//! draw sequence instead of re-drawing identities it already used.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::marker::PhantomData;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::envelope;
use crate::message_pack_format::kind;
use crate::message_pack_format::kind::UNIVMON_Q;
use crate::{HashProfile, SketchHasher};

use super::{
    Counters, HashLayout, Layer, OrderedOccurrence, PackedCountSketch, UnivMonQ, UnivMonQConfig,
    decode_error, layer_cols, validate_config,
};

/// UnivMon-Q descriptor metadata (ASAPv1 §2), a msgpack **map**
/// (`to_vec_named`) with keys in this declaration order — the canonical order
/// the wire spec fixes (Go must mirror it). Hash-spec fields first, then the
/// configuration that shapes the payload.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UnivMonQMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) seed_list: Vec<u64>,
    pub(crate) seed_index: u32,
    pub(crate) layers: u32,
    pub(crate) top_layer_cols: u32,
    pub(crate) cols_halving_period: u8,
    pub(crate) rows: u32,
    pub(crate) counter_type: String,
    pub(crate) heap_size: u32,
    pub(crate) ordered_samples: u32,
}

/// Builds the UnivMon-Q descriptor metadata from the hasher's [`HashProfile`],
/// so the wire bytes truthfully describe how the sketch was hashed.
/// `seed_index` is the config's own `seed_index`, a construction parameter
/// rather than a profile constant, so it is carried as a structural param.
pub(crate) fn univmon_q_metadata<H: HashProfile>(
    config: UnivMonQConfig,
    counter_type: &str,
) -> Result<UnivMonQMetadata, String> {
    let field = |name: &str, value: usize| {
        u32::try_from(value)
            .map_err(|_| format!("UnivMon-Q {name} {value} exceeds the u32 metadata field"))
    };
    Ok(UnivMonQMetadata {
        metadata_version: 1,
        hash_profile_id: H::PROFILE_ID.to_string(),
        hash_algorithm: H::ALGORITHM.to_string(),
        seed_derivation: H::SEED_DERIVATION.to_string(),
        input_encoding: H::INPUT_ENCODING.to_string(),
        seed_list: H::seed_list(),
        seed_index: field("hash_seed", config.seed_index)?,
        layers: field("layers", config.layers)?,
        top_layer_cols: field("top_layer_cols", config.top_layer_cols)?,
        cols_halving_period: config.cols_halving_period,
        rows: field("rows", config.rows)?,
        counter_type: counter_type.to_string(),
        heap_size: field("heap_size", config.heap_size)?,
        ordered_samples: field("ordered_samples", config.ordered_samples)?,
    })
}

/// The config a validated metadata block describes.
fn config_of(meta: &UnivMonQMetadata) -> Result<UnivMonQConfig, RmpDecodeError> {
    Ok(UnivMonQConfig {
        layers: meta.layers as usize,
        top_layer_cols: meta.top_layer_cols as usize,
        cols_halving_period: meta.cols_halving_period,
        rows: meta.rows as usize,
        counter_bits: counter_bits_of(&meta.counter_type)?,
        heap_size: meta.heap_size as usize,
        ordered_samples: meta.ordered_samples as usize,
        seed_index: meta.seed_index as usize,
    })
}

/// Metadata `counter_type` of a counter width: 32-bit counters are `"i32"`,
/// 64-bit are `"i64"`. Counters are signed, so Count-Min's `"f64"` has no
/// counterpart.
fn counter_type_of(counter_bits: u8) -> Result<&'static str, String> {
    match counter_bits {
        32 => Ok("i32"),
        64 => Ok("i64"),
        other => Err(format!(
            "UnivMon-Q counter_bits {other} is not a wire counter width"
        )),
    }
}

/// Reads the metadata `counter_type` back into a counter width.
fn counter_bits_of(counter_type: &str) -> Result<u8, RmpDecodeError> {
    match counter_type {
        "i32" => Ok(32),
        "i64" => Ok(64),
        other => Err(decode_error(format!(
            "UnivMon-Q counter_type {other:?} is not a wire counter type"
        ))),
    }
}

/// UnivMon-Q payload (ASAPv1 §3.x), a msgpack **array** (`to_vec`,
/// positional). `counters` concatenates the levels' CountSketch rows; the
/// candidate and occurrence arrays are parallel runs cut by `candidate_lens`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct UnivMonQPayload<C> {
    pub(crate) counters: Vec<C>,
    pub(crate) candidate_lens: Vec<u32>,
    pub(crate) candidate_keys: Vec<u64>,
    pub(crate) candidate_scores: Vec<u64>,
    pub(crate) ever_evicted: Vec<bool>,
    pub(crate) count: u64,
    pub(crate) min: Option<u64>,
    pub(crate) max: Option<u64>,
    pub(crate) source_id: u64,
    pub(crate) next_sequence: u64,
    pub(crate) occurrence_priority_high: Vec<u64>,
    pub(crate) occurrence_priority_low: Vec<u64>,
    pub(crate) occurrence_keys: Vec<u64>,
}

/// Everything the payload carries besides the counters, which are read at the
/// width `counter_type` names.
struct SharedPayload {
    candidate_lens: Vec<u32>,
    candidate_keys: Vec<u64>,
    candidate_scores: Vec<u64>,
    ever_evicted: Vec<bool>,
    count: u64,
    min: Option<u64>,
    max: Option<u64>,
    source_id: u64,
    next_sequence: u64,
    occurrences: Vec<OrderedOccurrence>,
}

/// The occurrences in emitted order: ascending
/// `(priority_high, priority_low, key)`.
fn emitted_occurrences(heap: &BinaryHeap<OrderedOccurrence>) -> Vec<OrderedOccurrence> {
    let mut occurrences = heap.clone().into_vec();
    occurrences.sort_unstable();
    occurrences
}

/// The per-level CountSketch widths the config implies.
fn level_widths(config: UnivMonQConfig) -> Vec<usize> {
    (0..config.layers)
        .map(|level| layer_cols(config, level))
        .collect()
}

/// Counters a flat block holds.
fn counters_len(counters: &Counters) -> usize {
    match counters {
        Counters::I32(values) => values.len(),
        Counters::I64(values) => values.len(),
    }
}

/// One level's run of the flat counter block, at the block's own width.
fn counters_slice(counters: &Counters, start: usize, len: usize) -> Counters {
    match counters {
        Counters::I32(values) => Counters::I32(values[start..start + len].to_vec()),
        Counters::I64(values) => Counters::I64(values[start..start + len].to_vec()),
    }
}

/// Reads the payload with `counters` at the width `counter_type` names, and
/// zips the three occurrence arrays back into records.
fn read_payload(
    counter_type: &str,
    payload: &[u8],
) -> Result<(Counters, SharedPayload), RmpDecodeError> {
    macro_rules! unpack {
        ($variant:ident, $ty:ty) => {{
            let decoded: UnivMonQPayload<$ty> = envelope::decode_block(payload)?;
            (Counters::$variant(decoded.counters), {
                if decoded.occurrence_priority_high.len() != decoded.occurrence_priority_low.len()
                    || decoded.occurrence_priority_high.len() != decoded.occurrence_keys.len()
                {
                    return Err(decode_error(
                        "UnivMon-Q ordered occurrence arrays are not parallel".to_string(),
                    ));
                }
                let occurrences = decoded
                    .occurrence_priority_high
                    .into_iter()
                    .zip(decoded.occurrence_priority_low)
                    .zip(decoded.occurrence_keys)
                    .map(|((priority_high, priority_low), key)| OrderedOccurrence {
                        priority_high,
                        priority_low,
                        key,
                    })
                    .collect();
                SharedPayload {
                    candidate_lens: decoded.candidate_lens,
                    candidate_keys: decoded.candidate_keys,
                    candidate_scores: decoded.candidate_scores,
                    ever_evicted: decoded.ever_evicted,
                    count: decoded.count,
                    min: decoded.min,
                    max: decoded.max,
                    source_id: decoded.source_id,
                    next_sequence: decoded.next_sequence,
                    occurrences,
                }
            })
        }};
    }

    Ok(match counter_type {
        "i32" => unpack!(I32, i32),
        "i64" => unpack!(I64, i64),
        other => {
            return Err(decode_error(format!(
                "UnivMon-Q counter_type {other:?} is not a wire counter type"
            )));
        }
    })
}

/// Checks everything the counters do not cover: the per-level runs, the
/// capacities, the extrema and the ordered sample.
fn validate_shared(config: UnivMonQConfig, shared: &SharedPayload) -> Result<(), RmpDecodeError> {
    if shared.candidate_lens.len() != config.layers || shared.ever_evicted.len() != config.layers {
        return Err(decode_error(format!(
            "UnivMon-Q carries {} candidate runs and {} eviction flags over {} levels",
            shared.candidate_lens.len(),
            shared.ever_evicted.len(),
            config.layers
        )));
    }
    let mut seated = 0usize;
    for (index, &len) in shared.candidate_lens.iter().enumerate() {
        if len as usize > config.heap_size {
            return Err(decode_error(format!(
                "UnivMon-Q candidate capacity exceeded at level {index}"
            )));
        }
        seated = seated
            .checked_add(len as usize)
            .ok_or_else(|| decode_error("UnivMon-Q candidate count overflows".to_string()))?;
    }
    if shared.candidate_keys.len() != seated || shared.candidate_scores.len() != seated {
        return Err(decode_error(format!(
            "UnivMon-Q carries {} candidate keys and {} scores against the declared {seated}",
            shared.candidate_keys.len(),
            shared.candidate_scores.len()
        )));
    }
    if shared.occurrences.len() > config.ordered_samples {
        return Err(decode_error(
            "UnivMon-Q ordered sample capacity exceeded".to_string(),
        ));
    }
    if config.ordered_samples == 0 && !shared.occurrences.is_empty() {
        return Err(decode_error(
            "UnivMon-Q has ordered state while ordered sampling is disabled".to_string(),
        ));
    }
    if shared.occurrences.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(decode_error(
            "UnivMon-Q ordered occurrences are not strictly ascending".to_string(),
        ));
    }
    let valid_extrema = if shared.count == 0 {
        shared.min.is_none() && shared.max.is_none()
    } else {
        shared.min.is_some() && shared.max.is_some()
    };
    if !valid_extrema {
        return Err(decode_error(
            "UnivMon-Q count/min/max state is inconsistent".to_string(),
        ));
    }
    if shared
        .min
        .zip(shared.max)
        .is_some_and(|(min, max)| min > max)
    {
        return Err(decode_error(
            "UnivMon-Q minimum exceeds maximum".to_string(),
        ));
    }
    Ok(())
}

// Wire serialization for UnivMon-Q. `wire` is a descendant of the sketch
// module, so this impl reads the private fields directly.
impl<H: SketchHasher + HashProfile> UnivMonQ<H> {
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x1a 0x00`). The metadata is derived from the hasher's
    /// [`HashProfile`], so it truthfully describes how the sketch was hashed.
    ///
    /// Fails on any state the decoder would refuse: a level whose CountSketch
    /// does not match the config's layout, a candidate table over capacity, an
    /// ordered sample over capacity, or a `count`/`min`/`max` triple that
    /// cannot have arisen.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let syntax = |problem: String| RmpEncodeError::Syntax(problem);
        let counter_type = counter_type_of(self.config.counter_bits).map_err(syntax)?;
        let metadata = rmp_serde::to_vec_named(
            &univmon_q_metadata::<H>(self.config, counter_type).map_err(syntax)?,
        )?;
        if self.layers.len() != self.config.layers {
            return Err(syntax(format!(
                "ASAPv1 UnivMon-Q envelope: {} levels against a config of {}",
                self.layers.len(),
                self.config.layers
            )));
        }
        let valid_extrema = if self.count == 0 {
            self.min.is_none() && self.max.is_none()
        } else {
            self.min.is_some() && self.max.is_some()
        };
        if !valid_extrema || self.min.zip(self.max).is_some_and(|(min, max)| min > max) {
            return Err(syntax(
                "ASAPv1 UnivMon-Q envelope: count/min/max state is inconsistent".to_string(),
            ));
        }
        let occurrences = emitted_occurrences(&self.ordered_heap);
        if occurrences.len() > self.config.ordered_samples {
            return Err(syntax(format!(
                "ASAPv1 UnivMon-Q envelope: {} ordered samples over a capacity of {}",
                occurrences.len(),
                self.config.ordered_samples
            )));
        }
        if occurrences.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(syntax(
                "ASAPv1 UnivMon-Q envelope: the same ordered occurrence appears twice".to_string(),
            ));
        }
        let mut candidate_lens = Vec::with_capacity(self.layers.len());
        let mut candidate_keys = Vec::new();
        let mut candidate_scores = Vec::new();
        let mut ever_evicted = Vec::with_capacity(self.layers.len());
        for (index, level) in self.layers.iter().enumerate() {
            if !level.sketch.matches(
                layer_cols(self.config, index),
                self.config.rows,
                self.config.counter_bits,
            ) {
                return Err(syntax(format!(
                    "ASAPv1 UnivMon-Q envelope: CountSketch layout mismatch at level {index}"
                )));
            }
            if level.candidate_scores.len() > self.config.heap_size {
                return Err(syntax(format!(
                    "ASAPv1 UnivMon-Q envelope: candidate capacity exceeded at level {index}"
                )));
            }
            if level
                .candidate_scores
                .keys()
                .any(|key| self.sample_level(*key) != index)
            {
                return Err(syntax(format!(
                    "ASAPv1 UnivMon-Q envelope: a candidate sits in the wrong terminal level {index}"
                )));
            }
            let mut candidates: Vec<(u64, u64)> = level
                .candidate_scores
                .iter()
                .map(|(&key, &score)| (key, score))
                .collect();
            candidates.sort_unstable();
            candidate_lens.push(u32::try_from(candidates.len()).map_err(|_| {
                syntax(format!(
                    "ASAPv1 UnivMon-Q envelope: level {index} holds more candidates than u32 can name"
                ))
            })?);
            candidate_keys.extend(candidates.iter().map(|entry| entry.0));
            candidate_scores.extend(candidates.iter().map(|entry| entry.1));
            ever_evicted.push(level.ever_evicted);
        }

        macro_rules! pack {
            ($variant:ident) => {{
                let mut counters = Vec::new();
                for level in &self.layers {
                    match &level.sketch.counters {
                        Counters::$variant(values) => counters.extend_from_slice(values),
                        _ => {
                            return Err(syntax(
                                "ASAPv1 UnivMon-Q envelope: levels mix counter widths".to_string(),
                            ));
                        }
                    }
                }
                rmp_serde::to_vec(&UnivMonQPayload {
                    counters,
                    candidate_lens,
                    candidate_keys,
                    candidate_scores,
                    ever_evicted,
                    count: self.count,
                    min: self.min,
                    max: self.max,
                    source_id: self.source_id,
                    next_sequence: self.next_sequence,
                    occurrence_priority_high: occurrences
                        .iter()
                        .map(|entry| entry.priority_high)
                        .collect(),
                    occurrence_priority_low: occurrences
                        .iter()
                        .map(|entry| entry.priority_low)
                        .collect(),
                    occurrence_keys: occurrences.iter().map(|entry| entry.key).collect(),
                })?
            }};
        }

        let payload = match counter_type {
            "i32" => pack!(I32),
            _ => pack!(I64),
        };
        Ok(envelope::encode(kind::UNIVMON_Q, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope. The whole
    /// config is structural (it is a property of the stored sketch), so it is
    /// echoed back into the expected metadata; the hash spec is pinned against
    /// this target.
    ///
    /// Every state the algorithm could not have produced is rejected with an
    /// error rather than a panic, and no declared capacity — `candidates`,
    /// `ordered_samples` or a level width — sizes an allocation before the
    /// payload is measured against it.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::UNIVMON_Q {
            return Err(decode_error(format!(
                "UnivMon-Q kind_id mismatch: stored {kind_id:?}, expected {UNIVMON_Q:?}"
            )));
        }
        let meta: UnivMonQMetadata = envelope::decode_block(metadata)?;
        let config = config_of(&meta)?;
        let expected = univmon_q_metadata::<H>(config, &meta.counter_type).map_err(decode_error)?;
        if meta != expected {
            return Err(decode_error(
                "ASAPv1 UnivMon-Q envelope: metadata mismatch".to_string(),
            ));
        }
        validate_config(config).map_err(|error| decode_error(error.to_string()))?;
        let hash_layout = HashLayout::new(config).map_err(|e| decode_error(e.to_string()))?;

        let (counters, shared) = read_payload(&meta.counter_type, payload)?;
        let widths = level_widths(config);
        let cells = widths
            .iter()
            .try_fold(0usize, |total, width| {
                width
                    .checked_mul(config.rows)
                    .and_then(|cells| total.checked_add(cells))
            })
            .ok_or_else(|| decode_error("UnivMon-Q level layout overflows".to_string()))?;
        if counters_len(&counters) != cells {
            return Err(decode_error(format!(
                "UnivMon-Q carries {} counters against the config's {cells}",
                counters_len(&counters)
            )));
        }
        validate_shared(config, &shared)?;

        let mut levels = Vec::with_capacity(widths.len());
        let mut cell = 0usize;
        let mut candidate = 0usize;
        for (index, &width) in widths.iter().enumerate() {
            let run = shared.candidate_lens[index] as usize;
            let mut candidate_scores = HashMap::with_capacity(run);
            let mut candidate_heap = BinaryHeap::with_capacity(run);
            if shared.candidate_keys[candidate..candidate + run]
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return Err(decode_error(format!(
                    "UnivMon-Q candidate keys at layer {index} are not strictly ascending"
                )));
            }
            for offset in candidate..candidate + run {
                let key = shared.candidate_keys[offset];
                let score = shared.candidate_scores[offset];
                candidate_scores.insert(key, score);
                candidate_heap.push(Reverse((score, key)));
            }
            candidate += run;
            let span = width * config.rows;
            levels.push(Layer {
                sketch: PackedCountSketch {
                    width,
                    depth: config.rows,
                    counters: counters_slice(&counters, cell, span),
                },
                candidate_scores,
                candidate_heap,
                candidate_capacity: config.heap_size,
                ever_evicted: shared.ever_evicted[index],
            });
            cell += span;
        }

        let sketch = UnivMonQ {
            config,
            hash_layout,
            layers: levels,
            count: shared.count,
            min: shared.min,
            max: shared.max,
            source_id: shared.source_id,
            next_sequence: shared.next_sequence,
            ordered_heap: BinaryHeap::from(shared.occurrences),
            hasher: PhantomData,
        };
        // A candidate only reaches a level its own key hashes into.
        for (index, level) in sketch.layers.iter().enumerate() {
            if level
                .candidate_scores
                .keys()
                .any(|key| sketch.sample_level(*key) != index)
            {
                return Err(decode_error(format!(
                    "UnivMon-Q candidate stored in the wrong terminal level {index}"
                )));
            }
        }
        Ok(sketch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, OwnedDataInput, RegularPath,
        Vector2D,
    };
    use rmp_serde::from_slice;

    fn tiny_config() -> UnivMonQConfig {
        UnivMonQConfig {
            layers: 4,
            top_layer_cols: 64,
            cols_halving_period: 0,
            rows: 3,
            counter_bits: 64,
            heap_size: 16,
            ordered_samples: 16,
            seed_index: 5,
        }
    }

    fn populated() -> UnivMonQ<DefaultXxHasher> {
        let mut sketch = UnivMonQ::with_source_id(tiny_config(), 7).expect("config");
        for value in (0..200).map(|value| (value % 23) as f64) {
            sketch.update(&value);
        }
        sketch
    }

    fn metadata_of(bytes: &[u8]) -> UnivMonQMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    fn payload_of(bytes: &[u8]) -> UnivMonQPayload<i64> {
        let (_, _, payload) = envelope::split(bytes).expect("split");
        from_slice(payload).expect("payload")
    }

    fn crafted(meta: &UnivMonQMetadata, payload: &UnivMonQPayload<i64>) -> Vec<u8> {
        let metadata = rmp_serde::to_vec_named(meta).expect("metadata");
        let payload = rmp_serde::to_vec(payload).expect("payload");
        envelope::encode(kind::UNIVMON_Q, &metadata, &payload)
    }

    /// A config that differs from [`tiny_config`] in the named fields.
    fn config_with(mutate: impl FnOnce(&mut UnivMonQConfig)) -> UnivMonQConfig {
        let mut config = tiny_config();
        mutate(&mut config);
        config
    }

    #[test]
    fn univmon_q_round_trip_serialization() {
        let sketch = populated();
        let encoded = sketch.to_asapv1().expect("serialize UnivMon-Q");
        assert!(encoded.starts_with(b"ASAPv1"));
        assert_eq!(&encoded[7..10], &[2u8, 0x1a, 0x00]); // kind_id_len=2, kind_id=[0x1a,0x00]

        let meta = metadata_of(&encoded);
        assert_eq!(meta.metadata_version, 1);
        assert_eq!((meta.layers, meta.top_layer_cols, meta.rows), (4, 64, 3));
        assert_eq!(meta.counter_type, "i64");
        assert_eq!(meta.seed_index, 5);

        let decoded =
            UnivMonQ::<DefaultXxHasher>::from_asapv1(&encoded).expect("deserialize UnivMon-Q");
        assert_eq!(decoded.config(), sketch.config());
        assert_eq!(decoded.source_id(), sketch.source_id());
        assert_eq!(decoded.count(), sketch.count());
        assert_eq!(decoded.min(), sketch.min());
        assert_eq!(decoded.max(), sketch.max());
        assert_eq!(decoded.cdf(), sketch.cdf());
        assert_eq!(decoded.estimate_f2(), sketch.estimate_f2());
        assert_eq!(
            decoded.to_asapv1().expect("re-serialize"),
            encoded,
            "a decoded sketch re-serialized to different bytes"
        );
    }

    /// The ordering state survives: a decoded sketch fed the same subsequent
    /// updates agrees with the original fed those updates.
    #[test]
    fn univmon_q_ordering_state_continues_after_a_round_trip() {
        let mut original = UnivMonQ::with_source_id(tiny_config(), 991).expect("config");
        for value in (0..300).map(|value| (value % 37) as f64) {
            original.update(&value);
        }
        let encoded = original.to_asapv1().expect("serialize");
        let mut resumed = UnivMonQ::<DefaultXxHasher>::from_asapv1(&encoded).expect("decode");

        for value in (300..700).map(|value| (value % 53) as f64) {
            original.update(&value);
            resumed.update(&value);
        }
        assert_eq!(resumed.next_sequence, original.next_sequence);
        assert_eq!(
            resumed.ordered_heap.clone().into_sorted_vec(),
            original.ordered_heap.clone().into_sorted_vec(),
            "the resumed sketch drew different occurrence priorities"
        );
        assert_eq!(resumed.cdf(), original.cdf());
        assert_eq!(
            resumed.to_asapv1().expect("serialize"),
            original.to_asapv1().expect("serialize")
        );
    }

    /// An empty sketch has exactly one encoding, and `min` / `max` travel as
    /// msgpack nil. A cleared sketch keeps its occurrence sequence, so it is
    /// deliberately not the same bytes as a fresh one.
    #[test]
    fn univmon_q_empty_has_one_encoding() {
        let left = UnivMonQ::<DefaultXxHasher>::with_source_id(tiny_config(), 3).expect("left");
        let right = UnivMonQ::<DefaultXxHasher>::with_source_id(tiny_config(), 3).expect("right");
        let mut cleared =
            UnivMonQ::<DefaultXxHasher>::with_source_id(tiny_config(), 3).expect("cleared");
        cleared.update(&5.0);
        cleared.clear();

        let encoded = left.to_asapv1().expect("serialize");
        assert_eq!(right.to_asapv1().expect("serialize"), encoded);
        assert_ne!(cleared.to_asapv1().expect("serialize"), encoded);
        let payload = payload_of(&encoded);
        assert_eq!(payload.count, 0);
        assert!(payload.min.is_none() && payload.max.is_none());

        let decoded = UnivMonQ::<DefaultXxHasher>::from_asapv1(&encoded).expect("decode");
        assert!(decoded.is_empty());
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    /// The counter width is pinned by `counter_type`: an i32 sketch and an i64
    /// one holding the same counters do not share bytes.
    #[test]
    fn univmon_q_counter_type_is_pinned() {
        let narrow_config = config_with(|config| config.counter_bits = 32);
        let mut narrow =
            UnivMonQ::<DefaultXxHasher>::with_source_id(narrow_config, 7).expect("narrow config");
        let mut wide =
            UnivMonQ::<DefaultXxHasher>::with_source_id(tiny_config(), 7).expect("wide config");
        for value in (0..50).map(|value| value as f64) {
            narrow.update(&value);
            wide.update(&value);
        }

        let narrow_bytes = narrow.to_asapv1().expect("serialize i32");
        assert_eq!(metadata_of(&narrow_bytes).counter_type, "i32");
        let decoded = UnivMonQ::<DefaultXxHasher>::from_asapv1(&narrow_bytes).expect("decode i32");
        assert_eq!(decoded.estimate_f2(), narrow.estimate_f2());
        assert_ne!(narrow_bytes, wide.to_asapv1().expect("serialize"));

        let mut relabelled = metadata_of(&narrow_bytes);
        relabelled.counter_type = "f64".to_string();
        let (_, _, payload) = envelope::split(&narrow_bytes).expect("split");
        let forged = envelope::encode(
            kind::UNIVMON_Q,
            &rmp_serde::to_vec_named(&relabelled).expect("metadata"),
            payload,
        );
        assert!(
            UnivMonQ::<DefaultXxHasher>::from_asapv1(&forged).is_err(),
            "f64 is not a UnivMon-Q wire counter type"
        );
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

    #[test]
    fn univmon_q_custom_hasher_profile_round_trips_and_is_self_describing() {
        // (a) A sketch built with a custom-profile hasher round-trips.
        let mut alt =
            UnivMonQ::<AltHasher>::with_hasher_and_source_id(tiny_config(), 7).expect("alt config");
        let mut std =
            UnivMonQ::<DefaultXxHasher>::with_source_id(tiny_config(), 7).expect("std config");
        for value in (0..100).map(|value| (value % 13) as f64) {
            alt.update(&value);
            std.update(&value);
        }

        let alt_bytes = alt.to_asapv1().expect("alt serialize");
        let decoded = UnivMonQ::<AltHasher>::from_asapv1(&alt_bytes).expect("alt decode");
        assert_eq!(decoded.estimate_f2(), alt.estimate_f2());

        // (b) Bytes differ from the standard-profile sketch.
        let std_bytes = std.to_asapv1().expect("std serialize");
        assert_ne!(alt_bytes, std_bytes);

        // (c) Standard-profile decode fails closed on custom-profile bytes.
        assert!(
            UnivMonQ::<DefaultXxHasher>::from_asapv1(&alt_bytes).is_err(),
            "standard-profile decode must reject custom-profile bytes"
        );
    }

    /// Each family's envelope is rejected by the other three, and by a plain
    /// Count Sketch envelope.
    #[test]
    fn univmon_q_rejects_foreign_kind_ids() {
        let count_sketch = crate::CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8)
            .to_asapv1()
            .expect("serialize Count Sketch");
        let count_l2hh =
            crate::sketches::countsketch_heap::CountSketchWithL2Norm::<DefaultXxHasher>::with_dimensions(2, 8)
                .to_asapv1()
                .expect("serialize CountL2HH");
        let univmon = crate::UnivMon::new(4, 2, 8, 2)
            .to_asapv1()
            .expect("serialize UnivMon");
        let pyramid = crate::UnivMonTwoTier::new(4, 1, 2, 8, 2, 4, 2)
            .to_asapv1()
            .expect("serialize UnivMonPyramid");

        for foreign in [count_sketch, count_l2hh, univmon, pyramid] {
            assert!(
                UnivMonQ::<DefaultXxHasher>::from_asapv1(&foreign).is_err(),
                "a foreign envelope must not decode as a UnivMon-Q"
            );
        }
    }

    /// Fail closed (not panic) on crafted level counts, widths and capacities,
    /// including a level layout far larger than the payload carries. Every
    /// check precedes an allocation.
    #[test]
    fn univmon_q_rejects_crafted_shapes() {
        let encoded = populated().to_asapv1().expect("serialize");
        let base = metadata_of(&encoded);
        let payload = payload_of(&encoded);

        let shaped = |config: UnivMonQConfig| {
            univmon_q_metadata::<DefaultXxHasher>(config, "i64").expect("metadata")
        };
        let cases = [
            shaped(config_with(|config| config.layers = 63)),
            shaped(config_with(|config| {
                config.top_layer_cols = u32::MAX as usize
            })),
            shaped(config_with(|config| config.layers = 1)),
            shaped(config_with(|config| config.rows = 4)),
            shaped(config_with(|config| config.heap_size = 0)),
            shaped(config_with(|config| config.ordered_samples = 0)),
            shaped(config_with(|config| config.seed_index = 6)),
        ];
        for meta in cases {
            assert!(
                UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&meta, &payload)).is_err(),
                "a crafted shape must be rejected, not decoded"
            );
        }

        let mut short = payload_of(&encoded);
        short.candidate_scores.pop();
        assert!(UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &short)).is_err());

        let mut flags = payload_of(&encoded);
        flags.ever_evicted.pop();
        assert!(UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &flags)).is_err());

        let mut extrema = payload_of(&encoded);
        extrema.min = None;
        assert!(UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &extrema)).is_err());

        let mut swapped = payload_of(&encoded);
        std::mem::swap(&mut swapped.min, &mut swapped.max);
        assert!(UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &swapped)).is_err());

        let mut truncated = payload_of(&encoded);
        truncated.occurrence_keys.pop();
        assert!(UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &truncated)).is_err());

        let mut misplaced = payload_of(&encoded);
        misplaced.candidate_keys.reverse();
        assert!(
            UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, &misplaced)).is_err(),
            "a candidate must live in the level its own key hashes into"
        );
    }

    /// Candidates within a layer and the occurrences are each strictly
    /// ascending; a swapped pair or a repeat is rejected.
    #[test]
    fn univmon_q_rejects_entries_out_of_emitted_order() {
        let encoded = populated().to_asapv1().expect("serialize");
        let base = metadata_of(&encoded);
        let decode = |payload: &UnivMonQPayload<i64>| {
            UnivMonQ::<DefaultXxHasher>::from_asapv1(&crafted(&base, payload))
        };
        assert!(decode(&payload_of(&encoded)).is_ok());

        let payload = payload_of(&encoded);
        let layer = payload
            .candidate_lens
            .iter()
            .position(|&len| len >= 2)
            .expect("a layer holding two candidates");
        let start: usize = payload.candidate_lens[..layer]
            .iter()
            .map(|&len| len as usize)
            .sum();
        let mut swapped = payload_of(&encoded);
        swapped.candidate_keys.swap(start, start + 1);
        swapped.candidate_scores.swap(start, start + 1);
        let error = decode(&swapped).map(drop).expect_err("swapped candidates");
        assert!(
            error.to_string().contains("not strictly ascending"),
            "{error}"
        );

        let mut repeated = payload_of(&encoded);
        repeated.candidate_keys[start + 1] = repeated.candidate_keys[start];
        assert!(decode(&repeated).is_err(), "a repeated candidate key");

        let mut occurrences = payload_of(&encoded);
        assert!(occurrences.occurrence_keys.len() >= 2);
        occurrences.occurrence_priority_high.swap(0, 1);
        occurrences.occurrence_priority_low.swap(0, 1);
        occurrences.occurrence_keys.swap(0, 1);
        let error = decode(&occurrences)
            .map(drop)
            .expect_err("swapped occurrences");
        assert!(
            error.to_string().contains("not strictly ascending"),
            "{error}"
        );

        let mut twice = payload_of(&encoded);
        twice.occurrence_priority_high[1] = twice.occurrence_priority_high[0];
        twice.occurrence_priority_low[1] = twice.occurrence_priority_low[0];
        twice.occurrence_keys[1] = twice.occurrence_keys[0];
        assert!(decode(&twice).is_err(), "a repeated occurrence");
    }

    /// A sketch whose levels disagree with its own config must not serialize.
    #[test]
    fn univmon_q_rejects_serializing_an_inconsistent_state() {
        let mut wrong_layout = populated();
        wrong_layout.layers[1].sketch = PackedCountSketch::new(8, 3, 64);
        assert!(
            wrong_layout.to_asapv1().is_err(),
            "a level whose CountSketch is not the config's size must not serialize"
        );

        let mut wrong_extrema = populated();
        wrong_extrema.min = None;
        assert!(
            wrong_extrema.to_asapv1().is_err(),
            "a count/min/max triple the algorithm cannot reach must not serialize"
        );

        let mut over_capacity = populated();
        over_capacity.config.heap_size = 1;
        assert!(
            over_capacity.to_asapv1().is_err(),
            "a candidate table over its capacity must not serialize"
        );
    }

    /// Fail closed on an unexpected metadata key, and on a missing required
    /// one.
    #[test]
    fn univmon_q_metadata_rejects_unknown_and_missing_keys() {
        #[derive(Serialize)]
        struct WithExtra {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            seed_index: u32,
            layers: u32,
            top_layer_cols: u32,
            cols_halving_period: u8,
            rows: u32,
            counter_type: String,
            heap_size: u32,
            ordered_samples: u32,
            bogus_field: u8, // key not in UnivMonQMetadata
        }
        #[derive(Serialize)]
        struct WithoutCounterType {
            metadata_version: u8,
            hash_profile_id: String,
            hash_algorithm: String,
            seed_derivation: String,
            input_encoding: String,
            seed_list: Vec<u64>,
            seed_index: u32,
            layers: u32,
            top_layer_cols: u32,
            cols_halving_period: u8,
            rows: u32,
            heap_size: u32,
            ordered_samples: u32,
        }
        let m = univmon_q_metadata::<DefaultXxHasher>(tiny_config(), "i64").expect("metadata");
        let extra = WithExtra {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            seed_index: m.seed_index,
            layers: m.layers,
            top_layer_cols: m.top_layer_cols,
            cols_halving_period: m.cols_halving_period,
            rows: m.rows,
            counter_type: m.counter_type.clone(),
            heap_size: m.heap_size,
            ordered_samples: m.ordered_samples,
            bogus_field: 7,
        };
        let without = WithoutCounterType {
            metadata_version: m.metadata_version,
            hash_profile_id: m.hash_profile_id.clone(),
            hash_algorithm: m.hash_algorithm.clone(),
            seed_derivation: m.seed_derivation.clone(),
            input_encoding: m.input_encoding.clone(),
            seed_list: m.seed_list.clone(),
            seed_index: m.seed_index,
            layers: m.layers,
            top_layer_cols: m.top_layer_cols,
            cols_halving_period: m.cols_halving_period,
            rows: m.rows,
            heap_size: m.heap_size,
            ordered_samples: m.ordered_samples,
        };
        assert!(
            from_slice::<UnivMonQMetadata>(&rmp_serde::to_vec_named(&extra).unwrap()).is_err(),
            "an unknown metadata key must be rejected"
        );
        assert!(
            from_slice::<UnivMonQMetadata>(&rmp_serde::to_vec_named(&without).unwrap()).is_err(),
            "a missing required key must be rejected"
        );
    }
}
