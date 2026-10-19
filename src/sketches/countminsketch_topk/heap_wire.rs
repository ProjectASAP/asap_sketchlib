//! ASAPv1 wire pieces shared by the heap-backed top-k sketches.
//!
//! [`CMSHeap`](super::CMSHeap) (kind_id `0x03 0x00`) and
//! [`CSHeap`](crate::sketches::countsketch_topk::CSHeap) (kind_id `0x0a 0x00`)
//! are each a matrix sketch plus an [`HHHeap`], so both carry the same
//! descriptor metadata and the same positional payload
//! `[counts, keys, heap_counts]`; their deltas carry
//! `[indices, deltas, keys, heap_counts]` under the same metadata. This module
//! is the one place that encoding lives: the metadata DTO, the payload DTOs, the
//! `key_type` mapping, the emitted order, and the heap rebuild. See
//! `docs/asapv1_wire_format.md` §3.7, §3.10 and §3.23 for the pieces it follows.
//!
//! ## `key_type` names the exact `HeapItem` variant
//!
//! A heap key's type is a **runtime** property: keys are [`HeapItem`]s, a
//! 16-variant enum, and one heap's keys are whatever the caller inserted. One
//! metadata `key_type` names the exact variant and the payload's `keys` array
//! is homogeneous in it. The variant is never widened (an `i32` key is `"i32"`,
//! not `"i64"`) because `HeapItem`'s equality against a query `DataInput`
//! discriminates on the variant while the digest does not: a widened key lands
//! in the same index slot and then silently fails equality.
//! `HeapItem::I128` / `U128` have no msgpack integer form and are not wire
//! types, so a heap holding one refuses to serialize — as does one whose keys
//! mix variants, `String` and `Bytes` included. A `Bytes` key is written as
//! msgpack `bin` through `WireBytes` and read back from `bin` alone, so any
//! byte string survives whether or not it is UTF-8 and a `str`-keyed payload
//! relabelled `"bytes"` is refused. A `String` key is read from `str` alone
//! through `WireString`, so `bin` keys relabelled `"string"` are refused too.
//!
//! ## Emitted order (byte-stable round trips)
//!
//! The heap's array order follows the sift path and does not survive a rebuild,
//! so the payload is **order-defined**: descending count ([`HeapCount::order`],
//! `f64::total_cmp` for `f64` counts), ties broken by [`key_order`], a total
//! order over [`HeapItem`] (variant tag first, then the value). Two heaps holding
//! the same entries emit the same bytes whatever order they were seated in, and
//! re-serializing a decoded sketch reproduces its bytes exactly. The decoder
//! rejects entries in any other order.
//!
//! ## What the heap does not put on the wire
//!
//! `HHHeap`'s `slots` and `positions` are `#[serde(skip)]` and are rebuilt from
//! the entries, so no index reaches the wire and no crafted payload can point
//! one out of bounds or into a cycle. `heap_size` is construction config, so it
//! lives in the metadata; it never sizes an allocation on decode.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashSet;

use crate::common::hash::check_matrix_rows;
use crate::message_pack_format::envelope;
use crate::message_pack_format::wire_key::{WireBytes, WireString, exact_vec};
use crate::{HHHeap, HashProfile, HeapCount, HeapItem};

/// Metadata `key_type` of a heap that holds nothing. A heap with no entries has
/// no variant to report, so the wire pins one: empty heaps have one encoding.
pub(crate) const EMPTY_KEY_TYPE: &str = "u64";

/// The metadata `key_type` naming a key's exact [`HeapItem`] variant, or `None`
/// for the 128-bit variants, which are not wire types.
pub(crate) fn key_type_of(key: &HeapItem) -> Option<&'static str> {
    Some(match key {
        HeapItem::I8(_) => "i8",
        HeapItem::I16(_) => "i16",
        HeapItem::I32(_) => "i32",
        HeapItem::I64(_) => "i64",
        HeapItem::ISIZE(_) => "isize",
        HeapItem::U8(_) => "u8",
        HeapItem::U16(_) => "u16",
        HeapItem::U32(_) => "u32",
        HeapItem::U64(_) => "u64",
        HeapItem::USIZE(_) => "usize",
        HeapItem::F32(_) => "f32",
        HeapItem::F64(_) => "f64",
        HeapItem::String(_) => "string",
        HeapItem::Bytes(_) => "bytes",
        HeapItem::I128(_) | HeapItem::U128(_) => return None,
    })
}

/// A total order over keys: variant tag first, then the value.
fn key_order(key: &HeapItem) -> (u8, u128, &[u8]) {
    match key {
        HeapItem::I8(v) => (0, *v as i128 as u128, b""),
        HeapItem::I16(v) => (1, *v as i128 as u128, b""),
        HeapItem::I32(v) => (2, *v as i128 as u128, b""),
        HeapItem::I64(v) => (3, *v as i128 as u128, b""),
        HeapItem::I128(v) => (4, *v as u128, b""),
        HeapItem::ISIZE(v) => (5, *v as i128 as u128, b""),
        HeapItem::U8(v) => (6, u128::from(*v), b""),
        HeapItem::U16(v) => (7, u128::from(*v), b""),
        HeapItem::U32(v) => (8, u128::from(*v), b""),
        HeapItem::U64(v) => (9, u128::from(*v), b""),
        HeapItem::U128(v) => (10, *v, b""),
        HeapItem::USIZE(v) => (11, *v as u128, b""),
        HeapItem::F32(v) => (12, u128::from(v.to_bits()), b""),
        HeapItem::F64(v) => (13, u128::from(v.to_bits()), b""),
        HeapItem::String(v) => (14, 0, v.as_bytes()),
        HeapItem::Bytes(v) => (15, 0, v.as_slice()),
    }
}

/// The emitted order of two entries: descending count, ties broken by
/// [`key_order`].
fn emitted_order<N: HeapCount>(a: (&HeapItem, N), b: (&HeapItem, N)) -> Ordering {
    b.1.order(&a.1)
        .then_with(|| key_order(a.0).cmp(&key_order(b.0)))
}

/// The complaint both directions report when one key is on the wire twice.
const DUPLICATE_KEY: &str = "ASAPv1 top-k heap: the same key appears twice";

/// Rejects a key set holding the same key twice, judged on the **wire**
/// representation [`key_order`] gives it: floats by their bits, so two identical
/// `NaN`s are one key twice while `+0.0` and `-0.0` stay two keys. Both the
/// encode and the decode path run this one check, so neither emits nor admits a
/// key set the other refuses. The UnivMon pyramid encoders call it per layer,
/// so the heap-backed sketches and the pyramids share one predicate.
pub(crate) fn check_distinct_keys<'a>(
    keys: impl ExactSizeIterator<Item = &'a HeapItem>,
) -> Result<(), &'static str> {
    let mut seen: HashSet<(u8, u128, &[u8])> = HashSet::with_capacity(keys.len());
    for key in keys {
        if !seen.insert(key_order(key)) {
            return Err(DUPLICATE_KEY);
        }
    }
    Ok(())
}

/// Top-k descriptor metadata (ASAPv1 §2), a msgpack **map** (`to_vec_named`)
/// with keys in this declaration order — the canonical order the wire spec
/// fixes (Go must mirror it). Hash-spec fields first, then the base sketch's
/// structural params `rows` / `cols` / `counter_type` / `mode`, then the heap's
/// `heap_size` / `key_type`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TopKMetadata {
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
    pub(crate) heap_size: u32,
    pub(crate) key_type: String,
}

/// Builds the descriptor metadata from the hasher's [`HashProfile`], so the wire
/// bytes truthfully describe how the sketch was hashed. `rows` / `cols` /
/// `counter_type` / `mode` describe the base matrix; `heap_size` is the heap
/// capacity and `key_type` the exact [`HeapItem`] variant the `keys` array
/// carries.
pub(crate) fn topk_metadata<H: HashProfile>(
    rows: u32,
    cols: u32,
    counter_type: &str,
    mode: &str,
    heap_size: u32,
    key_type: &str,
) -> TopKMetadata {
    TopKMetadata {
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
        heap_size,
        key_type: key_type.to_string(),
    }
}

/// The wire-eligible base matrix: `1 <= rows <= MATRIX_MAX_ROWS` and
/// `1 <= cols <= u32::MAX`.
pub(crate) fn check_geometry(sketch: &str, rows: usize, cols: usize) -> Result<(), String> {
    if rows == 0 || cols == 0 {
        return Err(format!(
            "{sketch} dimensions must be non-zero: rows={rows}, cols={cols}"
        ));
    }
    check_matrix_rows(sketch, rows)?;
    if u32::try_from(cols).is_err() {
        return Err(format!(
            "{sketch} cols {cols} exceeds the u32 metadata field"
        ));
    }
    Ok(())
}

/// The metadata block of a `sketch` (`"CMSHeap"` / `"CSHeap"`) state or delta.
/// Fails when the geometry is not wire-eligible or `heap_size` overflows the
/// metadata's `u32` field.
pub(crate) fn encode_metadata<H: HashProfile>(
    sketch: &str,
    rows: usize,
    cols: usize,
    counter_type: &str,
    mode: &str,
    heap_size: usize,
    key_type: &str,
) -> Result<Vec<u8>, RmpEncodeError> {
    let complain = |e: String| RmpEncodeError::Syntax(format!("ASAPv1 {sketch} envelope: {e}"));
    check_geometry(sketch, rows, cols).map_err(complain)?;
    let heap_size = u32::try_from(heap_size).map_err(|_| {
        complain(format!(
            "heap_size {heap_size} exceeds the u32 metadata field"
        ))
    })?;
    rmp_serde::to_vec_named(&topk_metadata::<H>(
        rows as u32,
        cols as u32,
        counter_type,
        mode,
        heap_size,
        key_type,
    ))
}

/// What a top-k metadata block declares beyond the pinned fields.
pub(crate) struct TopKShape {
    pub(crate) rows: usize,
    pub(crate) cols: usize,
    pub(crate) heap_size: usize,
    pub(crate) key_type: String,
}

/// Reads a metadata block. The hash spec, `counter_type` and `mode` must be the
/// target's; `rows` / `cols` / `heap_size` / `key_type` are echoed back, and the
/// geometry must be wire-eligible, checked before anything is sized from it.
pub(crate) fn decode_metadata<H: HashProfile>(
    sketch: &str,
    block: &[u8],
    counter_type: &str,
    mode: &str,
) -> Result<TopKShape, RmpDecodeError> {
    let meta: TopKMetadata = envelope::decode_block(block)?;
    if meta
        != topk_metadata::<H>(
            meta.rows,
            meta.cols,
            counter_type,
            mode,
            meta.heap_size,
            &meta.key_type,
        )
    {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 {sketch} envelope: metadata mismatch"
        )));
    }
    let (rows, cols) = (meta.rows as usize, meta.cols as usize);
    check_geometry(sketch, rows, cols).map_err(RmpDecodeError::Uncategorized)?;
    Ok(TopKShape {
        rows,
        cols,
        heap_size: meta.heap_size as usize,
        key_type: meta.key_type,
    })
}

/// Top-k payload, a msgpack **array** (`to_vec`, positional):
/// `[counts, keys, heap_counts]`. `counts` is the base matrix packed row-major
/// with the element type the metadata `counter_type` fixes; `keys` and
/// `heap_counts` are the heap's entries, parallel and equal-length, with `keys`
/// typed by the metadata `key_type`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TopKPayload<C, K, N> {
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
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "N: Deserialize<'de>")
    )]
    pub(crate) heap_counts: Vec<N>,
}

/// Top-k delta payload (§3.23), a msgpack **array**:
/// `[indices, deltas, keys, heap_counts]`. `indices` / `deltas` are the changed
/// cells; `keys` / `heap_counts` are the producer's whole heap, as in
/// [`TopKPayload`].
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TopKDeltaPayload<C, K, N> {
    pub(crate) indices: Vec<u64>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "C: Deserialize<'de>")
    )]
    pub(crate) deltas: Vec<C>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "K: Deserialize<'de>")
    )]
    pub(crate) keys: Vec<K>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "N: Deserialize<'de>")
    )]
    pub(crate) heap_counts: Vec<N>,
}

/// The matrix part of a top-k payload, written ahead of the heap's `keys` and
/// `heap_counts`: a state's `counts`, or a delta's [`DeltaCells`].
pub(crate) trait TopKBody: Sized {
    /// Packs `self` and the heap arrays into one payload.
    fn encode<K: Serialize, N: Serialize>(
        self,
        keys: Vec<K>,
        heap_counts: Vec<N>,
    ) -> Result<Vec<u8>, RmpEncodeError>;

    /// Reads one payload back into `self` and the heap arrays.
    fn decode<K, N>(payload: &[u8]) -> Result<(Self, Vec<K>, Vec<N>), RmpDecodeError>
    where
        K: for<'de> Deserialize<'de>,
        N: for<'de> Deserialize<'de>;
}

impl<C: Serialize + for<'de> Deserialize<'de>> TopKBody for Vec<C> {
    fn encode<K: Serialize, N: Serialize>(
        self,
        keys: Vec<K>,
        heap_counts: Vec<N>,
    ) -> Result<Vec<u8>, RmpEncodeError> {
        rmp_serde::to_vec(&TopKPayload {
            counts: self,
            keys,
            heap_counts,
        })
    }

    fn decode<K, N>(payload: &[u8]) -> Result<(Self, Vec<K>, Vec<N>), RmpDecodeError>
    where
        K: for<'de> Deserialize<'de>,
        N: for<'de> Deserialize<'de>,
    {
        let p: TopKPayload<C, K, N> = envelope::decode_block(payload)?;
        Ok((p.counts, p.keys, p.heap_counts))
    }
}

/// A delta's changed cells: row-major `indices` and the value added at each.
pub(crate) struct DeltaCells<C> {
    pub(crate) indices: Vec<u64>,
    pub(crate) deltas: Vec<C>,
}

impl<C: Serialize + for<'de> Deserialize<'de>> TopKBody for DeltaCells<C> {
    fn encode<K: Serialize, N: Serialize>(
        self,
        keys: Vec<K>,
        heap_counts: Vec<N>,
    ) -> Result<Vec<u8>, RmpEncodeError> {
        rmp_serde::to_vec(&TopKDeltaPayload {
            indices: self.indices,
            deltas: self.deltas,
            keys,
            heap_counts,
        })
    }

    fn decode<K, N>(payload: &[u8]) -> Result<(Self, Vec<K>, Vec<N>), RmpDecodeError>
    where
        K: for<'de> Deserialize<'de>,
        N: for<'de> Deserialize<'de>,
    {
        let p: TopKDeltaPayload<C, K, N> = envelope::decode_block(payload)?;
        let cells = DeltaCells {
            indices: p.indices,
            deltas: p.deltas,
        };
        Ok((cells, p.keys, p.heap_counts))
    }
}

/// The heap's entries in emitted order: descending count, ties broken by
/// [`key_order`]. Independent of the heap array's sift order, so the bytes are
/// stable across a round trip.
pub(crate) fn heap_entries<N: HeapCount>(heap: &HHHeap<N>) -> Vec<(&HeapItem, N)> {
    let mut entries: Vec<(&HeapItem, N)> = heap
        .heap()
        .iter()
        .map(|item| (&item.key, item.count))
        .collect();
    entries.sort_unstable_by(|a, b| emitted_order(*a, *b));
    entries
}

/// The `key_type` the payload will be written in, taken from the first key in
/// emitted order. Rejects a 128-bit key outright; the homogeneity of the rest is
/// enforced by [`encode_payload`].
pub(crate) fn wire_key_type<N>(entries: &[(&HeapItem, N)]) -> Result<&'static str, RmpEncodeError> {
    match entries.first() {
        None => Ok(EMPTY_KEY_TYPE),
        Some((key, _)) => key_type_of(key).ok_or_else(|| {
            RmpEncodeError::Syntax("ASAPv1 top-k heap: 128-bit keys are not wire types".to_string())
        }),
    }
}

fn mixed_variant_error(key_type: &str, key: &HeapItem) -> RmpEncodeError {
    RmpEncodeError::Syntax(format!(
        "ASAPv1 top-k heap: keys mix variants — key_type is {key_type}, but a {} key is held",
        key_type_of(key).unwrap_or("128-bit")
    ))
}

/// Packs the matrix part and the emitted heap entries into the positional
/// payload, with `keys` typed by `key_type`. Any key that is not of that variant
/// fails the encode, as does a key the entries hold twice on the wire — a live
/// heap can seat one, since `NaN` misses its own lookup.
pub(crate) fn encode_payload<B: TopKBody, N: HeapCount>(
    key_type: &str,
    body: B,
    entries: &[(&HeapItem, N)],
) -> Result<Vec<u8>, RmpEncodeError> {
    check_distinct_keys(entries.iter().map(|(key, _)| *key))
        .map_err(|problem| RmpEncodeError::Syntax(problem.to_string()))?;
    let heap_counts: Vec<N> = entries.iter().map(|entry| entry.1).collect();

    macro_rules! pack {
        ($variant:ident, $value:ident => $wire:expr) => {{
            let mut keys = Vec::with_capacity(entries.len());
            for (key, _) in entries {
                match key {
                    HeapItem::$variant($value) => keys.push($wire),
                    _ => return Err(mixed_variant_error(key_type, key)),
                }
            }
            body.encode(keys, heap_counts)
        }};
    }

    match key_type {
        "i8" => pack!(I8, v => *v),
        "i16" => pack!(I16, v => *v),
        "i32" => pack!(I32, v => *v),
        "i64" => pack!(I64, v => *v),
        "isize" => pack!(ISIZE, v => *v),
        "u8" => pack!(U8, v => *v),
        "u16" => pack!(U16, v => *v),
        "u32" => pack!(U32, v => *v),
        "u64" => pack!(U64, v => *v),
        "usize" => pack!(USIZE, v => *v),
        "f32" => pack!(F32, v => *v),
        "f64" => pack!(F64, v => *v),
        "string" => pack!(String, v => WireString(v.clone())),
        "bytes" => pack!(Bytes, v => WireBytes(v.clone())),
        other => Err(RmpEncodeError::Syntax(format!(
            "ASAPv1 top-k heap: key_type {other:?} is not a wire key type"
        ))),
    }
}

/// The matrix part and the heap entries one payload decodes into.
pub(crate) type DecodedTopK<B, N> = (B, Vec<(HeapItem, N)>);

/// Reads the payload with `keys` typed by the metadata `key_type`, returning the
/// matrix part and the heap entries. Rejects an unknown `key_type` and parallel
/// arrays of unequal length; a `keys` array whose msgpack types do not match
/// `key_type` fails to decode.
pub(crate) fn decode_payload<B: TopKBody, N: HeapCount>(
    key_type: &str,
    payload: &[u8],
) -> Result<DecodedTopK<B, N>, RmpDecodeError> {
    macro_rules! unpack {
        ($ty:ty, $into:expr) => {{
            let (body, keys, heap_counts) = B::decode::<$ty, N>(payload)?;
            let keys: Vec<HeapItem> = keys.into_iter().map($into).collect();
            (body, keys, heap_counts)
        }};
    }

    let (body, keys, heap_counts) = match key_type {
        "i8" => unpack!(i8, HeapItem::I8),
        "i16" => unpack!(i16, HeapItem::I16),
        "i32" => unpack!(i32, HeapItem::I32),
        "i64" => unpack!(i64, HeapItem::I64),
        "isize" => unpack!(isize, HeapItem::ISIZE),
        "u8" => unpack!(u8, HeapItem::U8),
        "u16" => unpack!(u16, HeapItem::U16),
        "u32" => unpack!(u32, HeapItem::U32),
        "u64" => unpack!(u64, HeapItem::U64),
        "usize" => unpack!(usize, HeapItem::USIZE),
        "f32" => unpack!(f32, HeapItem::F32),
        "f64" => unpack!(f64, HeapItem::F64),
        "string" => unpack!(WireString, |key: WireString| HeapItem::String(
            key.into_string()
        )),
        "bytes" => unpack!(WireBytes, |key: WireBytes| HeapItem::Bytes(key.into_vec())),
        other => {
            return Err(RmpDecodeError::Uncategorized(format!(
                "ASAPv1 top-k heap: key_type {other:?} is not a wire key type"
            )));
        }
    };

    if keys.len() != heap_counts.len() {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 top-k heap: {} keys against {} counts",
            keys.len(),
            heap_counts.len()
        )));
    }
    Ok((body, keys.into_iter().zip(heap_counts).collect()))
}

/// Seats the decoded entries into a heap of capacity `heap_size`, rebuilding the
/// index as it goes. `heap_size` is checked against the entry count first and
/// the heap reserves from that count, so a declared capacity never sizes an
/// allocation on its own.
///
/// The keys are checked for duplicates on their **wire** representation before
/// any is seated — floats by their bits, so two identical `NaN`s are one key
/// twice even though they do not compare equal — then for the emitted order,
/// and the seated count is checked afterwards, which catches keys that are
/// distinct on the wire but equal to the heap.
pub(crate) fn rebuild_heap<N: HeapCount>(
    heap_size: usize,
    entries: Vec<(HeapItem, N)>,
) -> Result<HHHeap<N>, RmpDecodeError> {
    if entries.len() > heap_size {
        return Err(RmpDecodeError::Uncategorized(format!(
            "ASAPv1 top-k heap: {} entries over a heap_size of {heap_size}",
            entries.len()
        )));
    }
    check_distinct_keys(entries.iter().map(|(key, _)| key))
        .map_err(|problem| RmpDecodeError::Uncategorized(problem.to_string()))?;
    if entries
        .windows(2)
        .any(|pair| emitted_order((&pair[0].0, pair[0].1), (&pair[1].0, pair[1].1)).is_ge())
    {
        return Err(RmpDecodeError::Uncategorized(
            "ASAPv1 top-k heap: entries are not in descending count, ties by key".to_string(),
        ));
    }
    let mut heap = HHHeap::with_expected_len(heap_size, entries.len());
    let seated = entries.len();
    for (key, count) in entries {
        heap.update_heap_item(&key, count);
    }
    if heap.len() != seated {
        return Err(RmpDecodeError::Uncategorized(DUPLICATE_KEY.to_string()));
    }
    Ok(heap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_pack_format::envelope;
    use crate::{DataInput, DefaultXxHasher, RegularPath, Vector2D};
    use rmp_serde::from_slice;

    use super::super::CMSHeap;

    /// CMSHeap's kind_id, so the shared-heap tests can build real envelopes.
    const KIND: &[u8] = &[0x03, 0x00];

    fn sketch_with(keys: &[(DataInput, i64)]) -> CMSHeap<Vector2D<i64>, RegularPath> {
        let mut sketch = CMSHeap::<Vector2D<i64>, RegularPath>::new(2, 8, 8);
        for (key, count) in keys {
            sketch.heap_mut().update(key, *count);
        }
        sketch
    }

    /// The complaint a crafted envelope draws, with no `Debug` bound on the
    /// sketch.
    fn decode_error(bytes: &[u8]) -> String {
        match CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(bytes) {
            Ok(_) => panic!("a crafted envelope must be rejected, not decoded"),
            Err(err) => err.to_string(),
        }
    }

    fn metadata_of(bytes: &[u8]) -> TopKMetadata {
        let (_, metadata, _) = envelope::split(bytes).expect("split");
        from_slice(metadata).expect("metadata")
    }

    /// Every wire-eligible key family round-trips under its own name, and the
    /// decoded key still answers to the caller's original `DataInput`. A widened
    /// `key_type` would keep the digest but rebuild a different variant.
    #[test]
    fn heap_every_key_type_round_trips_and_keeps_its_variant() {
        fn check(expected: &str, values: &[DataInput]) {
            let entries: Vec<(DataInput, i64)> = values
                .iter()
                .enumerate()
                .map(|(i, value)| (value.clone(), 3 * (i as i64 + 1)))
                .collect();
            let sketch = sketch_with(&entries);
            assert_eq!(
                sketch.heap().len(),
                values.len(),
                "{expected}: keys collided"
            );

            let bytes = sketch.serialize_to_bytes().expect("serialize");
            assert_eq!(metadata_of(&bytes).key_type, expected);

            let decoded = CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes)
                .expect("decode");
            assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), bytes);
            for (i, value) in values.iter().enumerate() {
                let index = decoded
                    .heap()
                    .find(value)
                    .unwrap_or_else(|| panic!("{expected}: {value:?} lost its variant"));
                assert_eq!(decoded.heap().heap()[index].count, 3 * (i as i64 + 1));
            }
        }

        check("i8", &[DataInput::I8(-3), DataInput::I8(120)]);
        check("i16", &[DataInput::I16(-3000), DataInput::I16(9)]);
        check("i32", &[DataInput::I32(-7), DataInput::I32(65_537)]);
        check("i64", &[DataInput::I64(-1), DataInput::I64(1 << 40)]);
        check("isize", &[DataInput::ISIZE(-11), DataInput::ISIZE(11)]);
        check("u8", &[DataInput::U8(0), DataInput::U8(255)]);
        check("u16", &[DataInput::U16(1), DataInput::U16(65_535)]);
        check("u32", &[DataInput::U32(7), DataInput::U32(u32::MAX)]);
        check("u64", &[DataInput::U64(9), DataInput::U64(u64::MAX)]);
        check("usize", &[DataInput::USIZE(4), DataInput::USIZE(1 << 33)]);
        check("f32", &[DataInput::F32(-0.5), DataInput::F32(3.25)]);
        check("f64", &[DataInput::F64(2.5), DataInput::F64(-1e300)]);
        check("string", &[DataInput::Str("alpha"), DataInput::Str("beta")]);
    }

    #[test]
    fn heap_refuses_mixed_and_128_bit_keys() {
        let mixed = sketch_with(&[(DataInput::I32(1), 5), (DataInput::I64(2), 3)]);
        let problem = mixed
            .serialize_to_bytes()
            .expect_err("a heap mixing key variants must not serialize")
            .to_string();
        assert!(problem.contains("keys mix variants"), "got {problem}");
        assert!(problem.contains("key_type is i32"), "got {problem}");

        for key in [DataInput::I128(1), DataInput::U128(2)] {
            let wide = sketch_with(&[(key.clone(), 5)]);
            let problem = wide
                .serialize_to_bytes()
                .expect_err("a 128-bit key is not a wire type")
                .to_string();
            assert!(problem.contains("128-bit"), "got {problem}");
        }

        // A 128-bit key behind a wire-eligible one is caught on the way into the
        // payload rather than by the first-key check.
        let trailing = sketch_with(&[(DataInput::U64(1), 9), (DataInput::U128(2), 1)]);
        assert!(trailing.serialize_to_bytes().is_err());
    }

    #[test]
    fn heap_empty_emits_the_pinned_key_type_and_round_trips() {
        let sketch = CMSHeap::<Vector2D<i64>, RegularPath>::new(2, 8, 8);
        let bytes = sketch.serialize_to_bytes().expect("serialize");
        let meta = metadata_of(&bytes);
        assert_eq!(meta.key_type, EMPTY_KEY_TYPE);
        assert_eq!(meta.heap_size, 8);

        let decoded =
            CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.heap().len(), 0);
        assert_eq!(decoded.heap().capacity(), 8);
        assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), bytes);
    }

    /// The emitted order is fixed by (count, key_order), not by the sift path,
    /// so the same entries always produce the same bytes. Both count ties below
    /// are broken only by the key.
    #[test]
    fn heap_emitted_order_is_independent_of_seat_order() {
        let entries = [
            (DataInput::U64(3), 5i64),
            (DataInput::U64(1), 9),
            (DataInput::U64(2), 5),
            (DataInput::U64(4), 9),
        ];
        let mut reversed = entries.clone();
        reversed.reverse();

        let seated = sketch_with(&entries);
        let reseated = sketch_with(&reversed);
        let bytes = seated.serialize_to_bytes().expect("serialize");
        assert_eq!(
            bytes,
            reseated.serialize_to_bytes().expect("serialize"),
            "the emitted order followed the seat order"
        );

        let (_, _, payload) = envelope::split(&bytes).expect("split");
        let emitted: TopKPayload<i64, u64, i64> = from_slice(payload).expect("payload");
        assert_eq!(emitted.heap_counts, vec![9, 9, 5, 5], "not descending");
        assert_eq!(emitted.keys, vec![1, 4, 2, 3], "ties not broken by key");

        let decoded =
            CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(
            decoded.serialize_to_bytes().expect("re-serialize"),
            bytes,
            "a decoded heap re-serialized to different bytes"
        );
    }

    /// The `keys` array is read **as** the declared type, so string-keyed bytes
    /// relabelled `"u64"` do not decode.
    #[test]
    fn heap_rejects_a_key_type_the_payload_does_not_carry() {
        let strings = sketch_with(&[(DataInput::Str("alpha"), 5)]);
        let string_bytes = strings.serialize_to_bytes().expect("serialize");
        let (_, _, string_payload) = envelope::split(&string_bytes).expect("split");

        let numbers = sketch_with(&[(DataInput::U64(11), 5)]);
        let number_bytes = numbers.serialize_to_bytes().expect("serialize");
        let (_, _, number_payload) = envelope::split(&number_bytes).expect("split");

        for (claimed, payload) in [("u64", string_payload), ("string", number_payload)] {
            let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
                2, 8, "i64", "regular", 8, claimed,
            ))
            .expect("metadata");
            let forged = envelope::encode(KIND, &metadata, payload);
            assert!(
                CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&forged).is_err(),
                "a payload relabelled {claimed} must be rejected"
            );
        }
    }

    /// A `"string"` key is read from msgpack `str` alone: UTF-8 `bin` keys
    /// relabelled `"string"` are rejected, as `str` keys relabelled `"bytes"` are.
    #[test]
    fn heap_string_and_bytes_keys_do_not_decode_as_each_other() {
        let forge = |sketch: CMSHeap<Vector2D<i64>, RegularPath>, claimed: &str| {
            let bytes = sketch.serialize_to_bytes().expect("serialize");
            let (_, _, payload) = envelope::split(&bytes).expect("split");
            let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
                2, 8, "i64", "regular", 8, claimed,
            ))
            .expect("metadata");
            envelope::encode(KIND, &metadata, payload)
        };
        let bin = sketch_with(&[(DataInput::Bytes(b"alpha"), 5)]);
        let str_keys = sketch_with(&[(DataInput::Str("alpha"), 5)]);
        for (forged, claimed) in [
            (forge(bin, "string"), "string"),
            (forge(str_keys, "bytes"), "bytes"),
        ] {
            let problem =
                match CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&forged) {
                    Ok(_) => panic!("keys relabelled {claimed} must be rejected"),
                    Err(err) => err.to_string(),
                };
            assert!(problem.contains("must be msgpack"), "{claimed}: {problem}");
        }
    }

    /// `slots` and `positions` are rebuilt on decode: an update that moves an
    /// entry lands the same way on the decoded heap as on the original.
    #[test]
    fn heap_index_is_rebuilt_so_updates_still_move_entries() {
        let mut original = sketch_with(&[
            (DataInput::U64(1), 9),
            (DataInput::U64(2), 7),
            (DataInput::U64(3), 5),
            (DataInput::U64(4), 3),
        ]);
        let bytes = original.serialize_to_bytes().expect("serialize");
        let mut decoded =
            CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).expect("decode");

        // Rescore the root up, then seat a new key that displaces nothing.
        original.heap_mut().update(&DataInput::U64(4), 40);
        decoded.heap_mut().update(&DataInput::U64(4), 40);
        original.heap_mut().update(&DataInput::U64(5), 20);
        decoded.heap_mut().update(&DataInput::U64(5), 20);

        assert_eq!(
            decoded.serialize_to_bytes().expect("re-serialize"),
            original.serialize_to_bytes().expect("serialize"),
            "the decoded heap diverged after an update"
        );
        for key in 1..=5u64 {
            let probe = DataInput::U64(key);
            let found = decoded.heap().find(&probe).expect("key");
            assert_eq!(
                decoded.heap().heap()[found].count,
                original.heap().heap()[original.heap().find(&probe).expect("key")].count
            );
        }
    }

    /// A declared `heap_size` is metadata, never an allocation size.
    #[test]
    fn heap_does_not_allocate_a_declared_heap_size() {
        let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
            2,
            4,
            "i64",
            "regular",
            u32::MAX,
            "u64",
        ))
        .expect("metadata");
        let payload = rmp_serde::to_vec(&TopKPayload {
            counts: vec![0i64; 8],
            keys: vec![7u64, 8],
            heap_counts: vec![9i64, 4],
        })
        .expect("payload");
        let bytes = envelope::encode(KIND, &metadata, &payload);

        let decoded =
            CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.heap().capacity(), u32::MAX as usize);
        assert_eq!(decoded.heap().len(), 2);
    }

    /// More entries than `heap_size`, and the same key twice, are both states
    /// the heap could not have reached.
    #[test]
    fn heap_rejects_crafted_entry_sets() {
        let crafted = |heap_size: u32, keys: Vec<u64>, heap_counts: Vec<i64>| {
            let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
                2, 4, "i64", "regular", heap_size, "u64",
            ))
            .expect("metadata");
            let payload = rmp_serde::to_vec(&TopKPayload {
                counts: vec![0i64; 8],
                keys,
                heap_counts,
            })
            .expect("payload");
            envelope::encode(KIND, &metadata, &payload)
        };

        let cases = [
            (
                crafted(1, vec![1, 2], vec![5, 4]),
                "2 entries over a heap_size of 1",
            ),
            (
                crafted(4, vec![1, 1], vec![5, 4]),
                "the same key appears twice",
            ),
            (crafted(4, vec![1, 2], vec![5]), "2 keys against 1 counts"),
        ];
        for (bytes, expected) in cases {
            let problem = decode_error(&bytes);
            assert!(
                problem.contains(expected),
                "expected a complaint about {expected}, got {problem}"
            );
        }
    }

    /// Entries must arrive in emitted order: descending count, a tie broken by
    /// the key's unsigned bits.
    #[test]
    fn heap_rejects_entries_out_of_emitted_order() {
        let crafted = |keys: Vec<i64>, heap_counts: Vec<i64>| {
            let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
                2, 4, "i64", "regular", 4, "i64",
            ))
            .expect("metadata");
            let payload = rmp_serde::to_vec(&TopKPayload {
                counts: vec![0i64; 8],
                keys,
                heap_counts,
            })
            .expect("payload");
            envelope::encode(KIND, &metadata, &payload)
        };
        let rows = [
            ("i64 tie", crafted(vec![-1, 1], vec![5, 5])),
            ("i64 ascending", crafted(vec![1, 2], vec![4, 9])),
        ];
        for (label, bytes) in rows {
            let problem = decode_error(&bytes);
            assert!(
                problem.contains("not in descending count"),
                "{label}: {problem}"
            );
        }
    }

    /// Duplicates are judged on the wire bits, not on `HeapItem`'s equality.
    /// Two bit-identical `NaN`s do not compare equal, so both would seat and the
    /// seated count would agree; `+0.0` and `-0.0` do compare equal but hash
    /// apart, so the live heap seats both and that payload must still decode.
    #[test]
    fn heap_judges_duplicate_float_keys_on_their_bits() {
        let crafted = |keys: Vec<f64>| {
            let metadata = rmp_serde::to_vec_named(&topk_metadata::<DefaultXxHasher>(
                2, 4, "i64", "regular", 4, "f64",
            ))
            .expect("metadata");
            let payload = rmp_serde::to_vec(&TopKPayload {
                counts: vec![0i64; 8],
                keys,
                heap_counts: vec![5i64, 4],
            })
            .expect("payload");
            envelope::encode(KIND, &metadata, &payload)
        };

        let problem = decode_error(&crafted(vec![f64::NAN, f64::NAN]));
        assert!(
            problem.contains("the same key appears twice"),
            "got {problem}"
        );

        let signed_zeros = crafted(vec![0.0, -0.0]);
        let decoded = CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&signed_zeros)
            .expect("two zeros of opposite sign are two keys the heap can hold");
        assert_eq!(decoded.heap().len(), 2);
    }

    /// A live heap can seat one key twice: `update` compares residents with
    /// `HeapItem`'s equality, and `NaN != NaN`, so a second `NaN` misses the
    /// lookup and takes its own seat. That heap must fail the encode rather than
    /// emit a payload the decoder refuses. `+0.0` and `-0.0` differ in their
    /// bits, so they are two keys on both sides and still round-trip.
    #[test]
    fn heap_refuses_to_serialize_a_key_it_holds_twice() {
        let nans = sketch_with(&[(DataInput::F64(f64::NAN), 5), (DataInput::F64(f64::NAN), 4)]);
        assert_eq!(nans.heap().len(), 2, "the two NaNs did not both seat");
        let problem = nans
            .serialize_to_bytes()
            .expect_err("a heap holding one key twice must not serialize")
            .to_string();
        assert!(
            problem.contains("the same key appears twice"),
            "got {problem}"
        );

        let zeros = sketch_with(&[(DataInput::F64(0.0), 5), (DataInput::F64(-0.0), 4)]);
        assert_eq!(zeros.heap().len(), 2, "the signed zeros did not both seat");
        let bytes = zeros
            .serialize_to_bytes()
            .expect("zeros of opposite sign are two keys");
        let decoded =
            CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.heap().len(), 2);
        assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), bytes);
    }
}
