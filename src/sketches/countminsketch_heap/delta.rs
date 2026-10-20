//! The CountMinSketchWithHeap delta, ASAPv1 kind `0x03 0x80` (`docs/asapv1_wire_format.md`
//! §3.23). Its metadata is the state's (§3.7); its payload is
//! `[indices, deltas, keys, heap_counts]`: the changed cells and the producer's
//! whole heap.

use std::marker::PhantomData;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::delta::{add_cells, check_cells};
use crate::message_pack_format::envelope;
use crate::sketches::countminsketch::{CmsWireCounter, CmsWireMode};
use crate::{
    DefaultXxHasher, HashProfile, HeapCounter, RegularPath, SketchHasher, TopKHeap, Vector2D,
};

use super::CountMinSketchWithHeap;
use super::heap_wire::{
    DeltaCells, check_geometry, decode_metadata, decode_payload, encode_metadata, encode_payload,
    heap_entries, rebuild_heap, wire_key_type,
};

/// CountMinSketchWithHeap delta kind_id: the state's `0x03 0x00` with the variant's top bit set.
const CMS_HEAP_DELTA_KIND: &[u8] = &[0x03, 0x80];

/// A change to a `rows x cols` [`CountMinSketchWithHeap`] state: each value of `deltas` is
/// added to the cell its position names in `indices`, a row-major index
/// `row * cols + col`, and `heap` replaces the state's heap. Indices are
/// strictly ascending and below `rows * cols`, and no delta is zero.
#[derive(Clone, Debug)]
pub struct CountMinSketchWithHeapDelta<
    T: HeapCounter = i64,
    Mode = RegularPath,
    H = DefaultXxHasher,
> {
    rows: usize,
    cols: usize,
    indices: Vec<u64>,
    deltas: Vec<T>,
    heap: TopKHeap<T::HeapCount>,
    _config: PhantomData<(Mode, H)>,
}

impl<T, Mode, H> CountMinSketchWithHeapDelta<T, Mode, H>
where
    T: CmsWireCounter + HeapCounter + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Builds a delta over a `rows x cols` matrix from the changed cells and the
    /// producer's heap. Fails when the geometry is not wire-eligible or the cells
    /// break the rules above.
    pub fn new(
        rows: usize,
        cols: usize,
        indices: Vec<u64>,
        deltas: Vec<T>,
        heap: TopKHeap<T::HeapCount>,
    ) -> Result<Self, String> {
        check_geometry("CMSHeap", rows, cols)?;
        check_cells(&indices, &deltas, ..rows as u64 * cols as u64)
            .map_err(|e| format!("CMSHeap delta: {e}"))?;
        Ok(Self {
            rows,
            cols,
            indices,
            deltas,
            heap,
            _config: PhantomData,
        })
    }

    /// Serializes the delta into an ASAPv1 MessagePack envelope. Fails as the
    /// state's encode does on the heap: keys that mix `OwnedDataInput` variants, a
    /// 128-bit key, or a `heap_size` past `u32`.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let entries = heap_entries(&self.heap);
        let key_type = wire_key_type(&entries)?;
        let metadata = encode_metadata::<H>(
            "CMSHeap",
            self.rows,
            self.cols,
            T::COUNTER_TYPE,
            Mode::MODE,
            self.heap.capacity(),
            key_type,
        )?;
        let cells = DeltaCells {
            indices: self.indices.clone(),
            deltas: self.deltas.clone(),
        };
        let payload = encode_payload(key_type, cells, &entries)?;
        Ok(envelope::encode(CMS_HEAP_DELTA_KIND, &metadata, &payload))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != CMS_HEAP_DELTA_KIND {
            return Err(RmpDecodeError::Uncategorized(format!(
                "CMSHeap delta kind_id mismatch: stored {kind_id:?}, expected {CMS_HEAP_DELTA_KIND:?}"
            )));
        }
        let shape = decode_metadata::<H>("CMSHeap", metadata, T::COUNTER_TYPE, Mode::MODE)?;
        let (cells, entries) = decode_payload::<DeltaCells<T>, _>(&shape.key_type, payload)?;
        let heap = rebuild_heap(shape.heap_size, entries)?;
        Self::new(shape.rows, shape.cols, cells.indices, cells.deltas, heap)
            .map_err(RmpDecodeError::Uncategorized)
    }
}

impl<T, Mode, H> CountMinSketchWithHeap<Vector2D<T>, Mode, H>
where
    T: CmsWireCounter + HeapCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Adds a delta's cells to the counters and replaces the heap with the
    /// delta's. Fails, changing nothing, when the delta is for another geometry
    /// or heap capacity, or an integer add overflows.
    pub fn merge_delta(
        &mut self,
        delta: &CountMinSketchWithHeapDelta<T, Mode, H>,
    ) -> Result<(), String> {
        let (rows, cols, heap_size) = (self.rows(), self.cols(), self.heap.capacity());
        if (rows, cols, heap_size) != (delta.rows, delta.cols, delta.heap.capacity()) {
            return Err(format!(
                "CMSHeap delta: metadata mismatch: a {}x{} delta with heap_size {} for a \
                 {rows}x{cols} sketch with heap_size {heap_size}",
                delta.rows,
                delta.cols,
                delta.heap.capacity()
            ));
        }
        add_cells(
            self.cms.as_storage_mut().as_mut_slice(),
            &delta.indices,
            &delta.deltas,
        )
        .map_err(|e| format!("CMSHeap delta: {e}"))?;
        self.heap = delta.heap.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_pack_format::envelope::hex;
    use crate::{DataInput, OwnedDataInput};

    type Delta = CountMinSketchWithHeapDelta<i64, RegularPath>;
    type Sketch = CountMinSketchWithHeap<Vector2D<i64>, RegularPath>;

    const DELTA: &str = include_str!("../../../asapv1_golden/cmsheap_i64_regular_2x3_delta.hex");

    /// A 2x3 `i64` delta envelope, heap_size 2, around the given payload arrays.
    fn delta_bytes(indices: &[u64], keys: &[u64], heap_counts: &[i64]) -> Vec<u8> {
        let metadata =
            encode_metadata::<DefaultXxHasher>("CMSHeap", 2, 3, "i64", "regular", 2, "u64")
                .unwrap();
        let deltas = vec![1i64; indices.len()];
        let payload = rmp_serde::to_vec(&(indices, deltas, keys, heap_counts)).unwrap();
        envelope::encode(CMS_HEAP_DELTA_KIND, &metadata, &payload)
    }

    #[test]
    fn decode_rejects_each_payload_rule() {
        let decodes = |bytes: Vec<u8>| Delta::from_asapv1(&bytes).is_ok();
        assert!(decodes(delta_bytes(&[0, 5], &[1, 2], &[9, 4])));
        assert!(!decodes(delta_bytes(&[6], &[1], &[9])), "index rows * cols");
        assert!(
            !decodes(delta_bytes(&[0], &[1, 2], &[4, 9])),
            "heap out of order"
        );
    }

    #[test]
    fn decode_rejects_another_kind() {
        let delta = hex(DELTA);
        assert!(Delta::from_asapv1(&delta).is_ok());
        let mut state_kind = delta.clone();
        state_kind[9] = 0x00;
        assert!(Delta::from_asapv1(&state_kind).is_err());
    }

    #[test]
    fn merge_delta_rejects_without_changing_the_state() {
        let mut heap = TopKHeap::new(2);
        heap.update(&DataInput::U64(7), 3);
        let delta =
            |heap: TopKHeap, deltas: Vec<i64>| Delta::new(2, 3, vec![0, 5], deltas, heap).unwrap();
        let cells = |_, c| if c == 2 { i64::MAX } else { 0 };
        let mut state = Sketch::from_storage(Vector2D::from_fn(2, 3, cells), 2);
        state.heap_mut().update(&DataInput::Str("kept"), 1);

        let transposed = Delta::new(3, 2, vec![0, 5], vec![1, -1], TopKHeap::new(2)).unwrap();
        let wider = delta(TopKHeap::new(3), vec![1, -1]);
        let overflowing = delta(heap, vec![1, 1]);
        for rejected in [transposed, wider, overflowing] {
            assert!(state.merge_delta(&rejected).is_err());
            assert_eq!(
                state.cms().as_storage().as_slice(),
                &[0, 0, i64::MAX, 0, 0, i64::MAX]
            );
            assert_eq!(state.heap().len(), 1);
            assert!(
                state
                    .heap()
                    .find_owned(&OwnedDataInput::String("kept".into()))
                    .is_some()
            );
        }
    }
}
