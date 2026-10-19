//! The Count Sketch delta, ASAPv1 kind `0x04 0x80` (`docs/asapv1_wire_format.md`
//! §3.23). Its metadata is the state's (§3.6); its payload is
//! `[indices, deltas]`.

use std::marker::PhantomData;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::message_pack_format::delta::{add_cells, check_cells};
use crate::message_pack_format::envelope;
use crate::message_pack_format::wire_key::exact_vec;
use crate::{DefaultXxHasher, HashProfile, RegularPath, SketchHasher, Vector2D};

use super::wire::{CsWireCounter, CsWireMode, check_geometry, decode_metadata, encode_metadata};
use super::{Count, CountSketchCounter};

/// Count Sketch delta kind_id: the state's `0x04 0x00` with the variant's top bit set.
const CS_DELTA_KIND: &[u8] = &[0x04, 0x80];

/// A change to a `rows x cols` [`Count`] state: each value of `deltas` is added
/// to the cell its position names in `indices`, a row-major index
/// `row * cols + col`. Indices are strictly ascending and below `rows * cols`,
/// and no delta is zero.
#[derive(Clone, Debug)]
pub struct CountSketchDelta<T = i32, Mode = RegularPath, H = DefaultXxHasher> {
    rows: usize,
    cols: usize,
    indices: Vec<u64>,
    deltas: Vec<T>,
    admission_rate: f64,
    _config: PhantomData<(Mode, H)>,
}

/// The delta payload: `[indices, deltas]`.
#[derive(Serialize, Deserialize)]
struct CsDeltaPayload<T> {
    indices: Vec<u64>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "T: Deserialize<'de>")
    )]
    deltas: Vec<T>,
}

impl<T, Mode, H> CountSketchDelta<T, Mode, H>
where
    T: CsWireCounter + Serialize + for<'de> Deserialize<'de>,
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Builds a delta over a `rows x cols` matrix from the changed cells. Fails
    /// when the geometry is not wire-eligible or the cells break the rules above.
    pub fn new(
        rows: usize,
        cols: usize,
        indices: Vec<u64>,
        deltas: Vec<T>,
    ) -> Result<Self, String> {
        check_geometry(rows, cols)?;
        check_cells(&indices, &deltas, ..rows as u64 * cols as u64)
            .map_err(|e| format!("Count Sketch delta: {e}"))?;
        Ok(Self {
            rows,
            cols,
            indices,
            deltas,
            admission_rate: admission_rate::admit_all(),
            _config: PhantomData,
        })
    }

    /// Sets the admission rate of the state this delta changes; `1` unless set.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// Serializes the delta into an ASAPv1 MessagePack envelope.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = encode_metadata::<T, Mode, H>(self.rows, self.cols, self.admission_rate)?;
        let payload = rmp_serde::to_vec(&CsDeltaPayload {
            indices: self.indices.clone(),
            deltas: self.deltas.clone(),
        })?;
        Ok(envelope::encode(CS_DELTA_KIND, &metadata, &payload))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != CS_DELTA_KIND {
            return Err(RmpDecodeError::Uncategorized(format!(
                "Count Sketch delta kind_id mismatch: stored {kind_id:?}, expected {CS_DELTA_KIND:?}"
            )));
        }
        let (rows, cols, admission_rate) = decode_metadata::<T, Mode, H>(metadata)?;
        let p: CsDeltaPayload<T> = envelope::decode_block(payload)?;
        let delta =
            Self::new(rows, cols, p.indices, p.deltas).map_err(RmpDecodeError::Uncategorized)?;
        Ok(delta.with_admission_rate(admission_rate))
    }
}

impl<T, Mode, H> Count<Vector2D<T>, Mode, H>
where
    T: CsWireCounter + CountSketchCounter,
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Adds a delta to the counters. Fails, changing nothing, when the delta is
    /// for another geometry or admission rate, or an integer add overflows.
    pub fn merge_delta(&mut self, delta: &CountSketchDelta<T, Mode, H>) -> Result<(), String> {
        if (self.rows(), self.cols()) != (delta.rows, delta.cols) {
            return Err(format!(
                "Count Sketch delta: metadata mismatch: a {}x{} delta for a {}x{} sketch",
                delta.rows,
                delta.cols,
                self.rows(),
                self.cols()
            ));
        }
        admission_rate::check_equal(self.admission_rate, delta.admission_rate)
            .map_err(|e| format!("Count Sketch delta: metadata mismatch: {e}"))?;
        add_cells(self.counts.as_mut_slice(), &delta.indices, &delta.deltas)
            .map_err(|e| format!("Count Sketch delta: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_pack_format::envelope::hex;

    type Delta = CountSketchDelta<i64, RegularPath>;

    /// A 2x4 `i64` delta envelope around the given payload arrays.
    fn delta_bytes(indices: &[u64], deltas: &[i64]) -> Vec<u8> {
        let metadata = encode_metadata::<i64, RegularPath, DefaultXxHasher>(2, 4, 1.0).unwrap();
        let payload = rmp_serde::to_vec(&(indices, deltas)).unwrap();
        envelope::encode(CS_DELTA_KIND, &metadata, &payload)
    }

    #[test]
    fn decode_rejects_an_index_past_the_matrix() {
        assert!(Delta::deserialize_from_bytes(&delta_bytes(&[7], &[-1])).is_ok());
        assert!(Delta::deserialize_from_bytes(&delta_bytes(&[8], &[-1])).is_err());
    }

    #[test]
    fn decode_rejects_another_kind() {
        let delta = hex(include_str!(
            "../../../asapv1_golden/cs_i64_regular_2x4_delta.hex"
        ));
        assert!(Delta::deserialize_from_bytes(&delta).is_ok());
        let mut state_kind = delta.clone();
        state_kind[9] = 0x00;
        assert!(Delta::deserialize_from_bytes(&state_kind).is_err());
    }

    #[test]
    fn new_rejects_a_geometry_past_the_seed_list() {
        assert!(Delta::new(20, 4, vec![0], vec![1]).is_ok());
        assert!(Delta::new(21, 4, vec![0], vec![1]).is_err());
    }

    #[test]
    fn merge_delta_rejects_without_changing_the_state() {
        let delta = |indices: Vec<u64>, deltas: Vec<i32>| {
            CountSketchDelta::<i32, RegularPath>::new(2, 4, indices, deltas).unwrap()
        };
        let mut transposed = Count::<Vector2D<i32>, RegularPath>::with_dimensions(4, 2);
        assert!(transposed.merge_delta(&delta(vec![0], vec![1])).is_err());
        assert_eq!(transposed.as_storage().as_slice(), &[0; 8]);

        let cells = |_, c| if c == 3 { i32::MIN } else { 0 };
        let mut full =
            Count::<Vector2D<i32>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, cells));
        assert!(full.merge_delta(&delta(vec![0, 7], vec![-1, -1])).is_err());
        assert_eq!(
            full.as_storage().as_slice(),
            &[0, 0, 0, i32::MIN, 0, 0, 0, i32::MIN]
        );
    }
}
