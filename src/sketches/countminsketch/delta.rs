//! The Count-Min delta, ASAPv1 kind `0x02 0x80` (`docs/asapv1_wire_format.md`
//! §3.23). Its metadata is the state's (§3.2); its payload is
//! `[indices, deltas]`.

use std::marker::PhantomData;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::kind::COUNT_MIN_DELTA;
use crate::message_pack_format::kind;
use crate::message_pack_format::admission_rate;
use crate::message_pack_format::delta::{add_cells, check_cells};
use crate::message_pack_format::envelope;
use crate::message_pack_format::wire_key::exact_vec;
use crate::{DefaultXxHasher, HashProfile, RegularPath, SketchHasher, Vector2D};

use super::CountMinSketch;
use super::wire::{CmsWireCounter, CmsWireMode, check_geometry, decode_metadata, encode_metadata};


/// A change to a `rows x cols` [`CountMinSketch`] state: each value of `deltas` is
/// added to the cell its position names in `indices`, a row-major index
/// `row * cols + col`. Indices are strictly ascending and below `rows * cols`,
/// and no delta is zero.
#[derive(Clone, Debug)]
pub struct CountMinSketchDelta<T = i32, Mode = RegularPath, H = DefaultXxHasher> {
    rows: usize,
    cols: usize,
    indices: Vec<u64>,
    deltas: Vec<T>,
    admission_rate: f64,
    _config: PhantomData<(Mode, H)>,
}

/// The delta payload: `[indices, deltas]`.
#[derive(Serialize, Deserialize)]
struct CmsDeltaPayload<T> {
    indices: Vec<u64>,
    #[serde(
        deserialize_with = "exact_vec",
        bound(deserialize = "T: Deserialize<'de>")
    )]
    deltas: Vec<T>,
}

impl<T, Mode, H> CountMinSketchDelta<T, Mode, H>
where
    T: CmsWireCounter + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
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
            .map_err(|e| format!("CMS delta: {e}"))?;
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
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = encode_metadata::<T, Mode, H>(self.rows, self.cols, self.admission_rate)?;
        let payload = rmp_serde::to_vec(&CmsDeltaPayload {
            indices: self.indices.clone(),
            deltas: self.deltas.clone(),
        })?;
        Ok(envelope::encode(kind::COUNT_MIN_DELTA, &metadata, &payload))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::COUNT_MIN_DELTA {
            return Err(RmpDecodeError::Uncategorized(format!(
                "CMS delta kind_id mismatch: stored {kind_id:?}, expected {COUNT_MIN_DELTA:?}"
            )));
        }
        let (rows, cols, admission_rate) = decode_metadata::<T, Mode, H>(metadata)?;
        let p: CmsDeltaPayload<T> = envelope::decode_block(payload)?;
        let delta =
            Self::new(rows, cols, p.indices, p.deltas).map_err(RmpDecodeError::Uncategorized)?;
        Ok(delta.with_admission_rate(admission_rate))
    }
}

impl<T, Mode, H> CountMinSketch<Vector2D<T>, Mode, H>
where
    T: CmsWireCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    /// Adds a delta to the counters. Fails, changing nothing, when the delta is
    /// for another geometry or admission rate, or an integer add overflows.
    pub fn merge_delta(&mut self, delta: &CountMinSketchDelta<T, Mode, H>) -> Result<(), String> {
        if (self.rows(), self.cols()) != (delta.rows, delta.cols) {
            return Err(format!(
                "CMS delta: metadata mismatch: a {}x{} delta for a {}x{} sketch",
                delta.rows,
                delta.cols,
                self.rows(),
                self.cols()
            ));
        }
        admission_rate::check_equal(self.admission_rate, delta.admission_rate)
            .map_err(|e| format!("CMS delta: metadata mismatch: {e}"))?;
        add_cells(self.counts.as_mut_slice(), &delta.indices, &delta.deltas)
            .map_err(|e| format!("CMS delta: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_pack_format::envelope::hex;

    type Delta = CountMinSketchDelta<i64, RegularPath>;

    /// A 2x3 `i64` delta envelope around the given payload arrays.
    fn delta_bytes(indices: &[u64], deltas: &[i64]) -> Vec<u8> {
        let metadata = encode_metadata::<i64, RegularPath, DefaultXxHasher>(2, 3, 1.0).unwrap();
        let payload = rmp_serde::to_vec(&(indices, deltas)).unwrap();
        envelope::encode(kind::COUNT_MIN_DELTA, &metadata, &payload)
    }

    #[test]
    fn decode_rejects_each_payload_rule() {
        assert!(Delta::from_asapv1(&delta_bytes(&[0, 5], &[1, -1])).is_ok());
        let rows: [(&str, &[u64], &[i64]); 4] = [
            ("unequal lengths", &[0, 1], &[1]),
            ("repeated index", &[1, 1], &[1, 1]),
            ("index rows * cols", &[6], &[1]),
            ("zero delta", &[0], &[0]),
        ];
        for (label, indices, deltas) in rows {
            assert!(
                Delta::from_asapv1(&delta_bytes(indices, deltas)).is_err(),
                "{label}"
            );
        }
    }

    #[test]
    fn decode_rejects_another_kind() {
        let delta = hex(include_str!(
            "../../../asapv1_golden/cms_i64_regular_2x3_delta.hex"
        ));
        assert!(Delta::from_asapv1(&delta).is_ok());
        let mut state_kind = delta.clone();
        state_kind[9] = 0x00;
        assert!(Delta::from_asapv1(&state_kind).is_err());
    }

    #[test]
    fn new_rejects_a_geometry_past_the_seed_list() {
        assert!(Delta::new(20, 3, vec![0], vec![1]).is_ok());
        assert!(Delta::new(21, 3, vec![0], vec![1]).is_err());
    }

    #[test]
    fn merge_delta_rejects_without_changing_the_state() {
        let delta = |indices: Vec<u64>, deltas: Vec<i32>| {
            CountMinSketchDelta::<i32, RegularPath>::new(2, 3, indices, deltas).unwrap()
        };
        let mut transposed = CountMinSketch::<Vector2D<i32>, RegularPath>::with_dimensions(3, 2);
        assert!(transposed.merge_delta(&delta(vec![0], vec![1])).is_err());
        assert_eq!(transposed.as_storage().as_slice(), &[0; 6]);

        let cells = |_, c| if c == 2 { i32::MAX } else { 0 };
        let mut full = CountMinSketch::<Vector2D<i32>, RegularPath>::from_storage(
            Vector2D::from_fn(2, 3, cells),
        );
        assert!(full.merge_delta(&delta(vec![0, 5], vec![1, 1])).is_err());
        assert_eq!(
            full.as_storage().as_slice(),
            &[0, 0, i32::MAX, 0, 0, i32::MAX]
        );

        let mut unfilled =
            CountMinSketch::<Vector2D<i32>, RegularPath>::from_storage(Vector2D::init(2, 3));
        assert!(unfilled.merge_delta(&delta(vec![0], vec![1])).is_err());
    }
}
