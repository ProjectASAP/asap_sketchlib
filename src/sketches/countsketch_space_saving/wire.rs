//! ASAPv1 wire serialization for CountSketchWithSpaceSaving (kind_id `0x1f 0x01`,
//! `docs/asapv1_wire_format.md` §3.25), through the encoding it shares with
//! CountMinSketchWithSpaceSaving.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::asapv1::admission_rate;
use crate::asapv1::kind;
use crate::sketches::countminsketch_space_saving::summary_wire::{
    self, DecodedSummaryTopK, SummaryKind,
};
use crate::sketches::countsketch::{CountSketchCounter, CsWireCounter, CsWireMode};
use crate::{CountSketch, HashProfile, SketchHasher, Vector2D};

use super::CountSketchWithSpaceSaving;

impl<T, Mode, H> CountSketchWithSpaceSaving<Vector2D<T>, Mode, H>
where
    T: CsWireCounter + CountSketchCounter + Serialize + for<'de> Deserialize<'de>,
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    const KIND: SummaryKind = SummaryKind {
        name: "CountSketchWithSpaceSaving",
        kind_id: kind::COUNT_SKETCH_WITH_SPACE_SAVING,
        retired_kind_id_with_total: kind::COUNT_SKETCH_WITH_SPACE_SAVING_WITH_TOTAL,
        counter_type: T::COUNTER_TYPE,
        mode: Mode::MODE,
    };

    /// Serializes the sketch into an ASAPv1 envelope (kind_id `0x1f 0x01`). Fails
    /// when the matrix samples its updates or the state breaks a §3.24 decode
    /// rule, and on mixed-variant or 128-bit keys.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        admission_rate::check_admits_all(self.count_sketch.admission_rate()).map_err(|e| {
            RmpEncodeError::Syntax(format!("ASAPv1 CountSketchWithSpaceSaving envelope: {e}"))
        })?;
        summary_wire::encode::<T, H>(
            &Self::KIND,
            self.count_sketch.rows(),
            self.count_sketch.cols(),
            self.count_sketch.as_storage().as_slice(),
            &self.summary,
        )
    }

    /// Deserializes a sketch from an ASAPv1 envelope: kind_id `0x1f 0x01`, or
    /// the retired `0x1f 0x00` with the summary's `total` dropped. The hash spec, counter
    /// type and mode are pinned against this target; `rows`, `cols`,
    /// `capacity` and `key_type` are echoed back.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let DecodedSummaryTopK {
            rows,
            cols,
            counts,
            summary,
        } = summary_wire::decode::<T, H>(&Self::KIND, bytes)?;
        let storage = Vector2D::from_fn(rows, cols, |r, c| counts[r * cols + c]);
        Ok(CountSketchWithSpaceSaving {
            count_sketch: CountSketch::from_storage(storage),
            summary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CountMinSketchWithSpaceSaving, CountSketchWithHeap, DataInput, DefaultXxHasher, FastPath,
        OwnedDataInput, RegularPath, SpaceSaving,
    };

    type Target = CountSketchWithSpaceSaving<Vector2D<i64>, RegularPath>;

    #[test]
    fn round_trip_keeps_matrix_summary_and_bytes() {
        let mut sketch = Target::new(3, 8, 2);
        for (key, weight) in [(1u64, 9i64), (2, 5), (3, -4), (4, 2)] {
            sketch.update_weighted(&DataInput::U64(key), weight);
        }
        let encoded = sketch.to_asapv1().expect("serialize");
        assert_eq!(&encoded[7..10], &[2u8, 0x1f, 0x01]);

        let decoded = Target::from_asapv1(&encoded).expect("decode");
        assert_eq!(
            decoded.count_sketch().as_storage().as_slice(),
            sketch.count_sketch().as_storage().as_slice()
        );
        assert_eq!(
            decoded.space_saving().emitted_entries(),
            vec![
                (&OwnedDataInput::U64(1), 9, 0),
                (&OwnedDataInput::U64(4), 7, 5)
            ]
        );
        assert_eq!(decoded.top_k(2), sketch.top_k(2));
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
    }

    #[test]
    fn f64_counters_round_trip_on_the_fast_path() {
        let mut sketch = CountSketchWithSpaceSaving::<Vector2D<f64>, FastPath>::new(3, 16, 4);
        sketch.update_weighted(&DataInput::I32(-7), 2.5);
        sketch.update_weighted(&DataInput::I32(3), 0.25);
        let encoded = sketch.to_asapv1().expect("serialize");
        let decoded = CountSketchWithSpaceSaving::<Vector2D<f64>, FastPath>::from_asapv1(&encoded)
            .expect("f64");
        assert_eq!(decoded.estimate(&DataInput::I32(-7)), 2.5);
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), encoded);
        assert!(
            CountSketchWithSpaceSaving::<Vector2D<f64>, RegularPath>::from_asapv1(&encoded)
                .is_err()
        );
        assert!(
            CountSketchWithSpaceSaving::<Vector2D<i64>, FastPath>::from_asapv1(&encoded).is_err()
        );
    }

    #[test]
    fn foreign_kind_ids_are_rejected() {
        let cms_ss = CountMinSketchWithSpaceSaving::<Vector2D<i64>, RegularPath>::new(3, 8, 2);
        let cs_heap = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(3, 8, 2);
        let count = CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(3, 8);
        for (bytes, what) in [
            (
                cms_ss.to_asapv1().expect("CountMinSketchWithSpaceSaving"),
                "CountMinSketchWithSpaceSaving",
            ),
            (
                cs_heap.to_asapv1().expect("CountSketchWithHeap"),
                "CountSketchWithHeap",
            ),
            (count.to_asapv1().expect("CountSketch"), "Count Sketch"),
        ] {
            assert!(
                Target::from_asapv1(&bytes).is_err(),
                "{what} bytes must not decode as a CountSketchWithSpaceSaving"
            );
        }
    }

    #[test]
    fn a_sampled_matrix_fails_to_serialize() {
        let sampled = Target::from_parts(
            CountSketch::with_dimensions(3, 8).with_admission_rate(0.25),
            SpaceSaving::<DefaultXxHasher>::with_capacity(2),
        );
        assert!(sampled.to_asapv1().is_err());
    }
}
