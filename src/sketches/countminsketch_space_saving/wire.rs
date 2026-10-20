//! ASAPv1 wire serialization for CountMinSketchWithSpaceSaving (kind_id `0x1e 0x00`,
//! `docs/asapv1_wire_format.md` §3.24). The encoding is shared with
//! CountSketchWithSpaceSaving and lives in [`super::summary_wire`].

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::message_pack_format::kind;
use crate::sketches::countminsketch::{CmsWireCounter, CmsWireMode};
use crate::{CountMinSketch, HashProfile, SketchHasher, Vector2D};

use super::CountMinSketchWithSpaceSaving;
use super::summary_wire::{self, DecodedSummaryTopK, SummaryKind};

impl<T, Mode, H> CountMinSketchWithSpaceSaving<Vector2D<T>, Mode, H>
where
    T: CmsWireCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    const KIND: SummaryKind = SummaryKind {
        name: "CountMinSketchWithSpaceSaving",
        kind_id: kind::COUNT_MIN_WITH_SPACE_SAVING,
        counter_type: T::COUNTER_TYPE,
        mode: Mode::MODE,
    };

    /// Serializes the sketch into an ASAPv1 envelope (kind_id `0x1e 0x00`). Fails
    /// when the matrix samples its updates or the state breaks a §3.24 decode
    /// rule, and on mixed-variant or 128-bit keys.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        admission_rate::check_admits_all(self.count_min_sketch.admission_rate()).map_err(|e| {
            RmpEncodeError::Syntax(format!(
                "ASAPv1 CountMinSketchWithSpaceSaving envelope: {e}"
            ))
        })?;
        summary_wire::encode::<T, H>(
            &Self::KIND,
            self.count_min_sketch.rows(),
            self.count_min_sketch.cols(),
            self.count_min_sketch.as_storage().as_slice(),
            &self.summary,
        )
    }

    /// Deserializes a sketch from an ASAPv1 envelope. The hash spec, counter
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
        Ok(CountMinSketchWithSpaceSaving {
            count_min_sketch: CountMinSketch::from_storage(storage),
            summary,
        })
    }
}
