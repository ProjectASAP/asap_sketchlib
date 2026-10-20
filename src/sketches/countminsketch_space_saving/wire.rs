//! ASAPv1 wire serialization for CMSSpaceSaving (kind_id `0x1e 0x00`,
//! `docs/asapv1_wire_format.md` §3.24). The encoding is shared with
//! CSSpaceSaving and lives in [`super::summary_wire`].

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::sketches::countminsketch::{CmsWireCounter, CmsWireMode};
use crate::{CountMin, HashProfile, SketchHasher, Vector2D};

use super::CMSSpaceSaving;
use super::summary_wire::{self, DecodedSummaryTopK, SummaryKind};

/// CMSSpaceSaving kind_id: family `0x1e`, variant `0x00`.
pub(crate) const CMS_SPACE_SAVING_KIND: &[u8] = &[0x1e, 0x00];

impl<T, Mode, H> CMSSpaceSaving<Vector2D<T>, Mode, H>
where
    T: CmsWireCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    const KIND: SummaryKind = SummaryKind {
        name: "CMSSpaceSaving",
        kind_id: CMS_SPACE_SAVING_KIND,
        counter_type: T::COUNTER_TYPE,
        mode: Mode::MODE,
    };

    /// Serializes the sketch into an ASAPv1 envelope (kind_id `0x1e 0x00`). Fails
    /// when the matrix samples its updates or the state breaks a §3.24 decode
    /// rule, and on mixed-variant or 128-bit keys.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        admission_rate::check_admits_all(self.cms.admission_rate())
            .map_err(|e| RmpEncodeError::Syntax(format!("ASAPv1 CMSSpaceSaving envelope: {e}")))?;
        summary_wire::encode::<T, H>(
            &Self::KIND,
            self.cms.rows(),
            self.cms.cols(),
            self.cms.as_storage().as_slice(),
            &self.summary,
        )
    }

    /// Deserializes a sketch from an ASAPv1 envelope. The hash spec, counter
    /// type and mode are pinned against this target; `rows`, `cols`,
    /// `capacity` and `key_type` are echoed back.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let DecodedSummaryTopK {
            rows,
            cols,
            counts,
            summary,
        } = summary_wire::decode::<T, H>(&Self::KIND, bytes)?;
        let storage = Vector2D::from_fn(rows, cols, |r, c| counts[r * cols + c]);
        Ok(CMSSpaceSaving {
            cms: CountMin::from_storage(storage),
            summary,
        })
    }
}
