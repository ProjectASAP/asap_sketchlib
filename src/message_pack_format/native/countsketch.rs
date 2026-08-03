//! Native MessagePack codec impl for [`crate::sketches::countsketch::Count`].
//!
//! Only the canonical wire config — `i64` counters with a fast/regular mode
//! (`CsWireMode`) — is serializable. Count Sketch counters must be signed and
//! negatable, which leaves `i64` as the only wire-eligible type; exotic
//! in-memory counters (i32/i128/…) must be converted first.

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countsketch::{Count, CsWireMode};
use crate::{HashProfile, SketchHasher, Vector2D};

impl<Mode, H> MessagePackCodec for Count<Vector2D<i64>, Mode, H>
where
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
