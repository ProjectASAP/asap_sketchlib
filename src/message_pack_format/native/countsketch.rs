//! Native MessagePack codec impl for [`crate::sketches::countsketch::CountSketch`].
//!
//! Only the canonical wire configs — `i32`, `i64` or `f64` counters
//! (`CsWireCounter`) with a fast/regular mode (`CsWireMode`) — are
//! serializable; `i128` and non-`Vector2D` storage must be converted first.

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countsketch::{CountSketch, CountSketchCounter, CsWireCounter, CsWireMode};
use crate::{HashProfile, SketchHasher, Vector2D};

impl<T, Mode, H> MessagePackCodec for CountSketch<Vector2D<T>, Mode, H>
where
    T: CsWireCounter
        + CountSketchCounter
        + std::ops::AddAssign
        + Serialize
        + for<'de> Deserialize<'de>,
    Mode: CsWireMode,
    H: SketchHasher + HashProfile,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
