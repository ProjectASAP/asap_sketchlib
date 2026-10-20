//! Native MessagePack codec impl for [`crate::sketches::countminsketch::CountMinSketch`].
//!
//! Only the canonical wire configs — i32/i64/f64 counters (`CmsWireCounter`)
//! with a fast/regular mode (`CmsWireMode`) — are serializable. Exotic in-memory
//! counters (i128/…) must be converted to a wire type first.

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countminsketch::{CmsWireCounter, CmsWireMode, CountMinSketch};
use crate::{HashProfile, SketchHasher, Vector2D};

impl<T, Mode, H> MessagePackCodec for CountMinSketch<Vector2D<T>, Mode, H>
where
    // `AddAssign` is required for `Vector2D<T>: MatrixStorage`.
    T: CmsWireCounter + std::ops::AddAssign + Serialize + for<'de> Deserialize<'de>,
    Mode: CmsWireMode,
    H: SketchHasher + HashProfile,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
