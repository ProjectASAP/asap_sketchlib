//! Native MessagePack codec impl for [`crate::sketches::ddsketch::DDSketch`].

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::ddsketch::DDSketch;

impl MessagePackCodec for DDSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
