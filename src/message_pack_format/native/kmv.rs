//! Native MessagePack codec impl for [`crate::sketches::kmv::KMV`].

use crate::SketchHasher;
use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::kmv::KMV;

impl<H: SketchHasher> MessagePackCodec for KMV<H> {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
