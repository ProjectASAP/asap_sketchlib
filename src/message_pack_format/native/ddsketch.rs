//! Native MessagePack codec impl for [`crate::sketches::ddsketch::DdSketch`].
//!
//! Produces and consumes the ASAPv1 envelope (kind_id `0x05 0x00`): `alpha` in
//! the metadata, the eight-field payload of the wire spec §3.8.

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::ddsketch::DdSketch;

impl MessagePackCodec for DdSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
