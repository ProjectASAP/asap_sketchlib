//! Native MessagePack codec impl for [`crate::sketches::countsketch_heap::CountSketchWithL2Norm`].

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countsketch_heap::CountSketchWithL2Norm;
use crate::{HashProfile, SketchHasher};

impl<H: SketchHasher + HashProfile> MessagePackCodec for CountSketchWithL2Norm<H> {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
