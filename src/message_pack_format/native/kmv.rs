//! Native MessagePack codec impl for [`crate::sketches::kmv::Kmv`].
//!
//! The bytes are the ASAPv1 wire envelope (kind `0x0e 0x00`); the impl is
//! bounded on `H: HashProfile`, so a sketch built with an unprofiled hasher has
//! no codec.

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::kmv::Kmv;
use crate::{HashProfile, SketchHasher};

impl<H: SketchHasher + HashProfile> MessagePackCodec for Kmv<H> {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
