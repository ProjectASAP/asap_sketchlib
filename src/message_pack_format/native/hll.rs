//! Native MessagePack codec impls for [`crate::sketches::hll`].

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::hll::{HllWireVariant, HyperLogLogHipImpl, HyperLogLogImpl};
use crate::{HashProfile, HllRegisterStorage, SketchHasher};

impl<Variant: HllWireVariant, Registers: HllRegisterStorage, H: SketchHasher + HashProfile>
    MessagePackCodec for HyperLogLogImpl<Variant, Registers, H>
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}

impl<Registers: HllRegisterStorage> MessagePackCodec for HyperLogLogHipImpl<Registers> {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
