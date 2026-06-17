//! Native MessagePack codec impls for [`crate::sketches::hll`].

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::hll::{Classic, ErtlMLE, HyperLogLogHIPImpl, HyperLogLogImpl};
use crate::{HllRegisterStorage, SketchHasher};

impl<Registers, H: SketchHasher> MessagePackCodec for HyperLogLogImpl<Classic, Registers, H>
where
    Registers: HllRegisterStorage + Serialize + for<'de> Deserialize<'de>,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}

impl<Registers, H: SketchHasher> MessagePackCodec for HyperLogLogImpl<ErtlMLE, Registers, H>
where
    Registers: HllRegisterStorage + Serialize + for<'de> Deserialize<'de>,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}

impl<Registers: HllRegisterStorage> MessagePackCodec for HyperLogLogHIPImpl<Registers> {
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
