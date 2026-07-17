//! Native MessagePack codec impl for [`crate::sketches::countsketch::Count`].

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countsketch::{Count, CountSketchCounter};
use crate::{FastPath, MatrixStorage, RegularPath, SketchHasher};

impl<S, C, H: SketchHasher> MessagePackCodec for Count<S, RegularPath, H>
where
    S: MatrixStorage<Counter = C> + Serialize + for<'de> Deserialize<'de>,
    C: CountSketchCounter,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}

impl<S, C, H: SketchHasher> MessagePackCodec for Count<S, FastPath, H>
where
    S: MatrixStorage<Counter = C> + Serialize + for<'de> Deserialize<'de>,
    C: CountSketchCounter,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
