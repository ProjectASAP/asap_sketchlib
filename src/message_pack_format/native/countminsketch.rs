//! Native MessagePack codec impl for [`crate::sketches::countminsketch::CountMin`].

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::countminsketch::CountMin;
use crate::{MatrixStorage, SketchHasher};

impl<S, Mode, H: SketchHasher> MessagePackCodec for CountMin<S, Mode, H>
where
    S: MatrixStorage + Serialize + for<'de> Deserialize<'de>,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.serialize_to_bytes()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::deserialize_from_bytes(bytes)?)
    }
}
