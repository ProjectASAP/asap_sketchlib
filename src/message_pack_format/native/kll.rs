//! Native MessagePack codec impl for [`crate::sketches::kll::Kll`].

use serde::{Deserialize, Serialize};

use crate::common::numerical::NumericalValue;
use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::kll::{Kll, KllWireItem};

impl<T> MessagePackCodec for Kll<T>
where
    T: NumericalValue + KllWireItem + Serialize + for<'de> Deserialize<'de>,
{
    fn to_msgpack(&self) -> Result<Vec<u8>, Error> {
        Ok(self.to_asapv1()?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_asapv1(bytes)?)
    }
}
