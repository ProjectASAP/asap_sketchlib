//! Native MessagePack codec impl for [`crate::sketches::kll_dynamic::KllDynamic`].

use serde::{Deserialize, Serialize};

use crate::common::numerical::NumericalValue;
use crate::message_pack_format::{Error, MessagePackCodec};
use crate::sketches::kll::KllWireItem;
use crate::sketches::kll_dynamic::KllDynamic;

impl<T> MessagePackCodec for KllDynamic<T>
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
