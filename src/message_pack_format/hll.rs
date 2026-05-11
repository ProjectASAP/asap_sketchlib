//! MessagePack wire-format for [`crate::wrapper::HllSketch`].
//!
//! `HllSketch` has no independent DTO: the wrapper derives
//! `Serialize` / `Deserialize` and IS the wire format. Its public field
//! layout matches the on-the-wire shape exactly, so this module only
//! provides the [`MessagePackCodec`] impl that serializes the wrapper
//! struct directly.

use super::{Error as MsgPackError, MessagePackCodec};
use crate::wrapper::hll::HllSketch;

impl MessagePackCodec for HllSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        Ok(rmp_serde::to_vec(self)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }
}
