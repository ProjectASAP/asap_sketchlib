//! The msgpack form of a byte-array key.
//!
//! ASAPv1's `key_type` names the exact key variant and the decoder reads the
//! `keys` array **as** that type, so a payload whose keys are not of the
//! declared type is refused (`docs/asapv1_wire_format.md` §3.5). `bin` and
//! `str` are the one pair msgpack's own decode does not separate: a `str` value
//! deserializes happily into a byte buffer, and a `bin` value holding UTF-8
//! deserializes happily into a `String`. A `Bytes` key and a `String` key are
//! different keys, so a relabelled payload would decode into the wrong variant
//! and then answer `0` for every key it holds.
//!
//! [`WireBytes`] closes that half: it writes msgpack `bin` and accepts only
//! `bin`, so a `str`-keyed payload relabelled `"bytes"` is rejected. The other
//! half needs nothing — a `bin` value is not a `str`, and `String`'s own
//! deserializer refuses it.

use std::fmt;

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::ser::{Serialize, Serializer};

/// A byte-array key on the wire: msgpack `bin`, and `bin` only.
#[derive(Debug)]
pub(crate) struct WireBytes(pub(crate) Vec<u8>);

impl WireBytes {
    /// Takes the bytes back out.
    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for WireBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for WireBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesOnly;

        impl<'de> Visitor<'de> for BytesOnly {
            type Value = WireBytes;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a msgpack bin value")
            }

            fn visit_bytes<E: Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(WireBytes(value.to_vec()))
            }

            fn visit_byte_buf<E: Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(WireBytes(value))
            }

            fn visit_str<E: Error>(self, _: &str) -> Result<Self::Value, E> {
                Err(E::custom(
                    "ASAPv1: a bytes key must be msgpack bin, not str",
                ))
            }
        }

        deserializer.deserialize_bytes(BytesOnly)
    }
}
