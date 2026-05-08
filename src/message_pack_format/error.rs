//! Unified error type for MessagePack encode/decode operations.

use std::error::Error as StdError;
use std::fmt;

/// Error returned by [`crate::message_pack_format::MessagePackCodec`]
/// implementations and by the `dto`-level free functions in
/// [`crate::message_pack_format`].
#[derive(Debug)]
pub enum Error {
    /// MessagePack encoding failed.
    Encode(rmp_serde::encode::Error),
    /// MessagePack decoding failed.
    Decode(rmp_serde::decode::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Encode(e) => write!(f, "MessagePack encode failed: {e}"),
            Error::Decode(e) => write!(f, "MessagePack decode failed: {e}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Encode(e) => Some(e),
            Error::Decode(e) => Some(e),
        }
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Error::Encode(value)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Error::Decode(value)
    }
}

impl Error {
    /// Extracts the underlying `rmp_serde::encode::Error`. Used by the
    /// inherent `serialize_msgpack` shims in the wrapper modules that
    /// preserve the pre-trait return type. The decode arm is unreachable
    /// because callers only invoke this on values produced by an encode
    /// path.
    pub(crate) fn into_encode(self) -> rmp_serde::encode::Error {
        match self {
            Error::Encode(e) => e,
            Error::Decode(_) => {
                unreachable!("Error::into_encode called on a decode error")
            }
        }
    }
}
