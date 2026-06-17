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
    /// The leading magic-ID byte was missing or did not match the expected value.
    /// `got` is the byte that was found (or `None` if the buffer was empty).
    BadMagicId { expected: u8, got: Option<u8> },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Encode(e) => write!(f, "MessagePack encode failed: {e}"),
            Error::Decode(e) => write!(f, "MessagePack decode failed: {e}"),
            Error::BadMagicId { expected, got } => match got {
                Some(b) => write!(
                    f,
                    "MessagePack magic-ID mismatch: expected 0x{expected:02x}, got 0x{b:02x}"
                ),
                None => write!(
                    f,
                    "MessagePack magic-ID missing: expected 0x{expected:02x} but buffer is empty"
                ),
            },
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Encode(e) => Some(e),
            Error::Decode(e) => Some(e),
            Error::BadMagicId { .. } => None,
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
