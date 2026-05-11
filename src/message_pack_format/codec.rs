//! `MessagePackCodec` trait — unified encode/decode contract for the
//! wire-format-aligned types in [`crate::wrapper`].

use super::Error;

/// Round-trippable MessagePack codec.
///
/// All [`crate::wrapper`] sketch / aggregator types implement this trait;
/// it is the single entry point for encode/decode against the wire format.
///
/// There is no `dyn`-safe usage — callers always know the concrete
/// type — so impls are dispatched statically (zero-cost) via
/// monomorphization.
pub trait MessagePackCodec: Sized {
    /// Serialize `self` to MessagePack bytes matching the wire format
    /// shared with `sketchlib-go`.
    fn to_msgpack(&self) -> Result<Vec<u8>, Error>;

    /// Deserialize a MessagePack byte slice into `Self`.
    fn from_msgpack(bytes: &[u8]) -> Result<Self, Error>;
}
