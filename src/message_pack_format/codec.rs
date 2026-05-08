//! `MessagePackCodec` trait — unified encode/decode contract for the
//! wire-format-aligned types in [`crate::wrapper`].

use super::Error;

/// Round-trippable MessagePack codec.
///
/// All [`crate::wrapper`] sketch / aggregator types implement this trait.
/// The trait is the canonical entry point; the legacy inherent methods
/// `serialize_msgpack` / `deserialize_msgpack` on each wrapper are kept
/// as thin shims for backwards compatibility.
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
