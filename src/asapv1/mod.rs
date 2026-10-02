//! ASAPv1 framing shared by every sketch's `wire.rs`.
//!
//! Each sketch serializes into one self-delimiting envelope: a sketch-agnostic
//! frame (magic, version, `kind_id`, two length prefixes) around a metadata map
//! and a payload array. `envelope` holds the frame and `wire_key` the msgpack
//! form of byte and string keys; the `kind_id`, metadata and payload are
//! per-sketch, in the `wire.rs` beside each sketch under [`crate::sketches`]
//! and [`crate::sketch_framework`]. `sketchlib-go` mirrors ASAPv1,
//! `asapv1_golden/` guards against drift, and `docs/asapv1_wire_format.md` is
//! the spec.

pub(crate) mod envelope;
pub(crate) mod wire_key;
