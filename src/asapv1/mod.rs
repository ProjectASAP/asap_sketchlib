//! ASAPv1 framing shared by every sketch's `to_asapv1` / `from_asapv1`.
//!
//! Each sketch serializes into one self-delimiting envelope: a sketch-agnostic
//! frame (magic, version, `kind_id`, two length prefixes) around a metadata
//! map and a payload array. The framing is in `envelope`; the `kind_id`,
//! metadata and payload are per-sketch, in the `wire.rs` beside each sketch.
//! `docs/asapv1_wire_format.md` is the spec.

pub(crate) mod admission_rate;
pub(crate) mod delta;
pub(crate) mod envelope;
pub(crate) mod kind;
pub(crate) mod wire_key;
