//! Cross-language MessagePack envelope compatibility tests.
//!
//! These tests pin down the wire format described in
//! [`asap_sketchlib::message_pack_format`] and shared with `sketchlib-go`.
//!
//! # Coverage today (round-trip only)
//!
//! For every wire-format-aligned type, encode a populated instance,
//! decode the bytes back, and assert structural equality. This catches
//! Rust-side encoder/decoder regressions but does NOT verify byte-level
//! parity with the Go implementation.
//!
//! # Future: golden-bytes fixtures
//!
//! The `tests/fixtures/msgpack/` directory is reserved for canonical
//! byte streams produced by `sketchlib-go`. When those land, each
//! `*_round_trip` test below should grow a sibling `*_decodes_go_bytes`
//! test that loads the corresponding `<type>.msgpack` file via
//! `include_bytes!` and asserts deserialize succeeds and field values
//! match the producer's expectations.

use std::collections::HashSet;

use asap_sketchlib::SetAggregator;
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::message_pack_format::portable::delta_set_aggregator::DeltaResult;

// ===== round-trip: every wire-format-aligned type =====

#[test]
fn set_aggregator_round_trip() {
    let mut s = SetAggregator::new();
    s.update("web");
    s.update("api");
    let bytes = s.to_msgpack().expect("encode");
    let restored = SetAggregator::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.values.len(), 2);
    assert!(restored.values.contains("web"));
}

#[test]
fn delta_result_round_trip() {
    let mut added = HashSet::new();
    added.insert("a".to_string());
    let mut removed = HashSet::new();
    removed.insert("b".to_string());
    let dr = DeltaResult { added, removed };
    let bytes = dr.to_msgpack().expect("encode");
    let restored = DeltaResult::from_msgpack(&bytes).expect("decode");
    assert!(restored.added.contains("a"));
    assert!(restored.removed.contains("b"));
}
