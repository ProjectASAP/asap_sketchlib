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

use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::message_pack_format::portable::delta_set_aggregator::DeltaResult;
use asap_sketchlib::message_pack_format::portable::hydra_kll::HydraKllSketchWire;
use asap_sketchlib::{DdSketch, HydraKllSketch, SetAggregator};

// ===== round-trip: every wire-format-aligned type =====

#[test]
fn dd_sketch_round_trip() {
    let mut s = DdSketch::new(0.01);
    s.update(1.0);
    s.update(10.0);
    s.update(100.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = DdSketch::from_msgpack(&bytes).expect("decode");
    // `count` is not on the wire; recover it by summing the bucket array.
    assert_eq!(restored.total_count(), 3);
}

#[test]
fn hydra_kll_sketch_round_trip() {
    let mut s = HydraKllSketch::with_seed(2, 4, 200, 0x5EED_0900);
    s.update("a", 1.0);
    s.update("a", 2.0);
    s.update("b", 3.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = HydraKllSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.rows, 2);
    assert_eq!(restored.cols, 4);
}

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

// ===== DTO-level structural sanity =====
//
// Verify that the DTO field shapes still match what `sketchlib-go`
// expects (map keys / nesting). A producer that drops a field would
// trip these.

#[test]
fn hydra_kll_wire_shape() {
    let bytes = HydraKllSketch::new(2, 3, 200).to_msgpack().unwrap();
    let restored: HydraKllSketchWire = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored.rows, 2);
    assert_eq!(restored.cols, 3);
    assert_eq!(restored.sketches.len(), 2);
    assert_eq!(restored.sketches[0].len(), 3);
}

// ===== golden-bytes placeholders: an ignored, uncovered gap =====
//
// The four tests below are **empty and ignored**. They verify nothing today.
//
// What they need is a msgpack payload produced by `sketchlib-go` and checked in
// under `tests/fixtures/msgpack/`. That fixture cannot be generated here: this
// repository has no Go toolchain and no vendored copy of the Go encoder, so any
// bytes produced locally would be this crate's own output compared against
// itself — which is precisely the thing a cross-language golden exists to rule
// out. Writing such bytes would make the tests pass while testing nothing, so
// they stay ignored until a real fixture lands.
//
// The related `asapv1_golden/*.hex` files *are* cross-checked with the Go repo
// (see `asapv1_golden/README.md`), so the ASAPv1 envelope has cross-language
// coverage; what is missing is the older msgpack facade.
//
// When the fixtures arrive, each test should:
//   1. include_bytes!("fixtures/msgpack/<type>.msgpack")
//   2. <Type>::from_msgpack(bytes) succeeds
//   3. assert specific field values match the Go producer

#[ignore = "gap: needs a sketchlib-go-produced msgpack fixture; none can be generated in this repo"]
#[test]
fn dd_sketch_decodes_go_bytes() {}

#[ignore = "gap: needs a sketchlib-go-produced msgpack fixture; none can be generated in this repo"]
#[test]
fn hydra_kll_decodes_go_bytes() {}
