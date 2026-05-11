//! Cross-language MessagePack envelope compatibility tests.
//!
//! These tests pin down the wire format described in
//! [`asap_sketchlib::message_pack_format`] and shared with `sketchlib-go`.
//!
//! # Coverage today (round-trip only)
//!
//! For every wire-format-aligned wrapper, encode a populated instance,
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
use asap_sketchlib::message_pack_format::portable::countminsketch::CountMinSketchWire;
use asap_sketchlib::message_pack_format::portable::countminsketch_topk::{
    CountMinSketchInnerWire, CountMinSketchWithHeapWire,
};
use asap_sketchlib::message_pack_format::portable::delta_set_aggregator::DeltaResult;
use asap_sketchlib::message_pack_format::portable::hydra_kll::HydraKllSketchWire;
use asap_sketchlib::message_pack_format::portable::kll::KllSketchData;
use asap_sketchlib::wrapper::countminsketch_topk::CmsHeapItem;
use asap_sketchlib::{
    CountMinSketch, CountMinSketchWithHeap, CountSketch, DdSketch, HllSketch, HllVariant,
    HydraKllSketch, KllSketch, SetAggregator,
};

// ===== round-trip: every wrapper =====

#[test]
fn count_min_sketch_round_trip() {
    let mut s = CountMinSketch::new(3, 64);
    s.update("alpha", 1.0);
    s.update("beta", 2.0);
    s.update("alpha", 4.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = CountMinSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.rows, 3);
    assert_eq!(restored.cols, 64);
}

#[test]
fn count_min_sketch_with_heap_round_trip() {
    let mut s = CountMinSketchWithHeap::new(3, 64, 8);
    s.update("hot", 100.0);
    s.update("warm", 10.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = CountMinSketchWithHeap::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.rows, 3);
    assert_eq!(restored.cols, 64);
    assert_eq!(restored.heap_size, 8);
}

#[test]
fn count_sketch_round_trip() {
    let mut s = CountSketch::new(3, 64);
    s.update("k1", 1.0);
    s.update("k2", 2.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = CountSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.rows, 3);
    assert_eq!(restored.cols, 64);
}

#[test]
fn dd_sketch_round_trip() {
    let mut s = DdSketch::new(0.01);
    s.update(1.0);
    s.update(10.0);
    s.update(100.0);
    let bytes = s.to_msgpack().expect("encode");
    let restored = DdSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.count, 3);
}

#[test]
fn hll_sketch_round_trip() {
    let mut s = HllSketch::new(HllVariant::Regular, 8);
    s.update(b"a");
    s.update(b"b");
    s.update(b"c");
    let bytes = s.to_msgpack().expect("encode");
    let restored = HllSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.registers.len(), s.registers.len());
}

#[test]
fn kll_sketch_round_trip() {
    let mut s = KllSketch::new(200);
    for i in 0..100 {
        s.update(i as f64);
    }
    let bytes = s.to_msgpack().expect("encode");
    let restored = KllSketch::from_msgpack(&bytes).expect("decode");
    assert_eq!(restored.k, 200);
    assert_eq!(restored.count(), 100);
}

#[test]
fn hydra_kll_sketch_round_trip() {
    let mut s = HydraKllSketch::new(2, 4, 200);
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
fn count_min_wire_shape() {
    let wire = CountMinSketchWire {
        sketch: vec![vec![1.0, 2.0]; 3],
        rows: 3,
        cols: 2,
    };
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let restored: CountMinSketchWire = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored.rows, 3);
    assert_eq!(restored.cols, 2);
    assert_eq!(restored.sketch.len(), 3);
}

#[test]
fn count_min_with_heap_wire_shape() {
    let wire = CountMinSketchWithHeapWire {
        sketch: CountMinSketchInnerWire {
            sketch: vec![vec![0.0; 4]; 2],
            rows: 2,
            cols: 4,
        },
        topk_heap: vec![CmsHeapItem {
            key: "hot".to_string(),
            value: 42.0,
        }],
        heap_size: 8,
    };
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let restored: CountMinSketchWithHeapWire = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored.heap_size, 8);
    assert_eq!(restored.topk_heap.len(), 1);
    assert_eq!(restored.topk_heap[0].key, "hot");
}

#[test]
fn hydra_kll_wire_shape() {
    let wire = HydraKllSketchWire {
        rows: 2,
        cols: 3,
        sketches: vec![
            vec![
                KllSketchData {
                    k: 200,
                    sketch_bytes: vec![],
                };
                3
            ];
            2
        ],
    };
    let bytes = rmp_serde::to_vec(&wire).unwrap();
    let restored: HydraKllSketchWire = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(restored.rows, 2);
    assert_eq!(restored.cols, 3);
    assert_eq!(restored.sketches.len(), 2);
    assert_eq!(restored.sketches[0].len(), 3);
}

// ===== golden-bytes placeholders =====
//
// When `sketchlib-go` fixtures land in `tests/fixtures/msgpack/`,
// uncomment and fill these in. Each test should:
//   1. include_bytes!("fixtures/msgpack/<type>.msgpack")
//   2. <Type>::from_msgpack(bytes) succeeds
//   3. assert specific field values match the Go producer

#[ignore = "awaiting sketchlib-go fixture"]
#[test]
fn count_min_decodes_go_bytes() {
    // let bytes = include_bytes!("fixtures/msgpack/count_min.msgpack");
    // let s = CountMinSketch::from_msgpack(bytes).unwrap();
    // assert_eq!(s.rows, EXPECTED_ROWS);
}

#[ignore = "awaiting sketchlib-go fixture"]
#[test]
fn dd_sketch_decodes_go_bytes() {}

#[ignore = "awaiting sketchlib-go fixture"]
#[test]
fn hll_sketch_decodes_go_bytes() {}

#[ignore = "awaiting sketchlib-go fixture"]
#[test]
fn kll_sketch_decodes_go_bytes() {}

#[ignore = "awaiting sketchlib-go fixture"]
#[test]
fn hydra_kll_decodes_go_bytes() {}
