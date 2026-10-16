//! Cross-language ASAPv1 golden byte-vector tests.
//!
//! Each fixture is built from a fixed, KNOWN raw sketch state (register bytes /
//! matrix values set directly, never hashed) and asserted to serialize to the
//! exact bytes in `asapv1_golden/*.hex`. That directory is a git submodule of
//! <https://github.com/ProjectASAP/sketchlib-golden-bytes>, the fixtures every ASAPv1 implementation
//! must reproduce.
//!
//! See `asapv1_golden/README.md`.

use std::collections::HashSet;

use asap_sketchlib::sketches::CountSketchDelta;
use asap_sketchlib::sketches::hll::HllWireVariant;
use asap_sketchlib::{
    CMSHeap, CMSHeapDelta, CSHeap, CSHeapDelta, Classic, Count, CountMin, CountMinDelta,
    DeltaResult, ErtlMLE, FastPath, HHHeap, HeapCount, HeapItem, HllBucketListP14, HllSketch,
    HllVariant, HyperLogLogDelta, HyperLogLogHIPP12, HyperLogLogP12, HyperLogLogP14, KLL,
    MessagePackCodec, RegularPath, SetAggregator, Vector2D, peek_kind_id,
};

fn decode_hex(s: &str) -> Vec<u8> {
    let s = s.trim();
    assert!(s.len() % 2 == 0, "golden hex must have even length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

const GOLDEN_CLASSIC: &str = include_str!("../asapv1_golden/hll_classic_p12.hex");
const GOLDEN_ERTL: &str = include_str!("../asapv1_golden/hll_ertl_mle_p12.hex");
const GOLDEN_HIP: &str = include_str!("../asapv1_golden/hll_hip_p12.hex");
const GOLDEN_CLASSIC_P14: &str = include_str!("../asapv1_golden/hll_classic_p14.hex");
const GOLDEN_ERTL_P14: &str = include_str!("../asapv1_golden/hll_ertl_mle_p14.hex");
const GOLDEN_CLASSIC_P14_DELTA: &str = include_str!("../asapv1_golden/hll_classic_p14_delta.hex");
const GOLDEN_ERTL_P14_DELTA: &str = include_str!("../asapv1_golden/hll_ertl_mle_p14_delta.hex");
const GOLDEN_CMS_I64: &str = include_str!("../asapv1_golden/cms_i64_regular_2x3.hex");
const GOLDEN_CMS_F64: &str = include_str!("../asapv1_golden/cms_f64_fast_2x3.hex");
const GOLDEN_CMS_I64_DELTA: &str = include_str!("../asapv1_golden/cms_i64_regular_2x3_delta.hex");
const GOLDEN_CMS_F64_DELTA: &str = include_str!("../asapv1_golden/cms_f64_fast_2x3_delta.hex");
const GOLDEN_CS_REGULAR: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4.hex");
const GOLDEN_CS_FAST: &str = include_str!("../asapv1_golden/cs_i64_fast_2x4.hex");
const GOLDEN_CS_I32: &str = include_str!("../asapv1_golden/cs_i32_regular_2x4.hex");
const GOLDEN_CS_F64_FAST: &str = include_str!("../asapv1_golden/cs_f64_fast_2x4.hex");
const GOLDEN_CS_I64_DELTA: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4_delta.hex");
const GOLDEN_CS_F64_DELTA: &str = include_str!("../asapv1_golden/cs_f64_fast_2x4_delta.hex");
const GOLDEN_CMSHEAP_STR: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_strkeys.hex");
const GOLDEN_CMSHEAP_I32: &str = include_str!("../asapv1_golden/cmsheap_i32_fast_2x3_i64keys.hex");
const GOLDEN_CMSHEAP_I64_TIE: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_i64tie.hex");
const GOLDEN_CMSHEAP_STR_TIE: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_strtie.hex");
const GOLDEN_CMSHEAP_EMPTY: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_empty.hex");
const GOLDEN_CMSHEAP_F64: &str = include_str!("../asapv1_golden/cmsheap_f64_fast_2x3_f64keys.hex");
const GOLDEN_CMSHEAP_DELTA: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_delta.hex");
const GOLDEN_CSHEAP_STR: &str = include_str!("../asapv1_golden/csheap_i64_regular_2x4_strkeys.hex");
const GOLDEN_CSHEAP_F64: &str = include_str!("../asapv1_golden/csheap_f64_fast_2x4_strkeys.hex");
const GOLDEN_CSHEAP_DELTA: &str = include_str!("../asapv1_golden/csheap_i64_regular_2x4_delta.hex");
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_SET_STRINGS: &str = include_str!("../asapv1_golden/set_aggregator_strings.hex");
const GOLDEN_SET_EMPTY: &str = include_str!("../asapv1_golden/set_aggregator_empty.hex");
const GOLDEN_DELTA_STRINGS: &str = include_str!("../asapv1_golden/delta_result_strings.hex");
const GOLDEN_DELTA_EMPTY: &str = include_str!("../asapv1_golden/delta_result_empty.hex");

/// The known P12 register pattern shared by all three HLL fixtures.
fn p12_registers() -> Vec<u8> {
    let mut r = vec![0u8; 4096];
    r[0] = 1;
    r[1] = 7;
    r[100] = 42;
    r[4095] = 3;
    r
}

const I64_VALS: [[i64; 3]; 2] = [[0, 1, 127], [128, 300, 65536]];
const F64_VALS: [[f64; 3]; 2] = [[0.0, 1.5, 2.25], [3.75, 4.125, 5.0625]];

/// Count Sketch cells are **signed** (the sketch adds `±weight`), so this
/// fixture sweeps the msgpack integer widths in both directions: row 0 is
/// positive fixint / positive fixint max / uint8 / uint32, row 1 is negative
/// fixint / int8 / int16 / int32.
const CS_VALS: [[i64; 4]; 2] = [[0, 127, 128, 65536], [-1, -33, -32768, -2147483648]];

/// The f64 Count Sketch matrix: signed, fractional, and both zeros.
const CS_F64_VALS: [[f64; 4]; 2] = [[0.0, -0.0, 1.5, -2.25], [127.0, -32768.5, 0.125, -1e300]];

// ---------------------------------------------------------------------------
// HLL: build known state -> serialize == golden, and golden round-trips.
// ---------------------------------------------------------------------------

#[test]
fn hll_classic_p12_matches_golden() {
    let want = decode_hex(GOLDEN_CLASSIC);
    let regs = p12_registers();

    // Build known state and serialize.
    let got = HllSketch::from_raw(HllVariant::Regular, 12, regs.clone(), 0.0, 0.0, 0.0)
        .to_msgpack()
        .expect("serialize");
    assert_eq!(got, want, "Classic P12 bytes diverge from golden");

    // Native wire path (src/sketches/hll.rs) round-trips the golden identically.
    let native = HyperLogLogP12::<Classic>::deserialize_from_bytes(&want).expect("native decode");
    assert_eq!(native.registers_as_slice(), regs.as_slice());
    assert_eq!(native.serialize_to_bytes().expect("re-serialize"), want);

    // Portable decode round-trips to the same known state.
    let decoded = HllSketch::from_msgpack(&want).expect("decode");
    assert_eq!(decoded.registers, regs);
    assert_eq!(decoded.precision, 12);
    assert_eq!(decoded.variant, HllVariant::Regular);
}

#[test]
fn hll_ertl_mle_p12_matches_golden() {
    let want = decode_hex(GOLDEN_ERTL);
    let regs = p12_registers();

    let got = HllSketch::from_raw(HllVariant::Datafusion, 12, regs.clone(), 0.0, 0.0, 0.0)
        .to_msgpack()
        .expect("serialize");
    assert_eq!(got, want, "Ertl-MLE P12 bytes diverge from golden");

    let native = HyperLogLogP12::<ErtlMLE>::deserialize_from_bytes(&want).expect("native decode");
    assert_eq!(native.registers_as_slice(), regs.as_slice());
    assert_eq!(native.serialize_to_bytes().expect("re-serialize"), want);

    let decoded = HllSketch::from_msgpack(&want).expect("decode");
    assert_eq!(decoded.registers, regs);
    assert_eq!(decoded.variant, HllVariant::Datafusion);
}

#[test]
fn hll_hip_p12_matches_golden() {
    let want = decode_hex(GOLDEN_HIP);
    let regs = p12_registers();

    let got = HllSketch::from_raw(HllVariant::Hip, 12, regs.clone(), 1.5, 2.5, 3.0)
        .to_msgpack()
        .expect("serialize");
    assert_eq!(got, want, "HIP P12 bytes diverge from golden");

    // Native HIP wire path round-trips the golden identically.
    let native = HyperLogLogHIPP12::deserialize_from_bytes(&want).expect("native decode");
    assert_eq!(native.serialize_to_bytes().expect("re-serialize"), want);

    let decoded = HllSketch::from_msgpack(&want).expect("decode");
    assert_eq!(decoded.registers, regs);
    assert_eq!(decoded.variant, HllVariant::Hip);
    assert_eq!(decoded.hip_kxq0, 1.5);
    assert_eq!(decoded.hip_kxq1, 2.5);
    assert_eq!(decoded.hip_est, 3.0);
}

// ---------------------------------------------------------------------------
// HLL delta: build known delta -> serialize == golden, round-trips, and merged
// into the matching P14 state golden gives the expected registers.
// ---------------------------------------------------------------------------

fn assert_hll_p14_delta<Variant: HllWireVariant>(delta_hex: &str, state_hex: &str) {
    type Delta<V> = HyperLogLogDelta<V, HllBucketListP14>;
    let want = decode_hex(delta_hex);
    let indices = vec![0, 2, 200, 8192, 16383];
    let registers = vec![51, 1, 9, 7, 1];
    let delta = Delta::<Variant>::new(indices, registers).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = Delta::<Variant>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state =
        HyperLogLogP14::<Variant>::deserialize_from_bytes(&decode_hex(state_hex)).expect("state");
    state.merge_delta(&decoded);
    let mut expected = vec![0u8; 16384];
    for (index, value) in [(0, 51), (1, 7), (2, 1), (200, 9), (8192, 42), (16383, 51)] {
        expected[index] = value;
    }
    assert_eq!(state.registers_as_slice(), expected.as_slice());
}

#[test]
fn hll_classic_p14_delta_matches_golden() {
    assert_hll_p14_delta::<Classic>(GOLDEN_CLASSIC_P14_DELTA, GOLDEN_CLASSIC_P14);
}

#[test]
fn hll_ertl_mle_p14_delta_matches_golden() {
    assert_hll_p14_delta::<ErtlMLE>(GOLDEN_ERTL_P14_DELTA, GOLDEN_ERTL_P14);
}

// ---------------------------------------------------------------------------
// Count-Min: build known matrix state -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

#[test]
fn cms_i64_regular_2x3_matches_golden() {
    let want = decode_hex(GOLDEN_CMS_I64);

    let sketch =
        CountMin::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::from_fn(2, 3, |r, c| {
            I64_VALS[r][c]
        }));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "CMS i64/regular bytes diverge from golden");

    let decoded =
        CountMin::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.rows(), 2);
    assert_eq!(decoded.cols(), 3);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn cms_f64_fast_2x3_matches_golden() {
    let want = decode_hex(GOLDEN_CMS_F64);

    let sketch =
        CountMin::<Vector2D<f64>, FastPath>::from_storage(Vector2D::from_fn(2, 3, |r, c| {
            F64_VALS[r][c]
        }));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "CMS f64/fast bytes diverge from golden");

    let decoded =
        CountMin::<Vector2D<f64>, FastPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<f64> = F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Count-Min delta: build known delta -> serialize == golden, round-trips, and
// merged into the matching state golden gives the expected state.
// ---------------------------------------------------------------------------

#[test]
fn cms_i64_regular_2x3_delta_matches_golden() {
    type Cms = CountMin<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CMS_I64_DELTA);
    let indices = vec![0, 1, 2, 4, 5];
    let deltas = vec![127, -1, -33, -32769, 4294967296];
    let delta = CountMinDelta::<i64, RegularPath>::new(2, 3, indices, deltas).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = CountMinDelta::<i64, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state = Cms::deserialize_from_bytes(&decode_hex(GOLDEN_CMS_I64)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [127, 0, 94, 128, -32469, 4295032832];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

#[test]
fn cms_f64_fast_2x3_delta_matches_golden() {
    type Cms = CountMin<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CMS_F64_DELTA);
    let indices = vec![0, 3, 5];
    let deltas = vec![2.5, -0.75, -5.0625];
    let delta = CountMinDelta::<f64, FastPath>::new(2, 3, indices, deltas).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = CountMinDelta::<f64, FastPath>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state = Cms::deserialize_from_bytes(&decode_hex(GOLDEN_CMS_F64)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [2.5, 1.5, 2.25, 3.0, 4.125, 0.0];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

// ---------------------------------------------------------------------------
// Count Sketch: build known matrix state -> serialize == golden, round-trips.
// The three integer fixtures hold the SAME matrix: the i64 pair differs only by
// `mode` and the i32 fixture only by `counter_type`, so the set pins that both
// metadata keys reach the bytes and that nothing else does.
// ---------------------------------------------------------------------------

#[test]
fn cs_i64_regular_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_CS_REGULAR);

    let sketch =
        Count::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
            CS_VALS[r][c]
        }));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(
        got, want,
        "Count Sketch i64/regular bytes diverge from golden"
    );

    let decoded =
        Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.rows(), 2);
    assert_eq!(decoded.cols(), 4);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn cs_i64_fast_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_CS_FAST);

    let sketch = Count::<Vector2D<i64>, FastPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
        CS_VALS[r][c]
    }));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Count Sketch i64/fast bytes diverge from golden");

    let decoded = Count::<Vector2D<i64>, FastPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    // Same matrix, different mode -> different bytes.
    assert_ne!(want, decode_hex(GOLDEN_CS_REGULAR));
}

#[test]
fn cs_i32_regular_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_CS_I32);

    let sketch =
        Count::<Vector2D<i32>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
            CS_VALS[r][c] as i32
        }));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(
        got, want,
        "Count Sketch i32/regular bytes diverge from golden"
    );

    let decoded =
        Count::<Vector2D<i32>, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i32> = CS_VALS.iter().flatten().map(|&v| v as i32).collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    // Same matrix, same mode, narrower counter: the fixtures differ only in the
    // metadata `counter_type`, so the payload bytes are identical.
    let wide = decode_hex(GOLDEN_CS_REGULAR);
    assert_eq!(want.len(), wide.len());
    assert_ne!(want, wide);
    assert!(
        Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).is_err(),
        "the i32 golden must not decode as an i64 sketch"
    );
}

#[test]
fn cs_f64_fast_2x4_matches_golden() {
    type Cs = Count<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CS_F64_FAST);
    let sketch = Cs::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_F64_VALS[r][c]));
    assert_eq!(sketch.serialize_to_bytes().expect("serialize"), want);

    let decoded = Cs::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<f64> = CS_F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Count Sketch delta: build known delta -> serialize == golden, round-trips,
// and merged into the matching state golden gives the expected state.
// ---------------------------------------------------------------------------

#[test]
fn cs_i64_regular_2x4_delta_matches_golden() {
    type Cs = Count<Vector2D<i64>, RegularPath>;
    type Delta = CountSketchDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CS_I64_DELTA);
    let indices = vec![0, 1, 2, 4, 6, 7];
    let deltas = vec![-32, -128, 255, -129, 65535, -2147483649];
    let delta = Delta::new(2, 4, indices, deltas).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = Delta::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state = Cs::deserialize_from_bytes(&decode_hex(GOLDEN_CS_REGULAR)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [-32, -1, 383, 65536, -130, -33, 32767, -4294967297];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

#[test]
fn cs_f64_fast_2x4_delta_matches_golden() {
    type Cs = Count<Vector2D<f64>, FastPath>;
    type Delta = CountSketchDelta<f64, FastPath>;
    let want = decode_hex(GOLDEN_CS_F64_DELTA);
    let indices = vec![1, 3, 5, 7];
    let deltas = vec![-0.75, 2.25, 0.5, 1e300];
    let delta = Delta::new(2, 4, indices, deltas).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = Delta::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state = Cs::deserialize_from_bytes(&decode_hex(GOLDEN_CS_F64_FAST)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [0.0, -0.75, 1.5, 0.0, 127.0, -32768.0, 0.125, 0.0];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

// ---------------------------------------------------------------------------
// CMSHeap / CSHeap: the matrix and the heap entries set directly -> serialize ==
// golden, round-trips. Entries are seated out of emitted order.
// ---------------------------------------------------------------------------

/// `(key, count)` pairs as string-keyed heap entries.
fn str_entries(entries: &[(&str, i64)]) -> Vec<(HeapItem, i64)> {
    entries
        .iter()
        .map(|&(key, count)| (HeapItem::String(key.to_string()), count))
        .collect()
}

/// `(key, count)` pairs as `i64`-keyed heap entries.
fn i64_entries(entries: &[(i64, i64)]) -> Vec<(HeapItem, i64)> {
    entries
        .iter()
        .map(|&(key, count)| (HeapItem::I64(key), count))
        .collect()
}

/// A heap of capacity `heap_size` holding `entries`, seated in the given order.
fn heap_of<N: HeapCount>(heap_size: usize, entries: &[(HeapItem, N)]) -> HHHeap<N> {
    let mut heap = HHHeap::new(heap_size);
    for (key, count) in entries {
        assert!(heap.update_heap_item(key, *count));
    }
    heap
}

/// Asserts `heap` has capacity `heap_size` and holds exactly `entries`.
fn assert_heap<N: HeapCount>(heap: &HHHeap<N>, heap_size: usize, entries: &[(HeapItem, N)]) {
    assert_eq!(heap.capacity(), heap_size);
    assert_eq!(heap.len(), entries.len());
    for (key, count) in entries {
        let at = heap.find_heap_item(key).expect("heap key");
        assert!(heap.heap()[at].count.order(count).is_eq(), "{key:?}");
    }
}

type CmsHeapI64 = CMSHeap<Vector2D<i64>, RegularPath>;

/// The i64 regular-path CMSHeap over the Count-Min i64 matrix with `entries`.
fn cmsheap_i64(heap_size: usize, entries: &[(HeapItem, i64)]) -> CmsHeapI64 {
    let mut sketch =
        CmsHeapI64::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]), heap_size);
    *sketch.heap_mut() = heap_of(heap_size, entries);
    sketch
}

fn check_cmsheap_i64_golden(golden: &str, heap_size: usize, entries: &[(HeapItem, i64)]) {
    let want = decode_hex(golden);
    let got = cmsheap_i64(heap_size, entries).serialize_to_bytes();
    assert_eq!(got.expect("serialize"), want);

    let decoded = CmsHeapI64::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_heap(decoded.heap(), heap_size, entries);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn cmsheap_i64_regular_2x3_strkeys_matches_golden() {
    let entries = [("cold", 1), ("warm", 300), ("hot", 65536), ("mild", 300)];
    check_cmsheap_i64_golden(GOLDEN_CMSHEAP_STR, 5, &str_entries(&entries));
}

/// Signed keys tie by their two's-complement bits read unsigned.
#[test]
fn cmsheap_i64_regular_2x3_i64tie_matches_golden() {
    let entries = [(-1, 5), (1, 5), (0, 5), (i64::MIN, 5), (2, 9)];
    check_cmsheap_i64_golden(GOLDEN_CMSHEAP_I64_TIE, 5, &i64_entries(&entries));
}

/// String keys tie byte-wise, a proper prefix first.
#[test]
fn cmsheap_i64_regular_2x3_strtie_matches_golden() {
    let entries = [("b", 5), ("aa", 5), ("Z", 5), ("a", 5), ("hot", 9)];
    check_cmsheap_i64_golden(GOLDEN_CMSHEAP_STR_TIE, 5, &str_entries(&entries));
}

#[test]
fn cmsheap_i64_regular_2x3_empty_matches_golden() {
    check_cmsheap_i64_golden(GOLDEN_CMSHEAP_EMPTY, 4, &[]);
}

#[test]
fn cmsheap_i32_fast_2x3_i64keys_matches_golden() {
    type Heap = CMSHeap<Vector2D<i32>, FastPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_I32);
    let entries = i64_entries(&[(4_294_967_296, 3), (-1, 7), (-129, 7)]);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c] as i32), 3);
    *sketch.heap_mut() = heap_of(3, &entries);
    assert_eq!(sketch.serialize_to_bytes().expect("serialize"), want);

    let decoded = Heap::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i32> = I64_VALS.iter().flatten().map(|&v| v as i32).collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_heap(decoded.heap(), 3, &entries);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// `f64` heap counts order by `total_cmp`: `+0.0` before `-0.0`, the
/// infinities at the ends; the `300.25` tie falls to the key's bits.
#[test]
fn cmsheap_f64_fast_2x3_f64keys_matches_golden() {
    type Heap = CMSHeap<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_F64);
    let entries: Vec<(HeapItem, f64)> = [
        (8.0, 0.0),
        (-0.5, 300.25),
        (9.0, f64::NEG_INFINITY),
        (7.0, -0.0),
        (1.5, 300.25),
        (2.0, f64::INFINITY),
    ]
    .into_iter()
    .map(|(key, count)| (HeapItem::F64(key), count))
    .collect();
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 3, |r, c| F64_VALS[r][c]), 6);
    *sketch.heap_mut() = heap_of(6, &entries);
    assert_eq!(sketch.serialize_to_bytes().expect("serialize"), want);

    let decoded = Heap::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<f64> = F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_heap(decoded.heap(), 6, &entries);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// The delta carries `i64` keys into a `string`-keyed state: applying it adds
/// the cells and replaces the heap, `key_type` with it.
#[test]
fn cmsheap_i64_regular_2x3_delta_matches_golden() {
    type Delta = CMSHeapDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_DELTA);
    let entries = i64_entries(&[(7, 128), (-129, 128), (-1, 4_295_032_832)]);
    let indices = vec![0, 1, 2, 4, 5];
    let deltas = vec![127, -1, -33, -32769, 4294967296];
    let delta = Delta::new(2, 3, indices, deltas, heap_of(5, &entries)).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = Delta::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state =
        CmsHeapI64::deserialize_from_bytes(&decode_hex(GOLDEN_CMSHEAP_STR)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [127, 0, 94, 128, -32469, 4295032832];
    assert_eq!(state.cms().as_storage().as_slice(), expected.as_slice());
    assert_heap(state.heap(), 5, &entries);
}

#[test]
fn csheap_i64_regular_2x4_strkeys_matches_golden() {
    type Heap = CSHeap<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_STR);
    let entries = str_entries(&[
        ("gamma", -33),
        ("delta", 127),
        ("alpha", 4_294_967_296),
        ("beta", 127),
    ]);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]), 5);
    *sketch.heap_mut() = heap_of(5, &entries);
    assert_eq!(sketch.serialize_to_bytes().expect("serialize"), want);

    let decoded = Heap::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cs().as_storage().as_slice(), flat.as_slice());
    assert_heap(decoded.heap(), 5, &entries);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// `f64` heap counts are the unrounded signed median; the `-0.75` tie falls
/// to the keys.
#[test]
fn csheap_f64_fast_2x4_strkeys_matches_golden() {
    type Heap = CSHeap<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CSHEAP_F64);
    let entries: Vec<(HeapItem, f64)> = [
        ("gamma", -16384.25),
        ("beta", -0.75),
        ("alpha", 63.5625),
        ("aa", -0.75),
    ]
    .into_iter()
    .map(|(key, count)| (HeapItem::String(key.to_string()), count))
    .collect();
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_F64_VALS[r][c]), 4);
    *sketch.heap_mut() = heap_of(4, &entries);
    assert_eq!(sketch.serialize_to_bytes().expect("serialize"), want);

    let decoded = Heap::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<f64> = CS_F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cs().as_storage().as_slice(), flat.as_slice());
    assert_heap(decoded.heap(), 4, &entries);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// The delta carries `i64` keys into a `string`-keyed state: applying it adds
/// the cells and replaces the heap, `key_type` with it.
#[test]
fn csheap_i64_regular_2x4_delta_matches_golden() {
    type Delta = CSHeapDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_DELTA);
    let entries = i64_entries(&[(7, -33), (-129, -4_294_967_297), (-1, 65536)]);
    let indices = vec![0, 1, 2, 4, 6, 7];
    let deltas = vec![-32, -128, 255, -129, 65535, -2147483649];
    let delta = Delta::new(2, 4, indices, deltas, heap_of(5, &entries)).expect("valid");
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = Delta::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let mut state = CSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&decode_hex(
        GOLDEN_CSHEAP_STR,
    ))
    .expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [-32, -1, 383, 65536, -130, -33, 32767, -4294967297];
    assert_eq!(state.cs().as_storage().as_slice(), expected.as_slice());
    assert_heap(state.heap(), 5, &entries);
}

// ---------------------------------------------------------------------------
// KLL: build known state (k=200, seed 42, integers 1..=50 — below the level-0
// capacity, so no compaction fires and the retained set is deterministic) ->
// serialize == golden, and golden round-trips. Matches the deterministic
// scenario the proto parity test uses (sketchlib-go's KLLSketch over the same
// input), so the coin state (42) lines up cross-language.
// ---------------------------------------------------------------------------

/// A k=200 KLL over `1..=50` with a fixed compaction seed: fully deterministic.
fn kll_1to50<T: From<u8> + asap_sketchlib::NumericalValue>(seed: u64) -> KLL<T> {
    let mut sketch = KLL::<T>::init_kll_with_seed(200, seed);
    for v in 1..=50u8 {
        sketch.update(&T::from(v));
    }
    sketch
}

#[test]
fn kll_f64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_F64);

    let sketch = kll_1to50::<f64>(42);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "KLL f64 bytes diverge from golden");

    // Golden round-trips: decode, and re-encode is byte-identical.
    let decoded = KLL::<f64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
}

#[test]
fn kll_i64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_I64);

    let sketch = kll_1to50::<i64>(42);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "KLL i64 bytes diverge from golden");

    let decoded = KLL::<i64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
}

// ---------------------------------------------------------------------------
// SetAggregator and its delta: set the members directly -> serialize ==
// golden, and golden round-trips.
// ---------------------------------------------------------------------------

fn set_of(members: &[&str]) -> HashSet<String> {
    members.iter().map(|m| m.to_string()).collect()
}

fn set_aggregator_matches(golden: &str, members: &[&str]) {
    let want = decode_hex(golden);
    let set = SetAggregator {
        values: set_of(members),
    };
    assert_eq!(set.serialize_to_bytes().expect("serialize"), want);

    let decoded = SetAggregator::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.values, set.values);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

fn delta_result_matches(golden: &str, added: &[&str], removed: &[&str]) {
    let want = decode_hex(golden);
    let delta = DeltaResult {
        added: set_of(added),
        removed: set_of(removed),
    };
    assert_eq!(delta.serialize_to_bytes().expect("serialize"), want);

    let decoded = DeltaResult::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.added, delta.added);
    assert_eq!(decoded.removed, delta.removed);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn set_aggregator_strings_matches_golden() {
    set_aggregator_matches(
        GOLDEN_SET_STRINGS,
        &[
            "\u{1f600}",
            "web",
            "\u{ff5e}",
            "abcdefghijklmnopqrstuvwxyz012345",
            "",
            "\u{4e2d}",
            "abc",
            "fixstr-max-31-bytes-0123456789a",
            "\u{e9}",
            "api",
            "ab",
        ],
    );
}

#[test]
fn set_aggregator_empty_matches_golden() {
    set_aggregator_matches(GOLDEN_SET_EMPTY, &[]);
}

#[test]
fn delta_result_strings_matches_golden() {
    delta_result_matches(
        GOLDEN_DELTA_STRINGS,
        &["\u{1f600}", "queue", "\u{e9}t\u{e9}"],
        &["\u{4e2d}", "db", "", "cache"],
    );
}

#[test]
fn delta_result_empty_matches_golden() {
    delta_result_matches(GOLDEN_DELTA_EMPTY, &[], &[]);
}

// ---------------------------------------------------------------------------
// peek_kind_id
// ---------------------------------------------------------------------------

#[test]
fn peek_kind_id_checks_magic_version_and_framing() {
    let bytes = decode_hex(GOLDEN_CMS_I64);

    let trailing = [&bytes[..], &[0xc0]].concat();
    assert_eq!(
        peek_kind_id(&trailing).expect("trailing bytes ignored"),
        [0x02, 0x00]
    );

    let mut bad_magic = bytes.clone();
    bad_magic[0] = b'X';
    assert!(peek_kind_id(&bad_magic).is_err(), "bad magic");

    let mut bad_version = bytes.clone();
    bad_version[6] = 0x02;
    assert!(peek_kind_id(&bad_version).is_err(), "unknown version");

    // Cut inside the length fields, then inside the payload.
    for len in [17, bytes.len() - 1] {
        assert!(peek_kind_id(&bytes[..len]).is_err(), "truncated to {len}");
    }
}
