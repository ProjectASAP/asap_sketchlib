//! Cross-language ASAPv1 golden byte-vector tests.
//!
//! Each fixture is built from a fixed, KNOWN raw sketch state (register bytes /
//! matrix values set directly, never hashed) and asserted to serialize to the
//! exact bytes in `asapv1_golden/*.hex`. That directory is a git submodule of
//! <https://github.com/ProjectASAP/sketchlib-golden-bytes>, the fixtures every ASAPv1 implementation
//! must reproduce.
//!
//! See `asapv1_golden/README.md`.

use asap_sketchlib::common::input::HydraCounter;
use asap_sketchlib::{
    CMSHeap, CSHeap, Classic, Coco, CocoBucket, Count, CountDelta, CountL2HH, CountMin, DDSketch,
    DataInput, Elastic, ErtlMLE, FastPath, HeapItem, HeavyBucket, HllBucketListP12,
    HllBucketListP14, HllRegisterStorage, Hydra, HyperLogLog, HyperLogLogHIPP12, HyperLogLogHIPP14,
    HyperLogLogP12, HyperLogLogP14, KLL, KLLDynamic, L2HH, RegularPath, UnivMon, Vector1D,
    Vector2D,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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
const GOLDEN_HIP_P14: &str = include_str!("../asapv1_golden/hll_hip_p14.hex");
const GOLDEN_CMS_I64: &str = include_str!("../asapv1_golden/cms_i64_regular_2x3.hex");
const GOLDEN_CMS_F64: &str = include_str!("../asapv1_golden/cms_f64_fast_2x3.hex");
const GOLDEN_CMSHEAP_STR: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_strkeys.hex");
const GOLDEN_CMSHEAP_I64: &str = include_str!("../asapv1_golden/cmsheap_i32_fast_2x3_i64keys.hex");
const GOLDEN_CMSHEAP_I64_TIE: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_i64tie.hex");
const GOLDEN_CMSHEAP_STR_TIE: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_strtie.hex");
const GOLDEN_CMSHEAP_EMPTY: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_empty.hex");
const GOLDEN_CS_REGULAR: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4.hex");
const GOLDEN_CS_FAST: &str = include_str!("../asapv1_golden/cs_i64_fast_2x4.hex");
const GOLDEN_CS_I32: &str = include_str!("../asapv1_golden/cs_i32_regular_2x4.hex");
const GOLDEN_CS_HEAP: &str = include_str!("../asapv1_golden/csheap_i64_regular_2x4_strkeys.hex");
const GOLDEN_L2HH: &str = include_str!("../asapv1_golden/count_l2hh_2x4_seed7.hex");
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_KLL_DYN_F64: &str = include_str!("../asapv1_golden/kll_dynamic_f64_k200.hex");
const GOLDEN_KLL_DYN_I64: &str = include_str!("../asapv1_golden/kll_dynamic_i64_k200.hex");
const GOLDEN_DD_POSITIVE: &str = include_str!("../asapv1_golden/ddsketch_positive_a001.hex");
const GOLDEN_DD_SIGNED: &str = include_str!("../asapv1_golden/ddsketch_signed_a001.hex");
const GOLDEN_DD_EMPTY: &str = include_str!("../asapv1_golden/ddsketch_empty_a001.hex");
const GOLDEN_HYDRA_KLL: &str = include_str!("../asapv1_golden/hydra_kll_2x2_k200.hex");
const GOLDEN_HYDRA_CM: &str = include_str!("../asapv1_golden/hydra_cm_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_CS: &str = include_str!("../asapv1_golden/hydra_cs_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_HLL: &str = include_str!("../asapv1_golden/hydra_hll_1x2_p14.hex");
const GOLDEN_HYDRA_UNIVMON: &str = include_str!("../asapv1_golden/hydra_univmon_1x2.hex");
const GOLDEN_UNIVMON_STR: &str = include_str!("../asapv1_golden/univmon_str_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_I64: &str = include_str!("../asapv1_golden/univmon_i64_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_EMPTY: &str = include_str!("../asapv1_golden/univmon_empty_l3_2x4_h5.hex");
const GOLDEN_COCO: &str = include_str!("../asapv1_golden/coco_3x7.hex");
const GOLDEN_ELASTIC: &str = include_str!("../asapv1_golden/elastic_4b_2x4.hex");
const GOLDEN_ELASTIC_STALE: &str = include_str!("../asapv1_golden/elastic_4b_2x4_stale.hex");

/// The known P12 register pattern shared by all three HLL fixtures.
fn p12_registers() -> Vec<u8> {
    let mut r = vec![0u8; 4096];
    r[0] = 1;
    r[1] = 7;
    r[100] = 42;
    r[4095] = 3;
    r
}

fn p12_storage() -> HllBucketListP12 {
    let mut storage = HllBucketListP12::default();
    storage.as_mut_slice().copy_from_slice(&p12_registers());
    storage
}

/// The known P14 register pattern: the first, a middle and the last index,
/// up to 51, the largest rank a P14 register holds.
fn p14_registers() -> Vec<u8> {
    let mut r = vec![0u8; 16384];
    r[0] = 1;
    r[1] = 7;
    r[8192] = 42;
    r[16383] = 51;
    r
}

fn p14_storage() -> HllBucketListP14 {
    let mut storage = HllBucketListP14::default();
    storage.as_mut_slice().copy_from_slice(&p14_registers());
    storage
}

const I64_VALS: [[i64; 3]; 2] = [[0, 1, 127], [128, 300, 65536]];
const F64_VALS: [[f64; 3]; 2] = [[0.0, 1.5, 2.25], [3.75, 4.125, 5.0625]];

/// Count Sketch cells are **signed** (the sketch adds `±weight`), so this
/// fixture sweeps the msgpack integer widths in both directions: row 0 is
/// positive fixint / positive fixint max / uint8 / uint32, row 1 is negative
/// fixint / int8 / int16 / int32.
const CS_VALS: [[i64; 4]; 2] = [[0, 127, 128, 65536], [-1, -33, -32768, -2147483648]];

// ---------------------------------------------------------------------------
// HLL: build known state -> serialize == golden, and golden round-trips.
// ---------------------------------------------------------------------------

#[test]
fn hll_classic_p12_matches_golden() {
    let want = decode_hex(GOLDEN_CLASSIC);

    let sketch = HyperLogLogP12::<Classic>::from_storage(p12_storage());
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Classic P12 bytes diverge from golden");

    let decoded = HyperLogLogP12::<Classic>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.registers_as_slice(), p12_registers().as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hll_ertl_mle_p12_matches_golden() {
    let want = decode_hex(GOLDEN_ERTL);

    let sketch = HyperLogLogP12::<ErtlMLE>::from_storage(p12_storage());
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Ertl-MLE P12 bytes diverge from golden");

    let decoded = HyperLogLogP12::<ErtlMLE>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.registers_as_slice(), p12_registers().as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hll_hip_p12_matches_golden() {
    let want = decode_hex(GOLDEN_HIP);

    let sketch = HyperLogLogHIPP12::from_storage(p12_storage(), 1.5, 2.5, 3.0);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "HIP P12 bytes diverge from golden");

    // `Debug` prints the registers and the three HIP scalars exactly.
    let decoded = HyperLogLogHIPP12::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(format!("{decoded:?}"), format!("{sketch:?}"));
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hll_classic_p14_matches_golden() {
    let want = decode_hex(GOLDEN_CLASSIC_P14);

    let sketch = HyperLogLogP14::<Classic>::from_storage(p14_storage());
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Classic P14 bytes diverge from golden");

    let decoded = HyperLogLogP14::<Classic>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.registers_as_slice(), p14_registers().as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hll_ertl_mle_p14_matches_golden() {
    let want = decode_hex(GOLDEN_ERTL_P14);

    let sketch = HyperLogLogP14::<ErtlMLE>::from_storage(p14_storage());
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Ertl-MLE P14 bytes diverge from golden");

    let decoded = HyperLogLogP14::<ErtlMLE>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.registers_as_slice(), p14_registers().as_slice());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hll_hip_p14_matches_golden() {
    let want = decode_hex(GOLDEN_HIP_P14);

    let sketch = HyperLogLogHIPP14::from_storage(p14_storage(), 16380.5, 0.25, 4.125);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "HIP P14 bytes diverge from golden");

    let decoded = HyperLogLogHIPP14::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(format!("{decoded:?}"), format!("{sketch:?}"));
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
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
// CMSHeap: the Count-Min i64 matrix plus heap entries set directly. Each heap
// holds a count tie, so the fixtures pin the emitted entry order.
// ---------------------------------------------------------------------------

/// `(key, count)` entries of the string-keyed fixture, in emitted order.
const CMSHEAP_STR_ENTRIES: [(&str, i64); 4] =
    [("hot", 65536), ("mild", 300), ("warm", 300), ("cold", 1)];

/// `(key, count)` entries of the i64-keyed fixture, in emitted order.
const CMSHEAP_I64_ENTRIES: [(i64, i64); 3] = [(-129, 7), (-1, 7), (4_294_967_296, 3)];

#[test]
fn cmsheap_i64_regular_2x3_strkeys_matches_golden() {
    let want = decode_hex(GOLDEN_CMSHEAP_STR);

    let mut sketch = CMSHeap::<Vector2D<i64>, RegularPath>::from_storage(
        Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]),
        5,
    );
    for (key, count) in [("cold", 1), ("warm", 300), ("hot", 65536), ("mild", 300)] {
        sketch
            .heap_mut()
            .update_heap_item(&HeapItem::String(key.to_string()), count);
    }
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(
        got, want,
        "CMSHeap i64/regular/string bytes diverge from golden"
    );

    let decoded =
        CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_eq!((decoded.rows(), decoded.cols()), (2, 3));
    assert_eq!(decoded.heap().capacity(), 5);
    let mut held: Vec<(String, i64)> = decoded
        .heap()
        .heap()
        .iter()
        .map(|item| match &item.key {
            HeapItem::String(k) => (k.clone(), item.count),
            other => panic!("key {other:?} is not a string"),
        })
        .collect();
    held.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let expected: Vec<(String, i64)> = CMSHEAP_STR_ENTRIES
        .iter()
        .map(|&(k, c)| (k.to_string(), c))
        .collect();
    assert_eq!(held, expected);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn cmsheap_i32_fast_2x3_i64keys_matches_golden() {
    let want = decode_hex(GOLDEN_CMSHEAP_I64);

    let mut sketch = CMSHeap::<Vector2D<i32>, FastPath>::from_storage(
        Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c] as i32),
        3,
    );
    for (key, count) in [(4_294_967_296, 3), (-1, 7), (-129, 7)] {
        sketch
            .heap_mut()
            .update_heap_item(&HeapItem::I64(key), count);
    }
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "CMSHeap i32/fast/i64 bytes diverge from golden");

    let decoded =
        CMSHeap::<Vector2D<i32>, FastPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i32> = I64_VALS.iter().flatten().map(|&v| v as i32).collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.heap().capacity(), 3);
    let mut held: Vec<(i64, i64)> = decoded
        .heap()
        .heap()
        .iter()
        .map(|item| match item.key {
            HeapItem::I64(k) => (k, item.count),
            ref other => panic!("key {other:?} is not an i64"),
        })
        .collect();
    held.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    assert_eq!(held, CMSHEAP_I64_ENTRIES);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// The i64 regular-path CMSHeap over the Count-Min i64 matrix with these
/// entries seated in the given order.
fn cmsheap_i64_regular(
    k: usize,
    entries: &[(HeapItem, i64)],
) -> CMSHeap<Vector2D<i64>, RegularPath> {
    let mut sketch = CMSHeap::<Vector2D<i64>, RegularPath>::from_storage(
        Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]),
        k,
    );
    for (key, count) in entries {
        assert!(sketch.heap_mut().update_heap_item(key, *count));
    }
    sketch
}

/// Asserts the golden's payload ends with exactly these `keys` and
/// `heap_counts` arrays, in this order.
fn assert_heap_tail<K: serde::Serialize>(golden: &[u8], keys: &[K], counts: &[i64]) {
    let mut tail = rmp_serde::to_vec(keys).expect("keys");
    tail.extend(rmp_serde::to_vec(counts).expect("counts"));
    assert!(
        golden.ends_with(&tail),
        "golden heap arrays are not in the pinned order"
    );
}

/// Encodes the entries seated forward and reversed, asserts both equal the
/// golden, then decodes it and asserts the held entries and a byte-identical
/// re-encode.
fn check_cmsheap_golden(want: &[u8], k: usize, entries: &[(HeapItem, i64)]) {
    let reversed: Vec<(HeapItem, i64)> = entries.iter().rev().cloned().collect();
    for seated in [entries, reversed.as_slice()] {
        let got = cmsheap_i64_regular(k, seated)
            .serialize_to_bytes()
            .expect("serialize");
        assert_eq!(got, want, "CMSHeap bytes diverge from golden");
    }

    let decoded =
        CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cms().as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.heap().capacity(), k);
    let mut held: Vec<(HeapItem, i64)> = decoded
        .heap()
        .heap()
        .iter()
        .map(|item| (item.key.clone(), item.count))
        .collect();
    let mut expected = entries.to_vec();
    let by_debug =
        |a: &(HeapItem, i64), b: &(HeapItem, i64)| format!("{a:?}").cmp(&format!("{b:?}"));
    held.sort_by(by_debug);
    expected.sort_by(by_debug);
    assert_eq!(held, expected);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// Signed keys tie by their two's-complement bit pattern read unsigned, so the
/// negatives follow every non-negative key.
#[test]
fn cmsheap_i64_regular_2x3_i64tie_matches_golden() {
    let want = decode_hex(GOLDEN_CMSHEAP_I64_TIE);
    let entries: Vec<(HeapItem, i64)> = [(-1, 5), (1, 5), (0, 5), (i64::MIN, 5), (2, 9)]
        .into_iter()
        .map(|(k, c)| (HeapItem::I64(k), c))
        .collect();
    check_cmsheap_golden(&want, 5, &entries);
    assert_heap_tail(&want, &[2i64, 0, 1, i64::MIN, -1], &[9, 5, 5, 5, 5]);
}

/// String keys tie byte-wise, a proper prefix first: case and length do not
/// enter except through the bytes.
#[test]
fn cmsheap_i64_regular_2x3_strtie_matches_golden() {
    let want = decode_hex(GOLDEN_CMSHEAP_STR_TIE);
    let entries: Vec<(HeapItem, i64)> = [("b", 5), ("aa", 5), ("Z", 5), ("a", 5), ("hot", 9)]
        .into_iter()
        .map(|(k, c)| (HeapItem::String(k.to_string()), c))
        .collect();
    check_cmsheap_golden(&want, 5, &entries);
    assert_heap_tail(&want, &["hot", "Z", "a", "aa", "b"], &[9, 5, 5, 5, 5]);
}

/// An empty heap has one encoding: `key_type` `"u64"` and two empty arrays.
#[test]
fn cmsheap_i64_regular_2x3_empty_matches_golden() {
    let want = decode_hex(GOLDEN_CMSHEAP_EMPTY);
    check_cmsheap_golden(&want, 4, &[]);
    assert_heap_tail::<u64>(&want, &[], &[]);
    assert!(
        CMSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want)
            .expect("decode")
            .heap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Count Sketch: build known matrix state -> serialize == golden, round-trips.
// All three fixtures hold the SAME matrix: the i64 pair differs only by `mode`
// and the i32 fixture only by `counter_type`, so the set pins that both
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

// ---------------------------------------------------------------------------
// CSHeap: the Count Sketch matrix above plus a heap whose entries are seated
// directly, never by inserting through the sketch.
// ---------------------------------------------------------------------------

/// Heap entries of the CSHeap fixture, in seating order. The counts span
/// uint64, a positive fixint tie (ordered by key on the wire) and a negative
/// int8; a CSHeap heap count is a signed median and may be negative.
const CS_HEAP_ENTRIES: [(&str, i64); 4] = [
    ("gamma", -33),
    ("delta", 127),
    ("alpha", 4_294_967_296),
    ("beta", 127),
];
const CS_HEAP_K: usize = 5;

#[test]
fn csheap_i64_regular_2x4_strkeys_matches_golden() {
    let want = decode_hex(GOLDEN_CS_HEAP);

    let mut sketch = CSHeap::<Vector2D<i64>, RegularPath>::from_storage(
        Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]),
        CS_HEAP_K,
    );
    for (key, count) in CS_HEAP_ENTRIES {
        sketch.heap_mut().update(&DataInput::Str(key), count);
    }
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "CSHeap i64/regular bytes diverge from golden");

    let decoded =
        CSHeap::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.cs().as_storage().as_slice(), flat.as_slice());
    assert_eq!((decoded.rows(), decoded.cols()), (2, 4));
    assert_eq!(decoded.heap().capacity(), CS_HEAP_K);
    assert_eq!(decoded.heap().len(), CS_HEAP_ENTRIES.len());
    for (key, count) in CS_HEAP_ENTRIES {
        let seat = decoded
            .heap()
            .find(&DataInput::Str(key))
            .unwrap_or_else(|| panic!("heap key {key:?} missing after decode"));
        assert_eq!(decoded.heap().heap()[seat].count, count, "heap key {key:?}");
    }
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    // A CSHeap golden is not a Count Sketch envelope.
    assert!(
        Count::<Vector2D<i64>, RegularPath>::deserialize_from_bytes(&want).is_err(),
        "the CSHeap golden must not decode as a plain Count Sketch"
    );
}

// ---------------------------------------------------------------------------
// KLL: build known state (k=200, seed 42, inputs below the level-0 capacity, so
// no compaction fires and the retained set is deterministic) -> serialize ==
// golden; golden decodes to that state and re-encodes byte-identically.
// ---------------------------------------------------------------------------

/// The coin of a sketch seeded with 42 that has not compacted.
const KLL_COIN_SEED_42: (u64, u64, u32) = (42, 0, 0);

/// A KLL payload `[levels, items, coin]`, read straight from the envelope.
type KllPayloadView<T> = (Vec<u32>, Vec<T>, (u64, u64, u32));

/// Splits an ASAPv1 envelope by hand and decodes its KLL payload.
fn kll_envelope<T: DeserializeOwned>(bytes: &[u8]) -> (Vec<u8>, KllPayloadView<T>) {
    assert_eq!(&bytes[..7], b"ASAPv1\x01", "magic and version");
    let kind_len = bytes[7] as usize;
    let kind_id = bytes[8..8 + kind_len].to_vec();
    let at = 8 + kind_len;
    let meta_len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
    let payload = &bytes[at + 8 + meta_len..];
    assert_eq!(payload.len(), payload_len, "payload length prefix");
    (
        kind_id,
        rmp_serde::from_slice(payload).expect("KLL payload"),
    )
}

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

    let (kind_id, (levels, items, coin)) = kll_envelope::<f64>(&want);
    assert_eq!(kind_id, [0x06, 0x00]);
    assert_eq!(levels, [0, 50]);
    assert_eq!(items, (1..=50).map(f64::from).collect::<Vec<_>>());
    assert_eq!(coin, KLL_COIN_SEED_42);

    let decoded = KLL::<f64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.wire_levels(), sketch.wire_levels());
    assert_eq!(decoded.wire_items(), sketch.wire_items());
    assert_eq!(decoded.wire_coin(), sketch.wire_coin());
    assert_eq!(decoded.count(), 50);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn kll_i64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_I64);

    let sketch = kll_1to50::<i64>(42);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "KLL i64 bytes diverge from golden");

    let (kind_id, (levels, items, coin)) = kll_envelope::<i64>(&want);
    assert_eq!(kind_id, [0x06, 0x00]);
    assert_eq!(levels, [0, 50]);
    assert_eq!(items, (1..=50).collect::<Vec<i64>>());
    assert_eq!(coin, KLL_COIN_SEED_42);

    let decoded = KLL::<i64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.wire_levels(), sketch.wire_levels());
    assert_eq!(decoded.wire_items(), sketch.wire_items());
    assert_eq!(decoded.wire_coin(), sketch.wire_coin());
    assert_eq!(decoded.count(), 50);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

/// Negative, zero, fractional and extreme f64 samples, in insertion order.
const KLL_DYN_F64_VALS: [f64; 7] = [2.5, -1.0, 0.0, 1e300, -0.125, 42.0, 3.0e-5];

/// i64 samples crossing every msgpack integer width: positive fixint / uint8 /
/// uint16 / uint32 / uint64 and negative fixint / int8 / int16 / int32 / int64.
const KLL_DYN_I64_VALS: [i64; 21] = [
    0,
    1,
    -1,
    127,
    -32,
    128,
    -33,
    255,
    -128,
    256,
    -129,
    65535,
    -32768,
    65536,
    -32769,
    4294967295,
    -2147483648,
    4294967296,
    -2147483649,
    i64::MAX,
    i64::MIN,
];

fn kll_dynamic_of<T: asap_sketchlib::NumericalValue>(values: &[T]) -> KLLDynamic<T> {
    let mut sketch = KLLDynamic::<T>::init_kll_with_seed(200, 42);
    for v in values {
        sketch.update(v);
    }
    sketch
}

#[test]
fn kll_dynamic_f64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_DYN_F64);

    let sketch = kll_dynamic_of(&KLL_DYN_F64_VALS);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "KLLDynamic f64 bytes diverge from golden");

    let (kind_id, (levels, items, coin)) = kll_envelope::<f64>(&want);
    assert_eq!(kind_id, [0x06, 0x01]);
    assert_eq!(levels, [0, KLL_DYN_F64_VALS.len() as u32]);
    assert_eq!(
        items.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        KLL_DYN_F64_VALS
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>()
    );
    assert_eq!(coin, KLL_COIN_SEED_42);

    // Encode copies the buffer, levels and coin verbatim, so equal bytes mean
    // equal state.
    let decoded = KLLDynamic::<f64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.count(), KLL_DYN_F64_VALS.len());
    assert_eq!(decoded.quantile(0.0), -1.0);
    assert_eq!(decoded.quantile(1.0), 1e300);
    for v in KLL_DYN_F64_VALS {
        assert_eq!(decoded.rank(v), sketch.rank(v), "rank({v})");
    }
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    assert!(KLL::<f64>::deserialize_from_bytes(&want).is_err());
    assert!(KLLDynamic::<f64>::deserialize_from_bytes(&decode_hex(GOLDEN_KLL_F64)).is_err());
}

#[test]
fn kll_dynamic_i64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_DYN_I64);

    let sketch = kll_dynamic_of(&KLL_DYN_I64_VALS);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "KLLDynamic i64 bytes diverge from golden");

    let (kind_id, (levels, items, coin)) = kll_envelope::<i64>(&want);
    assert_eq!(kind_id, [0x06, 0x01]);
    assert_eq!(levels, [0, KLL_DYN_I64_VALS.len() as u32]);
    assert_eq!(items, KLL_DYN_I64_VALS);
    assert_eq!(coin, KLL_COIN_SEED_42);

    let decoded = KLLDynamic::<i64>::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.count(), KLL_DYN_I64_VALS.len());
    assert_eq!(decoded.quantile(0.0), i64::MIN as f64);
    assert_eq!(decoded.quantile(1.0), i64::MAX as f64);
    for v in KLL_DYN_I64_VALS {
        assert_eq!(decoded.rank(v as f64), sketch.rank(v as f64), "rank({v})");
    }
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    assert!(KLL::<i64>::deserialize_from_bytes(&want).is_err());
    assert!(KLLDynamic::<i64>::deserialize_from_bytes(&decode_hex(GOLDEN_KLL_I64)).is_err());
}

// ---------------------------------------------------------------------------
// DDSketch: bucket stores, offsets, zero count and scalars set directly
// (DDSketch never hashes). The positive-only and empty states are metadata
// version 1; the signed state is version 2 and adds the negative store and
// zero count.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct DdStore {
    counts: Vector1D<u64>,
    offset: i32,
}

/// The field names of `DDSketch`'s serde form.
#[derive(Serialize)]
struct DdState {
    alpha: f64,
    store: DdStore,
    sum: f64,
    min: f64,
    max: f64,
    negative_store: DdStore,
    zero_count: u64,
}

/// Positive-only: `uint` widths fixint / uint8 / uint16 / uint32 / uint64 in
/// the counts and an int8 offset.
const DD_POSITIVE_COUNTS: [u64; 7] = [1, 0, 127, 128, 300, 65536, 4294967296];
const DD_POSITIVE_OFFSET: i32 = -40;
const DD_POSITIVE_SCALARS: (f64, f64, f64) = (2181071000.0, 0.453125, 0.5078125);

/// Signed: uint16 positive offset, int16 negative offset, nonzero zero count.
const DD_SIGNED_COUNTS: [u64; 3] = [3, 0, 2];
const DD_SIGNED_OFFSET: i32 = 310;
const DD_SIGNED_NEGATIVE_COUNTS: [u64; 2] = [5, 1];
const DD_SIGNED_NEGATIVE_OFFSET: i32 = -208;
const DD_SIGNED_ZERO_COUNT: u64 = 7;
const DD_SIGNED_SCALARS: (f64, f64, f64) = (2523.90625, -0.016, 515.0);

fn dd_sketch(
    (counts, offset): (&[u64], i32),
    (negative_counts, negative_offset): (&[u64], i32),
    zero_count: u64,
    (sum, min, max): (f64, f64, f64),
) -> DDSketch {
    let state = DdState {
        alpha: 0.01,
        store: DdStore {
            counts: Vector1D::from_vec(counts.to_vec()),
            offset,
        },
        sum,
        min,
        max,
        negative_store: DdStore {
            counts: Vector1D::from_vec(negative_counts.to_vec()),
            offset: negative_offset,
        },
        zero_count,
    };
    rmp_serde::from_slice(&rmp_serde::to_vec_named(&state).expect("encode state"))
        .expect("a valid DDSketch state")
}

fn dd_positive() -> DDSketch {
    dd_sketch(
        (&DD_POSITIVE_COUNTS, DD_POSITIVE_OFFSET),
        (&[], 0),
        0,
        DD_POSITIVE_SCALARS,
    )
}

fn dd_signed() -> DDSketch {
    dd_sketch(
        (&DD_SIGNED_COUNTS, DD_SIGNED_OFFSET),
        (&DD_SIGNED_NEGATIVE_COUNTS, DD_SIGNED_NEGATIVE_OFFSET),
        DD_SIGNED_ZERO_COUNT,
        DD_SIGNED_SCALARS,
    )
}

#[test]
fn ddsketch_positive_a001_matches_golden() {
    let want = decode_hex(GOLDEN_DD_POSITIVE);

    let got = dd_positive().serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "DDSketch positive bytes diverge from golden");

    let decoded = DDSketch::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.alpha(), 0.01);
    assert_eq!(decoded.store_counts(), DD_POSITIVE_COUNTS.as_slice());
    assert_eq!(decoded.store_offset(), DD_POSITIVE_OFFSET);
    assert!(decoded.negative_store_counts().is_empty());
    assert_eq!(decoded.zero_count(), 0);
    let (sum, min, max) = DD_POSITIVE_SCALARS;
    assert_eq!(decoded.sum(), sum);
    assert_eq!(decoded.min(), Some(min));
    assert_eq!(decoded.max(), Some(max));
    assert_eq!(decoded.get_count(), DD_POSITIVE_COUNTS.iter().sum::<u64>());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn ddsketch_signed_a001_matches_golden() {
    let want = decode_hex(GOLDEN_DD_SIGNED);

    let got = dd_signed().serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "DDSketch signed bytes diverge from golden");

    let decoded = DDSketch::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.alpha(), 0.01);
    assert_eq!(decoded.store_counts(), DD_SIGNED_COUNTS.as_slice());
    assert_eq!(decoded.store_offset(), DD_SIGNED_OFFSET);
    assert_eq!(
        decoded.negative_store_counts(),
        DD_SIGNED_NEGATIVE_COUNTS.as_slice()
    );
    assert_eq!(decoded.negative_store_offset(), DD_SIGNED_NEGATIVE_OFFSET);
    assert_eq!(decoded.zero_count(), DD_SIGNED_ZERO_COUNT);
    let (sum, min, max) = DD_SIGNED_SCALARS;
    assert_eq!(decoded.sum(), sum);
    assert_eq!(decoded.min(), Some(min));
    assert_eq!(decoded.max(), Some(max));
    assert_eq!(decoded.get_count(), 18);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn ddsketch_empty_a001_matches_golden() {
    let want = decode_hex(GOLDEN_DD_EMPTY);

    let state = dd_sketch(
        (&[], 0),
        (&[], 0),
        0,
        (0.0, f64::INFINITY, f64::NEG_INFINITY),
    );
    let got = state.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "DDSketch empty bytes diverge from golden");
    assert_eq!(
        DDSketch::new(0.01).serialize_to_bytes().expect("serialize"),
        want,
        "a fresh sketch is the empty state"
    );

    let decoded = DDSketch::deserialize_from_bytes(&want).expect("decode");
    assert_eq!(decoded.alpha(), 0.01);
    assert!(decoded.store_counts().is_empty());
    assert_eq!(decoded.store_offset(), 0);
    assert!(decoded.negative_store_counts().is_empty());
    assert_eq!(decoded.zero_count(), 0);
    assert_eq!(decoded.get_count(), 0);
    assert_eq!(decoded.sum(), 0.0);
    assert_eq!(decoded.min(), None);
    assert_eq!(decoded.max(), None);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Hydra: one fixture per counter variant. Each cell's state is set directly
// (matrix storage, crafted register hashes, KLL samples, UnivMon layer deltas
// and heap entries), so no subkey or value is ever hashed.
// ---------------------------------------------------------------------------

/// The key columns every Hydra fixture declares.
const HYDRA_SCHEMA: [&str; 2] = ["region", "service"];

/// A grid over [`HYDRA_SCHEMA`] whose cells are `cells`, row-major.
fn hydra_grid(
    rows: usize,
    cols: usize,
    prototype: HydraCounter,
    cells: Vec<HydraCounter>,
) -> Hydra {
    let mut hydra = Hydra::with_schema(rows, cols, HYDRA_SCHEMA, prototype).expect("valid schema");
    let mut cells = cells.into_iter();
    hydra.sketches = Vector2D::from_fn(rows, cols, |_, _| cells.next().expect("one cell each"));
    assert!(cells.next().is_none(), "one cell per grid position");
    hydra
}

/// Asserts the decoded grid's shape and schema, and that each decoded cell
/// holds the same state as `want`'s: KLL's retained state and coin, the
/// matrices, the registers, and UnivMon's own encoding.
fn assert_same_grid(got: &Hydra, want: &Hydra) {
    assert_eq!((got.row_num, got.col_num), (want.row_num, want.col_num));
    assert_eq!(got.schema(), want.schema());
    let cells = got.sketches.as_slice().iter().zip(want.sketches.as_slice());
    for (i, (g, w)) in cells.enumerate() {
        let same = match (g, w) {
            (HydraCounter::KLL(g), HydraCounter::KLL(w)) => {
                (g.wire_k(), g.wire_m(), g.wire_levels(), g.wire_coin())
                    == (w.wire_k(), w.wire_m(), w.wire_levels(), w.wire_coin())
                    && g.wire_items() == w.wire_items()
            }
            (HydraCounter::CM(g), HydraCounter::CM(w)) => {
                g.as_storage().as_slice() == w.as_storage().as_slice()
            }
            (HydraCounter::CS(g), HydraCounter::CS(w)) => {
                g.as_storage().as_slice() == w.as_storage().as_slice()
            }
            (HydraCounter::HLL(g), HydraCounter::HLL(w)) => {
                g.registers_as_slice() == w.registers_as_slice()
            }
            (HydraCounter::UNIVERSAL(g), HydraCounter::UNIVERSAL(w)) => {
                g.serialize_to_bytes().unwrap() == w.serialize_to_bytes().unwrap()
            }
            _ => false,
        };
        assert!(same, "cell {i} differs after decode");
    }
}

/// KLL cells (k=200, so no compaction fires), each with its own compaction
/// seed: `[1..=5]`, empty, `[2.5, -1.0, 0.0, 1e300, -0.125]`, `[3.0e-5]`.
fn hydra_kll_fixture() -> Hydra {
    let cell = |seed: u64, values: &[f64]| {
        let mut kll = KLL::init_kll_with_seed(200, seed);
        for v in values {
            kll.update(v);
        }
        HydraCounter::KLL(kll)
    };
    hydra_grid(
        2,
        2,
        HydraCounter::KLL(KLL::init_kll_with_seed(200, 0)),
        vec![
            cell(1, &[1.0, 2.0, 3.0, 4.0, 5.0]),
            cell(2, &[]),
            cell(3, &[2.5, -1.0, 0.0, 1e300, -0.125]),
            cell(4, &[3.0e-5]),
        ],
    )
}

/// Count-Min cells, each a 2x2 `i32` matrix, spanning positive fixint / uint8 /
/// uint16 / uint32 up to `i32::MAX`; the last cell is empty.
const HYDRA_CM_CELLS: [[[i32; 2]; 2]; 4] = [
    [[0, 1], [127, 128]],
    [[255, 256], [300, 65535]],
    [[65536, 1_000_000], [i32::MAX, 0]],
    [[0, 0], [0, 0]],
];

/// Count Sketch cells, each a 2x2 `i32` matrix, spanning negative fixint /
/// int8 / int16 / int32 down to `i32::MIN` beside positive values.
const HYDRA_CS_CELLS: [[[i32; 2]; 2]; 4] = [
    [[0, -1], [127, -32]],
    [[-33, 128], [-128, -129]],
    [[-32768, 65536], [-32769, i32::MAX]],
    [[i32::MIN, 1], [0, 0]],
];

fn hydra_cm_fixture() -> Hydra {
    let cell = |m: &[[i32; 2]; 2]| {
        HydraCounter::CM(CountMin::from_storage(Vector2D::from_fn(2, 2, |r, c| {
            m[r][c]
        })))
    };
    hydra_grid(
        2,
        2,
        HydraCounter::CM(CountMin::with_dimensions(2, 2)),
        HYDRA_CM_CELLS.iter().map(cell).collect(),
    )
}

fn hydra_cs_fixture() -> Hydra {
    let cell = |m: &[[i32; 2]; 2]| {
        HydraCounter::CS(Count::from_storage(Vector2D::from_fn(2, 2, |r, c| m[r][c])))
    };
    hydra_grid(
        2,
        2,
        HydraCounter::CS(Count::with_dimensions(2, 2)),
        HYDRA_CS_CELLS.iter().map(cell).collect(),
    )
}

/// The P14 register `(index, value)` pairs each HLL cell sets.
const HYDRA_HLL_CELLS: [&[(usize, u8)]; 2] = [
    &[(0, 1), (1, 7), (100, 42), (16383, 3)],
    &[(0, 2), (8192, 51)],
];

/// A P14 Ertl-MLE HLL whose registers are exactly `set`, zero elsewhere. Each
/// register is written by a pre-hashed value whose top 14 bits name the index
/// and whose leading zeros below them give the value.
fn hll_p14_with(set: &[(usize, u8)]) -> HyperLogLog<ErtlMLE> {
    let mut hll = HyperLogLog::<ErtlMLE>::default();
    for &(index, value) in set {
        let rank_bit = if value == 51 { 0 } else { 1u64 << (50 - value) };
        hll.insert_with_hash(((index as u64) << 50) | rank_bit);
    }
    let mut want = vec![0u8; 1 << 14];
    for &(index, value) in set {
        want[index] = value;
    }
    assert_eq!(hll.registers_as_slice(), want.as_slice());
    hll
}

fn hydra_hll_fixture() -> Hydra {
    hydra_grid(
        1,
        2,
        HydraCounter::HLL(HyperLogLog::<ErtlMLE>::default()),
        HYDRA_HLL_CELLS
            .iter()
            .map(|set| HydraCounter::HLL(hll_p14_with(set)))
            .collect(),
    )
}

/// UnivMon cells of 2 layers of 1x2 counters, heap capacity 2. Cell 0: layer 0
/// `[5, -3]`, full heap `{7: 5, 300: 2}`, incomplete; layer 1 `[0, 2]`, heap
/// `{4294967296: 2}`; total weight 7. Cell 1 is empty.
fn hydra_univmon_fixture() -> Hydra {
    let fresh = || UnivMon::init_univmon(2, 1, 2, 2);
    let mut um = fresh();
    for (layer, col, value) in [(0usize, 0u32, 5i32), (0, 1, -3), (1, 1, 2)] {
        um.l2_sketch_layers[layer].apply_delta(CountDelta { row: 0, col, value });
    }
    um.hh_layers[0].update(&DataInput::U64(7), 5);
    um.hh_layers[0].update(&DataInput::U64(300), 2);
    um.hh_layers[1].update(&DataInput::U64(4_294_967_296), 2);
    um.set_total_weight(7);
    um.mark_layer_candidates_incomplete(0);
    hydra_grid(
        1,
        2,
        HydraCounter::UNIVERSAL(fresh()),
        vec![
            HydraCounter::UNIVERSAL(um),
            HydraCounter::UNIVERSAL(fresh()),
        ],
    )
}

/// Serialize the fixture == golden; decode golden -> the fixture's state;
/// re-encode == golden.
fn assert_hydra_golden(golden: &str, fixture: Hydra, kind_id: [u8; 2]) {
    let want = decode_hex(golden);
    assert_eq!(&want[7..10], &[2, kind_id[0], kind_id[1]], "kind_id");
    let got = fixture.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Hydra {kind_id:02x?} bytes diverge from golden");

    let decoded = Hydra::deserialize_from_bytes(&want).expect("decode");
    assert_same_grid(&decoded, &fixture);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn hydra_kll_2x2_k200_matches_golden() {
    assert_hydra_golden(GOLDEN_HYDRA_KLL, hydra_kll_fixture(), [0x07, 0x00]);
}

#[test]
fn hydra_cm_2x2_counter_2x2_matches_golden() {
    assert_hydra_golden(GOLDEN_HYDRA_CM, hydra_cm_fixture(), [0x07, 0x01]);
}

#[test]
fn hydra_cs_2x2_counter_2x2_matches_golden() {
    assert_hydra_golden(GOLDEN_HYDRA_CS, hydra_cs_fixture(), [0x07, 0x02]);
}

#[test]
fn hydra_hll_1x2_p14_matches_golden() {
    assert_hydra_golden(GOLDEN_HYDRA_HLL, hydra_hll_fixture(), [0x07, 0x03]);
}

#[test]
fn hydra_univmon_1x2_matches_golden() {
    assert_hydra_golden(GOLDEN_HYDRA_UNIVMON, hydra_univmon_fixture(), [0x07, 0x04]);
}

// ---------------------------------------------------------------------------
// UnivMon: 3 layers of 2x4 CountL2HH with heap capacity 5. Cells are written
// through `L2HH::apply_delta` (which carries each row's L2 accumulator) and
// heap entries through `HHHeap::update`; no hash reaches the bytes.
// ---------------------------------------------------------------------------

/// `init_univmon`'s arguments `(heap_size, sketch_row, sketch_col, layer_size)`,
/// pairwise distinct so no length can stand in for another.
const UNIVMON_SHAPE: (usize, usize, usize, usize) = (5, 2, 4, 3);

/// The layers' counters; layer 0 holds `CS_VALS`.
const UNIVMON_LAYERS: [[[i64; 4]; 2]; 3] = [
    CS_VALS,
    [[3, -2, 0, 1], [0, 0, 5, -4]],
    [[0, 7, 0, 0], [-6, 0, 0, 0]],
];

/// Each row's sum of squared cells; both of layer 0's are uint64.
const UNIVMON_L2: [i64; 6] = [4_294_999_809, 4_611_686_019_501_130_818, 14, 41, 49, 36];

/// Entries per layer's heap.
const UNIVMON_HEAP_LENS: [u32; 3] = [3, 1, 2];

/// The heap counts in emitted order: layer by layer, each descending.
const UNIVMON_HEAP_COUNTS: [i64; 6] = [65536, 300, 128, 5, 9, 2];

/// The string fixture's keys in emitted order.
const UNIVMON_STR_KEYS: [&str; 6] = ["alpha", "beta", "delta", "gamma", "epsilon", "zeta"];

/// The i64 fixture's keys in emitted order: int64, negative fixint, int16,
/// uint8, uint64, positive fixint.
const UNIVMON_I64_KEYS: [i64; 6] = [i64::MIN, -1, -129, 128, 4_294_967_296, 7];

/// A UnivMon payload `[counts, l2, heap_lens, keys, heap_counts,
/// candidate_complete, bucket_size, update_mode]`.
type UnivMonPayloadView<K> = (
    Vec<i64>,
    Vec<i64>,
    Vec<u32>,
    Vec<K>,
    Vec<i64>,
    Vec<bool>,
    u64,
    u8,
);

/// The structural keys of the UnivMon metadata map.
#[derive(Deserialize)]
struct UnivMonShape {
    layer_size: u32,
    sketch_row: u32,
    sketch_col: u32,
    heap_size: u32,
    key_type: String,
}

/// Splits an ASAPv1 envelope by hand and decodes its UnivMon metadata and payload.
fn univmon_envelope<K: DeserializeOwned>(
    bytes: &[u8],
) -> (Vec<u8>, UnivMonShape, UnivMonPayloadView<K>) {
    assert_eq!(&bytes[..7], b"ASAPv1\x01", "magic and version");
    let kind_len = bytes[7] as usize;
    let kind_id = bytes[8..8 + kind_len].to_vec();
    let at = 8 + kind_len;
    let meta_len = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
    let metadata = &bytes[at + 8..at + 8 + meta_len];
    let payload = &bytes[at + 8 + meta_len..];
    assert_eq!(payload.len(), payload_len, "payload length prefix");
    (
        kind_id,
        rmp_serde::from_slice(metadata).expect("UnivMon metadata"),
        rmp_serde::from_slice(payload).expect("UnivMon payload"),
    )
}

/// The populated pyramid: `keys` fill the heaps in emitted order, cut by
/// `UNIVMON_HEAP_LENS`, at `UNIVMON_HEAP_COUNTS`; layer 1 is incomplete.
fn univmon_known(keys: [DataInput<'_>; 6]) -> UnivMon {
    let (heap_size, sketch_row, sketch_col, layer_size) = UNIVMON_SHAPE;
    let mut um = UnivMon::init_univmon(heap_size, sketch_row, sketch_col, layer_size);
    for (layer, cells) in UNIVMON_LAYERS.iter().enumerate() {
        for (row, values) in cells.iter().enumerate() {
            for (col, &value) in values.iter().enumerate() {
                um.l2_sketch_layers[layer].apply_delta(CountDelta {
                    row: row as u32,
                    col: col as u32,
                    value: value as i32,
                });
            }
        }
    }
    let layers = UNIVMON_HEAP_LENS
        .iter()
        .enumerate()
        .flat_map(|(layer, &len)| std::iter::repeat_n(layer, len as usize));
    for ((layer, key), count) in layers.zip(keys.iter()).zip(UNIVMON_HEAP_COUNTS) {
        um.hh_layers[layer].update(key, count);
    }
    um.set_total_weight(70_000);
    um.mark_layer_candidates_incomplete(1);
    um
}

/// Layer counters flattened in wire order.
fn univmon_counts(um: &UnivMon) -> Vec<i64> {
    (0..um.layer_size)
        .flat_map(|layer| {
            let L2HH::COUNT(counter) = &um.l2_sketch_layers[layer];
            counter.as_storage().as_slice().to_vec()
        })
        .collect()
}

/// Each layer's heap as `(key, count)`, sorted.
fn univmon_heaps(um: &UnivMon) -> Vec<Vec<(String, i64)>> {
    (0..um.layer_size)
        .map(|layer| {
            let mut entries: Vec<(String, i64)> = um.hh_layers[layer]
                .heap()
                .iter()
                .map(|item| (format!("{:?}", item.key), item.count))
                .collect();
            entries.sort();
            entries
        })
        .collect()
}

/// Decodes `want`, checks it against `source`, and checks the re-encode.
fn assert_univmon_decodes_to(want: &[u8], source: &UnivMon) {
    let decoded = UnivMon::deserialize_from_bytes(want).expect("decode");
    assert_eq!(
        (
            decoded.layer_size,
            decoded.sketch_row,
            decoded.sketch_col,
            decoded.heap_size
        ),
        (3, 2, 4, 5)
    );
    assert_eq!(decoded.bucket_size, source.bucket_size);
    assert_eq!(decoded.candidates_complete(), source.candidates_complete());
    assert_eq!(univmon_counts(&decoded), univmon_counts(source));
    for layer in 0..3 {
        assert_eq!(
            decoded.l2_sketch_layers[layer].get_l2(),
            source.l2_sketch_layers[layer].get_l2()
        );
    }
    assert_eq!(univmon_heaps(&decoded), univmon_heaps(source));
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

fn univmon_known_counts() -> Vec<i64> {
    UNIVMON_LAYERS.iter().flatten().flatten().copied().collect()
}

#[test]
fn univmon_str_l3_2x4_h5_matches_golden() {
    let want = decode_hex(GOLDEN_UNIVMON_STR);

    let sketch = univmon_known(UNIVMON_STR_KEYS.map(DataInput::Str));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "UnivMon string-key bytes diverge from golden");

    let (kind_id, shape, payload) = univmon_envelope::<String>(&want);
    assert_eq!(kind_id, [0x10, 0x00]);
    assert_eq!(
        (
            shape.layer_size,
            shape.sketch_row,
            shape.sketch_col,
            shape.heap_size
        ),
        (3, 2, 4, 5)
    );
    assert_eq!(shape.key_type, "string");
    let (counts, l2, heap_lens, keys, heap_counts, flags, bucket_size, update_mode) = payload;
    assert_eq!(counts, univmon_known_counts());
    assert_eq!(l2, UNIVMON_L2);
    assert_eq!(heap_lens, UNIVMON_HEAP_LENS);
    assert_eq!(keys, UNIVMON_STR_KEYS);
    assert_eq!(heap_counts, UNIVMON_HEAP_COUNTS);
    assert_eq!(flags, [true, false, true]);
    assert_eq!(bucket_size, 70_000);
    assert_eq!(update_mode, 1, "set_total_weight selects the standard mode");

    assert_univmon_decodes_to(&want, &sketch);
}

#[test]
fn univmon_i64_l3_2x4_h5_matches_golden() {
    let want = decode_hex(GOLDEN_UNIVMON_I64);

    let sketch = univmon_known(UNIVMON_I64_KEYS.map(DataInput::I64));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "UnivMon i64-key bytes diverge from golden");

    let (kind_id, shape, payload) = univmon_envelope::<i64>(&want);
    assert_eq!(kind_id, [0x10, 0x00]);
    assert_eq!(
        (
            shape.layer_size,
            shape.sketch_row,
            shape.sketch_col,
            shape.heap_size
        ),
        (3, 2, 4, 5)
    );
    assert_eq!(shape.key_type, "i64");
    let (counts, l2, heap_lens, keys, heap_counts, flags, bucket_size, update_mode) = payload;
    assert_eq!(counts, univmon_known_counts());
    assert_eq!(l2, UNIVMON_L2);
    assert_eq!(heap_lens, UNIVMON_HEAP_LENS);
    assert_eq!(keys, UNIVMON_I64_KEYS);
    assert_eq!(heap_counts, UNIVMON_HEAP_COUNTS);
    assert_eq!(flags, [true, false, true]);
    assert_eq!((bucket_size, update_mode), (70_000, 1));

    assert_univmon_decodes_to(&want, &sketch);
    let decoded = UnivMon::deserialize_from_bytes(&want).expect("decode");
    assert!(
        decoded.hh_layers[0]
            .heap()
            .iter()
            .any(|item| item.key == HeapItem::I64(i64::MIN))
    );

    // Same layers and heap counts, other keys: only `key_type` and `keys` differ.
    assert_ne!(want, decode_hex(GOLDEN_UNIVMON_STR));
}

#[test]
fn univmon_empty_l3_2x4_h5_matches_golden() {
    let want = decode_hex(GOLDEN_UNIVMON_EMPTY);

    let (heap_size, sketch_row, sketch_col, layer_size) = UNIVMON_SHAPE;
    let sketch = UnivMon::init_univmon(heap_size, sketch_row, sketch_col, layer_size);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "empty UnivMon bytes diverge from golden");

    let (kind_id, shape, payload) = univmon_envelope::<u64>(&want);
    assert_eq!(kind_id, [0x10, 0x00]);
    assert_eq!(
        (
            shape.layer_size,
            shape.sketch_row,
            shape.sketch_col,
            shape.heap_size
        ),
        (3, 2, 4, 5)
    );
    assert_eq!(
        shape.key_type, "u64",
        "an empty pyramid records key_type u64"
    );
    let (counts, l2, heap_lens, keys, heap_counts, flags, bucket_size, update_mode) = payload;
    assert_eq!(counts, [0; 24]);
    assert_eq!(l2, [0; 6]);
    assert_eq!(heap_lens, [0, 0, 0]);
    assert!(keys.is_empty() && heap_counts.is_empty());
    assert_eq!(flags, [true, true, true]);
    assert_eq!((bucket_size, update_mode), (0, 0));

    assert_univmon_decodes_to(&want, &sketch);
}

// ---------------------------------------------------------------------------
// Coco: build a known bucket table -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

/// The occupied buckets of the 3x7 Coco fixture as `(row, col, key, value)`;
/// every other bucket is unoccupied. Each key sits in the column its row hashes
/// it to, folded `% 7`, which the codec checks on both sides.
const COCO_CELLS: [(usize, usize, &str, u64); 15] = [
    (0, 0, "uint32-min", 65536),
    (0, 1, "uint16-max", 65535),
    (0, 2, "fixint-max", 127),
    (0, 3, "", 1),
    (0, 4, "uint8-max", 255),
    (0, 5, "emoji-😀", 5),
    (1, 0, "uint16-min", 256),
    (1, 1, "str8-min-32-bytes-0123456789abcd", 3),
    (1, 2, "uint8-min", 128),
    (1, 3, "clé-ünïcode-流量", 4),
    (1, 4, "fixstr-max-31-bytes-0123456789a", 2),
    (1, 5, "uint64-min", 4294967296),
    (1, 6, "zero", 0),
    (2, 1, "uint64-max", u64::MAX),
    (2, 5, "uint32-max", 4294967295),
];

/// The fixture's 21 buckets in row-major order.
fn coco_buckets() -> Vec<(Option<String>, u64)> {
    let mut buckets = vec![(None, 0); 21];
    for (r, c, key, val) in COCO_CELLS {
        buckets[r * 7 + c] = (Some(key.to_string()), val);
    }
    buckets
}

#[test]
fn coco_3x7_matches_golden() {
    let want = decode_hex(GOLDEN_COCO);

    let mut sketch: Coco = Coco::init_with_size(7, 3);
    for (r, c, key, val) in COCO_CELLS {
        sketch.table[r][c] = CocoBucket {
            full_key: Some(key.to_string()),
            val,
        };
    }
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Coco bytes diverge from golden");

    let decoded: Coco = Coco::deserialize_from_bytes(&want).expect("decode");
    assert_eq!((decoded.d, decoded.w), (3, 7));
    let cells: Vec<(Option<String>, u64)> = decoded
        .table
        .as_slice()
        .iter()
        .map(|b| (b.full_key.clone(), b.val))
        .collect();
    assert_eq!(cells, coco_buckets());
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Elastic: heavy buckets and light counters set directly, never inserted.
// The two fixtures hold the same state and differ only by `stale_copies`.
// ---------------------------------------------------------------------------

/// Heavy buckets `(flow_id, vote_pos, vote_neg, eviction)`: a free bucket with
/// the eviction flag, a 31-byte fixstr id, an empty id and a 32-byte str8 id;
/// the votes sweep positive fixint / uint8 / uint16 / uint32.
const ELASTIC_HEAVY: [(&str, i32, i32, bool); 4] = [
    ("", 0, 0, true),
    ("10.0.0.1:443>192.168.10.20:5123", 127, 128, false),
    ("", 1, 65535, true),
    ("10.0.0.1:443>192.168.10.20:51234", 2147483647, 256, true),
];

/// Light layer: row 0 sweeps the unsigned widths up to `i32::MAX`, row 1 the
/// signed widths down to `i32::MIN`.
const ELASTIC_LIGHT: [[i32; 4]; 2] = [[0, 255, 65536, 2147483647], [-1, -33, -32768, -2147483648]];

/// Sets the known heavy and light state on a 4-bucket, `2x4` sketch.
fn elastic_known_state(mut sketch: Elastic) -> Elastic {
    assert_eq!(sketch.heavy.len(), ELASTIC_HEAVY.len());
    for (bucket, &(id, pos, neg, eviction)) in sketch.heavy.iter_mut().zip(&ELASTIC_HEAVY) {
        *bucket = HeavyBucket {
            flow_id: id.to_string(),
            vote_pos: pos,
            vote_neg: neg,
            eviction,
        };
    }
    sketch.light = CountMin::from_storage(Vector2D::from_fn(2, 4, |r, c| ELASTIC_LIGHT[r][c]));
    sketch
}

fn assert_elastic_known_state(sketch: &Elastic) {
    let buckets: Vec<(&str, i32, i32, bool)> = sketch
        .heavy
        .iter()
        .map(|b| (b.flow_id.as_str(), b.vote_pos, b.vote_neg, b.eviction))
        .collect();
    assert_eq!(buckets, ELASTIC_HEAVY);
    assert_eq!(sketch.bktlen, 4);
    assert_eq!((sketch.light.rows(), sketch.light.cols()), (2, 4));
    let flat: Vec<i32> = ELASTIC_LIGHT.iter().flatten().copied().collect();
    assert_eq!(sketch.light.as_storage().as_slice(), flat.as_slice());
}

#[test]
fn elastic_4b_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_ELASTIC);

    let sketch = elastic_known_state(Elastic::init_with_dimensions(4, 2, 4));
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Elastic bytes diverge from golden");

    let decoded = Elastic::deserialize_from_bytes(&want).expect("decode");
    assert_elastic_known_state(&decoded);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}

#[test]
fn elastic_4b_2x4_stale_matches_golden() {
    let want = decode_hex(GOLDEN_ELASTIC_STALE);

    let mut expanded: Elastic = Elastic::init_with_dimensions(2, 2, 4);
    expanded.expand_heavy();
    let sketch = elastic_known_state(expanded);
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "Elastic stale-copies bytes diverge from golden");

    let decoded = Elastic::deserialize_from_bytes(&want).expect("decode");
    assert_elastic_known_state(&decoded);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);

    let fresh = decode_hex(GOLDEN_ELASTIC);
    assert_eq!(want.len(), fresh.len());
    let differing: Vec<usize> = (0..want.len()).filter(|&i| want[i] != fresh[i]).collect();
    assert_eq!(
        differing.len(),
        1,
        "the fixtures differ only by stale_copies"
    );
    assert_eq!((fresh[differing[0]], want[differing[0]]), (0xc2, 0xc3));
}

// ---------------------------------------------------------------------------
// CountL2HH: counts, l2 accumulators and seed index set through the sketch's
// serde form -> serialize == golden, and golden round-trips.
// ---------------------------------------------------------------------------

/// CountL2HH cells are signed: row 0 is positive fixint max / uint8 / uint16 /
/// int16, row 1 is negative fixint min / int8 / int32 / int64.
const L2HH_COUNTS: [[i64; 4]; 2] = [[127, 128, 65535, -32768], [-32, -33, -2147483648, i64::MIN]];
/// One accumulator per row, set independently of the cells: uint32 / uint64.
const L2HH_L2: [i64; 2] = [65536, i64::MAX];
const L2HH_SEED_INDEX: usize = 7;

#[derive(serde::Serialize, serde::Deserialize)]
struct L2hhMatrixForm {
    data: Vec<i64>,
    rows: usize,
    cols: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct L2hhVectorForm {
    data: Vec<i64>,
}

/// The named serde form of `CountL2HH`.
#[derive(serde::Serialize, serde::Deserialize)]
struct L2hhForm {
    counts: L2hhMatrixForm,
    l2: L2hhVectorForm,
    row: usize,
    col: usize,
    seed_idx: usize,
}

fn l2hh_form(counts: &[[i64; 4]; 2], l2: &[i64; 2], seed_idx: usize) -> L2hhForm {
    L2hhForm {
        counts: L2hhMatrixForm {
            data: counts.iter().flatten().copied().collect(),
            rows: 2,
            cols: 4,
        },
        l2: L2hhVectorForm { data: l2.to_vec() },
        row: 2,
        col: 4,
        seed_idx,
    }
}

#[test]
fn count_l2hh_2x4_seed7_matches_golden() {
    let want = decode_hex(GOLDEN_L2HH);

    let form = l2hh_form(&L2HH_COUNTS, &L2HH_L2, L2HH_SEED_INDEX);
    let sketch: CountL2HH =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&form).expect("serde form"))
            .expect("CountL2HH from its serde form");
    let got = sketch.serialize_to_bytes().expect("serialize");
    assert_eq!(got, want, "CountL2HH bytes diverge from golden");

    let decoded: CountL2HH = CountL2HH::deserialize_from_bytes(&want).expect("decode");
    let flat: Vec<i64> = L2HH_COUNTS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.rows(), 2);
    assert_eq!(decoded.cols(), 4);
    assert_eq!(decoded.seed_idx(), L2HH_SEED_INDEX);
    let state: L2hhForm =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&decoded).expect("serde form"))
            .expect("decoded serde form");
    assert_eq!(state.l2.data, L2HH_L2);
    assert_eq!(decoded.serialize_to_bytes().expect("re-serialize"), want);
}
