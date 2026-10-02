//! Cross-language ASAPv1 golden byte-vector tests.
//!
//! Each fixture is built from a fixed, KNOWN raw sketch state (register bytes /
//! matrix values set directly, never hashed) and asserted to serialize to the
//! exact bytes in `asapv1_golden/*.hex`. That directory is a git submodule of
//! <https://github.com/ProjectASAP/sketchlib-golden-bytes>, the fixtures every ASAPv1 implementation
//! must reproduce.
//!
//! See `asapv1_golden/README.md`.

use asap_sketchlib::{
    CMSHeap, CSHeap, Classic, Count, CountMin, DataInput, ErtlMLE, FastPath, HeapItem,
    HllBucketListP12, HllBucketListP14, HllRegisterStorage, HyperLogLogHIPP12, HyperLogLogHIPP14,
    HyperLogLogP12, HyperLogLogP14, KLL, KLLDynamic, RegularPath, Vector2D,
};
use serde::de::DeserializeOwned;

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
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_KLL_DYN_F64: &str = include_str!("../asapv1_golden/kll_dynamic_f64_k200.hex");
const GOLDEN_KLL_DYN_I64: &str = include_str!("../asapv1_golden/kll_dynamic_i64_k200.hex");

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
