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
    Classic, Count, CountDelta, CountMin, DataInput, ErtlMLE, FastPath, HeapItem, HllSketch,
    HllVariant, HyperLogLogHIPP12, HyperLogLogP12, KLL, L2HH, MessagePackCodec, RegularPath,
    UnivMon, Vector2D,
};
use serde::Deserialize;
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
const GOLDEN_CMS_I64: &str = include_str!("../asapv1_golden/cms_i64_regular_2x3.hex");
const GOLDEN_CMS_F64: &str = include_str!("../asapv1_golden/cms_f64_fast_2x3.hex");
const GOLDEN_CS_REGULAR: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4.hex");
const GOLDEN_CS_FAST: &str = include_str!("../asapv1_golden/cs_i64_fast_2x4.hex");
const GOLDEN_CS_I32: &str = include_str!("../asapv1_golden/cs_i32_regular_2x4.hex");
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_UNIVMON_STR: &str = include_str!("../asapv1_golden/univmon_str_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_I64: &str = include_str!("../asapv1_golden/univmon_i64_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_EMPTY: &str = include_str!("../asapv1_golden/univmon_empty_l3_2x4_h5.hex");

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
