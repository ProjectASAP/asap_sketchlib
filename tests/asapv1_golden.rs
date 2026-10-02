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
    Classic, Count, CountDelta, CountMin, DataInput, ErtlMLE, FastPath, HllSketch, HllVariant,
    Hydra, HyperLogLog, HyperLogLogHIPP12, HyperLogLogP12, KLL, MessagePackCodec, RegularPath,
    UnivMon, Vector2D,
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
const GOLDEN_CMS_I64: &str = include_str!("../asapv1_golden/cms_i64_regular_2x3.hex");
const GOLDEN_CMS_F64: &str = include_str!("../asapv1_golden/cms_f64_fast_2x3.hex");
const GOLDEN_CS_REGULAR: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4.hex");
const GOLDEN_CS_FAST: &str = include_str!("../asapv1_golden/cs_i64_fast_2x4.hex");
const GOLDEN_CS_I32: &str = include_str!("../asapv1_golden/cs_i32_regular_2x4.hex");
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_HYDRA_KLL: &str = include_str!("../asapv1_golden/hydra_kll_2x2_k200.hex");
const GOLDEN_HYDRA_CM: &str = include_str!("../asapv1_golden/hydra_cm_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_CS: &str = include_str!("../asapv1_golden/hydra_cs_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_HLL: &str = include_str!("../asapv1_golden/hydra_hll_1x2_p14.hex");
const GOLDEN_HYDRA_UNIVMON: &str = include_str!("../asapv1_golden/hydra_univmon_1x2.hex");

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
