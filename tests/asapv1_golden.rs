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

use asap_sketchlib::common::input::HydraCounter;
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::message_pack_format::portable::hll::HllSketch;
use asap_sketchlib::message_pack_format::portable::hll::HllVariant;
use asap_sketchlib::sketches::CountSketchDelta;
use asap_sketchlib::sketches::hll::HllWireVariant;
use asap_sketchlib::{
    Bloom, DefaultXxHasher, EhBucket, EhBucketSketch, ExponentialHistogram, KeyColumn, SpaceSaving,
    TupleKey, UnivMonQ, UnivMonQConfig, UnivMonTwoTier,
};
use asap_sketchlib::{
    Classic, CocoBucket, CocoSketch, CountMinSketch, CountMinSketchDelta, CountMinSketchWithHeap,
    CountMinSketchWithHeapDelta, CountMinSketchWithSpaceSaving, CountSketch, CountSketchCellDelta,
    CountSketchWithHeap, CountSketchWithHeapDelta, CountSketchWithL2Norm,
    CountSketchWithSpaceSaving, DataInput, DdSketch, DdSketchDelta, ElasticSketch, Ertl, FastPath,
    HeapCount, HeavyBucket, HllRegistersP14, Hydra, HyperLogLog, HyperLogLogDelta,
    HyperLogLogHipP12, HyperLogLogHipP14, HyperLogLogP12, HyperLogLogP14, Kll, KllDynamic, L2HH,
    OwnedDataInput, RegularPath, SetAggregator, SetAggregatorDelta, TopKHeap, UnivMon, Vector2D,
    peek_kind_id,
};
#[cfg(feature = "experimental")]
use asap_sketchlib::{Kmv, UniformSampling};

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
const GOLDEN_HIP_P14: &str = include_str!("../asapv1_golden/hll_hip_p14.hex");
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
const GOLDEN_CSHEAP_I64_TIE: &str =
    include_str!("../asapv1_golden/csheap_i64_regular_2x4_i64tie.hex");
const GOLDEN_CMSHEAP_TUPLE: &str =
    include_str!("../asapv1_golden/cmsheap_i64_regular_2x3_tuplekeys.hex");
const GOLDEN_CSHEAP_TUPLE: &str =
    include_str!("../asapv1_golden/csheap_i64_regular_2x4_tuplekeys.hex");
const GOLDEN_CMSSS_STR: &str =
    include_str!("../asapv1_golden/cmsspacesaving_i64_regular_2x3_strkeys.hex");
const GOLDEN_CSSS_I64: &str =
    include_str!("../asapv1_golden/csspacesaving_f64_fast_2x4_i64keys.hex");
const GOLDEN_DD_POSITIVE: &str = include_str!("../asapv1_golden/ddsketch_positive_a001.hex");
const GOLDEN_DD_SIGNED: &str = include_str!("../asapv1_golden/ddsketch_signed_a001.hex");
const GOLDEN_DD_EMPTY: &str = include_str!("../asapv1_golden/ddsketch_empty_a001.hex");
const GOLDEN_DD_DELTA: &str = include_str!("../asapv1_golden/ddsketch_signed_a001_delta.hex");
const GOLDEN_KLL_F64: &str = include_str!("../asapv1_golden/kll_f64_k200.hex");
const GOLDEN_KLL_I64: &str = include_str!("../asapv1_golden/kll_i64_k200.hex");
const GOLDEN_KLL_DYN_F64: &str = include_str!("../asapv1_golden/kll_dynamic_f64_k200.hex");
const GOLDEN_KLL_DYN_I64: &str = include_str!("../asapv1_golden/kll_dynamic_i64_k200.hex");
const GOLDEN_SET_STRINGS: &str = include_str!("../asapv1_golden/set_aggregator_strings.hex");
const GOLDEN_SET_EMPTY: &str = include_str!("../asapv1_golden/set_aggregator_empty.hex");
const GOLDEN_DELTA_STRINGS: &str = include_str!("../asapv1_golden/delta_result_strings.hex");
const GOLDEN_DELTA_EMPTY: &str = include_str!("../asapv1_golden/delta_result_empty.hex");
const GOLDEN_COCO: &str = include_str!("../asapv1_golden/coco_3x7.hex");
const GOLDEN_ELASTIC: &str = include_str!("../asapv1_golden/elastic_4b_2x4.hex");
const GOLDEN_ELASTIC_STALE: &str = include_str!("../asapv1_golden/elastic_4b_2x4_stale.hex");
const GOLDEN_UNIVMON_STR: &str = include_str!("../asapv1_golden/univmon_str_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_I64: &str = include_str!("../asapv1_golden/univmon_i64_l3_2x4_h5.hex");
const GOLDEN_UNIVMON_EMPTY: &str = include_str!("../asapv1_golden/univmon_empty_l3_2x4_h5.hex");
const GOLDEN_L2HH: &str = include_str!("../asapv1_golden/count_l2hh_2x4_seed7.hex");
const GOLDEN_HYDRA_KLL: &str = include_str!("../asapv1_golden/hydra_kll_2x2_k200.hex");
const GOLDEN_HYDRA_CM: &str = include_str!("../asapv1_golden/hydra_cm_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_CS: &str = include_str!("../asapv1_golden/hydra_cs_2x2_counter_2x2.hex");
const GOLDEN_HYDRA_HLL: &str = include_str!("../asapv1_golden/hydra_hll_1x2_p14.hex");
const GOLDEN_HYDRA_UNIVMON: &str = include_str!("../asapv1_golden/hydra_univmon_1x2.hex");
const GOLDEN_CLASSIC_ADMISSION: &str =
    include_str!("../asapv1_golden/hll_classic_p12_admission.hex");
const GOLDEN_CMS_ADMISSION: &str =
    include_str!("../asapv1_golden/cms_i64_regular_2x3_admission.hex");
const GOLDEN_CS_ADMISSION: &str = include_str!("../asapv1_golden/cs_i64_regular_2x4_admission.hex");
const GOLDEN_DD_ADMISSION: &str =
    include_str!("../asapv1_golden/ddsketch_signed_a001_admission.hex");
#[cfg(feature = "experimental")]
const GOLDEN_KMV: &str = include_str!("../asapv1_golden/kmv_k8.hex");
const GOLDEN_BLOOM_REGULAR: &str = include_str!("../asapv1_golden/bloom_regular_3x128.hex");
const GOLDEN_BLOOM_FAST: &str = include_str!("../asapv1_golden/bloom_fast_5x32.hex");
const GOLDEN_SPACE_SAVING_I64: &str = include_str!("../asapv1_golden/space_saving_i64_c8.hex");
const GOLDEN_SPACE_SAVING_BYTES: &str = include_str!("../asapv1_golden/space_saving_bytes_c3.hex");
#[cfg(feature = "experimental")]
const GOLDEN_UNIFORM: &str = include_str!("../asapv1_golden/uniform_sampling_r0p25.hex");
const GOLDEN_PYRAMID: &str =
    include_str!("../asapv1_golden/univmon_optimized_l5_e2_3x8_1x4_h6.hex");
const GOLDEN_UNIVMON_Q: &str = include_str!("../asapv1_golden/univmon_q_i32_l3_c8_r3.hex");
const GOLDEN_EHSL_CM: &str = include_str!("../asapv1_golden/ehsketchlist_cm_i32_fast_2x4.hex");
const GOLDEN_EHSL_DD: &str = include_str!("../asapv1_golden/ehsketchlist_ddsketch_a001.hex");
const GOLDEN_EH: &str = include_str!("../asapv1_golden/eh_w100_k2_cm_2x4.hex");

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
    let native = HyperLogLogP12::<Classic>::from_asapv1(&want).expect("native decode");
    assert_eq!(native.registers_as_slice(), regs.as_slice());
    assert_eq!(native.to_asapv1().expect("re-serialize"), want);

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

    let native = HyperLogLogP12::<Ertl>::from_asapv1(&want).expect("native decode");
    assert_eq!(native.registers_as_slice(), regs.as_slice());
    assert_eq!(native.to_asapv1().expect("re-serialize"), want);

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
    let native = HyperLogLogHipP12::from_asapv1(&want).expect("native decode");
    assert_eq!(native.to_asapv1().expect("re-serialize"), want);

    let decoded = HllSketch::from_msgpack(&want).expect("decode");
    assert_eq!(decoded.registers, regs);
    assert_eq!(decoded.variant, HllVariant::Hip);
    assert_eq!(decoded.hip_kxq0, 1.5);
    assert_eq!(decoded.hip_kxq1, 2.5);
    assert_eq!(decoded.hip_est, 3.0);
}

#[test]
fn hll_hip_p14_matches_golden() {
    let want = decode_hex(GOLDEN_HIP_P14);
    let mut regs = vec![0u8; 16384];
    for (index, value) in [(0, 1), (1, 7), (8192, 42), (16383, 51)] {
        regs[index] = value;
    }

    let got = HllSketch::from_raw(HllVariant::Hip, 14, regs.clone(), 16380.5, 0.25, 4.125)
        .to_msgpack()
        .expect("serialize");
    assert_eq!(got, want, "HIP P14 bytes diverge from golden");

    let native = HyperLogLogHipP14::from_asapv1(&want).expect("native decode");
    assert_eq!(native.to_asapv1().expect("re-serialize"), want);

    let decoded = HllSketch::from_msgpack(&want).expect("decode");
    assert_eq!(decoded.registers, regs);
    assert_eq!(decoded.precision, 14);
    assert_eq!(decoded.variant, HllVariant::Hip);
    assert_eq!(decoded.hip_kxq0, 16380.5);
    assert_eq!(decoded.hip_kxq1, 0.25);
    assert_eq!(decoded.hip_est, 4.125);
}

// ---------------------------------------------------------------------------
// HLL delta: build known delta -> serialize == golden, round-trips, and merged
// into the matching P14 state golden gives the expected registers.
// ---------------------------------------------------------------------------

fn assert_hll_p14_delta<Variant: HllWireVariant>(delta_hex: &str, state_hex: &str) {
    type Delta<V> = HyperLogLogDelta<V, HllRegistersP14>;
    let want = decode_hex(delta_hex);
    let indices = vec![0, 2, 200, 8192, 16383];
    let registers = vec![51, 1, 9, 7, 1];
    let delta = Delta::<Variant>::new(indices, registers).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = Delta::<Variant>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = HyperLogLogP14::<Variant>::from_asapv1(&decode_hex(state_hex)).expect("state");
    state.merge_delta(&decoded).expect("merge");
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
    assert_hll_p14_delta::<Ertl>(GOLDEN_ERTL_P14_DELTA, GOLDEN_ERTL_P14);
}

// ---------------------------------------------------------------------------
// Count-Min: build known matrix state -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

#[test]
fn cms_i64_regular_2x3_matches_golden() {
    let want = decode_hex(GOLDEN_CMS_I64);

    let sketch = CountMinSketch::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::from_fn(
        2,
        3,
        |r, c| I64_VALS[r][c],
    ));
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "CMS i64/regular bytes diverge from golden");

    let decoded = CountMinSketch::<Vector2D<i64>, RegularPath>::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.rows(), 2);
    assert_eq!(decoded.cols(), 3);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn cms_f64_fast_2x3_matches_golden() {
    let want = decode_hex(GOLDEN_CMS_F64);

    let sketch =
        CountMinSketch::<Vector2D<f64>, FastPath>::from_storage(Vector2D::from_fn(2, 3, |r, c| {
            F64_VALS[r][c]
        }));
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "CMS f64/fast bytes diverge from golden");

    let decoded = CountMinSketch::<Vector2D<f64>, FastPath>::from_asapv1(&want).expect("decode");
    let flat: Vec<f64> = F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Count-Min delta: build known delta -> serialize == golden, round-trips, and
// merged into the matching state golden gives the expected state.
// ---------------------------------------------------------------------------

#[test]
fn cms_i64_regular_2x3_delta_matches_golden() {
    type Cms = CountMinSketch<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CMS_I64_DELTA);
    let indices = vec![0, 1, 2, 4, 5];
    let deltas = vec![127, -1, -33, -32769, 4294967296];
    let delta = CountMinSketchDelta::<i64, RegularPath>::new(2, 3, indices, deltas).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = CountMinSketchDelta::<i64, RegularPath>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = Cms::from_asapv1(&decode_hex(GOLDEN_CMS_I64)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [127, 0, 94, 128, -32469, 4295032832];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

#[test]
fn cms_f64_fast_2x3_delta_matches_golden() {
    type Cms = CountMinSketch<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CMS_F64_DELTA);
    let indices = vec![0, 3, 5];
    let deltas = vec![2.5, -0.75, -5.0625];
    let delta = CountMinSketchDelta::<f64, FastPath>::new(2, 3, indices, deltas).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = CountMinSketchDelta::<f64, FastPath>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = Cms::from_asapv1(&decode_hex(GOLDEN_CMS_F64)).expect("state");
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
        CountSketch::<Vector2D<i64>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
            CS_VALS[r][c]
        }));
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(
        got, want,
        "Count Sketch i64/regular bytes diverge from golden"
    );

    let decoded = CountSketch::<Vector2D<i64>, RegularPath>::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.rows(), 2);
    assert_eq!(decoded.cols(), 4);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn cs_i64_fast_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_CS_FAST);

    let sketch =
        CountSketch::<Vector2D<i64>, FastPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
            CS_VALS[r][c]
        }));
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "Count Sketch i64/fast bytes diverge from golden");

    let decoded = CountSketch::<Vector2D<i64>, FastPath>::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    // Same matrix, different mode -> different bytes.
    assert_ne!(want, decode_hex(GOLDEN_CS_REGULAR));
}

#[test]
fn cs_i32_regular_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_CS_I32);

    let sketch =
        CountSketch::<Vector2D<i32>, RegularPath>::from_storage(Vector2D::from_fn(2, 4, |r, c| {
            CS_VALS[r][c] as i32
        }));
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(
        got, want,
        "Count Sketch i32/regular bytes diverge from golden"
    );

    let decoded = CountSketch::<Vector2D<i32>, RegularPath>::from_asapv1(&want).expect("decode");
    let flat: Vec<i32> = CS_VALS.iter().flatten().map(|&v| v as i32).collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    // Same matrix, same mode, narrower counter: the fixtures differ only in the
    // metadata `counter_type`, so the payload bytes are identical.
    let wide = decode_hex(GOLDEN_CS_REGULAR);
    assert_eq!(want.len(), wide.len());
    assert_ne!(want, wide);
    assert!(
        CountSketch::<Vector2D<i64>, RegularPath>::from_asapv1(&want).is_err(),
        "the i32 golden must not decode as an i64 sketch"
    );
}

#[test]
fn cs_f64_fast_2x4_matches_golden() {
    type Cs = CountSketch<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CS_F64_FAST);
    let sketch = Cs::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_F64_VALS[r][c]));
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Cs::from_asapv1(&want).expect("decode");
    let flat: Vec<f64> = CS_F64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Count Sketch delta: build known delta -> serialize == golden, round-trips,
// and merged into the matching state golden gives the expected state.
// ---------------------------------------------------------------------------

#[test]
fn cs_i64_regular_2x4_delta_matches_golden() {
    type Cs = CountSketch<Vector2D<i64>, RegularPath>;
    type Delta = CountSketchDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CS_I64_DELTA);
    let indices = vec![0, 1, 2, 4, 6, 7];
    let deltas = vec![-32, -128, 255, -129, 65535, -2147483649];
    let delta = Delta::new(2, 4, indices, deltas).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = Delta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = Cs::from_asapv1(&decode_hex(GOLDEN_CS_REGULAR)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [-32, -1, 383, 65536, -130, -33, 32767, -4294967297];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

#[test]
fn cs_f64_fast_2x4_delta_matches_golden() {
    type Cs = CountSketch<Vector2D<f64>, FastPath>;
    type Delta = CountSketchDelta<f64, FastPath>;
    let want = decode_hex(GOLDEN_CS_F64_DELTA);
    let indices = vec![1, 3, 5, 7];
    let deltas = vec![-0.75, 2.25, 0.5, 1e300];
    let delta = Delta::new(2, 4, indices, deltas).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = Delta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = Cs::from_asapv1(&decode_hex(GOLDEN_CS_F64_FAST)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [0.0, -0.75, 1.5, 0.0, 127.0, -32768.0, 0.125, 0.0];
    assert_eq!(state.as_storage().as_slice(), expected.as_slice());
}

// ---------------------------------------------------------------------------
// CountMinSketchWithHeap / CountSketchWithHeap: the matrix and the heap entries set directly -> serialize ==
// golden, round-trips. Entries are seated out of emitted order.
// ---------------------------------------------------------------------------

/// `(key, count)` pairs as string-keyed heap entries.
fn str_entries(entries: &[(&str, i64)]) -> Vec<(OwnedDataInput, i64)> {
    entries
        .iter()
        .map(|&(key, count)| (OwnedDataInput::String(key.to_string()), count))
        .collect()
}

/// `(key, count)` pairs as `i64`-keyed heap entries.
fn i64_entries(entries: &[(i64, i64)]) -> Vec<(OwnedDataInput, i64)> {
    entries
        .iter()
        .map(|&(key, count)| (OwnedDataInput::I64(key), count))
        .collect()
}

/// A heap of capacity `heap_size` holding `entries`, seated in the given order.
fn heap_of<N: HeapCount>(heap_size: usize, entries: &[(OwnedDataInput, N)]) -> TopKHeap<N> {
    let mut heap = TopKHeap::new(heap_size);
    for (key, count) in entries {
        assert!(heap.update_owned(key, *count));
    }
    heap
}

/// Asserts `heap` has capacity `heap_size` and holds exactly `entries`.
fn assert_heap<N: HeapCount>(
    heap: &TopKHeap<N>,
    heap_size: usize,
    entries: &[(OwnedDataInput, N)],
) {
    assert_eq!(heap.capacity(), heap_size);
    assert_eq!(heap.len(), entries.len());
    for (key, count) in entries {
        let at = heap.find_owned(key).expect("heap key");
        assert!(heap.heap()[at].count.order(count).is_eq(), "{key:?}");
    }
}

type CmsHeapI64 = CountMinSketchWithHeap<Vector2D<i64>, RegularPath>;

/// The i64 regular-path CountMinSketchWithHeap over the Count-Min i64 matrix with `entries`.
fn cmsheap_i64(heap_size: usize, entries: &[(OwnedDataInput, i64)]) -> CmsHeapI64 {
    let mut sketch =
        CmsHeapI64::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]), heap_size);
    *sketch.heap_mut() = heap_of(heap_size, entries);
    sketch
}

fn check_cmsheap_i64_golden(golden: &str, heap_size: usize, entries: &[(OwnedDataInput, i64)]) {
    let want = decode_hex(golden);
    let got = cmsheap_i64(heap_size, entries).to_asapv1();
    assert_eq!(got.expect("serialize"), want);

    let decoded = CmsHeapI64::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_min_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), heap_size, entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
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
    type Heap = CountMinSketchWithHeap<Vector2D<i32>, FastPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_I32);
    let entries = i64_entries(&[(4_294_967_296, 3), (-1, 7), (-129, 7)]);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c] as i32), 3);
    *sketch.heap_mut() = heap_of(3, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<i32> = I64_VALS.iter().flatten().map(|&v| v as i32).collect();
    assert_eq!(
        decoded.count_min_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 3, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// `f64` heap counts order by `total_cmp`: `+0.0` before `-0.0`, the
/// infinities at the ends; the `300.25` tie falls to the key's bits.
#[test]
fn cmsheap_f64_fast_2x3_f64keys_matches_golden() {
    type Heap = CountMinSketchWithHeap<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_F64);
    let entries: Vec<(OwnedDataInput, f64)> = [
        (8.0, 0.0),
        (-0.5, 300.25),
        (9.0, f64::NEG_INFINITY),
        (7.0, -0.0),
        (1.5, 300.25),
        (2.0, f64::INFINITY),
    ]
    .into_iter()
    .map(|(key, count)| (OwnedDataInput::F64(key), count))
    .collect();
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 3, |r, c| F64_VALS[r][c]), 6);
    *sketch.heap_mut() = heap_of(6, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<f64> = F64_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_min_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 6, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// The delta carries `i64` keys into a `string`-keyed state: applying it adds
/// the cells and replaces the heap, `key_type` with it.
#[test]
fn cmsheap_i64_regular_2x3_delta_matches_golden() {
    type Delta = CountMinSketchWithHeapDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CMSHEAP_DELTA);
    let entries = i64_entries(&[(7, 128), (-129, 128), (-1, 4_295_032_832)]);
    let indices = vec![0, 1, 2, 4, 5];
    let deltas = vec![127, -1, -33, -32769, 4294967296];
    let delta = Delta::new(2, 3, indices, deltas, heap_of(5, &entries)).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = Delta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = CmsHeapI64::from_asapv1(&decode_hex(GOLDEN_CMSHEAP_STR)).expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [127, 0, 94, 128, -32469, 4295032832];
    assert_eq!(
        state.count_min_sketch().as_storage().as_slice(),
        expected.as_slice()
    );
    assert_heap(state.heap(), 5, &entries);
}

#[test]
fn csheap_i64_regular_2x4_strkeys_matches_golden() {
    type Heap = CountSketchWithHeap<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_STR);
    let entries = str_entries(&[
        ("gamma", -33),
        ("delta", 127),
        ("alpha", 4_294_967_296),
        ("beta", 127),
    ]);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]), 5);
    *sketch.heap_mut() = heap_of(5, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 5, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// Signed heap counts; the `-3` tie emits by two's-complement bits, so
/// `0, 1, i64::MIN, -1` rather than signed order.
#[test]
fn csheap_i64_regular_2x4_i64tie_matches_golden() {
    type Heap = CountSketchWithHeap<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_I64_TIE);
    let entries = i64_entries(&[
        (-1, -3),
        (-129, -40),
        (1, -3),
        (0, -3),
        (i64::MIN, -3),
        (2, 9),
    ]);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]), 6);
    *sketch.heap_mut() = heap_of(6, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 6, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// `f64` heap counts are the unrounded signed median; the `-0.75` tie falls
/// to the keys.
#[test]
fn csheap_f64_fast_2x4_strkeys_matches_golden() {
    type Heap = CountSketchWithHeap<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CSHEAP_F64);
    let entries: Vec<(OwnedDataInput, f64)> = [
        ("gamma", -16384.25),
        ("beta", -0.75),
        ("alpha", 63.5625),
        ("aa", -0.75),
    ]
    .into_iter()
    .map(|(key, count)| (OwnedDataInput::String(key.to_string()), count))
    .collect();
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_F64_VALS[r][c]), 4);
    *sketch.heap_mut() = heap_of(4, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<f64> = CS_F64_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 4, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// The delta carries `i64` keys into a `string`-keyed state: applying it adds
/// the cells and replaces the heap, `key_type` with it.
#[test]
fn csheap_i64_regular_2x4_delta_matches_golden() {
    type Delta = CountSketchWithHeapDelta<i64, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_DELTA);
    let entries = i64_entries(&[(7, -33), (-129, -4_294_967_297), (-1, 65536)]);
    let indices = vec![0, 1, 2, 4, 6, 7];
    let deltas = vec![-32, -128, 255, -129, 65535, -2147483649];
    let delta = Delta::new(2, 4, indices, deltas, heap_of(5, &entries)).expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = Delta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&decode_hex(
        GOLDEN_CSHEAP_STR,
    ))
    .expect("state");
    state.merge_delta(&decoded).expect("merge");
    let expected = [-32, -1, 383, 65536, -130, -33, 32767, -4294967297];
    assert_eq!(
        state.count_sketch().as_storage().as_slice(),
        expected.as_slice()
    );
    assert_heap(state.heap(), 5, &entries);
}

// ---------------------------------------------------------------------------
// Tuple keys: CountMinSketchWithHeap and CountSketchWithHeap keys made of typed columns, each encoded as a
// `bytes` key -> serialize == golden, round-trips back to the columns.
// ---------------------------------------------------------------------------

/// The tuples, named; `-0.0` and `NaN` take their canonical encodings.
fn tuple(name: &str) -> TupleKey {
    use KeyColumn::*;
    TupleKey::new(match name {
        "east443" => vec![String("us-east".into()), I64(443), Bool(true)],
        "east80" => vec![String("us-east".into()), I64(80), Null],
        "null" => vec![Null, I64(-1), F64(2.5)],
        "zero" => vec![String("".into()), I64(i64::MIN), F64(-0.0)],
        "nan" => vec![String("é".into()), I64(0), F64(f64::NAN), Bool(false)],
        other => panic!("no tuple {other}"),
    })
}

fn tuple_entries(entries: &[(&str, i64)]) -> Vec<(OwnedDataInput, i64)> {
    entries
        .iter()
        .map(|&(name, count)| {
            (
                OwnedDataInput::Bytes(tuple(name).to_bytes().unwrap()),
                count,
            )
        })
        .collect()
}

/// Every decoded heap key is a canonical tuple key naming one of `entries`.
fn assert_tuple_keys<N: HeapCount>(heap: &TopKHeap<N>, entries: &[(&str, i64)]) {
    for item in heap.heap() {
        let OwnedDataInput::Bytes(bytes) = &item.key else {
            panic!("expected a bytes key, got {:?}", item.key);
        };
        let key = TupleKey::from_bytes(bytes).expect("a tuple key");
        assert_eq!(&key.to_bytes().unwrap(), bytes);
        assert!(
            entries
                .iter()
                .any(|&(name, _)| tuple(name).to_bytes().unwrap() == *bytes)
        );
    }
}

#[test]
fn cmsheap_i64_regular_2x3_tuplekeys_matches_golden() {
    let entries = [
        ("zero", 1),
        ("east80", 300),
        ("null", 65536),
        ("nan", 127),
        ("east443", 300),
    ];
    check_cmsheap_i64_golden(GOLDEN_CMSHEAP_TUPLE, 5, &tuple_entries(&entries));
    let decoded = CmsHeapI64::from_asapv1(&decode_hex(GOLDEN_CMSHEAP_TUPLE)).unwrap();
    assert_tuple_keys(decoded.heap(), &entries);
    let positive_zero = TupleKey::new(vec![
        KeyColumn::String(String::new()),
        KeyColumn::I64(i64::MIN),
        KeyColumn::F64(0.0),
    ]);
    let found = decoded
        .heap()
        .find_owned(&OwnedDataInput::Bytes(positive_zero.to_bytes().unwrap()));
    assert!(found.is_some(), "-0.0 and 0.0 are one tuple key");
}

#[test]
fn csheap_i64_regular_2x4_tuplekeys_matches_golden() {
    type Heap = CountSketchWithHeap<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CSHEAP_TUPLE);
    let named = [
        ("null", -33),
        ("east443", 127),
        ("nan", 65536),
        ("zero", 127),
    ];
    let entries = tuple_entries(&named);
    let mut sketch = Heap::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]), 6);
    *sketch.heap_mut() = heap_of(6, &entries);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Heap::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    assert_heap(decoded.heap(), 6, &entries);
    assert_tuple_keys(decoded.heap(), &named);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// CountMinSketchWithSpaceSaving / CountSketchWithSpaceSaving: the matrix set directly and the summary built
// by weighted inserts, whose triples do not depend on the hash.
// ---------------------------------------------------------------------------

/// Asserts `summary` has `capacity`, `total` and `ceiling` and holds exactly
/// `entries` as `(key, count, error)`.
fn assert_summary(
    summary: &SpaceSaving,
    capacity: usize,
    total: u64,
    ceiling: u64,
    entries: &[(DataInput<'_>, u64, u64)],
) {
    assert_eq!(summary.capacity(), capacity);
    assert_eq!(summary.total(), total);
    assert_eq!(summary.min_count(), ceiling);
    assert_eq!(summary.len(), entries.len());
    for (key, count, error) in entries {
        assert_eq!(summary.estimate(key), *count, "{key:?}");
        assert_eq!(summary.error(key), *error, "{key:?}");
    }
}

/// `aa` displaces `cold` (count 1) and carries it as error; the `7` tie falls
/// to the keys, so `"aa"` precedes `"b"`.
#[test]
fn cmsspacesaving_i64_regular_2x3_strkeys_matches_golden() {
    type Sketch = CountMinSketchWithSpaceSaving<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CMSSS_STR);
    let mut summary = SpaceSaving::with_capacity(4);
    for (key, weight) in [
        ("hot", 300),
        ("warm", 128),
        ("cold", 1),
        ("b", 7),
        ("aa", 6),
    ] {
        summary.update_weighted(&DataInput::Str(key), weight);
    }
    let matrix = CountMinSketch::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]));
    let sketch = Sketch::from_parts(matrix, summary);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Sketch::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_min_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    let entries = [
        (DataInput::Str("hot"), 300, 0),
        (DataInput::Str("warm"), 128, 0),
        (DataInput::Str("aa"), 7, 1),
        (DataInput::Str("b"), 7, 0),
    ];
    assert_summary(decoded.space_saving(), 4, 442, 7, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// `7` displaces `128` (count 5) and carries it as error; the `6` tie falls to
/// the keys' two's-complement bits, so `7` precedes `i64::MIN`.
#[test]
fn csspacesaving_f64_fast_2x4_i64keys_matches_golden() {
    type Sketch = CountSketchWithSpaceSaving<Vector2D<f64>, FastPath>;
    let want = decode_hex(GOLDEN_CSSS_I64);
    let mut summary = SpaceSaving::with_capacity(3);
    for (key, weight) in [(-1, 65536), (i64::MIN, 5), (128, 5), (7, 1), (i64::MIN, 1)] {
        summary.update_weighted(&DataInput::I64(key), weight);
    }
    let matrix = CountSketch::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_F64_VALS[r][c]));
    let sketch = Sketch::from_parts(matrix, summary);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Sketch::from_asapv1(&want).expect("decode");
    let flat: Vec<f64> = CS_F64_VALS.iter().flatten().copied().collect();
    assert_eq!(
        decoded.count_sketch().as_storage().as_slice(),
        flat.as_slice()
    );
    let entries = [
        (DataInput::I64(-1), 65536, 0),
        (DataInput::I64(7), 6, 5),
        (DataInput::I64(i64::MIN), 6, 0),
    ];
    assert_summary(decoded.space_saving(), 3, 65548, 6, &entries);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// KLL: build known state (k=200, seed 42, integers 1..=50 — below the level-0
// capacity, so no compaction fires and the retained set is deterministic) ->
// serialize == golden, and golden round-trips. Matches the deterministic
// scenario the proto parity test uses (sketchlib-go's KLLSketch over the same
// input), so the coin state (42) lines up cross-language.
// ---------------------------------------------------------------------------

/// A k=200 KLL over `1..=50` with a fixed compaction seed: fully deterministic.
fn kll_1to50<T: From<u8> + asap_sketchlib::NumericalValue>(seed: u64) -> Kll<T> {
    let mut sketch = Kll::<T>::with_k_and_seed(200, seed);
    for v in 1..=50u8 {
        sketch.update(&T::from(v));
    }
    sketch
}

#[test]
fn kll_f64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_F64);

    let sketch = kll_1to50::<f64>(42);
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "KLL f64 bytes diverge from golden");

    // Golden round-trips: decode, and re-encode is byte-identical.
    let decoded = Kll::<f64>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
}

#[test]
fn kll_i64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_I64);

    let sketch = kll_1to50::<i64>(42);
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "KLL i64 bytes diverge from golden");

    let decoded = Kll::<i64>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
    assert_eq!(decoded.quantile(0.0), 1.0);
    assert_eq!(decoded.quantile(1.0), 50.0);
}

// ---------------------------------------------------------------------------
// KllDynamic: k=200, seed 42, inputs below the level-0 capacity -> serialize ==
// golden, and golden round-trips.
// ---------------------------------------------------------------------------

const KLL_DYN_F64_VALS: [f64; 7] = [2.5, -1.0, 0.0, 1e300, -0.125, 42.0, 3.0e-5];

/// Every msgpack integer width, positive and negative.
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

fn kll_dynamic_of<T: asap_sketchlib::NumericalValue>(values: &[T]) -> KllDynamic<T> {
    let mut sketch = KllDynamic::<T>::with_k_and_seed(200, 42);
    for v in values {
        sketch.update(v);
    }
    sketch
}

#[test]
fn kll_dynamic_f64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_DYN_F64);

    let sketch = kll_dynamic_of(&KLL_DYN_F64_VALS);
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "KllDynamic f64 bytes diverge from golden");

    let decoded = KllDynamic::<f64>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.count(), KLL_DYN_F64_VALS.len());
    assert_eq!(decoded.quantile(0.0), -1.0);
    assert_eq!(decoded.quantile(1.0), 1e300);
    for v in KLL_DYN_F64_VALS {
        assert_eq!(decoded.rank(v), sketch.rank(v), "rank({v})");
    }
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    assert!(Kll::<f64>::from_asapv1(&want).is_err());
    assert!(KllDynamic::<f64>::from_asapv1(&decode_hex(GOLDEN_KLL_F64)).is_err());
}

#[test]
fn kll_dynamic_i64_k200_matches_golden() {
    let want = decode_hex(GOLDEN_KLL_DYN_I64);

    let sketch = kll_dynamic_of(&KLL_DYN_I64_VALS);
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "KllDynamic i64 bytes diverge from golden");

    let decoded = KllDynamic::<i64>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.count(), KLL_DYN_I64_VALS.len());
    assert_eq!(decoded.quantile(0.0), i64::MIN as f64);
    assert_eq!(decoded.quantile(1.0), i64::MAX as f64);
    for v in KLL_DYN_I64_VALS {
        assert_eq!(decoded.rank(v as f64), sketch.rank(v as f64), "rank({v})");
    }
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    assert!(Kll::<i64>::from_asapv1(&want).is_err());
    assert!(KllDynamic::<i64>::from_asapv1(&decode_hex(GOLDEN_KLL_I64)).is_err());
}

// ---------------------------------------------------------------------------
// DDSketch and its delta: set the stores and scalars directly -> serialize ==
// golden, golden round-trips, and the delta merged into the signed state gives
// the expected stores and scalars.
// ---------------------------------------------------------------------------

const DD_ALPHA: f64 = 0.01;

/// An α=0.01 DDSketch with its stores, zero count and scalars set directly,
/// built through `DdSketch`'s serde form.
fn dd_state(
    positive: (&[u64], i32),
    negative: (&[u64], i32),
    zero_count: u64,
    scalars: (f64, f64, f64),
) -> DdSketch {
    let (sum, min, max) = scalars;
    let store = |(counts, offset): (&[u64], i32)| ((counts.to_vec(),), offset);
    let state = (
        DD_ALPHA,
        store(positive),
        sum,
        min,
        max,
        store(negative),
        zero_count,
    );
    rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).expect("valid state")
}

fn assert_dd_golden(golden: &str, sketch: &DdSketch) {
    let want = decode_hex(golden);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);
    let decoded = DdSketch::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// A store's non-zero buckets as `(index, count)`.
fn dd_buckets(counts: &[u64], offset: i32) -> Vec<(i32, u64)> {
    (offset..)
        .zip(counts.iter().copied())
        .filter(|&(_, c)| c != 0)
        .collect()
}

#[test]
fn ddsketch_positive_a001_matches_golden() {
    let counts = [1, 0, 127, 128, 300, 65536, 4294967296];
    let scalars = (2181071000.0, 0.453125, 0.5078125);
    let sketch = dd_state((&counts, -40), (&[], 0), 0, scalars);
    assert_dd_golden(GOLDEN_DD_POSITIVE, &sketch);
}

#[test]
fn ddsketch_signed_a001_matches_golden() {
    let scalars = (2523.90625, -0.016, 515.0);
    let sketch = dd_state((&[3, 0, 2], 310), (&[5, 1], -208), 7, scalars);
    assert_dd_golden(GOLDEN_DD_SIGNED, &sketch);
}

#[test]
fn ddsketch_empty_a001_matches_golden() {
    assert_dd_golden(GOLDEN_DD_EMPTY, &DdSketch::new(DD_ALPHA));
}

#[test]
fn ddsketch_signed_a001_delta_matches_golden() {
    let want = decode_hex(GOLDEN_DD_DELTA);
    let indices = vec![-300, -100, -5, 7, 200, 400];
    let deltas = vec![1, 127, 128, 256, 65536, 4294967296];
    let negative_indices = vec![-208, -33, -1, 0, 130, 1000];
    let negative_deltas = vec![2, 3, 3, 1, 3, 1];
    let delta = DdSketchDelta::new(
        DD_ALPHA,
        indices.clone(),
        deltas.clone(),
        negative_indices.clone(),
        negative_deltas.clone(),
        9,
    )
    .expect("valid");
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = DdSketchDelta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);

    let mut state = DdSketch::from_asapv1(&decode_hex(GOLDEN_DD_SIGNED)).expect("state");
    state.merge_delta(&decoded).expect("merge");

    let gamma = (1.0 + DD_ALPHA) / (1.0 - DD_ALPHA);
    let r = |k: i32| (1.0 + DD_ALPHA) * gamma.powf(f64::from(k));
    let mut sum = 2523.90625;
    for (k, c) in indices.iter().zip(&deltas) {
        sum += r(*k) * *c as f64;
    }
    for (k, c) in negative_indices.iter().zip(&negative_deltas) {
        sum += -r(*k) * *c as f64;
    }
    assert_eq!(
        dd_buckets(state.positive_store_counts(), state.positive_store_offset()),
        [
            (-300, 1),
            (-100, 127),
            (-5, 128),
            (7, 256),
            (200, 65536),
            (310, 3),
            (312, 2),
            (400, 4294967296)
        ]
    );
    assert_eq!(
        dd_buckets(state.negative_store_counts(), state.negative_store_offset()),
        [
            (-208, 7),
            (-207, 1),
            (-33, 3),
            (-1, 3),
            (0, 1),
            (130, 3),
            (1000, 1)
        ]
    );
    assert_eq!((state.zero_count(), state.get_count()), (16, 4295033384));
    assert_eq!(state.sum(), sum);
    assert_eq!(state.min(), Some(-r(1000)));
    assert_eq!(state.max(), Some(r(400)));
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
    assert_eq!(set.to_asapv1().expect("serialize"), want);

    let decoded = SetAggregator::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.values, set.values);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

fn delta_result_matches(golden: &str, added: &[&str], removed: &[&str]) {
    let want = decode_hex(golden);
    let delta = SetAggregatorDelta {
        added: set_of(added),
        removed: set_of(removed),
    };
    assert_eq!(delta.to_asapv1().expect("serialize"), want);

    let decoded = SetAggregatorDelta::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.added, delta.added);
    assert_eq!(decoded.removed, delta.removed);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
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
// Coco: the bucket table set directly -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

/// The occupied buckets `(row, col, key, value)`; each key sits in the column
/// its row hashes it to. Every other bucket is unoccupied.
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

#[test]
fn coco_3x7_matches_golden() {
    let want = decode_hex(GOLDEN_COCO);

    let mut sketch: CocoSketch = CocoSketch::init_with_size(7, 3);
    for (r, c, key, val) in COCO_CELLS {
        sketch.table[r][c] = CocoBucket {
            full_key: Some(key.to_string()),
            val,
        };
    }
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "Coco bytes diverge from golden");

    let decoded: CocoSketch = CocoSketch::from_asapv1(&want).expect("decode");
    assert_eq!((decoded.rows, decoded.cols), (3, 7));
    let mut expected = vec![(None, 0); 21];
    for (r, c, key, val) in COCO_CELLS {
        expected[r * 7 + c] = (Some(key.to_string()), val);
    }
    let cells: Vec<(Option<String>, u64)> = decoded
        .table
        .as_slice()
        .iter()
        .map(|b| (b.full_key.clone(), b.val))
        .collect();
    assert_eq!(cells, expected);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Elastic: heavy buckets and light counters set directly -> serialize ==
// golden, round-trips. The two fixtures differ only by `stale_copies`.
// ---------------------------------------------------------------------------

/// Heavy buckets `(flow_id, vote_pos, vote_neg, eviction)`.
const ELASTIC_HEAVY: [(&str, i32, i32, bool); 4] = [
    ("", 0, 0, true),
    ("10.0.0.1:443>192.168.10.20:5123", 127, 128, false),
    ("", 1, 65535, true),
    ("10.0.0.1:443>192.168.10.20:51234", 2147483647, 256, true),
];

const ELASTIC_LIGHT: [[i32; 4]; 2] = [[0, 255, 65536, 2147483647], [-1, -33, -32768, -2147483648]];

fn elastic_known_state(mut sketch: ElasticSketch) -> ElasticSketch {
    assert_eq!(sketch.heavy.len(), ELASTIC_HEAVY.len());
    for (bucket, &(id, pos, neg, eviction)) in sketch.heavy.iter_mut().zip(&ELASTIC_HEAVY) {
        *bucket = HeavyBucket {
            flow_id: id.to_string(),
            vote_pos: pos,
            vote_neg: neg,
            eviction,
        };
    }
    sketch.light =
        CountMinSketch::from_storage(Vector2D::from_fn(2, 4, |r, c| ELASTIC_LIGHT[r][c]));
    sketch
}

fn assert_elastic_golden(golden: &str, sketch: &ElasticSketch) {
    let want = decode_hex(golden);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded: ElasticSketch = ElasticSketch::from_asapv1(&want).expect("decode");
    let buckets: Vec<(&str, i32, i32, bool)> = decoded
        .heavy
        .iter()
        .map(|b| (b.flow_id.as_str(), b.vote_pos, b.vote_neg, b.eviction))
        .collect();
    assert_eq!(buckets, ELASTIC_HEAVY);
    assert_eq!(decoded.heavy_buckets, 4);
    let flat: Vec<i32> = ELASTIC_LIGHT.iter().flatten().copied().collect();
    assert_eq!(decoded.light.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn elastic_4b_2x4_matches_golden() {
    let sketch = elastic_known_state(ElasticSketch::with_heavy_and_light(4, 2, 4));
    assert_elastic_golden(GOLDEN_ELASTIC, &sketch);
}

/// A 2-bucket sketch expanded to 4 carries `stale_copies`.
#[test]
fn elastic_4b_2x4_stale_matches_golden() {
    let mut expanded: ElasticSketch = ElasticSketch::with_heavy_and_light(2, 2, 4);
    expanded.expand_heavy();
    assert_elastic_golden(GOLDEN_ELASTIC_STALE, &elastic_known_state(expanded));

    let (fresh, stale) = (decode_hex(GOLDEN_ELASTIC), decode_hex(GOLDEN_ELASTIC_STALE));
    assert_eq!(fresh.len(), stale.len());
    let differing: Vec<usize> = (0..fresh.len()).filter(|&i| fresh[i] != stale[i]).collect();
    assert_eq!(differing.len(), 1);
    assert_eq!((fresh[differing[0]], stale[differing[0]]), (0xc2, 0xc3));
}

// ---------------------------------------------------------------------------
// UnivMon: 3 layers of 2x4 CountSketchWithL2Norm, heap capacity 5. Cells are written through
// `apply_delta`, heap entries through `update` -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

/// `new`'s `(heap_size, rows, cols, layers)`.
const UNIVMON_SHAPE: (usize, usize, usize, usize) = (5, 2, 4, 3);

const UNIVMON_LAYERS: [[[i64; 4]; 2]; 3] = [
    CS_VALS,
    [[3, -2, 0, 1], [0, 0, 5, -4]],
    [[0, 7, 0, 0], [-6, 0, 0, 0]],
];

/// Entries per layer's heap, and the heap counts in emitted order.
const UNIVMON_HEAP_LENS: [usize; 3] = [3, 1, 2];
const UNIVMON_HEAP_COUNTS: [i64; 6] = [65536, 300, 128, 5, 9, 2];

const UNIVMON_STR_KEYS: [&str; 6] = ["alpha", "beta", "delta", "gamma", "epsilon", "zeta"];
const UNIVMON_I64_KEYS: [i64; 6] = [i64::MIN, -1, -129, 128, 4_294_967_296, 7];

fn univmon_empty() -> UnivMon {
    let (heap_size, rows, cols, layers) = UNIVMON_SHAPE;
    UnivMon::new(heap_size, rows, cols, layers)
}

/// `keys` fill the heaps in emitted order; layer 1 is incomplete.
fn univmon_known(keys: [DataInput<'_>; 6]) -> UnivMon {
    let mut um = univmon_empty();
    for (layer, cells) in UNIVMON_LAYERS.iter().enumerate() {
        for (row, values) in cells.iter().enumerate() {
            for (col, &value) in values.iter().enumerate() {
                um.layer_sketches[layer].apply_delta(CountSketchCellDelta {
                    row: row as u32,
                    col: col as u32,
                    count: value as i32,
                });
            }
        }
    }
    let layers = UNIVMON_HEAP_LENS
        .iter()
        .enumerate()
        .flat_map(|(layer, &len)| std::iter::repeat_n(layer, len));
    for ((layer, key), count) in layers.zip(keys.iter()).zip(UNIVMON_HEAP_COUNTS) {
        um.layer_heaps[layer].update(key, count);
    }
    um.set_total_weight(70_000);
    um.mark_layer_candidates_incomplete(1);
    um
}

fn univmon_counts(um: &UnivMon) -> Vec<i64> {
    (0..um.layers)
        .flat_map(|layer| {
            let L2HH::COUNT(counter) = &um.layer_sketches[layer];
            counter.as_storage().as_slice().to_vec()
        })
        .collect()
}

/// Each layer's heap as sorted `(key, count)`.
fn univmon_heaps(um: &UnivMon) -> Vec<Vec<(String, i64)>> {
    (0..um.layers)
        .map(|layer| {
            let mut entries: Vec<(String, i64)> = um.layer_heaps[layer]
                .heap()
                .iter()
                .map(|item| (format!("{:?}", item.key), item.count))
                .collect();
            entries.sort();
            entries
        })
        .collect()
}

fn assert_univmon_golden(golden: &str, sketch: &UnivMon) {
    let want = decode_hex(golden);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = UnivMon::from_asapv1(&want).expect("decode");
    let shape = (
        decoded.heap_size,
        decoded.rows,
        decoded.cols,
        decoded.layers,
    );
    assert_eq!(shape, UNIVMON_SHAPE);
    assert_eq!(decoded.total_weight, sketch.total_weight);
    assert_eq!(decoded.candidates_complete(), sketch.candidates_complete());
    assert_eq!(univmon_counts(&decoded), univmon_counts(sketch));
    for layer in 0..decoded.layers {
        assert_eq!(
            decoded.layer_sketches[layer].l2(),
            sketch.layer_sketches[layer].l2()
        );
    }
    assert_eq!(univmon_heaps(&decoded), univmon_heaps(sketch));
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn univmon_str_l3_2x4_h5_matches_golden() {
    let sketch = univmon_known(UNIVMON_STR_KEYS.map(DataInput::Str));
    assert_univmon_golden(GOLDEN_UNIVMON_STR, &sketch);
}

#[test]
fn univmon_i64_l3_2x4_h5_matches_golden() {
    let sketch = univmon_known(UNIVMON_I64_KEYS.map(DataInput::I64));
    assert_univmon_golden(GOLDEN_UNIVMON_I64, &sketch);
}

#[test]
fn univmon_empty_l3_2x4_h5_matches_golden() {
    assert_univmon_golden(GOLDEN_UNIVMON_EMPTY, &univmon_empty());
}

// ---------------------------------------------------------------------------
// CountSketchWithL2Norm: counts, l2 accumulators and seed index set through the sketch's
// serde form -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

const L2HH_COUNTS: [[i64; 4]; 2] = [[127, 128, 65535, -32768], [-32, -33, -2147483648, i64::MIN]];
/// Set independently of the counts.
const L2HH_L2: [i64; 2] = [65536, i64::MAX];
const L2HH_SEED_INDEX: usize = 7;

#[test]
fn count_l2hh_2x4_seed7_matches_golden() {
    let want = decode_hex(GOLDEN_L2HH);

    let flat: Vec<i64> = L2HH_COUNTS.iter().flatten().copied().collect();
    let state = ((&flat, 2, 4), (L2HH_L2,), 2, 4, L2HH_SEED_INDEX);
    let sketch: CountSketchWithL2Norm =
        rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).expect("valid state");
    let got = sketch.to_asapv1().expect("serialize");
    assert_eq!(got, want, "CountSketchWithL2Norm bytes diverge from golden");

    let decoded: CountSketchWithL2Norm = CountSketchWithL2Norm::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!((decoded.rows(), decoded.cols()), (2, 4));
    assert_eq!(decoded.seed_index(), L2HH_SEED_INDEX);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Hydra: one fixture per counter variant, each cell's state set directly ->
// serialize == golden, round-trips.
// ---------------------------------------------------------------------------

const HYDRA_SCHEMA: [&str; 2] = ["region", "service"];

/// A grid over [`HYDRA_SCHEMA`] holding `cells` row-major.
fn hydra_grid(
    rows: usize,
    cols: usize,
    prototype: HydraCounter,
    cells: Vec<HydraCounter>,
) -> Hydra {
    let mut hydra = Hydra::with_schema(rows, cols, HYDRA_SCHEMA, prototype).expect("valid schema");
    let mut cells = cells.into_iter();
    hydra.cells = Vector2D::from_fn(rows, cols, |_, _| cells.next().expect("one cell each"));
    assert!(cells.next().is_none(), "one cell per grid position");
    hydra
}

fn assert_same_grid(got: &Hydra, want: &Hydra) {
    assert_eq!((got.rows, got.cols), (want.rows, want.cols));
    assert_eq!(got.schema(), want.schema());
    let cells = got.cells.as_slice().iter().zip(want.cells.as_slice());
    for (i, (g, w)) in cells.enumerate() {
        let same = match (g, w) {
            (HydraCounter::Kll(g), HydraCounter::Kll(w)) => {
                (g.wire_k(), g.m(), g.level_bounds_top_down(), g.coin_state())
                    == (w.wire_k(), w.m(), w.level_bounds_top_down(), w.coin_state())
                    && g.items_top_down() == w.items_top_down()
            }
            (HydraCounter::CountMinSketch(g), HydraCounter::CountMinSketch(w)) => {
                g.as_storage().as_slice() == w.as_storage().as_slice()
            }
            (HydraCounter::CountSketch(g), HydraCounter::CountSketch(w)) => {
                g.as_storage().as_slice() == w.as_storage().as_slice()
            }
            (HydraCounter::Hll(g), HydraCounter::Hll(w)) => {
                g.registers_as_slice() == w.registers_as_slice()
            }
            (HydraCounter::UnivMon(g), HydraCounter::UnivMon(w)) => {
                g.to_asapv1().unwrap() == w.to_asapv1().unwrap()
            }
            _ => false,
        };
        assert!(same, "cell {i} differs after decode");
    }
}

fn assert_hydra_golden(golden: &str, fixture: Hydra) {
    let want = decode_hex(golden);
    assert_eq!(fixture.to_asapv1().expect("serialize"), want);

    let decoded = Hydra::from_asapv1(&want).expect("decode");
    assert_same_grid(&decoded, &fixture);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn hydra_kll_2x2_k200_matches_golden() {
    let cell = |seed: u64, values: &[f64]| {
        let mut kll = Kll::with_k_and_seed(200, seed);
        for v in values {
            kll.update(v);
        }
        HydraCounter::Kll(kll)
    };
    let cells = vec![
        cell(1, &[1.0, 2.0, 3.0, 4.0, 5.0]),
        cell(2, &[]),
        cell(3, &[2.5, -1.0, 0.0, 1e300, -0.125]),
        cell(4, &[3.0e-5]),
    ];
    let prototype = HydraCounter::Kll(Kll::with_k_and_seed(200, 0));
    assert_hydra_golden(GOLDEN_HYDRA_KLL, hydra_grid(2, 2, prototype, cells));
}

#[test]
fn hydra_cm_2x2_counter_2x2_matches_golden() {
    const CELLS: [[[i32; 2]; 2]; 4] = [
        [[0, 1], [127, 128]],
        [[255, 256], [300, 65535]],
        [[65536, 1_000_000], [i32::MAX, 0]],
        [[0, 0], [0, 0]],
    ];
    let cells = CELLS
        .iter()
        .map(|m| {
            HydraCounter::CountMinSketch(CountMinSketch::from_storage(Vector2D::from_fn(
                2,
                2,
                |r, c| m[r][c],
            )))
        })
        .collect();
    let prototype = HydraCounter::CountMinSketch(CountMinSketch::with_dimensions(2, 2));
    assert_hydra_golden(GOLDEN_HYDRA_CM, hydra_grid(2, 2, prototype, cells));
}

#[test]
fn hydra_cs_2x2_counter_2x2_matches_golden() {
    const CELLS: [[[i32; 2]; 2]; 4] = [
        [[0, -1], [127, -32]],
        [[-33, 128], [-128, -129]],
        [[-32768, 65536], [-32769, i32::MAX]],
        [[i32::MIN, 1], [0, 0]],
    ];
    let cells = CELLS
        .iter()
        .map(|m| {
            HydraCounter::CountSketch(CountSketch::from_storage(Vector2D::from_fn(
                2,
                2,
                |r, c| m[r][c],
            )))
        })
        .collect();
    let prototype = HydraCounter::CountSketch(CountSketch::with_dimensions(2, 2));
    assert_hydra_golden(GOLDEN_HYDRA_CS, hydra_grid(2, 2, prototype, cells));
}

/// A P14 Ertl-MLE HLL whose registers are exactly `set`. Each register is
/// written by a hash whose top 14 bits name the index and whose leading zeros
/// below them give the value.
fn hll_p14_with(set: &[(usize, u8)]) -> HyperLogLog<Ertl> {
    let mut hll = HyperLogLog::<Ertl>::default();
    let mut want = vec![0u8; 1 << 14];
    for &(index, value) in set {
        let rank_bit = if value == 51 { 0 } else { 1u64 << (50 - value) };
        hll.update_with_hash(((index as u64) << 50) | rank_bit);
        want[index] = value;
    }
    assert_eq!(hll.registers_as_slice(), want.as_slice());
    hll
}

#[test]
fn hydra_hll_1x2_p14_matches_golden() {
    let cells = vec![
        HydraCounter::Hll(hll_p14_with(&[(0, 1), (1, 7), (100, 42), (16383, 3)])),
        HydraCounter::Hll(hll_p14_with(&[(0, 2), (8192, 51)])),
    ];
    let prototype = HydraCounter::Hll(HyperLogLog::<Ertl>::default());
    assert_hydra_golden(GOLDEN_HYDRA_HLL, hydra_grid(1, 2, prototype, cells));
}

/// Cell 0: 2 layers of 1x2 counters, heap capacity 2, layer 0 incomplete,
/// total weight 7. Cell 1 is empty.
#[test]
fn hydra_univmon_1x2_matches_golden() {
    let fresh = || UnivMon::new(2, 1, 2, 2);
    let mut um = fresh();
    for (layer, col, value) in [(0usize, 0u32, 5i32), (0, 1, -3), (1, 1, 2)] {
        um.layer_sketches[layer].apply_delta(CountSketchCellDelta {
            row: 0,
            col,
            count: value,
        });
    }
    um.layer_heaps[0].update(&DataInput::U64(7), 5);
    um.layer_heaps[0].update(&DataInput::U64(300), 2);
    um.layer_heaps[1].update(&DataInput::U64(4_294_967_296), 2);
    um.set_total_weight(7);
    um.mark_layer_candidates_incomplete(0);
    let cells = vec![HydraCounter::UnivMon(um), HydraCounter::UnivMon(fresh())];
    let prototype = HydraCounter::UnivMon(fresh());
    assert_hydra_golden(GOLDEN_HYDRA_UNIVMON, hydra_grid(1, 2, prototype, cells));
}

// ---------------------------------------------------------------------------
// admission_rate: a sampled state's metadata closes with the key; the state is
// the unsampled fixture's, decoded back with its rate.
// ---------------------------------------------------------------------------

#[test]
fn hll_classic_p12_admission_matches_golden() {
    let want = decode_hex(GOLDEN_CLASSIC_ADMISSION);
    let state = HyperLogLogP12::<Classic>::from_asapv1(&decode_hex(GOLDEN_CLASSIC)).expect("state");
    let sketch = state.with_admission_rate(0.5);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = HyperLogLogP12::<Classic>::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.registers_as_slice(), p12_registers().as_slice());
    assert_eq!(decoded.admission_rate(), 0.5);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn cms_i64_regular_2x3_admission_matches_golden() {
    type Cms = CountMinSketch<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CMS_ADMISSION);
    let sketch = Cms::from_storage(Vector2D::from_fn(2, 3, |r, c| I64_VALS[r][c]));
    let sketch = sketch.with_admission_rate(0.1);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Cms::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = I64_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.admission_rate(), 0.1);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn cs_i64_regular_2x4_admission_matches_golden() {
    type Cs = CountSketch<Vector2D<i64>, RegularPath>;
    let want = decode_hex(GOLDEN_CS_ADMISSION);
    let sketch = Cs::from_storage(Vector2D::from_fn(2, 4, |r, c| CS_VALS[r][c]));
    let sketch = sketch.with_admission_rate(0.25);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Cs::from_asapv1(&want).expect("decode");
    let flat: Vec<i64> = CS_VALS.iter().flatten().copied().collect();
    assert_eq!(decoded.as_storage().as_slice(), flat.as_slice());
    assert_eq!(decoded.admission_rate(), 0.25);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn ddsketch_signed_a001_admission_matches_golden() {
    let scalars = (2523.90625, -0.016, 515.0);
    let sketch = dd_state((&[3, 0, 2], 310), (&[5, 1], -208), 7, scalars);
    assert_dd_golden(GOLDEN_DD_ADMISSION, &sketch.with_admission_rate(0.125));
    let decoded = DdSketch::from_asapv1(&decode_hex(GOLDEN_DD_ADMISSION)).expect("decode");
    assert_eq!(decoded.admission_rate(), 0.125);
    assert_eq!((decoded.get_count(), decoded.sum()), (18, 2523.90625));
}

// ---------------------------------------------------------------------------
// KMV: digests inserted directly -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

#[cfg(feature = "experimental")]
#[test]
fn kmv_k8_matches_golden() {
    let want = decode_hex(GOLDEN_KMV);
    let digests = [1u64, 127, 128, 65535, 4294967296, 0xfedc_ba98_7654_3210];
    let mut sketch: Kmv = Kmv::new(8);
    for digest in [128, 0xfedc_ba98_7654_3210, 1, 4294967296, 127, 65535] {
        sketch.update_with_hash(digest);
    }
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded: Kmv = Kmv::from_asapv1(&want).expect("decode");
    assert_eq!(decoded.k, 8);
    let mut held: Vec<u64> = decoded.retained_hashes.iter().copied().collect();
    held.sort_unstable();
    assert_eq!(held, digests);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Bloom: the bit grid set through the filter's serde form -> serialize ==
// golden, round-trips. Column `c` of row `r` is bit `c % 64` of word
// `r * ceil(cols / 64) + c / 64`.
// ---------------------------------------------------------------------------

fn assert_bloom_golden<Mode: asap_sketchlib::BloomMode>(
    golden: &str,
    (rows, cols, mode): (usize, usize, &str),
    words: &[u64],
    inserted: u64,
) {
    let want = decode_hex(golden);
    let state = ((words, rows, cols), inserted, mode);
    let sketch: Bloom<Mode> =
        rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).expect("valid state");
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = Bloom::<Mode>::from_asapv1(&want).expect("decode");
    assert_eq!((decoded.rows(), decoded.cols()), (rows, cols));
    assert_eq!(decoded.inserted(), inserted);
    let stride = cols.div_ceil(64);
    for r in 0..rows {
        for c in 0..cols {
            let bit = words[r * stride + c / 64] >> (c % 64) & 1 == 1;
            assert_eq!(decoded.as_bits().get(r, c), bit, "bit ({r}, {c})");
        }
    }
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

#[test]
fn bloom_regular_3x128_matches_golden() {
    let words = [1, 1 << 63, 0, 0xff, u64::MAX, 0x10000];
    assert_bloom_golden::<RegularPath>(GOLDEN_BLOOM_REGULAR, (3, 128, "regular"), &words, 300);
}

#[test]
fn bloom_fast_5x32_matches_golden() {
    let words = [1, 0x8000_0000, 0, 0xffff_ffff, 0x10000];
    assert_bloom_golden::<FastPath>(GOLDEN_BLOOM_FAST, (5, 32, "fast"), &words, 9);
}

// ---------------------------------------------------------------------------
// Space-Saving: the triples set through the summary's serde form, seated out
// of emitted order -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

fn assert_space_saving_golden(
    golden: &str,
    capacity: usize,
    total: u64,
    discarded_max: u64,
    emitted: &[(OwnedDataInput, u64, u64)],
    min_count: u64,
) {
    let want = decode_hex(golden);
    let mut seated = emitted.to_vec();
    seated.reverse();
    let state = (capacity, total, discarded_max, seated);
    let sketch: SpaceSaving =
        rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).expect("valid state");
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded: SpaceSaving = SpaceSaving::from_asapv1(&want).expect("decode");
    assert_eq!((decoded.capacity(), decoded.total()), (capacity, total));
    assert_eq!(decoded.min_count(), min_count);
    let mut held = decoded.entries();
    held.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    let mut expected = emitted.to_vec();
    expected.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(held, expected);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// The count-5 tie emits by two's-complement bits: `0, 1, i64::MIN, -129`.
#[test]
fn space_saving_i64_c8_matches_golden() {
    let emitted = [
        (-1, 70000, 0),
        (300, 9, 2),
        (0, 5, 0),
        (1, 5, 1),
        (i64::MIN, 5, 4),
        (-129, 5, 0),
    ]
    .map(|(key, count, error)| (OwnedDataInput::I64(key), count, error));
    assert_space_saving_golden(GOLDEN_SPACE_SAVING_I64, 8, 70030, 4, &emitted, 4);
}

/// A full summary: `min_count` is its smallest count. `b""` precedes `b"ab"`.
#[test]
fn space_saving_bytes_c3_matches_golden() {
    let emitted = [(&b"\xff\x00"[..], 9, 0), (b"", 4, 1), (b"ab", 4, 0)]
        .map(|(key, count, error)| (OwnedDataInput::Bytes(key.to_vec()), count, error));
    assert_space_saving_golden(GOLDEN_SPACE_SAVING_BYTES, 3, 20, 3, &emitted, 4);
}

// ---------------------------------------------------------------------------
// UniformSampling: entries and running scalars set through the sampler's serde
// form -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

/// The priority-3 tie orders `-0.0` before `0.0`; a `NaN` sample is legal.
#[cfg(feature = "experimental")]
#[test]
fn uniform_sampling_r0p25_matches_golden() {
    let want = decode_hex(GOLDEN_UNIFORM);
    let entries = [
        (3u64, -0.0f64),
        (3, 0.0),
        (1 << 63, 1e300),
        (u64::MAX, f64::NAN),
    ];
    let state = (0.25f64, 20u64, 0x0123_4567_89ab_cdefu64, entries);
    let sketch: UniformSampling =
        rmp_serde::from_slice(&rmp_serde::to_vec(&state).unwrap()).expect("valid state");
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = UniformSampling::from_asapv1(&want).expect("decode");
    assert_eq!((decoded.sample_rate(), decoded.total_seen()), (0.25, 20));
    let bits: Vec<u64> = decoded.samples().iter().map(|v| v.to_bits()).collect();
    let expected: Vec<u64> = entries.iter().map(|(_, v)| v.to_bits()).collect();
    assert_eq!(bits, expected);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// Two-tier UnivMon: 5 layers, elephant tier 2 layers of 3x8, mouse tier 1x4,
// heap capacity 6. Cells through `apply_delta`, heap entries through `update`
// -> serialize == golden, round-trips.
// ---------------------------------------------------------------------------

/// Each layer's rows, and each layer's heap `(key, count)`.
fn pyramid_layers() -> Vec<Vec<Vec<i32>>> {
    vec![
        vec![
            vec![0, 127, -1, 0, 0, 128, 0, -33],
            vec![65536, 0, 0, 0, 0, 0, 0, -32768],
            vec![0, 0, 0, 0, 0, 0, 300, 0],
        ],
        vec![
            vec![5, 0, 0, 0, 0, 0, 0, 0],
            vec![0, 0, -3, 0, 0, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 0, 2],
        ],
        vec![vec![0, 4, 0, -2]],
        vec![vec![1, 0, 0, 0]],
        vec![vec![0, 0, 0, 0]],
    ]
}

const PYRAMID_HEAPS: [&[(&str, i64)]; 5] = [
    &[("gamma", 7), ("alpha", 40), ("beta", 7)],
    &[("delta", 3)],
    &[],
    &[("eps", 1)],
    &[],
];

#[test]
fn univmon_optimized_l5_e2_3x8_1x4_h6_matches_golden() {
    let want = decode_hex(GOLDEN_PYRAMID);
    let mut pyramid = UnivMonTwoTier::new(6, 2, 3, 8, 1, 4, 5);
    for (layer, rows) in pyramid_layers().iter().enumerate() {
        for (row, values) in rows.iter().enumerate() {
            for (col, &value) in values.iter().enumerate() {
                pyramid.layer_sketches[layer].apply_delta(CountSketchCellDelta {
                    row: row as u32,
                    col: col as u32,
                    count: value,
                });
            }
        }
        for &(key, count) in PYRAMID_HEAPS[layer] {
            pyramid.layer_heaps[layer].update(&DataInput::Str(key), count);
        }
    }
    pyramid.total_weight = 70;
    assert_eq!(pyramid.to_asapv1().expect("serialize"), want);

    let decoded = UnivMonTwoTier::from_asapv1(&want).expect("decode");
    let shape = (
        decoded.layers,
        decoded.elephant_layers,
        decoded.elephant_rows,
        decoded.elephant_cols,
        decoded.mouse_rows,
        decoded.mouse_cols,
        decoded.heap_size,
        decoded.total_weight,
    );
    assert_eq!(shape, (5, 2, 3, 8, 1, 4, 6, 70));
    for (layer, rows) in pyramid_layers().iter().enumerate() {
        let L2HH::COUNT(counter) = &decoded.layer_sketches[layer];
        let flat: Vec<i64> = rows.iter().flatten().map(|&v| i64::from(v)).collect();
        assert_eq!(counter.as_storage().as_slice(), flat.as_slice());
        let mut heap: Vec<(String, i64)> = decoded.layer_heaps[layer]
            .heap()
            .iter()
            .map(|item| (format!("{:?}", item.key), item.count))
            .collect();
        heap.sort();
        let mut expected: Vec<(String, i64)> = PYRAMID_HEAPS[layer]
            .iter()
            .map(|&(key, count)| {
                (
                    format!("{:?}", OwnedDataInput::String(key.to_string())),
                    count,
                )
            })
            .collect();
        expected.sort();
        assert_eq!(heap, expected, "layer {layer} heap");
    }
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// UnivMon-Q: decode the golden, check the state through the public accessors,
// re-serialize byte-identically. Its fields are private, so the known state is
// reached by decoding; each candidate key hashes into the layer it sits in.
// ---------------------------------------------------------------------------

#[test]
fn univmon_q_i32_l3_c8_r3_matches_golden() {
    let want = decode_hex(GOLDEN_UNIVMON_Q);
    let decoded = UnivMonQ::<DefaultXxHasher>::from_asapv1(&want).expect("decode");
    let config = decoded.config();
    let expected = UnivMonQConfig {
        layers: 3,
        top_layer_cols: 8,
        cols_halving_period: 1,
        rows: 3,
        counter_bits: 32,
        heap_size: 2,
        ordered_samples: 3,
        seed_index: 5,
    };
    assert_eq!(config, expected);
    assert_eq!(decoded.source_id(), 7);
    assert_eq!(decoded.count(), 12);
    assert_eq!((decoded.min(), decoded.max()), (Some(-2.5), Some(5.0)));
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

// ---------------------------------------------------------------------------
// EhBucketSketch and ExponentialHistogram: nested blocks are the variant's own
// metadata and payload, verbatim.
// ---------------------------------------------------------------------------

type EhCm = CountMinSketch<Vector2D<i32>, FastPath>;

fn eh_cm(cells: [[i32; 4]; 2]) -> EhBucketSketch {
    EhBucketSketch::CountMinSketch(EhCm::from_storage(Vector2D::from_fn(2, 4, |r, c| {
        cells[r][c]
    })))
}

fn eh_cm_cells(sketch: &EhBucketSketch) -> Vec<i32> {
    match sketch {
        EhBucketSketch::CountMinSketch(cm) => cm.as_storage().as_slice().to_vec(),
        other => panic!("expected a Count-Min, got {}", other.sketch_type()),
    }
}

#[test]
fn ehsketchlist_cm_i32_fast_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_EHSL_CM);
    let cells = [[127, -1, 0, 65536], [-32768, 128, i32::MAX, i32::MIN]];
    assert_eq!(eh_cm(cells).to_asapv1().expect("serialize"), want);

    let decoded = EhBucketSketch::from_asapv1(&want).expect("decode");
    let flat: Vec<i32> = cells.iter().flatten().copied().collect();
    assert_eq!(eh_cm_cells(&decoded), flat);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

/// The nested blocks are `ddsketch_signed_a001`'s metadata and payload.
#[test]
fn ehsketchlist_ddsketch_a001_matches_golden() {
    let want = decode_hex(GOLDEN_EHSL_DD);
    let dd = DdSketch::from_asapv1(&decode_hex(GOLDEN_DD_SIGNED)).expect("dd");
    let sketch = EhBucketSketch::DdSketch(dd);
    assert_eq!(sketch.to_asapv1().expect("serialize"), want);

    let decoded = EhBucketSketch::from_asapv1(&want).expect("decode");
    let EhBucketSketch::DdSketch(inner) = &decoded else {
        panic!("expected a DDSketch, got {}", decoded.sketch_type());
    };
    assert_eq!(
        inner.to_asapv1().expect("inner"),
        decode_hex(GOLDEN_DD_SIGNED)
    );
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
}

const EH_BUCKETS: [([[i32; 4]; 2], usize, u64, u64); 2] = [
    ([[1, 0, 2, 0], [0, 3, 0, 0]], 3, 10, 30),
    ([[0, 0, 0, 1], [1, 0, 0, 0]], 1, 40, 40),
];

#[test]
fn eh_w100_k2_cm_2x4_matches_golden() {
    let want = decode_hex(GOLDEN_EH);
    let mut eh = ExponentialHistogram::new(2, 100, eh_cm([[0; 4]; 2]));
    eh.payload = EH_BUCKETS
        .iter()
        .map(|&(cells, size, min_time, max_time)| EhBucket {
            bucket: eh_cm(cells),
            size,
            l2_mass: 0.0,
            min_time,
            max_time,
        })
        .collect();
    assert_eq!(eh.to_asapv1().expect("serialize"), want);

    let decoded = ExponentialHistogram::from_asapv1(&want).expect("decode");
    assert_eq!((decoded.window, decoded.k), (100, 2));
    assert_eq!(decoded.payload.len(), EH_BUCKETS.len());
    for (bucket, &(cells, size, min_time, max_time)) in decoded.payload.iter().zip(&EH_BUCKETS) {
        let flat: Vec<i32> = cells.iter().flatten().copied().collect();
        assert_eq!(eh_cm_cells(&bucket.bucket), flat);
        assert_eq!(
            (bucket.size, bucket.min_time, bucket.max_time),
            (size, min_time, max_time)
        );
    }
    assert_eq!(eh_cm_cells(&decoded.prototype), vec![0; 8]);
    assert_eq!(decoded.to_asapv1().expect("re-serialize"), want);
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
