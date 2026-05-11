//! Probe: do `sketches::*` defaults already produce Go-byte-parity output,
//! making the parallel `wrapper::*` implementations redundant?
//!
//! Approach: reuse the exact same Go-golden envelopes that the wrapper
//! parity tests use, but build the matrix via `sketches::Count` /
//! `sketches::CountMin` with `FastPath` + `DefaultXxHasher` instead of
//! the wrapper's hashspec-direct path. Identical bytes → wrapper is
//! redundant.

use asap_sketchlib::common::DataInput;
use asap_sketchlib::proto::sketchlib::{
    CountMinState, CountSketchState, CounterType, SketchEnvelope, sketch_envelope::SketchState,
};
use asap_sketchlib::sketches::countminsketch::CountMin;
use asap_sketchlib::sketches::countsketch::Count;
use asap_sketchlib::{DefaultXxHasher, FastPath, Vector2D};
use prost::Message;

/// 1577-byte envelope captured from `sketchlib-go::CountSketch.SerializeProtoBytes`
/// for `(rows=3, cols=512)` × `goldenCsKeys()` (25 keys "k-a".."k-e", each
/// repeated 5×). Identical to the constant in
/// `src/wrapper/countsketch.rs::test_update_then_envelope_matches_sketchlib_go_bytes`.
const COUNTSKETCH_GOLDEN_HEX: &str = include_str!("cs_envelope_golden.hex");

/// 8275-byte CMS Frequency-Only golden, captured from
/// `sketchlib-go::CountMinSketch.SerializeProtoBytesFO` for
/// `(rows=4, cols=2048)` × `goldenCmsKeys()` (10 keys "flow-0".."flow-9",
/// each repeated 5×). Same file the wrapper test already uses.
const COUNTMIN_GOLDEN_HEX: &str = include_str!("../src/sketches/testdata/cms_envelope_golden.hex");

#[test]
fn sketches_count_fastpath_matches_go_count_sketch_envelope() {
    let rows = 3usize;
    let cols = 512usize;

    let mut sk: Count<Vector2D<i64>, FastPath, DefaultXxHasher> =
        Count::with_dimensions(rows, cols);
    for i in 0..25 {
        let key = format!("k-{}", (b'a' + (i % 5) as u8) as char);
        sk.insert_many(&DataInput::String(key), 1i64);
    }

    let storage = sk.as_storage();
    let mut counts_int: Vec<i64> = Vec::with_capacity(rows * cols);
    let mut l2: Vec<f64> = Vec::with_capacity(rows);
    for r in 0..rows {
        let mut row_l2 = 0.0f64;
        for c in 0..cols {
            let cell = *storage.get(r, c).expect("cell in range");
            counts_int.push(cell);
            row_l2 += (cell as f64) * (cell as f64);
        }
        l2.push(row_l2);
    }

    let state = CountSketchState {
        rows: rows as u32,
        cols: cols as u32,
        counter_type: CounterType::Int64 as i32,
        counts_int,
        counts_float: Vec::new(),
        l2,
        topk: None,
    };
    let envelope = SketchEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sketch_state: Some(SketchState::CountSketch(state)),
    };
    let mut got = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut got).expect("prost encode");

    let want = decode_hex(COUNTSKETCH_GOLDEN_HEX);
    assert_eq!(
        got, want,
        "sketches::Count<Vector2D<i64>, FastPath> envelope diverges from Go golden \
         ({} bytes got vs {} bytes want)",
        got.len(),
        want.len(),
    );
}

#[test]
fn sketches_countmin_fastpath_matches_go_count_min_envelope() {
    let rows = 4usize;
    let cols = 2048usize;

    let mut sk: CountMin<Vector2D<f64>, FastPath, DefaultXxHasher> =
        CountMin::with_dimensions(rows, cols);
    for i in 0..50u64 {
        let key = format!("flow-{}", i % 10);
        sk.insert_many(&DataInput::String(key), 1.0f64);
    }

    let storage = sk.as_storage();
    let mut counts_int: Vec<i64> = Vec::with_capacity(rows * cols);
    let mut l1: Vec<f64> = Vec::with_capacity(rows);
    let mut l2: Vec<f64> = Vec::with_capacity(rows);
    for r in 0..rows {
        let mut row_l1 = 0.0f64;
        let mut row_l2 = 0.0f64;
        for c in 0..cols {
            let cell = *storage.get(r, c).expect("cell in range");
            counts_int.push(cell as i64);
            row_l1 += cell;
            row_l2 += cell * cell;
        }
        l1.push(row_l1);
        l2.push(row_l2);
    }

    let state = CountMinState {
        rows: rows as u32,
        cols: cols as u32,
        counter_type: CounterType::Int64 as i32,
        counts_int,
        counts_float: Vec::new(),
        sum_counts: Vec::new(),
        sum2_counts: Vec::new(),
        l1,
        l2,
    };
    let envelope = SketchEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sketch_state: Some(SketchState::CountMin(state)),
    };
    let mut got = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut got).expect("prost encode");

    let want = decode_hex(COUNTMIN_GOLDEN_HEX);
    assert_eq!(
        got.len(),
        want.len(),
        "CountMin envelope length: got {} want {}",
        got.len(),
        want.len(),
    );
    assert_eq!(
        got, want,
        "sketches::CountMin<Vector2D<f64>, FastPath> envelope diverges from Go golden",
    );
}

fn decode_hex(s: &str) -> Vec<u8> {
    s.trim()
        .as_bytes()
        .chunks(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("non-hex byte {}", c as char),
    }
}
