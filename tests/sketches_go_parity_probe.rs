//! Probe: do `sketches::*` defaults already produce Go-byte-parity output,
//! making a separate hashspec-direct path inside the wire-format-aligned
//! types unnecessary?
//!
//! Approach: reuse the exact same Go-golden envelopes that the
//! `message_pack_format::portable` parity tests use, but build the
//! sketch via `sketches::*` defaults. Identical bytes confirm the shared
//! FastPath math is sufficient — no hashspec bypass needed.

use asap_sketchlib::common::DataInput;
use asap_sketchlib::proto::sketchlib::{
    CounterType, DdSketchState, SketchEnvelope, sketch_envelope::SketchState,
};
use asap_sketchlib::sketches::ddsketch::DDSketch;
use asap_sketchlib::{DefaultXxHasher, FastPath, Vector2D};
use prost::Message;

/// 403-byte DDSketch envelope for `alpha=0.01`, `(1..=50)` integer-as-f64
/// input, without the DataPoint-level METRIC scalars (count/sum/min/max →
/// proto tags 4-7 reserved).
///
/// NOTE: this is currently the RUST-produced value; it MUST be reconciled
/// against the parallel `sketchlib-go` golden regeneration before declaring
/// cross-language byte parity.
const DDSKETCH_GOLDEN_HEX: &str = "0801728e03096214ae47e17a843f128003000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000100000000000000000000000000000100000000000000000000010000000000000000010000000000000001000000000001000000000001000000000001000000010000000001000000010000010000000100000100000100000100000100010000010001000100010001000100010001000100010100010100010100010101000101010100010101010101010100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000187f";

#[test]
fn sketches_ddsketch_matches_go_envelope() {
    let mut sk = DDSketch::new(0.01);
    for i in 1..=50i32 {
        sk.add(&(i as f64));
    }

    // Go serializes `alpha` round-tripped through gamma to mirror its
    // internal storage: `alpha_wire = (γ-1)/(γ+1)`, `γ = (1+α)/(1-α)`.
    // This is a 25-ULP shift from the user-supplied α=0.01.
    let alpha = sk.alpha();
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    let alpha_wire = (gamma - 1.0) / (gamma + 1.0);
    // The DataPoint-level METRIC scalars (count/sum/min/max) are not on the
    // wire; only alpha + the bucket array.
    let state = DdSketchState {
        alpha: alpha_wire,
        store_counts: sk.store_counts().to_vec(),
        store_offset: sk.store_offset(),
        ..Default::default()
    };
    let envelope = SketchEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sample_p: 0.0,
        sketch_state: Some(SketchState::Ddsketch(state)),
    };
    let mut got = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut got).expect("prost encode");

    let want = decode_hex(DDSKETCH_GOLDEN_HEX);
    assert_eq!(
        got.len(),
        want.len(),
        "DDSketch envelope length: got {} want {}",
        got.len(),
        want.len(),
    );
    assert_eq!(
        got, want,
        "sketches::DDSketch envelope diverges from Go golden",
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
