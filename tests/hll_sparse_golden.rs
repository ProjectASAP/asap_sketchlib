//! Cross-language golden test for the HLL sparse full-state encoding.
//!
//! The fixtures under `src/message_pack_format/portable/testdata/` were
//! produced by the Go encoder (`sketchlib-go`
//! `sketches/HLL/golden_test.go::TestGenerateGoldenFixtures`). For each sample
//! cardinality there are two files:
//!
//! - `hll_sparse_<card>.pb.hex`: hex of a proto-marshaled SketchEnvelope carrying a SPARSE-encoded HyperLogLogState.
//! - `hll_sparse_<card>.regs.txt`: the ground-truth non-zero registers, one `index:value` line per register.
//!
//! This test decodes the Go-written bytes through the Rust dual-read path
//! (`registers_from_state`) and asserts the reconstructed dense register array
//! matches the ground truth EXACTLY. It guards wire compatibility: any drift in
//! the sparse layout between the Go encoder and the Rust decoder fails here.

use asap_sketchlib::message_pack_format::portable::hll::{
    encode_sparse_registers, registers_from_state,
};
use asap_sketchlib::proto::sketchlib::{SketchEnvelope, sketch_envelope::SketchState};
use prost::Message;

const CARDS: &[u32] = &[0, 50, 1000, 5000];

fn testdata_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/message_pack_format/portable/testdata")
}

fn decode_hex(s: &str) -> Vec<u8> {
    let s = s.trim();
    s.as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = hex_nibble(pair[0]);
            let lo = hex_nibble(pair[1]);
            (hi << 4) | lo
        })
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

/// Parse a `regs.txt` body into the expected dense register array of length n.
fn expected_dense(regs_txt: &str, n: usize) -> Vec<u8> {
    let mut regs = vec![0u8; n];
    for line in regs_txt.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (idx, val) = line
            .split_once(':')
            .unwrap_or_else(|| panic!("malformed regs line {line:?}"));
        let idx: usize = idx.parse().expect("index parse");
        let val: u8 = val.parse().expect("value parse");
        regs[idx] = val;
    }
    regs
}

#[test]
fn go_sparse_fixtures_decode_exact() {
    let dir = testdata_dir();
    for &card in CARDS {
        let hex_path = dir.join(format!("hll_sparse_{card}.pb.hex"));
        let regs_path = dir.join(format!("hll_sparse_{card}.regs.txt"));

        let pb = decode_hex(
            &std::fs::read_to_string(&hex_path)
                .unwrap_or_else(|e| panic!("read {}: {e}", hex_path.display())),
        );
        let regs_txt = std::fs::read_to_string(&regs_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", regs_path.display()));

        let env = SketchEnvelope::decode(pb.as_slice())
            .unwrap_or_else(|e| panic!("card={card}: decode envelope: {e}"));
        let state = match env.sketch_state {
            Some(SketchState::Hll(s)) => s,
            other => panic!("card={card}: expected HLL state, got {other:?}"),
        };

        // The Go encoder must have emitted SPARSE for these (below-crossover)
        // cardinalities: registers (dense, tag 3) empty, registers_sparse set.
        assert!(
            state.registers.is_empty(),
            "card={card}: expected empty dense field (sparse encoding)"
        );
        assert!(
            state.registers_sparse.is_some(),
            "card={card}: expected sparse field to be present"
        );

        let n = 1usize << state.precision;
        let got = registers_from_state(&state)
            .unwrap_or_else(|e| panic!("card={card}: registers_from_state: {e}"));
        let want = expected_dense(&regs_txt, n);

        assert_eq!(got.len(), n, "card={card}: register length");
        assert_eq!(
            got, want,
            "card={card}: Rust-decoded sparse registers diverge from Go ground truth"
        );
    }
}

/// Reverse direction: the Rust sparse ENCODER must produce byte-identical
/// `registers_sparse.packed` to what Go wrote, for the same register array.
/// This locks the encoder in both languages, not just the decoder.
#[test]
fn rust_encoder_matches_go_packed_bytes() {
    let dir = testdata_dir();
    for &card in CARDS {
        let hex_path = dir.join(format!("hll_sparse_{card}.pb.hex"));
        let pb = decode_hex(&std::fs::read_to_string(&hex_path).unwrap());
        let env = SketchEnvelope::decode(pb.as_slice()).unwrap();
        let state = match env.sketch_state {
            Some(SketchState::Hll(s)) => s,
            other => panic!("expected HLL state, got {other:?}"),
        };
        let go_sparse = state.registers_sparse.clone().expect("sparse present");

        // Reconstruct dense, re-encode in Rust, compare packed bytes.
        let n = 1usize << state.precision;
        let dense = registers_from_state(&state).unwrap();
        assert_eq!(dense.len(), n);
        let rust_sparse = encode_sparse_registers(&dense);

        assert_eq!(
            rust_sparse.num_registers, go_sparse.num_registers,
            "card={card}: num_registers mismatch"
        );
        assert_eq!(
            rust_sparse.packed, go_sparse.packed,
            "card={card}: Rust-encoded packed bytes diverge from Go"
        );
    }
}
