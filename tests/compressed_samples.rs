//! Failing-first tests for `CompressedSamples`: every sample of one series in
//! one window, timestamps as delta of delta and values as XOR of their `f64`
//! bits (the Gorilla encoding), in a portable byte format.
//!
//! Format (the bytes every implementation must write):
//! - header, 24 bytes, little-endian: sample count `u32`, first timestamp
//!   `i64` (ms), first value's `f64` bits `u64`, payload length in bits `u32`;
//!   an empty block is 24 zero bytes;
//! - payload, for each sample after the first, its timestamp code then its
//!   value code, most significant bit first, the last byte zero-padded:
//!   - timestamp: `d = t - t_prev` (the first delta taken against
//!     `d_prev = 0`), `dod = d - d_prev`: `0` -> `0`; [-64, 63] -> `10` + 7
//!     bits; [-256, 255] -> `110` + 9; [-2048, 2047] -> `1110` + 12;
//!     otherwise `1111` + 64; two's complement;
//!   - value: `x = bits(v) ^ bits(v_prev)`: `0` -> `0`; else
//!     `lead = min(leading_zeros(x), 31)`, `trail = trailing_zeros(x)`; if a
//!     window `(L, T)` exists with `lead >= L` and `trail >= T` -> `10` + the
//!     `64 - L - T` bits `x >> T`; else `11` + `lead` (5 bits) + `width - 1`
//!     (6 bits, `width = 64 - lead - trail`) + the `width` bits `x >> trail`,
//!     and `(lead, trail)` becomes the window.
//!
//! Samples are kept in timestamp order: a sample older than the newest is
//! inserted in place, and one whose timestamp is already present replaces
//! that sample's value (the last write wins).

use asap_sketchlib::{CompressedSamples, CompressedSamplesFormatError};

/// Bit pattern of a value, so NaN payloads and signed zeros compare exactly.
fn bits_of(samples: &[(i64, f64)]) -> Vec<(i64, u64)> {
    samples.iter().map(|&(t, v)| (t, v.to_bits())).collect()
}

fn block_of(samples: &[(i64, f64)]) -> CompressedSamples {
    let mut block = CompressedSamples::new();
    for &(t, v) in samples {
        block.push(t, v);
    }
    block
}

fn decoded(block: &CompressedSamples) -> Vec<(i64, u64)> {
    block.iter().map(|(t, v)| (t, v.to_bits())).collect()
}

/// A deterministic stream of 64-bit words (splitmix64), for random doubles.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Timestamps whose delta of delta lands in every code bucket (0, 7, 9, 12
/// and 64 bits, both signs) and a 30-day gap.
fn irregular_timestamps() -> Vec<i64> {
    let mut times = vec![0i64, 1_000, 2_000, 3_000];
    let mut delta = 1_000i64;
    for dod in [5, -60, 200, -250, 1_500, -2_000, 3_000, -2_500, 0, 0] {
        delta += dod;
        let next = times.last().unwrap() + delta;
        times.push(next);
    }
    let gap = 30 * 24 * 3_600 * 1_000i64;
    let last = *times.last().unwrap();
    times.extend([last + gap, last + gap + 1_000, last + gap + 1_001]);
    times
}

/// Values covering the bit patterns XOR coding must keep: NaN with payloads
/// (a PromQL stale marker is `0x7ff0000000000002`), both zeros, both
/// infinities, subnormals, runs of equal values, and random doubles.
fn awkward_values() -> Vec<f64> {
    let mut values = vec![
        f64::from_bits(0x7ff0_0000_0000_0002),
        f64::NAN,
        f64::from_bits(0xfff8_0000_0000_0001),
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(1),
        f64::MIN_POSITIVE / 2.0,
        42.0,
        42.0,
        42.0,
        -1.5,
        f64::MAX,
        f64::MIN,
    ];
    let mut state = 862;
    values.extend((0..200).map(|_| f64::from_bits(splitmix64(&mut state))));
    values
}

/// K1. Every sample comes back bit for bit, from the block and from its bytes:
/// regular and irregular timestamps (every delta-of-delta code, a 30-day
/// gap) and values with NaN payloads, signed zeros, infinities, subnormals,
/// equal runs and random doubles. Pushes in shuffled order give the same
/// samples and bytes as in order, and a repeated timestamp keeps its last
/// value. A wrong timestamp code width, a sign error in two's complement, a
/// lost XOR window, or a value compared as `f64` (NaN != NaN, 0.0 == -0.0)
/// fails it.
#[test]
fn round_trip_is_bitwise() {
    let values = awkward_values();
    let regular: Vec<(i64, f64)> = values
        .iter()
        .enumerate()
        .map(|(i, &v)| (1_700_000_000_000 + 1_000 * i as i64, v))
        .collect();
    let irregular: Vec<(i64, f64)> = irregular_timestamps()
        .into_iter()
        .zip(values.iter().copied().cycle())
        .collect();
    for samples in [&regular, &irregular] {
        let block = block_of(samples);
        assert_eq!(block.len() as usize, samples.len());
        assert!(!block.is_empty());
        assert_eq!(decoded(&block), bits_of(samples));
        let restored = CompressedSamples::from_bytes(&block.to_bytes()).unwrap();
        assert_eq!(decoded(&restored), bits_of(samples));
        assert_eq!(restored.to_bytes(), block.to_bytes());
    }

    // Shuffled arrival: the block is the in-order block.
    let mut shuffled = regular.clone();
    let mut state = 7;
    for i in (1..shuffled.len()).rev() {
        let j = (splitmix64(&mut state) % (i as u64 + 1)) as usize;
        shuffled.swap(i, j);
    }
    let from_shuffled = block_of(&shuffled);
    assert_eq!(decoded(&from_shuffled), bits_of(&regular));
    assert_eq!(from_shuffled.to_bytes(), block_of(&regular).to_bytes());

    // A repeated timestamp keeps the last value written.
    let mut block = block_of(&[(0, 1.0), (1_000, 2.0), (2_000, 3.0)]);
    block.push(1_000, 9.0);
    assert_eq!(block.len(), 3);
    assert_eq!(
        decoded(&block),
        bits_of(&[(0, 1.0), (1_000, 9.0), (2_000, 3.0)])
    );

    // An empty block round-trips as an empty block.
    let empty = CompressedSamples::new();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let restored = CompressedSamples::from_bytes(&empty.to_bytes()).unwrap();
    assert!(restored.is_empty());
}

/// One sample `(1000 ms, 1.5)`: a header and no payload.
const ONE: &str = "01000000e803000000000000000000000000f83f00000000";

/// `(0, 2.0), (1000, 2.0), (2000, 2.0)`. Header: count 3, first time 0,
/// first value 0x4000000000000000, payload 19 bits. Payload bits:
/// - sample 2: `d = 1000`, `dod = 1000 - 0` in [-2048, 2047] -> `1110` +
///   `001111101000`; value XOR 0 -> `0`;
/// - sample 3: `dod = 0` -> `0`; value -> `0`;
///
/// `1110001111101000 0 0 0` = `11100011 11101000 000` -> bytes `e3 e8 00`.
const REGULAR: &str = "030000000000000000000000000000000000004013000000e3e800";

/// `(0, 1.0), (10, 1.0), (25, 2.0), (100, 3.0), (400, -0.0),
/// (3400, +inf), (3_000_003_400, NaN 0x7ff0000000000002)`: deltas of delta
/// 10, 5, 60 (7-bit codes), 225 (9-bit), 2700 and about 3e9 (64-bit; the
/// 12-bit code is in `REGULAR`), an equal value, and new XOR windows.
const MIXED: &str = "070000000000000000000000000000000000f03f2c0100008520b84afff3cd807387019801f0000000000000a8cbffde0000000165a0a491fdf000000010";

/// `(0, 1.0), (1000, 1.5), (2000, 1.25), (3000, 1.75)`: the third and
/// fourth values reuse the second's XOR window (`10` + window bits).
const VALUE_WINDOW_REUSE: &str = "040000000000000000000000000000000000f03f33000000e3e8d805b01d40";

/// An empty block: 24 zero bytes.
const EMPTY: &str = "000000000000000000000000000000000000000000000000";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// K2. Fixed inputs encode to fixed bytes, computed by a separate reference
/// implementation of the format in the module doc; the bytes decode back to
/// the inputs; malformed bytes are refused with the matching error. Any
/// deviation from the format (byte order, bit order, a code's width or
/// prefix, the first delta's base, the padding) changes the bytes, so a data
/// plane in another language that reads them would decode other samples.
#[test]
fn portable_format_golden_vectors() {
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let cases: [(&str, Vec<(i64, f64)>); 5] = [
        (ONE, vec![(1_000, 1.5)]),
        (REGULAR, vec![(0, 2.0), (1_000, 2.0), (2_000, 2.0)]),
        (
            MIXED,
            vec![
                (0, 1.0),
                (10, 1.0),
                (25, 2.0),
                (100, 3.0),
                (400, -0.0),
                (3_400, f64::INFINITY),
                (3_000_003_400, stale),
            ],
        ),
        (
            VALUE_WINDOW_REUSE,
            vec![(0, 1.0), (1_000, 1.5), (2_000, 1.25), (3_000, 1.75)],
        ),
        (EMPTY, vec![]),
    ];
    for (expected, samples) in cases {
        let block = block_of(&samples);
        assert_eq!(hex(&block.to_bytes()), expected, "{samples:?}");
        let restored = CompressedSamples::from_bytes(&unhex(expected)).unwrap();
        assert_eq!(decoded(&restored), bits_of(&samples));
    }

    // Shorter than a header.
    let truncated = &unhex(REGULAR)[..10];
    assert!(matches!(
        CompressedSamples::from_bytes(truncated),
        Err(CompressedSamplesFormatError::Truncated)
    ));
    // The header says 19 payload bits (3 bytes); 2 or 4 bytes follow.
    let regular = unhex(REGULAR);
    let short = &regular[..regular.len() - 1];
    assert!(matches!(
        CompressedSamples::from_bytes(short),
        Err(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    let mut long = regular.clone();
    long.push(0);
    assert!(matches!(
        CompressedSamples::from_bytes(&long),
        Err(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    // No samples, yet 8 payload bits and one payload byte.
    let mut no_samples = unhex(EMPTY);
    no_samples[20] = 8;
    no_samples.push(0);
    assert!(matches!(
        CompressedSamples::from_bytes(&no_samples),
        Err(CompressedSamplesFormatError::PayloadWithoutSamples)
    ));
}

/// K3. Merging blocks of one series: over disjoint time spans the merge is
/// the block of the concatenated samples (byte for byte, whichever block is
/// the receiver); over overlapping spans it is the block of the sorted union,
/// the second block's value winning on an equal timestamp. A merge that
/// appends bytes without re-coding the boundary, drops or duplicates a
/// sample, or keeps the first value on a tie fails it.
#[test]
fn merge_disjoint_concatenates_overlapping_resorts() {
    let earlier: Vec<(i64, f64)> = (0..50).map(|i| (i * 1_000, i as f64 * 0.5)).collect();
    let later: Vec<(i64, f64)> = (50..120).map(|i| (i * 1_000, (i as f64).sqrt())).collect();
    let all: Vec<(i64, f64)> = earlier.iter().chain(&later).copied().collect();
    let expected = block_of(&all).to_bytes();
    assert_eq!(
        block_of(&earlier).merge(&block_of(&later)).to_bytes(),
        expected
    );
    assert_eq!(
        block_of(&later).merge(&block_of(&earlier)).to_bytes(),
        expected
    );

    // Two shards' halves of one window, interleaved, with one tie at 10 s.
    let even: Vec<(i64, f64)> = (0..40).step_by(2).map(|i| (i * 500, i as f64)).collect();
    let mut odd: Vec<(i64, f64)> = (1..40).step_by(2).map(|i| (i * 500, -(i as f64))).collect();
    odd.push((10_000, 99.0));
    let merged = block_of(&even).merge(&block_of(&odd));
    let mut union: Vec<(i64, f64)> = even.iter().filter(|s| s.0 != 10_000).copied().collect();
    union.extend(&odd);
    union.sort_by_key(|s| s.0);
    assert_eq!(decoded(&merged), bits_of(&union));
    assert_eq!(merged.to_bytes(), block_of(&union).to_bytes());

    // Merging with an empty block changes nothing.
    let empty = CompressedSamples::new();
    assert_eq!(
        block_of(&earlier).merge(&empty).to_bytes(),
        block_of(&earlier).to_bytes()
    );
    assert_eq!(
        empty.merge(&block_of(&earlier)).to_bytes(),
        block_of(&earlier).to_bytes()
    );
}

/// K4. `samples_in(a, b)` returns exactly the samples with `a < t <= b`, in
/// time order: bounds on, between, before and after sample times, an empty
/// range, an empty block and a one-sample block. An off-by-one at either end
/// (a closed start or an open end) double-counts or drops a sample at a
/// window boundary, so adjacent ranges would not tile the data.
#[test]
fn samples_in_trims_half_open() {
    let samples: Vec<(i64, f64)> = (1..=10).map(|i| (i * 1_000, i as f64)).collect();
    let block = block_of(&samples);
    let window = |a: i64, b: i64| -> Vec<(i64, u64)> {
        block
            .samples_in(a, b)
            .map(|(t, v)| (t, v.to_bits()))
            .collect()
    };
    let expected = |a: i64, b: i64| -> Vec<(i64, u64)> {
        bits_of(
            &samples
                .iter()
                .copied()
                .filter(|&(t, _)| a < t && t <= b)
                .collect::<Vec<_>>(),
        )
    };
    for (a, b) in [
        (3_000, 6_000),
        (2_500, 6_500),
        (0, 10_000),
        (-5_000, 0),
        (10_000, 20_000),
        (5_000, 5_000),
        (4_999, 5_000),
        (5_000, 5_001),
        (i64::MIN, i64::MAX),
    ] {
        assert_eq!(window(a, b), expected(a, b), "({a}, {b}]");
    }
    // Adjacent ranges tile the block.
    let mut tiled = window(0, 4_000);
    tiled.extend(window(4_000, 10_000));
    assert_eq!(tiled, bits_of(&samples));

    let empty = CompressedSamples::new();
    assert_eq!(empty.samples_in(i64::MIN, i64::MAX).count(), 0);
    let one = block_of(&[(7_000, 1.0)]);
    assert_eq!(one.samples_in(6_999, 7_000).count(), 1);
    assert_eq!(one.samples_in(7_000, 8_000).count(), 0);
}

/// K5. The payload's length follows the code lengths: `n` samples 1 s apart
/// with a constant value take one 16-bit timestamp code for the first delta
/// (`1110` + 12 bits for `dod = 1000`), then 1 bit per timestamp and 1 bit
/// per value: `16 + 1 + 2 (n - 2)` bits for `n >= 2`; `encoded_bytes` is the
/// 24-byte header plus the payload rounded up to bytes, which is the length
/// of `to_bytes`. The cost model prices a window with these numbers, so a
/// codec that spends more bits than the format says (or sizes its bytes
/// differently) makes the planner's memory predictions wrong.
#[test]
fn payload_bits_follow_the_code_lengths() {
    assert_eq!(CompressedSamples::HEADER_BYTES, 24);
    let one = block_of(&[(0, 5.0)]);
    assert_eq!(one.payload_bits(), 0);
    assert_eq!(one.encoded_bytes(), 24);
    assert_eq!(CompressedSamples::new().encoded_bytes(), 24);
    for n in [2u64, 3, 30, 120, 3_600] {
        let block = block_of(
            &(0..n as i64)
                .map(|i| (1_700_000_000_000 + i * 1_000, 5.0))
                .collect::<Vec<_>>(),
        );
        let bits = 16 + 1 + 2 * (n - 2);
        assert_eq!(block.payload_bits(), bits, "n = {n}");
        let bytes = 24 + bits.div_ceil(8) as usize;
        assert_eq!(block.encoded_bytes(), bytes, "n = {n}");
        assert_eq!(block.to_bytes().len(), bytes, "n = {n}");
    }
}
