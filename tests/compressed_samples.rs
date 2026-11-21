//! Failing-first tests for `CompressedSamples`: every sample of one series in
//! one window, as one block whose first byte names its codec.
//!
//! Format (the bytes every implementation must write; integers little-endian;
//! "varint" is a zigzag LEB128 varint of a signed integer):
//!
//! **Codec 0, Gorilla (XOR).** Header, 25 bytes: codec `0` (`u8`), sample
//! count `u32`, first timestamp `i64` (ms), first value's `f64` bits `u64`,
//! payload length in bits `u32`; an empty block is 25 zero bytes. Payload, for
//! each sample after the first, its timestamp code then its value code, most
//! significant bit first, the last byte zero-padded:
//! - timestamp: `d = t - t_prev` (the first delta taken against
//!   `d_prev = 0`), `dod = d - d_prev`: `0` -> `0`; [-64, 63] -> `10` + 7
//!   bits; [-256, 255] -> `110` + 9; [-2048, 2047] -> `1110` + 12;
//!   otherwise `1111` + 64; two's complement;
//! - value: `x = bits(v) ^ bits(v_prev)`: `0` -> `0`; else
//!   `lead = min(leading_zeros(x), 31)`, `trail = trailing_zeros(x)`; if a
//!   window `(L, T)` exists with `lead >= L` and `trail >= T` -> `10` + the
//!   `64 - L - T` bits `x >> T`; else `11` + `lead` (5 bits) + `width - 1`
//!   (6 bits, `width = 64 - lead - trail`) + the `width` bits `x >> trail`,
//!   and `(lead, trail)` becomes the window.
//!
//! **Codec 1, decimal + zstd.** Eligible when every value is finite, not
//! -0, and equals `m / 10^D` exactly (`f64` division) for integers
//! `|m| < 2^53` and one `D <= 17`, `D` the largest of the values' smallest
//! digit counts. Header, 19 bytes: codec `1`, sample count `u32`, first
//! timestamp `i64`, `D` (`u8`), mantissa order (`u8`: 1 deltas, 2 deltas of
//! deltas), frame length `u32`. Then one zstd frame (written at level 1, read
//! at any level) of: the varint of each timestamp's delta of delta after the
//! first (as codec 0 computes it), then the varint of every sample's mantissa
//! delta (order 1, from `m_prev = 0`) or delta of delta (order 2, from a
//! previous delta of 0); the shorter order is written, order 1 on a tie.
//!
//! **Blocks.** `push` appends to the open block, always codec 0. `seal`
//! re-encodes with codec 0 and, when eligible, codec 1, and keeps the shorter
//! (codec 0 on a tie). Samples are kept in timestamp order: an older push is
//! inserted in place, and a timestamp already present has its value replaced
//! (the last write wins). `merge` is the sealed block of both blocks' samples,
//! the second block's value winning on an equal timestamp.

use asap_sketchlib::{CompressedSamples, CompressedSamplesFormatError, SampleCodec};

/// Bit pattern of a value, so NaN payloads and signed zeros compare exactly.
fn bits_of(samples: &[(i64, f64)]) -> Vec<(i64, u64)> {
    samples.iter().map(|&(t, v)| (t, v.to_bits())).collect()
}

/// The open (codec 0) block of `samples`, pushed in order.
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

/// Timestamps whose delta of delta lands in every codec-0 code bucket (0, 7,
/// 9, 12 and 64 bits, both signs) and a 30-day gap.
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
/// infinities, subnormals, runs of equal values, and random doubles. None
/// of these blocks is eligible for codec 1.
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

/// Random doubles 1 s apart: codec 0 only.
fn random_doubles(n: i64) -> Vec<(i64, f64)> {
    let mut state = 17;
    (0..n)
        .map(|i| (i * 1_000, f64::from_bits(splitmix64(&mut state) >> 2)))
        .collect()
}

/// Two-decimal latencies 1 s apart, `m / 100` for `m = 1234 + (37 i mod 101)
/// + 321 (i mod 7)`: 12.34, 15.92, 19.5, 22.07, …
fn two_decimal_latencies(n: i64) -> Vec<(i64, f64)> {
    (0..n)
        .map(|i| {
            let mantissa = 1_234 + (i * 37 % 101) + (i % 7) * 321;
            (i * 1_000, mantissa as f64 / 100.0)
        })
        .collect()
}

/// An integer counter 1 s apart, `1000 + 3 i`.
fn integer_counter(n: i64) -> Vec<(i64, f64)> {
    (0..n)
        .map(|i| (i * 1_000, (1_000 + 3 * i) as f64))
        .collect()
}

/// K1. Every sample comes back bit for bit from the open block, from the
/// sealed block and from the sealed block's bytes: regular and irregular
/// timestamps (every delta-of-delta code, a 30-day gap) with values that
/// only codec 0 can hold (NaN payloads, signed zeros, infinities,
/// subnormals, equal runs, random doubles), and two-decimal latencies and
/// integer counters, which seal as codec 1. Pushes in shuffled order give the
/// same samples and bytes as in order, and a repeated timestamp keeps its last
/// value. A wrong code width, a sign error in two's complement or zigzag, a
/// lost XOR window, a mantissa rounded instead of exact, or a value compared
/// as `f64` (NaN != NaN, 0.0 == -0.0) fails it.
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
    let irregular_latencies: Vec<(i64, f64)> = irregular_timestamps()
        .into_iter()
        .zip(two_decimal_latencies(100).into_iter().map(|(_, v)| v))
        .collect();
    let cases = [
        (&regular, SampleCodec::Gorilla),
        (&irregular, SampleCodec::Gorilla),
        (&two_decimal_latencies(600), SampleCodec::DecimalZstd),
        (&integer_counter(600), SampleCodec::DecimalZstd),
        (&irregular_latencies, SampleCodec::DecimalZstd),
    ];
    for (samples, sealed_codec) in cases {
        let open = block_of(samples);
        assert_eq!(open.codec(), SampleCodec::Gorilla);
        assert_eq!(open.len() as usize, samples.len());
        assert!(!open.is_empty());
        assert_eq!(decoded(&open), bits_of(samples));
        let sealed = open.seal();
        assert_eq!(sealed.codec(), sealed_codec, "{:?}", &samples[..3]);
        assert_eq!(decoded(&sealed), bits_of(samples));
        let restored = CompressedSamples::from_bytes(&sealed.to_bytes()).unwrap();
        assert_eq!(restored.codec(), sealed_codec);
        assert_eq!(decoded(&restored), bits_of(samples));
        assert_eq!(restored.to_bytes(), sealed.to_bytes());
        let restored_open = CompressedSamples::from_bytes(&open.to_bytes()).unwrap();
        assert_eq!(decoded(&restored_open), bits_of(samples));
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
    let expected = bits_of(&[(0, 1.0), (1_000, 9.0), (2_000, 3.0)]);
    assert_eq!(decoded(&block), expected);
    assert_eq!(decoded(&block.seal()), expected);

    // An empty block round-trips as an empty block.
    let empty = CompressedSamples::new();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let restored = CompressedSamples::from_bytes(&empty.seal().to_bytes()).unwrap();
    assert!(restored.is_empty());
}

// Codec 0 golden blocks: the open block's bytes, which `encoded_with(Gorilla)`
// gives for any block.

/// One sample `(1000 ms, 1.5)`: a header and no payload.
const ONE: &str = "0001000000e803000000000000000000000000f83f00000000";

/// `(0, 2.0), (1000, 2.0), (2000, 2.0)`. Header: codec 0, count 3, first
/// time 0, first value 0x4000000000000000, payload 19 bits. Payload bits:
/// - sample 2: `d = 1000`, `dod = 1000 - 0` in [-2048, 2047] -> `1110` +
///   `001111101000`; value XOR 0 -> `0`;
/// - sample 3: `dod = 0` -> `0`; value -> `0`;
///
/// `1110001111101000 0 0 0` = `11100011 11101000 000` -> bytes `e3 e8 00`.
const REGULAR: &str = "00030000000000000000000000000000000000004013000000e3e800";

/// `(0, 1.0), (10, 1.0), (25, 2.0), (100, 3.0), (400, -0.0),
/// (3400, +inf), (3_000_003_400, NaN 0x7ff0000000000002)`: deltas of delta
/// 10, 5, 60 (7-bit codes), 225 (9-bit), 2700 and about 3e9 (64-bit; the
/// 12-bit code is in `REGULAR`), an equal value, and new XOR windows.
const MIXED: &str = "00070000000000000000000000000000000000f03f2c0100008520b84afff3cd807387019801f0000000000000a8cbffde0000000165a0a491fdf000000010";

/// `(0, 1.0), (1000, 1.5), (2000, 1.25), (3000, 1.75)`: the third and
/// fourth values reuse the second's XOR window (`10` + window bits).
const VALUE_WINDOW_REUSE: &str = "00040000000000000000000000000000000000f03f33000000e3e8d805b01d40";

/// An empty block: 25 zero bytes.
const EMPTY: &str = "00000000000000000000000000000000000000000000000000";

// Codec 1 golden blocks. The frame's compressed bytes depend on the zstd
// build, so what is fixed is the header up to the frame length (15 bytes),
// the frame length (it must equal the frame's size) and the frame's content.

/// `(0, 1.25), (1000, 1.5), (2000, 1.75), (3010, 2.0)`, header bytes 0..15:
/// codec 1, count 4, first time 0, `D = 2`, order 1.
/// Content: timestamp deltas of delta 1000, 0, 10 -> zigzag 2000, 0, 20 ->
/// `d0 0f`, `00`, `14`; mantissas 125, 150, 175, 200: order 1 deltas 125, 25,
/// 25, 25 -> zigzag 250, 50, 50, 50 -> `fa 01 32 32 32` (5 bytes), shorter than
/// order 2's 125, -100, 0, 0 -> `fa 01 c7 01 00 00` (6 bytes).
const SMALL_DECIMAL_HEADER: &str = "010400000000000000000000000201";
const SMALL_DECIMAL_CONTENT: &str = "d00f0014fa01323232";

/// `two_decimal_latencies(60)`: header bytes 0..15 (codec 1, count 60, first
/// time 0, `D = 2`, order 2) and the frame's content: 59 timestamp codes
/// (`d0 0f`, then 58 zeros), then 60 mantissa deltas of delta.
const LATENCY_HEADER: &str = "013c00000000000000000000000202";
const LATENCY_CONTENT: &str = "d00f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a413d70d00c901ca0100c901c3218e23c901ca01c901ca0100d724d82400c901ca0100c901c321c421ca0100c901ca0100d724d82400c901ca01c901ca018d23c421ca0100c901ca01c901c3218e23c901ca0100c901ca018d23c421ca01c901ca0100c901c3218e23c901ca01";

/// `two_decimal_latencies(60)` as a complete block whose frame was written at
/// zstd level 19 (67 bytes, header frame length 0x43), not the encoder's
/// level 1: a reader must accept any level.
const LATENCY_LEVEL_19_BLOCK: &str = "013c000000000000000000000002024300000028b52ffd0068d50100f8d00f00a413d70d00c901ca0100c901c3218e23d724d824c421ca01008d23000a005ce8d0b34fc03bbc85bb20ec949a27249902b804cb271802";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// Checks a codec-1 block's bytes against its golden header prefix and
/// frame content.
fn assert_decimal_block(bytes: &[u8], header: &str, content: &str) {
    assert_eq!(hex(&bytes[..15]), header);
    let frame_length = u32::from_le_bytes(bytes[15..19].try_into().unwrap()) as usize;
    assert_eq!(frame_length, bytes.len() - 19, "frame length field");
    let decompressed = zstd::stream::decode_all(&bytes[19..]).unwrap();
    assert_eq!(hex(&decompressed), content);
}

/// K2. Codec 0: fixed inputs encode to fixed bytes. Codec 1: a fixed block's
/// header and its frame's decompressed content are fixed, and a frame written
/// at another zstd level decodes to the same samples. All expected bytes come
/// from a separate reference implementation of the format in the module doc.
/// Malformed bytes are refused with the matching error. Any deviation from
/// the format (byte order, bit order, a code's width or prefix, the first
/// delta's base, zigzag, the order choice, the padding) changes the bytes, so
/// a data plane in another language reading them would decode other samples.
#[test]
fn portable_format_golden_vectors() {
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let gorilla_cases: [(&str, Vec<(i64, f64)>); 5] = [
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
    for (expected, samples) in gorilla_cases {
        let open = block_of(&samples);
        assert_eq!(hex(&open.to_bytes()), expected, "{samples:?}");
        let gorilla = open.seal().encoded_with(SampleCodec::Gorilla).unwrap();
        assert_eq!(hex(&gorilla.to_bytes()), expected, "{samples:?}");
        let restored = CompressedSamples::from_bytes(&unhex(expected)).unwrap();
        assert_eq!(restored.codec(), SampleCodec::Gorilla);
        assert_eq!(decoded(&restored), bits_of(&samples));
    }

    let small = vec![(0, 1.25), (1_000, 1.5), (2_000, 1.75), (3_010, 2.0)];
    let latencies = two_decimal_latencies(60);
    for (samples, header, content) in [
        (&small, SMALL_DECIMAL_HEADER, SMALL_DECIMAL_CONTENT),
        (&latencies, LATENCY_HEADER, LATENCY_CONTENT),
    ] {
        let decimal = block_of(samples)
            .encoded_with(SampleCodec::DecimalZstd)
            .unwrap();
        assert_eq!(decimal.codec(), SampleCodec::DecimalZstd);
        let bytes = decimal.to_bytes();
        assert_decimal_block(&bytes, header, content);
        let restored = CompressedSamples::from_bytes(&bytes).unwrap();
        assert_eq!(decoded(&restored), bits_of(samples));
    }
    let level_19 = CompressedSamples::from_bytes(&unhex(LATENCY_LEVEL_19_BLOCK)).unwrap();
    assert_eq!(level_19.codec(), SampleCodec::DecimalZstd);
    assert_eq!(decoded(&level_19), bits_of(&latencies));

    let refused = |bytes: &[u8]| CompressedSamples::from_bytes(bytes).err();
    // A codec byte no codec has.
    let mut unknown = unhex(REGULAR);
    unknown[0] = 7;
    assert!(matches!(
        refused(&unknown),
        Some(CompressedSamplesFormatError::UnknownCodec)
    ));
    // Shorter than its codec's header: 10 bytes of codec 0, 15 of codec 1.
    assert!(matches!(
        refused(&unhex(REGULAR)[..10]),
        Some(CompressedSamplesFormatError::Truncated)
    ));
    assert!(matches!(
        refused(&unhex(LATENCY_LEVEL_19_BLOCK)[..15]),
        Some(CompressedSamplesFormatError::Truncated)
    ));
    assert!(matches!(
        refused(&[]),
        Some(CompressedSamplesFormatError::Truncated)
    ));
    // Codec 0's header says 19 payload bits (3 bytes); 2 or 4 bytes follow.
    let regular = unhex(REGULAR);
    assert!(matches!(
        refused(&regular[..regular.len() - 1]),
        Some(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    let mut long = regular.clone();
    long.push(0);
    assert!(matches!(
        refused(&long),
        Some(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    // Codec 1's frame length says 67 bytes; one more or one fewer follows.
    let block = unhex(LATENCY_LEVEL_19_BLOCK);
    assert!(matches!(
        refused(&block[..block.len() - 1]),
        Some(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    let mut long = block.clone();
    long.push(0);
    assert!(matches!(
        refused(&long),
        Some(CompressedSamplesFormatError::PayloadLengthMismatch)
    ));
    // No samples, yet 8 payload bits and one payload byte.
    let mut no_samples = unhex(EMPTY);
    no_samples[21] = 8;
    no_samples.push(0);
    assert!(matches!(
        refused(&no_samples),
        Some(CompressedSamplesFormatError::PayloadWithoutSamples)
    ));
    // A codec-1 header over 4 bytes that are not a zstd frame.
    let mut not_a_frame = unhex(LATENCY_HEADER);
    not_a_frame.extend(4u32.to_le_bytes());
    not_a_frame.extend([0xde, 0xad, 0xbe, 0xef]);
    assert!(matches!(
        refused(&not_a_frame),
        Some(CompressedSamplesFormatError::InvalidFrame)
    ));
}

/// K3. A merge is the sealed block of the union of both blocks' samples,
/// byte for byte, the second block's value winning on an equal timestamp:
/// for disjoint spans in either order, for interleaved spans with a tie (two
/// ingest shards' halves of one window), for a codec-0 block with a codec-1
/// block, and with an empty block. A merge that appends bytes without
/// re-coding, keeps the first block's codec, drops or duplicates a sample, or
/// keeps the first value on a tie fails it.
#[test]
fn merge_equals_sealed_union() {
    let sealed_union = |first: &[(i64, f64)], second: &[(i64, f64)]| -> Vec<u8> {
        let mut union: Vec<(i64, f64)> = first
            .iter()
            .filter(|s| !second.iter().any(|o| o.0 == s.0))
            .copied()
            .collect();
        union.extend(second);
        union.sort_by_key(|s| s.0);
        block_of(&union).seal().to_bytes()
    };
    let check = |first: &[(i64, f64)], second: &[(i64, f64)]| {
        let expected = sealed_union(first, second);
        for (a, b) in [
            (block_of(first), block_of(second)),
            (block_of(first).seal(), block_of(second).seal()),
            (block_of(first), block_of(second).seal()),
        ] {
            assert_eq!(a.merge(&b).to_bytes(), expected);
        }
    };

    // Disjoint, codec 0 (random doubles), both orders.
    let doubles = random_doubles(120);
    let (earlier, later) = doubles.split_at(50);
    check(earlier, later);
    check(later, earlier);

    // Disjoint, codec 1 (latencies).
    let latencies = two_decimal_latencies(120);
    let (earlier, later) = latencies.split_at(70);
    check(earlier, later);

    // Interleaved halves of one window with a tie at 10 s.
    let even: Vec<(i64, f64)> = (0..40).step_by(2).map(|i| (i * 500, i as f64)).collect();
    let mut odd: Vec<(i64, f64)> = (1..40).step_by(2).map(|i| (i * 500, -(i as f64))).collect();
    odd.push((10_000, 99.0));
    check(&even, &odd);
    check(&odd, &even);

    // A codec-0 block (random doubles) with a codec-1 block (latencies) over
    // the next minute: the union holds random doubles, so it seals as codec 0.
    let shifted: Vec<(i64, f64)> = two_decimal_latencies(60)
        .into_iter()
        .map(|(t, v)| (t + 120_000, v))
        .collect();
    assert_eq!(block_of(&shifted).seal().codec(), SampleCodec::DecimalZstd);
    check(&doubles, &shifted);
    check(&shifted, &doubles);

    // An empty block changes nothing.
    check(&latencies, &[]);
    check(&[], &latencies);
}

/// K4. `samples_in(a, b)` returns exactly the samples with `a < t <= b`, in
/// time order, from the open block and from blocks sealed as codec 0 and as
/// codec 1: bounds on, between, before and after sample times, an empty range,
/// an empty block and a one-sample block. An off-by-one at either end (a
/// closed start or an open end) double-counts or drops a sample at a window
/// boundary, so adjacent ranges would not tile the data.
#[test]
fn samples_in_trims_half_open() {
    let doubles: Vec<(i64, f64)> = random_doubles(11).into_iter().skip(1).collect();
    let latencies: Vec<(i64, f64)> = two_decimal_latencies(11).into_iter().skip(1).collect();
    let open = block_of(&latencies);
    let sealed_gorilla = block_of(&doubles).seal();
    let sealed_decimal = block_of(&latencies).seal();
    assert_eq!(sealed_gorilla.codec(), SampleCodec::Gorilla);
    assert_eq!(sealed_decimal.codec(), SampleCodec::DecimalZstd);
    for (block, samples) in [
        (&open, &latencies),
        (&sealed_gorilla, &doubles),
        (&sealed_decimal, &latencies),
    ] {
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
        assert_eq!(tiled, bits_of(samples));
    }

    let empty = CompressedSamples::new();
    assert_eq!(empty.samples_in(i64::MIN, i64::MAX).count(), 0);
    let one = block_of(&[(7_000, 1.0)]);
    assert_eq!(one.samples_in(6_999, 7_000).count(), 1);
    assert_eq!(one.samples_in(7_000, 8_000).count(), 0);
}

/// K5. Code lengths and the codec choice. Codec 0 of `n` samples 1 s apart
/// with a constant value takes one 16-bit timestamp code for the first delta
/// (`1110` + 12 bits for `dod = 1000`), then 1 bit per timestamp and 1 bit per
/// value: `16 + 1 + 2 (n - 2)` payload bits. Headers are 25 bytes (codec 0)
/// and 19 (codec 1). Sealing keeps the shorter codec: two-decimal latencies
/// and integer counters seal as codec 1 (about 100 bytes against 456 for 60
/// latencies), random doubles as codec 0, and a decimal block with one NaN as
/// codec 0, since codec 1 cannot hold it. `encoded_bytes` is the header plus
/// the payload rounded up, the length of `to_bytes`. The cost model prices a
/// window with these numbers and the calibration measures them, so a codec
/// that spends more bits than the format says, or seals the longer codec,
/// makes the planner's memory predictions wrong.
#[test]
fn codec_choice_and_code_lengths() {
    assert_eq!(SampleCodec::Gorilla.header_bytes(), 25);
    assert_eq!(SampleCodec::DecimalZstd.header_bytes(), 19);
    let one = block_of(&[(0, 5.0)]);
    assert_eq!(one.payload_bits(), 0);
    assert_eq!(one.encoded_bytes(), 25);
    assert_eq!(CompressedSamples::new().encoded_bytes(), 25);
    for n in [2u64, 3, 30, 120, 3_600] {
        let block = block_of(
            &(0..n as i64)
                .map(|i| (1_700_000_000_000 + i * 1_000, 5.0))
                .collect::<Vec<_>>(),
        );
        assert_eq!(block.codec(), SampleCodec::Gorilla);
        let bits = 16 + 1 + 2 * (n - 2);
        assert_eq!(block.payload_bits(), bits, "n = {n}");
        let bytes = 25 + bits.div_ceil(8) as usize;
        assert_eq!(block.encoded_bytes(), bytes, "n = {n}");
        assert_eq!(block.to_bytes().len(), bytes, "n = {n}");
    }

    let latencies = block_of(&two_decimal_latencies(60));
    let gorilla = latencies.encoded_with(SampleCodec::Gorilla).unwrap();
    let decimal = latencies.encoded_with(SampleCodec::DecimalZstd).unwrap();
    assert_eq!(gorilla.encoded_bytes(), 456);
    assert!(decimal.encoded_bytes() < 150, "{}", decimal.encoded_bytes());
    assert_eq!(
        decimal.payload_bits(),
        8 * (decimal.encoded_bytes() as u64 - 19)
    );
    assert_eq!(decimal.encoded_bytes(), decimal.to_bytes().len());
    assert_eq!(latencies.seal().to_bytes(), decimal.to_bytes());

    let counter = block_of(&integer_counter(120)).seal();
    assert_eq!(counter.codec(), SampleCodec::DecimalZstd);
    assert_eq!(counter.encoded_bytes(), counter.to_bytes().len());

    let doubles = block_of(&random_doubles(120));
    assert!(doubles.encoded_with(SampleCodec::DecimalZstd).is_none());
    assert_eq!(doubles.seal().codec(), SampleCodec::Gorilla);

    let mut with_nan = two_decimal_latencies(60);
    with_nan[30].1 = f64::NAN;
    let with_nan = block_of(&with_nan);
    assert!(with_nan.encoded_with(SampleCodec::DecimalZstd).is_none());
    assert_eq!(with_nan.seal().codec(), SampleCodec::Gorilla);

    let negative_zero = block_of(&[(0, 1.5), (1_000, -0.0)]);
    assert!(
        negative_zero
            .encoded_with(SampleCodec::DecimalZstd)
            .is_none()
    );
}
