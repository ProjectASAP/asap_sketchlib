use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use twox_hash::{XxHash3_64, XxHash3_128};

use super::{DataInput, HeapItem, MatrixFastHash, MatrixHashType};
use smallvec::SmallVec;

/// Default seed index used by single-hash sketch operations.
pub const CANONICAL_HASH_SEED: usize = 5; // 18 and 19 will cause hll test to fail...? is 5 faster...?
/// Seed index used for UnivMon bottom-layer selection.
pub const BOTTOM_LAYER_FINDER: usize = 19;
/// Seed index reserved for Hydra hashing.
pub const HYDRA_SEED: usize = 6;

/// Built-in seed values used by the default hasher.
pub const SEEDLIST: [u64; 20] = [
    0xcafe3553,
    0xade3415118,
    0x8cc70208,
    0x2f024b2b,
    0x451a3df5,
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19,
    0xcbbb9d5d,
    0x629a292a,
    0x9159015a,
    0x152fecd8,
    0x67332667,
    0x8eb44a87,
    0xdb0c2e0d,
];

#[inline(always)]
fn normalized_seed_idx(d: usize) -> usize {
    d % SEEDLIST.len()
}

/// Trait abstracting hash function signatures for probabilistic data structures.
///
/// All methods are static (no `&self`) to enable zero-cost monomorphization.
/// Implement this trait to inject a custom hash algorithm into any sketch struct.
pub trait SketchHasher: Clone + Debug {
    /// Hash representation used by matrix-backed sketches.
    type HashType: MatrixFastHash + Clone + Debug;

    /// Hashes an input into a 64-bit value with the selected seed.
    fn hash64_seeded(d: usize, key: &DataInput) -> u64;
    /// Hashes an input into a 128-bit value with the selected seed.
    fn hash128_seeded(d: usize, key: &DataInput) -> u128;
    /// Hashes a heap-owned key into a 64-bit value with the selected seed.
    fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64;
    /// Hashes a heap-owned key into a 128-bit value with the selected seed.
    fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128;

    /// Produces the matrix hash form used by matrix-backed sketches.
    fn hash_for_matrix_seeded(
        seed_idx: usize,
        rows: usize,
        cols: usize,
        key: &DataInput,
    ) -> Self::HashType;
}

/// Default hasher using twox_hash (XxHash3). This is the built-in implementation
/// used when no custom hasher is specified.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DefaultXxHasher;

impl SketchHasher for DefaultXxHasher {
    type HashType = MatrixHashType;

    #[inline(always)]
    fn hash64_seeded(d: usize, key: &DataInput) -> u64 {
        let seed = SEEDLIST[normalized_seed_idx(d)];
        match key {
            DataInput::I32(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I64(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::U32(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U64(u) => XxHash3_64::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            DataInput::F32(f) => XxHash3_64::oneshot_with_seed(seed, &f.to_ne_bytes()),
            DataInput::F64(f) => XxHash3_64::oneshot_with_seed(seed, &f.to_ne_bytes()),
            DataInput::Str(s) => XxHash3_64::oneshot_with_seed(seed, (*s).as_bytes()),
            DataInput::String(s) => XxHash3_64::oneshot_with_seed(seed, (*s).as_bytes()),
            DataInput::Bytes(items) => XxHash3_64::oneshot_with_seed(seed, items),
            DataInput::I8(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I16(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I128(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u128).to_ne_bytes()),
            DataInput::ISIZE(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::U8(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U16(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U128(u) => XxHash3_64::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            DataInput::USIZE(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
        }
    }

    #[inline(always)]
    fn hash128_seeded(d: usize, key: &DataInput) -> u128 {
        let seed = SEEDLIST[normalized_seed_idx(d)];
        match key {
            DataInput::I32(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I64(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::U32(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U64(u) => XxHash3_128::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            DataInput::F32(f) => XxHash3_128::oneshot_with_seed(seed, &f.to_ne_bytes()),
            DataInput::F64(f) => XxHash3_128::oneshot_with_seed(seed, &f.to_ne_bytes()),
            DataInput::Str(s) => XxHash3_128::oneshot_with_seed(seed, (*s).as_bytes()),
            DataInput::String(s) => XxHash3_128::oneshot_with_seed(seed, (*s).as_bytes()),
            DataInput::Bytes(items) => XxHash3_128::oneshot_with_seed(seed, items),
            DataInput::I8(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I16(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::I128(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u128).to_ne_bytes()),
            DataInput::ISIZE(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            DataInput::U8(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U16(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            DataInput::U128(u) => XxHash3_128::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            DataInput::USIZE(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
        }
    }

    #[inline(always)]
    fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
        let seed = SEEDLIST[normalized_seed_idx(d)];
        match key {
            HeapItem::I32(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I64(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::U32(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U64(u) => XxHash3_128::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            HeapItem::F32(f) => XxHash3_128::oneshot_with_seed(seed, &f.to_ne_bytes()),
            HeapItem::F64(f) => XxHash3_128::oneshot_with_seed(seed, &f.to_ne_bytes()),
            HeapItem::String(s) => XxHash3_128::oneshot_with_seed(seed, (*s).as_bytes()),
            HeapItem::I8(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I16(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I128(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u128).to_ne_bytes()),
            HeapItem::ISIZE(i) => XxHash3_128::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::U8(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U16(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U128(u) => XxHash3_128::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            HeapItem::USIZE(u) => XxHash3_128::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
        }
    }

    #[inline(always)]
    fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
        let seed = SEEDLIST[normalized_seed_idx(d)];
        match key {
            HeapItem::I32(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I64(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::U32(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U64(u) => XxHash3_64::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            HeapItem::F32(f) => XxHash3_64::oneshot_with_seed(seed, &f.to_ne_bytes()),
            HeapItem::F64(f) => XxHash3_64::oneshot_with_seed(seed, &f.to_ne_bytes()),
            HeapItem::String(s) => XxHash3_64::oneshot_with_seed(seed, (*s).as_bytes()),
            HeapItem::I8(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I16(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::I128(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u128).to_ne_bytes()),
            HeapItem::ISIZE(i) => XxHash3_64::oneshot_with_seed(seed, &(*i as u64).to_ne_bytes()),
            HeapItem::U8(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U16(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
            HeapItem::U128(u) => XxHash3_64::oneshot_with_seed(seed, &(*u).to_ne_bytes()),
            HeapItem::USIZE(u) => XxHash3_64::oneshot_with_seed(seed, &(*u as u64).to_ne_bytes()),
        }
    }

    #[inline(always)]
    fn hash_for_matrix_seeded(
        seed_idx: usize,
        rows: usize,
        cols: usize,
        key: &DataInput,
    ) -> Self::HashType {
        hash_for_matrix_seeded_generic::<Self>(seed_idx, rows, cols, key)
    }
}

// ---------------------------------------------------------------------------
// Backwards-compatible free functions — delegate to DefaultXxHasher
// ---------------------------------------------------------------------------

/// I32, U32, F32 will all be treated as 64-bit value.
#[inline(always)]
pub fn hash64_seeded(d: usize, key: &DataInput) -> u64 {
    DefaultXxHasher::hash64_seeded(d, key)
}

#[inline(always)]
/// Hashes an input into a 128-bit value with the selected seed.
pub fn hash128_seeded(d: usize, key: &DataInput) -> u128 {
    DefaultXxHasher::hash128_seeded(d, key)
}

// for speed, add separate function
/// Hashes a heap-owned key into a 128-bit value with the selected seed.
#[inline(always)]
pub fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
    DefaultXxHasher::hash_item128_seeded(d, key)
}

// for speed, add separate function
/// Hashes a heap-owned key into a 64-bit value with the selected seed.
#[inline(always)]
pub fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
    DefaultXxHasher::hash_item64_seeded(d, key)
}

// ---------------------------------------------------------------------------
// Matrix hash helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn mask_bits_for_cols(cols: usize) -> u32 {
    if cols.is_power_of_two() {
        cols.ilog2()
    } else {
        cols.ilog2() + 1
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Layout used to store per-row fast-path hashes.
pub enum MatrixHashMode {
    /// Packs row hashes into one 64-bit value.
    Packed64,
    /// Packs row hashes into one 128-bit value.
    Packed128,
    /// Stores one row hash per row.
    Rows,
}

/// Chooses a matrix hash layout for the given sketch dimensions.
#[inline(always)]
pub fn hash_mode_for_matrix(rows: usize, cols: usize) -> MatrixHashMode {
    let mask_bits = mask_bits_for_cols(cols) as usize;
    // Reserve one extra bit per row for sketches that use a sign bit (e.g., Count Sketch).
    let bits_per_row = mask_bits + 1;
    let bits_required = bits_per_row.saturating_mul(rows);
    if bits_required <= 64 {
        MatrixHashMode::Packed64
    } else if bits_required <= 128 {
        MatrixHashMode::Packed128
    } else {
        MatrixHashMode::Rows
    }
}

/// Creates a fast-path hash for a matrix-backed sketch using the default seed.
pub fn hash_for_matrix(rows: usize, cols: usize, key: &DataInput) -> MatrixHashType {
    hash_for_matrix_seeded(0, rows, cols, key)
}

/// Creates a fast-path hash for a matrix-backed sketch with a custom seed.
/// Chooses a packed hash when the required bits fit in 128; otherwise uses per-row hashes.
pub fn hash_for_matrix_seeded(
    seed_idx: usize,
    rows: usize,
    cols: usize,
    key: &DataInput,
) -> MatrixHashType {
    let mode = hash_mode_for_matrix(rows, cols);
    hash_for_matrix_seeded_with_mode(seed_idx, mode, rows, key)
}

/// Creates a fast-path hash using a pre-selected hash mode.
#[inline(always)]
pub fn hash_for_matrix_seeded_with_mode(
    seed_idx: usize,
    mode: MatrixHashMode,
    rows: usize,
    key: &DataInput,
) -> MatrixHashType {
    hash_for_matrix_seeded_with_mode_generic::<DefaultXxHasher>(seed_idx, mode, rows, key)
}

/// Generic version of matrix hash that uses a custom hasher.
#[inline(always)]
pub fn hash_for_matrix_seeded_with_mode_generic<H: SketchHasher>(
    seed_idx: usize,
    mode: MatrixHashMode,
    rows: usize,
    key: &DataInput,
) -> MatrixHashType {
    match mode {
        MatrixHashMode::Packed64 => {
            MatrixHashType::Packed64(H::hash64_seeded(seed_idx % SEEDLIST.len(), key))
        }
        MatrixHashMode::Packed128 => {
            MatrixHashType::Packed128(H::hash128_seeded(seed_idx % SEEDLIST.len(), key))
        }
        MatrixHashMode::Rows => {
            let mut hashes = SmallVec::<[u64; 8]>::with_capacity(rows);
            for row in 0..rows {
                let seed = (seed_idx + row) % SEEDLIST.len();
                hashes.push(H::hash64_seeded(seed, key));
            }
            MatrixHashType::Rows(hashes)
        }
    }
}

/// Generic version of hash_for_matrix that uses a custom hasher.
pub fn hash_for_matrix_generic<H: SketchHasher>(
    rows: usize,
    cols: usize,
    key: &DataInput,
) -> MatrixHashType {
    hash_for_matrix_seeded_generic::<H>(0, rows, cols, key)
}

/// Generic version of hash_for_matrix_seeded that uses a custom hasher.
pub fn hash_for_matrix_seeded_generic<H: SketchHasher>(
    seed_idx: usize,
    rows: usize,
    cols: usize,
    key: &DataInput,
) -> MatrixHashType {
    let mode = hash_mode_for_matrix(rows, cols);
    hash_for_matrix_seeded_with_mode_generic::<H>(seed_idx, mode, rows, key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{sample_uniform_f64, sample_zipf_u64};
    use std::collections::HashSet;

    #[derive(Clone, Debug)]
    struct Packed64Hasher;

    impl SketchHasher for Packed64Hasher {
        type HashType = u64;

        fn hash64_seeded(d: usize, key: &DataInput) -> u64 {
            DefaultXxHasher::hash64_seeded(d, key)
        }

        fn hash128_seeded(d: usize, key: &DataInput) -> u128 {
            DefaultXxHasher::hash128_seeded(d, key)
        }

        fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
            DefaultXxHasher::hash_item64_seeded(d, key)
        }

        fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
            DefaultXxHasher::hash_item128_seeded(d, key)
        }

        fn hash_for_matrix_seeded(
            seed_idx: usize,
            rows: usize,
            cols: usize,
            key: &DataInput,
        ) -> Self::HashType {
            <u64 as MatrixFastHash>::assert_compatible(rows, cols);
            DefaultXxHasher::hash64_seeded(seed_idx, key)
        }
    }

    #[derive(Clone, Debug)]
    struct Packed128Hasher;

    impl SketchHasher for Packed128Hasher {
        type HashType = u128;

        fn hash64_seeded(d: usize, key: &DataInput) -> u64 {
            DefaultXxHasher::hash64_seeded(d, key)
        }

        fn hash128_seeded(d: usize, key: &DataInput) -> u128 {
            DefaultXxHasher::hash128_seeded(d, key)
        }

        fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
            DefaultXxHasher::hash_item64_seeded(d, key)
        }

        fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
            DefaultXxHasher::hash_item128_seeded(d, key)
        }

        fn hash_for_matrix_seeded(
            seed_idx: usize,
            rows: usize,
            cols: usize,
            key: &DataInput,
        ) -> Self::HashType {
            <u128 as MatrixFastHash>::assert_compatible(rows, cols);
            DefaultXxHasher::hash128_seeded(seed_idx, key)
        }
    }

    // Test: ensures the hash collision is not likely to happen
    // the input cardinality should be roughly the same with cardinality of hashed value
    #[test]
    fn hash128_seeded_preserves_cardinality() {
        const SEED_IDX: usize = 0;
        const SAMPLE_SIZE: usize = 5_000;

        let uniform_values = sample_uniform_f64(0.0, 1_000_000.0, SAMPLE_SIZE, 42);
        let uniform_input_cardinality = uniform_values
            .iter()
            .map(|value| value.to_bits())
            .collect::<HashSet<_>>()
            .len();
        let uniform_hash_cardinality = uniform_values
            .iter()
            .map(|value| hash128_seeded(SEED_IDX, &DataInput::F64(*value)))
            .collect::<HashSet<_>>()
            .len();
        assert_eq!(
            uniform_input_cardinality, uniform_hash_cardinality,
            "uniform samples should not collide after hashing"
        );

        let zipf_values = sample_zipf_u64(10_000, 1.1, SAMPLE_SIZE, 7);
        let zipf_input_cardinality = zipf_values.iter().copied().collect::<HashSet<_>>().len();
        let zipf_hash_cardinality = zipf_values
            .iter()
            .map(|value| hash128_seeded(SEED_IDX, &DataInput::U64(*value)))
            .collect::<HashSet<_>>()
            .len();
        assert_eq!(
            zipf_input_cardinality, zipf_hash_cardinality,
            "zipf samples should not collide after hashing"
        );
    }

    #[test]
    fn hash128_seeded_is_deterministic_for_repeated_inputs() {
        const SEED_IDX: usize = 3;
        let key = DataInput::String("deterministic-key".to_string());
        let expected = hash128_seeded(SEED_IDX, &key);
        for _ in 0..100 {
            assert_eq!(expected, hash128_seeded(SEED_IDX, &key));
        }
    }

    #[test]
    fn xxh3_regression_vectors_match_go() {
        let key = DataInput::Bytes(b"projectasap");

        assert_eq!(hash64_seeded(0, &key), 887548862923853302);
        assert_eq!(
            hash64_seeded(CANONICAL_HASH_SEED, &key),
            8535098769003547387
        );
        assert_eq!(
            hash128_seeded(CANONICAL_HASH_SEED, &key),
            199634325175509853918794253804029959851u128
        );
    }

    #[test]
    fn hash_seed_index_wraps_like_go() {
        let key = DataInput::Bytes(b"projectasap");

        assert_eq!(
            hash64_seeded(SEEDLIST.len() + CANONICAL_HASH_SEED, &key),
            hash64_seeded(CANONICAL_HASH_SEED, &key)
        );
        assert_eq!(
            hash128_seeded(SEEDLIST.len() + CANONICAL_HASH_SEED, &key),
            hash128_seeded(CANONICAL_HASH_SEED, &key)
        );
    }

    #[test]
    fn packed64_hasher_accepts_compatible_dimensions() {
        let key = DataInput::U64(7);
        let hash = Packed64Hasher::hash_for_matrix_seeded(0, 3, 4096, &key);
        assert_eq!(hash, DefaultXxHasher::hash64_seeded(0, &key));
    }

    #[test]
    fn packed128_hasher_accepts_larger_dimensions() {
        let key = DataInput::U64(11);
        let hash = Packed128Hasher::hash_for_matrix_seeded(0, 8, 4096, &key);
        assert_eq!(hash, DefaultXxHasher::hash128_seeded(0, &key));
    }

    #[test]
    #[should_panic(
        expected = "SketchHasher hash type u64 cannot represent fast-path hash for rows=8, cols=4096; use u128 or MatrixHashType"
    )]
    fn packed64_hasher_rejects_oversized_dimensions() {
        let key = DataInput::U64(19);
        let _ = Packed64Hasher::hash_for_matrix_seeded(0, 8, 4096, &key);
    }
}
