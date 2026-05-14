//! Lazily-built table of `hash128_seeded(0, &DataInput::U64(i))` for
//! `i` in `0..PRECOMPUTED_HASH_LEN`.
//!
//! Storing this as a [`LazyLock`] instead of a 16K-entry literal array keeps
//! the crate small enough to publish to crates.io while preserving the same
//! `PRECOMPUTED_HASH[i]` access pattern. The table is computed once per
//! process — driven by the crate's own [`hash128_seeded`] — so it can never
//! drift out of sync with the hasher and incurs roughly 16K XxHash3_128
//! evaluations on first access (microseconds on modern hardware).

use std::sync::LazyLock;

use super::DataInput;
use super::hash::hash128_seeded;

/// Number of precomputed entries. `PRECOMPUTED_HASH[i]` is defined for
/// `i` in `0..PRECOMPUTED_HASH_LEN`.
pub const PRECOMPUTED_HASH_LEN: usize = 0x4000;

/// Precomputed 128-bit hashes for the small `u64` inputs `0..0x4000`.
///
/// `PRECOMPUTED_HASH[i] == hash128_seeded(0, &DataInput::U64(i as u64))`.
///
/// The table is materialised lazily on first access through [`LazyLock`].
/// Indexing (`PRECOMPUTED_HASH[i]`), iteration (`PRECOMPUTED_HASH.iter()`),
/// and length (`PRECOMPUTED_HASH.len()`) all work via `Deref` to the
/// underlying slice.
pub static PRECOMPUTED_HASH: LazyLock<Box<[u128]>> = LazyLock::new(|| {
    (0..PRECOMPUTED_HASH_LEN as u64)
        .map(|value| hash128_seeded(0, &DataInput::U64(value)))
        .collect()
});
