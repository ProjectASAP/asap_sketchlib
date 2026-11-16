//! The set of hashes a KMV sketch retains, for membership in expected `O(1)`.
//!
//! Child submodule of [`crate::sketches::kmv`]. The heap answers "what is the
//! largest retained hash" and evicts it; this index answers "is this hash
//! already retained", which the heap can only answer by reading every slot.
//!
//! Open addressing with linear probing over `slot_count` slots, where the
//! sketch keeps `slot_count` at twice the heap's allocated slots, so the load
//! stays at most one half. A slot holds a hash, or 0 for "empty"; the hash 0
//! itself is held by a flag beside the slots. A removal shifts the run behind
//! it back into the gap (no tombstones), so a lookup never probes past the
//! run of its own home slot.
//!
//! Example: with 8 slots, hashes whose home slots are 3, 3 and 4 sit at 3, 4
//! and 5. Removing the one at 3 moves the second (home 3) back to 3; the third
//! (home 4) moves to 4, so every hash stays reachable from its home slot.

/// Odd 64-bit multiplier (2^64 / golden ratio) that spreads hashes over the
/// slots even when the hashes themselves follow a pattern, such as multiples
/// of 2^32 or a counting sequence passed to `update_with_hash`.
const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Clone, Debug, Default)]
pub(super) struct RetainedIndex {
    /// The hashes, 0 marking an empty slot.
    slots: Vec<u64>,
    /// Whether the hash 0 is retained; 0 cannot sit in `slots`.
    holds_zero: bool,
}

impl RetainedIndex {
    /// An index of `slot_count` slots holding `hashes`. `slot_count` must
    /// exceed the number of hashes.
    pub(super) fn with_slots(slot_count: usize, hashes: &[u64]) -> Self {
        debug_assert!(slot_count > hashes.len() || hashes.is_empty());
        let mut index = Self {
            slots: vec![0; slot_count],
            holds_zero: false,
        };
        for &hash in hashes {
            index.insert(hash);
        }
        index
    }

    /// Slots allocated, each one `u64`.
    pub(super) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// The slot `hash` is probed from: the top bits of the spread hash,
    /// scaled to the slot count, so any slot count works.
    #[inline]
    fn home(&self, hash: u64) -> usize {
        ((u128::from(hash.wrapping_mul(SPREAD)) * self.slots.len() as u128) >> 64) as usize
    }

    #[inline]
    fn next(&self, slot: usize) -> usize {
        if slot + 1 == self.slots.len() {
            0
        } else {
            slot + 1
        }
    }

    /// Whether `hash` is retained.
    #[inline]
    pub(super) fn contains(&self, hash: u64) -> bool {
        if hash == 0 {
            return self.holds_zero;
        }
        if self.slots.is_empty() {
            return false;
        }
        let mut slot = self.home(hash);
        loop {
            match self.slots[slot] {
                0 => return false,
                held if held == hash => return true,
                _ => slot = self.next(slot),
            }
        }
    }

    /// Adds `hash`, which is not held yet. The caller keeps a free slot.
    #[inline]
    pub(super) fn insert(&mut self, hash: u64) {
        if hash == 0 {
            self.holds_zero = true;
            return;
        }
        let mut slot = self.home(hash);
        while self.slots[slot] != 0 {
            slot = self.next(slot);
        }
        self.slots[slot] = hash;
    }

    /// Removes `hash`, which is held, and shifts the rest of its run back so
    /// every hash stays reachable from its home slot.
    #[inline]
    pub(super) fn remove(&mut self, hash: u64) {
        if hash == 0 {
            self.holds_zero = false;
            return;
        }
        let mut gap = self.home(hash);
        while self.slots[gap] != hash {
            gap = self.next(gap);
        }
        let mut slot = self.next(gap);
        loop {
            let held = self.slots[slot];
            if held == 0 {
                break;
            }
            // `held` may fill the gap when its home is not strictly after the
            // gap and up to `slot`, going around the end.
            let home = self.home(held);
            let stays = if gap <= slot {
                gap < home && home <= slot
            } else {
                gap < home || home <= slot
            };
            if !stays {
                self.slots[gap] = held;
                gap = slot;
            }
            slot = self.next(slot);
        }
        self.slots[gap] = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Inserts and removals in any order agree with a `BTreeSet`, including
    /// the hash 0, hashes that share a home slot, and runs that wrap past the
    /// last slot.
    #[test]
    fn agrees_with_a_set_under_churn() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for slot_count in [2usize, 3, 8, 17, 64] {
            let mut index = RetainedIndex::with_slots(slot_count, &[]);
            let mut oracle = BTreeSet::new();
            // A small domain, so the same homes and the wrap recur.
            for _ in 0..20_000 {
                let hash = rng.random_range(0..3 * slot_count as u64);
                if oracle.contains(&hash) {
                    index.remove(hash);
                    oracle.remove(&hash);
                } else if oracle.iter().filter(|&&h| h != 0).count() + 1 < slot_count {
                    index.insert(hash);
                    oracle.insert(hash);
                }
                for probe in 0..3 * slot_count as u64 {
                    assert_eq!(
                        index.contains(probe),
                        oracle.contains(&probe),
                        "slots {slot_count}, hash {probe}"
                    );
                }
            }
        }
    }
}
