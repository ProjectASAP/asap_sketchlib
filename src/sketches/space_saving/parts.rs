//! A summary's wire state, for the kinds that inline a Space-Saving summary
//! beside a matrix (`docs/asapv1_wire_format.md` §3.24, §3.25).

use std::cmp::Ordering;

use crate::{HeapItem, SketchHasher};

use super::{SpaceSaving, SpaceSavingState, key_order};

/// Ascending heap key order (§3.5).
pub(crate) fn key_cmp(a: &HeapItem, b: &HeapItem) -> Ordering {
    key_order(a).cmp(&key_order(b))
}

/// §3.5's emitted order of two `(key, count)` pairs: descending count, ties
/// broken by ascending heap key order.
pub(crate) fn emitted_order(a: (&HeapItem, u64), b: (&HeapItem, u64)) -> Ordering {
    b.1.cmp(&a.1).then_with(|| key_cmp(a.0, b.0))
}

impl<H: SketchHasher> SpaceSaving<H> {
    /// The monitored `(key, count, error)` triples in emitted order.
    pub(crate) fn emitted_entries(&self) -> Vec<(&HeapItem, u64, u64)> {
        let mut entries: Vec<(&HeapItem, u64, u64)> = self
            .monitored
            .iter()
            .map(|c| (&c.key, self.buckets[c.bucket].count, c.error))
            .collect();
        entries.sort_unstable_by(|a, b| emitted_order((a.0, a.1), (b.0, b.1)));
        entries
    }

    /// The largest count known to have left the summary.
    pub(crate) fn discarded_max(&self) -> u64 {
        self.discarded_max
    }

    /// Rebuilds a summary from its wire state, rejecting a state §3.5's decode
    /// rules 5 to 10 reject. `capacity` never sizes an allocation.
    pub(crate) fn from_parts(
        capacity: usize,
        total: u64,
        discarded_max: u64,
        entries: Vec<(HeapItem, u64, u64)>,
    ) -> Result<Self, String> {
        Self::rebuild(SpaceSavingState {
            capacity,
            total,
            discarded_max,
            entries,
        })
    }
}
