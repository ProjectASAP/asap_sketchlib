//! # Space-Saving (Metwally, Agrawal, El Abbadi — ICDT 2005)
//!
//! Tracks the most frequent keys of a stream in a fixed number of counters.
//! Every arrival either lands on a monitored key or displaces the smallest one,
//! taking over its count as an error allowance, so a monitored key's recorded
//! count never underestimates its true count and overstates it by at most
//! [`SpaceSaving::error`].
//!
//! ## Structure
//!
//! The Stream-Summary of section 3.1: buckets carrying a count, ordered by
//! count in a doubly linked list, each owning a doubly linked list of the
//! counters at that count, plus a key index into those counters. A unit arrival
//! moves one counter to the neighbouring bucket and an eviction takes the head
//! of the lowest bucket, so [`SpaceSaving::insert`] touches a constant number of
//! links whatever the capacity. A weighted arrival lands further along the
//! bucket list and walks it to reach its destination, one step per bucket it
//! passes.
//!
//! Both lists are arenas of indices rather than pointers: `counters` is
//! allocated once up to `capacity` and reused in place, and `buckets` recycles
//! through a free list. Only the `(key, count, error)` triples reach the wire;
//! the arenas and the key index are rebuilt from them on load.
//!
//! ## Reference
//! * "Efficient Computation of Frequent and Top-k Elements in Data Streams",
//!   ICDT 2005. <https://doi.org/10.1007/978-3-540-30570-5_27>

use crate::common::DigestBuildHasher;
use crate::{DataInput, DefaultXxHasher, HeapItem, SketchHasher, input_to_owned};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::marker::PhantomData;

const NIL: usize = usize::MAX;

/// Counters in a default sketch.
pub const SPACE_SAVING_DEFAULT_CAPACITY: usize = 1024;

/// Counter positions sharing one digest. Two monitored keys colliding on a
/// 64-bit digest is rare enough that the inline pair is effectively never
/// spilled.
type Slot = SmallVec<[usize; 2]>;

type Index = HashMap<u64, Slot, DigestBuildHasher>;

/// One monitored key.
#[derive(Clone, Debug)]
struct Counter {
    key: HeapItem,
    digest: u64,
    error: u64,
    bucket: usize,
    prev: usize,
    next: usize,
}

/// One count value, owning every counter currently at that count.
#[derive(Clone, Debug)]
struct Bucket {
    count: u64,
    head: usize,
    prev: usize,
    next: usize,
}

/// A Space-Saving summary over a fixed number of counters.
#[derive(Clone, Debug)]
pub struct SpaceSaving<H: SketchHasher = DefaultXxHasher> {
    capacity: usize,
    counters: Vec<Counter>,
    buckets: Vec<Bucket>,
    bucket_free: Vec<usize>,
    bucket_head: usize,
    bucket_tail: usize,
    index: Index,
    total: u64,
    /// Largest count known to have left the summary, so the ceiling on any key
    /// that is no longer monitored.
    floor: u64,
    _hasher: PhantomData<H>,
}

impl<H: SketchHasher> Default for SpaceSaving<H> {
    fn default() -> Self {
        Self::with_capacity(SPACE_SAVING_DEFAULT_CAPACITY)
    }
}

impl<H: SketchHasher> SpaceSaving<H> {
    /// Creates a summary that monitors at most `capacity` keys at a time.
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            counters: Vec::with_capacity(capacity),
            buckets: Vec::new(),
            bucket_free: Vec::new(),
            bucket_head: NIL,
            bucket_tail: NIL,
            index: Index::with_capacity_and_hasher(capacity, DigestBuildHasher::default()),
            total: 0,
            floor: 0,
            _hasher: PhantomData,
        }
    }

    /// Counters this summary can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Keys currently monitored.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.counters.len()
    }

    /// True while nothing has been recorded.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Total weight recorded, monitored or displaced.
    #[inline(always)]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// The ceiling on the true count of any key the summary does not monitor.
    ///
    /// The larger of the smallest count still held, once every counter is in
    /// use, and the largest count that has left the summary through an eviction
    /// or a merge. Zero while the summary has spare counters and has dropped
    /// nothing.
    #[inline(always)]
    pub fn min_count(&self) -> u64 {
        let lowest = if self.counters.len() == self.capacity && self.bucket_head != NIL {
            self.buckets[self.bucket_head].count
        } else {
            0
        };
        self.floor.max(lowest)
    }

    /// Drops every counter.
    pub fn clear(&mut self) {
        self.counters.clear();
        self.buckets.clear();
        self.bucket_free.clear();
        self.bucket_head = NIL;
        self.bucket_tail = NIL;
        self.index.clear();
        self.total = 0;
        self.floor = 0;
    }

    /// Records one occurrence of `value`.
    #[inline]
    pub fn insert(&mut self, value: &DataInput) {
        self.insert_many(value, 1);
    }

    /// Records `count` occurrences of `value` in one step.
    ///
    /// A weighted arrival on a monitored key raises it by `count`; one that
    /// takes a free or displaced counter seats itself at [`Self::min_count`]
    /// plus `count` and carries that minimum as its error. `count` of zero is a
    /// no-op, and counts saturate at [`u64::MAX`].
    pub fn insert_many(&mut self, value: &DataInput, count: u64) {
        if count == 0 {
            return;
        }
        self.total = self.total.saturating_add(count);
        let digest = H::hash64_seeded(0, value);

        if let Some(cid) = self.find(digest, value) {
            self.raise(cid, count);
            return;
        }

        if self.counters.len() < self.capacity {
            let seated = self.floor.saturating_add(count);
            let floor = self.floor;
            self.seat(digest, input_to_owned(value), seated, floor);
            return;
        }

        let victim = self.buckets[self.bucket_head].head;
        let lowest = self.buckets[self.bucket_head].count;
        self.floor = self.floor.max(lowest);
        self.unindex(self.counters[victim].digest, victim);
        self.counters[victim].key = input_to_owned(value);
        self.counters[victim].digest = digest;
        self.counters[victim].error = self.floor;
        self.index.entry(digest).or_default().push(victim);
        self.raise(victim, (self.floor - lowest).saturating_add(count));
    }

    /// Records every value in `values`.
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// The recorded count for `value`, or zero when it is not monitored.
    ///
    /// A monitored key's count never falls below its true count and exceeds it
    /// by at most [`Self::error`]. An unmonitored key reports zero; its true
    /// count cannot exceed [`Self::min_count`], which [`Self::upper_bound`]
    /// reports instead.
    pub fn estimate(&self, value: &DataInput) -> u64 {
        let digest = H::hash64_seeded(0, value);
        match self.find(digest, value) {
            Some(cid) => self.buckets[self.counters[cid].bucket].count,
            None => 0,
        }
    }

    /// The largest count `value` can have, monitored or not.
    ///
    /// Equal to [`Self::estimate`] for a monitored key and to
    /// [`Self::min_count`] otherwise, so it never falls below the truth for any
    /// key in the stream.
    pub fn upper_bound(&self, value: &DataInput) -> u64 {
        let digest = H::hash64_seeded(0, value);
        match self.find(digest, value) {
            Some(cid) => self.buckets[self.counters[cid].bucket].count,
            None => self.min_count(),
        }
    }

    /// How far [`Self::estimate`] may sit from the truth for `value`: above it
    /// by this much for a monitored key, and below it by this much for one the
    /// summary does not hold, where the estimate reads zero.
    pub fn error(&self, value: &DataInput) -> u64 {
        let digest = H::hash64_seeded(0, value);
        match self.find(digest, value) {
            Some(cid) => self.counters[cid].error,
            None => self.min_count(),
        }
    }

    /// True when `value`'s count is above every unmonitored key's ceiling, so
    /// its place among the heavy hitters is certain rather than probable.
    pub fn is_guaranteed(&self, value: &DataInput) -> bool {
        let digest = H::hash64_seeded(0, value);
        match self.find(digest, value) {
            Some(cid) => {
                let count = self.buckets[self.counters[cid].bucket].count;
                count.saturating_sub(self.counters[cid].error) > self.min_count()
            }
            None => false,
        }
    }

    /// The `k` monitored keys with the largest counts, highest first, as
    /// `(key, count, error)`.
    pub fn top_k(&self, k: usize) -> Vec<(HeapItem, u64, u64)> {
        let mut out = Vec::with_capacity(k.min(self.counters.len()));
        let mut bid = self.bucket_tail;
        while bid != NIL && out.len() < k {
            let count = self.buckets[bid].count;
            let mut cid = self.buckets[bid].head;
            while cid != NIL && out.len() < k {
                out.push((
                    self.counters[cid].key.clone(),
                    count,
                    self.counters[cid].error,
                ));
                cid = self.counters[cid].next;
            }
            bid = self.buckets[bid].prev;
        }
        out
    }

    /// Every monitored key as `(key, count, error)`, in no particular order.
    pub fn entries(&self) -> Vec<(HeapItem, u64, u64)> {
        self.counters
            .iter()
            .map(|c| (c.key.clone(), self.buckets[c.bucket].count, c.error))
            .collect()
    }

    /// Absorbs `other`.
    ///
    /// Counts for a shared key add, and a key held by only one side takes the
    /// other's `min_count` as both extra count and extra error, since that is
    /// all the other side can say about it. The union is then trimmed back to
    /// `capacity`. This is not equivalent to running one summary over the
    /// concatenated streams — a key evicted on both sides cannot be recovered.
    ///
    /// The `min_count` a one-sided key picks up is added weight that the stream
    /// never carried, so the merged counts sum to more than [`Self::total`] and
    /// an estimate divided by the total is no longer a frequency.
    pub fn merge_from(&mut self, other: &Self) {
        let mine_min = self.min_count();
        let theirs_min = other.min_count();

        let mut merged: HashMap<u64, SmallVec<[MergeEntry; 2]>, DigestBuildHasher> =
            HashMap::default();
        for c in &self.counters {
            merged.entry(c.digest).or_default().push(MergeEntry {
                key: c.key.clone(),
                digest: c.digest,
                count: self.buckets[c.bucket].count,
                error: c.error,
                paired: false,
            });
        }
        for c in &other.counters {
            let count = other.buckets[c.bucket].count;
            let slot = merged.entry(c.digest).or_default();
            match slot.iter_mut().find(|entry| entry.key == c.key) {
                Some(entry) => {
                    entry.count = entry.count.saturating_add(count);
                    entry.error = entry.error.saturating_add(c.error);
                    entry.paired = true;
                }
                None => slot.push(MergeEntry {
                    key: c.key.clone(),
                    digest: c.digest,
                    count: count.saturating_add(mine_min),
                    error: c.error.saturating_add(mine_min),
                    paired: true,
                }),
            }
        }

        let mut flat: Vec<MergeEntry> = merged.into_values().flatten().collect();
        for entry in &mut flat {
            if !entry.paired {
                entry.count = entry.count.saturating_add(theirs_min);
                entry.error = entry.error.saturating_add(theirs_min);
            }
        }
        flat.sort_unstable_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.digest.cmp(&b.digest))
                .then_with(|| key_order(&a.key).cmp(&key_order(&b.key)))
        });

        let mut floor = mine_min.saturating_add(theirs_min);
        if flat.len() > self.capacity {
            floor = floor.max(flat[self.capacity].count);
            flat.truncate(self.capacity);
        }

        let total = self.total.saturating_add(other.total);
        self.clear();
        self.total = total;
        self.floor = floor;
        for entry in flat {
            self.seat(entry.digest, entry.key, entry.count, entry.error);
        }
    }

    // -- Stream-Summary internals -------------------------------------------

    fn find(&self, digest: u64, value: &DataInput) -> Option<usize> {
        self.index.get(&digest).and_then(|ids| {
            ids.iter()
                .copied()
                .find(|cid| self.counters[*cid].key == *value)
        })
    }

    fn find_key(&self, digest: u64, key: &HeapItem) -> Option<usize> {
        self.index.get(&digest).and_then(|ids| {
            ids.iter()
                .copied()
                .find(|cid| self.counters[*cid].key == *key)
        })
    }

    fn unindex(&mut self, digest: u64, cid: usize) {
        if let Some(ids) = self.index.get_mut(&digest) {
            ids.retain(|id| *id != cid);
            if ids.is_empty() {
                self.index.remove(&digest);
            }
        }
    }

    /// Takes a free counter for `key` at `count` with `error`.
    fn seat(&mut self, digest: u64, key: HeapItem, count: u64, error: u64) {
        let cid = self.counters.len();
        self.counters.push(Counter {
            key,
            digest,
            error,
            bucket: NIL,
            prev: NIL,
            next: NIL,
        });
        let bucket = self.bucket_for(NIL, count);
        self.attach(cid, bucket);
        self.index.entry(digest).or_default().push(cid);
    }

    /// Moves `cid` up by `count`, creating the destination bucket if needed.
    fn raise(&mut self, cid: usize, count: u64) {
        let from = self.counters[cid].bucket;
        let target_count = self.buckets[from].count.saturating_add(count);
        if target_count == self.buckets[from].count {
            return;
        }
        let target = self.bucket_for(from, target_count);
        self.detach(cid);
        self.attach(cid, target);
    }

    /// Returns the bucket holding `count`, inserting one after `after` if no
    /// such bucket exists. `after` of `NIL` searches from the low end, and any
    /// other `after` holds a count below `count`.
    fn bucket_for(&mut self, after: usize, count: u64) -> usize {
        let mut prev = after;
        let mut next = if after == NIL {
            self.bucket_head
        } else {
            self.buckets[after].next
        };
        while next != NIL && self.buckets[next].count < count {
            prev = next;
            next = self.buckets[next].next;
        }
        if next != NIL && self.buckets[next].count == count {
            return next;
        }
        self.insert_bucket(prev, next, count)
    }

    fn insert_bucket(&mut self, prev: usize, next: usize, count: u64) -> usize {
        let bucket = Bucket {
            count,
            head: NIL,
            prev,
            next,
        };
        let bid = match self.bucket_free.pop() {
            Some(id) => {
                self.buckets[id] = bucket;
                id
            }
            None => {
                self.buckets.push(bucket);
                self.buckets.len() - 1
            }
        };
        if prev != NIL {
            self.buckets[prev].next = bid;
        } else {
            self.bucket_head = bid;
        }
        if next != NIL {
            self.buckets[next].prev = bid;
        } else {
            self.bucket_tail = bid;
        }
        bid
    }

    fn attach(&mut self, cid: usize, bid: usize) {
        let head = self.buckets[bid].head;
        self.counters[cid].prev = NIL;
        self.counters[cid].next = head;
        self.counters[cid].bucket = bid;
        if head != NIL {
            self.counters[head].prev = cid;
        }
        self.buckets[bid].head = cid;
    }

    fn detach(&mut self, cid: usize) {
        let bid = self.counters[cid].bucket;
        let prev = self.counters[cid].prev;
        let next = self.counters[cid].next;
        if prev != NIL {
            self.counters[prev].next = next;
        } else {
            self.buckets[bid].head = next;
        }
        if next != NIL {
            self.counters[next].prev = prev;
        }
        self.counters[cid].prev = NIL;
        self.counters[cid].next = NIL;
        self.counters[cid].bucket = NIL;
        if self.buckets[bid].head == NIL {
            self.drop_bucket(bid);
        }
    }

    fn drop_bucket(&mut self, bid: usize) {
        let prev = self.buckets[bid].prev;
        let next = self.buckets[bid].next;
        if prev != NIL {
            self.buckets[prev].next = next;
        } else {
            self.bucket_head = next;
        }
        if next != NIL {
            self.buckets[next].prev = prev;
        } else {
            self.bucket_tail = prev;
        }
        self.buckets[bid].head = NIL;
        self.buckets[bid].prev = NIL;
        self.buckets[bid].next = NIL;
        self.bucket_free.push(bid);
    }
}

/// One key of the union while a merge is in flight. `paired` marks an entry
/// that has already taken the other side's contribution.
struct MergeEntry {
    key: HeapItem,
    digest: u64,
    count: u64,
    error: u64,
    paired: bool,
}

/// A total order over keys, breaking the merge ties that counts and digests
/// leave open.
fn key_order(key: &HeapItem) -> (u8, u128, &str) {
    match key {
        HeapItem::I8(v) => (0, *v as i128 as u128, ""),
        HeapItem::I16(v) => (1, *v as i128 as u128, ""),
        HeapItem::I32(v) => (2, *v as i128 as u128, ""),
        HeapItem::I64(v) => (3, *v as i128 as u128, ""),
        HeapItem::I128(v) => (4, *v as u128, ""),
        HeapItem::ISIZE(v) => (5, *v as i128 as u128, ""),
        HeapItem::U8(v) => (6, u128::from(*v), ""),
        HeapItem::U16(v) => (7, u128::from(*v), ""),
        HeapItem::U32(v) => (8, u128::from(*v), ""),
        HeapItem::U64(v) => (9, u128::from(*v), ""),
        HeapItem::U128(v) => (10, *v, ""),
        HeapItem::USIZE(v) => (11, *v as u128, ""),
        HeapItem::F32(v) => (12, u128::from(v.to_bits()), ""),
        HeapItem::F64(v) => (13, u128::from(v.to_bits()), ""),
        HeapItem::String(v) => (14, 0, v.as_str()),
    }
}

/// Serialized form: the summary as the triples it answers with, since the
/// bucket and counter lists and the key index all follow from them.
#[derive(Serialize)]
struct SpaceSavingRef<'a> {
    capacity: usize,
    total: u64,
    floor: u64,
    entries: Vec<(&'a HeapItem, u64, u64)>,
}

#[derive(Deserialize)]
struct SpaceSavingState {
    capacity: usize,
    total: u64,
    floor: u64,
    entries: Vec<(HeapItem, u64, u64)>,
}

impl<H: SketchHasher> Serialize for SpaceSaving<H> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SpaceSavingRef {
            capacity: self.capacity,
            total: self.total,
            floor: self.floor,
            entries: self
                .counters
                .iter()
                .map(|c| (&c.key, self.buckets[c.bucket].count, c.error))
                .collect(),
        }
        .serialize(serializer)
    }
}

impl<'de, H: SketchHasher> Deserialize<'de> for SpaceSaving<H> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let state = SpaceSavingState::deserialize(deserializer)?;
        Self::rebuild(state).map_err(serde::de::Error::custom)
    }
}

impl<H: SketchHasher> SpaceSaving<H> {
    /// Rebuilds a summary from decoded triples, rejecting any that no run of
    /// the algorithm could have produced.
    fn rebuild(state: SpaceSavingState) -> Result<Self, String> {
        if state.capacity == 0 {
            return Err("space-saving capacity is zero".to_string());
        }
        if state.entries.len() > state.capacity {
            return Err(format!(
                "space-saving carries {} counters over a capacity of {}",
                state.entries.len(),
                state.capacity
            ));
        }
        for (_, count, error) in &state.entries {
            if *count == 0 {
                return Err("space-saving carries a counter at zero".to_string());
            }
            if *error > *count {
                return Err(format!(
                    "space-saving carries an error of {error} against a count of {count}"
                ));
            }
        }

        let mut entries = state.entries;
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.1));

        let mut summary = Self {
            capacity: state.capacity,
            counters: Vec::with_capacity(entries.len()),
            buckets: Vec::new(),
            bucket_free: Vec::new(),
            bucket_head: NIL,
            bucket_tail: NIL,
            index: Index::with_capacity_and_hasher(entries.len(), DigestBuildHasher::default()),
            total: state.total,
            floor: state.floor,
            _hasher: PhantomData,
        };
        for (key, count, error) in entries {
            let digest = H::hash_item64_seeded(0, &key);
            if summary.find_key(digest, &key).is_some() {
                return Err("space-saving carries the same key twice".to_string());
            }
            summary.seat(digest, key, count, error);
        }
        Ok(summary)
    }
}

#[cfg(test)]
impl<H: SketchHasher> SpaceSaving<H> {
    /// Checks every Stream-Summary invariant: both directions of both linked
    /// lists, strict count ordering, arena bookkeeping and index agreement.
    fn validate(&self) -> Result<(), String> {
        if self.counters.len() > self.capacity {
            return Err(format!(
                "{} counters over a capacity of {}",
                self.counters.len(),
                self.capacity
            ));
        }
        if self.counters.is_empty() != (self.bucket_head == NIL) {
            return Err("the bucket list disagrees with counter residency".to_string());
        }

        let mut live: Vec<usize> = Vec::new();
        let mut previous = NIL;
        let mut bid = self.bucket_head;
        while bid != NIL {
            if bid >= self.buckets.len() {
                return Err(format!("bucket {bid} is outside the arena"));
            }
            if live.len() > self.buckets.len() {
                return Err("the bucket list cycles".to_string());
            }
            let bucket = &self.buckets[bid];
            if bucket.prev != previous {
                return Err(format!("bucket {bid} does not point back at {previous}"));
            }
            if bucket.head == NIL {
                return Err(format!("live bucket {bid} holds no counter"));
            }
            if bucket.count == 0 {
                return Err(format!("live bucket {bid} sits at zero"));
            }
            if let Some(lower) = live.last()
                && self.buckets[*lower].count >= bucket.count
            {
                return Err("the bucket counts are not strictly increasing".to_string());
            }
            live.push(bid);
            previous = bid;
            bid = bucket.next;
        }
        if previous != self.bucket_tail {
            return Err("the bucket list does not end at the tail".to_string());
        }

        let mut backwards: Vec<usize> = Vec::new();
        let mut bid = self.bucket_tail;
        while bid != NIL {
            if backwards.len() > self.buckets.len() {
                return Err("the bucket list cycles backwards".to_string());
            }
            backwards.push(bid);
            bid = self.buckets[bid].prev;
        }
        backwards.reverse();
        if backwards != live {
            return Err("the bucket list reads differently in each direction".to_string());
        }

        let mut seen = vec![false; self.counters.len()];
        for bid in &live {
            let count = self.buckets[*bid].count;
            let mut chain: Vec<usize> = Vec::new();
            let mut previous = NIL;
            let mut cid = self.buckets[*bid].head;
            while cid != NIL {
                if cid >= self.counters.len() {
                    return Err(format!("counter {cid} is outside the arena"));
                }
                if seen[cid] {
                    return Err(format!("counter {cid} is reached twice"));
                }
                let counter = &self.counters[cid];
                if counter.prev != previous {
                    return Err(format!("counter {cid} does not point back at {previous}"));
                }
                if counter.bucket != *bid {
                    return Err(format!("counter {cid} points at bucket {}", counter.bucket));
                }
                if counter.error > count {
                    return Err(format!(
                        "counter {cid} carries an error of {} against a count of {count}",
                        counter.error
                    ));
                }
                seen[cid] = true;
                chain.push(cid);
                previous = cid;
                cid = counter.next;
            }

            let mut backwards: Vec<usize> = Vec::new();
            let mut cid = previous;
            while cid != NIL {
                if backwards.len() > chain.len() {
                    return Err(format!("bucket {bid}'s counter list cycles backwards"));
                }
                backwards.push(cid);
                cid = self.counters[cid].prev;
            }
            backwards.reverse();
            if backwards != chain {
                return Err(format!(
                    "bucket {bid}'s counter list reads differently in each direction"
                ));
            }
        }
        if let Some(cid) = seen.iter().position(|reached| !reached) {
            return Err(format!("counter {cid} hangs off no bucket"));
        }

        let mut free = vec![false; self.buckets.len()];
        for bid in &self.bucket_free {
            if *bid >= self.buckets.len() {
                return Err(format!("free bucket {bid} is outside the arena"));
            }
            if free[*bid] {
                return Err(format!("bucket {bid} is freed twice"));
            }
            free[*bid] = true;
        }
        for bid in &live {
            if free[*bid] {
                return Err(format!("bucket {bid} is both live and free"));
            }
        }
        if live.len() + self.bucket_free.len() != self.buckets.len() {
            return Err(format!(
                "{} live and {} free buckets in an arena of {}",
                live.len(),
                self.bucket_free.len(),
                self.buckets.len()
            ));
        }

        let mut indexed = vec![false; self.counters.len()];
        for (digest, slot) in &self.index {
            if slot.is_empty() {
                return Err(format!("digest {digest} indexes nothing"));
            }
            for cid in slot {
                if *cid >= self.counters.len() {
                    return Err(format!("digest {digest} indexes counter {cid}"));
                }
                if indexed[*cid] {
                    return Err(format!("counter {cid} is indexed twice"));
                }
                if self.counters[*cid].digest != *digest {
                    return Err(format!("counter {cid} is filed under the wrong digest"));
                }
                indexed[*cid] = true;
            }
        }
        if let Some(cid) = indexed.iter().position(|filed| !filed) {
            return Err(format!("counter {cid} is not indexed"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(item: &HeapItem) -> i64 {
        match item {
            HeapItem::I64(v) => *v,
            other => panic!("unexpected key form {other:?}"),
        }
    }

    fn walk(summary: &SpaceSaving) -> Vec<(i64, u64)> {
        summary
            .top_k(usize::MAX)
            .iter()
            .map(|(key, count, _)| (key_of(key), *count))
            .collect()
    }

    fn next_random(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Every key the summary reports must bracket its truth, and every key it
    /// does not must sit under the ceiling.
    fn assert_sound_against(summary: &SpaceSaving, truth: &HashMap<i64, u64>) {
        for (key, count) in truth {
            let probe = DataInput::I64(*key);
            assert!(
                summary.upper_bound(&probe) >= *count,
                "key {key} has true count {count} above the {} ceiling",
                summary.upper_bound(&probe)
            );
            let estimate = summary.estimate(&probe);
            if estimate == 0 {
                continue;
            }
            assert!(
                estimate >= *count,
                "monitored key {key} reads {estimate} against a truth of {count}"
            );
            assert!(
                estimate - summary.error(&probe) <= *count,
                "monitored key {key} reads {estimate} with too small an error for {count}"
            );
        }
    }

    fn fuzzed(
        capacity: usize,
        domain: i64,
        steps: usize,
        seed: u64,
    ) -> (SpaceSaving, HashMap<i64, u64>) {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(capacity);
        let mut truth: HashMap<i64, u64> = HashMap::new();
        let mut state = seed;
        for step in 0..steps {
            let draw = next_random(&mut state);
            let key = (draw % domain as u64) as i64;
            let weight = match (draw >> 40) % 8 {
                0..=4 => 1,
                5 => 3,
                6 => 11,
                _ => 97,
            };
            summary.insert_many(&DataInput::I64(key), weight);
            *truth.entry(key).or_default() += weight;
            if let Err(problem) = summary.validate() {
                panic!("capacity {capacity} step {step}: {problem}");
            }
        }
        (summary, truth)
    }

    #[test]
    fn a_fresh_summary_is_well_formed() {
        let summary: SpaceSaving = SpaceSaving::with_capacity(4);
        summary.validate().expect("empty summary");
        assert_eq!(summary.min_count(), 0);
        assert_eq!(summary.capacity(), 4);
    }

    #[test]
    fn a_capacity_of_zero_floors_at_one() {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(0);
        assert_eq!(summary.capacity(), 1);
        summary.insert(&DataInput::I64(1));
        summary.insert(&DataInput::I64(2));
        summary.validate().expect("single counter");
        assert_eq!(summary.len(), 1);
        assert_eq!(summary.estimate(&DataInput::I64(2)), 2);
    }

    #[test]
    fn a_weighted_arrival_displaces_the_minimum_and_starts_above_it() {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(2);
        for _ in 0..5 {
            summary.insert(&DataInput::I64(1));
        }
        for _ in 0..2 {
            summary.insert(&DataInput::I64(2));
        }

        summary.insert_many(&DataInput::I64(3), 4);

        summary.validate().expect("after a weighted eviction");
        assert_eq!(summary.len(), 2);
        assert_eq!(summary.estimate(&DataInput::I64(2)), 0);
        assert_eq!(summary.estimate(&DataInput::I64(3)), 6);
        assert_eq!(summary.error(&DataInput::I64(3)), 2);
        assert_eq!(summary.estimate(&DataInput::I64(1)), 5);
        assert_eq!(summary.min_count(), 5);
        assert_eq!(summary.total(), 11);
    }

    #[test]
    fn a_weighted_raise_passes_every_bucket_below_its_destination() {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(4);
        for (key, count) in [(1i64, 1u64), (2, 2), (3, 3), (4, 4)] {
            summary.insert_many(&DataInput::I64(key), count);
        }
        assert_eq!(walk(&summary), vec![(4, 4), (3, 3), (2, 2), (1, 1)]);

        summary.insert_many(&DataInput::I64(1), 9);

        summary.validate().expect("after a multi-hop raise");
        assert_eq!(walk(&summary), vec![(1, 10), (4, 4), (3, 3), (2, 2)]);
    }

    #[test]
    fn counts_saturate_and_keep_the_bucket_order() {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(3);
        summary.insert_many(&DataInput::I64(1), u64::MAX - 2);
        summary.insert_many(&DataInput::I64(2), u64::MAX);
        summary.insert_many(&DataInput::I64(1), 10);
        summary.validate().expect("after a saturating raise");

        summary.insert_many(&DataInput::I64(1), 5);
        summary
            .validate()
            .expect("after raising a saturated counter");

        assert_eq!(summary.estimate(&DataInput::I64(1)), u64::MAX);
        assert_eq!(summary.estimate(&DataInput::I64(2)), u64::MAX);
        assert_eq!(summary.total(), u64::MAX);
        let walked = walk(&summary);
        assert_eq!(walked.len(), 2);
        assert!(walked[0].1 >= walked[1].1, "the walk is out of order");
    }

    #[test]
    fn an_eviction_from_a_saturated_counter_stays_sound() {
        let mut summary: SpaceSaving = SpaceSaving::with_capacity(1);
        summary.insert_many(&DataInput::I64(1), u64::MAX);
        summary.insert(&DataInput::I64(2));
        summary
            .validate()
            .expect("after evicting a saturated counter");

        assert_eq!(summary.len(), 1);
        assert_eq!(summary.estimate(&DataInput::I64(2)), u64::MAX);
        assert_eq!(summary.error(&DataInput::I64(2)), u64::MAX);
        assert_eq!(summary.upper_bound(&DataInput::I64(1)), u64::MAX);
    }

    #[test]
    fn a_merge_saturates_instead_of_wrapping() {
        let mut left: SpaceSaving = SpaceSaving::with_capacity(2);
        left.insert_many(&DataInput::I64(1), u64::MAX);
        left.insert_many(&DataInput::I64(2), u64::MAX - 1);
        let mut right: SpaceSaving = SpaceSaving::with_capacity(3);
        right.insert_many(&DataInput::I64(1), u64::MAX / 2);
        right.insert_many(&DataInput::I64(3), 100);
        right.insert_many(&DataInput::I64(4), 50);

        left.merge_from(&right);

        left.validate().expect("after a saturating merge");
        assert_eq!(left.len(), 2);
        assert_eq!(left.total(), u64::MAX);
        for (key, count) in walk(&left) {
            assert_eq!(count, u64::MAX, "key {key} wrapped past the ceiling");
        }
        for key in 1..=4i64 {
            assert_eq!(left.upper_bound(&DataInput::I64(key)), u64::MAX);
        }
    }

    #[test]
    fn a_merge_carries_the_ceiling_into_an_under_full_summary() {
        let mut left: SpaceSaving = SpaceSaving::with_capacity(33);
        let mut right: SpaceSaving = SpaceSaving::with_capacity(1);
        for _ in 0..10 {
            right.insert(&DataInput::I64(7));
        }
        for _ in 0..20 {
            right.insert(&DataInput::I64(8));
        }

        left.merge_from(&right);

        left.validate()
            .expect("after merging into an empty summary");
        assert_eq!(left.len(), 1);
        assert!(left.len() < left.capacity(), "the merge left room to spare");
        assert!(
            left.min_count() >= 10,
            "key 7 truly reached 10 but the ceiling is {}",
            left.min_count()
        );
        assert!(left.upper_bound(&DataInput::I64(7)) >= 10);
        assert!(
            !left.is_guaranteed(&DataInput::I64(8)),
            "nothing outranks a ceiling it does not clear"
        );
    }

    #[test]
    fn a_chain_of_merges_keeps_the_ceiling_above_everything_dropped() {
        let mut left: SpaceSaving = SpaceSaving::with_capacity(5);
        let mut middle: SpaceSaving = SpaceSaving::with_capacity(1);
        let mut right: SpaceSaving = SpaceSaving::with_capacity(1);
        for _ in 0..10 {
            middle.insert(&DataInput::I64(7));
        }
        for _ in 0..20 {
            middle.insert(&DataInput::I64(8));
        }
        for _ in 0..5 {
            right.insert(&DataInput::I64(9));
        }
        for _ in 0..7 {
            right.insert(&DataInput::I64(10));
        }

        left.merge_from(&middle);
        left.validate().expect("after the first merge");
        left.merge_from(&right);
        left.validate().expect("after the second merge");

        assert!(left.len() < left.capacity(), "the chain left room to spare");
        for (key, truth) in [(7i64, 10u64), (9, 5)] {
            assert!(
                left.upper_bound(&DataInput::I64(key)) >= truth,
                "key {key} truly reached {truth} but is capped at {}",
                left.upper_bound(&DataInput::I64(key))
            );
        }
        assert!(left.min_count() >= 15, "the two ceilings did not compound");
    }

    #[test]
    fn a_key_that_re_enters_after_a_merge_never_reads_low() {
        let mut left: SpaceSaving = SpaceSaving::with_capacity(8);
        let mut right: SpaceSaving = SpaceSaving::with_capacity(1);
        for _ in 0..12 {
            right.insert(&DataInput::I64(7));
        }
        for _ in 0..30 {
            right.insert(&DataInput::I64(8));
        }
        left.merge_from(&right);

        let ceiling = left.min_count();
        left.insert(&DataInput::I64(7));

        left.validate().expect("after re-entry");
        let estimate = left.estimate(&DataInput::I64(7));
        assert!(
            estimate >= 13,
            "key 7 truly reached 13 but reads {estimate} on re-entry"
        );
        assert_eq!(estimate, ceiling + 1);
        assert_eq!(left.error(&DataInput::I64(7)), ceiling);
    }

    #[test]
    fn randomized_operations_keep_the_structure_sound() {
        for (capacity, seed) in [(1usize, 11u64), (2, 22), (7, 33), (64, 44), (257, 55)] {
            let (summary, truth) = fuzzed(capacity, 96, 4_000, seed);
            assert_sound_against(&summary, &truth);
            assert_eq!(summary.len(), capacity.min(truth.len()));
            assert_eq!(summary.total(), truth.values().sum::<u64>());
            let walked = walk(&summary);
            assert_eq!(walked.len(), summary.len());
            for pair in walked.windows(2) {
                assert!(pair[0].1 >= pair[1].1, "capacity {capacity}: walk order");
            }
        }
    }

    #[test]
    fn randomized_merges_keep_the_structure_sound() {
        for (capacity, domain, seed) in [
            (1usize, 64i64, 101u64),
            (3, 64, 202),
            (32, 64, 303),
            (128, 64, 404),
            (256, 40, 505),
        ] {
            let (mut left, mut truth) = fuzzed(capacity, domain, 900, seed);
            let (right, right_truth) = fuzzed(2, 80, 700, seed ^ 0xabcd);
            let (third, third_truth) = fuzzed(capacity + 5, 70, 500, seed ^ 0x1234);

            left.merge_from(&right);
            left.validate().expect("after the first merge");
            left.merge_from(&third);
            left.validate().expect("after the second merge");

            for (key, count) in right_truth.iter().chain(third_truth.iter()) {
                *truth.entry(*key).or_default() += *count;
            }
            assert_sound_against(&left, &truth);
            assert_eq!(left.total(), truth.values().sum::<u64>());
            assert!(left.len() <= left.capacity());

            let mut state = seed;
            for _ in 0..200 {
                let key = (next_random(&mut state) % 90) as i64;
                left.insert(&DataInput::I64(key));
                *truth.entry(key).or_default() += 1;
            }
            left.validate()
                .expect("after inserting into a merged summary");
            assert_sound_against(&left, &truth);
        }
    }

    #[test]
    fn a_decoded_summary_rebuilds_both_link_directions() {
        let (summary, truth) = fuzzed(48, 200, 3_000, 77);
        let bytes = rmp_serde::to_vec(&summary).expect("serialize");
        let decoded: SpaceSaving = rmp_serde::from_slice(&bytes).expect("deserialize");

        decoded.validate().expect("decoded summary");
        assert_eq!(decoded.len(), summary.len());
        assert_eq!(decoded.min_count(), summary.min_count());
        assert_eq!(decoded.total(), summary.total());
        assert_sound_against(&decoded, &truth);

        let mut walked = walk(&decoded);
        let mut expected = walk(&summary);
        walked.sort_unstable();
        expected.sort_unstable();
        assert_eq!(walked, expected);
    }

    #[test]
    fn a_crafted_state_fails_closed() {
        let over_capacity = SpaceSavingState {
            capacity: 1,
            total: 4,
            floor: 0,
            entries: vec![(HeapItem::I64(1), 2, 0), (HeapItem::I64(2), 2, 0)],
        };
        let cases = [
            (
                SpaceSavingState {
                    capacity: 0,
                    total: 0,
                    floor: 0,
                    entries: Vec::new(),
                },
                "capacity is zero",
            ),
            (over_capacity, "over a capacity"),
            (
                SpaceSavingState {
                    capacity: 4,
                    total: 1,
                    floor: 0,
                    entries: vec![(HeapItem::I64(1), 0, 0)],
                },
                "at zero",
            ),
            (
                SpaceSavingState {
                    capacity: 4,
                    total: 1,
                    floor: 0,
                    entries: vec![(HeapItem::I64(1), 3, 4)],
                },
                "error of 4",
            ),
            (
                SpaceSavingState {
                    capacity: 4,
                    total: 2,
                    floor: 0,
                    entries: vec![(HeapItem::I64(1), 2, 0), (HeapItem::I64(1), 1, 0)],
                },
                "same key twice",
            ),
        ];
        for (state, expected) in cases {
            let problem = SpaceSaving::<DefaultXxHasher>::rebuild(state)
                .expect_err("a crafted state must be rejected");
            assert!(
                problem.contains(expected),
                "expected a complaint about {expected}, got {problem}"
            );
        }
    }

    #[test]
    fn a_declared_capacity_is_not_allocated_on_decode() {
        let state = SpaceSavingState {
            capacity: 1 << 40,
            total: 3,
            floor: 0,
            entries: vec![(HeapItem::I64(1), 3, 0)],
        };
        let summary = SpaceSaving::<DefaultXxHasher>::rebuild(state).expect("a sparse state");
        summary.validate().expect("decoded summary");
        assert_eq!(summary.capacity(), 1 << 40);
        assert_eq!(summary.len(), 1);
    }
}
