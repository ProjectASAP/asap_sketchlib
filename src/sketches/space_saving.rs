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
//! counters at that count, plus a key index into those counters. An increment
//! moves one counter to the neighbouring bucket and an eviction takes the head
//! of the lowest bucket, so `insert` touches a constant number of links
//! whatever the capacity.
//!
//! Both lists are arenas of indices rather than pointers: `counters` is
//! allocated once up to `capacity` and reused in place, and `buckets` recycles
//! through a free list.
//!
//! ## Reference
//! * "Efficient Computation of Frequent and Top-k Elements in Data Streams",
//!   ICDT 2005. <https://doi.org/10.1007/978-3-540-30570-5_27>

use crate::{DataInput, DefaultXxHasher, HeapItem, SketchHasher, input_to_owned};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

const NIL: usize = usize::MAX;

/// Counters in a default sketch.
pub const SPACE_SAVING_DEFAULT_CAPACITY: usize = 1024;

/// One monitored key.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Counter {
    key: HeapItem,
    digest: u64,
    error: u64,
    bucket: usize,
    prev: usize,
    next: usize,
}

/// One count value, owning every counter currently at that count.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bucket {
    count: u64,
    head: usize,
    prev: usize,
    next: usize,
}

/// A Space-Saving summary over a fixed number of counters.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct SpaceSaving<H: SketchHasher = DefaultXxHasher> {
    capacity: usize,
    counters: Vec<Counter>,
    buckets: Vec<Bucket>,
    bucket_free: Vec<usize>,
    bucket_head: usize,
    bucket_tail: usize,
    index: HashMap<u64, Vec<usize>>,
    total: u64,
    #[serde(skip)]
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
            index: HashMap::with_capacity(capacity),
            total: 0,
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

    /// The smallest monitored count, and the ceiling on any unmonitored key's
    /// true count. Zero while the summary has spare counters.
    #[inline(always)]
    pub fn min_count(&self) -> u64 {
        if self.counters.len() < self.capacity || self.bucket_head == NIL {
            0
        } else {
            self.buckets[self.bucket_head].count
        }
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
    }

    /// Records one occurrence of `value`.
    #[inline]
    pub fn insert(&mut self, value: &DataInput) {
        self.insert_many(value, 1);
    }

    /// Records `count` occurrences of `value` in one step.
    ///
    /// A weighted arrival on a monitored key raises it by `count`; one that
    /// displaces the minimum seats the arrival at `min + count`. `count` of
    /// zero is a no-op.
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
            let cid = self.counters.len();
            self.counters.push(Counter {
                key: input_to_owned(value),
                digest,
                error: 0,
                bucket: NIL,
                prev: NIL,
                next: NIL,
            });
            let bucket = self.bucket_for(NIL, count);
            self.attach(cid, bucket);
            self.index.entry(digest).or_default().push(cid);
            return;
        }

        let victim = self.buckets[self.bucket_head].head;
        let floor = self.buckets[self.bucket_head].count;
        self.unindex(self.counters[victim].digest, victim);
        self.counters[victim].key = input_to_owned(value);
        self.counters[victim].digest = digest;
        self.counters[victim].error = floor;
        self.index.entry(digest).or_default().push(victim);
        self.raise(victim, count);
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

    /// How far above the truth [`Self::estimate`] may sit for `value`.
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
    pub fn merge_from(&mut self, other: &Self) {
        let mine_min = self.min_count();
        let theirs_min = other.min_count();

        let mut merged: HashMap<u64, Vec<(HeapItem, u64, u64)>> = HashMap::new();
        for c in &self.counters {
            merged.entry(c.digest).or_default().push((
                c.key.clone(),
                self.buckets[c.bucket].count + theirs_min,
                c.error + theirs_min,
            ));
        }
        for c in &other.counters {
            let slot = merged.entry(c.digest).or_default();
            let count = other.buckets[c.bucket].count;
            match slot.iter_mut().find(|(key, _, _)| *key == c.key) {
                Some(entry) => {
                    entry.1 = entry.1 - theirs_min + count;
                    entry.2 = entry.2 - theirs_min + c.error;
                }
                None => slot.push((c.key.clone(), count + mine_min, c.error + mine_min)),
            }
        }

        let mut flat: Vec<(HeapItem, u64, u64)> =
            merged.into_values().flat_map(|v| v.into_iter()).collect();
        flat.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        flat.truncate(self.capacity);

        let total = self.total.saturating_add(other.total);
        let capacity = self.capacity;
        self.clear();
        self.capacity = capacity;
        for (key, count, error) in flat {
            self.seat(key, count, error);
        }
        self.total = total;
    }

    // -- Stream-Summary internals -------------------------------------------

    fn find(&self, digest: u64, value: &DataInput) -> Option<usize> {
        self.index.get(&digest).and_then(|ids| {
            ids.iter()
                .copied()
                .find(|cid| self.counters[*cid].key == *value)
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

    /// Seats a counter holding `key` at `count` with `error`, used by merge.
    fn seat(&mut self, key: HeapItem, count: u64, error: u64) {
        let digest = H::hash_item64_seeded(0, &key);
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
        let target_count = self.buckets[from].count + count;
        let target = self.bucket_for(from, target_count);
        self.detach(cid);
        self.attach(cid, target);
    }

    /// Returns the bucket holding `count`, inserting one after `after` if no
    /// such bucket exists. `after` of `NIL` searches from the low end.
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
        if prev != NIL && self.buckets[prev].count == count {
            return prev;
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
