# API: SpaceSaving

Status: `Ready`

## Purpose

Tracks the most frequent keys of a stream in a fixed number of counters. A
monitored key's recorded count never falls below its true count, and exceeds it
by at most that key's own error allowance.

## Type/Struct

- `SpaceSaving<H = DefaultXxHasher>`

The Stream-Summary of the paper's section 3.1: buckets carrying a count, ordered
by count in a doubly linked list, each owning a doubly linked list of the
counters at that count, plus a key index into those counters. An increment moves
one counter to the neighbouring bucket and an eviction takes the head of the
lowest bucket, so `insert` touches a constant number of links whatever the
capacity.

Both lists are arenas of indices rather than pointers. `counters` is allocated
once up to `capacity` and reused in place — an eviction overwrites the victim's
slot — and `buckets` recycles through a free list.

## Constructors

```rust
fn with_capacity(capacity: usize) -> Self
```

`capacity` is the number of keys monitored at once, and it is the only sizing
parameter. `Default` is 1024. Capacity floors at 1.

Size it against the ceiling you can tolerate: with `capacity` counters over a
stream of total weight `N`, the smallest monitored count is at most `N /
capacity`, and no key below that is guaranteed to survive. Choosing a capacity
at or above the number of distinct keys makes the summary exact.

## Insert/Update

```rust
fn insert(&mut self, value: &DataInput)
fn insert_many(&mut self, value: &DataInput, count: u64)
fn bulk_insert(&mut self, values: &[DataInput])
```

A monitored key rises by `count`. An unmonitored key takes a free counter while
one is left; after that it displaces the smallest monitored key, seats itself at
`min + count`, and carries `min` as its error. `insert_many` with a weight is
the same as repeating `insert` that many times. A `count` of zero is a no-op.

## Query

```rust
fn estimate(&self, value: &DataInput) -> u64
fn upper_bound(&self, value: &DataInput) -> u64
fn error(&self, value: &DataInput) -> u64
fn is_guaranteed(&self, value: &DataInput) -> bool
fn top_k(&self, k: usize) -> Vec<(HeapItem, u64, u64)>
fn entries(&self) -> Vec<(HeapItem, u64, u64)>
fn min_count(&self) -> u64
fn total(&self) -> u64
fn len(&self) -> usize
fn capacity(&self) -> usize
```

For a monitored key, `estimate - error <= truth <= estimate`. For an unmonitored
key `estimate` is zero and the truth is at most `min_count`, which is what
`upper_bound` reports instead; `upper_bound` therefore never reads below the
truth for any key in the stream, and `estimate` does so only for keys the
summary has dropped.

`is_guaranteed` holds when `estimate - error > min_count`, so the key outranks
every key the summary discarded — a claim about the stream, not about the
summary.

`top_k` walks the bucket list from the high end and returns `(key, count,
error)` in non-increasing count order.

## Merge

```rust
fn merge_from(&mut self, other: &Self)
```

Counts for a shared key add. A key held by only one side takes the other's
`min_count` as both extra count and extra error, since that is the most the
other side can say about it. The union is then trimmed back to `capacity`.

This is **not** equivalent to running one summary over the concatenated streams:
a key both sides evicted cannot be recovered. `merge_equivalence_battery` does
not apply for that reason.

## Serialization

Derived serde. The key index serializes with the arenas, so a decoded summary
takes inserts immediately.

## Examples

```rust
use asap_sketchlib::{DataInput, SpaceSaving};

let mut top: SpaceSaving = SpaceSaving::with_capacity(1024);
for flow in &flows {
    top.insert(&DataInput::Str(flow));
}

for (key, count, error) in top.top_k(10) {
    println!("{key:?}: {count} (+0/-{error})");
}
```

## Caveats

- No decrement path. Counts only rise, so the summary does not model a
  turnstile stream.
- A key can be evicted and re-enter later. Its count then restarts from the
  minimum rather than from what it had, and its error grows accordingly.
- `estimate` returns zero for a key that is not monitored. That reads below the
  truth for an evicted key; use `upper_bound` when the caller needs a value
  that never does.
- `top_k(k)` allocates and clones `k` keys.

## Relation to the paper

- **Structure.** Section 3.1's Stream-Summary, with the bucket and counter
  lists as index arenas.
- **Guarantee.** Section 3.2's `count - error <= truth <= count`, and the
  `min_count` ceiling on anything unmonitored.
- **Merge.** Not in the paper; the rule above follows Cormode and
  Hadjieleftheriou's survey treatment and is approximate.

## Status

Ready.
