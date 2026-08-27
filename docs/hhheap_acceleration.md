# HHHeap acceleration

`HHHeap` is the top-k structure behind `CMSHeap`, `CSHeap`, `FoldCMS`, `FoldCS`,
`UnivMon`, and the Octo aggregator. It sits on the insert path of every one of
them, and it is the reason those inserts get slower as the candidate capacity
grows. This page measures that cost, names its cause, and gives the change that
removes it.

Reproduce with `cargo run --release --example hhheap_probe`. Numbers below are
from an Apple M-series laptop, 200k updates drawn from a Zipf(1.1) stream over
20k distinct keys.

## The cost

`HHHeap::update` runs at a rate inversely proportional to capacity:

| capacity | `HHHeap` Mups/s (string keys) | `HHHeap` Mups/s (u64 keys) |
| --- | --- | --- |
| 8 | 4.63 | 8.26 |
| 32 | 1.14 | 1.63 |
| 128 | 0.22 | 0.33 |
| 512 | 0.05 | 0.07 |
| 2048 | 0.01 | 0.02 |

Every doubling of `k` halves the rate. At the sketch level this is not a rounding
error — it is essentially all of the insert:

| `CMSHeap` top_k | Minsert/s | share of time spent in the heap |
| --- | --- | --- |
| 8 | 5.32 | 97.3% |
| 32 | 1.14 | 99.4% |
| 128 | 0.23 | 99.9% |
| 512 | 0.05 | 100.0% |
| 2048 | 0.01 | 100.0% |

The same `CountMin` without a heap runs at 200.6 Minsert/s.

## The cause

The heap itself is fine: `CommonHeap` sifts in `O(log k)`. The cost is
`refresh_positions`, which every accepted update calls:

```rust
fn refresh_positions(&mut self) {
    self.positions.clear();
    for (idx, item) in self.heap.iter().enumerate() {
        let slot = self.slot_for_item(&item.key);
        self.positions.entry(slot).or_default().push((item.key.clone(), idx));
    }
}
```

Per update that is `k` xxh3 digests over full keys, `k` `HeapItem` clones (a
heap allocation each for string keys), `k` SipHash probes into `positions`, and
`k` `Vec` pushes — to repair the `O(log k)` positions a sift actually moved.
It runs from all four mutation paths (`update`, `update_heap_item`, and both
insert branches).

## The change

Maintain the index incrementally instead of rebuilding it.

1. `positions: HashMap<u64, SmallVec<[usize; 2]>>` — store heap indices only.
   Identity is re-checked against `heap[idx].key`, so no key is ever cloned.
   `smallvec` is already a dependency.
2. `slots: Vec<u64>` parallel to the heap array, holding each resident's slot
   hash. A sift that moves an element then needs no re-hash of its key.
3. `CommonHeap` grows movement-reporting entry points — `push_with`,
   `update_at_with`, taking `&mut impl FnMut(usize, usize)` — so `HHHeap`
   patches only the slots a sift touched. The existing `push`/`update_at`
   delegate with a no-op closure, so no other caller changes.
4. `refresh_positions` survives for `clear` and for rebuilding after
   deserialization.

Per update this is one key hash, one map probe, and `O(log k)` index patches.

The probe implements this design as `IndexedHeap` and asserts, at every
capacity, that it retains the same top-k key set and the same counts as
`HHHeap`:

| capacity | `HHHeap` | indexed | speedup |
| --- | --- | --- | --- |
| 8 | 4.63 | 71.37 | 15.4x |
| 32 | 1.14 | 54.84 | 48.2x |
| 128 | 0.22 | 57.20 | 255.1x |
| 512 | 0.05 | 60.02 | 1218.1x |
| 2048 | 0.01 | 53.84 | 5815.8x |

The rate is flat in `k`, which is the point: capacity stops being a throughput
parameter. Folding 71 Mups/s into a 200 Minsert/s `CountMin` puts `CMSHeap`
around 50 Minsert/s at any top-k, against 5.3 today at top_k=8.

## Smaller wins, in the same change

- **Identity-hash the index.** `positions` is keyed by a u64 that is already an
  xxh3 digest; running SipHash over it again buys nothing. A
  `BuildHasherDefault<IdentityHasher>` removes that from every `find`.
- **Keep the index out of the serialized state.** `positions` is derived data
  and is serialized today. `#[serde(skip)]` on both `positions` and `slots`,
  with a rebuild on first use after deserialization, shrinks `UnivMon` state by
  the whole candidate index.
- **Counts only ever rise**, since they come from the sketch estimate. In a
  min-heap that means a resident update only ever sinks, so `update_at`'s
  "try `bubble_down`, else `bubble_up`" order is already the right one.

## The option that is not worth taking

Dropping the index entirely and scanning the `k` residents linearly is simpler
and still beats today's code by 5-30x, because it allocates and hashes nothing:

| capacity | `HHHeap` | linear scan | indexed |
| --- | --- | --- | --- |
| 8 | 4.63 | 24.47 | 71.37 |
| 32 | 1.14 | 7.54 | 54.84 |
| 128 | 0.22 | 2.05 | 57.20 |
| 512 | 0.05 | 0.57 | 60.02 |
| 2048 | 0.01 | 0.16 | 53.84 |

But it is `O(k)` per update, so it reintroduces the same falloff one order of
magnitude up. The indexed design costs one `Vec<u64>` and a movement callback
and is flat.

A Space-Saving / Stream-Summary structure would give `O(1)` amortized updates,
but it owns its own counters and its own eviction rule. `HHHeap` takes counts
from the sketch it is attached to, so that would be a different algorithm rather
than a faster index, and it would change which keys are retained.

## Compatibility

`HHHeap`'s field layout is internal. The portable MessagePack wire for the
top-k sketches carries a `(key, value)` list, not the heap's serde layout, and
no golden pins `HHHeap` bytes, so the field changes above are wire-safe.
