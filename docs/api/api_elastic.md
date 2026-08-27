# API: Elastic

Status: `Unstable`

> Warning: This API is kept for backward compatibility. It does not follow the full structured API parity used by Ready sketches.

## Purpose

Heavy/light split frequency estimator with a heavy bucket plus Count-Min backing sketch.

## Type/Struct

- `Elastic<H = DefaultXxHasher>`
- `HeavyBucket`
- `LAMBDA` — the paper's eviction threshold (8).
- `DEFAULT_LIGHT_ROWS` / `DEFAULT_LIGHT_COLS` — the default light layer (3 x 4096).

## Constructors

```rust
fn new() -> Self
fn init_with_length(l: i32) -> Self
fn init_with_dimensions(bucket_count: i32, light_rows: usize, light_cols: usize) -> Self
```

`new()` builds 8 heavy buckets over a 3 x 4096 light layer; `init_with_length`
takes the bucket count and keeps that same light layer. Both panic on a
non-positive bucket count or an empty light layer.

**Footprint.** The light layer is `rows * cols * 4` bytes of `i32` counters —
48 KiB at the default. Each heavy bucket is 40 bytes plus one heap allocation
for its resident flow id, so the heavy part is `bucket_count * 40` bytes and
change.

**Sizing the heavy part.** Bucket count is set by how many elephant flows you
expect to separate, not by the stream length. Section 3.1.2 gives the elephant
collision rate — the fraction of buckets holding more than one elephant flow —
in closed form:

```
P_hc = 1 - (H/w + 1) * e^(-H/w)
```

for `H` elephant flows over `w` buckets. Ten buckets per expected elephant
(`H/w = 0.1`) puts collisions at 0.46%; a hundred (`H/w = 0.01`) puts them at
0.005%. Collisions are what cost accuracy here, since a colliding elephant gets
evicted into the light layer and can inflate the mouse flows sharing its
counters.

**Sizing the light part.** These dimensions set the error on every flow that is
not resident in the heavy part — evicted elephants and all mice. Section 4.1
recommends depth 1 for speed, on the grounds that the heavy part already
carries the accuracy; the default here is 3, which is more accurate and slower.
Widening matters more than deepening: on a 20k-flow stream that evicted a
50-packet flow, a 1 x 64 light estimated it at 379, a 1 x 512 at 98, and the
default 3 x 4096 at exactly 50.

## Insert/Update

```rust
fn insert(&mut self, id: String)
```

Follows the SIGCOMM '18 insertion rule. A vacant bucket seats the flow. A
matching bucket takes a positive vote. Otherwise the bucket takes a negative
vote, and either the arriving flow goes to the light layer, or — once
`vote_neg >= LAMBDA * vote_pos` — the **resident** flow is evicted into the
light layer with its whole positive vote and the arrival takes the bucket with
`(vote_pos, vote_neg, eviction) = (1, 1, true)`.

### Overload mode

```rust
fn insert_heavy_only(&mut self, id: String)
```

Section 3.3 adapts to packet rate by letting arrivals "only access the heavy
part, so as to record the information of elephant flows only and discard mouse
flows". Call this instead of `insert` while your input queue is backed up, and
go back to `insert` when it drains; the sketch holds no mode of its own, since
only the caller can see the queue.

Seating and matching behave exactly as in `insert`. The two other cases differ:

- A non-matching arrival takes its negative vote and is then **dropped**. It
  does not reach the light layer.
- On takeover the arrival **inherits the evicted flow's positive vote** rather
  than starting at 1 — the paper's "the flow size of `f'` is set to the flow
  size of `f`" — and the evicted flow's size is **discarded**, not spilled.

The light layer is still read by `query`; it is only never written.

The arrival also **inherits the bucket's eviction flag**, and the negative vote
resets to `0`. The paper's prose does not name either, but the authors'
reference implementation ([BlockLiu/ElasticSketchCode](https://github.com/BlockLiu/ElasticSketchCode),
`src/CPU/ElasticSketch/HeavyPart.cpp`) settles both: `quick_insert` writes only
the new key and a zeroed guard, leaving the counter untouched. Since the flag
lives in that counter's high bit, the arrival takes over the size and the flag
together. The normal path differs — `insert` writes `0x80000001` there, setting
the flag and resetting the size to 1, which is the paper's case 4 `(f, 1, T, 1)`.

> **This path breaks the one-sided guarantee.** Every other operation on this
> sketch never returns less than the true count. Overload mode drops mouse
> flows outright and loses each evicted flow's accumulated size, so its
> estimates can and do come back low. Mixing it into a stream permanently
> lowers the answers for the flows it dropped.

Measured on a 400k-arrival Zipf(1.1) stream over 20k flows, single core:
33.3 vs 24.4 Mops/s at 64 buckets (1.37x), and 35.9 vs 30.2 Mops/s at 1024
buckets (1.19x). The gain comes from skipping the light layer's 3 hashes and
3 counter writes, so it is largest when collisions are frequent enough to send
arrivals there often.

## Query

```rust
fn query(&self, id: String) -> i32
```

Returns `vote_pos` for a resident flow whose bucket carries no eviction flag,
`vote_pos + light.estimate(id)` when it does, and the light estimate otherwise.
The estimator is one-sided: it never returns less than the true count.

## Merge

```rust
fn merge(&mut self, other: &Elastic<H>)
fn merge_max(&mut self, other: &Elastic<H>)
```

Both fold the two heavy parts into their light layers first. The merged sketch
then answers every flow from the light layer; its vacated buckets keep the
eviction flag so a later resident still reads its pre-merge mass. Both require
the same heavy bucket count, and `merge_max` additionally requires the same
light dimensions — SIGCOMM '18 §3.2.2 gives merging across different widths
only in its technical report, and that is not implemented.

`merge` is the paper's **Sum merging**: it adds the light layers counter by
counter. It is correct whatever the two sketches saw, including flows that
appear on both sides, and the paper calls it "simple and fast, but not
accurate".

`merge_max` is the paper's **Maximum merging**: it keeps the larger of each
counter pair. It is tighter than `merge` and still never underestimates, but
**only when the two sketches observed disjoint flow sets** — one flow per
measurement point, never the same flow at two of them.

> Using `merge_max` on sketches that share a flow underestimates that flow: it
> reads back as the larger side rather than the sum. Underestimation is exactly
> what Elastic's one-sided guarantee otherwise rules out, so reach for `merge`
> whenever a flow can repeat across sketches.

Measured on 80 disjoint flows totalling 275 through an 8-bucket heavy table and
a 2x64 light layer: `merge` estimates 434 in total, `merge_max` 359, roughly
halving the over-estimate.

## Growing the heavy part

```rust
fn expand_heavy(&mut self)
fn full_bucket_count(&self, t2: i32) -> usize
```

Section 3.4 starts the heavy part small and doubles it when elephants fill it:
"just copy the heavy part and combine the heavy part with the copy into one",
changing `h(.) % w` to `h(.) % 2w`. Lemma 3.2 — `(i % w) % w' = i % w'` — is
what makes that safe: every resident still hashes to a half that holds it, so
no estimate moves.

The paper's trigger is two thresholds: a bucket is full when its flows all
exceed `T2`, and the table is full when more than `T1` buckets are. Deciding
that is left to the caller — `full_bucket_count(t2)` reports the numerator, and
a library that silently doubled its own memory would be a surprise. Call
`expand_heavy` when your own `T1` is crossed.

After a doubling each flow sits in both halves, and the copy in the half it no
longer hashes to is stale. Cleanup is incremental, as the paper describes: an
insert landing on a stale copy drops it and seats the arrival. Buckets nobody
lands on keep their copy, which the paper notes "does not negatively impact the
algorithm".

Stale copies are skipped when a merge flushes the heavy part, so a flow is
spilled once rather than once per half — on both sides of the merge. Two
sketches expanded a different number of times have different bucket counts, so
`merge` and `merge_max` assert rather than silently misalign.

The reference implementation does not include this operation; its `HeavyPart`
is a fixed-size `template<int bucket_num>`. This follows the paper text.

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use asap_sketchlib::Elastic;

let mut sk = Elastic::init_with_length(8);
sk.insert("flow".to_string());
let _ = sk.query("flow".to_string());
```

## Caveats

- Light-layer counters are fixed at `i32`; the paper's 8-bit counters are not
  selectable.
- `insert_heavy_only` is lossy by design and does not keep the one-sided
  guarantee the rest of the API holds.
- String-centric API (`String` in insert/query).
- Lifecycle and parity differ from structured sketches.

## Departures from the reference implementation

The authors' code is at [BlockLiu/ElasticSketchCode](https://github.com/BlockLiu/ElasticSketchCode);
`src/CPU/ElasticSketch/` holds the sketch.

- **Eviction threshold.** Section 3.1.1 evicts once `vote-/vote+ >= lambda`,
  and this module does. `param.h` defines
  `JUDGE_IF_SWAP(min_val, guard_val) ((guard_val) > ((min_val) << 3))`, a strict
  `>`, so the reference swaps one packet later.
- **Bucket shape.** The reference is the software version of section 4.3: eight
  counters per bucket, seven flows sharing one guard counter, the smallest flow
  evicted, and the flag packed into the counter's top bit. This module is the
  basic version of section 3.1, one flow per bucket.

The takeover itself matches the paper and the reference: `HeavyPart.cpp` writes
`0x80000001` on a normal-path swap, which is the paper's `(f, 1, T, 1)`.

## Status

Unstable; migration work is tracked in `features.md`.
