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
- String-centric API (`String` in insert/query).
- Lifecycle and parity differ from structured sketches.

## Status

Unstable; migration work is tracked in `features.md`.
