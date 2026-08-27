# API: Elastic

Status: `Unstable`

> Warning: This API is kept for backward compatibility. It does not follow the full structured API parity used by Ready sketches.

## Purpose

Heavy/light split frequency estimator with a heavy bucket plus Count-Min backing sketch.

## Type/Struct

- `Elastic<H = DefaultXxHasher>`
- `HeavyBucket`
- `LAMBDA` — the paper's eviction threshold (8).

## Constructors

```rust
fn new() -> Self
fn init_with_length(l: i32) -> Self
```

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
```

Folds both heavy parts into their light layers and sums the light layers. The
merged sketch answers every flow from the light layer; its vacated buckets keep
the eviction flag so a later resident still reads its pre-merge mass.

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

- String-centric API (`String` in insert/query).
- Lifecycle and parity differ from structured sketches.

## Status

Unstable; migration work is tracked in `features.md`.
