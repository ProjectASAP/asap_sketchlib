# API: UnivMon

Status: `Ready`

## Purpose

Universal stream-monitoring sketch for L1/L2/cardinality/entropy, and a
free-list pool of scratch `UnivMon`s.

## Type/Struct

- `UnivMon`
- `UnivMonPool`

## Constructors

```rust
fn default() -> Self
fn new(heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self
fn check_shape(heap_size: usize, rows: usize, cols: usize, layers: usize) -> Result<(), String>

// UnivMonPool
fn new(initial_size: usize, heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self
fn take(&mut self) -> UnivMon
fn put(&mut self, sketch: UnivMon)
fn available(&self) -> usize
fn total_allocated(&self) -> usize
```

`UnivMonPool` is a free-list of scratch `UnivMon`s rather than a sketch, and
has no wire kind.

`check_shape` accepts exactly the shapes `new` builds, so a caller can refuse a
configuration without catching a panic. `new` panics on a zero dimension, on a
counter count past `usize`, on more than `MATRIX_MAX_ROWS` (20) rows, and on a
shape whose hashes would not be independent:

- `layers > UNIVMON_MAX_LAYERS` (19). Layer `i` hashes at seed index `i`, and
  the layers a key reaches are read from the hash at `UNIVMON_BOTTOM_LAYER_SEED_INDEX`
  (19). A 20th layer would hash at the finder's seed, and later ones would wrap
  onto layer 0's.
- `rows * (ceil_log2(cols) + 1) > 128`. Every row of a layer reads
  its column and its sign from one 128-bit hash; past this budget a deep row's
  column reuses a shallow row's sign bit.

## Insert/Update

```rust
fn update_weighted(&mut self, key: &DataInput, weight: i64)
fn update_weighted_terminal(&mut self, key: &DataInput, weight: i64)
fn clear(&mut self)
```

`update_weighted` maintains the cumulative sampled substream at every selected layer.
`update_weighted_terminal` uses the Joltik update-last-layer layout and reconstructs logical
layers during queries. Choose one update method for the lifetime of a sketch;
mixing the two physical layouts is rejected.

## Query

```rust
fn estimate_l1(&self) -> f64
fn estimate_l2(&self) -> f64
fn estimate_entropy(&self) -> f64
fn estimate_cardinality(&self) -> f64
fn estimate_g_sum<F>(&self, g: F, count_every_key: bool) -> f64
fn top_layer_complete(&self) -> bool
```

Because UnivMon accepts only non-negative updates, `estimate_l1()` returns the
exact tracked total weight. `estimate_entropy()` is Shannon entropy in bits
(log2); UnivMon-Q's is in nats. The generic recurrence remains available through
`estimate_g_sum` for compatible frequency functions.

`estimate_l2()` uses layer 0's Count Sketch F2 estimate in standard mode, and
`sqrt` of the summed per-layer F2 estimates in terminal mode, where the layers
hold disjoint strata. This differs from the paper's generic recurrence, which
`estimate_g_sum(|x| x * x, false)` computes. `estimate_cardinality()` counts
every heap key, including keys inserted with weight zero, whatever its
frequency estimate. `top_layer_complete()` reports whether the top layer's heap
holds every key that layer received. See [Error bounds](#error-bounds).

## Merge

```rust
fn merge(&mut self, other: &UnivMon)
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x10 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). UnivMon hashes through the
crate's default hasher, so its metadata carries that profile and no seed index:
the bottom-layer finder's seed is fixed by the algorithm, and layer `i`'s
counter hashes at seed index `i`.

The pyramid shape (`layers`, `rows`, `cols`, `heap_size`) and
the heaps' `key_type` live in the metadata; the payload is
`[counts, l2, heap_lens, keys, heap_counts, candidate_complete, bucket_size,
update_mode]`. Each layer's `CountSketchWithL2Norm` state is inlined rather than nested in
an envelope of its own, and the heap indexes are rebuilt on load, so no index
reaches the wire. Heap entries are emitted layer by layer, each layer in
descending count with ties broken by a total order over the key, so a decoded
pyramid re-serializes byte-identically. `update_mode` and `candidate_complete`
are carried: they are acquired from the stream, and a decoder that guessed them
would pick the wrong query recurrence or report a widened threshold as zero.

Every layer is a `CountSketchWithL2Norm` matrix hashed per row, so `rows` carries that
sketch's bound: `1 <= rows <= 20` (`MATRIX_MAX_ROWS`, the seed list
length), refused on both sides past it.

## Examples

```rust
use asap_sketchlib::{DataInput, UnivMon};

let mut um = UnivMon::new(32, 3, 1024, 4);
um.update_weighted(&DataInput::Str("flow"), 1);
assert!(um.estimate_l1() >= 1.0);
```

## Caveats

- Updates must have non-negative weights.
- Structure parameters and update strategies must match before merge.
- Merge combines total weight, recomputes cached L2 state, and re-estimates the
  union of bounded heavy-hitter candidates from merged counters.

## Status

Primary multi-metric framework.

## Error bounds

For `0 < epsilon < 1`, these statements use an ideal independent uniform hash
model, with the finder hash independent of each layer's Count Sketch; `new`
refuses the shapes that break this (see Constructors). They apply to full-stream
updates, not to a promoted-only Octo aggregator. Write `r = rows` and `w = cols`.
No counter, squared-norm accumulator or intermediate arithmetic may overflow;
`F2 < i64::MAX` alone does not ensure that a colliding row's squared norm fits.

### L2

Take one row. With column `c_i` and sign `s_i` for key `i`, the row's estimate
is `Z = Σ_c (Σ_{i: c_i = c} s_i f_i)² = F2 + Σ_{i≠j} s_i s_j f_i f_j [c_i = c_j]`.
The signs are independent of each other and of the columns, so `E[Z] = F2` and
`Var[Z] = 2q (F2² − F4) ≤ 2q F2²`, where `F4 = Σ f_i⁴` and `q = P(c_i = c_j)`.
A column is a hash slice of `⌈log2 w⌉` bits taken modulo `w`, so `q = 1/w` when
`w` is a power of two and `q ≤ 9/(8w)` otherwise (the slice values
`w..2^⌈log2 w⌉` fold onto the low columns; the worst case,
`w = ¾·2^⌈log2 w⌉`, doubles a third of them).

By Chebyshev, one row misses by more than `ε F2` with probability at most
`p = 2q / ε²`. The rows hash disjoint bit slices of one 128-bit hash, so they
are independent. Their median (the mean of the two middle rows when `r` is
even) misses only when at least `⌈r/2⌉` rows miss on the same side, hence

`P(|F2_est − F2| > ε F2) ≤ δ = P(Binomial(r, p) ≥ ⌈r/2⌉)`.

The bound is useful only for `p < 1/2`. Because `|√(1 ± ε) − 1| ≤ ε`, the same
`ε` and `δ` bound the relative error of the returned L2 norm.

In terminal mode each stratum's estimate satisfies this bound for its own `F2`.
If every non-empty stratum is within `ε` of its own `F2`, the sum is within `ε`
of the total, so by a union bound the failure probability is at most `δ` times
the number of non-empty strata (at most `layers`).

### Distinct count (standard mode)

Let `S_j` be the keys reaching layer `j`, `H_j` its heap of `min(|S_j|, k)`
keys, and `M_j = S_j \ H_j`, where `k = heap_size >= 8`. The finder bits
`1..=j` select `S_j`; write `b_(j+1)(i)` for the next bit. For `L = layers`,
define an ideal estimator `Y*` that uses the exact top count `|S_(L-1)|` and
the implementation's recurrence below it:

`Y*_j = 2 Y*_(j+1) + sum_(i in H_j) (1 - 2 b_(j+1)(i))`.

Telescoping gives

`Y*_0 - F0 = -sum_(j=0..L-2) 2^j sum_(i in M_j) (1 - 2 b_(j+1)(i))`.

Each `M_j` is determined before the independent next finder bits are exposed,
so the layer increments form a martingale difference sequence: unconditionally
`E[Y*_0] = F0` and `Var[Y*_0] = sum_j 4^j E[(|S_j| - k)^+]`. Bound the
expectation by `F0 / 2^j` on low layers, and on high layers by
`(F0 / 2^j)^3 / (5 k^2)`, using `(x-k)^+ <= x(x-1)(x-2)/(5 k^2)` for `k >= 8`.
Splitting at `2^j` in `[0.45, 0.9) * F0/k` gives
`Var[Y*_0] <= 1.35 F0^2/k <= 2 F0^2/k`.

Let `C` be the event that the top heap is complete (`top_layer_complete()`),
and `Y = estimate_cardinality()`. On `C`, `Y = Y*_0`, so Chebyshev gives the
**joint** bound

`P(|Y - F0| > epsilon F0 AND C) <= d`, where `d = min(1, 2/(k epsilon^2))`.

Completeness does not imply conditional unbiasedness. With `F0 = 9`, `k = 8`,
`L = 2`, any layer-0 heap misses one key. Of 512 equally likely next-bit
assignments only the all-ones one makes the top incomplete, and the errors over
the other 511 sum to -1, so `E[Y - F0 | C] = -1/511`.

If `P(not C) <= rho < 1`, then

- `P(|Y-F0| > epsilon F0 | C) <= min(1, d/(1-rho))`;
- without rejecting incomplete results, `P(|Y-F0| > epsilon F0) <= min(1, d+rho)`.

For ordinary updates, `C` holds exactly when `|S_(L-1)| <= k`. Sizing
`L >= ceil(log2(F0_max/k)) + 2` makes its mean at most `k/2`, so Chernoff gives
`rho <= (e/4)^(k/2)`. A completeness flag lowered by a merge or by partial
delivery needs its own completeness probability. A normal approximation for
the ideal estimator suggests `2 exp(-epsilon^2 k/4)`; this is not a rigorous
tail bound. Terminal mode reconstructs candidates from strata and is outside
this proof.
