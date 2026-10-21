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
counter count past `usize`, and on a shape whose hashes would not be
independent:

- `layers > UNIVMON_MAX_LAYERS` (19). Layer `i` hashes at seed index `i`, and
  the layers a key reaches are read from the hash at `UNIVMON_BOTTOM_LAYER_SEED_INDEX`
  (19). A 20th layer would hash at the finder's seed.
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
fn estimate_g_sum<F>(&self, g: F) -> f64
fn top_layer_complete(&self) -> bool
```

Every query is a G-sum `Σ_i g(f_i)` computed with the paper's offline
recursion (Algorithm 2, see [Paper](#paper)):

| Query | `g` | Paper |
| --- | --- | --- |
| `estimate_cardinality()` | `g(x) = x⁰`, `g(0) = 0` | §4.3, DDoS victim detection |
| `estimate_l2()` | `sqrt` of the G-sum with `g(x) = x²` | §4.1, F2 and the L2 norm |
| `estimate_entropy()` | `log2(m) − Y/m`, `Y` the G-sum with `g(x) = x log2 x` | §4.3, entropy estimation |
| `estimate_l1()` | the exact stream mass `m` | §4.3 footnote 4: `m` from a single counter |
| `estimate_g_sum(g)` | any `g` | Algorithm 2 |

Because UnivMon accepts only non-negative updates, the L1 norm is the stream
mass, so `estimate_l1()` returns the exact tracked total weight, the single
counter the paper reads `m` from; its G-sum form is `estimate_g_sum(|x| x)`.
`estimate_entropy()` is Shannon entropy in bits (log2); UnivMon-Q's is in nats.
`top_layer_complete()` reports whether the top layer's heap holds every key that
layer received: the check a caller makes for the Recursive Sum's exact top
layer (see [Paper](#paper)).

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

Every layer is a `CountSketchWithL2Norm` matrix, so `rows` carries that
sketch's wire bound: `1 <= rows <= 20` (`MATRIX_MAX_ROWS`), refused by
`to_asapv1` and `from_asapv1` past it.

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

## Paper

UnivMon follows Liu et al., "One Sketch to Rule Them All: Rethinking Network
Flow Monitoring with UnivMon", SIGCOMM 2016
(<https://zaoxing.github.io/papers/2016/SIGCOMM16_UnivMon.pdf>).

### Online sketching (Algorithm 1)

Layer `j` holds the substream `D_j` of keys whose sampling bits `h_1..h_j` are
all 1 (layer 0 holds the whole stream) and runs an L2 heavy-hitter sketch on
it. As in the paper's P4 data plane (§6.2), a layer's L2 heavy-hitter sketch is
a Count Sketch (`rows x cols`, `CountSketchWithL2Norm`) and a priority queue of
the `heap_size` keys with the largest estimates (`TopKHeap`). `update_weighted`
is Algorithm 1: a key updates every layer up to the deepest one its sampling
bits select.

### Offline estimation (Algorithm 2)

`Q_j` is layer `j`'s heap with each key's Count Sketch estimate `w_j(i)`: the
top-k heavy keys the paper's control plane feeds the recursion (§6.3). With `L`
the top layer:

- `Y_L = Σ_{i ∈ Q_L} g(w_L(i))`;
- `Y_j = 2 Y_{j+1} + Σ_{i ∈ Q_j} (1 − 2 h_{j+1}(i)) g(w_j(i))` for
  `j = L−1, ..., 0`;
- the estimate is `Y_0`.

`g` is applied to the Count Sketch estimate as a real number, and only to
positive estimates: a key whose estimate is not positive contributes
`g(0) = 0`, the paper's convention for the distinct count (`g(0) = 0`) and the
entropy (`0 log 0 = 0`), both in §4.3.

### Guarantee

The paper states no `(ε, δ)` bound of its own. §4.1 and §4.2 state that a G-sum
whose `g` is in the class Stream-PolyLog (informally, `g` monotonic and upper
bounded by `O(f²)`; the precise characterization is in Braverman and
Ostrovsky, "Zero-one frequency laws", STOC 2010) can be approximated in
polylogarithmic space by this universal sketch, and that Algorithm 2's output
is an unbiased estimator of the G-sum. For the theorems it refers to Braverman
and Ostrovsky's Recursive Sum ("Generalizing the layering method of Indyk and
Woodruff: Recursive sketches for frequency-based vectors on streams",
APPROX/RANDOM 2013; arXiv:1011.2571v1, "Recursive Sketching For Frequency
Moments"). §4.3 shows that the distinct count (`g(x) = x⁰`), the L1 and L2
norms and the entropy's `Σ f log f` (`g(x) = x log x`) are G-sums in
Stream-PolyLog.

arXiv:1011.2571v1, page 8 (verbatim; `ε` is the paper's `ǫ`):

> **Algorithm 4.1.** Recursive Sum[0](D, ε)
>
> 1. Generate φ = O(log(n)) pairwise independent zero-one vectors
>    H₁, . . . , H_φ. Denote D_j to be a stream D_{H₁H₂...H_j}.
> 2. Compute, in parallel, random cores Q_j = HH(D_j, φ³/ε², ε, 1/φ)
> 3. If F₀(V_φ) > 10¹⁰ then output 0 and stop. Otherwise compute precisely
>    Y_φ = |V_φ|.
> 4. For each j = φ − 1, . . . , 0, compute
>    Y_j = 2Y_{j+1} − Σ_{i∈Ind(Q_j)} (1 − 2h_i^j) w_{Q_j}(i).
> 5. Output Y₀.
>
> **Theorem 4.2.** Algorithm 4.1 computes (1 ± ε)-approximation of |V| and
> errs w.p. at most 0.3. The algorithm uses
> O(log(n) µ(n, 1/(ε² log³(n)), ε, 1/log(n))) memory bits, where µ is the
> space required by the above algorithm HH.

Here `V = (g(f_1), ..., g(f_n))`, and `HH(D, α, ε, δ)` is "an algorithm that
produces (α, ε)-cover w.r.t. V(D) w.p. 1 − δ" (page 8): every `α`-major key
(`g(f_i) ≥ α|V|`) is reported, and every reported value is within `(1 ± ε)` of
its `g(f_i)` (Definition 3.2, page 6).

Reading the parameters. As printed, Algorithm 4.1's `α = φ³/ε²`, Theorem 4.2's
`1/(ε² log³ n)` and step 4's sign and `h^j` index disagree with the recursion
defined on page 7 (`Y'_j = 2Y'_{j+1} + Σ (1 − 2h_i^{j+1}) w_i^j`, which
Algorithm 2 of the UnivMon paper and this implementation use). The proof
fixes the requirements: Lemma 3.8 (page 7) holds "for α = Ω(γ²/φ³) and
δ = Ω(1/φ)", and its proof chooses the per-layer accuracy
`ε' = γ/(30(φ + 1))` and needs `(φ + 2)(α/ε'² + 2δ)` small (page 8). That is,
for a target error `γ`: covers accurate to `γ/(30(φ + 1))`, `α` of order
`γ²/φ³`, and a per-layer failure probability `δ` of order `1/φ`, added over
the `φ + 1` layers by a union bound (Fact 3.7, page 7). Lemma 3.8 fails with
probability at most 0.2; Theorem 4.2's proof adds Markov's inequality for the
rest of the 0.3.

Independence: the analysis needs only pairwise independent sampling vectors
`H_j` (Lemma 2.2, page 3). The paper's introduction adds that its
frequency-moment application, which finds heavy hitters with Count-Sketch,
needs only 4-wise independent hashing rather than pseudorandom generators.

What this gives each query, when every condition holds:

- **Cardinality, F2, `Σ f log f`, L1:** `(1 ± ε)` of the G-sum with probability
  at least 0.7. `estimate_l2()` is the square root of the F2 G-sum, so its
  relative error is at most `ε` too (`|√(1 ± ε) − 1| ≤ ε`).
- **Entropy:** the statement covers `Y = Σ f log2 f`, not `H`. With
  `H = log2 m − Y/m` and `m` exact, `|Ĥ − H| = |Ŷ − Y|/m ≤ ε Y/m`, so the
  bound on `H` is additive: `|Ĥ − H| ≤ ε (log2 m − H)`, with probability at
  least 0.7. It is not a relative bound on `H`.

The failure probability is a constant; a smaller one needs the median of
independent copies, which this library does not take.

**The defaults are far outside this regime.** The statement needs
`heap_size` of order `φ³/ε²` with Count Sketches accurate to `ε/(30(φ + 1))`;
the default shapes (8 layers, heaps of 32 to 128) are orders of magnitude
smaller, so the 0.7 statement does not apply to them.

### Where this implementation departs from the paper

- **Number of layers.** The Recursive Sum uses `φ = O(log n)` sampled layers
  (the paper `log(n)`). `layers` is a constructor argument (default 8), at most
  `UNIVMON_MAX_LAYERS` (19); matching the statement needs `layers` near
  `log2(n) + 1` for the largest `n` expected.
- **Sampling hashes.** The paper draws `log(n)` pairwise independent hash
  functions `h_j : [n] → {0, 1}`. Here `h_j(i)` is bit `j` of one 64-bit hash
  of the key at seed index `UNIVMON_BOTTOM_LAYER_SEED_INDEX`, and layer `j`'s
  Count Sketch hashes at seed index `j`; the statement assumes these behave as
  independent uniform bits. `new` refuses a layer that would reuse the finder's
  seed and rows whose column and sign bits overlap (see
  [Constructors](#constructors)).
- **Cores.** A core here is the fixed-size top-`heap_size` heap with Count
  Sketch estimates (§6.2), not an `(α, ε)`-cover sized from `ε`. It is one when
  both conditions of Definition 3.2 hold. Capacity: at most `1/α` keys are
  `α`-major (their values sum to at most `|V_j|`), so `heap_size ≥ 1/α` leaves
  room for all of them, provided the heap ranks them above the other keys.
  Accuracy: every heap key's estimate must give `g(w_j(i))` within the cover's
  accuracy of `g(f_i)`, which `rows` and `cols` govern. The library does not
  size `heap_size`, `rows` or `cols` from `ε`, and does not check either
  condition.
- **Top layer.** The Recursive Sum computes `|V_φ|` exactly; Algorithm 2 and
  this implementation sum `g` over the top layer's heap. The two agree when
  the top layer's heap holds every key that layer received, which
  `top_layer_complete()` reports: a caller relying on the statement checks it.
- **Success probability.** 0.7 per instance; no median over copies.
- **L1 and entropy's `m`.** The exact total weight, as §4.3 footnote 4 allows.
- **Terminal mode.** `update_weighted_terminal` (the Joltik update-last-layer
  layout) is not in the paper. Its queries reconstruct each logical layer's
  candidates from the disjoint strata, apply `g` to integer counts, and on a
  layer whose candidate set is incomplete keep only keys above
  `floor(L2 / sqrt(heap_size))`, a filter of this implementation's own. No
  paper bound covers terminal-mode answers, for any `g`.
