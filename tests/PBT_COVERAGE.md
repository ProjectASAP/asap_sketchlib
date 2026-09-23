# Property Test Coverage

One `proptest` law per line. Each compares the sketch against an answer
computed without it; what that answer is comes first in every section. One
file per sketch under `tests/pbt/`. `cargo test --test pbt` runs 367 laws;
with `--all-features`, 435, adding the four **experimental** modules. Every
law was mutation-checked: break the implementation, the law goes red. Shared
generators, `grid` and `round_trip!` live in `support.rs`.

Notation: `S(xs)` is a sketch fed stream `xs`; `xs ++ ys` is concatenation;
`perm(xs)` a permutation; `truth(k)` the exact answer computed from the stream.
`round_trip!`: `decode(encode(s))` answers what `s` answers at the named probes
and re-encodes to the same bytes; a refused encode passes. Counts are laws as
`cargo test` lists them; a macro expanding one law per shape is noted.

## bloom.rs — 11

Compared against: the bit grid read via `as_bits`, and the stream's distinct keys. All exact.

- membership
  - `contains(k)` for every inserted `k`; 1-7 slices x 8-255 bits
  - once `contains(p)`, always `contains(p)`; every probe re-read after every insert
- both hash paths, `RegularPath` and `FastPath`, through their own `insert` / `contains`
  - no member lost; `slices <= bits set <= slices * distinct` (non-empty stream); `fill_ratio() == bits set / bit_capacity()`; `bit_capacity() <= BLOOM_MAX_BITS`; `inserted() == arrivals`. Geometries 1-4 x 1-8 (saturates) and 1-7 x 256-1023 (mostly zero)
- grid
  - `grid(S(xs)) == grid(S(perm(xs)))`
  - after `clear`: grid, `inserted()`, `is_empty()`, `fill_ratio()` equal a fresh `with_dimensions`; no former member `contains`
  - all bits set: every probe `contains`; `fill_ratio() == 1.0`; `estimated_fpp() == 1.0`
- merge
  - `merge(a, b)` contains every member of `a` and of `b`
  - `merge(a, b) == merge(b, a)`; `merge(a, a) == a`
  - `grid(merge(S(xs), S(ys))) == grid(S(xs ++ ys))`; `inserted()` adds
- sizing
  - `dimensions_for(expected, target)`: `1 <= slices <= BLOOM_MAX_SLICES`; width a power of two; `slices * width <= BLOOM_MAX_BITS`. `expected` up to `usize::MAX`; `target` includes `0.0`, `-0.0`, `1.0`, `-1.0`, `MIN_POSITIVE`, `EPSILON`, `MAX`, `MIN`
- wire
  - ASAPv1 keeps every bit; `edge_dimension` x `bloom_rows`

## coco.rs — 12

Compared against: a `(row, col)` scan of `table[i][j].full_key` / `.val`, and the stream's exact sum. Regime: key domain 4x the table, so every bucket is contested.

- mass
  - `sum(val) == sum(stream weights)`
  - a new key grows exactly one bucket by its whole weight; that bucket is one of its `d` candidates and held their minimum `val`
  - each key in at most one bucket
  - `recorded_flows()`: each occupied bucket once, row-major; values sum to the stream mass
- queries, against the scan
  - `group_by(f) == buckets folded by f(full_key), summed`
  - `estimate_projected(p, f) == that fold at one group`, including an empty group
  - `estimate_substring(p) == sum of buckets whose key contains p`; probes include `item1`, which is inside `item10`
  - `estimate_with_udf(p, pred) == sum of buckets pred accepts`
  - `estimate_key(k) == val of k's bucket`, 0 if none; `<= stream total`
- merge
  - merged mass `== mass(xs) + mass(ys)`
  - each key of either side in at most one bucket
- wire
  - ASAPv1 keeps `estimate_key` for 64 keys; `edge_dimension` x `edge_rows`

## count_min.rs — 19

Compared against: an exact `HashMap` count, columns via the public `hash_for_matrix`. Path laws compare two real implementations.

- paths
  - `cells(S(xs)) == cells(S(perm(xs)))`
  - `insert_many(k, n) == n x insert(k)`, cell for cell
  - `bulk_insert_many_with_hashes == insert_many`, cell for cell
  - `DefaultMatrixI32`, `QuickMatrixI64` == `Vector2D`, cell for cell, at their fixed geometries
- estimator
  - `estimate(k)` never drops on a non-negative insert; all 32 keys probed after every insert
  - `truth(k) <= estimate(k) <= truth(k) + min over rows of colliding mass`; 1-8 cols x 32 keys, every row collides
  - portable `CountMinSketch`: `estimate(k) >= truth(k)`
- merge
  - commutative, associative, empty is identity
  - `merge(S(xs), S(ys)) == S(xs ++ ys)`
  - `merge` never lowers `estimate(k)`; `k` from either stream or from neither
- `merge_max`
  - disjoint keys: `max(a, b) <= cell <= a + b`; an empty peer cell moves nothing; `estimate(k)` still inside the band above
  - `merge_max(s, s) == s`
  - shared keys: `max(a, b) <= cell <= a + b`, and the same for every estimate
- model across a rebuilt backend
  - after any sequence of insert, merge, `apply_delta`, msgpack round trip, mismatched merge: geometry unchanged; every cell `>= 0`; every row sums to total mass; `truth(k) <= estimate(k) <= band`. `apply_delta == merge`; a mismatched merge is refused and changes nothing
- wire
  - msgpack keeps geometry, cells, estimates
  - ASAPv1 keeps 64 estimates; `edge_rows` x `edge_dimension`

## count_sketch.rs — 12

Compared against: a key's `(col, sign)` per row, read off a one-key sketch; the stream's exact signed sum. Regime: 512 keys; sign law 1-5 rows x 1-7 cols, median law 1-7 rows x 1-5 cols.

- paths
  - `cells(S(xs)) == cells(S(perm(xs)))`
  - `insert_many(k, n) == n x insert(k)`
- sign and column
  - `k` hits one fixed `(col, sign)` per row and nothing else in the row; checked after each of 120 background arrivals
  - one-key stream: `estimate(k) == truth(k)`; unit and signed weights
  - `min(row readings) <= estimate <= max(row readings)`; odd rows: `estimate == median`
- linearity
  - insert `w` then `-w` for every `w`: cells back to start; signed stream over a background
  - `insert(k, -1)` undoes `insert(k)`, cell for cell
  - `cells(S(xs)) + cells(S(ys)) == cells(S(xs ++ ys))`; integral weights, power-of-two widths
  - negate every weight: every cell negated
- merge
  - portable `CountSketch`: `merge(S(xs), S(ys)) == S(xs ++ ys)` to `REL_TOL`
- wire
  - msgpack keeps cells and estimates
  - ASAPv1 keeps 64 estimates; `edge_rows` x `edge_dimension`

## ddsketch.rs — 14

Compared against: the sorted stream. `DDSketch::get_value_at_quantile` targets the `ceil(q*n)`-th value, portable `DdSketch::quantile` the `floor(q*(n-1))`-th. Streams carry negatives, zeros and positives inside the indexable band.

- placement
  - negative store, zero counter, positive store hold exactly the three class counts; `get_count() == arrivals`
  - portable `DdSketch`: the same three counts
  - `sum() == stream sum`, bit for bit
  - first dense allocation: one 128-bucket chunk, seed value at its center; both impls, both signed stores
- relative error `<= alpha`
  - `|quantile(q) - exact| <= alpha * |exact|`, across the sign boundary, both impls
  - `quantile(q)` non-decreasing in `q`, across the sign boundary
  - single value `v`: `quantile(q) == v` for every `q`; `min`, `max`, `q=0`, `q=1` equal the tracked extremes
- merge
  - counts add; both stores, both offsets, zero counter identical either way round
  - merged `quantile(q)` within `alpha` of the combined stream's exact value
  - different `alpha`: refused, receiver count unchanged, both impls; same `alpha`: merges
- wire
  - msgpack keeps stores, offsets, zero count, quantiles
  - ASAPv1 keeps count, `alpha`, `sum`, quantiles; stream of NaN, ±inf, ±0, f64 extremes

## elastic.rs — 12

Compared against: an exact `HashMap` count; each arrival adds its weight to exactly one place, and a takeover moves the resident's positive vote into the light layer. Regime: 1-4 heavy buckets, 4-47 flows, streams of 64+; eviction needs `LAMBDA` negative votes per positive.

- generator
  - 1 bucket round-robined over 9 flows evicts, and mass reaches the light layer
- mass
  - `heavy positive votes + any one light row == arrivals`
  - weighted: `== summed weight`
  - after merge: `heavy + any light row == arrivals of both`
  - merged light cell `>= a + b`
- estimates
  - resident with no eviction flag: `query(k) == truth(k)`
  - `query(k) >= truth(k)`, before and after merge
- bucket invariants, every arrival
  - an insert never clears an eviction flag
  - occupied bucket: `negative votes < LAMBDA * positive votes`
- heavy messages
  - `merge_heavy(id, a + b, flag)` == `merge_heavy(id, a, flag)` then `merge_heavy(id, b, flag)`: estimate, residency, flag, total mass
- wire
  - ASAPv1 keeps `query` for 64 flows; `edge_dimension` buckets, `edge_rows` x `edge_dimension` light grid

## ensemble.rs — 7

Compared against: a bare sketch of the member's type on the same stream. The layer hashes once per insert from a cached layout; members decode Count-Min packed columns, Count column and sign bit, and HLL low 64 bits from that one hash. Member lists carry both matrix families and all three HLL variants.

- member == bare sketch
  - each frequency member == bare `CountMin` / `Count` of its geometry; widths cycle through a pool, so one layer mixes widths on one hash
  - no matrix member, hashing at `CANONICAL_HASH_SEED`: each HLL member == bare HLL
- entry points
  - `hash_input` + `bulk_insert_with_hashes == insert`; `estimate_with_hash == estimate`
  - `bulk_insert_at(chosen, values)`: chosen members == fully fed, the rest == untouched
  - a mid-stream `push` lands last and sees only the rest of the stream; with a matrix member already fixing the layout, existing members do not move
- contract
  - `sketch_type()` names the family; `estimate` succeeds exactly for frequency members, `cardinality` exactly for the rest; any accessor at `index >= len()` fails
  - a matrix member of another row count: refused by `Layer::new` and by `push`; a refused `push` changes no answer

## exponential_histogram.rs — 16

Compared against: the raw arrival stream, from which the window and each bucket's run are recomputed. A Count-Min row sums to its inserts, so a bucket's arrivals are readable without its `size` field.

- bucket invariants, every arrival
  - at most `k/2 + 2` buckets per size
  - sizes are powers of two, non-increasing from oldest to newest
- window
  - retained contains the window; `|retained| - |window| <= oldest bucket size`
  - buckets tile the retained suffix: arrivals per bucket `== size`; `(min_time, max_time)` == first and last of its run; runs abut; `merge(all buckets) == CountMin(suffix)`
  - `query_interval_merge` on bucket boundaries == exactly those buckets' arrivals, cell for cell; no two arrivals share a timestamp
- tumbling windows, vs the retained set from stream, width and `max_windows`
  - `closed_count() == min(elapsed periods, max_windows)`, empty periods included
  - `TumblingWindow<FoldCMS>`, `TumblingWindow<FoldCS>`: `merge(retained windows)` flat counters == one sketch on the retained arrivals
  - `TumblingWindow<KLL>`, `k = 512`, no compaction: merged buffer == retained observations, one level
  - `TumblingWindow<KLL>`, `k = 32`, compacted: merged weight `>= retained count`; every item arrived; quantiles inside the retained range
  - `TumblingWindow<UnivMonQ>`: merged count == retained count; merged extremes == retained extremes
- pool recycling: recycled == fresh, on return and after both eat the same second half of a stream
  - `SketchPool<FoldCMS>`, `SketchPool<FoldCS>`: counters, entry count, heap length
  - `SketchPool<KLL>`: count, items, levels, compaction coin
  - `SketchPool<UnivMonQ>`: count, extremes, L1, F2, CDF
  - `UnivSketchPool`: L1, L2, cardinality, candidate flags

## fold.rs — 21

Every law is an equality. Compared against: the sketch definition over an exact `HashMap`; a key's `(col, sign)` read off a level-0 one-key sketch via `cell` / `iter`. Regime: 48 keys, 8-64 full cols, folded 0-6 levels, so `FoldCell::Collided` occurs.

- per `FoldCMS` and `FoldCS` (12)
  - `fold(S, level).query(k) == S.query(k)` at every admitted level; `cols() == full_cols >> level`
  - `unfold_to(t)` keeps every query, for every `t <= level`
  - each row holds at most one entry per full column, at every level
  - `unfold_to(h).unfold_to(l) == unfold_to(l)`; `unfold_full() == unfold_to(0)`
  - `unfold_merge(a, b) == widen(merge_same_level(a, b))`, cell for cell
  - `hierarchical_merge` over `2^level` sub-windows == balanced tree of `unfold_merge` at level 0 == one full-width sketch on the concatenation
- Count-Min
  - `to_flat_counters() == column sums`, every level
  - `query(k) == min over rows of the counter k addresses`
  - `query(k) >= truth(k)`; non-negative stream
- Count sketch
  - `to_flat_counters() == signed column sums`
  - `min(row readings) <= query(k) <= max(row readings)`
  - 3 or 5 rows: `query(k) == median`
  - `(col, sign)` of `k` unchanged across levels and unfolds
- `FoldCell`
  - variant is a function of distinct columns held: `Empty` at 0, `Single` at 1 (a repeat accumulates), `Collided` at 2+; `entry_count() == distinct columns`; `query == running sum`, 0 for an absent column
  - `merge(c1, c2)` sums per column; `merge(c1, c2) == merge(c2, c1)`

## heaps.rs — 12

Compared against: the offers sorted and truncated. `HHHeap` is path dependent once full, so its exact laws run only where the path cannot matter.

- `CommonHeap`
  - `len() == min(pushed, capacity)`; `len() <= capacity`; `is_full()` agrees
  - min-heap retains the `capacity` largest offers as a multiset; max-heap the smallest
  - `peek()` == the retained extreme in the heap's direction
  - heap property at every parent-child pair, both orderings
  - `pop` drains non-decreasing, yields `min(pushed, capacity)` values, leaves the heap empty
  - `clear`: `len() == 0`; `peek() == None`; capacity kept
- `HHHeap`
  - `len() <= k` at every step
  - a resident's count == the last count offered for its key
  - domain `<= k`: every distinct key resident at its last count
  - one offer per key: the `k` largest counts resident
  - `update` returns true iff the key was resident or there was room, i.e. nothing was displaced

## hll.rs — 101

Models use only what the paper fixes; register indexing and rank mapping stay free. For `Classic` and `ErtlMLE`, `estimate` is a function of the registers, so laws relate estimates rather than restate the formula. Registers `<= 40`, so `sum(2^-M[j])` is exact in f64.

- insertion, per precision 4, 6, 8, 10 (16)
  - registers are a function of the distinct set; repeats and interleaving invisible
  - `insert_many_with_hashes` over `canonical_hash` == `insert`, register for register
  - no register decreases; whole array checked every arrival
  - one arrival moves at most one register
- estimation, per `Classic` and `ErtlMLE` at those precisions (40); registers set via public `insert_with_hash` and asserted equal to the target
  - all registers 0: `estimate() == 0`
  - `estimate(perm(registers)) == estimate(registers)`
  - every register +1: `estimate` doubles; drift `0..=1` for `Classic` (truncates), `-1..=1` for `ErtlMLE` (rounds)
  - `estimate(S(xs)) == estimate(S(perm(xs)))`
  - a register rises: `estimate` does not fall
- merge, per precision (20)
  - `merge == elementwise max`
  - commutative, idempotent, empty is identity
  - `merge(S(xs), S(ys)) == S(xs ++ ys)`
- HIP, per precision (20); registers read back through the wire format
  - empty: `estimate() == 0`
  - `estimate` non-decreasing; a repeated key adds 0
  - order matters: a descending run into one register estimates exactly 1, an ascending run `> 1`
  - an arrival that raises a register adds `m / sum(2^-M[j])` over the prior registers; one that raises nothing adds 0; both sides exact in f64, compared after truncation to `usize`
- portable `HllSketch` (5)
  - merge commutative, idempotent, empty identity, `== S(xs ++ ys)`; precisions 4-11
  - msgpack keeps variant, precision, registers, estimate; `Regular`, `Datafusion`, `Hip`; precisions 4-13
  - ASAPv1 on `HyperLogLog<ErtlMLE>` keeps registers and estimate; empty included

## hydra.rs — 7

Compared against: the map rebuilt from the subkey encoding (`label ":" value`, joined by `";"`, escaped) and the public matrix hash at `HYDRA_SEED`. All records share one payload value, so each cell's Count-Min is exact. Labels and values include the escaped characters. Cols in {1, 2, 3, 8, 64, 251, 256}.

- write
  - a record writes each of its `2^D - 1` non-empty equality subsets to the cell that subkey names, per row, and nothing else
  - `count` is the weight added, not a write count
- read
  - an answer is a median of the subkey's cells: at least half the rows `<=` it and at least half `>=` it. Probes: every subset of every record, every single-column equality, one absent value
- merge
  - `merge(S(xs), S(ys)) == S(xs ++ ys)`, cell for cell
- `HydraKllSketch`, no schema, no fan-out
  - one key hits one cell per row at `xxh32(key, row) % cols`, read back as retained count
  - query == median of its row cells, as an order statistic; an absent key reads 0
- wire
  - ASAPv1 keeps every subpopulation answer and the schema

## kll.rs — 14

Compaction is randomized, so no law names retained items. Compared against: the sorted stream. Rank band `16 * n / k`; worst drift seen about 3.3. The accuracy law draws `k` from {200, 400} and asserts band `< n/10`.

- count
  - `count() == n` below compaction
  - merged `count()` within `max(0.1 n, 4)` of `|xs| + |ys|`
- quantiles
  - `quantile(q)` non-decreasing in `q`
  - `quantile(0.0)`, `quantile(1.0)` inside `[min, max]` of the stream
  - every `quantile(q)` is a stream value, bit for bit; narrow domain
  - `n <= k`: `quantile(0.0) == min`, `quantile(1.0) == max`; `n == k` drawn explicitly
- rank and CDF
  - `q*n - 1e-9*n <= rank(quantile(q)) <= q*n + 2^(num_levels - 1)`
  - `rank` non-decreasing; 0 below the stream; `== count()` at max
  - `cdf(x) * count == rank(x)` to `REL_TOL`; CDF monotone in `[0, 1]`, 0 below, 1 at max. `rank` and `cdf` share no code
- accuracy
  - `|true_rank(quantile(q)) - q*n| <= 16 * n / k`; a claim on rank, not on value
- determinism
  - same seed, same stream: same items
  - `clear` re-seeds from the stored seed: replay == fresh, items and levels
- wire
  - msgpack keeps `k`, count, quantiles
  - ASAPv1 keeps count and quantiles under NaN, ±inf, ±0, f64 extremes

## kll_dynamic.rs — 14

The dynamic item path. Compared against: the sorted stream. Streams `>= 30 k`, so level 0 spills about `n/k` times; a narrow and a wide domain mixed. Rank band `8 * n / k`.

- quantiles
  - `quantile(q)` non-decreasing in `total_cmp` order, so NaN and ±0 are ordered too
  - every `quantile(q)` is a stream value, bit for bit
  - before compaction: count, both ends, and every `quantile(q)` == the exact rank-`q` value; `n == k` drawn explicitly
- rank and CDF
  - `rank(quantile(q)) >= q*n`; `rank(stream predecessor of quantile(q)) <= q*n`; exact under ties
  - `rank`: 0 below the stream, `count()` at `quantile(1.0)`, `>= 1` at `quantile(0.0)`, non-decreasing
  - CDF in `[0, 1]`, monotone, 0 below, 1 above
  - `cdf(x) * count == rank(x)` to `REL_TOL`; distinct values only
  - `rank` counts by `<=`, CDF sorts by `total_cmp`: `rank(NaN) == 0`; a retained NaN counts for no probe; `-0.0` and `0.0` are one value to `rank`
- accuracy
  - `|true_rank(quantile(q)) - q*n| <= 8 * n / k`; the one law that tells halving a sorted level from halving an unsorted one
- merge
  - `merge(fresh(k), s)` == `s` in `count` and in `rank` at every probe
- determinism
  - same seed and stream: byte-identical
  - `clear`: byte-identical to empty; replay == fresh
- wire
  - ASAPv1 keeps count and quantiles under extreme values
  - coin state is carried: decoded and original, fed the same 200-400 values, re-encode identically

## nitro.rs — 10

Compared against: a bare target fed through its own `insert`, not `NitroTarget::update_sample`; and integer arithmetic in the test. Below rate 1.0 a single insert has no deterministic expected value, so nothing asserts one.

- rate 1.0
  - `CountMin` target cells == bare `CountMin`; via `insert` and `insert_cached_step`
  - same for a `Count` target
  - `merge == bare S(xs ++ ys)`
- the seed fixes the sample
  - same seed and stream: same cells, both paths, rates 1.0 down to 0.01
  - dyadic rate `1/2^s`: 1-15 `admitted_weight` probes do not move the admitted subset
- compensation
  - admitted weight is `scaled_increment(w)` or `+1`, over 16 draws; factor unchanged after
  - rate `1/2^s`: compensation `== w << s`, every draw
  - 20,000 draws: `|mean admitted - w / rate| < 0.05`; non-integer reciprocal
- thinning
  - rate `1/2^s`: every cell `% 2^s == 0`; rows hold equal mass; `admitted < arrivals`; streams of 256-511
- merge
  - cells add; operand unchanged

## octo_delta.rs — 25

A worker holds counts back until one reaches `tau`, promotes that `tau` step, and keeps the rest as residual. Compared against: a single-threaded sketch on the same stream; `parent + residuals` must reconstruct it. Per type: addition for Count-Min, Count, DDSketch and UnivMon layers; max for HLL; replay through the parent's election for Coco and Elastic.

- Count-Min, Count: addition
  - `parent + residual == reference`, cell for cell; `|residual| < tau`; via `insert_emit_delta_with_threshold`, and via `CmWorkerSketch` / `CountWorkerSketch` with a byte-wide residual
  - flush over 1-4 shards: `parent == reference`; every worker counter 0
- DDSketch: addition, sparse
  - `parent + residual == reference` per bucket; no bucket the stream never filled; `promoted + held == reference count`; `residual < tau`
  - flush over 1-4 shards: buckets and count `== reference`
- HLL: max, never cleared; 16 registers, so shards collide
  - `parent[j] <= stream[j]`; `2^stream[j] - 2^parent[j] < 2^tau`
  - flush over 1-4 shards: `parent == reference`
  - `tau == 0`: `parent == max over shards == reference`, no flush
  - applying deltas is order independent and idempotent
- Coco: replay
  - `promoted + residual == arrivals`; each promotion carries exactly `tau` and a key that arrived; `residual < tau`
  - after flush: parent mass == stream mass; no worker holds a bucket back
  - victim == least loaded of the `d` candidates; one bucket per array, distinct keys below `tau` stay within 1 of each other
  - `tau == 1`: the promotion names the arrival, not the previous occupant
- Elastic: heavy replaced, light added
  - `heavy promotions + evicted votes + light total / rows == arrivals`; a heavy promotion carries `tau`, an eviction `< tau`; every named key arrived
  - flush over 1-4 shards: `heavy votes + light mass per row == arrivals`
  - the 8th rival arrival (`LAMBDA` negatives vs 1 positive) hands over the resident with exactly its 1 vote
- UnivMon: one Count layer per level
  - `parent + residual == layer cells`; flush closes the gap; a delta-only parent has `L2 == reference L2`
  - a delta carries `max(tau >> layer, 1)`, worker id, the worker's running weight, the key
- keyed deltas beside the aggregator heap
  - `CmTopKOctoPlan`, keyed `CountWorkerSketch`: `parent + residual == cells`; heap holds `min(promoted keys, top_k)`, only arrived keys, and (Count-Min) no count `> parent estimate`
- default thresholds
  - `CM_PROMASK`, `COUNT_PROMASK`, `DD_PROMASK`, `COCO_PROMASK`, `ELASTIC_PROMASK`, `UNIVMON_PROMASK` in `1..=MAX_PROMASK`; each fires within 600-800 inserts of one key; equals every promotion step of its type
  - `max_hll_threshold(p) == 64 - p`; `HLL_PROMASK` below it at default precision; a worker at it holds nothing back
  - `MAX_PROMASK == i8::MAX`; `OctoThreshold` clamps 0 up to 1 and above down to `MAX_PROMASK`; every promotion fits an `i8`

## set_aggregator.rs — 9

Exact. Compared against: a `HashSet` of the stream. Keys from `[a-c]{0,2}`, so streams repeat and intersect.

- exactness
  - `keys() == distinct(stream)`; `len() == |distinct|`
  - `contains(k)` iff `k` was inserted
  - `S(xs) == S(perm(xs))`
  - `S(xs ++ xs) == S(xs)`
- merge is union
  - `merge(a, b) == union(a, b)`; operand unchanged
  - commutative, associative, idempotent
  - empty is a two-sided identity
  - `merge_refs` unions all inputs; refuses an empty list
- wire
  - msgpack keeps the key set, and again on a second round trip; keys include `""`, non-ASCII, embedded NUL, 300 chars, arbitrary `String`

## space_saving.rs — 19

Compared against: an exact `HashMap` count. Every bound is deterministic.

- bounds
  - monitored `k`: `error <= count`; `count >= truth(k)`; `count - error <= truth(k)`
  - `upper_bound(k) >= truth(k)` for every stream key
  - unseen `k`: `estimate == 0`; `upper_bound == error == min_count`; `is_guaranteed == false`
- structure
  - `len() == min(distinct, capacity)`; `len() <= capacity`
  - every `error <= min_count`
  - `sum(counts) == total() == arrivals`
  - full summary: `min_count >= smallest counter`; `min_count * capacity <= total()`
  - `top_k(k)`: `min(k, len)` entries, non-increasing, each matching the table; nothing above the cut left out
  - a guaranteed key's truth `>` every dropped key's truth
  - `min_count` non-decreasing over the stream
- domain `<= capacity`
  - exact: `count == truth`; `error == 0`; `upper_bound == truth`; order-free
  - `insert(k, w) == w x insert(k)`: counters, total, `min_count`
  - `bulk_insert == one at a time`
  - `clear` then a different stream `== fresh`
- merge
  - bounds hold on `xs ++ ys` after merging two capacities; `total()` adds
  - `merge(s, empty) == s`: counters, len, total, `min_count`
  - `merge(a, b) == merge(b, a)`; domain `capacity + 3`
  - after two narrow merges and a tail: `upper_bound(k) >= truth(k)` for every key
- wire
  - ASAPv1 at `edge_dimension` keeps `total()`, `min_count`, 64 `(estimate, error)`; after merging a 1-counter summary whose ceiling no counter holds

## topk_wrappers.rs — 15

Path laws compare two real implementations: the wrapper's matrix must equal the bare sketch's.

- matrix
  - `CMSHeap.cms()` cells == bare `CountMin`
  - `CSHeap.cs()` cells == bare Count sketch
- heap
  - `CMSHeap` resident: `truth(k) <= count <= estimate(k)`
  - `CSHeap` resident: `count == estimate at its last insert`
  - `len() <= top_k` always; `capacity() == top_k`
  - domain `<= top_k`: every key resident, heaviest included
- merge
  - merged keys are a subset of `a.keys + b.keys`
  - every merged resident: `count == merged estimate(k)`
  - room for the union: merged keys `== a.keys + b.keys`
- `CountL2HH`, via hash-taking entry points
  - `fast_insert_with_count_without_l2_and_hash` writes the cells `l2hh_cell_for_row` names, with the row sign
  - `fast_get_est_with_hash == median(sign-corrected cells)`
  - `get_l2_sqr() == median over rows of sum of squares`, every update
  - merge adds cells and recomputes L2 from them
- wire
  - `CMSHeap`, `CSHeap` at `edge_rows` x `edge_dimension`: 64 estimates and heap `(key, count)` kept

## univmon.rs — 10

Layer placement is a deterministic hash, so nesting is exact. Layer membership compared against: the trailing-ones closed form over `hash64_seeded(BOTTOM_LAYER_FINDER, key)`, not the pyramid's loop. Heaps as wide as the 48-key domain, so nothing evicts.

- layers
  - `layer[i].keys == {k arrived : deepest(k) >= i}`
  - `bottom_layer_for_hash(h, layers)` == set bits of `h` above bit 0, capped at depth
  - `layer[i].keys` contains `layer[i+1].keys`
- counters
  - `layer[i]` cells == signed mass of exactly the keys reaching `i`, placed by `l2hh_cell_for_row` under that layer's seed; the expected cells are the stream summed directly
- one key
  - `calc_l1() == weight` exactly; `calc_l2() == weight`, `calc_card() == 1`, `calc_entropy() == 0` to `REL_TOL`
- merge
  - cells add per layer; L1 adds
  - each layer's heap rebuilt from the union of both key sets, scored on the merged counters
  - `merge(S(xs), S(ys)) == S(xs ++ ys)`: cells, layer key sets, L1
  - `merge(a, b) == merge(b, a)`: counters and key sets per layer
- wire
  - ASAPv1 at small pyramids keeps per-layer estimates, heap entries, L1, candidate flags, geometry

## univmon_q.rs — 7

The u64 key encoding must be injective and order preserving over every f64. Compared against: `f64::total_cmp`. The encoder is private; laws reach it through `min` / `max`.

- encoding
  - `min(a, b)`, `max(a, b)` == what `total_cmp` puts first and last, bit-identical
  - `min(v) == max(v) == v`, bit-identical; `-0.0` and NaN payloads included
  - stream `min` / `max` == `total_cmp` extrema of the multiset
- ordered queries
  - `quantile(q)` non-decreasing in `q`, inside `[min, max]`; probes always include `q=0` and `q=1`
  - `quantiles(&qs)[i] == quantile(qs[i])`
  - `rank(x)` non-decreasing, in `0..=count`; both saturating branches hit
- wire
  - ASAPv1 keeps count, extrema, CDF breakpoints, F2; both counter widths, even and odd halving period, an overrun ordered sample

## count_min_hll.rs — 25 (experimental)

A Count-Min grid of HLL registers. Compared against: a standalone `HyperLogLogImpl<Classic, _>` on the same value hashes, byte-equal. Column routing read from a one-key probe sketch. 9 laws, 8 expanded over 3 shapes: 4 x 512 at p4 (rows mostly private), 4 x 32 at p6 and 3 x 32 at p8 (collisions).

- vs standalone
  - a bucket only `k` routes to == standalone registers, register for register
  - every row: `register >= what k alone wrote`
- set model
  - registers are a function of the distinct `(key, value)` pairs; repeats and order invisible
- merge
  - `merge(S(xs), S(ys)) == S(xs ++ ys)`
  - commutative, associative, idempotent, empty identity
  - different shape: refused with "different shape"; target byte-identical
- wire
  - ASAPv1 keeps storage, `(rows, cols, precision)`, per-key estimates

## kmv.rs — 12 (experimental)

Retained set == the `k` smallest distinct hashes, so laws are mostly equalities. Compared against: a `BTreeSet` truncated to `k`. Laws drive `insert_by_hash`; one covers the key path. Hash domains: below the smallest bound, straddling the largest, full u64.

- retention
  - `retained == k smallest distinct hashes`; `capacity == k`
  - reinserting a seen hash changes nothing
  - `S(xs) == S(perm(xs))`
  - the key path hashes once, at `CANONICAL_HASH_SEED`
- estimate
  - full sketch: `estimate == (k - 1) / U(k)`, finite, `> 0`; `U(k)` in `(0, 1]` for every u64; `estimate >= k - 1`; within 4 ulps of `(k - 1) * 2^64 / (kth + 1)`; a larger `kth` never raises it
  - `estimate` non-decreasing over the stream, every arrival
  - below the bound: `estimate == distinct count`
  - same `k - 1` smallest, smaller `kth`: strictly larger estimate
- merge
  - `merge == k smallest distinct of both`; capacity kept; operand unchanged
  - order and grouping of operands irrelevant
  - `merge(s, s) == s`
- wire
  - ASAPv1 keeps retained set, `k`, estimate; empty included

## microscope.rs — 15 (experimental)

Sliding-window frequency with adaptive zoom. A cell holds `t + 2` byte **pixels** (a ring indexed by sub-window) sharing one **zoom** exponent `Z`; a pixel is worth `c^Z` items and the cell rescales when a pixel would overflow. The **shutter** is the current sub-window's carry, `< c^Z`, rounded into its pixel at the boundary. A **reclaim** lowers `Z` at a boundary when load has fallen. Compared against: an exact `VecDeque` window tagged by sub-window.

Slack: collisions inflate, summed per sub-window; zoom rounding is `< 2 * c^Z` per pixel plus `< c^Z` for the shutter, over `t + 1` sub-windows, so `slack = 3 * c^Z * (t + 1)`. `Z == 0`: slack 0.

- generator
  - a hot key forces a zoom at both ends of every drawn axis; every law re-asserts a zoom on its own run
  - burst then spread load: `max_zoom()` back to 0; the exponent is not monotone
- zoom arithmetic
  - zoom-out then reclaim is exact: `Over == window count` before and after, `Under == the count one sub-window shorter`. Time clock, one key, `c = 2`, even counts
  - two seeds round a zoom differently; `c = 3`
  - shutter `< c^Z` after every arrival; `deserialize` refuses a payload breaching it
  - `255 * c^(Z-1) <= 2 * per_sub_window`
- estimate
  - `truth - slack <= estimate <= truth + lightest-row collisions + slack`
  - `Under <= Over`; `Linear(0.0) == Under`; `Linear(1.0) == Over`; `Linear(f)` non-decreasing, inside `[Under, Over]`; `estimate() == Linear(residual_fraction())`
- clocks
  - count clock: `sub_window == items seen / per_sub_window`; `residual_fraction` == complement of the position within it; keys irrelevant
  - time clock follows the high-water timestamp, not an out-of-order arrival
  - time clock at `timestamp == i` == count clock, byte-identical; stream past `t + 2` sub-windows, not ending on a boundary
- merge
  - refused, target byte-identical, each with its own reason: other sub-window length, time vs count clock, wider grid, earlier sub-window
  - merged `estimate >= each side's estimate - slack`
- wire
  - a sized-but-unfilled grid is refused, naming the row/col/depth mismatch; the same geometry filled decodes
  - a zoomed sketch round trips to the same `max_zoom()`, sub-window, answers

## uniform_sampling.rs — 16 (experimental)

No law names a priority. Values compared by `to_bits`, so `-0.0`, `0.0` and each NaN payload are distinct. Order independence is not a law: a priority comes from the rng position at arrival.

- counter
  - `total_seen() == arrivals`, every rate, every step
  - an unreadable input is not an arrival; `update_input == update`
- retention
  - `samples()` is a sub-multiset of the stream; `len() <= total_seen()`
  - `len() == ceil(total_seen * rate)` after every arrival
  - rate 1.0: `samples() == stream` as a multiset
  - `sample_at(i) == samples()[i]`; `None` past the end
- draws
  - same seed and stream: same samples, even with another sampler driven in between
  - rank replay: a rate-1.0 run lists entries in draw order, giving each arrival's rank; replaying with those ranks, dropping the worst-ranked when overfull, reproduces the lower-rate run, order included
  - two seeds on a 128-value stream keep different halves
- construction
  - a rate outside `(0, 1]` is refused by both constructors: `0.0`, `-0.0`, NaN, ±inf, `MAX`, `MIN`
- merge
  - `total_seen` adds; samples come from `a` or `b`; `len()` is the target for the combined stream
  - each side's head kept in its own order; sides ranked against each other and dropped from the far end; sides told apart by sign
  - `merge(a, b)` and `merge(b, a)` keep the same samples; distinct seeds, no ties
  - different rates: refused; samples and count unchanged
- wire
  - ASAPv1 keeps samples, `total_seen`, len, rate
  - rng state is carried: decoded and original, on the same continuation, keep identical samples
