# Property Test Coverage

The laws each sketch claims, checked with `proptest` against oracles derived
independently of the implementation. One file per sketch, one target:
`cargo test --test pbt` runs 367 laws; `cargo test --all-features --test pbt`
runs 435, adding the four modules marked **experimental** below. Every law was
mutation-checked — the implementation was broken deliberately and the law
confirmed red. Shared generators, the `grid` reader and the `round_trip!` macro
live in `tests/pbt/support.rs`.

`round_trip!` means: encode, decode, hold the decoded sketch to the original's
answers at the named probes, and re-encode to byte-identical bytes. A refused
encode passes — several payloads cannot represent every state their constructor
accepts — so a wire law is a law about silent corruption, not about coverage.
Counts below are laws as `cargo test` runs them; where a macro states one law
and expands it per shape, the section says so.

## bloom.rs — 11 laws

Oracle: the bit grid itself, read cell by cell through `as_bits`, and the
distinct key count of the generated stream. Both of Bloom's claims — a test may
lie upward and never downward, and a set bit stays set — are exact statements,
so a counterexample is a defect rather than an unlucky draw. The delivered
false-positive rate is a rate and is measured end to end, not here.

- no false negative, and membership only ever turns on
  - every key the stream carried reads back present, over 1-7 slices of 8-255 bits
  - a probe that reads present never reads absent again; every probe is re-read after every insert
- both hash paths, each through its own inherent `insert` and `contains`
  - `RegularPath` and `FastPath` lose no member; bits set are at most `slices * distinct keys` and at least `slices` for a non-empty stream; `fill_ratio()` is the bits set over `bit_capacity()`; `bit_capacity() <= BLOOM_MAX_BITS`; `inserted()` is the arrival count. Geometries: 1-4 by 1-8, where a few hundred keys saturate, and 1-7 by 256-1023, where the grid stays mostly zero
- what the bit grid depends on
  - the bit grid after a stream equals the grid after a permutation of it
  - `clear` restores the grid, the insert count, `is_empty()` and `fill_ratio()` of a filter fresh from `with_dimensions`, and no former member reads present
  - once every bit is set, every probe answers present, `fill_ratio()` is `1.0` and `estimated_fpp()` is exactly `1.0`
- merge
  - a union keeps every member of both sides present
  - merge is commutative and idempotent on the bit grid
  - a merged grid equals the grid of the concatenated streams, and `inserted()` is the sum of the two arrival counts
- sizing
  - `dimensions_for(expected, target)` chooses slices in `1..=BLOOM_MAX_SLICES`, a power-of-two width, and `slices * width <= BLOOM_MAX_BITS` — for expected counts up to `usize::MAX` and targets including `0.0`, `-0.0`, `1.0`, `-1.0`, `MIN_POSITIVE`, `EPSILON`, `MAX` and `MIN`
- wire
  - an ASAPv1 round trip preserves every bit, at `edge_dimension` widths and `bloom_rows` slice counts

## coco.rs — 12 laws

Oracle: a direct `(row, column)` scan of `table[i][j].full_key` and `.val`, plus
the exact sum of the generated stream — never `recorded_flows`, `estimate_key`
or the hash seeds. Regime: a key domain four times the table, so every bucket is
claimed and later arrivals contest an occupant.

- mass
  - the table's bucket values sum to exactly the weight the stream carried
  - an arrival for a key the table does not hold grows exactly one bucket, by its whole weight; that bucket is one of the `d` the key maps to, and held the smallest value among them
  - every distinct key occupies at most one bucket
  - `recorded_flows()` lists each occupied bucket once, in row-major order, and its values sum to the stream's mass
- queries, against that scan
  - `group_by(f)` is the occupied buckets folded onto `f(full_key)` and summed
  - `estimate_projected(p, f)` is the same fold read at one group, including a family no key belongs to
  - `estimate_substring(p)` sums every occupied bucket whose key contains `p`; probes include `item1`, which also occurs inside `item10`
  - `estimate_with_udf(p, pred)` sums the buckets its predicate accepts
  - `estimate_key(k)` is the value of the bucket holding `k`, zero when none does, and never above the stream total
- merge
  - a merged table holds the two streams' summed mass
  - a merge leaves every key of either side in at most one bucket
- wire
  - an ASAPv1 round trip preserves 64 keys' `estimate_key`, at `edge_dimension` widths and `edge_rows` depths

## count_min.rs — 19 laws

Oracle for the bound laws: an exact `HashMap` of the generated stream, grouped
into columns through the public `hash_for_matrix`. The path laws compare two
real implementations and rest on no model.

- path equivalence
  - the cell grid after a stream equals the grid after a permutation of it
  - `insert_many(k, n)` fills the cells `n` repeats of `insert(k)` fill
  - `bulk_insert_many_with_hashes` — a caller that hashed on its own thread — fills the cells `insert_many` fills
  - `DefaultMatrixI32` and `QuickMatrixI64` fill the same cells as `Vector2D` at the geometries the fixed matrices are cut for: the compile-time width folds a column the way the cached mask pair does
- the estimator
  - a non-negative arrival never lowers any key's estimate, probed over the whole 32-key domain after every insert
  - an estimate is at least the key's own mass and at most that plus the lightest row's collision mass — Cormode and Muthukrishnan's guarantee before the probabilistic step, so both ends are deterministic. Regime: 1-8 columns over 32 keys, where every row collides
  - the portable `CountMinSketch` never reports below a key's true total
- merge
  - commutative, associative, and the empty sketch is an identity
  - a merge equals streaming the concatenation
  - a merge never lowers either side's estimate, for keys of either stream and for two keys of neither
- `merge_max`
  - over disjoint key sets, the maxed cells sit between each operand and their sum, an empty peer cell moves nothing, and the estimate still covers the union stream's truth inside the lightest row's mass
  - maxing a sketch into a copy of itself changes no cell
  - over shared keys, every maxed cell and every maxed estimate sits at or above each operand's and at or below the summed sketch's
- the model held across a rebuilt backend
  - across a drawn sequence of inserts, merges, `apply_delta` calls, msgpack round trips and merges of a mismatched geometry, the sketch holds its model after every transition: the declared geometry, non-negative cells, every row carrying the model's whole mass, and each key's estimate between its truth and its lightest row's mass. `apply_delta` leaves the matrix the merge it stands for leaves; a mismatched merge is refused and changes nothing
- wire
  - a msgpack round trip preserves the geometry, every cell and every estimate
  - an ASAPv1 round trip preserves 64 keys' estimates at `edge_rows` by `edge_dimension`

## count_sketch.rs — 12 laws

Oracle: the column and sign a key takes in each row, read off a fresh sketch
carrying that key alone, plus the exact signed sum of the generated stream. So a
row's reading is predicted without restating the hash, and the laws pin the
estimator against the cells it claims to read. Regime: the sign and median laws
run at up to 5 rows over up to 7 columns against a 512-key domain, where every
row collides.

- path equivalence
  - the cell grid after a stream equals the grid after a permutation of it
  - `insert_many(k, n)` fills the cells `n` repeats fill
- the sign-and-column map
  - a key writes the same column with the same sign at every point of the stream, and touches no other column of its row — checked after each of 120 background arrivals
  - a stream of one key is estimated exactly, both for repeated unit inserts and for a sum of signed weights
  - the estimate sits inside the span of the rows' own sign-corrected readings, and at an odd row count is exactly the middle one
- negative weights, and the linearity they rest on
  - applying `-w` for every `w` returns every cell to where it stood, over a signed weight stream on top of a background
  - a weight of `-1` undoes a unit insert, cell for cell
  - the cells of two streams add to the cells of their concatenation (integral weights, power-of-two widths)
  - negating every weight negates every cell
- merge
  - a merge of the portable `CountSketch` equals streaming the concatenation, to `REL_TOL`
- wire
  - a msgpack round trip preserves every cell and every estimate
  - an ASAPv1 round trip preserves 64 keys' estimates at `edge_rows` by `edge_dimension`

## ddsketch.rs — 14 laws

Oracle: the sorted stream. `DDSketch::get_value_at_quantile` answers the
`ceil(q*n)`-th smallest, the portable `DdSketch::quantile` the
`floor(q*(n-1))`-th; each is compared against its own convention. Streams always
carry all three branches — negatives, exact zeros and positives — with
magnitudes inside the indexable band, rotated so the two dense stores grow in
varying order.

- where a sample lands
  - each sign reaches its own store: the negative store, the zero counter and the positive store hold exactly the counts of the three classes, and `get_count()` is the arrival count
  - the portable `DdSketch` holds the same three counts
  - `sum()` is the running total of the stream, bit for bit — it is carried on the wire, not estimated off the buckets
  - the first dense allocation is one 128-bucket chunk with the seeding value at its center, in both implementations and both signed stores
- relative error at most `alpha`
  - an answer is within `alpha` of the exact order statistic, across the sign boundary, for both implementations
  - answers are non-decreasing in `q` across the sign boundary
  - sharper than the bound: a lone value comes back unrounded at every `q`, and `min`, `max`, `q=0` and `q=1` read the tracked extremes exactly
- merge
  - count-additive and commutative: both stores, both offsets and the zero counter agree whichever side is merged into
  - a merged answer is within `alpha` of the combined stream's order statistic
  - two alphas are two index mappings: both implementations refuse the merge and leave the receiver's count standing, while a shared alpha merges
- wire
  - a msgpack round trip preserves both stores, both offsets, the zero count and every quantile
  - an ASAPv1 round trip preserves the count, `alpha`, `sum` and the quantiles, under a stream of NaN, the infinities, the signed zeros and the f64 extremes

## elastic.rs — 12 laws

Oracle: an exact `HashMap` over the stream, plus the paper's own accounting —
every arrival adds its weight to exactly one place and a takeover carries the
resident's whole positive vote into the light layer. Regime: a heavy table of
1-4 buckets against a domain of 4-47 flows over streams of 64 or more, so
residents are contested; `LAMBDA` negative votes per positive one must
accumulate before anything is evicted.

- the generator's reach into eviction
  - one bucket round-robined over nine flows evicts, and mass reaches the light layer — the guard that keeps the laws below off untouched buckets
- mass
  - the heavy part's positive votes plus any one light row equal the stream's arrival count
  - the same holds under weighted arrivals, against the summed weight
  - a merge conserves it: heavy plus any light row equals both streams' arrivals
  - a merge never lowers a light cell below the sum of what the two sides held there
- estimates
  - a resident whose bucket carries no eviction flag reads its exact count
  - no estimate falls below the exact count, before or after a merge
- bucket invariants, checked after every arrival
  - an insert never clears an eviction flag
  - an occupied bucket holds fewer than `LAMBDA` negative votes per positive vote
- heavy-part messages
  - `merge_heavy(id, a + b, flag)` leaves the same estimate, the same residency and flag, and moves the same total mass as `merge_heavy(id, a, flag)` then `merge_heavy(id, b, flag)`
- wire
  - an ASAPv1 round trip preserves 64 flows' `query`, at `edge_dimension` bucket counts and `edge_rows` by `edge_dimension` light grids

## ensemble.rs — 7 laws

Oracle: a bare sketch of the member's own type fed the same stream, so both
sides are real implementations. The layer hashes once from a layout cached at
construction; a bare fast-path sketch hashes from its own storage on every
insert, and the two must reach the same cells while one hash serves Count-Min's
packed columns, Count's columns and sign bit, and HyperLogLog's low 64 bits.
Member lists always carry both matrix families and all three HLL variants.

- a member holds what a bare sketch holds
  - every frequency member answers what a bare `CountMin` or `Count` of its geometry answers, over widths drawn from a pool the members take in turn — so one layer holds matrix members of several widths decoding one shared hash
  - in a layer with no matrix member, which hashes at `CANONICAL_HASH_SEED`, every HLL member answers what a bare HLL answers
- entry points
  - `hash_input` plus `bulk_insert_with_hashes` leaves every member where `insert` leaves it, and `estimate_with_hash` answers what `estimate` answers
  - `bulk_insert_at(chosen, values)` moves exactly the named members: each answers what a fully fed layer answers, the rest what an untouched one does
  - a member pushed mid-stream lands last and takes only the rest of the stream; a layer whose layout is already fixed by a matrix member does not move the members already in it
- the layer's own contract
  - `sketch_type()` names the member's family, `estimate` succeeds for exactly the frequency members, `cardinality` for exactly the others, and every accessor at an index past `len()` fails
  - a matrix member of another row count is refused both by `Layer::new` and by `push`, and a refused push leaves every member's answers unchanged

## exponential_histogram.rs — 16 laws

Oracle: the raw arrival stream kept beside the structure, from which the exact
window content and the run behind every bucket are recomputed. One Count-Min row
totals to the number of inserts that reached it, so a bucket's arrival count is
readable without trusting its `size` field.

- bucket invariants, after every arrival
  - at most `k/2 + 2` buckets share a size — the L1 rule that makes a window of `n` arrivals cost `O(k log n)` buckets
  - every bucket's size is a power of two, and sizes do not increase from oldest to newest
- the window
  - the retained arrivals are a superset of the window and exceed it by at most the oldest bucket's size — expiry is bucket-granular, so only the bucket straddling the cutoff carries anything older
  - the buckets tile the retained suffix: each bucket's arrival count equals its declared `size`, its `(min_time, max_time)` are the first and last timestamps of its run, the runs abut, and merging them all equals a Count-Min over exactly that suffix of the stream
  - a `query_interval_merge` whose ends are bucket boundaries returns exactly the arrivals of those buckets, cell for cell. Regime: streams with no two arrivals at one timestamp, since a shared timestamp leaves an endpoint ambiguous
- tumbling windows, against the retained set derived from the stream, the window width and `max_windows`
  - `closed_count()` is `min(elapsed periods, max_windows)`: every period between the first and last arrival is opened and closed, empty ones included
  - `TumblingWindow<FoldCMS>` and `TumblingWindow<FoldCS>`: merging the retained windows reproduces the flat counters of a single sketch fed the retained arrivals
  - `TumblingWindow<KLL>` at `k = 512`, below the first compaction: the merged buffer is exactly the retained observations, and the merge stays at one level
  - `TumblingWindow<KLL>` at `k = 32`, past compaction: the merged weight is at least the retained count, every retained item is one that arrived, and every quantile lies inside the retained range
  - `TumblingWindow<UnivMonQ>`: the merged count is exactly the retained count, and the merged extremes are the retained stream's own
- pool recycling — a recycled sketch must match a fresh one both when handed back and after both are fed the same second half of a stream, since whatever `tumbling_clear` forgets is visible only through later answers
  - `SketchPool<FoldCMS>` and `SketchPool<FoldCS>`: counters, entry count and heap length
  - `SketchPool<KLL>`: count, items, levels and the compaction coin
  - `SketchPool<UnivMonQ>`: count, extremes, L1, F2 and the CDF
  - `UnivSketchPool`: L1, L2, cardinality and the candidate flags

## fold.rs — 21 laws

The family's claim is an exact equality: folding adds no approximation error.
Point-query oracle: the sketch definition over an exact `HashMap` of the stream,
with a key's column and sign read off a level-0 sketch carrying that key alone
through `cell` and `iter` — none of the folding paths under test. Regime: a
48-key domain over grids of 8 to 64 full columns folded 0 to 6 levels, so
distinct full columns share a physical cell and `FoldCell::Collided` is reached.

- stated once and expanded over `FoldCMS` and `FoldCS` (12 laws)
  - a folded sketch answers what the full-width sketch over the same stream answers, at every level the width admits, and reports `full_cols >> level`
  - `unfold_to(target)` preserves every query for every target at or below the current level
  - a row holds at most one entry per full column, whatever the fold level — an entry is an address, so no two can be added together by mistake
  - `unfold_to(high).unfold_to(low)` reaches the same cells as `unfold_to(low)`, and `unfold_full()` the same as `unfold_to(0)`
  - `unfold_merge(a, b)` is `merge_same_level` followed by one level of widening, cell for cell
  - `hierarchical_merge` over `2^level` sub-windows reaches the same level-0 cells as a balanced tree of `unfold_merge` calls, and answers what one full-width sketch over the concatenated sub-windows answers
- Count-Min
  - `to_flat_counters()` is the column sums of the stream, at every fold level
  - a query is the smallest counter the key addresses
  - a query never falls below the true count, the stream being non-negative
- Count sketch
  - `to_flat_counters()` is the signed column sums
  - a query lies within the range of the rows' sign-corrected readings
  - at 3 or 5 rows, a query is exactly the middle row estimate
  - a key keeps its columns and its signs at every fold level and after any unfold
- `FoldCell`
  - the representation is a function of the distinct columns held: `Empty` at none, `Single` at one — a repeat of the column already there accumulates in place — and `Collided` at two or more; `entry_count()` is the distinct column count, `query` the running sum, and zero for a column never inserted
  - merging cells sums them column by column, and reaches the same cell either way round

## heaps.rs — 12 laws

Oracle: the offered values sorted, truncated to the end the ordering asks for.
`CommonHeap` answers to that model outright; `HHHeap` is path dependent once it
overflows, so its exact model is asserted only in the regimes where the path
cannot matter — a key domain no wider than the capacity, or one offer per key.

- `CommonHeap`, against the sorted offer list and against its own array
  - the heap never holds more than its capacity, holds `min(pushed, capacity)`, and `is_full()` agrees with that
  - a min-heap retains the largest `capacity` values of what was offered, as a multiset; a max-heap the smallest
  - `peek()` is the extreme of what is retained, in the direction the ordering asks for
  - the backing array satisfies the heap property at every parent-child pair, in both orderings
  - popping drains in non-decreasing order, yields `min(pushed, capacity)` values and empties the heap
  - `clear` leaves length zero, `peek()` `None` and the capacity standing
- `HHHeap`, whose retained set is path dependent once it overflows, so the exact model is asserted only where the path cannot matter
  - the heap never exceeds `k` at any point in the stream
  - a resident carries the last count offered for its key
  - with a key domain no wider than `k`, nothing is turned away: the heap holds every distinct key, each at its last count
  - given one offer per key, the heap holds the `k` largest counts
  - `update` returns whether the key was already resident or the heap had room — the documented meaning, that the key took its place without displacing another

## hll.rs — 101 laws

Models are derived from the published definition. Where the paper leaves a
choice free — which bits index a register, how a bucket maps to a leading-zero
count — the model states only what the paper fixes. For `Classic` and `ErtlMLE`,
`estimate` is a deterministic function of the registers, so its laws are
relations between estimates rather than a restatement of the formula. Register
values stay at or under 40, so `sum(2^-M[j])` is exact in f64 at every precision
here.

- insertion, stated once and expanded over precisions 4, 6, 8 and 10 (16 laws)
  - the registers are a function of the distinct set: neither repeat count nor interleaving can be read back out
  - `insert_many_with_hashes` over `canonical_hash` leaves the registers `insert` leaves
  - no register ever decreases, checked over the whole array after every arrival
  - one arrival moves at most one register
- estimation, expanded over `Classic` and `ErtlMLE` at those four precisions (40 laws). Registers are crafted through the public `insert_with_hash` and asserted to equal the target, so the hash split is pinned in place
  - an empty sketch estimates zero — every register zero puts the estimator in linear counting, where `m * ln(m/m)` is zero
  - the estimate ignores register order: the formula reads the multiset, never a position
  - raising every register by one halves `sum(2^-M[j])`, so the estimate doubles, within the estimator's own rounding — drift in `0..=1` for `Classic`, which truncates, and `-1..=1` for `ErtlMLE`, which rounds
  - the estimate ignores arrival order: order reaches the registers only through `max`
  - the estimate never falls when a register rises
- merge, expanded over the four precisions (20 laws)
  - a merge is the elementwise maximum — the one combination that cannot double count
  - commutative, idempotent, and the empty sketch is an identity
  - a merge equals streaming the concatenation
- HIP, expanded over the four precisions (20 laws). Registers are read back through the wire format, since the struct holds them privately behind the `kxq0`/`kxq1` accumulators the increment law checks
  - an empty sketch estimates zero
  - the estimate never decreases, and a repeated key does not move it
  - the estimate is a function of arrival order, not of the registers alone: a descending run into one register estimates exactly 1, an ascending run strictly more
  - every arrival that raises a register adds `m / sum(2^-M[j])` over the register state that preceded it, and an arrival that raises nothing adds nothing (Lang, arXiv:1708.06839). Both sides are exact in f64, so the truncation to `usize` is compared rather than tolerated
- the portable `HllSketch` (5 laws)
  - register merge is commutative and idempotent, the empty sketch is an identity, and a merge equals streaming the concatenation, at precisions 4 to 11
  - a msgpack round trip preserves the variant, the precision, the registers and the estimate, over `Regular`, `Datafusion` and `Hip` at precisions 4 to 13
  - an ASAPv1 round trip of `HyperLogLog<ErtlMLE>` preserves the registers and the estimate, the empty sketch included

## hydra.rs — 7 laws

Oracle: the map rebuilt from the documented subkey encoding (`label ":" value`
joined by `";"`, separators escaped) and the public matrix hash at `HYDRA_SEED`,
against a grouping of the generated stream. Every record carries the same
payload value, so each cell's Count-Min holds a single key and its estimate is
the exact mass that cell received — which turns the grid laws into equalities.
Labels and values carry the characters the encoding escapes. Column counts
straddle the collision regime: 1, 2, 3, 8, 64, 251 and 256.

- the write side
  - a record fans out into the `2^D - 1` non-empty subsets of its equalities, each written into the cell its labeled subkey names on each row, and nothing else reaches the grid
  - an update's `count` is the weight it adds, not one write per update
- the read side
  - an answer is a median of the cells its own subkey names, asserted as an order statistic — at least half the rows at or above and at least half at or below. Taking the minimum or maximum of the rows satisfies every other bound a subpopulation answer obeys, so nothing else catches it. Probes cover every subset of every record's equalities, every single-column equality over the shared domain, and one value no record carries
- merge
  - a merge equals streaming the concatenation, cell by cell rather than query by query, since a median can outvote an unmerged row
- `HydraKllSketch`, a second grid with no schema and no fan-out
  - one key reaches one cell per row, at `xxh32(key, row) % cols`, read back as each cell's retained count
  - a query is a median of its row cells, again as an order statistic; an absent key reads 0 from an empty cell, below every value the stream carries
- wire
  - an ASAPv1 round trip preserves every subpopulation answer and the schema

## kll.rs — 14 laws

Compaction is randomized, so no law names the items it keeps. The accuracy
oracle is the sorted stream; the rank band is `C * n / k` with `C = 16`, set well
above the worst drift this suite reaches (about 3.3) because the guarantee is
probabilistic and the suite draws thousands of streams. The accuracy law draws
`k` from 200 and 400 and asserts the budget is under `n/10`, since at `k = 8` it
would be twice the stream and bound nothing.

- count
  - the count is exact below the compaction threshold
  - a merged count is within `max(10% of n, 4)` of the two streams' lengths
- quantiles
  - a quantile is non-decreasing in `q`
  - `quantile(0.0)` and `quantile(1.0)` lie inside the stream's observed range
  - every quantile is a value the stream carried, to the bit — compaction only ever copies items. Regime: a narrow domain, so most values arrive many times
  - both ends are exactly the stream's own until the first compaction, at `n <= k`; `n == k` is drawn on its own so the boundary is always covered. Past it the extremes are ordinary items, there being no min/max register
- rank and CDF
  - `rank(quantile(q))` exceeds `q * n` by at most one item's weight, the top level's `2^(num_levels - 1)`, and falls below it by at most a rounding epsilon — `q * count()` lands a few ulps above the exact product, so a rank sitting on it reads a hair under
  - `rank` never falls as the probe rises, is 0 below the stream and the full count at the maximum
  - `cdf(x) * count` agrees with `rank(x)` to `REL_TOL` at every probe, and the CDF is monotone within `[0, 1]`, 0 below the stream and 1 at the maximum. The two readings share no code: `rank` sums level weights in place, `cdf` flattens every item into one sorted table and takes prefix sums
- accuracy
  - the true rank of `quantile(q)` in the sorted stream is within `16 * n / k` of `q * n`. The claim is on rank, not on the value: `quantile(q)` may sit far from the true `q`-th value wherever the distribution is flat
- determinism
  - a seeded sketch replays item for item — the compaction coin is the only nondeterminism
  - `clear` re-seeds the coin from the stored seed, so a cleared sketch replays the same stream to the same items and levels a fresh one does
- wire
  - a msgpack round trip preserves `k`, the count and the quantiles
  - an ASAPv1 round trip preserves the count and the quantiles under NaN, the infinities, the signed zeros and the f64 extremes

## kll_dynamic.rs — 14 laws

The same guarantees against the dynamic item path. Oracle: the sorted stream.
Streams run thirty times `k` and longer, so level 0 spills on the order of `n/k`
times; a narrow domain is mixed into a wide one so equal values are common. The
rank band is `8 * n / k` — an order of magnitude above what halving a sorted
level leaves and an order of magnitude below what halving an unsorted one would.

- quantiles
  - a quantile never falls as `q` rises, in `total_cmp` order, so NaN and the signed zeros are ordered too — the order `f64`'s own `<` cannot state
  - every quantile is a value the stream carried, to the bit
  - the answer is exact until the first compaction: the count, both ends and every answer sitting at the exact rank `q` asks for, with `n == k` drawn on its own
- rank and CDF
  - `rank(quantile(q))` is at least `q * n`, and the rank of the stream's own predecessor of that answer is at most `q * n` — a two-sided bracket that stays exact under ties
  - `rank` is 0 below the stream, the full count at `quantile(1.0)`, at least 1 at `quantile(0.0)`, and never falls in between
  - the CDF is within `[0, 1]`, monotone, 0 below the stream and 1 above it
  - `cdf(x) * count` agrees with `rank(x)` to `REL_TOL`, over streams with no repeated value so the CDF's binary search has one index per value
  - `rank` counts in `f64`'s own `<=` while the CDF sorts by `total_cmp`, and the two orders disagree on NaN and on the signed zeros: `rank(NaN)` is 0, a retained NaN is counted by no probe, and `-0.0` and `0.0` are one value to `rank`
- accuracy
  - the true rank of `quantile(q)` is within `8 * n / k` of `q * n`. This is the only law that reads the difference between halving a sorted level and an unsorted one; every shape law above survives either
- merge
  - merging a source into a fresh sketch of the same `k` reproduces the source: equal count and equal rank at every probe, so equal weight at every level. Nothing of the target's interleaves and no level exceeds the capacity `k` sets, so the cascade compacts nothing
- determinism
  - the same seed and stream produce byte-identical sketches
  - `clear` returns the sketch to an empty one byte for byte, and to a fresh one over a replay of the same stream
- wire
  - an ASAPv1 round trip preserves the count and the quantiles under extreme values
  - the payload carries the compaction coin's live state: a decoded sketch and the original, fed the same 200-400 further values, re-encode to the same bytes

## nitro.rs — 10 laws

Oracles: a bare target sketch driven through its own `insert` — a different code
path from `NitroTarget::update_sample` — and integer arithmetic done in the test.
A single insert below rate 1.0 has no deterministic oracle, so nothing asserts
one; the laws below rate 1.0 are the ones sampling does not disturb.

- transparency at rate 1.0
  - a `CountMin` target holds the cells a bare `CountMin` holds, through both `insert` and `insert_cached_step`
  - the same for a `Count` target
  - a merge at rate 1.0 equals streaming the concatenation into a bare target
- the seed's hold on the admitted subset
  - two samplers on one seed and stream fill the same cells, on both paths, at rates from 1.0 down to 0.01
  - at a dyadic rate — one of the form `1/2^s` — the exact weight is an integer, so `admitted_weight` must not touch the generator: 1 to 15 weight probes leave the admitted subset where a run without them leaves it
- the compensation as a function of the rate
  - an admitted weight is `scaled_increment(w)` or one above it, over 16 draws, and the factor is unchanged after those draws
  - at rate `1/2^s` the compensation is exactly `w << s`, on every draw
  - over 20,000 draws the mean admitted weight is within 0.05 of the unrounded `w / rate`, at rates whose reciprocal is not an integer. Stochastic rounding is what makes `E[W] = w/p` hold; rounding the same way every time would bias the estimator
- the thinning below rate 1.0
  - at rate `1/2^s` every cell holds a multiple of `2^s`, every row holds the same mass, and fewer arrivals were admitted than arrived. Regime: streams of 256-511 values, so a rate at or below 1/2 drops part of one past any probability worth naming
- merge
  - a merge adds the two targets cell by cell and leaves the operand alone

## octo_delta.rs — 25 laws

A worker holds its counts back until one reaches the threshold `tau`; it then
promotes that step of `tau` to the parent and keeps whatever is left over as its
residual. So the oracle is a single-threaded sketch over the same stream, and
what must reconstruct it is the parent's state plus every worker's residual.

The reconstruction is stated per delta type, because the types do not merge
alike — addition for Count-Min, Count, DDSketch and the UnivMon layers, maximum
for HyperLogLog, and replay through the parent's own insertion logic for Coco
and Elastic, which may elect a different victim.

- Count-Min and Count sketch, applied by addition
  - promoted plus residual reconstructs the reference cell for cell, and no residual reaches the threshold — through `insert_emit_delta_with_threshold` on the sketch itself, and again through `CmWorkerSketch` / `CountWorkerSketch` against the byte-wide worker residual
  - a flush over 1-4 shards leaves the parent exactly the single-pass sketch, with every worker counter at zero
- DDSketch, a sparse bucket store applied by addition
  - promoted plus residual reconstructs every bucket, the parent holds no bucket the stream never filled, promoted count plus held-back equals the reference count, and no residual reaches the threshold
  - a flush over 1-4 shards leaves the parent's buckets and count exactly the reference's
- HyperLogLog, registers applied by maximum and never cleared. Sixteen registers, so a register is improved many times over one stream and two shards land on the same one
  - the parent never holds a register above the stream's, and the gain it is behind by is under `2^tau`
  - a flush over 1-4 shards leaves the parent's registers exactly the single-pass sketch's
  - at a threshold of 0 nothing is held back: the parent is the shards' register-wise maximum, and equals the single-pass sketch, without any flush
  - applying a delta stream is order independent and idempotent
- CocoSketch, a keyed bucket replayed through the parent's election
  - promoted mass plus residual equals the arrival count; every promotion carries exactly `tau` and names a key that arrived; no residual reaches the threshold
  - after a flush the parent table holds the whole stream's mass, and no worker holds a bucket back
  - the victim is the least loaded of the buckets a key maps to: one bucket per array puts every key on the same `d`, and a stream of distinct keys below the threshold leaves them within one of each other
  - at a threshold of 1 every bucket is empty on arrival, so the arrival always wins the election and the promotion names it rather than the previous occupant
- Elastic sketch, a heavy half with replacement and a light half by addition
  - heavy promotions plus evicted votes plus the light total divided by the row count equal the arrival count; a heavy promotion carries `tau` and an eviction carries fewer than `tau`; every promoted or evicted key arrived
  - after a flush over 1-4 shards the parent's heavy votes plus its light mass per row equal the arrival count
  - a resident is handed over on the eighth rival arrival — `LAMBDA` negative votes against a single positive one — carrying exactly its one vote
- UnivMon, one Count layer per level
  - promoted plus residual reconstructs the layer's cells, a flush closes the gap exactly, and a parent fed only deltas reports exactly the reference's L2
  - a delta carries its own layer's threshold, `max(tau >> layer, 1)` — stated here rather than read back from the crate — its worker id, the worker's running weight at that insert, and the key that arrived
- keyed deltas, beside the aggregator's heap
  - for `CmTopKOctoPlan` and for `CountWorkerSketch`'s keyed path, promoted plus residual reconstructs the cells; the heap seats `min(promoted keys, top_k)` entries, holds only keys that arrived, and (Count-Min) no count above the parent's own estimate
- the default thresholds
  - each of `CM_PROMASK`, `COUNT_PROMASK`, `DD_PROMASK`, `COCO_PROMASK`, `ELASTIC_PROMASK` and `UNIVMON_PROMASK` is inside `1..=MAX_PROMASK`, fires over 600-800 inserts of one key, and is the step every promotion of its type carries
  - `max_hll_threshold(p)` is `64 - p`, `HLL_PROMASK` sits below that ceiling at the default precision, and a worker running at it holds no register improvement back
  - `MAX_PROMASK` is `i8::MAX`; `OctoThreshold` clamps 0 up to 1 and anything above down to it; every promotion at it carries a value an `i8` holds

## set_aggregator.rs — 9 laws

Exact structure, so every law is an equality. Oracle: a `std::collections::HashSet`
built from the stream. Keys are drawn from `[a-c]{0,2}`, so a stream repeats
itself and two streams intersect by construction.

- exactness
  - the key set is exactly the distinct keys of the stream, and its length the distinct count
  - membership answers yes for the inserted keys and no for the rest
  - stream order does not reach the key set
  - replaying a stream leaves the key set unchanged
- merge is set union
  - a merge is the union of the two key sets and leaves the operand alone
  - commutative, associative and idempotent
  - the empty aggregator is a two-sided identity
  - `merge_refs` unions every input and refuses an empty list
- wire
  - a msgpack round trip preserves the key set, and so does a second round trip of the decoded one. Keys include the empty string, non-ASCII, an embedded NUL, a 300-character key and arbitrary `String`

## space_saving.rs — 19 laws

Oracle: an exact `HashMap` count of the generated stream. Every bound here is
deterministic, so a counterexample is a defect rather than an unlucky draw.

- bounds against the exact counts
  - a monitored key's `error` is at most its `count`, its `count` is at least the truth, and `count - error` is at most the truth
  - `upper_bound` never falls below the truth, for every key the stream carried
  - a key the stream never carried estimates 0, and its `upper_bound` and `error` are both `min_count`, with `is_guaranteed` false
- structural invariants
  - the summary never exceeds its capacity and holds `min(distinct keys, capacity)` counters
  - every counter's `error` is at most `min_count`
  - the counters sum to `total()`, which is the arrival count
  - a full summary's `min_count` is at or above its smallest counter, and `min_count * capacity` is at most `total()` — the ceiling pinned from both sides
  - `top_k(k)` returns `min(k, len)` entries in non-increasing count order, each matching the summary's own table, and leaves no counter above its cut out
  - a guaranteed key strictly outranks, in true frequency, every key the summary dropped
  - `min_count` never falls as the stream advances
- the regime with a counter to spare for every key
  - with a domain no wider than the capacity the summary is exact — `count` the truth, `error` 0, `upper_bound` the truth — and order-free
  - a weighted arrival matches that many unit arrivals, in counters, total and `min_count`
  - `bulk_insert` matches the same values one at a time
  - `clear` starts the summary over as a fresh one, over a replay of a different stream
- merge
  - the bounds hold over the concatenated stream after a merge of two capacities, with `total()` the combined arrival count
  - merging an empty summary leaves every counter, the length, the total and `min_count` untouched
  - merging in either order reaches the same summary. Regime: a domain three wider than the capacity, so both sides monitor the same keys and their union fits
  - after two merges of narrow summaries and a further tail, `upper_bound` still covers every key of the whole stream — the keys a merge drops leave no counter, so all the summary can say about them is its ceiling
- wire
  - an ASAPv1 round trip at `edge_dimension` capacities preserves `total()`, `min_count` and 64 keys' `(estimate, error)`, after a merge of a one-counter summary that leaves behind a ceiling no counter holds

## topk_wrappers.rs — 15 laws

Both sides of the path laws are real implementations: a wrapper must leave its
inner matrix exactly as the bare sketch would, so a bug in the heap bookkeeping
cannot reach the counters.

- the matrix underneath
  - `CMSHeap` leaves the cell grid a bare `CountMin` builds
  - `CSHeap` leaves the cell grid a bare Count sketch builds
- the heap beside it
  - a `CMSHeap` resident's count sits between the key's true frequency and the matrix's current estimate — a Count-Min counter only grows, so the count is the estimate its own last occurrence read
  - a `CSHeap` resident carries exactly the estimate its last insert read; a Count sketch estimate is a signed median and moves either way, so nothing weaker holds
  - neither heap exceeds `top_k` at any point, and `capacity()` is `top_k`
  - with a key domain no wider than `top_k`, both heaps hold every distinct key, the heaviest included
- merge
  - a merged heap holds only keys one of the two heaps held
  - every merged resident carries the merged matrix's estimate — a merge re-queries every candidate, so no count survives from before the counters were added
  - where the capacity has room for the union, a merge keeps exactly the union of the two heaps' keys
- `CountL2HH`, driven through its hash-taking entry points so the cell laws hold whatever the hasher does
  - `fast_insert_with_count_without_l2_and_hash` writes the cells `l2hh_cell_for_row` names, with that row's sign — the map an OctoSketch worker addresses cells with, which must name the cell the sketch's own insert does
  - `fast_get_est_with_hash` is the median of those cells put back through their signs
  - `get_l2_sqr()` is the median of the rows' sums of squares, after every update
  - a merge adds the cells and re-derives the L2 from the merged rows
- wire
  - `CMSHeap` and `CSHeap` round trip at `edge_rows` by `edge_dimension`, preserving 64 keys' estimates and the heap's `(key, count)` contents

## univmon.rs — 10 laws

The layer a key lands in comes from a deterministic hash, so the nesting law is
an exact equality. Oracle for layer membership: the trailing-ones closed form
over `hash64_seeded(BOTTOM_LAYER_FINDER, key)`, which owes nothing to the
pyramid's `bottom_layer_for_hash` loop. Every pyramid is given a heap as wide as
the 48-key domain, so no layer can evict and the set equalities are statements
about the sampling hierarchy rather than about eviction.

- which layers an insert reaches
  - layer `i` holds exactly the arrived keys whose deepest layer is at least `i`
  - `bottom_layer_for_hash(h, layers)` is the count of set bits above bit 0 of the finder hash, capped by the depth
  - each layer's key set contains the next layer's
- the counters
  - layer `i`'s grid holds the signed mass of exactly the keys reaching `i`, placed by `l2hh_cell_for_row` under that layer's own seed — the oracle sums the generated stream into the cells directly, pinning routing, weight and sign at once
- one key
  - a stream over a single key reports its weight as L1 exactly, and reports that weight as L2, a cardinality of 1 and an entropy of 0 to `REL_TOL`: there are no collisions to estimate through and the alternating correction above the key's own layer cancels the doubling
- merge
  - a merge adds each layer's counters, and L1 is additive
  - a merge rebuilds each layer's heap from the union of both sides' keys, each entry scored against the merged counters rather than carried over
  - merging lands where streaming the concatenation does, in cells, layer key sets and L1
  - a merge is commutative on every layer's counters and key sets
- wire
  - an ASAPv1 round trip at small pyramids preserves the per-layer estimates, the heap entries, L1, the candidate flags and the geometry

## univmon_q.rs — 7 laws

The order-preserving u64 encoding under the quantile path must be injective and
order preserving over every f64. Oracle: IEEE 754-2008 `totalOrder`, read out of
the standard library as `f64::total_cmp` — a total order over bit patterns, so
NaN participates and `-0.0` sits below `0.0`. The encoder is private, so the laws
reach it through `min` and `max`, the two answers built by comparing encoded keys
and decoding the winner.

- the encoding
  - the extrema of a pair are the ones `total_cmp` puts first and last, bit-identical. A non-injective encoding collapses the pair and fails one of the two
  - a lone value returns bit-identical from both extrema — the sign of a zero and a NaN payload included
  - the extrema of a whole stream are the `totalOrder` extrema of its multiset
- ordered queries, which are relations rather than values
  - `quantile` never falls as `q` rises and stays inside the exact extrema. The probe list always carries both endpoints, since 0 and 1 read the extrema and everything between reads the CDF
  - `quantiles(&qs)` agrees with `quantile(q)` one at a time at every breakpoint — the CDF is reconstructed once for the batch and once per single call
  - `rank` never falls as its argument rises and stays within `0..=count`, including the two saturating branches outside the extrema
- wire
  - an ASAPv1 round trip preserves the count, the extrema, the CDF breakpoints and F2, over both counter widths, an even and an odd halving period, and an ordered sample the stream overruns

## count_min_hll.rs — 25 laws (experimental)

A Count-Min-shaped grid whose buckets hold HyperLogLog registers. Oracle: a
standalone `HyperLogLogImpl<Classic, _>` of the same precision fed the same value
hashes through its own register and rank code — two separate derivations of the
same published construction, held to byte equality. Column routing is never
recomputed: it is read back from a probe sketch that has seen one key, so the
laws constrain the grid without restating its hashing.

Nine laws, eight of them stated once and expanded over three shapes: 4 by 512 at
precision 4, where a key usually has every row to itself, and 4 by 32 at
precision 6 and 3 by 32 at precision 8, where collisions are the rule.

- against the standalone sketch
  - a bucket no other key routes to holds the standalone HyperLogLog's registers, register for register — not merely to within the estimator's band
  - in every row, no register falls below what the key wrote on its own, a bucket carrying the union of the distinct sets routed to it
- the set model
  - the registers are a function of the distinct `(key, value)` pairs: neither repeat count nor arrival order can be read back out of the grid
- merge
  - a merge equals streaming the concatenation, register-wise maximum being the one combination that cannot count twice what both sides held
  - commutative, associative, idempotent, and the empty grid is an identity
  - a merge across shapes is refused with a "different shape" error and leaves the target byte-identical — registers line up only when both grids route a pair to the same bucket and the same register
- wire
  - an ASAPv1 round trip preserves the whole storage, `(rows, cols, precision)` and the per-key estimates

## kmv.rs — 12 laws (experimental)

The retained set is exactly the `k` smallest distinct hashes, so most laws are
equalities. Oracle: that definition read literally — a `BTreeSet` over the
stream, truncated to `k`. Most laws drive `insert_by_hash`, so no hash function
stands between the oracle and the sketch; one law covers the key path. Hash
streams straddle the bound: a domain under the smallest bound, one straddling
the largest, and the full 64-bit range.

- retention
  - the retained hashes are the `k` smallest distinct hashes of the stream, and the capacity is `k`
  - reinserting anything the stream already carried changes nothing
  - the retained set does not depend on arrival order
  - the key path hashes each key once, at `CANONICAL_HASH_SEED` — what the wire format declares
- estimation
  - a full sketch reports `(k - 1) / U(k)`: the estimate is finite and positive, the implied `U(k)` lies in `(0, 1]` for every hash the 64-bit range holds, the estimate is at least `k - 1`, it agrees within four ulps with `(k - 1) * 2^64 / (kth + 1)` associated the other way round, and raising the k-th minimum cannot raise it
  - the estimate never falls as the stream runs, checked after every arrival
  - below the bound the estimate is exactly the distinct count
  - of two full sketches agreeing on their `k - 1` smallest, the one holding the smaller k-th hash estimates strictly more
- merge
  - a merge retains the `k` smallest distinct hashes of both streams, keeps the capacity and leaves the operand it read unchanged
  - a merge does not depend on the order or the grouping of its operands
  - merging a sketch with a copy of itself changes nothing
- wire
  - an ASAPv1 round trip preserves the retained set, `k` and the estimate, the empty sketch included

## microscope.rs — 15 laws (experimental)

Sliding-window frequency estimation with adaptive zoom. A cell holds `t + 2`
byte-wide **pixels** — a ring indexed by sub-window number — and one **zoom**
exponent `Z` they share, so a pixel is worth `c^Z` items and the cell rescales
every pixel when one is about to overflow. The **shutter** is the current
sub-window's carry, below one `c^Z` unit, which a sub-window boundary rounds
into that sub-window's pixel. A boundary that finds the load has fallen
**reclaims** the resolution, lowering `Z` and multiplying the pixels back up.

Oracle: an exact sliding window — a `VecDeque` of raw arrivals tagged with the
sub-window they were recorded in — that knows nothing of pixels, shutters or
exponents.

An answer may differ from that window two ways, and they are bounded separately.
Count-Min collisions only inflate it, and are summed per sub-window because that
is where the minimum across rows is taken. Zoom rounding moves it either way: a
cell at exponent `Z` divided its pixels at most `Z` times, each division
rounding by under the unit it produced, so one pixel's compounded error is under
`c + ... + c^Z <= 2 * c^Z`, and the shutter a boundary closes out adds under one
further unit, rounded up to `c^Z` to keep the constant whole. An estimate reads
`t + 1` sub-windows, which makes the slack `3 * c^Z * (t + 1)`. At `Z = 0`
nothing has been divided and no shutter has held a partial unit, so the
arithmetic is exact and the slack is zero.

- the generator
  - a hot key saturating a pixel forces a zoom, pinned at both ends of every drawn axis. A load that never zooms leaves the laws holding over a plain windowed Count-Min. That is the one shape they are not for, so every property below re-asserts a zoom on the run it drew
  - a burst that zooms, followed by a spread load, leaves `max_zoom()` back at zero — the witness for why the exponent is not asserted monotone
- the zoom arithmetic
  - a zoom-out rescales what the cell already holds and the reclaim scales it back, neither losing nor inventing counts. This is an equality, not a bound: `Over` reads the exact window count before the zoom and after the reclaim, and `Under` the span one sub-window shorter. Everything is arranged to keep it one — a time clock sets the load per sub-window, one key owns the grid so nothing collides, and `c = 2` over even counts divides exactly. The properties below bound the answer by a slack that grows with the exponent, so a zoom that divided the wrong pixels, or a reclaim that dropped `Z` without multiplying, would move the answer by a factor of `c` and be absorbed
  - two seeds round a zoom differently, so the constructor's seed reaches the rounding (`c = 3`, which makes the divisions inexact)
  - a live cell never holds a whole `c^Z` unit in its shutter, after every arrival — the assumption bounding the carry a merge computes, and the one `deserialize` refuses a payload for breaching
  - the exponent stays within what one sub-window can fill: `255 * c^(Z-1)` is at most twice `per_sub_window`. This is also what keeps the band law from being self-defeating, its slack being read off the run's own peak
- the estimate
  - a zoomed estimate sits within the rounding slack of the collision band: at or above the true window count less the slack, and at or below what the lightest row's collisions allow plus it
  - `Under` is at or below `Over`, `Linear(0.0)` is `Under`, `Linear(1.0)` is `Over`, `Linear(f)` is non-decreasing in `f` and inside `[Under, Over]`, and `estimate()` is `Linear(residual_fraction())`
- the clocks
  - a count-based clock's sub-window is `items seen / per_sub_window` and its residual fraction the complement of the position within it, both moving with the item count and not with the keys
  - a time-based clock's sub-window follows the high-water timestamp, never an out-of-order arrival
  - a time clock fed item `i` at timestamp `i` leaves byte-identical storage to a count clock of the same sub-window length, over a stream that runs past `t + 2` sub-windows and does not end on a boundary
- merge
  - a merge needs a shared frame: a different sub-window length, a time clock against a count clock, a wider grid and a sketch at an earlier sub-window are each refused, each for its own stated reason — every candidate is brought to the target's own sub-window first — and a refused merge leaves the target byte-identical
  - a merge never puts either side's estimate more than the rounding slack below what that side read
- wire
  - a grid sized but never filled is refused, naming the row-column-depth mismatch, while the same geometry filled decodes
  - a zoomed sketch round trips to the same `max_zoom()`, the same sub-window and the same answers

## uniform_sampling.rs — 16 laws (experimental)

The draw is the rng's business, so no law names a priority. Values are compared
by `to_bits`, so `-0.0`, `0.0` and each NaN payload stay distinct members of the
stream multiset. Order independence is not a law here: a value's priority comes
from the rng position it arrives at, so a permuted stream retains a different
set.

- the stream counter
  - `total_seen()` counts every arrival at every rate, after each one
  - an input the sampler cannot read is not an arrival, and `update_input` leaves the sampler where `update` does
- what is retained
  - every retained sample is one that arrived, as a multiset containment, and the retained count never exceeds the arrival count
  - the retained count is `ceil(total_seen * rate)` after every arrival — read off the spec, not off the sampler
  - a rate of 1.0 retains the whole stream as a multiset
  - `sample_at(i)` walks `samples()` and is `None` past the end
- the draw sequence
  - a seed and a stream fix which samples are kept, even with a second sampler on another seed driven between the arrivals
  - which draws survive, not just how many: a full-rate run over the same seed and stream drops nothing and lists its entries in draw order, revealing each arrival's rank; replaying the stream against that ranking — each arrival taking its rank's place, an overfull sampler giving up its worst-ranked entry — reproduces the dropping-rate run exactly, order included
  - two samplers differing only in their seed keep different halves of a 128-value stream; agreeing would mean the seed never reached the priorities
- construction
  - a rate outside `(0, 1]` is refused by both constructors — including `0.0`, `-0.0`, NaN, both infinities and `MAX`/`MIN`
- merge
  - a merge adds the stream lengths, holds only what the two sides held, and retains the target for the combined stream
  - a merge keeps the head of each side in that side's own order: the two sides are ranked against each other and dropped from the far end. The two streams are told apart by sign, so each retained value names its side and position
  - which sampler the merge is called on does not move which samples survive (distinct seeds, so the ranking has no tie for the side order to settle)
  - a merge across rates is refused and leaves the sampler's samples and count alone
- wire
  - an ASAPv1 round trip preserves the samples, `total_seen`, the retained count and the rate
  - the envelope carries the draw sequence: a decoded sampler and the original, fed the same continuation, keep the same samples in the same order
