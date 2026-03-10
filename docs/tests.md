# Test Matrix

One component per section. Each section contains a table with test name, description, and what the test validates.

## How To Run

```bash
cargo test
```

## Sketches

### CountMin

Test file: [`src/sketches/countmin.rs`](../src/sketches/countmin.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `dimension_test` | Default/custom dimensions initialize zeroed counters. | Verifies default dimensions (`rows=3`, `cols=4096`), custom dimensions (`3x17`), and zero-initialized counters after construction. |
| `fast_insert_same_estimate` | Fast and regular insert paths produce identical estimates. | Inserts five string keys once into both `RegularPath` and `FastPath` sketches (`3x64`) and asserts equal estimates for every key. |
| `merge_adds_counters_element_wise` | Merge sums counters element-wise for matching dimensions. | Merges two `2x32` sketches after inserting the same key (`1` on left, `2` on right) and checks merged per-row target counters equal `3`. |
| `merge_requires_matching_dimensions` | Merge panics on dimension mismatch. | Confirms merging sketches with mismatched dimensions (`2x32` vs `3x32`) panics with `dimension mismatch while merging CountMin sketches`. |
| `cm_regular_path_correctness` | Regular-path hashing, counters, and estimates are exact on a deterministic stream. | Recomputes expected counter indices for `I32(0..9)` using per-row hashing, asserts exact full-matrix equality after one pass, doubled counters after second pass, and estimate `== 2` for each inserted key. |
| `cm_fast_path_correctness` | Fast-path counter placement matches bit-sliced hash mapping. | Recomputes expected fast-path indices for `I32(0..9)` from one hash plus row bit-slices/mask bits and asserts exact full-matrix equality. |
| `cm_error_bound_zipf` | Zipf-stream error bound holds for regular and fast paths. | On `200_000` Zipf samples with domain `8192` and exponent `1.1`, checks both paths satisfy: number of distinct queried keys with `|estimate - true| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `cm_error_bound_uniform` | Uniform-stream error bound holds for regular and fast paths. | On `200_000` uniform samples in `[100.0, 1000.0]`, checks both paths satisfy: number of distinct queried keys with `|estimate - true| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `count_min_round_trip_serialization` | Serialization round trip preserves full sketch state. | Serializes/deserializes a populated `3x8` regular-path sketch and verifies dimensions plus the full counter array are unchanged. |

### Count Sketch

Test file: [`src/sketches/count.rs`](../src/sketches/count.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `default_initializes_expected_dimensions` | Default dimensions initialize zeroed counters. | Verifies default Count Sketch dimensions (`rows=3`, `cols=4096`) and that all counters are zero after construction. |
| `with_dimensions_uses_custom_sizes` | Custom dimensions initialize zeroed rows. | Verifies `with_dimensions(3, 17)` applies requested shape and each row slice is zero-initialized. |
| `insert_updates_signed_counters_per_row` | Regular insert applies per-row signed updates. | After one insert of key `"alpha"` into a `3x64` sketch, checks each row’s hashed counter equals that row’s expected sign (`+1` or `-1`). |
| `fast_insert_produces_consistent_estimates` | Fast-path single inserts return unit estimates. | Inserts five string keys once into a fast-path sketch (`4x128`) and asserts estimate `== 1.0` for each key. |
| `insert_produces_consistent_estimates` | Regular-path single inserts return unit estimates. | Inserts five string keys once into a regular-path sketch (`3x64`) and asserts estimate `== 1.0` for each key. |
| `estimate_recovers_frequency_for_repeated_key` | Regular path recovers repeated-key frequency. | Inserts key `"theta"` 37 times into a regular-path sketch (`3x64`) and asserts estimate `== 37.0`. |
| `fast_path_recovers_repeated_insertions` | Fast path recovers repeated insertions across keys. | Inserts five keys for 5 rounds into a fast-path sketch (`4x256`) and asserts estimate `== 5.0` for each key. |
| `merge_adds_counters_element_wise` | Merge sums signed counters for matching dimensions. | Merges two regular-path `2x32` sketches after inserting the same key (`1` on left, `2` on right) and checks per-row target counters equal `sign(row,key) * 3`. |
| `merge_requires_matching_dimensions` | Merge panics on dimension mismatch. | Confirms merging `2x32` with `3x32` panics with `dimension mismatch while merging CountMin sketches`. |
| `zipf_stream_stays_within_twenty_percent_for_most_keys` | Zipf stream keeps relative error under 20% for most keys. | On Zipf stream (`rows=5`, `cols=8192`, `domain=8192`, `exponent=1.1`, `N=200_000`), computes per-key relative error and requires at least 70% of keys with error `< 0.20`. |
| `cs_regular_path_correctness` | Regular-path counter/sign mapping and estimates are exact on deterministic inserts. | Recomputes expected signed counter updates for `I32(0..9)` using regular hashing/sign logic, asserts exact matrix match after one pass, doubled counters after second pass, and estimate `== 2.0` for each inserted key. |
| `cs_fast_path_correctness` | Fast-path row-hash/sign mapping matches expected counters. | Recomputes expected fast-path updates for `I32(0..9)` using matrix hash row slices and row signs, then asserts exact full-matrix equality. |
| `cs_error_bound_zipf` | Zipf-stream error bound check passes for regular and fast paths. | On Zipf samples (`domain=8192`, `exponent=1.1`, `N=200_000`) with default dimensions, both paths require: count of distinct queried keys with `|estimate - true| < epsilon * N` is `> (1 - delta) * distinct_key_count`, with `epsilon = e / cols`, `delta = e^-rows`. |
| `cs_error_bound_uniform` | Uniform-stream error bound check passes for regular and fast paths. | On uniform samples in `[100.0, 1000.0]` with `N=200_000` and default dimensions, requires for both paths that in-bound distinct keys exceed `(1-delta)` fraction (`delta = e^-rows`); regular path uses `epsilon = sqrt(e / cols)` and bound `epsilon * L2_norm`, fast path uses `epsilon = e / cols` and bound `epsilon * N`. |
| `count_sketch_round_trip_serialization` | Serialization round trip preserves full Count Sketch state. | Serializes/deserializes a populated regular-path `3x8` sketch and verifies dimensions plus full counter array are unchanged. |
| `countl2hh_estimates_and_l2_are_consistent` | CountL2HH updates keep estimate and L2 consistent. | For `CountL2HH(3x32)`, applies `+5` then `-2` to one key, verifies estimates `5.0` then `3.0`, and asserts non-trivial L2 (`>= 3.0`). |
| `countl2hh_merge_combines_frequency_vectors` | CountL2HH merge combines per-key frequencies. | Merges two `CountL2HH(3x32)` sketches with same key counts `4` and `9`, then verifies merged estimate `== 13.0`. |
| `countl2hh_round_trip_serialization` | CountL2HH serialization round trip preserves estimate and L2. | Serializes/deserializes `CountL2HH::with_dimensions_and_seed(3,32,7)` after updates, verifying rows/cols and that both estimate and L2 remain unchanged (within `f64::EPSILON`). |

### HyperLogLog

Test file: [`src/sketches/hll.rs`](../src/sketches/hll.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hyperloglog_accuracy_within_two_percent` | Regular HyperLogLog stays within 2% relative error across scale checkpoints. | Inserts sequential unique `U64` values and checks at targets `[10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]` that relative error `|estimate-truth|/truth <= 0.02`. |
| `hlldf_accuracy_within_two_percent` | DataFusion HyperLogLog stays within 2% relative error across scale checkpoints. | Applies the same checkpointed unique-stream accuracy test as regular HLL, requiring relative error `<= 0.02` at each target cardinality. |
| `hllds_accuracy_within_two_percent` | HIP HyperLogLog stays within 2% relative error across scale checkpoints. | Applies the same checkpointed unique-stream accuracy test to `HyperLogLogHIP`, requiring relative error `<= 0.02` at each target cardinality. |
| `hyperloglog_merge_within_two_percent` | Regular HyperLogLog merge remains within 2% relative error. | Splits unique stream into even keys (left) and odd keys (right), merges sketches at each target checkpoint, and requires merged relative error `<= 0.02`. |
| `hlldf_merge_within_two_percent` | DataFusion HyperLogLog merge remains within 2% relative error. | Uses the same even/odd split merge scenario and requires merged relative error `<= 0.02` at each target checkpoint. |
| `hyperloglog_round_trip_serialization` | Regular HyperLogLog round trip preserves bytes and estimate stability. | After inserting `100_000` unique values, verifies serialized payload is non-empty, `deserialize -> reserialize` bytes are identical, and estimate drift is within `0.02 * max(original_est, 1.0)`. |
| `hlldf_round_trip_serialization` | DataFusion HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hllds_round_trip_serialization` | HIP HyperLogLog round trip preserves bytes and estimate stability. | Applies the same `100_000`-value serialization round-trip checks for `HyperLogLogHIP`: non-empty bytes, byte-for-byte reserialization equality, and bounded estimate drift. |
| `hll_correctness_test` | Register update logic matches expected bucket/index behavior for all HLL variants. | Runs fixed hashed inserts against Regular, DataFusion, and HIP variants; asserts exact expected register values at specific bucket indices and confirms an untouched bucket remains zero. |

### KLL

Test file: [`src/sketches/kll.rs`](../src/sketches/kll.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `coin_bit_cache_behavior` | Coin consumes cached random bits in deterministic bit order. | From a fixed seed, validates 3 successive 64-bit RNG blocks are consumed bit-by-bit (`0..63`) before refill, matching expected xorshift-derived bits exactly. |
| `coin_state_never_zero` | Coin state is never zero, including zero-seed initialization. | Verifies `Coin::from_seed(0)` normalizes to non-zero state and remains non-zero across 128 tosses. |
| `distributions_quantiles_stay_within_rank_error` | KLL quantiles stay within 2% rank tolerance across distributions and scales. | For `k=200`, checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` on Uniform (`0..100,000,000`) and Zipf (`1,000,000..10,000,000`, domain `8192`, exponent `1.1`) streams at sizes `[1_000, 5_000, 20_000, 100_000, 1_000_000, 5_000_000]`; each estimate must fall within truth interval defined by `q +/- 0.02`. |
| `test_sketch_input_api` | SketchInput numeric API is accepted and non-numeric input is rejected. | Inserts `I32`, `I64`, `F64`, `F32`, and `U32` values, checks median query lies between `20.0` and `40.2`, and verifies string input returns error `KLL sketch only accepts numeric inputs`. |
| `test_forced_compact` | Small-capacity KLL triggers compaction and keeps median in valid compacted outcomes. | With `KLL::init(3,3)` and inserts `[10,20,30,40,50]`, asserts median query is one of `{30.0, 40.0}` under forced compaction. |
| `test_no_compact` | Larger-capacity KLL avoids compaction for small stream and returns exact median. | With `KLL::init_kll(8)` and inserts `[10,20,30,40,50]`, asserts median query equals `30.0`. |
| `merge_preserves_quantiles_within_tolerance` | Merging two KLL sketches preserves quantiles within 2% rank tolerance. | Splits 10,000 uniform samples (`1,000,000..10,000,000`, seed `0xC0FFEE`) across two `k=200` sketches by index parity, merges, and checks quantiles `{0,0.1,0.25,0.5,0.75,0.9,1}` remain within `q +/- 0.02` truth bounds. |
| `cdf_handles_empty_sketch` | Empty KLL CDF queries return zero-valued defaults. | For empty `KLL::init_kll(64)`, asserts `cdf.quantile(123.0) == 0.0`, `cdf.query(0.5) == 0.0`, and `cdf.query_li(0.5) == 0.0`. |
| `kll_round_trip_rmp` | RMP round trip preserves KLL structure, packed data, and queried quantiles. | Serializes/deserializes `KLL::init_kll(256)` after 5,000 uniform updates (`0..1,000,000`, seed `0xDEAD_BEEF`), verifies non-empty bytes, core fields and packed arrays (`levels`, `items`) are identical, and CDF queries at `{0,0.1,0.25,0.5,0.75,0.9,1}` match within `f64::EPSILON`. |

### DDSketch

Test file: [`src/sketches/ddsketch.rs`](../src/sketches/ddsketch.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_query_basic` | Basic insert/query preserves count semantics and quantile monotonicity. | Inserts mixed values `[0.0, -5.0, 1.0, 2.0, 3.0, 10.0, 50.0, 100.0, 1000.0]`, verifies non-positive values are ignored (`count == 7`), and checks queried quantiles at `{0.0, 0.5, 0.9, 0.99, 1.0}` are monotone and bounded by sketch min/max. |
| `empty_quantile_returns_none` | Empty sketch returns no quantiles and zero count. | For a new `DDSketch(alpha=0.01)`, asserts `get_value_at_quantile` returns `None` for `p in {0.0, 0.5, 1.0}` and `get_count() == 0`. |
| `dds_uniform_distribution_quantiles` | Uniform-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from uniform range `[1_000_000, 10_000_000]` (seeded), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}` against sorted-truth quantiles. |
| `dds_zipf_distribution_quantiles` | Zipf-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from Zipf range `[1_000_000, 10_000_000]` (domain `8192`, exponent `1.1`, seeded), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `dds_normal_distribution_quantiles` | Normal-distribution quantiles stay within configured relative error. | With `alpha=0.01`, samples sizes `[1_000, 5_000, 20_000]` from normal distribution (`mean=1000.0`, `std=100.0`, positive finite values retained), and requires relative error `<= 0.01` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `dds_exponential_distribution_quantiles` | Exponential-distribution quantiles stay within near-1% relative error. | With `alpha=0.01`, `lambda=1e-3`, and sample sizes `[1_000, 5_000, 20_000]`, requires relative error `<= 0.011 + 1e-9` at quantiles `{0, 0.1, 0.25, 0.5, 0.75, 0.9, 1}`. |
| `merge_two_sketches_combines_counts_and_bounds` | Merge combines counts and preserves quantile boundary invariants. | Merges sketches built from `[1,2,3,4]` and `[5,10,20]`, then verifies merged `count=7`, `min=1`, `max=20`, exact boundary quantiles (`q0=1`, `q1=20`), and median lies within `[1,20]`. |
| `dds_serialization_round_trip` | Serialization round trip preserves count, bounds, and selected quantiles. | Serializes/deserializes a populated sketch (`alpha=0.01`), verifies non-empty bytes, equal `count/min/max`, and exact quantile matches at `{0.0, 0.1, 0.5, 0.9, 1.0}`. |

### FoldCMS

Test file: [`src/sketches/fold_cms.rs`](../src/sketches/fold_cms.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `cell_starts_empty` | Empty FoldCell reports no entries and zero counts. | Verifies `FoldCell::Empty` has `entry_count=0`, `is_empty=true`, and returns `0` for arbitrary queries. |
| `cell_single_insert` | First insert creates a single-entry cell. | Inserts `(full_col=10, delta=5)` into an empty cell and checks single-entry shape plus exact hit/miss query behavior. |
| `cell_single_accumulates` | Re-inserting same full column accumulates in place. | Inserts `(10,5)` then `(10,3)`, verifies count becomes `8`, entry count stays `1`, and state remains `Single`. |
| `cell_collision_upgrades_to_collided` | Distinct full-column inserts promote single cell to collision map. | Inserts two different `full_col` values (`10`, `42`) and verifies `Collided` state with both counts preserved. |
| `cell_collided_accumulates` | Collided entries keep independent accumulation per full column. | After creating collision state, re-inserts existing columns and checks per-column totals (`10->7`, `42->10`) with stable entry count. |
| `cell_collided_third_entry` | Collision map accepts additional distinct entries. | Inserts three distinct full columns (`10`, `42`, `99`) and verifies all counts and `entry_count=3`. |
| `cell_merge_from_empty` | Merging from empty source leaves destination unchanged. | Merges populated destination cell with `Empty` source and checks existing count is preserved exactly. |
| `cell_merge_from_single` | Merging compatible single entries sums counts without creating collision state. | Merges two single cells sharing `full_col=10`, verifies summed count `8` and resulting shape remains `Single`. |
| `cell_merge_from_collision` | Merging disjoint single entries creates collision state. | Merges destination containing `10` with source containing `42` and verifies both counts plus `Collided` representation. |
| `cell_iter_empty` | Empty cell iterator yields no entries. | Asserts `FoldCell::Empty.iter().count() == 0`. |
| `cell_iter_single` | Single cell iterator yields one exact pair. | Inserts one pair `(7,99)` and verifies iterator output is exactly `[(7,99)]`. |
| `cell_iter_collided` | Collided cell iterator yields all stored pairs. | Inserts two distinct pairs, collects/sorts iterator output, and verifies exact set `{(7,10), (15,20)}`. |
| `fold_cms_dimensions` | Constructor sets fold geometry consistently with level. | For `new(3,4096,4,10)`, verifies `rows=3`, `full_cols=4096`, `fold_cols=256`, and `fold_level=4`. |
| `fold_cms_level_zero_is_full` | Level-0 constructor uses full column width. | For `new_full(3,1024,10)`, verifies `fold_cols=full_cols=1024` and `fold_level=0`. |
| `fold_cms_rejects_non_power_of_two` | Constructor panics when full column count is not power-of-two. | Verifies `new(3,1000,0,10)` panics with `full_cols must be a power of two`. |
| `fold_cms_rejects_excessive_fold_level` | Constructor panics when fold level exceeds column bit width. | Verifies `new(3,256,9,10)` panics because fold level is invalid for `256 = 2^8`. |
| `fold_cms_insert_query_single_key` | Insert/query returns exact count for one key update. | Inserts key `"hello"` with delta `7` and verifies query returns `7`. |
| `fold_cms_insert_accumulates` | Multiple updates for same key accumulate exactly. | Inserts key `"hello"` with deltas `3` and `4`, then verifies query returns `7`. |
| `fold_cms_absent_key_returns_zero` | Querying unseen key returns zero. | After inserting only `"present"`, verifies querying `"absent"` returns `0`. |
| `fold_cms_multiple_keys` | Multi-key updates preserve Count-Min over-estimate property. | Inserts keys `0..99` with deltas equal to key value and verifies each estimate is at least the true count. |
| `fold_cms_matches_standard_cms_exact` | FoldCMS matches standard CountMin exactly on deterministic stream. | With `rows=3`, `cols=256`, `fold_level=3`, inserts 50 keys once, then verifies per-key query equality and full flat-counter equality vs `CountMin<Vector2D<i64>, RegularPath>`. |
| `fold_cms_matches_standard_cms_insert_many` | Weighted inserts match standard CountMin insert_many semantics. | With `rows=3`, `cols=512`, `fold_level=4`, inserts keys `0..29` with counts `i+1` and verifies exact per-key equality to standard CMS. |
| `same_level_merge_adds_counts` | Same-level merge sums counts for shared keys. | Merges two `fold_level=3` sketches containing `"user_001"` counts `100` and `200`, then verifies merged query `300`. |
| `same_level_merge_matches_standard_cms_merge` | Same-level merge remains equivalent to standard CMS merge. | Merges overlapping key ranges (`0..19` and `10..29`) and verifies merged FoldCMS queries equal merged standard CMS for keys `0..29`. |
| `unfold_merge_reduces_level` | Unfold merge lowers fold level by one step. | Unfold-merging two sketches at `fold_level=3` returns result with `fold_level=2` and `fold_cols=cols>>2`. |
| `unfold_merge_preserves_counts` | Unfold merge keeps per-key counts while reducing level. | After inserting disjoint keys (`alpha=10`, `beta=20`) into two `fold_level=2` sketches, verifies merged sketch at level `1` returns exact counts. |
| `unfold_merge_matches_standard_cms_merge` | Unfold merge stays exact with standard CMS under weighted overlaps. | Inserts weighted overlapping ranges (`0..39`, `20..59`), performs unfold merge, and verifies exact per-key equality vs merged standard CMS for keys `0..59`. |
| `hierarchical_merge_four_sketches` | Hierarchical merge over 4 epochs reaches level 0 and matches standard CMS. | Merges four sketches (10 unique keys each), verifies resulting `fold_level=0`, and checks keys `0..39` match standard CMS exactly. |
| `unfold_full_matches_flat_counters` | Full unfolding preserves exact counter tensor. | Compares `to_flat_counters()` before and after `unfold_full()`, verifying equality and resulting full-width geometry (`fold_level=0`, `fold_cols=full_cols`). |
| `to_flat_counters_matches_standard_cms` | Flat counter extraction matches standard CMS layout exactly. | After inserting keys `0..19`, verifies every flat counter index equals corresponding standard CMS counter value. |
| `sparse_subwindow_has_few_collisions` | Sparse folded window keeps entry count near ideal with limited collisions. | With `rows=3`, `full_cols=4096`, `fold_level=4`, inserts 50 distinct keys and asserts `total_entries` is between `rows*45` and `rows*50` and `collided_cells < 30`. |
| `heap_tracks_heavy_hitters` | Integrated heap tracks heavy hitters and their counts. | Inserts `"heavy"` 100 times, `"medium"` 10, `"light"` 1, then verifies heap contains `"heavy"` with count `100`. |
| `heap_survives_same_level_merge` | Heap reconciliation after same-level merge preserves top-key totals. | Merges sketches with `"user_x"` counts `50` and `70`, then verifies heap contains `"user_x"` with merged count `120`. |
| `heap_survives_unfold_merge` | Heap reconciliation after unfold merge preserves top-key totals. | Unfold-merges sketches with `"endpoint_a"` counts `40` and `60`, then verifies heap contains `"endpoint_a"` with count `100`. |
| `fold_cms_error_bound_zipf` | Zipf-stream CMS error-bound criterion holds. | On Zipf stream (`domain=8192`, `exponent=1.1`, `N=200_000`) with `rows=3`, `cols=4096`, verifies keys with `|estimate-true| < epsilon * N` exceed `(1-delta) * distinct_key_count`, where `epsilon=e/cols` and `delta=e^-rows`. |
| `scenario_rate_limiting` | Rate-limiting scenario merge reproduces expected per-user totals. | Merges two epoch sketches and verifies exact counts: `user_001=700`, `user_002=15`, `user_003=1300`. |
| `scenario_error_frequency` | Endpoint error-frequency scenario merge reproduces expected totals. | Merges two epoch sketches and verifies exact endpoint counts: `search=350`, `login=210`, `recommend=101`, `checkout=10`. |
| `large_window_merge_benchmark_cms` | Large-window hierarchical merge satisfies CMS statistical bound on Zipf workload. | Splits `500_000` Zipf samples (`domain=10_000`, `exponent=1.1`) into 16 folded subwindows (`rows=3`, `full_cols=4096`, `fold_level=4`), merges hierarchically, and asserts fraction within `epsilon*N` exceeds `1-delta` (`epsilon=e/full_cols`, `delta=e^-rows`). |
| `scenario_ddos_detection` | DDoS detection scenario identifies true offender after 3-epoch merge. | Hierarchically merges three epochs and verifies exact IP totals (`10.0.0.42=37_000`, `172.16.5.99=9_055`, `10.0.0.43=8_300`) plus threshold classification at `15_000`. |
| `scatter_merge_matches_standard_cms_n1_to_n8` | Scatter-based hierarchical merge matches standard CMS for N-way inputs. | For `N=1..8` sketches, merges each set hierarchically, verifies result reaches level 0, and checks all queried keys match a standard CMS fed the same inserts. |
| `unfold_to_single_pass_preserves_flat_counters` | `unfold_to` preserves exact flat counters at every target level. | Starting from `fold_level=4` sketch with weighted inserts (`1..40`), unfolds to targets `3,2,1,0` and verifies each unfolded sketch keeps identical flat counters. |
| `unfold_to_same_level_returns_clone` | `unfold_to` at current level returns equivalent sketch state. | Calls `unfold_to(3)` on a `fold_level=3` sketch containing key `"x"` count `42`, and verifies level and query value are unchanged. |
| `hierarchical_merge_mixed_fold_levels` | Hierarchical merge across mixed fold levels matches explicit level-0 reference merge. | Merges sketches at fold levels `4` and `2`, verifies output level is `0`, and checks keys `0..29` match reference built by `unfold_to(0)` then `merge_same_level`. |

### FoldCS

Test file: [`src/sketches/fold_cs.rs`](../src/sketches/fold_cs.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `fold_cs_dimensions` | Constructor sets FoldCS geometry consistently with level. | For `new(3,4096,4,10)`, verifies `rows=3`, `full_cols=4096`, `fold_cols=256`, and `fold_level=4`. |
| `fold_cs_level_zero_is_full` | Level-0 constructor uses full column width. | For `new_full(3,1024,10)`, verifies `fold_cols=full_cols=1024` and `fold_level=0`. |
| `fold_cs_rejects_non_power_of_two` | Constructor panics when full column count is not power-of-two. | Verifies `new(3,1000,0,10)` panics with `full_cols must be a power of two`. |
| `fold_cs_rejects_excessive_fold_level` | Constructor panics when fold level exceeds column bit width. | Verifies `new(3,256,9,10)` panics because fold level is invalid for `256 = 2^8`. |
| `fold_cs_insert_query_single_key` | Insert/query returns exact count for one key update. | Inserts key `"hello"` with delta `7` and verifies query returns `7`. |
| `fold_cs_insert_accumulates` | Multiple updates for same key accumulate exactly. | Inserts key `"hello"` with deltas `3` and `4`, then verifies query returns `7`. |
| `fold_cs_absent_key_returns_zero` | Querying unseen key returns zero. | After inserting only `"present"`, verifies querying `"absent"` returns `0`. |
| `fold_cs_multiple_keys` | Multi-key estimates stay near truth under wide sketch dimensions. | Inserts keys `0..99` with deltas equal to key value and verifies absolute error per key is at most `10`. |
| `fold_cs_sign_application` | FoldCS applies signed updates into raw cells. | Inserts 50 keys into `FoldCS::new(5,1024,4,10)` and verifies stored cell values contain both positive and negative counts. |
| `fold_cs_matches_standard_cs_exact` | FoldCS matches standard Count Sketch exactly on deterministic stream. | With `rows=3`, `cols=256`, `fold_level=3`, inserts 50 keys once and verifies per-key query equality vs `Count<Vector2D<i64>, RegularPath>`. |
| `fold_cs_matches_standard_cs_flat_counters` | FoldCS flat counters match standard Count Sketch layout. | Under the same deterministic setup, verifies `to_flat_counters()` length and every counter index exactly match standard CS storage. |
| `fold_cs_matches_standard_cs_insert_many` | Weighted inserts match standard Count Sketch `insert_many` semantics. | With `rows=3`, `cols=512`, `fold_level=4`, inserts keys `0..29` with counts `i+1` and verifies exact per-key equality to standard CS. |
| `same_level_merge_adds_counts` | Same-level merge sums counts for shared keys. | Merges two `fold_level=3` sketches containing `"user_001"` counts `100` and `200`, then verifies merged query `300`. |
| `same_level_merge_matches_standard_cs_merge` | Same-level merge remains equivalent to standard CS merge. | Merges overlapping key ranges (`0..19` and `10..29`) and verifies merged FoldCS queries equal merged standard CS for keys `0..29`. |
| `unfold_merge_reduces_level` | Unfold merge lowers fold level by one step. | Unfold-merging two sketches at `fold_level=3` returns result with `fold_level=2` and `fold_cols=cols>>2`. |
| `unfold_merge_preserves_counts` | Unfold merge keeps per-key counts while reducing level. | After inserting disjoint keys (`alpha=10`, `beta=20`) into two `fold_level=2` sketches, verifies merged sketch at level `1` returns exact counts. |
| `unfold_merge_matches_standard_cs_merge` | Unfold merge stays exact with standard CS under weighted overlaps. | Inserts weighted overlapping ranges (`0..39`, `20..59`), performs unfold merge, and verifies exact per-key equality vs merged standard CS for keys `0..59`. |
| `hierarchical_merge_four_sketches` | Hierarchical merge over 4 epochs reaches level 0 and matches standard CS. | Merges four sketches (10 unique keys each), verifies resulting `fold_level=0`, and checks keys `0..39` match standard CS exactly. |
| `unfold_full_matches_flat_counters` | Full unfolding preserves exact counter tensor. | Compares `to_flat_counters()` before and after `unfold_full()`, verifying equality and resulting full-width geometry (`fold_level=0`, `fold_cols=full_cols`). |
| `to_flat_counters_matches_standard_cs` | Flat counter extraction matches standard Count Sketch layout exactly. | After inserting keys `0..19`, verifies every flat counter index equals the corresponding standard CS counter value. |
| `sparse_subwindow_has_few_collisions` | Sparse folded window keeps entry count near ideal with limited collisions. | With `rows=3`, `full_cols=4096`, `fold_level=4`, inserts 50 distinct keys and asserts `total_entries` is between `rows*45` and `rows*50` and `collided_cells < 30`. |
| `heap_tracks_heavy_hitters` | Integrated heap tracks heavy hitters and their counts. | Inserts `"heavy"` 100 times, `"medium"` 10, `"light"` 1, then verifies heap contains `"heavy"` with count `100`. |
| `heap_survives_same_level_merge` | Heap reconciliation after same-level merge preserves top-key totals. | Merges sketches with `"user_x"` counts `50` and `70`, then verifies heap contains `"user_x"` with merged count `120`. |
| `heap_survives_unfold_merge` | Heap reconciliation after unfold merge preserves top-key totals. | Unfold-merges sketches with `"endpoint_a"` counts `40` and `60`, then verifies heap contains `"endpoint_a"` with count `100`. |
| `fold_cs_error_bound_zipf` | Zipf-stream Count Sketch error-bound criterion holds. | On Zipf stream (`domain=8192`, `exponent=1.1`, `N=200_000`) with `rows=3`, `cols=4096`, verifies keys with `|estimate-true| < epsilon * L2_norm` exceed `(1-delta) * distinct_key_count`, where `epsilon=sqrt(e/cols)` and `delta=e^-rows`. |
| `large_window_merge_benchmark_cs` | Large-window hierarchical merge satisfies Count Sketch statistical bound on Zipf workload. | Splits `500_000` Zipf samples (`domain=10_000`, `exponent=1.1`) into 16 folded subwindows (`rows=3`, `full_cols=4096`, `fold_level=4`), merges hierarchically, and asserts fraction within `epsilon * L2_norm` exceeds `1-delta` (`epsilon=sqrt(e/full_cols)`, `delta=e^-rows`). |
| `scatter_merge_matches_standard_cs_n1_to_n8` | Scatter-based hierarchical merge matches standard CS for N-way inputs. | For `N=1..8` sketches, merges each set hierarchically, verifies result reaches level 0, and checks all queried keys match a standard CS fed the same inserts. |
| `unfold_to_single_pass_preserves_flat_counters` | `unfold_to` preserves exact flat counters at every target level. | Starting from `fold_level=4` sketch with weighted inserts (`1..40`), unfolds to targets `3,2,1,0` and verifies each unfolded sketch keeps identical flat counters. |
| `unfold_to_same_level_returns_clone` | `unfold_to` at current level returns equivalent sketch state. | Calls `unfold_to(3)` on a `fold_level=3` sketch containing key `"x"` count `42`, and verifies level and query value are unchanged. |
| `hierarchical_merge_mixed_fold_levels` | Hierarchical merge across mixed fold levels matches explicit level-0 reference merge. | Merges sketches at fold levels `4` and `2`, verifies output level is `0`, and checks keys `0..29` match reference built by `unfold_to(0)` then `merge_same_level`. |

### CMSHeap

Test file: [`src/sketches/cms_heap.rs`](../src/sketches/cms_heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_estimate` | Repeated inserts increment Count-Min estimate for one key. | Inserts `"hello"` 5 times into `CMSHeap::new(3,64,10)` and verifies `estimate("hello") == 5`. |
| `heap_tracks_top_k` | Heap keeps highest-frequency keys within top-k capacity. | Inserts keys `1..5` with frequencies `10,20,30,40,50` into `top_k=3` sketch and verifies heap counts are exactly `[30,40,50]`. |
| `merge_reconciles_heaps` | Merge combines counters and refreshes heap counts from merged sketch. | Merges two sketches containing `"merge_key"` counts `10` and `20`, then verifies merged estimate and heap count are both `30`. |
| `insert_many_updates_estimate_and_heap` | `insert_many` updates estimate and heap entry consistently. | Calls `insert_many("many", 11)` and verifies `estimate == 11` plus heap entry count `11`. |
| `bulk_insert_updates_multiple_keys` | `bulk_insert` updates multiple keys and heavy-hitter counts correctly. | Inserts stream `[7,8,7,9,7]` and verifies estimates `7->3`, `8->1`, `9->1`, with heap count for key `7` equal to `3`. |
| `clear_heap_keeps_cms_counters` | Clearing heap does not clear CMS counters. | After `insert_many("persist",5)`, calls `clear_heap()`, verifies estimate remains `5`, then one more insert rebuilds heap entry to `6`. |
| `from_storage_uses_storage_dimensions` | `from_storage` preserves backend dimensions and requested heap capacity. | Builds from `Vector2D::init(4,128)` with `top_k=9` and verifies `rows=4`, `cols=128`, `heap.capacity=9`. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes pre-existing self heap keys to merged estimates. | After merging sketches with `a-key` counts `10` and `5`, verifies merged `a-key` estimate `15` and heap entry count `15`. |
| `fast_path_insert_and_estimate` | Fast path repeated inserts keep estimate exact for single key. | Inserts `"fast"` 7 times into fast-path sketch and verifies estimate `7`. |
| `fast_path_insert_many_and_bulk_insert` | Fast path batched APIs keep heap and estimate in sync. | Applies `insert_many("fast-many",6)` plus bulk inserts adding 2 more hits, then verifies estimate and heap count are `8`. |
| `fast_path_heap_tracks_top_k` | Fast path heap still preserves top-k ordering under weighted updates. | Inserts keys `1..5` with counts `10,20,30,40,50` via `insert_many` and verifies heap counts `[30,40,50]`. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes self heap entries using merged totals. | Merges sketches where `"a-fast"` contributes `10` and `5` across sides, then verifies estimate and heap count are `15`. |
| `default_construction` | Default CMSHeap constructor uses expected dimensions and heap capacity. | Verifies `CMSHeap::<Vector2D<i64>, RegularPath>::default()` has `rows=3`, `cols=4096`, and `heap.capacity=DEFAULT_TOP_K`. |
| `default_construction_fixed_backends_parity` | Default constructors across storage backends keep intended size/capacity contracts. | Verifies defaults for Fixed/Quick backends are `5x2048`, DefaultMatrix backends are `3x4096`, and all regular/fast variants use `DEFAULT_TOP_K`. |
| `merge_requires_matching_dimensions_panics` | Merge panics on incompatible sketch dimensions. | Verifies merging `CMSHeap::new(3,256,4)` with `CMSHeap::new(4,256,4)` panics with dimension-mismatch message. |
| `heap_entries_match_cms_estimates_after_mutations` | Every heap entry count matches current CMS estimate after updates and merge. | Checks heap-entry equality to `estimate(key)` both before and after merging another mutated sketch. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert is equivalent to repeated single inserts. | Compares bulk vs repeated insertion on same stream and verifies identical per-key estimates and heap counts for keys `1..5`. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular and fast wrappers agree on identical deterministic stream. | Feeds same 10-item string stream to both paths and verifies per-key estimates and heap counts match for `{alpha,beta,gamma,delta,epsilon}`. |
| `merge_with_empty_other_and_empty_self` | Merge behavior is stable when one side is empty. | Verifies merging non-empty with empty leaves counts unchanged and merging empty-self with non-empty copies counts/heap visibility correctly. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate merge candidates do not duplicate heap entries. | Merges sketches both containing `"dup"`; verifies merged count `19`, heap size within capacity, and exactly one heap entry for `"dup"`. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Regular path heap achieves high top-k recall on Zipf stream. | On Zipf stream (`rows=3`, `cols=4096`, `top_k=16`, `domain=1024`, `exponent=1.1`, `N=20_000`), verifies heap size bound, entry-count consistency, and recall hits `>= 15` vs truth top-16. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Fast path heap achieves high top-k recall on Zipf stream. | Runs same Zipf setup in fast mode and verifies heap size bound, entry-count consistency, and recall hits `>= 15`. |
| `zipf_stream_regular_fast_heap_overlap` | Regular and fast heaps substantially overlap on Zipf heavy hitters. | On shared Zipf stream (`top_k=16`), verifies key overlap ratio between regular and fast top-k heaps is at least `0.8`. |

### CSHeap

Test file: [`src/sketches/cs_heap.rs`](../src/sketches/cs_heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_estimate` | Repeated inserts increment Count Sketch estimate for one key. | Inserts `"hello"` 5 times into `CSHeap::new(5,256,10)` and verifies estimate is `5.0` within `1e-9`. |
| `heap_tracks_top_k` | Heap keeps highest-frequency keys within top-k capacity. | Inserts keys `1..5` with frequencies `100,200,300,400,500` into `top_k=3` sketch and verifies heap counts are exactly `[300,400,500]`. |
| `merge_reconciles_heaps` | Merge combines counters and refreshes heap counts from merged sketch. | Merges two sketches containing `"merge_key"` counts `10` and `20`, then verifies estimate is `30.0` and heap count is `30`. |
| `insert_many_updates_estimate_and_heap` | `insert_many` updates estimate and heap entry consistently. | Calls `insert_many("many", 17)` and verifies estimate `17.0` plus heap entry count equals estimated count. |
| `bulk_insert_updates_multiple_keys` | `bulk_insert` updates multiple keys and heavy-hitter counts correctly. | Inserts stream `[7,8,7,9,7]`, verifies estimate for key `7` is `3.0`, and heap count for key `7` matches estimate cast to integer. |
| `clear_heap_keeps_cs_counters` | Clearing heap does not clear Count Sketch counters. | After `insert_many("persist",5)`, calls `clear_heap()`, verifies estimate remains `5.0`, then one more insert repopulates heap with updated estimate count. |
| `from_storage_uses_storage_dimensions` | `from_storage` preserves backend dimensions and requested heap capacity. | Builds from `Vector2D::init(4,128)` with `top_k=9` and verifies `rows=4`, `cols=128`, `heap.capacity=9`. |
| `merge_refreshes_existing_self_heap_entries` | Merge refreshes pre-existing self heap keys to merged estimates. | Merges sketches where `"a-key"` is updated on both sides (`120` and `40`), then verifies heap count for `"a-key"` equals merged estimate. |
| `fast_path_insert_and_estimate` | Fast path repeated inserts keep estimate exact for single key. | Inserts `"fast"` 7 times into fast-path sketch and verifies estimate is `7.0` within `1e-9`. |
| `fast_path_insert_many_and_bulk_insert` | Fast path batched APIs keep heap and estimate in sync. | Applies `insert_many("fast-many",6)` plus bulk inserts adding 2 hits, then verifies estimate is `8.0` and heap count matches it. |
| `fast_path_heap_tracks_top_k` | Fast path heap preserves top-k ordering under weighted updates. | Inserts keys `1..5` with counts `100,200,300,400,500` via `insert_many` and verifies heap counts `[300,400,500]`. |
| `fast_path_merge_refreshes_existing_self_heap_entries` | Fast path merge refreshes self heap entries using merged totals. | Merges fast sketches where `"a-fast"` is updated on both sides (`120` and `40`) and verifies heap count equals merged estimate. |
| `default_construction` | Default CSHeap constructor uses expected dimensions and heap capacity. | Verifies `CSHeap::<Vector2D<i64>, RegularPath>::default()` has `rows=3`, `cols=4096`, and `heap.capacity=DEFAULT_TOP_K`. |
| `default_construction_fixed_backends_parity` | Default constructors across storage backends keep intended size/capacity contracts. | Verifies defaults for Fixed/Quick backends are `5x2048`, DefaultMatrix backends are `3x4096`, and all regular/fast variants use `DEFAULT_TOP_K`. |
| `merge_requires_matching_dimensions_panics` | Merge panics on incompatible sketch dimensions. | Verifies merging `CSHeap::new(5,256,4)` with `CSHeap::new(6,256,4)` panics with dimension-mismatch message. |
| `heap_entries_match_cs_estimates_after_mutations` | Every heap entry count matches current sketch estimate after updates and merge. | Checks heap-entry equality to `estimate(key)` both before and after merging another mutated sketch. |
| `bulk_insert_equivalent_to_repeated_insert` | Bulk insert is equivalent to repeated single inserts. | Compares bulk vs repeated insertion on same stream and verifies per-key estimates match within `1e-9` plus identical heap counts for keys `1..5`. |
| `regular_vs_fast_equivalence_on_same_stream` | Regular and fast wrappers agree on identical deterministic stream. | Feeds same 10-item string stream to both paths and verifies per-key estimates match within `1e-9` and heap counts match for `{alpha,beta,gamma,delta,epsilon}`. |
| `merge_with_empty_other_and_empty_self` | Merge behavior is stable when one side is empty. | Verifies merging non-empty with empty leaves estimates/heap size unchanged and merging empty-self with non-empty reproduces estimates and heap visibility. |
| `duplicate_candidate_keys_during_merge_do_not_corrupt_heap` | Duplicate merge candidates do not duplicate heap entries. | Merges sketches both containing `"dup"`; verifies heap count equals merged estimate, heap size stays within capacity, and only one heap entry exists for `"dup"`. |
| `zipf_stream_top_k_recall_regular_fast_budget` | Regular path heap achieves high top-k recall on Zipf stream. | On Zipf stream (`rows=5`, `cols=4096`, `top_k=16`, `domain=1024`, `exponent=1.1`, `N=20_000`), verifies heap size bound, entry-count consistency, and recall hits `>= 15` vs truth top-16. |
| `zipf_stream_top_k_recall_fast_path_fast_budget` | Fast path heap achieves high top-k recall on Zipf stream. | Runs same Zipf setup in fast mode and verifies heap size bound, entry-count consistency, and recall hits `>= 15`. |
| `zipf_stream_regular_fast_heap_overlap` | Regular and fast heaps substantially overlap on Zipf heavy hitters. | On shared Zipf stream (`top_k=16`), verifies key overlap ratio between regular and fast top-k heaps is at least `0.8`. |

### Elastic

Test file: [`src/sketches/elastic.rs`](../src/sketches/elastic.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `heavy_bucket_tracks_repeated_flow_exactly` | Heavy bucket tracks repeated flow exactly. | Top-K/heavy-hitter tracking and updates behave as expected. |
| `light_sketch_counts_colliding_flows` | Light sketch counts colliding flows. | Core functional behavior for this component path is validated. |

### Coco

Test file: [`src/sketches/coco.rs`](../src/sketches/coco.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_then_estimate_matches_full_value_for_partial_key` | Insert then estimate matches full value for partial key. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `estimate_with_udf_allows_custom_partial_matching` | Estimate with udf allows custom partial matching. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `merge_combines_tables_without_losing_counts` | Merge combines tables without losing counts. | Merge behavior preserves expected aggregate semantics and internal invariants. |

### KMV

Test file: [`src/sketches/kmv.rs`](../src/sketches/kmv.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `assert_accuracy` | Assert accuracy. | Accuracy/error behavior stays within expected bounds on representative workloads. |
| `assert_merge_accuracy` | Assert merge accuracy. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `assert_serialization_round_trip` | Assert serialization round trip. | Serialization/deserialization preserves component state and behavior after round trip. |

<!-- ### Locher

Test file: [`src/sketches/locher.rs`](../src/sketches/locher.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `locher_estimate_tracks_inserted_frequency` | Locher estimate tracks inserted frequency. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `median_handles_even_and_empty_inputs` | Median handles even and empty inputs. | Core behavior for insert/query/update and deterministic semantics is validated. | -->

<!-- ### MicroScope

Test file: [`src/sketches/microscope.rs`](../src/sketches/microscope.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_and_query_track_recent_volume` | Insert and query track recent volume. | Temporal/windowed behavior remains correct under time-based scenarios. |
| `merge_combines_counters_for_matching_windows` | Merge combines counters for matching windows. | Merge behavior preserves expected aggregate semantics and internal invariants. | -->

### UniformSampling

Test file: [`src/sketches/uniform.rs`](../src/sketches/uniform.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `sample_count_tracks_rate` | Sample count tracks rate. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `samples_are_drawn_from_input_stream` | Samples are drawn from input stream. | Core behavior for insert/query/update and deterministic semantics is validated. |
| `merge_combines_samples_using_rate_based_target` | Merge combines samples using rate based target. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `merge_rejects_different_rates` | Merge rejects different rates. | Merge behavior preserves expected aggregate semantics and internal invariants. |
| `sample_access_is_stable` | Sample access is stable. | Core behavior for insert/query/update and deterministic semantics is validated. |

## Sketch Frameworks

### Hydra

Test file: [`src/sketch_framework/hydra.rs`](../src/sketch_framework/hydra.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hydra_updates_countmin_frequency` | Hydra updates countmin frequency. | Updates `"user;session"` with value `"event"` 5 times and verifies combined query `>= 5` while an unrelated key query is exactly `0.0`. |
| `hydra_updates_countmin_frequency_multiple_values` | Hydra updates countmin frequency multiple values. | Inserts values `I64(0..4)` with multiplicity `i` under one key, verifies per-value fan-out query `>= i`, and checks unrelated-key query returns `0.0`. |
| `hydra_round_trip_serialization` | Hydra round trip serialization. | After mixed inserts, verifies MessagePack round trip keeps non-empty payload, preserves dimensions/template type, and keeps queried frequencies exactly unchanged. |
| `multihead_hydra_updates_multiple_dimensions` | Multihead hydra updates multiple dimensions. | With two heads (`events`, `latency`), repeated updates make full-key and fan-out frequency queries for each head return at least `3.0`. |
| `hydra_subpopulation_frequency_test` | Hydra subpopulation frequency test. | On a fixed labeled dataset, asserts exact subpopulation frequencies for single-label, multi-label, full-key, and disjoint cross-population queries (including zero-result case). |
| `hydra_subpopulation_cardinality_test` | Hydra subpopulation cardinality test. | Using HLL-backed counters, checks single/multi/full-key cardinalities are approximately `3.0` (within `EPSILON`) and disjoint/unknown keys return `0.0`. |
| `hydra_tracks_kll_quantiles` | Hydra tracks KLL quantiles. | For inserted samples `[10,20,30,40,50]`, verifies CDF query at `30.0` is `0.6` (within `1e-9`) and empty-bucket query returns `0.0`. |
| `hydra_kll_single_label_cdfs` | Hydra KLL single label cdfs. | For each label group, verifies exact expected CDF levels `{1/3, 2/3, 1}` at chosen thresholds using `EPSILON` tolerance. |
| `hydra_kll_multi_label_cdfs` | Hydra KLL multi label cdfs. | Verifies exact CDF values for multi-label combinations and confirms a non-overlapping key pair returns CDF `0.0`. |
| `hydra_kll_extreme_queries` | Hydra KLL extreme queries. | Confirms CDF boundary behavior (`0` below range, `1` above range) for known keys and `0` for unknown keys. |
| `test_count_min_frequency_query` | Test count min frequency query. | Inserts one key three times into `HydraCounter::CM`, then verifies `Frequency` query succeeds and returns exactly `3.0`. |
| `test_count_min_invalid_query_types` | Test count min invalid query types. | Verifies unsupported CM queries return errors, including exact message for `Quantile` (`"Count-Min Sketch Counter does not support Quantile Query"`). |
| `test_hll_cardinality_query` | Test HLL cardinality query. | Inserts `100` unique items plus one duplicate and verifies `Cardinality` query succeeds with estimate constrained to `(90.0, 110.0)`. |
| `test_kll_quantile_query` | Test KLL quantile query. | Inserts values `1..=100` and verifies median query succeeds with estimate within `+/-5` of `50.0`. |
| `test_univmon_universal_queries` | Test univmon universal queries. | Inserts `A` 10 times and `B` 20 times, then checks `L1=30.0`, cardinality is approximately `2.0` (`abs err < 0.5`), and entropy is positive. |
| `test_merge_counters` | Test merge counters. | Merges two CM counters and verifies frequency sum (`2.0`) for shared key, then confirms merging with mismatched counter type (`HLL`) returns error. |
| `test_count_frequency_query` | Test count frequency query. | Inserts one Count Sketch key four times and verifies `Frequency` query succeeds with exact result `4.0`. |
| `test_count_invalid_query_types` | Test count invalid query types. | Verifies unsupported Count Sketch queries fail, including exact `Quantile` error message and error on `Cardinality`. |

### HashLayer

Test file: [`src/sketch_framework/hashlayer.rs`](../src/sketch_framework/hashlayer.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `test_hashlayer_insert_all` | Test hashlayer insert all. | On Zipf stream (`N=10_000`, `domain=1000`, `exp=1.5`), verifies default layer size is `3` and average relative error for CountMin/Count queries over sampled keys is below `0.1`. |
| `test_hashlayer_insert_at_specific_indices` | Test hashlayer insert at specific indices. | Inserts only into indices `[0,1]` and verifies queried estimates at CountMin/Count indices are successful and strictly positive. |
| `test_hashlayer_query_all` | Test hashlayer query all. | After bulk inserts, verifies `query_all` returns exactly `3` results and each result is `Ok` (including HLL index). |
| `test_hashlayer_with_hash_optimization` | Test hashlayer with hash optimization. | Uses precomputed `Packed128` hashes for insert/query paths and verifies average CountMin relative error over sampled keys remains below `0.1`. |
| `test_hashlayer_hll_cardinality` | Test hashlayer HLL cardinality. | Compares HLL cardinality estimate (index `2`) to true distinct count and requires relative error `< 0.02`. |
| `test_hashlayer_direct_access` | Test hashlayer direct access. | Verifies `get(0..2)` returns `Some`, out-of-bounds `get(3)` returns `None`, and mutable access at index `0` reports sketch type `"CountMin"`. |
| `test_hashlayer_bounds_checking` | Test hashlayer bounds checking. | Confirms `query_at(999, ...)` and `query_at_with_hash(999, ...)` both return `Err("Index out of bounds")`. |
| `test_hashlayer_custom_sketches` | Test hashlayer custom sketches. | Builds custom two-sketch layer (`CountMin` + `Count`, `5x2048`), verifies `len=2`/non-empty, and confirms both indices return successful positive estimates after inserts. |

### UnivMon

Test file: [`src/sketch_framework/univmon.rs`](../src/sketch_framework/univmon.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `univmon_round_trip_serialization` | Univmon round trip serialization. | After weighted inserts, verifies non-empty serialization and round-trip preservation of configuration fields, `bucket_size`, `L1/L2/entropy` (`<1e-6` drift), and cardinality (`< EPSILON` drift). |
| `update_populates_bucket_size_and_heavy_hitters` | Update populates bucket size and heavy hitters. | Inserting one hot key `40` times sets `bucket_size=40`, tracks key in heavy-hitter heap with count `>=20`, and yields exact `L1=40` and `cardinality=1`. |
| `merge_with_combines_heavy_hitters` | Merge with combines heavy hitters. | Merging sketches with disjoint heavy keys verifies merged left heap contains both contributions (`left=25`, `right=30`) while right heap retains `right=30`. |
| `univmon_layers_use_different_seeds` | Univmon layers use different seeds. | Verifies hash outputs for the same key with seed indices `0..3` are all pairwise different. |
| `univmon_cardinality_is_positive` | Univmon cardinality is positive. | After inserting `20` distinct flow keys, cardinality estimate is exactly `20.0`. |
| `univmon_bucket_size_tracked_correctly` | Univmon bucket size tracked correctly. | Inserts counts `100`, `200`, `150` for three flows and verifies `bucket_size` equals total `450`. |
| `univmon_basic_operation` | Univmon basic operation. | On fixed mixed workload, verifies exact aggregate metrics `cardinality=10.0` and `L1=131.0`. |
| `test_statistical_accuracy` | Test statistical accuracy. | On heavy/medium/noise synthetic distribution, verifies relative error for both `L2` and `entropy` is below `0.15`. |
| `univmon_random_data_matches_ground_truth_within_five_percent` | Univmon random data matches ground truth within five percent. | Over `10_000` random weighted updates, requires relative error `<= 0.05` for `cardinality`, `L1`, `L2`, and `entropy` against exact truth map. |

### UnivMon Optimized

Test file: [`src/sketch_framework/univmon_optimized.rs`](../src/sketch_framework/univmon_optimized.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `pool_basic_take_put` | Pool basic take put. | Validates pool accounting: initial preallocation (`available=2`, `allocated=2`), on-demand allocation when empty (`allocated=3`), and reuse on put/take without further allocation. |
| `pool_free_resets_sketch` | Pool free resets sketch. | Confirms returning a used sketch to pool resets state so retaken sketch has `bucket_size=0` and near-zero `L2` in layer `0`. |
| `pyramid_basic_insert_and_query` | Pyramid basic insert and query. | For simple inserts, verifies exact aggregate state with `bucket_size=65`, `L1` approximately `65` (`<1e-6`), and `cardinality=3`. |
| `pyramid_fast_insert_matches_standard` | Pyramid fast insert matches standard. | On identical 500-item stream, verifies standard vs fast paths keep identical `bucket_size`, with `L1` deviation `<10%` and cardinality deviation `<15%`. |
| `pyramid_two_tier_dimensions` | Pyramid two tier dimensions. | Verifies two-tier layout metadata for configured pyramid (`layer_size=8`, `elephant_layers=4`). |
| `pyramid_free_resets_state` | Pyramid free resets state. | After bulk inserts, `free()` resets sketch to empty baseline (`bucket_size=0`, layer-0 `L2` approximately `0`). |
| `pyramid_merge_combines_data` | Pyramid merge combines data. | Merging disjoint halves verifies merged `L1` stays within `10%` of the sum of pre-merge `L1` values. |
| `pyramid_accuracy_zipf` | Pyramid accuracy Zipf. | On heavy/medium/light Zipf-like workload, requires relative error `<15%` for `L1`, `L2`, cardinality, and entropy. |
| `pyramid_fast_insert_accuracy` | Pyramid fast insert accuracy. | Using `fast_insert` only, requires relative error `<15%` for `L1`, `L2`, cardinality, and entropy versus exact frequency map. |
| `pyramid_memory_savings_vs_uniform` | Pyramid memory savings vs uniform. | Verifies pyramid column budget is smaller than uniform baseline and computed memory savings exceed `30%`. |

### NitroBatch

Test file: [`src/sketch_framework/nitro.rs`](../src/sketch_framework/nitro.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `nitro_batch_countmin_error_bound_zipf` | Nitro batch countmin error bound Zipf. | On Zipf stream (`rows=3`, `cols=4096`, `N=200_000`), verifies CountMin estimates satisfy in-bound key count `> (1-delta)*distinct` using `epsilon=e/cols`, `delta=e^-rows`, and bound `epsilon*N`. |
| `nitro_batch_count_error_bound_zipf` | Nitro batch count error bound Zipf. | Applies the same probabilistic in-bound criterion to Count Sketch median estimates with `epsilon=e/cols`, `delta=e^-rows`, and bound `epsilon*N`. |

### ExponentialHistogram

Test file: [`src/sketch_framework/eh.rs`](../src/sketch_framework/eh.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `constructor_infers_merge_norm` | Constructor infers merge norm. | Verifies constructor infers `SketchNorm::L1` for CM payload and `SketchNorm::L2` for `COUNTL2HH` payload. |
| `l1_merge_invariant_same_size` | L1 merge invariant same size. | Under repeated updates with `k=2`, verifies L1 merge policy compacts buckets so `bucket_count < 10`. |
| `l2_merge_invariant_sum_l22` | L2 merge invariant sum l22. | With `k=1` and weighted updates, verifies L2 merge rule keeps bucket count bounded (`bucket_count <= 2`). |
| `merge_recomputes_l2_mass` | Merge recomputes L2 mass. | After L2 merges, verifies bounded bucket count (`<=2`) and non-negative recomputed `l2_mass` for every payload bucket. |
| `test_basic_insertion_and_query` | Test basic insertion and query. | After one update at `t=100`, verifies single bucket presence, exact min/max timestamps (`100`), and successful interval merge query for `[100,100]`. |

### EHSketchList

Test file: [`src/sketch_framework/eh_sketch_list.rs`](../src/sketch_framework/eh_sketch_list.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `insert_routes_to_countl2hh_and_univmon` | Insert routes to countl2hh and univmon. | Verifies variant routing by checking `COUNTL2HH` estimate `>=9` after 9 inserts and `UNIVMON` `bucket_size=6` after 6 inserts. |
| `count_sketch_insert_and_query_round_trip` | Count sketch insert and query round trip. | Confirms Count Sketch variant updates/query path by inserting one key and verifying returned estimate is at least `1.0`. |
| `ddsketch_insert_and_quantile_query_round_trip` | DDSketch insert and quantile query round trip. | Inserts `10,20,30` into DDSketch variant and verifies queried median (`q=0.5`) lies within `[10.0, 30.0]`. |
| `supports_norm_whitelist_is_enforced` | Supports norm whitelist is enforced. | Validates norm capability matrix: `CM/CS/DDS` support `L1` only, while `COUNTL2HH/UNIVMON` support `L2` only. |

### EHUnivOptimized

Test file: [`src/sketch_framework/eh_univ_optimized.rs`](../src/sketch_framework/eh_univ_optimized.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `basic_insertion_and_query` | Basic insertion and query. | For updates `{(1,5),(2,3),(1,2)}` across `[100,102]`, verifies map-tier result with exact counts (`1->7`, `2->3`, `total=10`) plus `L1=10` and `cardinality=2`. |
| `map_merge_bounds_volume` | Map merge bounds volume. | With `k=1` and 50 one-count updates, verifies merge policy bounds growth so `bucket_count < 50`. |
| `promotion_creates_sketch_buckets` | Promotion creates sketch buckets. | Under small promotion thresholds and many distinct updates, verifies at least one map bucket is promoted (`um_buckets` becomes non-empty). |
| `window_expiration` | Window expiration. | With `window=100`, advancing to `t=200` after earlier inserts confirms expiration by forcing oldest surviving `min_time` to recent range (`>=100` or `==200`). |
| `hybrid_query_returns_sketch` | Hybrid query returns sketch. | After forcing both map and sketch tiers to coexist, verifies interval query spanning both returns `EHUnivQueryResult::Sketch` (not map-only). |
| `cover_check` | Cover check. | Verifies coverage logic transitions from false (empty) to true for contained intervals and remains false when query extends outside observed range. |
| `accuracy_known_distribution` | Accuracy known distribution. | On fixed known histogram, verifies query estimates for `L1`, `L2`, cardinality, and entropy each stay within `10%` relative error. |
| `pool_used_during_promotion` | Pool used during promotion. | With bounded preallocated pool, promotion workload verifies sketch-tier creation and confirms pool allocation accounting remains active (`total_allocated >= 2`). |
| `correctness_map_only_exact` | Correctness map only exact. | For map-only regime, verifies `L1/L2/cardinality/entropy` each match exact truth within `1%` tolerance. |
| `correctness_subinterval_query` | Correctness subinterval query. | For two-phase stream, verifies full-interval query recovers `L1` approximately `200` and `cardinality` approximately `2` within `5%` tolerance. |
| `correctness_expired_data_excluded` | Correctness expired data excluded. | After sliding beyond window cutoff, verifies very old segment is excluded by checking earliest retained bucket time is at least `50`. |
| `correctness_volume_bounded_long_stream` | Correctness volume bounded long stream. | Over `20_000` updates with `k=4`, verifies EH volume bound by requiring maximum observed bucket count `< 200`. |
| `correctness_pool_recycling_across_cycles` | Correctness pool recycling across cycles. | Long-run expiration/promotion cycling keeps pool bounded (`total_allocated < 50`) and still returns valid interval query results. |
| `correctness_sketch_merge_preserves_metrics` | Correctness sketch merge preserves metrics. | After repeated promotions/merges, verifies each sketch bucket has positive `L2^2` and stored `l22` stays within `1%` relative difference of recomputed value. |
| `accuracy_zipf_distribution_sketch_tier` | Accuracy Zipf distribution sketch tier. | On heavy/medium/light Zipf-like stream in sketch tier, requires `L1/L2/cardinality/entropy` relative errors each `<= 15%`. |
| `accuracy_uniform_distribution` | Accuracy uniform distribution. | On uniform stream, requires `L1/L2/cardinality/entropy` relative errors each `<= 10%`. |
| `accuracy_sliding_window` | Accuracy sliding window. | Across suffix and periodic sliding-window queries, verifies average relative error for `L1`, `L2`, cardinality, and entropy is each below `15%`. |
| `accuracy_varies_with_k` | Accuracy varies with K. | For `k in {2,8,32}`, verifies per-k average of `L1/L2` relative errors remains under `15%` on same fixed stream/window. |
| `accuracy_suffix_queries` | Accuracy suffix queries. | Across suffix lengths `[1000,2000,5000,8000]`, verifies worst observed `L2` relative error remains below `20%`. |
| `accuracy_distribution_shift` | Accuracy distribution shift. | For two-phase distribution shift stream, verifies full-span `L1/L2/cardinality/entropy` estimates each stay within `15%` relative error. |

### TumblingWindow

Test file: [`src/sketch_framework/tumbling.rs`](../src/sketch_framework/tumbling.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `pool_take_returns_preallocated` | Pool take returns preallocated. | Verifies preallocated pool accounting (`available=4`, `allocated=4`) and that one `take()` decrements availability without increasing allocations. |
| `pool_take_allocates_when_empty` | Pool take allocates when empty. | Starting from zero-capacity pool, verifies `take()` performs on-demand allocation (`total_allocated` becomes `1`). |
| `pool_put_recycles` | Pool put recycles. | Confirms returned sketch is recycled by checking availability returns to `1` with no extra allocation growth. |
| `fold_cms_clear_resets_to_empty` | Fold CMS clear resets to empty. | After populating FoldCMS, `clear()` makes all queried keys `0` and empties heavy-hitter heap. |
| `fold_cs_clear_resets_to_empty` | Fold CS clear resets to empty. | After populating FoldCS, `clear()` makes all queried keys `0` and empties heavy-hitter heap. |
| `kll_clear_resets_to_empty` | KLL clear resets to empty. | After updates, `clear()` resets KLL count to `0` and empty-CDF query at `0.5` to `0.0`. |
| `zero_window_size_panics` | Zero window size panics. | Verifies constructor rejects `window_size=0` with panic message `"window_size must be > 0"`. |
| `window_closes_on_time_advance` | Window closes on time advance. | Verifies boundary crossings at `t=100` and `t=200` close windows and move `closed_count` from `0` to `1` to `2`. |
| `window_evicts_oldest_beyond_max` | Window evicts oldest beyond max. | With `max_windows=3`, advancing across multiple windows verifies closed-window retention is bounded (`closed_count <= 3`). |
| `window_pool_recycles_on_eviction` | Window pool recycles on eviction. | After repeated evictions, verifies pool reuse (`pool_available > 0`) and bounded allocation growth (`pool_total_allocated <= initial + 6`). |
| `query_all_matches_manual_merge` | Query all matches manual merge. | Confirms `query_all()` merged sketch matches manual FoldCMS merge exactly for every inserted key. |
| `query_recent_selects_subset` | Query recent selects subset. | Verifies `query_recent(1)` includes most recent closed + active windows (`new=10`, `active=7`) and excludes older window data (`old=0`). |
| `fold_cms_tumbling_hierarchical_merge` | Fold CMS tumbling hierarchical merge. | On 8-window Zipf workload, hierarchical merge keeps more than `90%` of keys within CMS bound `epsilon*L1` (`epsilon=e/full_cols`). |
| `kll_tumbling_quantile_accuracy` | KLL tumbling quantile accuracy. | For uniform stream across windows, merged KLL median is constrained to true rank interval `[48%, 52%]`. |
| `flush_closes_active_window` | Flush closes active window. | `flush()` closes active window (`closed_count=1`), clears active sketch state, and keeps flushed data visible in `query_all()`. |
| `fold_cs_tumbling_basic` | Fold CS tumbling basic. | Verifies cross-window accumulation in FoldCS by checking merged query for `"hello"` equals `10`. |
| `fold_cs_tumbling_hierarchical_merge` | Fold CS tumbling hierarchical merge. | Hierarchical merge is verified to reach `fold_level=0` and keep per-key error within `1` for all 40 inserted keys. |
| `fold_cms_tumbling_accuracy_zipf` | Fold CMS tumbling accuracy Zipf. | On `500_000` Zipf samples, verifies fraction within CMS bound `epsilon*L1` is at least theoretical target `(1 - e^-rows)`. |
| `fold_cms_hierarchical_vs_flat_merge` | Fold CMS hierarchical vs flat merge. | Verifies hierarchical result reaches level `0` and matches unfolded flat-merge estimates exactly for every queried key. |
| `fold_cs_tumbling_accuracy_zipf` | Fold CS tumbling accuracy Zipf. | On `500_000` Zipf samples, verifies fraction within CS bound `sqrt(e/cols)*L2` is at least `(1 - e^-rows)`. |
| `kll_tumbling_multi_quantile_accuracy` | KLL tumbling multi quantile accuracy. | Verifies quantiles `{0.10,0.25,0.50,0.75,0.90}` stay within `+/-0.02` rank tolerance on merged uniform stream. |
| `kll_tumbling_distribution_shift` | KLL tumbling distribution shift. | For two-phase normal distributions, checks shape constraints (`p10<200`, `50<p50<600`, `p90>350`) and monotonic ordering `p10<p50<p90`. |
| `tumbling_eviction_correctness` | Tumbling eviction correctness. | Verifies retained-window keys are `>90%` within CMS bound and keys appearing only in evicted windows stay below the same bound. |
| `tumbling_query_recent_accuracy` | Tumbling query recent accuracy. | For `query_recent(3)`, verifies recent-window keys are `>90%` within CMS bound and excluded-window-only keys remain below bound. |
| `fold_cms_tumbling_heap_correctness` | Fold CMS tumbling heap correctness. | On skewed Zipf stream, verifies merged heap is non-empty and recalls at least `80%` of true top-`20` keys. |
| `fold_cs_tumbling_query_recent_accuracy` | Fold CS tumbling query recent accuracy. | For FoldCS `query_recent(3)`, verifies `>90%` of recent keys are within CS bound and excluded-only keys have `|estimate| <= bound`. |
| `kll_tumbling_query_recent_accuracy` | KLL tumbling query recent accuracy. | Verifies recent-window KLL quantiles `{0.10,0.25,0.50,0.75,0.90}` fall within `+/-0.03` rank tolerance. |
| `fold_cs_tumbling_heap_correctness` | Fold CS tumbling heap correctness. | On skewed Zipf stream, verifies merged FoldCS heap is non-empty and recalls at least `80%` of true top-`20` keys. |
| `fold_cms_tumbling_vs_monolithic` | Fold CMS tumbling vs monolithic. | Compares same stream against monolithic FoldCMS and requires tumbling mean absolute error to stay within `1.5x` monolithic MAE. |
| `fold_cs_tumbling_vs_monolithic` | Fold CS tumbling vs monolithic. | Compares same stream against monolithic FoldCS and requires tumbling mean absolute error to stay within `1.5x` monolithic MAE. |
| `kll_tumbling_vs_monolithic` | KLL tumbling vs monolithic. | Across multiple quantiles, verifies tumbling max rank error is no more than monolithic max rank error plus `0.02`. |
| `tumbling_single_window_accuracy` | Tumbling single window accuracy. | When data fits one window (no closures), verifies FoldCMS/FoldCS each keep `>90%` keys within bounds and KLL q25/q50/q75 remain within `+/-0.02` rank tolerance. |
| `tumbling_very_small_windows` | Tumbling very small windows. | With tiny windows (`size=10`) and retained-tail evaluation, verifies CMS accuracy remains above `85%` keys within theoretical bound. |
| `tumbling_skewed_load` | Tumbling skewed load. | Under highly imbalanced per-window load, verifies merged CMS still keeps more than `90%` of keys within `epsilon*L1` bound. |
## Common

### Common Hash Utilities

Test file: [`src/common/hash.rs`](../src/common/hash.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `hash128_seeded_preserves_cardinality` | Hash128 seeded preserves cardinality. | With `SEED_IDX=0` and `SAMPLE_SIZE=5000`, verifies uniform and Zipf sample unique-input counts exactly match unique-hash counts (no observed collisions). |
| `hash128_seeded_is_deterministic_for_repeated_inputs` | Hash128 seeded is deterministic for repeated inputs. | For fixed key `"deterministic-key"` and seed `3`, verifies 100 repeated `hash128_seeded` calls always equal the first hash value. |

### Common Heap Utilities

Test file: [`src/common/heap.rs`](../src/common/heap.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `heap_retains_top_k_items_by_count` | Heap retains top K items by count. | For `HHHeap::new(3)` updated with counts `1..5`, verifies heap size is `3` and retained counts are exactly `[3,4,5]`. |
| `update_count_increments_existing_entry` | Update count increments existing entry. | Repeatedly updates key `alpha` with counts `1,2,3` and verifies stored heap entry count is `3` (incremental update, not replacement). |
| `clean_resets_heap_state` | Clean resets heap state. | After inserting two items into `HHHeap::new(2)`, `clear()` is verified to leave the heap empty. |
| `test_min_heap_basic` | Test min heap basic. | For `CommonHeap::<i32, KeepSmallest>::new_min(5)`, verifies `peek=1` and pop order `1,3,5,7`, then `None`. |
| `test_max_heap_basic` | Test max heap basic. | For `CommonHeap::<i32, KeepLargest>::new_max(5)`, verifies `peek=7` and pop order `7,5,3,1`, then `None`. |
| `test_bounded_heap_capacity` | Test bounded heap capacity. | With min-heap capacity `3`, verifies length never exceeds 3 and final retained values are `[5,7,10]` after pushing `5,3,7,1,10`. |
| `test_update_at` | Test update at. | After mutating an internal element (`heap[1]=3`) and calling `update_at(1)`, verifies heap root updates so `peek()` becomes `3`. |
| `test_custom_struct_with_ord` | Test custom struct with ord. | Uses `HHItem` values with counts `5,3,7` and verifies min-heap ordering by checking root count is `3`. |
| `test_topk_use_case` | Test topk use case. | Simulated top-k flow keeps only counts `[3,4,5]` at capacity 3 and verifies lookup for `key-4` succeeds with count `4`. |
| `test_heap_size` | Test heap size. | Verifies both `CommonHeap<u64, KeepSmallest>` and `CommonHeap<u64, KeepLargest>` sizes equal `size_of::<Vec<u64>>() + size_of::<usize>()`. |
| `test_topk_with_custom_comparator` | Test topk with custom comparator. | With custom comparator and capacity 3, verifies low-count insert is rejected/replaced as expected so heap size is 3 and root count is `5`. |
| `test_exact_topk_heap_replacement` | Test exact topk heap replacement. | Reproduces TopK-style find/update flow for keys `1..5`, verifies retained counts `[3,4,5]`, finds `key-4` with count `4`, then verifies `clear()` makes heap empty. |

### Common Structure Utilities

Test file: [`src/common/structure_utils.rs`](../src/common/structure_utils.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `median_test` | Median test. | For 1,000 seeded random arrays of lengths 3, 4, and 5, verifies `compute_median_inline_f64` exactly matches sort-based median for every case. |

### Vector2D (Common Structure)

Test file: [`src/common/structures/vector2d.rs`](../src/common/structures/vector2d.rs)

| test_name | test_description | what_is_tested |
|---|---|---|
| `required_bits_match_expected_thresholds` | Required bits match expected thresholds. | Verifies `Vector2D::get_required_bits()` returns `64` for `(3,4096)`, `32` for `(3,64)`, and `128` for `(5,1_048_576)`. |
