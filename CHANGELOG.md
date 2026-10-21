# Changelog

All notable changes to `asap_sketchlib` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
For pre-1.0 (`0.y.z`) releases we follow the Cargo convention of treating `y`
as the major component: bumping `y` signals a breaking change, bumping `z`
signals a backwards-compatible change.

## [Unreleased]

### Added

- **Checked f64 updates and heap rescoring for the Count-Min and Count Sketch
  heap sketches.** Over `Vector2D<f64>` counters, `check_weight` and
  `try_update_weighted` on `CountMinSketch` and `CountSketch` reject a
  non-finite weight (and a negative one for Count-Min) and a counter
  overflow, leaving the sketch unchanged; `CountMinSketchWithHeap` and
  `CountSketchWithHeap` gain the same `try_update_weighted`, which then
  updates the heap. `rescore_heap` on both heap sketches re-reads every
  resident's count from the counters, since an update rescores only the key
  it touches.

## [0.4.0] - 2026-10-20

Breaking release. ASAPv1 is the one serialized form: protobuf, the legacy
MessagePack wrappers and `serde` on sketch types are removed. Public names
follow one scheme (every write is `update`, getters drop `get_`, acronyms are
one word). Adds ASAPv1 deltas for the counter, HLL, DDSketch and SetAggregator
sketches, `TupleKey` heap keys, `Vector3D`, Count-Min and Count Sketch with
Space-Saving, and the experimental `CountMinHll` and `MicroCm`. Fixes the
UnivMon cardinality and L2 estimators, a KllDynamic NaN panic and an
`OctoRuntime::flush` race. See **Changed** and **Removed** below for the
renames and wire changes.

### Added

- **`Hydra::query_group_total(key)`** returns a subpopulation's total mass
  (`count by (g)`) from Count-Min cells, with an error bound, or KLL cells,
  approximately; **`HydraCounter::total_mass()`** gives one cell's. No wire
  change.
- **`UnivMon::check_shape`** accepts exactly the pyramid shapes `UnivMon::new`
  builds, so a caller can refuse a configuration without catching a panic.
- **`TupleKey` and `KeyColumn`**, a heap key of typed columns (null, bool,
  `i64`, `f64`, string) encoded into one canonical byte string, so a GROUP BY
  over several columns keeps one CountMinSketchWithHeap or CountSketchWithHeap candidate per tuple under
  `key_type` `"bytes"`. The encoding is ASAPv1's tuple key (spec Section 2);
  `-0.0` encodes as `0.0` and every `NaN` as one `NaN`.
- **ASAPv1 golden fixtures for KMV, Bloom, Space-Saving, UniformSampling,
  UnivMonTwoTier, UnivMon-Q, ExponentialHistogram and EhBucketSketch**, so
  every kind_id the spec marks implemented has one. The spec fixes Bloom's bit
  order within a word: column `c` is bit `c mod 64`.
- **`Vector3D<T>`**, filled in from a stub. A `rows x cols` grid whose every
  cell is a contiguous `depth`-length record, addressed by the same
  `MatrixFastHash` machinery as `Vector2D`. Reach for it when a cell is made
  of several mutually coupled fields — when the estimator or the update reads
  or writes them as a unit. `Vector2D<[T; N]>` has the identical memory
  layout; what `Vector3D` adds is a `depth` chosen at run time, which is what
  a HyperLogLog precision or a sub-window count needs and what a const generic
  cannot express. See
  [Common Structures](docs/api/api_common_structures.md#when-to-reach-for-vector3d-instead-of-vector2d).

  It previously existed as a placeholder: a publicly exported type with a
  single `init(layer, row, col)` that reserved capacity, no accessors, and so
  no way to put an element in or read one out. Filling it in reshaped that
  placeholder — the three dimensions are now `(rows, cols, depth)`, the
  `serde` field names follow, and `init` rejects a zero or overflowing shape
  rather than returning a container it cannot address. Code written against
  the placeholder could only construct one and serialize an empty container,
  so there is no behaviour to migrate, and the parameter arity is unchanged,
  so such code still compiles.
- **`CountMinHll`** (`experimental` feature). Grouped distinct counting: given
  a stream of `(key, distinct_value)` pairs, how many distinct values has this
  key seen? A Count-Min-shaped grid whose every cell is a small HyperLogLog,
  held in one contiguous `Vector3D<u8>` register plane. `Hydra` with
  `HydraCounter::Hll` answers the same query on a stable API, with one
  heap-allocated HLL per cell.
- **`MicroCm`** (`experimental` feature), a sliding-window frequency sketch
  following MicroscopeSketch (Zhao et al., KDD 2023). Each cell holds `T + 2`
  byte-wide pixels and one shared zoom exponent, so the counters stay narrow
  and the exponent buys the range instead of the counters being widened for
  the largest count they might ever hold. Count-based and time-based windows
  are one `SubWindowClock` type. Not yet checked against the paper's published
  measurements.
- **Property tests for every sketch** (`tests/pbt/`, one target:
  `cargo test --test pbt`). One file per sketch, named after it, holding every
  law that sketch claims — merge algebra, path equivalence, the paper's own
  model, and the wire / ASAPv1 round trip. Shared generators, the `grid`
  reader and the `round_trip!` macro live in `tests/pbt/support.rs`; four
  modules sit behind `experimental`. 435 tests under `--all-features`, 367
  under the default feature set. Every law was mutation-checked: the
  implementation was broken deliberately, the law confirmed red, and confirmed
  not to take unrelated laws down with it; a law no semantically real mutation
  could kill was deleted rather than kept. [PBT notes](docs/pbt_notes.md) say
  what the suite is for and what it has found;
  [`tests/PBT_COVERAGE.md`](tests/PBT_COVERAGE.md) lists every law as a
  relation, one section per sketch, with what it is checked against.
- **Seeded CocoSketch.** `CocoSketch::with_dimensions_and_seed(rows, cols, seed)` draws the
  tie-break and the election from a generator seeded with `seed`, so two
  sketches built with the same seed and fed the same inserts hold identical
  tables. The OctoSketch pair has the same: `CocoSketchWorker::with_seed`,
  `CocoOctoWorker::with_threshold_and_seed`, `CocoOctoAggregator::with_seed`,
  and `CocoOctoPlan::with_seed` / `with_threshold_and_seed`, which derive each
  worker's seed and the aggregator's from one seed. The unseeded constructors
  draw from the thread generator. The seed is not serialized, and the ASAPv1
  payload is unchanged.
- **`peek_kind_id(&[u8])`** returns an ASAPv1 envelope's `kind_id` after
  checking its magic, version, and framing, without decoding the sketch.
- **`CountMinSketchDelta`**, the ASAPv1 Count-Min delta (`0x02 0x80`): built with
  `new` from changed cells, serialized like a sketch, and added with
  `CountMinSketch::merge_delta`, which changes nothing when it fails.
- **`sketches::CountSketchDelta`**, the ASAPv1 Count Sketch delta (`0x04 0x80`),
  and `f64` Count Sketch counters (`counter_type` `"f64"`): the delta is built
  with `new` from changed cells, serialized like a sketch, and added with
  `CountSketch::merge_delta`, which changes nothing when it fails.
- **`CountMinSketchWithHeapDelta`**, the ASAPv1 CountMinSketchWithHeap delta (`0x03 0x80`), and `f64`
  CountMinSketchWithHeap insert and merge, with unrounded `f64` heap counts.
- **`CountSketchWithHeapDelta`**, the ASAPv1 CountSketchWithHeap delta (`0x0a 0x80`), and unrounded
  `f64` heap counts for an `f64` CountSketchWithHeap (`CountSketchHeapCounter`).
- **`HyperLogLogDelta`**, the ASAPv1 HLL Classic and Ertl-MLE deltas
  (`0x01 0x81`, `0x01 0x82`): built with `new` from changed registers,
  serialized like a sketch, and merged register-wise by `max` with
  `HyperLogLogImpl::merge_delta`.
- **ASAPv1 for `SetAggregator` (`0x08 0x00`) and `SetAggregatorDelta` (`0x08 0x80`)**:
  `to_asapv1` / `from_asapv1` on both types.
- **`DdSketchDelta`**, the ASAPv1 DDSketch delta (`0x05 0x80`): built with
  `new` from changed buckets of both stores and a zero-count increment,
  serialized like a sketch, and added with `DdSketch::merge_delta`, which
  changes nothing when it fails.
- **`admission_rate`**, the optional ASAPv1 metadata key of a sampled HLL,
  Count-Min, Count Sketch or DDSketch state and its delta: set with
  `with_admission_rate(p)` and read with `admission_rate()` on
  `HyperLogLogImpl`, `HyperLogLogHipImpl`, `CountMinSketch`, `CountSketch` and `DdSketch`,
  and set with `with_admission_rate(p)` on the four delta types. Estimates are
  of the admitted updates; a consumer divides them by `p`. States and deltas
  merge only at equal rates. A sketch nested in another kind's state must hold
  `p = 1` to serialize.
- **`CountMinSketchWithSpaceSaving` and `CountSketchWithSpaceSaving`**: a Count-Min or Count Sketch
  matrix beside a `SpaceSaving` summary of `capacity` counters. Every update
  reaches both; `top_k(k)` ranks the summary's keys by their matrix estimate.
  `update_weighted` gives the summary the weight rounded up, or nothing when it is
  not positive (`CandidateCounter`). Merge adds the matrices and merges the
  summaries, and panics on a dimension, admission-rate or capacity mismatch.
  ASAPv1 kinds `0x1e 0x00` and `0x1f 0x00` (spec §3.24, §3.25) for `i32`,
  `i64` and `f64` `Vector2D` matrices on either path, with no delta kind.
- **`Kll::memory_bytes` and `KllDynamic::memory_bytes`**: the bytes the
  sketch holds now.
- **`Kmv::memory_bytes`**: the struct plus the hash slots the heap has
  allocated.
- **`TopKHeap::estimated_memory_bytes(heap_size, key)`**: the bytes a heap
  of that capacity holds in steady state once keys shaped like `key` churn
  through it.
- **`check_shape(rows, cols)`** on `CountMinSketch`, `CountSketch`, their
  WithHeap and WithSpaceSaving forms: the shapes their constructors build,
  both dimensions non-zero and `rows * cols` within `usize`. Each is defined
  on the type with its default parameters, so `CountMinSketch::check_shape(3, 64)`
  names no storage, path or hasher.
- **`clear`** on `CountMinSketch`, `CountSketch` and their WithHeap forms:
  zeroes the counters (and empties the heap) and returns the Nitro sampler to
  its start, keeping shape and allocation. With it, `Nitro::reset` and
  `MatrixStorage::reset_sampler` (a no-op by default; `Vector2D` resets its
  Nitro).
- **`memory_bytes`** on `CountMinSketch` and `CountSketch`: the struct plus
  its counters. **`CountMinF64`** is re-exported from the crate root and
  `sketches`.
- **`TopKHeap::memory_bytes`**, and **`memory_bytes` and
  `estimated_memory_bytes(rows, cols, heap_size, key)`** on
  `CountMinSketchWithHeap` and `CountSketchWithHeap`.

### Changed

- **`UnivMon::estimate_g_sum` and `UnivMonTwoTier::estimate_g_sum` take
  `count_every_key`** (breaking), was `heavy_only`. `true` now counts every
  heap key; `heavy_only = true` applied the `L2 / sqrt(heap_size)` threshold.
- **The legacy MessagePack wrappers leave the crate root** (breaking).
  `CountMinSketch`, `CountMinSketchDelta`, `CountMinSketchWithHeap`,
  `CmsHeapItem`, `CountSketch`, `CountSketchDelta`,
  `COUNT_SKETCH_TOPK_CAPACITY`, `CountSketchWithHeap`, `CsHeapItem`,
  `DdSketch`, `DdSketchDelta`, `DDSKETCH_GROW_CHUNK`, `HllSketch`,
  `HllSketchDelta`, `HllVariant`, `HydraKllSketch`, `KllSketch`,
  `KllSketchData` and the sampling helpers are imported from
  `message_pack_format::portable::...`; `MessagePackCodec` from
  `message_pack_format`. `CountSketchDelta` at the root is the ASAPv1
  Count Sketch delta.
- **Every sketch write is `update`** (breaking). A write is named
  `update[_weighted][_batch][_at | _emit_delta | ...][_with_hash | _with_hashes |
  _with_row_hashes][_with_threshold | _and_threshold]`: `insert` -> `update`,
  `insert_many` -> `update_weighted`, `bulk_insert` -> `update_batch`,
  `bulk_insert_many_with_hashes` -> `update_weighted_batch_with_hashes`, and so
  on. The cases the rule does not spell out: `tumbling_insert` ->
  `tumbling_update_weighted`, `insert_cached_step` ->
  `update_batch_cached_step`, `insert_hashes_emit_*` ->
  `update_emit_*_with_row_hashes`, `CocoBucket::add_v` -> `add`. Weight
  parameters are named `weight`. Custom `sketch_catalog` and
  `TumblingWindowSketch` impls break.
- **Acronyms are one word in type names** (breaking). `KLL`, `KLLDynamic`,
  `KLLConfig` -> `Kll`, `KllDynamic`, `KllConfig`; `DDSketch`, `DDSketchDelta`
  -> `DdSketch`, `DdSketchDelta`; `KMV` -> `Kmv`; `HyperLogLogHIP*` ->
  `HyperLogLogHip*`; `MicroCM` -> `MicroCm`; `EHBucket` -> `EhBucket`.
- **ASAPv1 codec methods are `to_asapv1` / `from_asapv1`** (breaking), from
  `serialize_to_bytes` / `deserialize_from_bytes`. `CountMinHll` and `MicroCm`
  keep the old names: they write raw-serde MessagePack. The kind_id constants
  live in one crate-private `kind` module.
- **Count-Min and Count Sketch names** (breaking). `CountMin` ->
  `CountMinSketch`, `Count` -> `CountSketch`, `CountMinDelta` ->
  `CountMinSketchDelta`, `CMSHeap`/`CSHeap` (and deltas) ->
  `CountMinSketchWithHeap`/`CountSketchWithHeap`, `CMSSpaceSaving`/
  `CSSpaceSaving` -> `CountMinSketchWithSpaceSaving`/
  `CountSketchWithSpaceSaving`, `FoldCMS`/`FoldCS` (and configs) ->
  `CountMinSketchFold`/`CountSketchFold`; modules `countminsketch_heap`,
  `countsketch_heap`, `count_min_fold`, `count_sketch_fold`. `cms()`/`cs()` ->
  `count_min_sketch()`/`count_sketch()`; `top_k` -> `heap_size`; Fold
  `new_full` -> `unfolded`, `query` -> `estimate`; `FoldCell::insert`/`query`
  -> `add`/`get`; `fast_insert_with_hash_value` -> `update_with_hash`,
  `fast_insert_many_with_hash_value` -> `update_weighted_with_hash`,
  `fast_estimate_with_hash` -> `estimate_with_hash`, `fast_insert_nitro` ->
  `nitro_update`; `CsHeapCounter` -> `CountSketchHeapCounter`,
  `cs_heap_count` -> `heap_count_from_estimate`; `QUICKSTART_ROW_NUM`/
  `QUICKSTART_COL_NUM` -> `QUICKSTART_ROWS`/`QUICKSTART_COLS`.
- **`CountL2HH` -> `CountSketchWithL2Norm`** (breaking), with
  `update_weighted*`, `estimate*`, `l2()`, `l2_squared()`, `seed_index` and
  `with_dimensions_and_seed_index`; `l2hh_cell_for_row` ->
  `CountSketchWithL2Norm::cell_for_row`. It lives in module
  `countsketch_l2_norm`, and the single-variant `L2HH` enum is gone:
  `UnivMon` and `UnivMonTwoTier` `layer_sketches` hold `CountSketchWithL2Norm`
  directly.
- **Elastic and Coco** (breaking). `Elastic` -> `ElasticSketch`, `Coco` ->
  `CocoSketch`, `CocoWorkerSketch` -> `CocoSketchWorker`; `bktlen` ->
  `heavy_buckets`, `init_with_length` -> `with_heavy_buckets`,
  `init_with_dimensions` -> `with_heavy_and_light`, `query` -> `estimate`;
  Coco `w`/`d` -> `cols`/`rows`, `estimate_key` -> `estimate`. Coco
  constructors take `(rows, cols)`: `init_with_size(w, d)` ->
  `with_dimensions(rows, cols)`, `init_with_size_and_seed(w, d, seed)` ->
  `with_dimensions_and_seed(rows, cols, seed)`, and `CocoSketchWorker::new` /
  `with_seed` and the `CocoOctoWorker`, `CocoOctoAggregator` and
  `CocoOctoPlan` constructors swap their first two arguments the same way.
  `CocoSketchWorker::width()`/`depth()` -> `cols()`/`rows()`.
- **KLL, CDF and DDSketch** (breaking). KLL `init`/`init_with_seed`/
  `init_kll`/`init_kll_with_seed` -> `new`/`with_seed`/`with_k`/
  `with_k_and_seed`; `wire_*` accessors -> `level_bounds_top_down()`,
  `items_top_down()`, `coin_state()`, `num_levels()`, `m()`; `Cdf::quantile`/
  `query` (and `_li`) -> `normalized_rank`/`value_at` (and `_interpolated`);
  DDSketch `store_counts()`/`store_offset()` -> `positive_store_*`; `DdStore`
  -> `DdSign`.
- **HLL and KMV** (breaking). `ErtlMLE` -> `Ertl`; `REGISTER_BITS` ->
  `RANK_HASH_BITS`, `P_MASK` -> `INDEX_MASK`; `HllBucketList*` ->
  `HllRegisters*`, `impl_hll_bucket_list!` -> `impl_hll_registers!`; KMV
  `k_vals` -> `retained_hashes`.
- **UnivMon** (breaking). `UnivMonPyramid` -> `UnivMonTwoTier` (module
  `univmon_two_tier`), `UnivSketchPool` -> `UnivMonPool`, `EHUnivOptimized`
  -> `ExponentialHistogramUnivMon` (module `eh_univmon`) and its bucket and
  result types; `layer_size`/`sketch_row`/`sketch_col` -> `layers`/`rows`/
  `cols`; `bucket_size` -> `total_weight`; `l2_sketch_layers`/`hh_layers` ->
  `layer_sketches`/`layer_heaps`; `init_univmon` -> `new`; `fast_insert` ->
  `update_weighted_terminal`; `free` -> `clear`; `calc_*` -> `estimate_*`;
  `bottom_layer_for_hash` -> `UnivMon::bottom_layer_for_hash`;
  `UnivMonInput` -> `KeyedLayerHashes`; `LayeredCountDelta` ->
  `UnivMonCellDelta`; `L2hhWorkerSketch` -> `CountSketchWithL2NormWorker`;
  `MAX_LAYER_SIZE` -> `UNIVMON_MAX_LAYERS`; UnivMon-Q config fields `layers`,
  `rows`, `top_layer_cols`, `cols_halving_period`, `heap_size`, `seed_index`,
  `estimate_distinct` -> `estimate_cardinality`, `UnivMonQPoint` ->
  `UnivMonQCdfEntry`, `OrderedQueryDiagnostics` ->
  `UnivMonQOrderedQueryDiagnostics`.
- **Hydra** (breaking). `row_num`/`col_num` -> `rows`/`cols`, `sketches` ->
  `cells`, `type_to_clone` -> `prototype`, `HydraCounter` variants
  `CountMinSketch`/`CountSketch`/`UnivMon`/`Hll`/`Kll`, `query_quantile` ->
  `query_cdf`.
- **Exponential histogram** (breaking). `EHSketchList` -> `EhBucketSketch`
  (module `eh_bucket_sketch`) with variants named after their sketches;
  `type_to_clone` -> `prototype`; `EhBucket::to_merge` -> `merge`;
  `query_interval_merge` -> `query_interval`.
- **Octo** (breaking). Promotion messages `CountMinSketchCellDelta`,
  `CountSketchCellDelta` (and keyed forms), `HllRegisterDelta`,
  `DdSketchBucketDelta`; their `value` -> `count` (`register_value`,
  `votes`), `HllRegisterDelta.pos` -> `index`; `*_PROMASK` ->
  `OCTO_*_DEFAULT_THRESHOLD`, `MAX_PROMASK` -> `OCTO_MAX_PROMOTION_THRESHOLD`;
  workers, aggregators and plans named after their sketch
  (`CountMinSketchOctoWorker`, ...); `*WorkerSketch` -> `*Worker`.
- **Heaps and keys** (breaking). `HeapItem` -> `OwnedDataInput`, `ISIZE`/
  `USIZE` -> `Isize`/`Usize`; `HHItem` -> `HeapEntry` (`new` ->
  `from_input`, `create_item` -> `new`); `HHHeap` -> `TopKHeap` (`k` ->
  `heap_size`); `TopKCounter` -> `HeapCounter`, `SpaceSavingCounter` ->
  `CandidateCounter`; `KeepSmallest`/`KeepLargest` -> `RetainLargest`/
  `RetainSmallest`; `KeyColumn::Str` -> `KeyColumn::String`.
- **`DeltaResult` -> `SetAggregatorDelta`** (breaking).
- **Error, panic and label strings name the new types, methods and fields**
  (breaking for code that matches on them). `sketch_type()` reports
  `"CountMinSketch"`, `"CountSketch"` and `"CountSketchWithL2Norm"` for
  `EnsembleSketch` and `EhBucketSketch`; UnivMon-Q config errors name
  `layers`, `top_layer_cols`, `rows` and `heap_size`. ASAPv1 bytes are
  unchanged.
- **Hashing and seeds** (breaking). `hash_item64_seeded`/`hash_item128_seeded`
  -> `hash64_seeded_owned`/`hash128_seeded_owned`; seed-table index
  parameters are `seed_index`; `CANONICAL_HASH_SEED` ->
  `DEFAULT_CANONICAL_SEED_INDEX`, `HYDRA_SEED` -> `HYDRA_SEED_INDEX`,
  `BOTTOM_LAYER_FINDER` -> `UNIVMON_BOTTOM_LAYER_SEED_INDEX`, `SEEDLIST` ->
  `SEED_LIST`; `MatrixHashMode` -> `MatrixHashLayout`, `MatrixHashType` ->
  `MatrixHash`, `hash_mode_for_matrix` -> `hash_layout_for_matrix`.
- **Storage** (breaking). `Vector1D::init` -> `with_capacity`;
  `Vector2D`/`Vector3D::init` -> `unfilled`; `Vector3D` `depth` ->
  `bucket_len`.
- **Nitro and sampling** (breaking). `init_nitro*` -> `new`/`with_seed`;
  `get_sampling_rate` -> `sampling_rate`; `Vector2D::get_delta` ->
  `nitro_weight`, `get_nitro_skip` -> `nitro_skip`; `PRECOMPUTED_SAMPLE*` ->
  `LN_ONE_MINUS_U_TABLE*`.
- **Getters drop `get_`** (breaking): `max_time`, `min_time`, `memory_info`,
  `mask_bits`, `required_bits`.
- **KLL, UnivMon and Hydra metadata keys follow the spec.** Compact KLL
  writes `compaction_seed`; UnivMon writes `layers`, `rows` and `cols`; Hydra
  writes `cell_*` for its counter params (`cell_rows`, `cell_cols`,
  `cell_counter_type`, `cell_mode`, `cell_precision`, `cell_k`, `cell_m`,
  `cell_item_type`, `cell_layers`, `cell_heap_size`, `cell_key_type`). Bytes
  with the old keys are rejected; the affected goldens are regenerated.
- **UnivMonTwoTier and UnivMon-Q metadata keys follow the spec.**
  UnivMonTwoTier writes `layers`, `elephant_rows`, `elephant_cols`,
  `mouse_rows` and `mouse_cols`; UnivMon-Q writes `layers`, `top_layer_cols`,
  `cols_halving_period`, `rows` and `heap_size`. Bytes with the old keys are
  rejected.
- **An empty heap or summary must carry `key_type` `"u64"`.** CountMinSketchWithHeap, CountSketchWithHeap,
  their deltas, UnivMon, UnivMonTwoTier, Hydra's UnivMon counter,
  Space-Saving, CountMinSketchWithSpaceSaving and CountSketchWithSpaceSaving reject one under any other
  name.
- **CountMinSketchWithHeap, CountSketchWithHeap, their deltas, UnivMon, UnivMonTwoTier and Hydra's
  UnivMon counter read a `"string"` key from msgpack `str` alone**; UTF-8
  `bin` keys relabelled `"string"` are rejected.
- **Space-Saving and UnivMon-Q ASAPv1 decodes reject entries out of emitted
  order.** Space-Saving also rejects a key held twice judged by its bits, so
  two `NaN` keys fail on both doors.
- **`HyperLogLogImpl::merge_delta` returns `Result<(), String>`**, an `Err`
  when the delta's admission rate differs from the state's.
- **`EhBucketSketch::merge` and `EhBucket::merge` return `Result<(), String>`**,
  passing the inner sketch's error through.
- **The derived serde form of `HyperLogLogImpl`, `HyperLogLogHipImpl`,
  `CountMinSketch`, `CountSketch` and `DdSketch` gains a trailing `admission_rate` field**,
  `1` when absent and refused outside `(0, 1]`.
- **DDSketch has one ASAPv1 encoding**: `metadata_version` 1 with all eight
  payload fields, the negative store and zero count included. Bytes change for
  positive-only and empty sketches, and `metadata_version` 2 is rejected.
- **The CountMinSketchWithHeap and CountSketchWithHeap ASAPv1 metadata key `k` is renamed `heap_size`**,
  and the CountMinSketchWithHeap, CountSketchWithHeap and UnivMon decoders reject heap entries out of
  emitted order. `HeapEntry` and `TopKHeap` take a count type, `i64` by default.
- **ASAPv1 decoders reject a block with bytes after its msgpack value, and a
  float field that is not msgpack float64** (an `f32` key: not float32).
- **The ASAPv1 golden fixtures live in their own repo**,
  [`sketchlib-golden-bytes`](https://github.com/ProjectASAP/sketchlib-golden-bytes), mounted
  as the git submodule `asapv1_golden/`. The bytes are unchanged. `cargo test`
  needs the submodule: clone with `--recursive` or run
  `git submodule update --init`.
- `HyperLogLog`'s classic estimator body is now a shared function that
  `CountMinHll` also uses, so the two cannot drift. HyperLogLog's own numbers
  are unchanged.
- **The end-to-end suites are one target.** `tests/e2e_*.rs` are now
  `tests/e2e/*.rs` under a single `e2e` target, so `tests/common/` is compiled
  once for all of them instead of once per suite. `cargo test --test e2e` runs
  the set; a single suite is a filter, `cargo test --test e2e frequency::`.
- Two doc comments described behaviour the code does not have, and now say
  what it does: `TopKHeap::update`'s return value reports whether the key took
  its place without displacing another, not whether every key offered so far
  is retained; `TumblingWindow::update_weighted` closes a period that saw no arrivals
  as an empty window like any other, and that window takes a place in
  `max_windows`.

- **Duplicates are merged** (breaking). `UnivMonQ::add` and `update` are one
  generic `update<T: NumericalValue>(&T)`; `UnivMon::calc_g_sum_heuristic` is
  gone and `estimate_g_sum` is the one g-sum. `QUICKSTART_ROWS` /
  `QUICKSTART_COLS` and the default 3 x 4096 geometry are defined once in
  `common::structures::fixed_structure`, and the default heap size (32) and
  KLL's largest `k` once each.

- **Key conversions are methods** (breaking). `heap_item_to_sketch_input(&k)`
  -> `k.as_data_input()` on `OwnedDataInput`, `input_to_owned(&k)` ->
  `k.to_owned_input()` on `DataInput`.
- **`UnivMonPool` lives in `sketch_framework::univmon`** beside `UnivMon`;
  the crate-root path is unchanged.
- **`heap_count_from_estimate` is crate-private** (breaking);
  `CountSketchHeapCounter::heap_count` is the public form.
- KLL's default `m` is the constant `kll::DEFAULT_M` (8), which `with_k` and
  `with_k_and_seed` use. `Vector2D::filled(rows, cols, value)` builds a filled
  matrix in one call. `Vector2D::nitro_skip` takes `&self`.

- Docs state what the code does: `FastPath` / `RegularPath`, `MatrixFastHash`
  and `FastPathHasher` describe the hashing they select; UnivMon's
  `total_weight` is the total inserted weight; UnivMon's `estimate_entropy` is
  in bits and UnivMon-Q's in nats. Spec §3.15 / §3.16 rule 3 bounds `layers`
  by 19 (`UNIVMON_MAX_LAYERS`), the seed-table ceiling the code enforces.
- **`Kll` allocates its item buffer as items arrive.** The buffer starts
  empty, then holds 16 items and doubles up to the sketch's full capacity; a
  decoded sketch holds just its retained items. Retained items, coin tosses,
  quantiles, merges and ASAPv1 bytes do not depend on the buffer's size.
- **`Kmv::estimate` takes `&self` and `Kmv::merge` takes `&Kmv`.** A full
  sketch turns away a hash at or above its largest retained one before
  scanning for a duplicate; the retained set is unchanged.
- **`UnivMonQ::estimated_memory_bytes(&self)` is `UnivMonQ::memory_bytes`**
  (breaking) and counts what the instance holds now. Across the crate,
  `memory_bytes(&self)` is what an instance holds and
  `estimated_memory_bytes(<shape>)` prices a shape before building.
- **`CountMinSketch`, `CountSketch`, their WithHeap and WithSpaceSaving forms
  refuse a zero dimension and a counter count past `usize`** (breaking): their
  constructors and `from_storage` panic on such a shape.
- **Seed index `i` hashes with `seed_for_index(i)`** (new, public):
  `SEED_LIST[i % 20] * (i / 20 + 1) + i / 20` in wrapping `u64` arithmetic.
  Indexes below 20 keep their seed; an index past 20 draws its own, so matrix
  rows past 20 hash independently and UnivMon-Q's occurrence priorities hash at
  their own seed. Count-Min, Count Sketch, their WithHeap and WithSpaceSaving
  forms, Elastic, Hydra and UnivMon build any row count in memory; `to_asapv1`
  refuses more than `MATRIX_MAX_ROWS`.
- **ASAPv1 `seed_derivation` is `"seed_list_index_scaled"`** (breaking, wire),
  naming `seed_for_index`. Bytes carrying `"seed_list_index_wrap"` no longer
  decode. The golden fixtures are regenerated.

### Removed

- **Protobuf** (breaking). Module `proto`, the `.proto` sources,
  `tools/gen-proto` and the `prost`/`bytes` dependencies; with them the KLL
  value-offset sample encoding.
- **Legacy MessagePack** (breaking). Module `message_pack_format` with
  `portable`, `native` and `MessagePackCodec`: the wrapper types
  `CountMinSketch`, `CountSketch`, `CountMinSketchWithHeap`,
  `CountSketchWithHeap`, `KllSketch`, `HllSketch`, `DdSketch`,
  `HydraKllSketch`, their deltas and helpers, and the `xxhash-rust`
  dependency. `SetAggregator` and `SetAggregatorDelta` move to
  `sketches::set_aggregator`; the ASAPv1 framing is private.
- **serde on sketch types** (breaking). Sketches, their storage and their
  inputs no longer implement `Serialize` / `Deserialize`; ASAPv1 is the one
  serialized form. `CountMinHll` and `MicroCm` lose
  `serialize_to_bytes` / `deserialize_from_bytes`, and the
  `serde-big-array` dependency goes.
- **Dead and duplicate APIs** (breaking). `Kll::wire_k` (use `k()`);
  `PRECOMPUTED_SAMPLE_RATE_1PERCENT` and module `common::precompute_sample2`;
  module `common::hashspec` (`HashSpec`, `derive_index`, `derive_sign`,
  `CANONICAL_HASH_SEED_TABLE`); `Vector2D::get_row`/`get_col` (use
  `rows()`/`cols()`); `HeapEntry::init_item` (use `from_input`);
  `CountSketchWithL2Norm::fast_update_and_est_without_l2`.

### Fixed

- `UnivMonTwoTier` gets `UnivMon`'s estimator fixes: `estimate_cardinality`
  counts every heap key, `estimate_l2` reads layer 0's Count Sketch F2 estimate
  (terminal mode: the summed strata's), and new `top_layer_complete()`.
- `UnivMon::estimate_cardinality` counts every heap key, zero-weight keys and
  keys with a nonpositive Count Sketch estimate included. `estimate_l2` reads
  layer 0's Count Sketch F2 estimate (terminal mode: the summed strata's), not
  the heap recurrence. New `top_layer_complete()`; the error bounds, stated
  jointly with `top_layer_complete()`, are in `docs/api/api_univmon.md`.
- `KLLDynamic` panicked on every quantile query once a NaN had entered the
  stream: the CDF was sorted with `partial_cmp(..).unwrap()`. It sorts with
  `total_cmp`, as the three sibling sorts beside it already did.
- `OctoRuntime::flush` could return before the aggregator had applied every
  worker's flushed deltas, so a live read right after it could miss inputs. A
  drain still pending when the workers stop is answered.

## [0.3.0] - 2026-09-07

Breaking release. Adds `Bloom` and `SpaceSaving`, completes ASAPv1 wire
coverage for every implemented sketch, and corrects the Nitro row sampler.
The wire format and the `serde` shape of several types changed; see
**Changed** below for the payloads and types affected.

### Added

- **`UnivMonQQuery::ordered_query_diagnostics`**, returning
  `OrderedQueryDiagnostics`. A read-only view of the heavy set the ordered CDF
  actually used, the mass it credited to it, and the number of retained
  occurrence samples backing the residual — the `E_H`, `P_hat_R` and `m_R` of
  the documented bound `sup_x |F_hat(x) - F(x)| <= 2 E_H + P_hat_R * eps_R`.
  None of the three was reachable from outside the crate, so the bound could
  only be checked in the diffuse special case where the heavy set is empty; it
  is now verified in full. Purely observational: it reports state the CDF
  construction already computes, and changes no answer and no wire format.
- **`cs_heap_count`**, naming the `f64 -> i64` conversion `CSHeap` uses when it
  writes a Count Sketch estimate into its top-k heap. The conversion saturates
  at `i64::MAX` / `i64::MIN` (and maps `NaN` to `0`) rather than wrapping, which
  matters for the `i128`-backed instances: they accept `insert_many(key, i128)`
  and really can hold counts past `i64::MAX`. Making it a named, documented
  function rather than an inline cast means the choice cannot change silently.

- **`Bloom`, a partitioned Bloom filter.** `rows` slices of `cols` bits, one
  slice per hash function, over a new packed `BitMatrix` that implements
  `MatrixStorage` — so the filter probes the same `rows x cols` shape
  `CountMin` does and a membership query is the row-wise minimum, which over
  single bits is their AND. `with_capacity(n, p)` caps the slice count at
  `BLOOM_MAX_SLICES` (the seed list's length, since two slices seeded from the
  same entry are identical) and solves the slice width for that count, so
  `predicted_fpp` never claims a rate the filter cannot deliver; each slice is
  rounded up to a power of two, which removes the column fold's modulo bias.
  Every door holds the slice bound: `with_dimensions` panics past it and both
  serialized forms reject it, so a filter that cannot be stored cannot be built
  either. Total bits are capped at `BLOOM_MAX_BITS`, and a non-finite target
  rate is rejected. Union is exact, so the filter shards without loss. The
  `BloomMode` marker tags the hash path on the wire, so bytes written by one
  path do not decode into the other.
- **`SpaceSaving`, a fixed-counter heavy-hitter summary.** The paper's
  Stream-Summary: count-ordered buckets in a doubly linked list, each owning a
  doubly linked list of its counters, plus a key index — so a unit arrival moves
  one counter to the neighbouring bucket and an eviction takes the head of the
  lowest bucket, constant work at any capacity. Both lists are index arenas
  rather than pointers. A monitored key is sandwiched by its own error, and
  `upper_bound` never reads below the truth for any key in the stream: the
  summary carries the largest count known to have left it, so the ceiling
  survives a merge that leaves it holding fewer keys than its capacity. Counts
  saturate rather than wrap, and `merge` picks the same survivors on every
  run. Only the monitored `(key, count, error)` triples reach the wire; the
  arena is rebuilt on load and a payload that does not describe a valid summary
  is rejected.
- **`BitMatrix` in `common::structures`.** A packed one-bit-per-cell grid behind
  the `MatrixStorage` interface. It carries `words`, `rows` and `cols` on the
  wire and recomputes the word stride and column mask on load; out-of-range
  coordinates panic rather than aliasing into a neighbouring row.
- **`DigestHasher` in `common::hash`.** A `Hasher` for `u64` keys that are
  already digests: it replaces the full byte-wise hash with a finalizing mix,
  which is what a table index still needs once the digests come from a fixed
  seed list. `DigestBuildHasher` is its `BuildHasher`.
- **ASAPv1 wire serialization for `Bloom` (`0x17 0x00`) and `SpaceSaving`
  (`0x18 0x00`).** `serialize_to_bytes` / `deserialize_from_bytes` on each,
  through the shared envelope, with the metadata derived from the hasher's
  `HashProfile` as Count-Min's and HLL's are. Bloom's payload is the packed
  words and the insert count; its wire covers the geometries `with_capacity`
  produces, on both sides, so it never emits bytes it would refuse to read.
  Space-Saving's payload is the monitored `(key, count, error)` triples plus
  `total` and the dropped-count ceiling — the bucket list, counter arena and key
  index are rebuilt on load, so no crafted payload can point an arena index out
  of bounds or into a cycle. Its `key_type` names the exact `HeapItem` variant
  and is never widened, since the variant is part of a key's identity while the
  digest is blind to it; a mixed-variant or 128-bit-keyed summary refuses to
  serialize. Entries are emitted in a defined order, so equal summaries encode
  to equal bytes. Both payloads are specified in `docs/asapv1_wire_format.md`
  §3.4 and §3.5.
- **`membership_battery` in the conformance kit.** A new `MembershipOps`
  capability with the exact no-false-negative check, a false-positive-rate
  ceiling and a band around `predicted_fpp`, since the existing batteries are
  all frequency- or numeric-shaped.
- **ASAPv1 wire payloads for every remaining implemented sketch.** `CMSHeap`
  (`0x03 0x00`), `DDSketch` (`0x05 0x00`), `Hydra` over each of its five
  counters (`0x07 0x00`-`0x07 0x04`), `CSHeap` (`0x0a 0x00`), `Elastic`
  (`0x0b 0x00`), `Coco` (`0x0c 0x00`), `UniformSampling` (`0x0d 0x00`), `KMV`
  (`0x0e 0x00`), `UnivMon` (`0x10 0x00`), `UnivMon Optimized` (`0x11 0x00`),
  `ExponentialHistogram` (`0x13 0x00`), `EHSketchList` (`0x14 0x00`),
  `CountL2HH` (`0x19 0x00`) and `UnivMon-Q` (`0x1a 0x00`) each serialize through
  the shared envelope, with a closed metadata schema and a positional payload
  specified in `docs/asapv1_wire_format.md` Section 3. A nested sketch is
  inlined rather than wrapped in an envelope of its own, except where the nested
  algorithm is data rather than a type: an `EHSketchList` carries the variant's
  own `kind_id`, metadata block and payload block with the framing stripped, so
  that variant's own decoder validates it. Every decoder fails closed — a
  declared capacity never sizes an allocation, every geometry is bounds- and
  overflow-checked before anything is sized from it, and a container whose
  order does not survive a rebuild has its emitted order pinned, so a decoded
  sketch re-serializes byte-identically.
- **Seeded constructors for the sketches whose randomness was wall-clock or
  OS-drawn.** `KLLDynamic::init_with_seed` / `init_kll_with_seed`,
  `NitroBatch::with_target_and_seed` / `init_nitro_with_seed`,
  `KllSketch::with_seed` (and the free `new_sketchlib_kll_with_seed`) and
  `HydraKllSketch::with_seed`, each mirroring the unseeded constructor beside
  it. A KLL's compaction coin and a Nitro batch's geometric skip draw are the
  only randomness those types hold, so fixing them makes the sketch a
  deterministic function of its input — which is what reproducible replay,
  cross-process parity and any assertion of an accuracy bound require.
  `KLLDynamic` stores its seed so `clear()` re-seeds from it, as `KLL` already
  did; the seed is not part of the wire format, since it describes how a sketch
  was built rather than what it holds, so encoded bytes are unchanged.
  `HydraKllSketch::with_seed` shares one seed across every cell.

### Changed

- **BREAKING (wire format):** `serialize_to_bytes` / `deserialize_from_bytes`
  emit and read the ASAPv1 envelope for every sketch the `kind_id` registry
  marks implemented. Bytes produced by the earlier serde-derived form of those
  types **do not decode**; there is no legacy read path, since a new encoding
  takes a new `kind_id` rather than a payload version field. Golden byte-vector
  fixtures exist for HLL, Count-Min, Count Sketch and compact KLL only, so the
  other kinds have no cross-language drift guard yet and `portable` stays in
  place until they do.
- **BREAKING (ASAPv1 envelopes for matrix sketches past 20 rows):** every
  payload whose rows are seeded per row now rejects a matrix with more rows than
  `SEEDLIST` has seeds, on both the encode and the decode side. Seed indices
  wrap at the seed list, so row `r` and row `r + 20` draw the same seed, fold
  every key into the same column and hold identical cells — a wider matrix buys
  storage and a hash, not independence. The new public `MATRIX_MAX_ROWS` names
  the bound and `BLOOM_MAX_SLICES` is now defined as it, so the two names share
  one definition. Covered: Count-Min (`0x02 0x00`), CMSHeap (`0x03 0x00`),
  Count Sketch (`0x04 0x00`), Hydra's grid and matrix counters (`0x07 0x01` /
  `0x07 0x02`), CSHeap (`0x0a 0x00`), Elastic's light layer (`0x0b 0x00`),
  Coco's table (`0x0c 0x00`), UnivMon (`0x10 0x00`) and UnivMon Optimized
  (`0x11 0x00`) layers, and CountL2HH (`0x19 0x00`) — Bloom (`0x17 0x00`)
  already drew this line. UnivMon-Q (`0x1a 0x00`) is unaffected: its rows are
  bit fields of one 128-bit hash, bounded by that budget instead. A wider
  counter matrix is still buildable in memory; it no longer serializes, and
  Bloom refuses one at construction.
- **Made `HHHeap::update` independent of capacity.** The key index was rebuilt
  in full after every accepted update, cloning each resident's key, so the top-k
  structure behind `CMSHeap`, `CSHeap`, `FoldCMS`, `FoldCS`, `UnivMon` and the
  Octo aggregator cost `O(k)` per insert. It now holds heap indices only,
  re-checking identity against the heap, and patches them through each sift:
  `CommonHeap` gained `push_back_with`, `replace_root_with` and
  `update_at_with`, which report every swap to a caller-supplied closure, while
  `push` and `update_at` delegate with a no-op and are unchanged for every other
  caller. A parallel `slots` vector carries each resident's digest so a sift
  re-hashes nothing, and the index is keyed by `DigestHasher` rather than
  running SipHash over a value that is already an xxh3 digest. Measured on a
  Zipf(1.1) stream, `HHHeap::update` goes from 4.75 to 45.0 Mups/s at capacity
  8 and from 0.01 to 52.9 at capacity 2048 — flat in `k` rather than halving
  with each doubling — and `CMSHeap::insert` at top_k=2048 goes from 0.009 to
  27.0 Minsert/s. Retention is unchanged: a differential test compares the heap
  element for element against the rebuild implementation at capacities 0 through
  257 over 30k updates on both key forms.
- **BREAKING (positionally encoded `HHHeap` and `UnivMon` state):** `HHHeap` no
  longer serializes its key index, which is derived data rebuilt on load. The
  serialized form went from `{heap, positions, k}` to `{heap, k}`. A named-map
  encoding written by an earlier version still decodes, since the extra key is
  skipped; a positional encoding of the three-field form does not. Nothing
  in-crate writes the positional form — the portable MessagePack wire for the
  top-k sketches carries a `(key, value)` list and rebuilds through `update`,
  and no golden covers a top-k envelope.
- **BREAKING (`serde` shape of `Nitro`, and therefore of `Vector2D` and any
  sketch embedding one):** `Nitro::rounding_state` is now a serialized field,
  appended after `mask`, so a sketch resumed from a decode continues its weight
  sequence instead of restarting the rounding stream from a constant. It
  carries `#[serde(default)]`, so a payload written before this field existed
  decodes unchanged in both map and array encodings. A *new* payload read by an
  *older* build decodes only in a map encoding (`rmp_serde::to_vec_named`,
  `serde_json`), where the unknown key is skipped; a positional encoding gains
  one trailing element. **The ASAPv1 wire format is unaffected**: no ASAPv1
  payload has ever carried Nitro state — `CountMin` and `Count` envelopes
  serialize the counter slice and the metadata block only — so every
  `serialize_to_bytes` / `deserialize_from_bytes` byte string and every golden
  fixture is unchanged.
- **BREAKING (`Nitro::admit_rows`, `Nitro::table_cursor`,
  `Nitro::skip_table_len`, `NitroBatch::table_cursor`,
  `NitroBatch::skip_table_len`):** removed from the public API.
  `admit_rows` is now `pub(crate)` and takes a
  `SmallVec<[(usize, u64); MATRIX_MAX_ROWS]>`; the cursor and table-length
  accessors existed only so an integration test could read private state, and
  the tests that used them are now unit tests in the modules that own it. The
  seeded constructors — `enable_nitro_with_seed`, `init_nitro_seeded`,
  `with_target_and_seed`, `init_nitro_with_seed` — are unaffected: they answer
  a real need to reproduce a sampling run.
- **`CountMin::fast_insert_nitro` and `Count::fast_insert_nitro` no longer
  allocate per observation.** Each call built a `Vec` for the admitted rows.
  They now collect into a `SmallVec` inlined to `MATRIX_MAX_ROWS`, so no matrix
  the hash family supports reaches the heap, at any sampling rate. The hash is
  computed once per observation and both paths share the one admission walk in
  `Nitro::admit_rows`.
- **BREAKING (external implementors of `MatrixFastHash`):** the trait gained a
  required `row_hash(row, mask_bits, mask)` method, split out of
  `col_for_row` so storages can decode against precomputed parameters and
  skip `% cols` for power-of-two column counts. The three in-crate impls
  (`MatrixHashType`, `u64`, `u128`) are updated; downstream code implementing
  this trait directly must add the method. Ship in the next `0.y` bump
  (Cargo convention: `y` is the major component pre-1.0).

### Fixed

- **The row-level Nitro sampler never advanced its skip-table cursor, and its
  schedule ignored the configured rate.** `Nitro::draw_geometric` advanced the
  cursor with `(idx + 1) & mask` where `mask` was the table's *length*
  (`0x10000`) rather than `length - 1`; `x & 0x10000` is zero for every `x`
  below `0xFFFF`, so `idx` stayed at 0 for the life of the sketch. Every skip
  was the same distance, and the stochastic-rounding draw derived from that
  cursor was a constant — which meant the `p = 0.3` weight bias the previous
  release claimed to fix was still `ceil(1/p)` on this path. The same function
  also read a table pre-divided by `ln(0.99)`, so every sampling rate got the
  schedule for `p = 0.01`: at `p = 0.5` it admitted about 1% of the stream while
  weighting each admission as if it were 50%. The cursor now wraps on the
  table's real length (and folds an out-of-range value decoded from an old
  payload back into range), the schedule multiplies the unscaled `ln(1 - u)`
  table by `1 / ln(1 - p)`, and the rounding draw comes from a separate
  splitmix64 stream (`Nitro::rounding_state`) so it cannot phase-lock to the
  skip schedule. `NitroBatch::insert_cached_step` had the identical mask and
  rate bugs and is fixed the same way.
- **Nitro's insert and query derived cells from different hash forms, so every
  per-key estimate read 0.** `CountMin::fast_insert_nitro`,
  `Count::fast_insert_nitro` and `NitroBatch`'s own insert hashed with
  `hash128_seeded(0, value)` and sliced columns out of that raw `u128`, while
  `nitro_estimate`, `Count::estimate` and `NitroBatch::estimate_median` query
  through `FastPathHasher::hash_for_matrix` and `MatrixFastHash::col_for_row`.
  Those agree only when the matrix hash is the identity on the raw hash, which
  it is not — the packing mode is chosen from `rows` and `cols`. Every insert
  path now derives cell and sign exactly as a plain `insert` does. Separately,
  `NitroBatch` had assigned each sampled record to a single row
  (`position % rows`), which divided every per-row counter by the depth; a
  sampled record now reaches every row, which is what makes the estimator
  unbiased. Update weights saturate into the counter's domain
  (`nitro_delta_saturated_i32` / `_u32`) instead of wrapping into a decrement,
  reachable at rates below ~4.7e-10 or by writing the public `delta` field.
- **The row-level Nitro admission walk underflowed once skips became small.**
  The carry `(r + to_skip + 1) - rows` is negative on `usize` whenever the next
  admitted slot falls inside the same update — the common case at any rate above
  roughly `1/rows`, reachable only once the schedule above was rate-correct. The
  Count-Min path also admitted at most one row per update and dropped any
  further landings. Both now walk the update's slots through
  `Nitro::admit_rows`, whose arithmetic saturates so an outstanding skip near
  `usize::MAX` — reachable from `commit_ctx` or a decoded payload — cannot
  overflow the walk.
- **The row-level Nitro sampler admitted its first row slot unconditionally.**
  `Nitro` was built with `to_skip = 0`, so the first call to `admit_rows`
  admitted row 0 at every rate. For a one-row sketch that puts the estimate at
  `1/p` instead of 1 — 100× at `p = 0.01` — and for a `d`-row sketch it biases
  the first `d` slots of every fresh sketch. Both constructors now draw the
  first skip at construction; `init_nitro_seeded` places the cursor at the
  seed's table offset first, so the initial draw is the seed's own. At `p = 1`
  every row is still admitted with weight 1.
- **A decoded `Nitro` restarted its stochastic-rounding stream from a fixed
  constant.** `rounding_state` was `#[serde(skip)]`, so a sketch serialized
  mid-stream and resumed emitted a different weight sequence than the
  uninterrupted run at any rate whose reciprocal is not an integer. It is now
  serialized. See *Changed* for what that does to the encoded shape.

### Added

- **`CountMin::enable_nitro_with_seed`, `Count::enable_nitro_with_seed`,
  `Vector2D::enable_nitro_with_seed`, `Nitro::init_nitro_seeded`.** The
  unseeded path starts every sketch at the same point in the shared skip table,
  so two sketches at the same rate admit exactly the same subset — there was no
  way to run a Nitro accuracy battery over independent trials. The seed moves
  both the table cursor and the rounding stream. `NitroBatch::with_target_and_seed`
  now seeds the cursor too, so `insert_cached_step` is seed-dependent as
  `insert` already was.
- **`NitroContext`, `Nitro::context` and `Nitro::restore_context`.** The
  complete row-level sampling state — table cursor, outstanding skip, and the
  stochastic-rounding stream — so a snapshot restored onto another sketch
  continues the admission and weight sequence exactly. The older
  `Nitro::get_ctx` / `commit_ctx` pair carries the first two only; it is kept
  for compatibility and its documentation now says so rather than claiming to
  save the sampling state. `NitroBatch`'s pair covers the *cached* schedule
  only — its live `SmallRng` is not serializable — and says that too.

- **NitroSketch's compensating weight is no longer biased at rates whose
  reciprocal is not an integer.** `NitroBatch` and the row-level `Nitro` behind
  `CountMin::fast_insert_nitro` / `Count::fast_insert_nitro` both wrote
  `ceil(1 / p)` into the target on every admitted update. That is only the
  right compensation when `1 / p` is an integer: the public constructors accept
  any `0 < p <= 1`, and at `p = 0.3` every estimate came back
  `f * 0.3 * ceil(3.33) = 1.2 f` — a flat +20% on every key, in the shipped
  estimator, invisible to a test grid of `{1, 1/2, 1/10, 1/100}`. The weight is
  now rounded **stochastically**, `floor(1/p) + Bernoulli(frac(1/p))` drawn per
  admitted update, so `E[W] = 1/p` and hence `E[est] = f` at every rate, at the
  cost of `frac(1/p)(1 - frac(1/p)) <= 1/4` extra variance per admission.
  `NitroBatch::admitted_weight` draws from the sampling RNG; `Nitro::
  admitted_delta` draws from its own splitmix64 stream, advanced once per
  admitted slot and independent of the skip cursor, so the dither cannot become
  a fixed function of position in the skip table. When `1 / p` is an integer the
  fraction is zero, no draw is consumed, and the emitted weights are exactly
  what they were before — so the common rates (1, 1/2, 1/10, 1/100) are
  unchanged.

- **`ExponentialHistogram` no longer discards its payload when the bucket
  sketch is an `Elastic`.** `EHSketchList::merge` had no `ELASTIC` arm, so the
  catch-all returned `Cannot merge sketches of different types` for two Elastic
  payloads — and `EHBucket::to_merge` drops that `Result`, so the histogram
  went on summing bucket sizes while keeping only the counters of whichever
  bucket it merged into. Every interval query then answered from a single
  bucket: on a 10k-update Zipf stream each of the true top flows read 0 against
  counts in the thousands. The arm is now present and delegates to
  `Elastic::merge` like every other variant. No wire or API change.

- **`HeapItem` now has an owned byte-array key, so `DataInput::Bytes` no longer
  goes through `String`.** `input_to_owned` decoded every byte array as UTF-8
  and panicked on any that was not, and the ones that were became
  `HeapItem::String`, which made `Bytes(b"abc")` and `Str("abc")` the same key.
  `HeapItem::Bytes(Vec<u8>)` carries the bytes unchanged from insert through
  query, merge and the wire: `hash_item64_seeded` / `hash_item128_seeded` hash
  them exactly as `hash64_seeded` / `hash128_seeded` hash the borrowed
  `DataInput::Bytes`, equality against a `DataInput` is variant-exact so a byte
  key and a string key stay distinct, and `key_order` orders the two families
  apart. On the ASAPv1 wire this is a new `key_type` of `"bytes"`, written as
  msgpack `bin` and read back from `bin` alone (Space-Saving `0x18 0x00`,
  CMSHeap `0x03 0x00`, CSHeap `0x0a 0x00`, the UnivMon pyramids and Hydra's
  UnivMon cells). Existing integer and `"string"` encodings are unchanged and
  bytes written before this release still decode.

- **Restored the α relative-accuracy guarantee in portable `DdSketch`.**
  Quantile representatives changed from the log-midpoint γ^(k+0.5), whose edge
  error √γ−1 exceeds α, to γ^k·(1+α) — matching core `DDSketch` and DataDog's
  mapping. No wire/state migration; query outputs shift by at most α.
- **Portable `DdSketch` now rejects untrackable inputs like core.** Non-finite
  values (a NaN previously floor-cast into bucket 0) and finite-but-extreme
  values beyond the indexable range are dropped silently, mirroring core
  `DDSketch`'s min/max-indexable guards and DataDog's mapping (#70). Unguarded,
  one `f64::MAX` sample mapped ~35k buckets away at α=0.01 (~277 KiB of dense
  store per sample), scaling with 1/ln γ.
- **Portable `DdSketch::apply_delta` bounds hostile wire input.** Deltas now
  pre-validate their bucket span and return `Err` instead of padding the
  dense store: a corrupt delta carrying an index near `i32::MAX` would
  previously have attempted a ~2·10⁹-bucket (~17 GiB) allocation in one call.
  The limit (4M buckets) dwarfs any legitimate span; the `apply_delta*`
  family propagates the error. `merge`/`merge_refs` apply the same span cap
  to decoded snapshots, with a pass-through for stores that already
  legitimately exceed it. Portable `DdSketch::new` now asserts α ∈ (0,1)
  like core, and the indexable-range formulas live in one shared helper
  (`ddsketch_indexable_bounds`) used by both implementations, so their input
  guards cannot drift apart.
- Removed the raw-pointer write from `DDSketch`'s per-sample insertion path;
  bucket increments now go through safe indexing with the same bounds check
  they already performed (#70 item 6).
- **CountL2HH hot-path L2 accumulation saturates instead of wrapping.**
  Per-row Σcount² updates run through a saturating i128 intermediate, so
  extreme turnstile counts no longer wrap silently in release builds (or
  panic on overflow in debug builds).
- **Heap-allocate HLL register storage.** `impl_hll_bucket_list!`'s `Default`
  and `Deserialize` no longer build an `[u8; NUM_REGISTERS]` value on the
  stack before boxing it, which overflowed the stack in debug builds at
  larger precisions. The MessagePack encoding is unchanged.
- Corrected UnivMon and UnivMonPyramid merge semantics: aggregate stream
  weight, recomputed CountL2HH row norms, and merged-counter candidate
  re-estimation.
- Restored per-layer L2 maintenance for standard UnivMon updates and completed
  Joltik-style terminal-only updates with query-time logical reconstruction.
- Replaced UnivMon's fixed cardinality cutoff with a heap-sized L2-heavy-hitter
  threshold, tracked whether bounded candidate sets are complete, and made
  queries refresh candidate frequencies from current CountSketch counters.
- Removed duplicate stream-weight accounting in experimental UnivMon windows.
- Made L1 exact for UnivMon, UnivMonPyramid, and experimental UnivMon-Q by
  returning their tracked non-negative stream weight instead of evaluating a
  noisy generic recurrence.

### Added
- **Synthetic-data E2E testing harness with a reusable conformance kit.** A
  shared `tests/common` module (seeded stream generators, exact ground-truth
  trackers, tolerance-based assertion helpers) plus themed integration suites
  covering frequency, cardinality, quantile, framework/composition, and
  experimental sketches — every public sketch family is exercised end to end
  against exact ground truth. `tests/common/conformance.rs` defines standard
  batteries (`frequency`, `turnstile`, `cardinality`, `quantile`,
  `merge_equivalence`) that new sketches must pass via small adapter impls;
  `tests/README.md` documents the onboarding recipe and
  `tests/conformance_kit.rs` shows reference adapters. Also includes
  `tests/bug_verification.rs` regression tests for the fixes above and
  `examples/accuracy_probe.rs`, a release-mode ground-truth probe.
- **Deep Hydra coverage in `tests/e2e_frameworks.rs`.** A single-column Hydra —
  where the grid does the keying and each cell holds one counter — now runs the
  standard conformance batteries once per counter family it can host (CM,
  Count Sketch, HLL, KLL), joined by what no battery models: the `2^D - 1`
  fan-out across the subpopulation lattice checked against exact
  per-subpopulation truth, wildcard marginals reconciled against the cells
  beneath them, exact shard-merge equality, MessagePack round trips for every
  counter variant, and subkey injectivity for delimiter-laden key values. A
  Theorem 2 check (Manousis et al., VLDB 2022) asserts the additive `eps * G_s`
  bound against the exact binomial median-failure rate over 314 subpopulations
  at `G_s = 840k`, in both the sparse deployment regime (5x4096) and a
  deliberately overloaded grid (5x256) where the in-bound fraction is actually
  exercised. The mixed `HashSketchEnsemble` test moved here from
  `tests/e2e_cardinality.rs`.
- **Export the `impl_hll_bucket_list!` macro.** Downstream crates can now
  generate an `HllRegisterStorage` type at any precision (e.g. `lg_k = 18`)
  instead of being limited to the built-in `HllBucketListP12/P14/P16`.
- **`ErtlMLE` estimation at any precision.** `HyperLogLogImpl::<ErtlMLE, _>::estimate`
  is now a generic impl instead of one generated per storage type, so it works
  with custom `impl_hll_bucket_list!` precisions rather than only
  `HllBucketListP12/P14/P16`.
- **Experimental UnivMon-Q core sketch.** Adds `UnivMonQ<H>`, a Joltik-style
  terminal-stratum UnivMon implementation extended with an adaptively assisted
  occurrence sample for entropy, rank, CDF, and quantile estimates. The sketch
  supports pluggable `SketchHasher`, compatible merges, native MessagePack round-trips,
  exact extrema, point frequency, F0, F2, compatible generic g-sums, entropy,
  and recovered heavy-hitter queries. Reusable prepared query views share
  logical-hierarchy and CDF reconstruction across a metric batch. The API,
  estimators, and guarantees are experimental; an ASAPv1 cross-language kind
  is not yet assigned.
- **KLL `bulk_update` API for batch ingestion.** Adds `KLL::bulk_update(&[T])`
  and `KLL<f64>::bulk_update_data_input(&[DataInput])` (plus `KLLDynamic` and
  `HydraCounter::bulk_insert` delegating) — exactly equivalent to looping
  `update` (including `Coin` order, `count`, and byte-identical
  `serialize_to_bytes` for same seed), with empty slices as a no-op that
  preserves the memoized CDF. Fixes #88.

### Changed (breaking — wire format)
- **Drop the DataPoint-level METRIC scalars from `DDSketchState`.** Removed
  `count` (4), `sum` (5), `min` (6) and `max` (7) from
  `proto/ddsketch/ddsketch.proto` (tags now `reserved`), regenerated the prost
  bindings, and dropped the matching `count`/`sum`/`min`/`max` fields from the
  portable msgpack DTO (`message_pack_format::portable::ddsketch::DdSketch`) so
  its rmp-compact array shrinks from 7 to 3 elements
  (`[alpha, store_counts, store_offset]`). The total count is recovered by
  summing `store_counts` (`DdSketch::total_count()`); quantile estimation now
  derives the high-end estimate from the highest non-empty bucket instead of the
  removed `max`. The in-memory `sketches::DDSketch` keeps these as runtime-only
  state (`#[serde(skip)]`) and rebuilds them from the bucket store on decode.
  Byte-parity twin of the parallel `sketchlib-go` change
  (ProjectASAP/sketchlib-go#243).

## [0.2.2] - 2026-05-18

Performance patch release. No API changes; all public sketch APIs behave
identically to `0.2.1`.

### Performance
- **Restore `FixedMatrix` fast-path specialization in non-PGO builds**
  ([#50](https://github.com/ProjectASAP/asap_sketchlib/pull/50)). Added
  `#[inline(always)]` to `hash_for_matrix_seeded` and
  `hash_for_matrix_seeded_generic` in `src/common/hash.rs` so LLVM can
  propagate compile-time `rows` / `cols` literals from `FixedMatrix` call
  sites, fold the matrix-hash-mode dispatch, and unroll the inner loop.
  Measured on 1M `i64` inserts, single-threaded:
  - `CountMin<QuickMatrixI32, FastPath>` 5×2048: 89.7 → 173.9 M/s (+94%)
  - `Count<QuickMatrixI32, FastPath>` 5×2048: 71.5 → 115.1 M/s (+61%)

  Both within 3% of a PGO build. Downstream benchmarks no longer need PGO
  machinery or `-C llvm-args=-inline-threshold=...` overrides to hit the
  specialized path. KLL / HLL throughput unchanged.

## [0.2.1] - 2026-05-14

Maintenance release. No source-level changes to sketch algorithms; all public
sketch APIs (`CountMin`, `Count`, `HyperLogLog`, `KLL`, `DDSketch`, …)
behave identically to `0.2.0`.

### Changed
- **Precompute tables now build lazily at runtime via `std::sync::LazyLock`**
  instead of being shipped as multi-megabyte literal arrays
  ([`fba7a5b`](https://github.com/ProjectASAP/asap_sketchlib/commit/fba7a5b)).
  This drops ~147k lines from the published crate and brings it under the
  crates.io size limit. Index/iter/len access patterns are preserved through
  `Deref`, so typical usage is unaffected:

  ```rust
  let h = asap_sketchlib::PRECOMPUTED_HASH[42]; // still works
  for x in asap_sketchlib::PRECOMPUTED_SAMPLE.iter() { /* ... */ }
  ```

  Strictly per Rust's [SemVer guide](https://doc.rust-lang.org/cargo/reference/semver.html),
  changing the type of a `pub static` is classified as a breaking change. The
  affected items are:

  | Item | Old type | New type |
  | --- | --- | --- |
  | `PRECOMPUTED_HASH` | `[u128; 0x4000]` | `LazyLock<Box<[u128]>>` |
  | `PRECOMPUTED_SAMPLE` | `[f64; 0x10000]` | `LazyLock<Box<[f64]>>` |
  | `PRECOMPUTED_SAMPLE_RATE_1PERCENT` | `[f64; 0x10000]` | `LazyLock<Box<[f64]>>` |

  We are shipping this under a patch bump because (a) these tables are
  internal precompute artifacts that no known downstream binds by type, and
  (b) the value at every index is bit-for-bit identical to `0.2.0`. If you
  do depend on the concrete array type, please open an issue.

### Removed
- **Cargo feature `internal-bins`** and the three maintainer binaries it
  gated (`generate_precomputed_hash`, `generate_precomputed_sample`,
  `generate_precomputed_sample2`). These were intended only for regenerating
  the precompute tables, which now build lazily and require no codegen step.

### Build
- **Vendored prost output; `build.rs` removed**
  ([`bfcf906`](https://github.com/ProjectASAP/asap_sketchlib/commit/bfcf906)).
  Generated Rust types from `proto/**/*.proto` are now checked in under
  `src/proto/generated/` and refreshed manually by maintainers via
  `tools/gen-proto/`. Downstream users no longer need `protoc` and the crate
  no longer has any `[build-dependencies]`.
- CI now enforces that the committed `src/proto/generated/` matches the
  result of running `tools/gen-proto` against the current `.proto` sources
  ([`a13f353`](https://github.com/ProjectASAP/asap_sketchlib/commit/a13f353)).

### Docs
- README install instructions now point to crates.io with version, docs.rs,
  license, and MSRV badges
  ([`1a20eba`](https://github.com/ProjectASAP/asap_sketchlib/commit/1a20eba)).
- README's git-tag pin example updated to `v0.2.1`.

## [0.2.0] - 2026-05-12

Initial crates.io release.

- First publication of `asap_sketchlib` to [crates.io](https://crates.io/crates/asap_sketchlib).
- Public sketch APIs: `CountMin`, `Count`, `HyperLogLog` (`Classic`, `ErtlMLE`,
  HIP variants), `KLL`, `DDSketch`, plus framework layers (`Hydra`, `UnivMon`,
  `NitroBatch`, `ExponentialHistogram`) and shared primitives (`DataInput`,
  `SketchHasher`, `HashSketchEnsemble`).
- MessagePack wire format shared with `sketchlib-go`.
- MSRV: Rust 1.85 (Rust 2024 edition).

## [0.1.0] - 2026-04-24

Pre-release tag. Not published to crates.io.

[Unreleased]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ProjectASAP/asap_sketchlib/releases/tag/v0.1.0
