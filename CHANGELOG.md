# Changelog

All notable changes to `asap_sketchlib` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
For pre-1.0 (`0.y.z`) releases we follow the Cargo convention of treating `y`
as the major component: bumping `y` signals a breaking change, bumping `z`
signals a backwards-compatible change.

## [Unreleased]

### Added

- **`Bloom`, a partitioned Bloom filter.** `rows` slices of `cols` bits, one
  slice per hash function, over a new packed `BitMatrix` that implements
  `MatrixStorage` — so the filter probes the same `rows x cols` shape
  `CountMin` does and a membership query is the row-wise minimum, which over
  single bits is their AND. `with_capacity(n, p)` caps the slice count at
  `BLOOM_MAX_SLICES` (the seed list's length, since two slices seeded from the
  same entry are identical) and solves the slice width for that count, so
  `predicted_fpp` never claims a rate the filter cannot deliver; each slice is
  rounded up to a power of two, which removes the column fold's modulo bias.
  Total bits are capped at `BLOOM_MAX_BITS`, and a non-finite target rate is
  rejected. Union is exact, so the filter shards without loss. The `BloomMode`
  marker tags the hash path on the wire, so bytes written by one path do not
  decode into the other.
- **`SpaceSaving`, a fixed-counter heavy-hitter summary.** The paper's
  Stream-Summary: count-ordered buckets in a doubly linked list, each owning a
  doubly linked list of its counters, plus a key index — so a unit arrival moves
  one counter to the neighbouring bucket and an eviction takes the head of the
  lowest bucket, constant work at any capacity. Both lists are index arenas
  rather than pointers. A monitored key is sandwiched by its own error, and
  `upper_bound` never reads below the truth for any key in the stream: the
  summary carries the largest count known to have left it, so the ceiling
  survives a merge that leaves it holding fewer keys than its capacity. Counts
  saturate rather than wrap, and `merge_from` picks the same survivors on every
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

### Changed

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
  and the goldens cover CMS and HLL envelopes only.
- **BREAKING (external implementors of `MatrixFastHash`):** the trait gained a
  required `row_hash(row, mask_bits, mask)` method, split out of
  `col_for_row` so storages can decode against precomputed parameters and
  skip `% cols` for power-of-two column counts. The three in-crate impls
  (`MatrixHashType`, `u64`, `u128`) are updated; downstream code implementing
  this trait directly must add the method. Ship in the next `0.y` bump
  (Cargo convention: `y` is the major component pre-1.0).

### Fixed

- **Made `UnivMon::merge` deterministic.** The union of the two heaps' candidate
  keys was collected into a `HashSet` and seated by iterating it, so at the
  eviction boundary — where several candidates tie on count and only some fit in
  `k` — which keys survived varied between runs of the same program on the same
  input. Measured: six runs of one binary retained six different key sets. The
  union is now seated in a total order over `(count, key digest)`, the order the
  layer's own candidate selection already uses, which also makes the merge
  independent of which side is the receiver and of insertion order into either.
- **Made `NitroBatch` frequency estimates unbiased.** Inserts hashed keys with
  raw `hash128_seeded` while the public estimator derived cells from the
  packed matrix hash (Packed64 mode), so estimates read cells inserts never
  wrote and returned ~0; each sampled record now updates every row using the
  sketch's own fast-path hash derivation, and both batch and streaming skip
  draws use `floor` instead of `ceil` (the extra +1 per skip halved-ish the
  effective sampling rate). Estimates now converge to true frequencies at any
  rate. Update weights saturate at the `i32` counter domain instead of
  wrapping (reachable via rates below ~4.7e-10 or by writing the public
  `delta` field directly).
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
  counter variant, subkey injectivity for delimiter-laden key values, and
  `MultiHeadHydra`'s equivalence to independent single-head Hydras. A Theorem 2
  check (Manousis et al., VLDB 2022) asserts the additive `eps * G_s` bound
  against the exact binomial median-failure rate over 314 subpopulations at
  `G_s = 840k`, in both the sparse deployment regime (5x4096) and a
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

[Unreleased]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ProjectASAP/asap_sketchlib/releases/tag/v0.1.0
