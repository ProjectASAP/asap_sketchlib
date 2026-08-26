# Changelog

All notable changes to `asap_sketchlib` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
For pre-1.0 (`0.y.z`) releases we follow the Cargo convention of treating `y`
as the major component: bumping `y` signals a breaking change, bumping `z`
signals a backwards-compatible change.

## [Unreleased]

### Fixed

- **Made `NitroBatch` frequency estimates unbiased.** Inserts hashed keys with
  raw `hash128_seeded` while the public estimator derived cells from the
  packed matrix hash (Packed64 mode), so estimates read cells inserts never
  wrote and returned ~0; each sampled record now updates every row using the
  sketch's own fast-path hash derivation, and the geometric skip draw uses
  `floor` instead of `ceil` (the extra +1 per skip halved-ish the effective
  sampling rate). Estimates now converge to true frequencies at any rate.
- **Restored the α relative-accuracy guarantee in portable `DdSketch`.**
  Quantile representatives changed from the log-midpoint γ^(k+0.5), whose edge
  error √γ−1 exceeds α, to γ^k·(1+α) — matching core `DDSketch` and DataDog's
  mapping. No wire/state migration; query outputs shift by at most α.
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
