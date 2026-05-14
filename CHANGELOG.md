# Changelog

All notable changes to `asap_sketchlib` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
For pre-1.0 (`0.y.z`) releases we follow the Cargo convention of treating `y`
as the major component: bumping `y` signals a breaking change, bumping `z`
signals a backwards-compatible change.

## [Unreleased]

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

[Unreleased]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ProjectASAP/asap_sketchlib/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ProjectASAP/asap_sketchlib/releases/tag/v0.1.0
