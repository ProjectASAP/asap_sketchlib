# The E2E Testing Harness and Conformance Kit

This document explains how the end-to-end testing harness is designed and how
to use it when adding or changing a sketch. The short version: **every sketch
must pass the same conformance batteries, driven by seeded synthetic data and
compared against exactly-known ground truth.** Ad-hoc assertions alone are no
longer an acceptable bar for a new sketch.

Related reading:

- [`tests/README.md`](../tests/README.md) — quick onboarding recipe
- [`tests/common/conformance.rs`](../tests/common/conformance.rs) — the kit itself
- [`tests/conformance_kit.rs`](../tests/conformance_kit.rs) — reference adapters

## Why it exists

Sketches are approximate by design, which makes them easy to get subtly wrong.
Three properties make this harness effective where ordinary unit tests are not:

1. **Ground truth is exact.** Generators track the true frequency vector,
   order statistics, entropy, and cardinality while they emit data. Expected
   values are never derived from another approximation.
2. **Everything is deterministic.** All randomness flows through seeded RNGs.
   A failure today reproduces tomorrow, on any machine.
3. **Tolerances are justified.** Where theory exists we assert against it
   (α for DDSketch, rank error bands for KLL, one-sided ε·N bounds for
   Count-Min). Where it does not, tolerances are documented empirical values
   consistent across suites.

The harness earned its keep immediately: its first run surfaced four real
defects — a Nitro estimator that returned zero because inserts and queries
hashed keys differently, a structural under-count from single-row updates, a
skip-draw rounding bug that halved the effective sampling rate, and a portable
DDSketch representative that violated the advertised α bound at bucket edges.
None of these were caught by the existing unit tests.

## Architecture

```
tests/
├── common/
│   ├── mod.rs           # generators + truth trackers + assertion helpers
│   └── conformance.rs   # capability traits + reusable batteries  ← the kit
├── conformance_kit.rs   # reference adapters (copy these)
├── e2e_frequency.rs     # deep per-family suites…
├── e2e_cardinality.rs
├── e2e_quantiles.rs
├── e2e_frameworks.rs
├── e2e_experimental.rs  # …feature-gated variants
└── bug_verification.rs  # regression tests for fixed defects
```

There are three layers:

**Layer 1 — primitives (`common/mod.rs`).**
Seeded generators produce streams with known shape: `zipf_u64` (heavy heads),
`uniform_u64`, `normal_f64`, `exponential_f64`, and `log_uniform_f64`, whose
values deliberately land on bucket edges of logarithmic mappings. As each
stream is produced, truth accumulates in `FreqTruth` (per-key counts with
exact F0/F1/F2/L2/entropy/top-k) or `NumericTruth` (sorted values exposing
nearest-rank quantiles, CDF, and rank-tolerance value bands). Assertion
helpers turn comparisons into readable failures that print expected vs actual.

**Layer 2 — the conformance kit (`common/conformance.rs`).**
A sketch describes *what it guarantees* by implementing small capability
traits; batteries translate those guarantees into checks. Traits:

| Trait | Promise |
| --- | --- |
| `FrequencyOps<K>` | point frequency estimates per key |
| `SignedFrequencyOps<K>` | weighted/turnstile ingestion (+/− updates) |
| `CardinalityOps` | distinct count over opaque byte keys |
| `QuantileOps` | quantiles of a numeric stream |
| `MergeOps` | in-place merge from a same-config instance |

Batteries consume traits and a tolerance spec, accumulate every violation
into a `BatteryReport`, and report all failures at once via `.assert_ok()`:

| Battery | Checks |
| --- | --- |
| `frequency_battery` | dense-key accuracy; one-sided sketches must never underestimate and stay within `(1 + rel_tol)` |
| `merge_equivalence_battery` | shard-merged sketch ≡ single-pass sketch within slack |
| `turnstile_battery` | `+500 / −200 → ~300`; full cancellation → 0 |
| `cardinality_battery` | unique-stream accuracy at a checkpoint; re-ingesting seen keys must not move the estimate |
| `quantile_battery` | estimates land inside rank-tolerance value bands across the standard q grid |

Specs carry the tolerances: `FrequencySpec { one_sided, rel_tol, abs_tol }`,
`CardinalitySpec { rel_tol }`, `QuantileSpec { rank_tol, qs }`.

**Layer 3 — suites.**
`conformance_kit.rs` wires established sketches through the kit as reference
adapters. The `e2e_*.rs` suites add depth the kit deliberately does not
attempt: serialization round trips, window semantics, framework composition
(Hydra fan-out, tumbling windows, sliding histograms), and cross-implementation
parity between core types and their portable wire twins.

## Onboarding a new sketch

Worked example. Suppose you added `MyHeavyHitter` that tracks per-key counts,
supports decrements, and merges.

### 1. Choose batteries matching your documented promises

If your docs say "returns per-key counts", you implement `FrequencyOps`. If
they promise "counts never underestimate" (Count-Min style), set
`one_sided: true`. Only claim what you document — the battery enforces the
spec you select, not what the sketch happens to do.

### 2. Write one adapter struct

```rust
mod common;

use common::conformance::{self, FrequencyOps, SignedFrequencyOps};

struct MyAdapter(MyHeavyHitter);

impl FrequencyOps<i64> for MyAdapter {
    fn ingest(&mut self, key: &i64) {
        self.0.insert(&DataInput::I64(*key));
    }
    fn estimate(&self, key: &i64) -> f64 {
        self.0.estimate(&DataInput::I64(*key)) as f64
    }
}

impl SignedFrequencyOps<i64> for MyAdapter {
    fn ingest_weighted(&mut self, key: &i64, weight: i64) {
        self.0.insert_many(&DataInput::I64(*key), weight);
    }
}
```

Adapters exist so the kit stays stable while sketch constructors vary. Keep
them thin — no logic beyond translating types.

### 3. Run production-shaped batteries

```rust
#[test]
fn my_heavy_hitter_passes_conformance() {
    let stream = common::zipf_u64(60_000, 2048, 1.1, /* seed */ 9001);
    let mut truth = common::FreqTruth::default();
    for k in &stream { truth.observe(*k as i64); }

    let spec = conformance::FrequencySpec { one_sided: false, rel_tol: 0.05, abs_tol: 20.0 };
    conformance::frequency_battery(
        "MyHeavyHitter",
        || MyAdapter(MyHeavyHitter::with_dimensions(5, 4096)),
        // ^ construct at dimensions users actually deploy, not toy sizes
        &stream.iter().map(|v| *v as i64).collect::<Vec<_>>(),
        &truth,
        spec,
    )
    .assert_ok();

    conformance::turnstile_battery("MyHeavyHitter", || MyAdapter(MyHeavyHitter::with_dimensions(5, 4096)), 42)
        .assert_ok();
}
```

Use realistic parameters. A sketch tuned to pass a 16-column toy grid tells
you nothing about behavior at 4096 columns under Zipf traffic; several real
bugs only appeared at deployment-shaped dimensions.

### 4. Add depth where the sketch is unusual

Anything not covered by a battery belongs in the matching `e2e_*.rs` suite:
window semantics, merge order-independence, wire-format parity with the
portable type, heavy-hitter recall targets, and so on.

## Tolerance policy

- Prefer theoretical bounds and cite them in a comment next to the spec.
- Empirical tolerances must be no looser than comparable sketches in the same
  suite. If your sketch needs ±30% where KLL uses ±3%, either fix the sketch
  or document why the guarantee is genuinely weaker before widening.
- Fixed seeds are part of the contract. If a test is flaky, do not loosen the
  seed's randomness exposure — tighten the implementation or state the
  statistical intent explicitly and pick a defensible band.

## Rules and anti-patterns

- No unseeded randomness anywhere in tests. Coco-style nondeterministic
  implementations are tested statistically over bounded ranges instead.
- Never derive expected values from another approximation. Truth comes from
  `FreqTruth`/`NumericTruth`, full stop.
- Do not bypass public APIs in tests to "fix" them. The Nitro estimator
  shipped broken precisely because its tests re-implemented estimation with
  matching hashes instead of calling the public method.
- Beware generator bugs masquerading as sketch bugs: an early draft drew a
  fresh random number inside a `binary_search_by` comparator, corrupting the
  whole distribution. When accuracy looks wrong, first verify the stream's
  empirical shape matches its name.
- Batteries report everything; call `.assert_ok()` once rather than asserting
  inline so a single CI run shows the complete picture.

## Running

```bash
cargo test --all-features --locked          # full matrix incl. experimental
cargo test --test conformance_kit            # kit + reference adapters only
cargo run --release --example accuracy_probe --features experimental
                                             # heavy release-only probes
```

CI enforces format, clippy (`-D warnings`, all features), and the full test
matrix on every PR.
