# Experimental UnivMon-Q Evaluation

`UnivMonQ` is experimental. The measurements below are empirical results for
the current Rust implementations, not new theoretical guarantees. APIs,
estimators, configuration defaults, wire state, and performance may change.

## Reproduction

```bash
cargo run --release --example evaluate_univmon_q_skew -- 1000000 5 100000 256
```

The comparison uses one million observations per dataset, five independent
trials, a 100,000-value domain, Zipf exponents from 0.0 through 2.0, and an
eight-way merge. Both constructions use 14 logical levels, a top width of
4,096, depth 5, and 256 candidates. UnivMon uses terminal-only `fast_insert`.
UnivMon-Q additionally uses 32-bit counters, width halving every three levels,
and 4,096 coordinated occurrence samples.

## Comparison with UnivMon

Worst p95 error across the seven skew levels:

| Metric | UnivMon | UnivMon-Q |
| --- | ---: | ---: |
| Point-frequency normalized RMSE | 0.378% | 0.383% |
| F0 relative error | 9.52% | 12.04% |
| F2 relative error | 4.96% | 10.12% |
| Entropy relative error | 17.32% | 83.13% |
| Generic L1 relative error | 6.34% | 44.98% |
| Quantile mean rank error | Not supported | 0.45% |
| Maximum CDF error | Not supported | 2.42% |

Median performance ranges across the seven skew levels:

| Measurement | UnivMon | UnivMon-Q | UnivMon-Q improvement |
| --- | ---: | ---: | ---: |
| Update | 1.0–11.9 us/item | 48–130 ns/item | 7.8–211x |
| Eight-way merge | 33–217 ms | 2.2–4.1 ms | 15–53x |
| All-metric query batch | 0.92–2.78 ms | 0.19–0.63 ms | 4.4–5.3x |
| In-memory core | At least 2.188 MiB of counters, before heap metadata | 0.707 MiB estimated | At least 3.1x smaller |

UnivMon was generally more accurate for the shared universal metrics at this
candidate budget. UnivMon-Q added ordered queries and was substantially faster
and smaller. Entropy and generic g-sums were the clearest UnivMon-Q weakness;
profiles requesting them need more candidate/core capacity than this compact
256-candidate configuration.

The update gap is not solely a construction-level result. The current UnivMon
`HHHeap` rebuilds its candidate-position map after retained-key updates, so its
cost grows with candidate capacity and hot-key skew. UnivMon-Q maintains its
candidate state incrementally. UnivMon-Q also saves memory through packed
32-bit counters and geometric width reduction, whereas this UnivMon baseline
uses fixed-width 64-bit counter layers.

## Correctness checks

Across 35 large trials, both sketches preserved exact stream counts, produced
finite universal estimates, and round-tripped through their native MessagePack
state. Shared UnivMon metrics and all UnivMon-Q queries were stable across left
fold and balanced merge trees. Two uniform trials selected different
equal-frequency identities for UnivMon's top-20 result; the metric estimates
were unchanged, so these are reported as tie warnings rather than correctness
failures.

For UnivMon-Q, exact extrema, monotone CDF construction, monotone quantiles,
and ordered-query merge equivalence were also checked. These tests support the
implementation but do not replace the guarantees and caveats in the
[UnivMon-Q API documentation](./api/api_univmon_q.md).
