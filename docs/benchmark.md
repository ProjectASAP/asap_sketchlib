# Benchmark

This document summarizes the current Criterion benchmarks in `benches/` and the latest recorded results in `benchmark/`.

## Bench Suite

- Count-Min (`benches/countmin.rs`) — compares `insert_only` vs `fast_insert_only`, and `estimate` vs `fast_estimate` on the same workload.
- Count (`benches/count.rs`) — same comparisons as above, using explicit dimensions (`rows=5`, `cols=32768`).
- Hash variants (`benches/hash_detailed.rs`) — micro-benchmarks for `xxhash32`, `xxhash64`, `xxhash3_64`, and `xxhash3_128` against fixed-size payloads. (No checked-in results yet.)

## Setup & Methodology

- Workload size: 16,384 random `U64` keys generated from a fixed RNG seed.
- RNG seed: `0x5eed_c0de_1234_5678` (count, count-min); `0xFEED_CAFE_DEAD_BEEF` (hash suite).
- Estimation benches run against prefilled sketches to isolate read-path performance.
- Count dimensions: `rows=5`, `cols=32768`. Count-Min uses `CountMin::default()`.
- Criterion handles warm-up, measurement, and reports typical time ranges.

## Results (latest in `benchmark/`)

All numbers below use the middle value of Criterion's reported time range.

Count-Min (`benchmark/countmin_benchmark.txt`)

- insert_only: ~181.64 µs
- fast_insert_only: ~109.55 µs (≈40% faster)
- estimate: ~186.66 µs
- fast_estimate: ~100.90 µs (≈46% faster)

Count (`benchmark/count_benchmark.txt`)

- insert_only: ~309.22 µs
- fast_insert_only: ~207.27 µs (≈33% faster)
- estimate: ~1.0895 ms
- fast_estimate: ~994.94 µs (≈9% faster)

Hash Variants (`benchmark/hash_detailed_benchmark.txt`)

- xxhash32/64: ~10.822 µs
- xxhash64/64: ~20.815 µs
- xxhash3_64/64: ~8.3618 µs (fastest)
- xxhash3_128/64: ~11.804 µs

Notes

- Criterion may flag regressions/noise relative to prior runs; the numbers above compare fast vs. non-fast paths within the same run.
- Outliers are present in some groups but do not change the relative ordering.
- Count sketch performance improvement: `fast_estimate` is now ~9% faster (previously showed regression due to f64 sorting overhead, fixed by using i64 sorting).

## How To Run

- Count-Min: `cargo bench --bench countmin`
- Count: `cargo bench --bench count`
- Hash variants: `cargo bench --bench hash_detailed`
- all: `cargo bench -- --measurement-time 10`

Results will appear in `target/criterion/` and can be optionally summarized into text files under `benchmark/`.
