# Benchmark

This document summarizes the current Criterion benchmarks in `benches/` and the latest recorded results in `benchmark/`.

## Bench Suite

- Count-Min (`benches/countmin.rs`) — compares `insert_only` vs `fast_insert_only`, and `estimate` vs `fast_estimate` on the same workload.
- Count (`benches/count.rs`) — same comparisons as above, using explicit dimensions (`rows=5`, `cols=4096`).
- Hash variants (`benches/hash_detailed.rs`) — micro-benchmarks for `xxhash32`, `xxhash64`, `xxhash3_64`, and `xxhash3_128` against fixed-size payloads.
- HashLayer (`benches/hashlayer.rs`) — measures the performance benefit of computing hash once and reusing it across multiple sketches.

## Setup & Methodology

- Workload size: 16,384 random `U64` keys generated from a fixed RNG seed.
- RNG seed: `0x5eed_c0de_1234_5678` (count, count-min); `0xFEED_CAFE_DEAD_BEEF` (hash suite).
- Estimation benches run against prefilled sketches to isolate read-path performance.
- Count dimensions: `rows=5`, `cols=32768`. Count-Min uses `CountMin::default()`.
- Criterion handles warm-up, measurement, and reports typical time ranges.

## Results (latest in `benchmark/`)

All numbers below use the middle value of Criterion's reported time range.

Count-Min (`benchmark/countmin_benchmark.txt`)

- insert_only: ~181.49 µs
- fast_insert_only: ~109.62 µs (≈40% faster)
- estimate: ~185.96 µs
- fast_estimate: ~100.71 µs (≈46% faster)

Count (`benchmark/count_benchmark.txt`)

- insert_only: ~308.98 µs
- fast_insert_only: ~203.56 µs (≈34% faster)
- estimate: ~1.0922 ms
- fast_estimate: ~955.59 µs (≈12.5% faster)

Hash Variants (`benchmark/hash_detailed_benchmark.txt`)

- xxhash32/64: ~10.81 µs
- xxhash64/64: ~20.71 µs
- xxhash3_64/64: ~8.35 µs (fastest)
- xxhash3_128/64: ~11.72 µs

HashLayer (`benches/hashlayer.rs`)

- separate_insert_three_sketches: ~196.91 µs (each sketch computes its own hash)
- hashlayer_insert_all: ~144.97 µs (hash computed once, reused across all sketches) (≈26% faster)

Notes

- Criterion may flag regressions/noise relative to prior runs; the numbers above compare fast vs. non-fast paths within the same run.
- Outliers are present in some groups but do not change the relative ordering.
- Count sketch performance improvement: `fast_estimate` is now ~12.5% faster (previously showed regression due to f64 sorting overhead, fixed by using i64 sorting).
- HashLayer provides a practical abstraction for managing multiple sketches with minimal overhead (~9%) compared to raw hash-once implementation.

## How To Run

- Count-Min: `cargo bench --bench countmin`
- Count: `cargo bench --bench count`
- Hash variants: `cargo bench --bench hash_detailed`
- HashLayer: `cargo bench --bench hashlayer`
- All benchmarks: `cargo bench -- --measurement-time 10`

Results will appear in `target/criterion/` and can be optionally summarized into text files under `benchmark/`.
