use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use sketchlib_rust::{
    hash64_seeded, run_octo, CmOctoParent, CmOctoWorker, Count, CountMin, CountOctoParent,
    CountOctoWorker, HllOctoParent, HllOctoWorker, HyperLogLog, OctoConfig, Regular, RegularPath,
    SketchInput, Vector2D,
};
use std::thread;

const RNG_SEED: u64 = 0x0c70_2026_5eed_1234;
const CM_COUNT_ROWS: usize = 3;
const CM_COUNT_COLS: usize = 4096;
const CM_COUNT_INPUTS: usize = 1_000_000;
const HLL_INPUTS: usize = 500_000;
const DOMAIN_MASK: u64 = (1 << 20) - 1;
const QUEUE_CAPACITY: usize = 65_536;
const WORKER_COUNTS: [usize; 4] = [1, 2, 4, 8];
const PARTITION_SEED: usize = 19;

fn build_inputs(sample_count: usize) -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED ^ (sample_count as u64));
    (0..sample_count)
        .map(|_| SketchInput::U64(rng.random::<u64>() & DOMAIN_MASK))
        .collect()
}

fn partition_indices(inputs: &[SketchInput<'_>], num_workers: usize) -> Vec<Vec<usize>> {
    let mut partitions: Vec<Vec<usize>> = (0..num_workers).map(|_| Vec::new()).collect();
    for (idx, item) in inputs.iter().enumerate() {
        let h = hash64_seeded(PARTITION_SEED, item) as usize;
        partitions[h % num_workers].push(idx);
    }
    partitions
}

fn run_regular_countmin_merge(
    inputs: &[SketchInput<'_>],
    num_workers: usize,
) -> CountMin<Vector2D<i32>, RegularPath> {
    let partitions = partition_indices(inputs, num_workers);
    let mut workers = Vec::with_capacity(num_workers);

    thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_workers);
        for partition in partitions.iter() {
            handles.push(s.spawn(move || {
                let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                for &idx in partition {
                    child.insert(&inputs[idx]);
                }
                child
            }));
        }

        for h in handles {
            workers.push(h.join().expect("regular CountMin worker thread panicked"));
        }
    });

    let mut parent =
        CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS);
    for worker in workers.iter() {
        parent.merge(worker);
    }
    parent
}

fn run_regular_count_merge(
    inputs: &[SketchInput<'_>],
    num_workers: usize,
) -> Count<Vector2D<i32>, RegularPath> {
    let partitions = partition_indices(inputs, num_workers);
    let mut workers = Vec::with_capacity(num_workers);

    thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_workers);
        for partition in partitions.iter() {
            handles.push(s.spawn(move || {
                let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                for &idx in partition {
                    child.insert(&inputs[idx]);
                }
                child
            }));
        }

        for h in handles {
            workers.push(h.join().expect("regular Count worker thread panicked"));
        }
    });

    let mut parent =
        Count::<Vector2D<i32>, RegularPath>::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS);
    for worker in workers.iter() {
        parent.merge(worker);
    }
    parent
}

fn run_regular_hll_merge(inputs: &[SketchInput<'_>], num_workers: usize) -> HyperLogLog<Regular> {
    let partitions = partition_indices(inputs, num_workers);
    let mut workers = Vec::with_capacity(num_workers);

    thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_workers);
        for partition in partitions.iter() {
            handles.push(s.spawn(move || {
                let mut child = HyperLogLog::<Regular>::default();
                for &idx in partition {
                    child.insert(&inputs[idx]);
                }
                child
            }));
        }

        for h in handles {
            workers.push(h.join().expect("regular HLL worker thread panicked"));
        }
    });

    let mut parent = HyperLogLog::<Regular>::default();
    for worker in workers.iter() {
        parent.merge(worker);
    }
    parent
}

fn bench_countmin_merge_compare(c: &mut Criterion) {
    let inputs = build_inputs(CM_COUNT_INPUTS);
    let mut group = c.benchmark_group("countmin_merge_compare");
    group.sample_size(10);
    group.throughput(Throughput::Elements(inputs.len() as u64));

    for &workers in &WORKER_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("octo_style", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let config = OctoConfig {
                        num_workers: w,
                        pin_cores: false,
                        queue_capacity: QUEUE_CAPACITY,
                    };
                    let result = run_octo(
                        &inputs,
                        &config,
                        |_| CmOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
                        || CmOctoParent {
                            sketch: CountMin::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
                        },
                    );
                    black_box(result.parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_full_merge", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let merged = run_regular_countmin_merge(&inputs, w);
                    black_box(merged);
                });
            },
        );
    }

    group.finish();
}

fn bench_count_merge_compare(c: &mut Criterion) {
    let inputs = build_inputs(CM_COUNT_INPUTS);
    let mut group = c.benchmark_group("count_merge_compare");
    group.sample_size(10);
    group.throughput(Throughput::Elements(inputs.len() as u64));

    for &workers in &WORKER_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("octo_style", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let config = OctoConfig {
                        num_workers: w,
                        pin_cores: false,
                        queue_capacity: QUEUE_CAPACITY,
                    };
                    let result = run_octo(
                        &inputs,
                        &config,
                        |_| CountOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
                        || CountOctoParent {
                            sketch: Count::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
                        },
                    );
                    black_box(result.parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_full_merge", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let merged = run_regular_count_merge(&inputs, w);
                    black_box(merged);
                });
            },
        );
    }

    group.finish();
}

fn bench_hll_merge_compare(c: &mut Criterion) {
    let inputs = build_inputs(HLL_INPUTS);
    let mut group = c.benchmark_group("hll_merge_compare");
    group.sample_size(10);
    group.throughput(Throughput::Elements(inputs.len() as u64));

    for &workers in &WORKER_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("octo_style", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let config = OctoConfig {
                        num_workers: w,
                        pin_cores: false,
                        queue_capacity: QUEUE_CAPACITY,
                    };
                    let result = run_octo(
                        &inputs,
                        &config,
                        |_| HllOctoWorker::new(),
                        || HllOctoParent {
                            sketch: HyperLogLog::<Regular>::default(),
                        },
                    );
                    black_box(result.parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_full_merge", workers),
            &workers,
            |b, &w| {
                b.iter(|| {
                    let merged = run_regular_hll_merge(&inputs, w);
                    black_box(merged);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    octo_speedup_benches,
    bench_countmin_merge_compare,
    bench_count_merge_compare,
    bench_hll_merge_compare
);
criterion_main!(octo_speedup_benches);
