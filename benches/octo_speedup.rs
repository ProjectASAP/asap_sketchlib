use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{
    CmOctoParent, CmOctoWorker, Count, CountMin, CountOctoParent, CountOctoWorker, HllOctoParent,
    HllOctoWorker, HyperLogLog, OctoConfig, Regular, RegularPath, SketchInput, Vector2D,
    hash64_seeded, run_octo,
};
use std::sync::mpsc;
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
const MERGE_INTERVAL: usize = 10_000;

fn build_inputs(sample_count: usize) -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED ^ (sample_count as u64));
    (0..sample_count)
        .map(|_| SketchInput::U64(rng.random::<u64>() & DOMAIN_MASK))
        .collect()
}

#[inline(always)]
fn should_merge(number: usize, interval: usize, thread_num: usize, thread_id: usize) -> bool {
    number != 0 && number % (interval * thread_num) == interval * thread_id
}

fn run_regular_countmin_merge(
    inputs: &[SketchInput<'_>],
    num_workers: usize,
) -> CountMin<Vector2D<i32>, RegularPath> {
    let mut workers = Vec::new();

    thread::scope(|s| {
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let (tx, rx) = mpsc::channel::<Option<usize>>();
            worker_txs.push(tx);
            handles.push(s.spawn(move || {
                let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                let mut partials = Vec::new();
                let mut number = 0usize;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        Some(idx) => {
                            child.insert(&inputs[idx]);
                            number += 1;
                            if should_merge(number, MERGE_INTERVAL, num_workers, worker_id) {
                                partials.push(child);
                                child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                                    CM_COUNT_ROWS,
                                    CM_COUNT_COLS,
                                );
                            }
                        }
                        None => break,
                    }
                }
                partials.push(child);
                partials
            }));
        }

        s.spawn(move || {
            for (idx, item) in inputs.iter().enumerate() {
                let h = hash64_seeded(PARTITION_SEED, item) as usize;
                let worker_id = h % num_workers;
                worker_txs[worker_id]
                    .send(Some(idx))
                    .expect("regular CountMin worker receiver dropped unexpectedly");
            }
            for tx in worker_txs {
                let _ = tx.send(None);
            }
        });

        for h in handles {
            workers.extend(h.join().expect("regular CountMin worker thread panicked"));
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
    let mut workers = Vec::new();

    thread::scope(|s| {
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let (tx, rx) = mpsc::channel::<Option<usize>>();
            worker_txs.push(tx);
            handles.push(s.spawn(move || {
                let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                let mut partials = Vec::new();
                let mut number = 0usize;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        Some(idx) => {
                            child.insert(&inputs[idx]);
                            number += 1;
                            if should_merge(number, MERGE_INTERVAL, num_workers, worker_id) {
                                partials.push(child);
                                child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                                    CM_COUNT_ROWS,
                                    CM_COUNT_COLS,
                                );
                            }
                        }
                        None => break,
                    }
                }
                partials.push(child);
                partials
            }));
        }

        s.spawn(move || {
            for (idx, item) in inputs.iter().enumerate() {
                let h = hash64_seeded(PARTITION_SEED, item) as usize;
                let worker_id = h % num_workers;
                worker_txs[worker_id]
                    .send(Some(idx))
                    .expect("regular Count worker receiver dropped unexpectedly");
            }
            for tx in worker_txs {
                let _ = tx.send(None);
            }
        });

        for h in handles {
            workers.extend(h.join().expect("regular Count worker thread panicked"));
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
    let mut workers = Vec::new();

    thread::scope(|s| {
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let (tx, rx) = mpsc::channel::<Option<usize>>();
            worker_txs.push(tx);
            handles.push(s.spawn(move || {
                let mut child = HyperLogLog::<Regular>::default();
                let mut partials = Vec::new();
                let mut number = 0usize;
                while let Ok(msg) = rx.recv() {
                    match msg {
                        Some(idx) => {
                            child.insert(&inputs[idx]);
                            number += 1;
                            if should_merge(number, MERGE_INTERVAL, num_workers, worker_id) {
                                partials.push(child);
                                child = HyperLogLog::<Regular>::default();
                            }
                        }
                        None => break,
                    }
                }
                partials.push(child);
                partials
            }));
        }

        s.spawn(move || {
            for (idx, item) in inputs.iter().enumerate() {
                let h = hash64_seeded(PARTITION_SEED, item) as usize;
                let worker_id = h % num_workers;
                worker_txs[worker_id]
                    .send(Some(idx))
                    .expect("regular HLL worker receiver dropped unexpectedly");
            }
            for tx in worker_txs {
                let _ = tx.send(None);
            }
        });

        for h in handles {
            workers.extend(h.join().expect("regular HLL worker thread panicked"));
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

    group.bench_function("single_thread_insert", |b| {
        b.iter_with_setup(
            || {
                CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                )
            },
            |mut sketch| {
                for input in &inputs {
                    sketch.insert(input);
                }
                black_box(sketch);
            },
        );
    });

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

    group.bench_function("single_thread_insert", |b| {
        b.iter_with_setup(
            || Count::<Vector2D<i32>, RegularPath>::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
            |mut sketch| {
                for input in &inputs {
                    sketch.insert(input);
                }
                black_box(sketch);
            },
        );
    });

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

    group.bench_function("single_thread_insert", |b| {
        b.iter_with_setup(HyperLogLog::<Regular>::default, |mut sketch| {
            for input in &inputs {
                sketch.insert(input);
            }
            black_box(sketch);
        });
    });

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
