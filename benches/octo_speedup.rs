use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use crossbeam_channel::{TryRecvError, bounded};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{
    CmDelta, Count, CountMin, CountOctoAggregator, CountOctoWorker, HllOctoAggregator,
    HllOctoWorker, HyperLogLog, OctoAggregator, OctoConfig, OctoWorker, Regular, RegularPath,
    SketchInput, Vector2D,
};
use std::sync::{Once, mpsc};
use std::thread;
use std::time::Duration;

const RNG_SEED: u64 = 0x0c70_2026_5eed_1234;
const CM_COUNT_ROWS: usize = 3;
const CM_COUNT_COLS: usize = 4096;
const CM_COUNT_INPUTS: usize = 1_000_000;
const HLL_INPUTS: usize = 500_000;
const DOMAIN_MASK: u64 = (1 << 20) - 1;
const QUEUE_CAPACITY: usize = 65_536;
const WORKER_COUNTS: [usize; 4] = [1, 2, 4, 8];
const MERGE_INTERVAL: usize = 10_000;

static SANITY_ONCE: Once = Once::new();

struct BenchCmOctoWorker {
    sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl BenchCmOctoWorker {
    fn new(rows: usize, cols: usize) -> Self {
        Self {
            sketch: CountMin::with_dimensions(rows, cols),
        }
    }
}

impl OctoWorker for BenchCmOctoWorker {
    type Delta = CmDelta;

    fn process<F>(&mut self, input: &SketchInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.sketch.insert_emit_delta(input, emit);
    }
}

struct BenchCmOctoAggregator {
    sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl OctoAggregator for BenchCmOctoAggregator {
    type Delta = CmDelta;

    fn apply(&mut self, delta: Self::Delta) {
        self.sketch.apply_delta(delta);
    }
}

fn build_inputs(sample_count: usize) -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED ^ (sample_count as u64));
    (0..sample_count)
        .map(|_| SketchInput::U64(rng.random::<u64>() & DOMAIN_MASK))
        .collect()
}

fn build_round_robin_shards(
    inputs: &[SketchInput<'static>],
    num_workers: usize,
) -> Vec<Vec<SketchInput<'static>>> {
    let mut shards = vec![Vec::new(); num_workers];
    for (idx, input) in inputs.iter().enumerate() {
        shards[idx % num_workers].push(input.clone());
    }
    shards
}

fn run_octo_sharded<W, P>(
    shards: &[Vec<SketchInput<'static>>],
    config: &OctoConfig,
    worker_factory: impl Fn(usize) -> W,
    parent_factory: impl FnOnce() -> P + Send,
) -> P
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + 'static,
{
    let num_workers = shards.len();
    thread::scope(|s| {
        let queue_capacity = config.queue_capacity.max(1);
        let mut worker_txs = Vec::with_capacity(num_workers);
        let mut worker_rxs = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = bounded::<W::Delta>(queue_capacity);
            worker_txs.push(tx);
            worker_rxs.push(Some(rx));
        }

        let aggregator = s.spawn(move || {
            let mut parent = parent_factory();
            if config.pin_cores {
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: num_workers });
            }

            let mut disconnected = 0usize;
            while disconnected < num_workers {
                let mut made_progress = false;
                for rx_slot in &mut worker_rxs {
                    let Some(rx) = rx_slot else {
                        continue;
                    };
                    match rx.try_recv() {
                        Ok(delta) => {
                            parent.apply(delta);
                            made_progress = true;
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            *rx_slot = None;
                            disconnected += 1;
                        }
                    }
                }
                if !made_progress {
                    std::hint::spin_loop();
                }
            }
            parent
        });

        for (worker_id, (shard, delta_tx_worker)) in
            shards.iter().zip(worker_txs.into_iter()).enumerate()
        {
            let mut worker = worker_factory(worker_id);
            let pin_cores = config.pin_cores;
            s.spawn(move || {
                if pin_cores {
                    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: worker_id });
                }
                for input in shard {
                    worker.process(input, &mut |delta| {
                        delta_tx_worker
                            .send(delta)
                            .expect("octo sharded aggregator dropped unexpectedly");
                    });
                }
            });
        }

        aggregator
            .join()
            .expect("octo sharded aggregator thread panicked")
    })
}

enum CountMinMergeMsg {
    Snapshot(CountMin<Vector2D<i32>, RegularPath>),
    Done,
}

enum CountMergeMsg {
    Snapshot(Count<Vector2D<i32>, RegularPath>),
    Done,
}

enum HllMergeMsg {
    Snapshot(HyperLogLog<Regular>),
    Done,
}

fn run_periodic_countmin_full_merge_sharded(
    shards: &[Vec<SketchInput<'static>>],
) -> CountMin<Vector2D<i32>, RegularPath> {
    let num_workers = shards.len();
    thread::scope(|s| {
        let (merge_tx, merge_rx) = mpsc::channel::<CountMinMergeMsg>();
        let aggregator = s.spawn(move || {
            let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                CM_COUNT_ROWS,
                CM_COUNT_COLS,
            );
            let mut finished_workers = 0usize;
            while finished_workers < num_workers {
                match merge_rx
                    .recv()
                    .expect("CountMin merge channel closed unexpectedly")
                {
                    CountMinMergeMsg::Snapshot(snapshot) => parent.merge(&snapshot),
                    CountMinMergeMsg::Done => finished_workers += 1,
                }
            }
            parent
        });

        for shard in shards {
            let merge_tx_worker = merge_tx.clone();
            s.spawn(move || {
                let mut child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                let mut local_inserts = 0usize;
                for input in shard {
                    child.insert(input);
                    local_inserts += 1;
                    if local_inserts % MERGE_INTERVAL == 0 {
                        merge_tx_worker
                            .send(CountMinMergeMsg::Snapshot(child))
                            .expect("CountMin aggregator dropped unexpectedly");
                        child = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                            CM_COUNT_ROWS,
                            CM_COUNT_COLS,
                        );
                    }
                }
                merge_tx_worker
                    .send(CountMinMergeMsg::Snapshot(child))
                    .expect("CountMin aggregator dropped unexpectedly");
                merge_tx_worker
                    .send(CountMinMergeMsg::Done)
                    .expect("CountMin aggregator dropped unexpectedly");
            });
        }
        drop(merge_tx);

        aggregator
            .join()
            .expect("CountMin periodic merge aggregator panicked")
    })
}

fn run_periodic_count_full_merge_sharded(
    shards: &[Vec<SketchInput<'static>>],
) -> Count<Vector2D<i32>, RegularPath> {
    let num_workers = shards.len();
    thread::scope(|s| {
        let (merge_tx, merge_rx) = mpsc::channel::<CountMergeMsg>();
        let aggregator = s.spawn(move || {
            let mut parent =
                Count::<Vector2D<i32>, RegularPath>::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS);
            let mut finished_workers = 0usize;
            while finished_workers < num_workers {
                match merge_rx
                    .recv()
                    .expect("Count merge channel closed unexpectedly")
                {
                    CountMergeMsg::Snapshot(snapshot) => parent.merge(&snapshot),
                    CountMergeMsg::Done => finished_workers += 1,
                }
            }
            parent
        });

        for shard in shards {
            let merge_tx_worker = merge_tx.clone();
            s.spawn(move || {
                let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    CM_COUNT_ROWS,
                    CM_COUNT_COLS,
                );
                let mut local_inserts = 0usize;
                for input in shard {
                    child.insert(input);
                    local_inserts += 1;
                    if local_inserts % MERGE_INTERVAL == 0 {
                        merge_tx_worker
                            .send(CountMergeMsg::Snapshot(child))
                            .expect("Count aggregator dropped unexpectedly");
                        child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                            CM_COUNT_ROWS,
                            CM_COUNT_COLS,
                        );
                    }
                }
                merge_tx_worker
                    .send(CountMergeMsg::Snapshot(child))
                    .expect("Count aggregator dropped unexpectedly");
                merge_tx_worker
                    .send(CountMergeMsg::Done)
                    .expect("Count aggregator dropped unexpectedly");
            });
        }
        drop(merge_tx);

        aggregator
            .join()
            .expect("Count periodic merge aggregator panicked")
    })
}

fn run_periodic_hll_full_merge_sharded(
    shards: &[Vec<SketchInput<'static>>],
) -> HyperLogLog<Regular> {
    let num_workers = shards.len();
    thread::scope(|s| {
        let (merge_tx, merge_rx) = mpsc::channel::<HllMergeMsg>();
        let aggregator = s.spawn(move || {
            let mut parent = HyperLogLog::<Regular>::default();
            let mut finished_workers = 0usize;
            while finished_workers < num_workers {
                match merge_rx
                    .recv()
                    .expect("HLL merge channel closed unexpectedly")
                {
                    HllMergeMsg::Snapshot(snapshot) => parent.merge(&snapshot),
                    HllMergeMsg::Done => finished_workers += 1,
                }
            }
            parent
        });

        for shard in shards {
            let merge_tx_worker = merge_tx.clone();
            s.spawn(move || {
                let mut child = HyperLogLog::<Regular>::default();
                let mut local_inserts = 0usize;
                for input in shard {
                    child.insert(input);
                    local_inserts += 1;
                    if local_inserts % MERGE_INTERVAL == 0 {
                        merge_tx_worker
                            .send(HllMergeMsg::Snapshot(child))
                            .expect("HLL aggregator dropped unexpectedly");
                        child = HyperLogLog::<Regular>::default();
                    }
                }
                merge_tx_worker
                    .send(HllMergeMsg::Snapshot(child))
                    .expect("HLL aggregator dropped unexpectedly");
                merge_tx_worker
                    .send(HllMergeMsg::Done)
                    .expect("HLL aggregator dropped unexpectedly");
            });
        }
        drop(merge_tx);

        aggregator
            .join()
            .expect("HLL periodic merge aggregator panicked")
    })
}

fn sanity_check_periodic_baselines() {
    SANITY_ONCE.call_once(|| {
        let inputs = build_inputs(20_000);
        let workers = 4;
        let shards = build_round_robin_shards(&inputs, workers);

        let config = OctoConfig {
            num_workers: workers,
            pin_cores: false,
            queue_capacity: QUEUE_CAPACITY,
        };

        let octo_cm = run_octo_sharded(
            &shards,
            &config,
            |_| BenchCmOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
            || BenchCmOctoAggregator {
                sketch: CountMin::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
            },
        )
        .sketch;
        let periodic_cm = run_periodic_countmin_full_merge_sharded(&shards);
        for key in [1_u64, 7, 42, 4097] {
            let input = SketchInput::U64(key);
            assert!(octo_cm.estimate(&input) <= periodic_cm.estimate(&input));
        }

        let octo_count = run_octo_sharded(
            &shards,
            &config,
            |_| CountOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
            || CountOctoAggregator {
                sketch: Count::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
            },
        )
        .sketch;
        let periodic_count = run_periodic_count_full_merge_sharded(&shards);
        for key in [1_u64, 7, 42, 4097] {
            let input = SketchInput::U64(key);
            let octo = octo_count.estimate(&input);
            let full = periodic_count.estimate(&input);
            assert!(octo.abs() <= full.abs() + 500.0);
        }

        let octo_hll = run_octo_sharded(
            &shards,
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoAggregator {
                sketch: HyperLogLog::<Regular>::default(),
            },
        )
        .sketch;
        let periodic_hll = run_periodic_hll_full_merge_sharded(&shards);
        assert!(octo_hll.estimate() <= periodic_hll.estimate());
    });
}

fn bench_countmin_merge_compare(c: &mut Criterion) {
    sanity_check_periodic_baselines();

    let inputs = build_inputs(CM_COUNT_INPUTS);
    let mut group = c.benchmark_group("countmin_merge_compare");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
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
        let shards = build_round_robin_shards(&inputs, workers);
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
                    let parent = run_octo_sharded(
                        &shards,
                        &config,
                        |_| BenchCmOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
                        || BenchCmOctoAggregator {
                            sketch: CountMin::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
                        },
                    );
                    black_box(parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_periodic_full_merge", workers),
            &workers,
            |b, _| {
                b.iter(|| {
                    let merged = run_periodic_countmin_full_merge_sharded(&shards);
                    black_box(merged);
                });
            },
        );
    }

    group.finish();
}

fn bench_count_merge_compare(c: &mut Criterion) {
    sanity_check_periodic_baselines();

    let inputs = build_inputs(CM_COUNT_INPUTS);
    let mut group = c.benchmark_group("count_merge_compare");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
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
        let shards = build_round_robin_shards(&inputs, workers);
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
                    let parent = run_octo_sharded(
                        &shards,
                        &config,
                        |_| CountOctoWorker::new(CM_COUNT_ROWS, CM_COUNT_COLS),
                        || CountOctoAggregator {
                            sketch: Count::with_dimensions(CM_COUNT_ROWS, CM_COUNT_COLS),
                        },
                    );
                    black_box(parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_periodic_full_merge", workers),
            &workers,
            |b, _| {
                b.iter(|| {
                    let merged = run_periodic_count_full_merge_sharded(&shards);
                    black_box(merged);
                });
            },
        );
    }

    group.finish();
}

fn bench_hll_merge_compare(c: &mut Criterion) {
    sanity_check_periodic_baselines();

    let inputs = build_inputs(HLL_INPUTS);
    let mut group = c.benchmark_group("hll_merge_compare");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
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
        let shards = build_round_robin_shards(&inputs, workers);
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
                    let parent = run_octo_sharded(
                        &shards,
                        &config,
                        |_| HllOctoWorker::new(),
                        || HllOctoAggregator {
                            sketch: HyperLogLog::<Regular>::default(),
                        },
                    );
                    black_box(parent.sketch);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regular_periodic_full_merge", workers),
            &workers,
            |b, _| {
                b.iter(|| {
                    let merged = run_periodic_hll_full_merge_sharded(&shards);
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
