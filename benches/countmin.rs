use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{
    CountMin, FastPath, FixedMatrix, RegularPath, SketchInput, Vector2D, hash128_seeded,
};

const SAMPLE_COUNT: usize = 1_000_000;
const RNG_SEED: u64 = 0x5eed_c0de_1234_5678;
const STACK_CMS_ROWS: usize = 5;
const STACK_CMS_COLS: usize = 2048;
const STACK_CMS_SIZE: usize = STACK_CMS_ROWS * STACK_CMS_COLS;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

struct StackCms {
    counts: [i32; STACK_CMS_SIZE],
}

impl StackCms {
    fn new() -> Self {
        Self {
            counts: [0_i32; STACK_CMS_SIZE],
        }
    }

    #[inline(always)]
    fn insert(&mut self, value: &SketchInput) {
        for r in 0..STACK_CMS_ROWS {
            let hashed = hash128_seeded(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % STACK_CMS_COLS;
            let idx = r * STACK_CMS_COLS + col;
            self.counts[idx] += 1;
        }
    }

    #[inline(always)]
    fn estimate(&self, value: &SketchInput) -> i32 {
        let mut min = i32::MAX;
        for r in 0..STACK_CMS_ROWS {
            let hashed = hash128_seeded(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % STACK_CMS_COLS;
            let idx = r * STACK_CMS_COLS + col;
            min = min.min(self.counts[idx]);
        }
        min
    }
}

fn build_keys() -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT).map(|_| rng.random::<i64>()).collect()
}

fn bench_countmin(c: &mut Criterion) {
    let keys = build_keys();
    let mut group = c.benchmark_group("countmin_default");

    group.bench_function("insert_only", |b| {
        b.iter_with_setup(
            || CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(5, 2048),
            |mut sketch| {
                for &key in &keys {
                    let input = SketchInput::I64(key);
                    sketch.insert(&input);
                }
                black_box(sketch);
            },
        );
    });

    group.bench_function("fast_insert_only", |b| {
        b.iter_with_setup(
            || CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048),
            |mut sketch| {
                for &key in &keys {
                    let input = SketchInput::I64(key);
                    sketch.insert(&input);
                }
                black_box(sketch);
            },
        );
    });

    group.bench_function("fixed_insert_only", |b| {
        b.iter_with_setup(
            CountMin::<FixedMatrix, RegularPath>::default,
            |mut sketch| {
                for &key in &keys {
                    let input = SketchInput::I64(key);
                    sketch.insert(&input);
                }
                black_box(sketch);
            },
        );
    });

    group.bench_function("fixed_fast_insert_only", |b| {
        b.iter_with_setup(CountMin::<FixedMatrix, FastPath>::default, |mut sketch| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                sketch.insert(&input);
            }
            black_box(sketch);
        });
    });

    group.bench_function("baseline_stack_insert", |b| {
        b.iter_with_setup(StackCms::new, |mut sketch| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                sketch.insert(&input);
            }
            black_box(sketch);
        });
    });

    let mut insert_prefilled = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(5, 2048);
    for &key in &keys {
        let input = SketchInput::I64(key);
        insert_prefilled.insert(&input);
    }

    let mut fast_prefilled = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048);
    for &key in &keys {
        let input = SketchInput::I64(key);
        fast_prefilled.insert(&input);
    }

    let mut fixed_insert_prefilled = CountMin::<FixedMatrix, RegularPath>::default();
    for &key in &keys {
        let input = SketchInput::I64(key);
        fixed_insert_prefilled.insert(&input);
    }

    let mut fixed_fast_prefilled = CountMin::<FixedMatrix, FastPath>::default();
    for &key in &keys {
        let input = SketchInput::I64(key);
        fixed_fast_prefilled.insert(&input);
    }

    let mut stack_prefilled = StackCms::new();
    for &key in &keys {
        let input = SketchInput::I64(key);
        stack_prefilled.insert(&input);
    }

    group.bench_function("estimate", |b| {
        b.iter(|| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                black_box(insert_prefilled.estimate(&input));
            }
        });
    });

    group.bench_function("fast_estimate", |b| {
        b.iter(|| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                black_box(fast_prefilled.estimate(&input));
            }
        });
    });

    group.bench_function("fixed_estimate", |b| {
        b.iter(|| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                black_box(fixed_insert_prefilled.estimate(&input));
            }
        });
    });

    group.bench_function("fixed_fast_estimate", |b| {
        b.iter(|| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                black_box(fixed_fast_prefilled.estimate(&input));
            }
        });
    });

    group.bench_function("baseline_stack_estimate", |b| {
        b.iter(|| {
            for &key in &keys {
                let input = SketchInput::I64(key);
                black_box(stack_prefilled.estimate(&input));
            }
        });
    });

    group.finish();
}

criterion_group!(countmin_benches, bench_countmin);
criterion_main!(countmin_benches);
