use clap::Parser;
use rand::SeedableRng;
use rand::distr::{Distribution, Uniform, weighted::WeightedIndex};
use rand::rngs::StdRng;
use sketchlib_rust::{CountMin, FastPath, RegularPath, SketchInput, Vector2D};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const DEFAULT_ROWS: usize = 3;
const DEFAULT_COLS: usize = 4096;
const DEFAULT_OUT_DIR: &str = "data/results/cms_guarantee";
const DEFAULT_SEED_BASE: u64 = 0x5eed_c0de_1bad_b002;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum DistKind {
    Uniform,
    Zipf,
}

impl DistKind {
    fn as_str(self) -> &'static str {
        match self {
            DistKind::Uniform => "uniform",
            DistKind::Zipf => "zipf",
        }
    }
}

impl fmt::Display for DistKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum PathKind {
    Regular,
    Fast,
}

impl PathKind {
    fn as_str(self) -> &'static str {
        match self {
            PathKind::Regular => "regular",
            PathKind::Fast => "fast",
        }
    }
}

impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Parser, Debug)]
#[command(name = "cms_guarantee_plot")]
#[command(about = "Generate CMS epsilon/delta guarantee metrics")]
struct Cli {
    #[arg(long, default_value_t = DEFAULT_ROWS)]
    rows: usize,

    #[arg(long, default_value_t = DEFAULT_COLS)]
    cols: usize,

    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    out_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_SEED_BASE)]
    seed_base: u64,

    #[arg(long, default_value_t = 1.1)]
    zipf_exponent: f64,

    #[arg(long, default_value_t = 8192)]
    zipf_domain: usize,

    #[arg(long, default_value_t = 100.0)]
    uniform_min: f64,

    #[arg(long, default_value_t = 1000.0)]
    uniform_max: f64,

    #[arg(long)]
    max_n: Option<usize>,
}

#[derive(Clone, Debug)]
struct BoundStats {
    error_bound: f64,
    required_within_lower_bound: f64,
}

#[derive(Clone, Debug)]
struct MetricRow {
    path: PathKind,
    distribution: DistKind,
    n: usize,
    rows: usize,
    cols: usize,
    epsilon: f64,
    delta: f64,
    error_bound: f64,
    key_count: usize,
    within_count: usize,
    within_rate: f64,
    required_within_lower_bound: f64,
    pass: bool,
    max_abs_error: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    run(&cli)
}

fn run(cli: &Cli) -> Result<(), Box<dyn Error>> {
    validate_cli(cli)?;

    let n_values = n_grid(cli.max_n);
    if n_values.is_empty() {
        return Err("no stream size N remains after applying --max-n".into());
    }

    let epsilon = std::f64::consts::E / cli.cols as f64;
    let delta = (-(cli.rows as f64)).exp();
    let mut metrics = Vec::new();

    for &distribution in &[DistKind::Uniform, DistKind::Zipf] {
        for &n in &n_values {
            for &path in &[PathKind::Regular, PathKind::Fast] {
                let seed = derive_seed(cli.seed_base, distribution, path, n);
                let row = run_one(cli, path, distribution, n, seed, epsilon, delta);
                metrics.push(row);
            }
        }
    }

    let run_dir = make_run_dir(&cli.out_dir)?;
    write_metrics_csv(&run_dir.join("metrics.csv"), &metrics)?;

    println!("CMS guarantee artifacts written to {}", run_dir.display());
    println!(
        "generate plots with: python3 scripts/plot_cms_guarantee.py --summary {}/metrics.csv --out-dir {}",
        run_dir.display(),
        run_dir.display()
    );
    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<(), Box<dyn Error>> {
    if cli.rows == 0 {
        return Err("--rows must be > 0".into());
    }
    if cli.cols == 0 {
        return Err("--cols must be > 0".into());
    }
    if cli.zipf_domain == 0 {
        return Err("--zipf-domain must be > 0".into());
    }
    if !(cli.uniform_min <= cli.uniform_max) {
        return Err("--uniform-min must be <= --uniform-max".into());
    }
    if !(cli.zipf_exponent.is_finite() && cli.zipf_exponent > 0.0) {
        return Err("--zipf-exponent must be finite and > 0".into());
    }
    Ok(())
}

fn n_grid(max_n: Option<usize>) -> Vec<usize> {
    let base = vec![100, 1_000, 10_000, 100_000, 1_000_000];
    match max_n {
        Some(limit) => base.into_iter().filter(|n| *n <= limit).collect(),
        None => base,
    }
}

fn derive_seed(base: u64, distribution: DistKind, path: PathKind, n: usize) -> u64 {
    let dist_tag = match distribution {
        DistKind::Uniform => 0xA5A5_A5A5_A5A5_A5A5,
        DistKind::Zipf => 0x5A5A_5A5A_5A5A_5A5A,
    };
    let path_tag = match path {
        PathKind::Regular => 0x1111_1111_1111_1111,
        PathKind::Fast => 0x2222_2222_2222_2222,
    };

    base ^ dist_tag ^ path_tag ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn acceptable_bound(epsilon: f64, delta: f64, n: usize, key_count: usize) -> BoundStats {
    BoundStats {
        error_bound: epsilon * n as f64,
        required_within_lower_bound: key_count as f64 * (1.0 - delta),
    }
}

fn pass_test_style(within_count: usize, required_within_lower_bound: f64) -> bool {
    within_count as f64 > required_within_lower_bound
}

fn run_one(
    cli: &Cli,
    path: PathKind,
    distribution: DistKind,
    n: usize,
    seed: u64,
    epsilon: f64,
    delta: f64,
) -> MetricRow {
    match (path, distribution) {
        (PathKind::Regular, DistKind::Zipf) => {
            let (sketch, truth) = run_zipf_stream_regular(
                cli.rows,
                cli.cols,
                cli.zipf_domain,
                cli.zipf_exponent,
                n,
                seed,
            );
            evaluate_row(
                path,
                distribution,
                n,
                cli.rows,
                cli.cols,
                epsilon,
                delta,
                &truth,
                |key| sketch.estimate(&SketchInput::U64(key)),
            )
        }
        (PathKind::Fast, DistKind::Zipf) => {
            let (sketch, truth) = run_zipf_stream_fast(
                cli.rows,
                cli.cols,
                cli.zipf_domain,
                cli.zipf_exponent,
                n,
                seed,
            );
            evaluate_row(
                path,
                distribution,
                n,
                cli.rows,
                cli.cols,
                epsilon,
                delta,
                &truth,
                |key| sketch.estimate(&SketchInput::U64(key)),
            )
        }
        (PathKind::Regular, DistKind::Uniform) => {
            let (sketch, truth) = run_uniform_stream_regular(
                cli.rows,
                cli.cols,
                cli.uniform_min,
                cli.uniform_max,
                n,
                seed,
            );
            evaluate_row(
                path,
                distribution,
                n,
                cli.rows,
                cli.cols,
                epsilon,
                delta,
                &truth,
                |key| sketch.estimate(&SketchInput::F64(f64::from_bits(key))),
            )
        }
        (PathKind::Fast, DistKind::Uniform) => {
            let (sketch, truth) = run_uniform_stream_fast(
                cli.rows,
                cli.cols,
                cli.uniform_min,
                cli.uniform_max,
                n,
                seed,
            );
            evaluate_row(
                path,
                distribution,
                n,
                cli.rows,
                cli.cols,
                epsilon,
                delta,
                &truth,
                |key| sketch.estimate(&SketchInput::F64(f64::from_bits(key))),
            )
        }
    }
}

fn evaluate_row<F>(
    path: PathKind,
    distribution: DistKind,
    n: usize,
    rows: usize,
    cols: usize,
    epsilon: f64,
    delta: f64,
    truth: &HashMap<u64, i32>,
    estimate_fn: F,
) -> MetricRow
where
    F: Fn(u64) -> i32,
{
    let key_count = truth.len();
    let bound_stats = acceptable_bound(epsilon, delta, n, key_count);

    let mut within_count = 0usize;
    let mut max_abs_error = 0.0f64;

    for (&key, &true_count) in truth {
        let est = estimate_fn(key);
        let abs_error = est.abs_diff(true_count) as f64;
        if abs_error < bound_stats.error_bound {
            within_count += 1;
        }
        if abs_error > max_abs_error {
            max_abs_error = abs_error;
        }
    }

    let within_rate = if key_count == 0 {
        0.0
    } else {
        within_count as f64 / key_count as f64
    };

    let pass = pass_test_style(within_count, bound_stats.required_within_lower_bound);

    MetricRow {
        path,
        distribution,
        n,
        rows,
        cols,
        epsilon,
        delta,
        error_bound: bound_stats.error_bound,
        key_count,
        within_count,
        within_rate,
        required_within_lower_bound: bound_stats.required_within_lower_bound,
        pass,
        max_abs_error,
    }
}

fn run_zipf_stream_regular(
    rows: usize,
    cols: usize,
    domain: usize,
    exponent: f64,
    samples: usize,
    seed: u64,
) -> (CountMin<Vector2D<i32>, RegularPath>, HashMap<u64, i32>) {
    let mut truth = HashMap::<u64, i32>::new();
    let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

    for value in sample_zipf_u64(domain, exponent, samples, seed) {
        let key = SketchInput::U64(value);
        sketch.insert(&key);
        *truth.entry(value).or_insert(0) += 1;
    }

    (sketch, truth)
}

fn run_zipf_stream_fast(
    rows: usize,
    cols: usize,
    domain: usize,
    exponent: f64,
    samples: usize,
    seed: u64,
) -> (CountMin<Vector2D<i32>, FastPath>, HashMap<u64, i32>) {
    let mut truth = HashMap::<u64, i32>::new();
    let mut sketch = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

    for value in sample_zipf_u64(domain, exponent, samples, seed) {
        let key = SketchInput::U64(value);
        sketch.insert(&key);
        *truth.entry(value).or_insert(0) += 1;
    }

    (sketch, truth)
}

fn run_uniform_stream_regular(
    rows: usize,
    cols: usize,
    min: f64,
    max: f64,
    samples: usize,
    seed: u64,
) -> (CountMin<Vector2D<i32>, RegularPath>, HashMap<u64, i32>) {
    let mut truth = HashMap::<u64, i32>::new();
    let mut sketch = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);

    for value in sample_uniform_f64(min, max, samples, seed) {
        let key = SketchInput::F64(value);
        sketch.insert(&key);
        *truth.entry(value.to_bits()).or_insert(0) += 1;
    }

    (sketch, truth)
}

fn run_uniform_stream_fast(
    rows: usize,
    cols: usize,
    min: f64,
    max: f64,
    samples: usize,
    seed: u64,
) -> (CountMin<Vector2D<i32>, FastPath>, HashMap<u64, i32>) {
    let mut truth = HashMap::<u64, i32>::new();
    let mut sketch = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);

    for value in sample_uniform_f64(min, max, samples, seed) {
        let key = SketchInput::F64(value);
        sketch.insert(&key);
        *truth.entry(value.to_bits()).or_insert(0) += 1;
    }

    (sketch, truth)
}

fn make_run_dir(base: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let run_dir = base.join("run");
    create_dir_all(&run_dir)?;
    Ok(run_dir)
}

fn write_metrics_csv(path: &Path, rows: &[MetricRow]) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "path,distribution,n,rows,cols,epsilon,delta,error_bound,key_count,within_count,within_rate,required_within_lower_bound,pass,max_abs_error"
    )?;

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{:.12},{:.12},{:.12},{},{},{:.12},{:.12},{},{:.12}",
            row.path,
            row.distribution,
            row.n,
            row.rows,
            row.cols,
            row.epsilon,
            row.delta,
            row.error_bound,
            row.key_count,
            row.within_count,
            row.within_rate,
            row.required_within_lower_bound,
            row.pass,
            row.max_abs_error,
        )?;
    }

    writer.flush()?;
    Ok(())
}

fn sample_uniform_f64(min: f64, max: f64, sample_size: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Uniform::new_inclusive(min, max).expect("uniform bounds should be valid");
    (0..sample_size).map(|_| dist.sample(&mut rng)).collect()
}

fn sample_zipf_u64(domain: usize, exponent: f64, sample_size: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let weights: Vec<f64> = (1..=domain)
        .map(|k| 1.0 / (k as f64).powf(exponent))
        .collect();
    let dist = WeightedIndex::new(weights).expect("zipf weights should be valid");
    (0..sample_size)
        .map(|_| dist.sample(&mut rng) as u64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn acceptable_bound_formula_is_correct() {
        let epsilon = std::f64::consts::E / 4096.0;
        let delta = (-3.0f64).exp();
        let stats = acceptable_bound(epsilon, delta, 200_000, 1000);

        assert!((stats.error_bound - (epsilon * 200_000.0)).abs() < 1e-12);
        assert!((stats.required_within_lower_bound - (1000.0 * (1.0 - delta))).abs() < 1e-12);
    }

    #[test]
    fn pass_rule_matches_test_style_strict_gt() {
        let required = 95.0;
        assert!(!pass_test_style(95, required));
        assert!(pass_test_style(96, required));
    }

    #[test]
    fn n_grid_respects_max_n() {
        let limited = n_grid(Some(100_000));
        assert_eq!(limited, vec![100, 1_000, 10_000, 100_000]);
    }

    #[test]
    fn path_parity_sanity_writes_both_paths() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let out_root = std::env::temp_dir().join(format!("cms_guarantee_parity_{unique}"));

        let cli = Cli {
            rows: DEFAULT_ROWS,
            cols: DEFAULT_COLS,
            out_dir: out_root.clone(),
            seed_base: DEFAULT_SEED_BASE,
            zipf_exponent: 1.1,
            zipf_domain: 64,
            uniform_min: 100.0,
            uniform_max: 120.0,
            max_n: Some(100),
        };

        run(&cli).expect("run should succeed");

        let metrics_path = out_root.join("run").join("metrics.csv");
        let content = fs::read_to_string(&metrics_path).expect("metrics.csv readable");
        let mut lines = content.lines();
        let header = lines.next().expect("header line");
        assert_eq!(
            header,
            "path,distribution,n,rows,cols,epsilon,delta,error_bound,key_count,within_count,within_rate,required_within_lower_bound,pass,max_abs_error"
        );

        let records: Vec<&str> = lines.collect();
        assert_eq!(records.len(), 4);

        let mut seen_paths = HashSet::new();
        for record in records {
            let cols: Vec<&str> = record.split(',').collect();
            assert_eq!(cols.len(), 14);
            seen_paths.insert(cols[0].to_string());

            let epsilon: f64 = cols[5].parse().expect("epsilon parse");
            let delta: f64 = cols[6].parse().expect("delta parse");
            let error_bound: f64 = cols[7].parse().expect("error_bound parse");
            let within_rate: f64 = cols[10].parse().expect("within_rate parse");
            let max_abs_error: f64 = cols[13].parse().expect("max_abs_error parse");

            assert!(epsilon.is_finite() && epsilon > 0.0);
            assert!(delta.is_finite() && delta > 0.0);
            assert!(error_bound.is_finite() && error_bound > 0.0);
            assert!(within_rate.is_finite() && within_rate >= 0.0 && within_rate <= 1.0);
            assert!(max_abs_error.is_finite() && max_abs_error >= 0.0);
        }

        assert!(seen_paths.contains("regular"));
        assert!(seen_paths.contains("fast"));

        let _ = fs::remove_dir_all(out_root);
    }

    #[test]
    fn quick_cli_smoke_creates_only_metrics_csv() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let out_root = std::env::temp_dir().join(format!("cms_guarantee_smoke_{unique}"));
        let cli = Cli {
            rows: DEFAULT_ROWS,
            cols: DEFAULT_COLS,
            out_dir: out_root.clone(),
            seed_base: DEFAULT_SEED_BASE,
            zipf_exponent: 1.1,
            zipf_domain: 128,
            uniform_min: 100.0,
            uniform_max: 1000.0,
            max_n: Some(1_000),
        };

        run(&cli).expect("smoke run should succeed");

        let entries: Vec<_> = fs::read_dir(&out_root)
            .expect("read out root")
            .filter_map(Result::ok)
            .collect();
        assert_eq!(entries.len(), 1, "expected exactly one run directory");
        let run_dir = entries[0].path();

        assert!(run_dir.join("metrics.csv").is_file());
        assert!(!run_dir.join("summary.csv").exists());

        let _ = fs::remove_dir_all(out_root);
    }
}
