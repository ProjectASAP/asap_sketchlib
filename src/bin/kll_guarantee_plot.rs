use clap::Parser;
use rand::SeedableRng;
use rand::distr::{Distribution, Uniform, weighted::WeightedIndex};
use rand::rngs::StdRng;
use sketchlib_rust::{KLL, SketchInput};
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const DEFAULT_K: i32 = 200;
const DEFAULT_TOLERANCE: f64 = 0.02;
const DEFAULT_OUT_DIR: &str = "data/results/kll_guarantee";
const DEFAULT_SEED_BASE: u64 = 0x7eed_c0de_1bad_b002;
const DEFAULT_SAMPLE_SIZES: &str = "1000,5000,20000,100000,1000000,5000000";
const DEFAULT_QUANTILES: &str = "0.0,0.10,0.25,0.50,0.75,0.90,1.0";
const DEFAULT_UNIFORM_MIN: f64 = 0.0;
const DEFAULT_UNIFORM_MAX: f64 = 100_000_000.0;
const DEFAULT_ZIPF_MIN: f64 = 1_000_000.0;
const DEFAULT_ZIPF_MAX: f64 = 10_000_000.0;
const DEFAULT_ZIPF_DOMAIN: usize = 8192;
const DEFAULT_ZIPF_EXPONENT: f64 = 1.1;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

#[derive(Parser, Debug)]
#[command(name = "kll_guarantee_plot")]
#[command(about = "Generate KLL quantile tolerance guarantee metrics")]
struct Cli {
    #[arg(long, default_value_t = DEFAULT_K)]
    k: i32,

    #[arg(long, default_value_t = DEFAULT_TOLERANCE)]
    tolerance: f64,

    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    out_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_SEED_BASE)]
    seed_base: u64,

    #[arg(long, default_value_t = DEFAULT_UNIFORM_MIN)]
    uniform_min: f64,

    #[arg(long, default_value_t = DEFAULT_UNIFORM_MAX)]
    uniform_max: f64,

    #[arg(long, default_value_t = DEFAULT_ZIPF_MIN)]
    zipf_min: f64,

    #[arg(long, default_value_t = DEFAULT_ZIPF_MAX)]
    zipf_max: f64,

    #[arg(long, default_value_t = DEFAULT_ZIPF_DOMAIN)]
    zipf_domain: usize,

    #[arg(long, default_value_t = DEFAULT_ZIPF_EXPONENT)]
    zipf_exponent: f64,

    #[arg(long, default_value = DEFAULT_SAMPLE_SIZES)]
    sample_sizes: String,

    #[arg(long, default_value = DEFAULT_QUANTILES)]
    quantiles: String,
}

#[derive(Clone, Debug)]
struct QuantileSpec {
    q: f64,
    label: String,
}

#[derive(Clone, Debug)]
struct MetricRow {
    distribution: String,
    n: usize,
    k: i32,
    tolerance: f64,
    total_quantiles: usize,
    within_count: usize,
    within_rate: f64,
    required_count: usize,
    pass: bool,
    max_rank_error: f64,
}

#[derive(Clone, Debug)]
struct QuantileRow {
    distribution: String,
    n: usize,
    k: i32,
    tolerance: f64,
    quantile: f64,
    label: String,
    lower_q: f64,
    upper_q: f64,
    truth_lower: f64,
    truth_upper: f64,
    estimate: f64,
    in_bound: bool,
    rank_error: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    run(&cli)
}

fn run(cli: &Cli) -> Result<(), Box<dyn Error>> {
    validate_cli(cli)?;

    let sample_sizes = parse_usize_csv(&cli.sample_sizes)?;
    let quantiles = parse_quantiles_csv(&cli.quantiles)?;

    let mut metrics = Vec::new();
    let mut detail = Vec::new();

    for &distribution in &[DistKind::Uniform, DistKind::Zipf] {
        for (idx, &n) in sample_sizes.iter().enumerate() {
            let seed = derive_seed(cli.seed_base, distribution, idx as u64);
            let (sketch, mut values) = build_kll_with_distribution(cli, n, distribution, seed)?;
            values.sort_by(f64::total_cmp);
            let cdf = sketch.cdf();

            let mut within_count = 0usize;
            let mut max_rank_error = 0.0f64;

            for q in &quantiles {
                let lower_q = (q.q - cli.tolerance).max(0.0);
                let upper_q = (q.q + cli.tolerance).min(1.0);
                let truth_lower = quantile_from_sorted(&values, lower_q);
                let truth_upper = quantile_from_sorted(&values, upper_q);
                let estimate = cdf.query(q.q);
                let in_bound = (truth_lower..=truth_upper).contains(&estimate);
                if in_bound {
                    within_count += 1;
                }

                let rank = empirical_rank(&values, estimate);
                let rank_error = (rank - q.q).abs();
                if rank_error > max_rank_error {
                    max_rank_error = rank_error;
                }

                detail.push(QuantileRow {
                    distribution: distribution.as_str().to_string(),
                    n,
                    k: cli.k,
                    tolerance: cli.tolerance,
                    quantile: q.q,
                    label: q.label.clone(),
                    lower_q,
                    upper_q,
                    truth_lower,
                    truth_upper,
                    estimate,
                    in_bound,
                    rank_error,
                });
            }

            let total_quantiles = quantiles.len();
            let required_count = total_quantiles;
            let pass = within_count == required_count;
            let within_rate = within_count as f64 / total_quantiles as f64;

            metrics.push(MetricRow {
                distribution: distribution.as_str().to_string(),
                n,
                k: cli.k,
                tolerance: cli.tolerance,
                total_quantiles,
                within_count,
                within_rate,
                required_count,
                pass,
                max_rank_error,
            });
        }
    }

    let run_dir = make_run_dir(&cli.out_dir)?;
    write_metrics_csv(&run_dir.join("metrics.csv"), &metrics)?;
    write_quantiles_csv(&run_dir.join("quantiles.csv"), &detail)?;

    println!("KLL guarantee artifacts written to {}", run_dir.display());
    println!(
        "generate plots with: python3 scripts/plot_kll_guarantee.py --metrics {}/metrics.csv --quantiles {}/quantiles.csv --out-dir {}",
        run_dir.display(),
        run_dir.display(),
        run_dir.display()
    );

    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<(), Box<dyn Error>> {
    if cli.k <= 0 {
        return Err("--k must be > 0".into());
    }
    if !(cli.tolerance.is_finite() && (0.0..=1.0).contains(&cli.tolerance)) {
        return Err("--tolerance must be finite and in [0, 1]".into());
    }
    if !(cli.uniform_min <= cli.uniform_max) {
        return Err("--uniform-min must be <= --uniform-max".into());
    }
    if !(cli.zipf_min <= cli.zipf_max) {
        return Err("--zipf-min must be <= --zipf-max".into());
    }
    if cli.zipf_domain == 0 {
        return Err("--zipf-domain must be > 0".into());
    }
    if !(cli.zipf_exponent.is_finite() && cli.zipf_exponent > 0.0) {
        return Err("--zipf-exponent must be finite and > 0".into());
    }
    Ok(())
}

fn parse_usize_csv(input: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    let mut out = Vec::new();
    for raw in input.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let n = token.parse::<usize>()?;
        if n == 0 {
            return Err("sample sizes must all be > 0".into());
        }
        out.push(n);
    }
    if out.is_empty() {
        return Err("--sample-sizes must include at least one positive integer".into());
    }
    Ok(out)
}

fn quantile_label(q: f64) -> String {
    let eps = 1e-12;
    if (q - 0.0).abs() < eps {
        return "min".to_string();
    }
    if (q - 1.0).abs() < eps {
        return "max".to_string();
    }
    let pct = (q * 100.0).round() as i64;
    format!("p{pct}")
}

fn parse_quantiles_csv(input: &str) -> Result<Vec<QuantileSpec>, Box<dyn Error>> {
    let mut out = Vec::new();
    for raw in input.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let q = token.parse::<f64>()?;
        if !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return Err(format!("quantile {q} must be finite and in [0,1]").into());
        }
        out.push(QuantileSpec {
            q,
            label: quantile_label(q),
        });
    }
    if out.is_empty() {
        return Err("--quantiles must include at least one value in [0,1]".into());
    }
    Ok(out)
}

fn derive_seed(base: u64, distribution: DistKind, sample_idx: u64) -> u64 {
    let dist_tag = match distribution {
        DistKind::Uniform => 0xA5A5_A5A5_A5A5_A5A5,
        DistKind::Zipf => 0xB4B4_B4B4_B4B4_B4B4,
    };
    base ^ dist_tag ^ sample_idx.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn build_kll_with_distribution(
    cli: &Cli,
    sample_size: usize,
    distribution: DistKind,
    seed: u64,
) -> Result<(KLL, Vec<f64>), Box<dyn Error>> {
    let mut sketch = KLL::init_kll(cli.k);
    let values = match distribution {
        DistKind::Uniform => {
            sample_uniform_f64(cli.uniform_min, cli.uniform_max, sample_size, seed)
        }
        DistKind::Zipf => sample_zipf_f64(
            cli.zipf_min,
            cli.zipf_max,
            cli.zipf_domain,
            cli.zipf_exponent,
            sample_size,
            seed,
        ),
    };

    for &value in &values {
        sketch
            .update(&SketchInput::F64(value))
            .map_err(|e| std::io::Error::other(format!("failed to update KLL: {e}")))?;
    }

    Ok((sketch, values))
}

fn quantile_from_sorted(data: &[f64], quantile: f64) -> f64 {
    assert!(!data.is_empty(), "data set must not be empty");
    if quantile <= 0.0 {
        return data[0];
    }
    if quantile >= 1.0 {
        return data[data.len() - 1];
    }
    let n = data.len();
    let idx = ((quantile * n as f64).ceil() as isize - 1).clamp(0, (n - 1) as isize) as usize;
    data[idx]
}

fn empirical_rank(sorted_values: &[f64], estimate: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let count = sorted_values.partition_point(|x| *x <= estimate);
    count as f64 / sorted_values.len() as f64
}

fn sample_uniform_f64(min: f64, max: f64, sample_size: usize, seed: u64) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let dist = Uniform::new_inclusive(min, max).expect("uniform bounds should be valid");
    (0..sample_size).map(|_| dist.sample(&mut rng)).collect()
}

fn sample_zipf_f64(
    min: f64,
    max: f64,
    domain: usize,
    exponent: f64,
    sample_size: usize,
    seed: u64,
) -> Vec<f64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let weights: Vec<f64> = (1..=domain)
        .map(|k| 1.0 / (k as f64).powf(exponent))
        .collect();
    let dist = WeightedIndex::new(weights).expect("zipf weights should be valid");

    let step = if domain > 1 {
        (max - min) / (domain as f64 - 1.0)
    } else {
        0.0
    };

    (0..sample_size)
        .map(|_| {
            let idx = dist.sample(&mut rng);
            min + step * idx as f64
        })
        .collect()
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
        "distribution,n,k,tolerance,total_quantiles,within_count,within_rate,required_count,pass,max_rank_error"
    )?;

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{:.12},{},{},{:.12},{},{},{:.12}",
            row.distribution,
            row.n,
            row.k,
            row.tolerance,
            row.total_quantiles,
            row.within_count,
            row.within_rate,
            row.required_count,
            row.pass,
            row.max_rank_error
        )?;
    }

    writer.flush()?;
    Ok(())
}

fn write_quantiles_csv(path: &Path, rows: &[QuantileRow]) -> Result<(), Box<dyn Error>> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "distribution,n,k,tolerance,quantile,label,lower_q,upper_q,truth_lower,truth_upper,estimate,in_bound,rank_error"
    )?;

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{:.12},{:.12},{},{:.12},{:.12},{:.12},{:.12},{:.12},{},{:.12}",
            row.distribution,
            row.n,
            row.k,
            row.tolerance,
            row.quantile,
            row.label,
            row.lower_q,
            row.upper_q,
            row.truth_lower,
            row.truth_upper,
            row.estimate,
            row.in_bound,
            row.rank_error
        )?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn quantile_from_sorted_matches_reference_edges() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(quantile_from_sorted(&data, 0.0), 1.0);
        assert_eq!(quantile_from_sorted(&data, 1.0), 5.0);
        assert_eq!(quantile_from_sorted(&data, 0.5), 3.0);
    }

    #[test]
    fn default_parser_values_match_reference_constants() {
        let cli = Cli::parse_from(["kll_guarantee_plot"]);
        assert_eq!(cli.k, 200);
        assert!((cli.tolerance - 0.02).abs() < 1e-12);
        assert_eq!(cli.sample_sizes, DEFAULT_SAMPLE_SIZES);
        assert_eq!(cli.quantiles, DEFAULT_QUANTILES);
        assert!((cli.uniform_min - 0.0).abs() < 1e-12);
        assert!((cli.uniform_max - 100_000_000.0).abs() < 1e-12);
        assert!((cli.zipf_min - 1_000_000.0).abs() < 1e-12);
        assert!((cli.zipf_max - 10_000_000.0).abs() < 1e-12);
        assert_eq!(cli.zipf_domain, 8192);
        assert!((cli.zipf_exponent - 1.1).abs() < 1e-12);
    }

    #[test]
    fn pass_requires_all_quantiles() {
        let total_quantiles = 7;
        let within_count = 6;
        let required_count = total_quantiles;
        let pass = within_count == required_count;
        assert!(!pass);
        let pass_full = total_quantiles == required_count;
        assert!(pass_full);
    }

    #[test]
    fn summary_and_detail_csv_smoke() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let out_root = std::env::temp_dir().join(format!("kll_guarantee_smoke_{unique}"));

        let cli = Cli {
            k: 64,
            tolerance: 0.05,
            out_dir: out_root.clone(),
            seed_base: DEFAULT_SEED_BASE,
            uniform_min: 0.0,
            uniform_max: 1000.0,
            zipf_min: 1000.0,
            zipf_max: 10000.0,
            zipf_domain: 64,
            zipf_exponent: 1.1,
            sample_sizes: "100,500".to_string(),
            quantiles: "0.0,0.5,1.0".to_string(),
        };

        run(&cli).expect("run should succeed");

        let run_dir = out_root.join("run");
        let metrics_path = run_dir.join("metrics.csv");
        let quantiles_path = run_dir.join("quantiles.csv");

        assert!(metrics_path.is_file());
        assert!(quantiles_path.is_file());

        let metrics_lines = fs::read_to_string(&metrics_path)
            .expect("read metrics")
            .lines()
            .count();
        let detail_lines = fs::read_to_string(&quantiles_path)
            .expect("read quantiles")
            .lines()
            .count();

        assert_eq!(metrics_lines, 1 + 2 * 2);
        assert_eq!(detail_lines, 1 + 2 * 2 * 3);

        let _ = fs::remove_dir_all(out_root);
    }

    // #[test]
    // fn rank_error_is_finite_and_bounded() {
    //     let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    //     let rank = empirical_rank(&sorted, 3.2);
    //     let q = 0.5;
    //     let rank_error = (rank - q).abs();

    //     assert!(rank.is_finite() && (0.0..=1.0).contains(&rank));
    //     assert!(rank_error.is_finite() && (0.0..=1.0).contains(&rank_error));

    //     let within_rate = 2.0 / 3.0;
    //     assert!(within_rate.is_finite() && (0.0..=1.0).contains(&within_rate));
    // }
}
