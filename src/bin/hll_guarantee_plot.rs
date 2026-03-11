use clap::Parser;
use rand::SeedableRng;
use rand::distr::{Distribution, Uniform, weighted::WeightedIndex};
use rand::rngs::StdRng;
use sketchlib_rust::{DataFusion, HyperLogLog, HyperLogLogHIP, Regular, SketchInput};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const HLL_P: usize = 14;
const DEFAULT_OUT_DIR: &str = "data/results/hll_guarantee";
const DEFAULT_SEED_BASE: u64 = 0x3eed_c0de_1bad_b002;
const DEFAULT_SAMPLE_SIZES: &str = "1000,5000,20000,100000,1000000,5000000";
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

impl fmt::Display for DistKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum VariantKind {
    Regular,
    DataFusion,
    Hip,
}

impl VariantKind {
    fn as_str(self) -> &'static str {
        match self {
            VariantKind::Regular => "regular",
            VariantKind::DataFusion => "datafusion",
            VariantKind::Hip => "hip",
        }
    }
}

impl fmt::Display for VariantKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Parser, Debug)]
#[command(name = "hll_guarantee_plot")]
#[command(about = "Generate HLL cardinality guarantee metrics against theoretical RSE")]
struct Cli {
    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    out_dir: PathBuf,

    #[arg(long, default_value_t = DEFAULT_SEED_BASE)]
    seed_base: u64,

    #[arg(long, default_value = DEFAULT_SAMPLE_SIZES)]
    sample_sizes: String,

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
}

#[derive(Clone, Debug)]
struct MetricRow {
    variant: VariantKind,
    distribution: DistKind,
    n: usize,
    seed: u64,
    true_distinct: usize,
    estimate: usize,
    abs_error: f64,
    relative_error: f64,
    theoretical_rse: f64,
    pass: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    run(&cli)
}

fn run(cli: &Cli) -> Result<(), Box<dyn Error>> {
    validate_cli(cli)?;

    let sample_sizes = parse_usize_csv(&cli.sample_sizes)?;
    let theoretical_rse = theoretical_rse(HLL_P);
    let mut rows = Vec::new();

    for &distribution in &[DistKind::Uniform, DistKind::Zipf] {
        for &variant in &[
            VariantKind::Regular,
            VariantKind::DataFusion,
            VariantKind::Hip,
        ] {
            for (idx, &n) in sample_sizes.iter().enumerate() {
                let seed = derive_seed(cli.seed_base, distribution, variant, idx as u64);
                let values = match distribution {
                    DistKind::Uniform => {
                        sample_uniform_f64(cli.uniform_min, cli.uniform_max, n, seed)
                    }
                    DistKind::Zipf => sample_zipf_f64(
                        cli.zipf_min,
                        cli.zipf_max,
                        cli.zipf_domain,
                        cli.zipf_exponent,
                        n,
                        seed,
                    ),
                };

                let row =
                    evaluate_variant(variant, distribution, n, seed, &values, theoretical_rse);
                rows.push(row);
            }
        }
    }

    let run_dir = make_run_dir(&cli.out_dir)?;
    write_metrics_csv(&run_dir.join("metrics.csv"), &rows)?;

    println!("HLL guarantee artifacts written to {}", run_dir.display());
    println!(
        "generate plots with: python3 scripts/plot_hll_guarantee.py --metrics {}/metrics.csv --out-dir {}",
        run_dir.display(),
        run_dir.display()
    );

    Ok(())
}

fn validate_cli(cli: &Cli) -> Result<(), Box<dyn Error>> {
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

fn derive_seed(base: u64, distribution: DistKind, variant: VariantKind, sample_idx: u64) -> u64 {
    let dist_tag = match distribution {
        DistKind::Uniform => 0xA5A5_A5A5_A5A5_A5A5,
        DistKind::Zipf => 0xB4B4_B4B4_B4B4_B4B4,
    };
    let variant_tag = match variant {
        VariantKind::Regular => 0x1111_1111_1111_1111,
        VariantKind::DataFusion => 0x2222_2222_2222_2222,
        VariantKind::Hip => 0x3333_3333_3333_3333,
    };

    base ^ dist_tag ^ variant_tag ^ sample_idx.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn theoretical_rse(p: usize) -> f64 {
    let m = (1usize << p) as f64;
    1.04 / m.sqrt()
}

fn evaluate_variant(
    variant: VariantKind,
    distribution: DistKind,
    n: usize,
    seed: u64,
    values: &[f64],
    theoretical_rse: f64,
) -> MetricRow {
    let mut truth = HashSet::<u64>::with_capacity(values.len());

    let estimate = match variant {
        VariantKind::Regular => {
            let mut sketch = HyperLogLog::<Regular>::new();
            for &value in values {
                truth.insert(value.to_bits());
                sketch.insert(&SketchInput::F64(value));
            }
            sketch.estimate()
        }
        VariantKind::DataFusion => {
            let mut sketch = HyperLogLog::<DataFusion>::new();
            for &value in values {
                truth.insert(value.to_bits());
                sketch.insert(&SketchInput::F64(value));
            }
            sketch.estimate()
        }
        VariantKind::Hip => {
            let mut sketch = HyperLogLogHIP::new();
            for &value in values {
                truth.insert(value.to_bits());
                sketch.insert(&SketchInput::F64(value));
            }
            sketch.estimate()
        }
    };

    let true_distinct = truth.len();
    let abs_error = (estimate as f64 - true_distinct as f64).abs();
    let relative_error = if true_distinct == 0 {
        0.0
    } else {
        abs_error / true_distinct as f64
    };
    let pass = relative_error <= theoretical_rse;

    MetricRow {
        variant,
        distribution,
        n,
        seed,
        true_distinct,
        estimate,
        abs_error,
        relative_error,
        theoretical_rse,
        pass,
    }
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
        "variant,distribution,n,seed,true_distinct,estimate,abs_error,relative_error,theoretical_rse,pass"
    )?;

    for row in rows {
        writeln!(
            writer,
            "{},{},{},{},{},{},{:.12},{:.12},{:.12},{}",
            row.variant,
            row.distribution,
            row.n,
            row.seed,
            row.true_distinct,
            row.estimate,
            row.abs_error,
            row.relative_error,
            row.theoretical_rse,
            row.pass
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
    fn parse_sample_sizes_respects_defaults_and_validation() {
        let parsed = parse_usize_csv("1000,5000,20000").expect("parse sample sizes");
        assert_eq!(parsed, vec![1000, 5000, 20000]);
        assert!(parse_usize_csv("0").is_err());
        assert!(parse_usize_csv(",,").is_err());
    }

    #[test]
    fn theoretical_rse_formula_is_sane() {
        let rse = theoretical_rse(HLL_P);
        assert!(rse.is_finite() && rse > 0.0);
        assert!((rse - (1.04 / (16384.0_f64).sqrt())).abs() < 1e-12);
    }

    #[test]
    fn metrics_row_formulas_are_consistent() {
        let values = vec![1.0, 2.0, 1.0, 3.0, 3.0, 4.0];
        let row = evaluate_variant(
            VariantKind::Regular,
            DistKind::Uniform,
            values.len(),
            123,
            &values,
            theoretical_rse(HLL_P),
        );

        assert_eq!(row.true_distinct, 4);
        assert!(row.abs_error.is_finite() && row.abs_error >= 0.0);
        assert!(row.relative_error.is_finite() && row.relative_error >= 0.0);
        assert_eq!(
            row.pass,
            row.relative_error <= row.theoretical_rse,
            "pass condition should follow relative_error <= theoretical_rse"
        );
    }

    #[test]
    fn smoke_run_writes_metrics_csv() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let out_root = std::env::temp_dir().join(format!("hll_guarantee_smoke_{unique}"));

        let cli = Cli {
            out_dir: out_root.clone(),
            seed_base: DEFAULT_SEED_BASE,
            sample_sizes: "100,500".to_string(),
            uniform_min: 0.0,
            uniform_max: 1000.0,
            zipf_min: 1000.0,
            zipf_max: 10000.0,
            zipf_domain: 64,
            zipf_exponent: 1.1,
        };

        run(&cli).expect("run should succeed");

        let run_dir = out_root.join("run");
        let metrics_path = run_dir.join("metrics.csv");
        assert!(metrics_path.is_file());

        let content = fs::read_to_string(&metrics_path).expect("read metrics");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines[0],
            "variant,distribution,n,seed,true_distinct,estimate,abs_error,relative_error,theoretical_rse,pass"
        );
        assert_eq!(lines.len(), 1 + 3 * 2 * 2);

        let _ = fs::remove_dir_all(out_root);
    }

    #[test]
    fn relative_error_bounds_are_finite() {
        let rse = theoretical_rse(HLL_P);
        let values = sample_uniform_f64(0.0, 1000.0, 1000, 42);

        for variant in [
            VariantKind::Regular,
            VariantKind::DataFusion,
            VariantKind::Hip,
        ] {
            let row = evaluate_variant(variant, DistKind::Uniform, 1000, 42, &values, rse);
            assert!(row.relative_error.is_finite() && row.relative_error >= 0.0);
            assert!(row.theoretical_rse.is_finite() && row.theoretical_rse >= 0.0);
        }
    }
}
