//! Build script that compiles the crate's protobuf definitions and
//! generates the large precomputed sampling tables into `OUT_DIR`.
//!
//! Generating these tables at build time (rather than checking in the
//! ~131K-line generated source files) is a temporary fix to keep the
//! committed SLoC small. A fixed RNG seed is used so builds are
//! reproducible.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const SAMPLE_TABLE_LEN: usize = 0x10000;
const SAMPLE_SEED: u64 = 0xA5A0_5A71_B11B_C0DE_u64;

fn write_sample_table(
    out_dir: &Path,
    filename: &str,
    static_name: &str,
    doc: &str,
    scale: f64,
) -> std::io::Result<()> {
    let path = out_dir.join(filename);
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    let mut generator = SmallRng::seed_from_u64(SAMPLE_SEED);

    writeln!(writer, "/// {doc}")?;
    writeln!(
        writer,
        "pub static {static_name}: [f64; 0x{:x}] = [",
        SAMPLE_TABLE_LEN
    )?;
    for _ in 0..SAMPLE_TABLE_LEN {
        let k = loop {
            let r: f64 = generator.random::<f64>();
            if r != 0.0 && r != 1.0 {
                break r;
            }
        };
        let f = (1.0 - k).ln() * scale;
        writeln!(writer, "    {f},")?;
    }
    writeln!(writer, "];")?;
    Ok(())
}

fn main() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR not set by cargo"));

    write_sample_table(
        &out_dir,
        "precompute_sample.rs",
        "PRECOMPUTED_SAMPLE",
        "Precomputes part of geometric sampling: ln(1-u). Generated at build time.",
        1.0,
    )
    .expect("failed to generate precompute_sample.rs");

    write_sample_table(
        &out_dir,
        "precompute_sample2.rs",
        "PRECOMPUTED_SAMPLE_RATE_1PERCENT",
        "Precomputes skip number of sampling rate 0.01. Generated at build time.",
        1.0 / 0.99_f64.ln(),
    )
    .expect("failed to generate precompute_sample2.rs");

    let protoc =
        protoc_bin_vendored::protoc_bin_path().expect("failed to locate vendored protoc binary");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    prost_build::compile_protos(
        &[
            "proto/common/common.proto",
            "proto/countminsketch/countminsketch.proto",
            "proto/countsketch/countsketch.proto",
            "proto/hll/hll.proto",
            "proto/kll/kll.proto",
            "proto/ddsketch/ddsketch.proto",
            "proto/univmon/univmon.proto",
            "proto/hydra/hydra.proto",
            "proto/cocosketch/cocosketch.proto",
            "proto/elasticsketch/elasticsketch.proto",
            "proto/sketchlib.proto",
        ],
        &["proto"],
    )
    .expect("prost_build failed to compile proto files");
}
