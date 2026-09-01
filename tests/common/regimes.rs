#![allow(dead_code)]

use asap_sketchlib::DataInput;

use super::FreqTruth;
use super::streams::{uniform_u64, zipf_u64};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Scale {
    N10K,
    N100K,
    N1M,
    N10M,
}

impl Scale {
    pub fn n(self) -> usize {
        match self {
            Scale::N10K => 10_000,
            Scale::N100K => 100_000,
            Scale::N1M => 1_000_000,
            Scale::N10M => 10_000_000,
        }
    }

    pub fn domain(self) -> usize {
        match self {
            Scale::N10K => 1_024,
            Scale::N100K => 8_192,
            Scale::N1M => 65_536,
            Scale::N10M => 524_288,
        }
    }

    pub fn tag(self) -> &'static str {
        match self {
            Scale::N10K => "10k",
            Scale::N100K => "100k",
            Scale::N1M => "1m",
            Scale::N10M => "10m",
        }
    }

    pub const ALL: [Scale; 4] = [Scale::N10K, Scale::N100K, Scale::N1M, Scale::N10M];
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    Zipf070,
    Zipf110,
    Zipf150,
    Uniform,
}

impl Shape {
    pub fn tag(self) -> &'static str {
        match self {
            Shape::Zipf070 => "zipf(0.7)",
            Shape::Zipf110 => "zipf(1.1)",
            Shape::Zipf150 => "zipf(1.5)",
            Shape::Uniform => "uniform",
        }
    }

    fn draw(self, n: usize, domain: usize, seed: u64) -> Vec<u64> {
        match self {
            Shape::Zipf070 => zipf_u64(n, domain, 0.7, seed),
            Shape::Zipf110 => zipf_u64(n, domain, 1.1, seed),
            Shape::Zipf150 => zipf_u64(n, domain, 1.5, seed),
            Shape::Uniform => uniform_u64(n, domain as u64, seed),
        }
    }

    pub const ALL: [Shape; 4] = [
        Shape::Zipf070,
        Shape::Zipf110,
        Shape::Zipf150,
        Shape::Uniform,
    ];
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeyType {
    I64,
    U64,
    F64,
    Str,
}

impl KeyType {
    pub fn tag(self) -> &'static str {
        match self {
            KeyType::I64 => "i64",
            KeyType::U64 => "u64",
            KeyType::F64 => "f64",
            KeyType::Str => "str",
        }
    }

    fn materialise(self, stream: &[u64]) -> Keys {
        match self {
            KeyType::I64 => Keys::I64(stream.iter().map(|v| *v as i64).collect()),
            KeyType::U64 => Keys::U64(stream.to_vec()),
            KeyType::F64 => Keys::F64(stream.iter().map(|v| *v as f64).collect()),
            KeyType::Str => Keys::Str(stream.iter().map(|v| format!("k{v}")).collect()),
        }
    }

    pub fn with_input<R>(self, key: i64, f: impl FnOnce(&DataInput) -> R) -> R {
        match self {
            KeyType::I64 => f(&DataInput::I64(key)),
            KeyType::U64 => f(&DataInput::U64(key as u64)),
            KeyType::F64 => f(&DataInput::F64(key as f64)),
            KeyType::Str => f(&DataInput::Str(&format!("k{key}"))),
        }
    }

    pub const ALL: [KeyType; 4] = [KeyType::I64, KeyType::U64, KeyType::F64, KeyType::Str];
}

pub enum Keys {
    I64(Vec<i64>),
    U64(Vec<u64>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

impl Keys {
    pub fn len(&self) -> usize {
        match self {
            Keys::I64(v) => v.len(),
            Keys::U64(v) => v.len(),
            Keys::F64(v) => v.len(),
            Keys::Str(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn for_each(&self, mut f: impl FnMut(&DataInput)) {
        match self {
            Keys::I64(v) => v.iter().for_each(|x| f(&DataInput::I64(*x))),
            Keys::U64(v) => v.iter().for_each(|x| f(&DataInput::U64(*x))),
            Keys::F64(v) => v.iter().for_each(|x| f(&DataInput::F64(*x))),
            Keys::Str(v) => v.iter().for_each(|x| f(&DataInput::Str(x))),
        }
    }
}

const SEED_BASE: u64 = 0x10BE_C700_0000_0000;

fn seed_for(scale: Scale, shape: Shape) -> u64 {
    let s = match scale {
        Scale::N10K => 1,
        Scale::N100K => 2,
        Scale::N1M => 3,
        Scale::N10M => 4,
    };
    let d = match shape {
        Shape::Zipf070 => 1,
        Shape::Zipf110 => 2,
        Shape::Zipf150 => 3,
        Shape::Uniform => 4,
    };
    SEED_BASE | (s << 16) | d
}

pub struct Regime {
    pub scale: Scale,
    pub shape: Shape,
    pub key_type: KeyType,
    pub label: String,
}

impl Regime {
    pub fn new(scale: Scale, shape: Shape, key_type: KeyType) -> Self {
        let label = format!(
            "{} n={} domain={} keys={} seed={:#018x}",
            shape.tag(),
            scale.tag(),
            scale.domain(),
            key_type.tag(),
            seed_for(scale, shape),
        );
        Self {
            scale,
            shape,
            key_type,
            label,
        }
    }

    pub fn build(&self) -> (Keys, FreqTruth) {
        let stream = self.shape.draw(
            self.scale.n(),
            self.scale.domain(),
            seed_for(self.scale, self.shape),
        );
        let mut truth = FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }
        (self.key_type.materialise(&stream), truth)
    }
}

pub fn selected_scales() -> Vec<Scale> {
    let mut out = vec![Scale::N10K];
    // if cfg!(feature = "middlesize") {
    //     out.push(Scale::N100K);
    //     out.push(Scale::N1M);
    // }
    // if cfg!(feature = "largesize") {
    //     out.push(Scale::N10M);
    // }
    out.sort();
    out
}

pub fn frequency_regimes() -> Vec<Regime> {
    regimes_over(&selected_scales())
}

pub fn all_frequency_regimes() -> Vec<Regime> {
    regimes_over(&Scale::ALL)
}

fn regimes_over(scales: &[Scale]) -> Vec<Regime> {
    let mut out = Vec::with_capacity(scales.len() * Shape::ALL.len() * KeyType::ALL.len());
    for scale in scales {
        for shape in Shape::ALL {
            for key_type in KeyType::ALL {
                out.push(Regime::new(*scale, shape, key_type));
            }
        }
    }
    out
}
