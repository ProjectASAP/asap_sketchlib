use crate::common::CardinalityTruth;
use crate::common::storage::*;
use crate::common::streams::{UniformConfig, UniformGenerator, ZipfConfig, ZipfGenerator};
use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::sketches::hll::{HyperLogLogHIPImpl, HyperLogLogImpl};
use asap_sketchlib::{Classic, DataInput, DefaultXxHasher, ErtlMLE};

struct VariantCase {
    label: &'static str,
    make: fn() -> Variant,
}

enum Variant {
    V00(HyperLogLogImpl<Classic, HllBucketListP8, DefaultXxHasher>),
    V01(HyperLogLogImpl<ErtlMLE, HllBucketListP8, DefaultXxHasher>),
    V02(HyperLogLogHIPImpl<HllBucketListP8>),
    V03(HyperLogLogImpl<Classic, HllBucketListP9, DefaultXxHasher>),
    V04(HyperLogLogImpl<ErtlMLE, HllBucketListP9, DefaultXxHasher>),
    V05(HyperLogLogHIPImpl<HllBucketListP9>),
    V06(HyperLogLogImpl<Classic, HllBucketListP10, DefaultXxHasher>),
    V07(HyperLogLogImpl<ErtlMLE, HllBucketListP10, DefaultXxHasher>),
    V08(HyperLogLogHIPImpl<HllBucketListP10>),
    V09(HyperLogLogImpl<Classic, HllBucketListP11, DefaultXxHasher>),
    V10(HyperLogLogImpl<ErtlMLE, HllBucketListP11, DefaultXxHasher>),
    V11(HyperLogLogHIPImpl<HllBucketListP11>),
    V12(HyperLogLogImpl<Classic, HllBucketListP12, DefaultXxHasher>),
    V13(HyperLogLogImpl<ErtlMLE, HllBucketListP12, DefaultXxHasher>),
    V14(HyperLogLogHIPImpl<HllBucketListP12>),
    V15(HyperLogLogImpl<Classic, HllBucketListP13, DefaultXxHasher>),
    V16(HyperLogLogImpl<ErtlMLE, HllBucketListP13, DefaultXxHasher>),
    V17(HyperLogLogHIPImpl<HllBucketListP13>),
    V18(HyperLogLogImpl<Classic, HllBucketListP14, DefaultXxHasher>),
    V19(HyperLogLogImpl<ErtlMLE, HllBucketListP14, DefaultXxHasher>),
    V20(HyperLogLogHIPImpl<HllBucketListP14>),
    V21(HyperLogLogImpl<Classic, HllBucketListP15, DefaultXxHasher>),
    V22(HyperLogLogImpl<ErtlMLE, HllBucketListP15, DefaultXxHasher>),
    V23(HyperLogLogHIPImpl<HllBucketListP15>),
    V24(HyperLogLogImpl<Classic, HllBucketListP16, DefaultXxHasher>),
    V25(HyperLogLogImpl<ErtlMLE, HllBucketListP16, DefaultXxHasher>),
    V26(HyperLogLogHIPImpl<HllBucketListP16>),
    V27(HyperLogLogImpl<Classic, HllBucketListP17, DefaultXxHasher>),
    V28(HyperLogLogImpl<ErtlMLE, HllBucketListP17, DefaultXxHasher>),
    V29(HyperLogLogHIPImpl<HllBucketListP17>),
    V30(HyperLogLogImpl<Classic, HllBucketListP18, DefaultXxHasher>),
    V31(HyperLogLogImpl<ErtlMLE, HllBucketListP18, DefaultXxHasher>),
    V32(HyperLogLogHIPImpl<HllBucketListP18>),
    V33(HllSketch),
}

fn portable_bytes(input: &DataInput) -> Vec<u8> {
    match input {
        DataInput::I64(v) => v.to_be_bytes().to_vec(),
        DataInput::U64(v) => v.to_be_bytes().to_vec(),
        DataInput::F64(v) => v.to_bits().to_be_bytes().to_vec(),
        DataInput::Str(v) => v.as_bytes().to_vec(),
        _ => unreachable!(),
    }
}

impl Variant {
    fn insert(&mut self, input: &DataInput) {
        match self {
            Variant::V00(sketch) => sketch.insert(input),
            Variant::V01(sketch) => sketch.insert(input),
            Variant::V02(sketch) => sketch.insert(input),
            Variant::V03(sketch) => sketch.insert(input),
            Variant::V04(sketch) => sketch.insert(input),
            Variant::V05(sketch) => sketch.insert(input),
            Variant::V06(sketch) => sketch.insert(input),
            Variant::V07(sketch) => sketch.insert(input),
            Variant::V08(sketch) => sketch.insert(input),
            Variant::V09(sketch) => sketch.insert(input),
            Variant::V10(sketch) => sketch.insert(input),
            Variant::V11(sketch) => sketch.insert(input),
            Variant::V12(sketch) => sketch.insert(input),
            Variant::V13(sketch) => sketch.insert(input),
            Variant::V14(sketch) => sketch.insert(input),
            Variant::V15(sketch) => sketch.insert(input),
            Variant::V16(sketch) => sketch.insert(input),
            Variant::V17(sketch) => sketch.insert(input),
            Variant::V18(sketch) => sketch.insert(input),
            Variant::V19(sketch) => sketch.insert(input),
            Variant::V20(sketch) => sketch.insert(input),
            Variant::V21(sketch) => sketch.insert(input),
            Variant::V22(sketch) => sketch.insert(input),
            Variant::V23(sketch) => sketch.insert(input),
            Variant::V24(sketch) => sketch.insert(input),
            Variant::V25(sketch) => sketch.insert(input),
            Variant::V26(sketch) => sketch.insert(input),
            Variant::V27(sketch) => sketch.insert(input),
            Variant::V28(sketch) => sketch.insert(input),
            Variant::V29(sketch) => sketch.insert(input),
            Variant::V30(sketch) => sketch.insert(input),
            Variant::V31(sketch) => sketch.insert(input),
            Variant::V32(sketch) => sketch.insert(input),
            Variant::V33(sketch) => sketch.update(&portable_bytes(input)),
        }
    }
    fn estimate(&self) -> f64 {
        match self {
            Variant::V00(sketch) => sketch.estimate() as f64,
            Variant::V01(sketch) => sketch.estimate() as f64,
            Variant::V02(sketch) => sketch.estimate() as f64,
            Variant::V03(sketch) => sketch.estimate() as f64,
            Variant::V04(sketch) => sketch.estimate() as f64,
            Variant::V05(sketch) => sketch.estimate() as f64,
            Variant::V06(sketch) => sketch.estimate() as f64,
            Variant::V07(sketch) => sketch.estimate() as f64,
            Variant::V08(sketch) => sketch.estimate() as f64,
            Variant::V09(sketch) => sketch.estimate() as f64,
            Variant::V10(sketch) => sketch.estimate() as f64,
            Variant::V11(sketch) => sketch.estimate() as f64,
            Variant::V12(sketch) => sketch.estimate() as f64,
            Variant::V13(sketch) => sketch.estimate() as f64,
            Variant::V14(sketch) => sketch.estimate() as f64,
            Variant::V15(sketch) => sketch.estimate() as f64,
            Variant::V16(sketch) => sketch.estimate() as f64,
            Variant::V17(sketch) => sketch.estimate() as f64,
            Variant::V18(sketch) => sketch.estimate() as f64,
            Variant::V19(sketch) => sketch.estimate() as f64,
            Variant::V20(sketch) => sketch.estimate() as f64,
            Variant::V21(sketch) => sketch.estimate() as f64,
            Variant::V22(sketch) => sketch.estimate() as f64,
            Variant::V23(sketch) => sketch.estimate() as f64,
            Variant::V24(sketch) => sketch.estimate() as f64,
            Variant::V25(sketch) => sketch.estimate() as f64,
            Variant::V26(sketch) => sketch.estimate() as f64,
            Variant::V27(sketch) => sketch.estimate() as f64,
            Variant::V28(sketch) => sketch.estimate() as f64,
            Variant::V29(sketch) => sketch.estimate() as f64,
            Variant::V30(sketch) => sketch.estimate() as f64,
            Variant::V31(sketch) => sketch.estimate() as f64,
            Variant::V32(sketch) => sketch.estimate() as f64,
            Variant::V33(sketch) => sketch.estimate(),
        }
    }
    fn sigma_rel(&self) -> f64 {
        match self {
            Variant::V00(_) => 1.04 / (256f64).sqrt(),
            Variant::V01(_) => 1.04 / (256f64).sqrt(),
            Variant::V02(_) => (std::f64::consts::LN_2 / 256f64).sqrt(),
            Variant::V03(_) => 1.04 / (512f64).sqrt(),
            Variant::V04(_) => 1.04 / (512f64).sqrt(),
            Variant::V05(_) => (std::f64::consts::LN_2 / 512f64).sqrt(),
            Variant::V06(_) => 1.04 / (1024f64).sqrt(),
            Variant::V07(_) => 1.04 / (1024f64).sqrt(),
            Variant::V08(_) => (std::f64::consts::LN_2 / 1024f64).sqrt(),
            Variant::V09(_) => 1.04 / (2048f64).sqrt(),
            Variant::V10(_) => 1.04 / (2048f64).sqrt(),
            Variant::V11(_) => (std::f64::consts::LN_2 / 2048f64).sqrt(),
            Variant::V12(_) => 1.04 / (4096f64).sqrt(),
            Variant::V13(_) => 1.04 / (4096f64).sqrt(),
            Variant::V14(_) => (std::f64::consts::LN_2 / 4096f64).sqrt(),
            Variant::V15(_) => 1.04 / (8192f64).sqrt(),
            Variant::V16(_) => 1.04 / (8192f64).sqrt(),
            Variant::V17(_) => (std::f64::consts::LN_2 / 8192f64).sqrt(),
            Variant::V18(_) => 1.04 / (16384f64).sqrt(),
            Variant::V19(_) => 1.04 / (16384f64).sqrt(),
            Variant::V20(_) => (std::f64::consts::LN_2 / 16384f64).sqrt(),
            Variant::V21(_) => 1.04 / (32768f64).sqrt(),
            Variant::V22(_) => 1.04 / (32768f64).sqrt(),
            Variant::V23(_) => (std::f64::consts::LN_2 / 32768f64).sqrt(),
            Variant::V24(_) => 1.04 / (65536f64).sqrt(),
            Variant::V25(_) => 1.04 / (65536f64).sqrt(),
            Variant::V26(_) => (std::f64::consts::LN_2 / 65536f64).sqrt(),
            Variant::V27(_) => 1.04 / (131072f64).sqrt(),
            Variant::V28(_) => 1.04 / (131072f64).sqrt(),
            Variant::V29(_) => (std::f64::consts::LN_2 / 131072f64).sqrt(),
            Variant::V30(_) => 1.04 / (262144f64).sqrt(),
            Variant::V31(_) => 1.04 / (262144f64).sqrt(),
            Variant::V32(_) => (std::f64::consts::LN_2 / 262144f64).sqrt(),
            Variant::V33(sketch) => 1.04 / (sketch.registers.len() as f64).sqrt(),
        }
    }
}

enum DataCase {
    I64(&'static str, Vec<i64>),
    U64(&'static str, Vec<u64>),
    F64(&'static str, Vec<f64>),
    Str(&'static str, Vec<String>),
}
impl DataCase {
    fn label(&self) -> &'static str {
        match self {
            Self::I64(v, _) | Self::U64(v, _) | Self::F64(v, _) | Self::Str(v, _) => v,
        }
    }
    fn cardinality(&self) -> usize {
        match self {
            Self::I64(_, v) => CardinalityTruth::from_data(v).distinct(),
            Self::U64(_, v) => CardinalityTruth::from_data(v).distinct(),
            Self::F64(_, v) => {
                CardinalityTruth::from_data(&v.iter().map(|x| x.to_bits()).collect::<Vec<_>>())
                    .distinct()
            }
            Self::Str(_, v) => CardinalityTruth::from_data(v).distinct(),
        }
    }
    fn insert_all(&self, sketch: &mut Variant) {
        match self {
            Self::I64(_, v) => v.iter().for_each(|x| sketch.insert(&DataInput::I64(*x))),
            Self::U64(_, v) => v.iter().for_each(|x| sketch.insert(&DataInput::U64(*x))),
            Self::F64(_, v) => v.iter().for_each(|x| sketch.insert(&DataInput::F64(*x))),
            Self::Str(_, v) => v.iter().for_each(|x| sketch.insert(&DataInput::Str(x))),
        }
    }
}

fn data_cases() -> Vec<DataCase> {
    let distributions: Vec<(&'static str, Vec<u64>)> = vec![
        (
            "zipf(0.7)",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 0.7,
                seed: 0x10BE_C700_0001_0001,
            }),
        ),
        (
            "zipf(1.1)",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.1,
                seed: 0x10BE_C700_0001_0002,
            }),
        ),
        (
            "zipf(1.5)",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.5,
                seed: 0x10BE_C700_0001_0003,
            }),
        ),
        (
            "uniform",
            UniformGenerator::generate(&UniformConfig {
                count: 10_000,
                domain: 1_024,
                seed: 0x10BE_C700_0001_0004,
            }),
        ),
    ];
    let mut cases = Vec::new();
    for (label, values) in distributions {
        cases.push(DataCase::I64(
            label,
            values.iter().map(|v| *v as i64).collect(),
        ));
        cases.push(DataCase::U64(label, values.clone()));
        cases.push(DataCase::F64(
            label,
            values.iter().map(|v| *v as f64).collect(),
        ));
        cases.push(DataCase::Str(
            label,
            values.iter().map(|v| format!("k{v}")).collect(),
        ));
    }
    cases
}
fn variant_cases() -> Vec<VariantCase> {
    vec![
        VariantCase {
            label: "HyperLogLogImpl<Classic, p8>",
            make: || {
                Variant::V00(HyperLogLogImpl::<Classic, HllBucketListP8, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p8>",
            make: || {
                Variant::V01(HyperLogLogImpl::<ErtlMLE, HllBucketListP8, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p8>",
            make: || Variant::V02(HyperLogLogHIPImpl::<HllBucketListP8>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p9>",
            make: || {
                Variant::V03(HyperLogLogImpl::<Classic, HllBucketListP9, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p9>",
            make: || {
                Variant::V04(HyperLogLogImpl::<ErtlMLE, HllBucketListP9, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p9>",
            make: || Variant::V05(HyperLogLogHIPImpl::<HllBucketListP9>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p10>",
            make: || {
                Variant::V06(HyperLogLogImpl::<Classic, HllBucketListP10, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p10>",
            make: || {
                Variant::V07(HyperLogLogImpl::<ErtlMLE, HllBucketListP10, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p10>",
            make: || Variant::V08(HyperLogLogHIPImpl::<HllBucketListP10>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p11>",
            make: || {
                Variant::V09(HyperLogLogImpl::<Classic, HllBucketListP11, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p11>",
            make: || {
                Variant::V10(HyperLogLogImpl::<ErtlMLE, HllBucketListP11, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p11>",
            make: || Variant::V11(HyperLogLogHIPImpl::<HllBucketListP11>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p12>",
            make: || {
                Variant::V12(HyperLogLogImpl::<Classic, HllBucketListP12, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p12>",
            make: || {
                Variant::V13(HyperLogLogImpl::<ErtlMLE, HllBucketListP12, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p12>",
            make: || Variant::V14(HyperLogLogHIPImpl::<HllBucketListP12>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p13>",
            make: || {
                Variant::V15(HyperLogLogImpl::<Classic, HllBucketListP13, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p13>",
            make: || {
                Variant::V16(HyperLogLogImpl::<ErtlMLE, HllBucketListP13, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p13>",
            make: || Variant::V17(HyperLogLogHIPImpl::<HllBucketListP13>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p14>",
            make: || {
                Variant::V18(HyperLogLogImpl::<Classic, HllBucketListP14, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p14>",
            make: || {
                Variant::V19(HyperLogLogImpl::<ErtlMLE, HllBucketListP14, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p14>",
            make: || Variant::V20(HyperLogLogHIPImpl::<HllBucketListP14>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p15>",
            make: || {
                Variant::V21(HyperLogLogImpl::<Classic, HllBucketListP15, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p15>",
            make: || {
                Variant::V22(HyperLogLogImpl::<ErtlMLE, HllBucketListP15, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p15>",
            make: || Variant::V23(HyperLogLogHIPImpl::<HllBucketListP15>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p16>",
            make: || {
                Variant::V24(HyperLogLogImpl::<Classic, HllBucketListP16, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p16>",
            make: || {
                Variant::V25(HyperLogLogImpl::<ErtlMLE, HllBucketListP16, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p16>",
            make: || Variant::V26(HyperLogLogHIPImpl::<HllBucketListP16>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p17>",
            make: || {
                Variant::V27(HyperLogLogImpl::<Classic, HllBucketListP17, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p17>",
            make: || {
                Variant::V28(HyperLogLogImpl::<ErtlMLE, HllBucketListP17, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p17>",
            make: || Variant::V29(HyperLogLogHIPImpl::<HllBucketListP17>::new()),
        },
        VariantCase {
            label: "HyperLogLogImpl<Classic, p18>",
            make: || {
                Variant::V30(HyperLogLogImpl::<Classic, HllBucketListP18, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogImpl<ErtlMLE, p18>",
            make: || {
                Variant::V31(HyperLogLogImpl::<ErtlMLE, HllBucketListP18, DefaultXxHasher>::new())
            },
        },
        VariantCase {
            label: "HyperLogLogHIPImpl<p18>",
            make: || Variant::V32(HyperLogLogHIPImpl::<HllBucketListP18>::new()),
        },
        VariantCase {
            label: "HllSketch<Regular> p10",
            make: || Variant::V33(HllSketch::new(HllVariant::Regular, 10)),
        },
        VariantCase {
            label: "HllSketch<Datafusion> p10",
            make: || Variant::V33(HllSketch::new(HllVariant::Datafusion, 10)),
        },
        VariantCase {
            label: "HllSketch<Hip> p10",
            make: || Variant::V33(HllSketch::new(HllVariant::Hip, 10)),
        },
        VariantCase {
            label: "HllSketch<Regular> p12",
            make: || Variant::V33(HllSketch::new(HllVariant::Regular, 12)),
        },
        VariantCase {
            label: "HllSketch<Datafusion> p12",
            make: || Variant::V33(HllSketch::new(HllVariant::Datafusion, 12)),
        },
        VariantCase {
            label: "HllSketch<Hip> p12",
            make: || Variant::V33(HllSketch::new(HllVariant::Hip, 12)),
        },
        VariantCase {
            label: "HllSketch<Regular> p14",
            make: || Variant::V33(HllSketch::new(HllVariant::Regular, 14)),
        },
        VariantCase {
            label: "HllSketch<Datafusion> p14",
            make: || Variant::V33(HllSketch::new(HllVariant::Datafusion, 14)),
        },
        VariantCase {
            label: "HllSketch<Hip> p14",
            make: || Variant::V33(HllSketch::new(HllVariant::Hip, 14)),
        },
        VariantCase {
            label: "HllSketch<Regular> p16",
            make: || Variant::V33(HllSketch::new(HllVariant::Regular, 16)),
        },
        VariantCase {
            label: "HllSketch<Datafusion> p16",
            make: || Variant::V33(HllSketch::new(HllVariant::Datafusion, 16)),
        },
        VariantCase {
            label: "HllSketch<Hip> p16",
            make: || Variant::V33(HllSketch::new(HllVariant::Hip, 16)),
        },
    ]
}

#[test]
fn every_explicit_variant_is_checked_against_every_explicit_data_case() {
    let data = data_cases();
    let variants = variant_cases();
    for data_case in &data {
        for variant_case in &variants {
            let mut sketch = (variant_case.make)();
            data_case.insert_all(&mut sketch);
            let exact = data_case.cardinality() as f64;
            let estimate = sketch.estimate();
            let relative_error = (estimate - exact).abs() / exact;
            let tolerance = 4.0 * sketch.sigma_rel();
            assert!(
                relative_error <= tolerance,
                "{} on {}: estimate {estimate}, exact {exact}, relative error {relative_error} > {tolerance}",
                variant_case.label,
                data_case.label()
            );
        }
    }
}
