use crate::common::FreqTruth;
use crate::common::specs::{CountMinSpec, CountSketchSpec};
use crate::common::streams::{UniformConfig, UniformGenerator, ZipfConfig, ZipfGenerator};
use asap_sketchlib::{
    CMSHeap, CSHeap, DataInput, FastPath, FixedMatrix, QuickMatrixI64, QuickMatrixI128,
    RegularPath, Vector2D,
};

struct VariantCase {
    label: &'static str,
    make: fn() -> Variant,
}
enum Variant {
    V00(CMSHeap<Vector2D<i32>, RegularPath>),
    V01(CMSHeap<Vector2D<i32>, FastPath>),
    V02(CMSHeap<Vector2D<i64>, RegularPath>),
    V03(CMSHeap<Vector2D<i64>, FastPath>),
    V04(CMSHeap<FixedMatrix, RegularPath>),
    V05(CMSHeap<FixedMatrix, FastPath>),
    V06(CMSHeap<QuickMatrixI64, RegularPath>),
    V07(CMSHeap<QuickMatrixI64, FastPath>),
    V08(CSHeap<Vector2D<i32>, RegularPath>),
    V09(CSHeap<Vector2D<i32>, FastPath>),
    V10(CSHeap<Vector2D<i64>, RegularPath>),
    V11(CSHeap<Vector2D<i64>, FastPath>),
    V12(CSHeap<Vector2D<i128>, RegularPath>),
    V13(CSHeap<Vector2D<i128>, FastPath>),
    V14(CSHeap<FixedMatrix, RegularPath>),
    V15(CSHeap<FixedMatrix, FastPath>),
    V16(CSHeap<QuickMatrixI64, RegularPath>),
    V17(CSHeap<QuickMatrixI64, FastPath>),
    V18(CSHeap<QuickMatrixI128, RegularPath>),
    V19(CSHeap<QuickMatrixI128, FastPath>),
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
        }
    }
    fn estimate(&self, input: &DataInput) -> f64 {
        match self {
            Variant::V00(sketch) => sketch.estimate(input) as f64,
            Variant::V01(sketch) => sketch.estimate(input) as f64,
            Variant::V02(sketch) => sketch.estimate(input) as f64,
            Variant::V03(sketch) => sketch.estimate(input) as f64,
            Variant::V04(sketch) => sketch.estimate(input) as f64,
            Variant::V05(sketch) => sketch.estimate(input) as f64,
            Variant::V06(sketch) => sketch.estimate(input) as f64,
            Variant::V07(sketch) => sketch.estimate(input) as f64,
            Variant::V08(sketch) => sketch.estimate(input),
            Variant::V09(sketch) => sketch.estimate(input),
            Variant::V10(sketch) => sketch.estimate(input),
            Variant::V11(sketch) => sketch.estimate(input),
            Variant::V12(sketch) => sketch.estimate(input),
            Variant::V13(sketch) => sketch.estimate(input),
            Variant::V14(sketch) => sketch.estimate(input),
            Variant::V15(sketch) => sketch.estimate(input),
            Variant::V16(sketch) => sketch.estimate(input),
            Variant::V17(sketch) => sketch.estimate(input),
            Variant::V18(sketch) => sketch.estimate(input),
            Variant::V19(sketch) => sketch.estimate(input),
        }
    }
    fn dimensions(&self) -> (usize, usize) {
        match self {
            Variant::V00(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V01(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V02(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V03(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V04(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V05(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V06(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V07(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V08(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V09(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V10(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V11(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V12(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V13(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V14(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V15(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V16(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V17(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V18(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V19(sketch) => (sketch.rows(), sketch.cols()),
        }
    }
    fn is_count_min(&self) -> bool {
        match self {
            Variant::V00(_) => true,
            Variant::V01(_) => true,
            Variant::V02(_) => true,
            Variant::V03(_) => true,
            Variant::V04(_) => true,
            Variant::V05(_) => true,
            Variant::V06(_) => true,
            Variant::V07(_) => true,
            Variant::V08(_) => false,
            Variant::V09(_) => false,
            Variant::V10(_) => false,
            Variant::V11(_) => false,
            Variant::V12(_) => false,
            Variant::V13(_) => false,
            Variant::V14(_) => false,
            Variant::V15(_) => false,
            Variant::V16(_) => false,
            Variant::V17(_) => false,
            Variant::V18(_) => false,
            Variant::V19(_) => false,
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
            Self::I64(l, _) | Self::U64(l, _) | Self::F64(l, _) | Self::Str(l, _) => l,
        }
    }
    fn truth(&self) -> FreqTruth {
        match self {
            Self::I64(_, v) => FreqTruth::from_data(v),
            Self::U64(_, v) => {
                FreqTruth::from_data(&v.iter().map(|x| *x as i64).collect::<Vec<_>>())
            }
            Self::F64(_, v) => {
                FreqTruth::from_data(&v.iter().map(|x| *x as i64).collect::<Vec<_>>())
            }
            Self::Str(_, v) => FreqTruth::from_data(
                &v.iter()
                    .map(|x| x[1..].parse().unwrap())
                    .collect::<Vec<_>>(),
            ),
        }
    }
    fn insert_all(&self, s: &mut Variant) {
        match self {
            Self::I64(_, v) => v.iter().for_each(|x| s.insert(&DataInput::I64(*x))),
            Self::U64(_, v) => v.iter().for_each(|x| s.insert(&DataInput::U64(*x))),
            Self::F64(_, v) => v.iter().for_each(|x| s.insert(&DataInput::F64(*x))),
            Self::Str(_, v) => v.iter().for_each(|x| s.insert(&DataInput::Str(x))),
        }
    }
    fn estimate(&self, s: &Variant, key: i64) -> f64 {
        match self {
            Self::I64(..) => s.estimate(&DataInput::I64(key)),
            Self::U64(..) => s.estimate(&DataInput::U64(key as u64)),
            Self::F64(..) => s.estimate(&DataInput::F64(key as f64)),
            Self::Str(..) => s.estimate(&DataInput::Str(&format!("k{key}"))),
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
    for (label, v) in distributions {
        cases.push(DataCase::I64(label, v.iter().map(|x| *x as i64).collect()));
        cases.push(DataCase::U64(label, v.clone()));
        cases.push(DataCase::F64(label, v.iter().map(|x| *x as f64).collect()));
        cases.push(DataCase::Str(
            label,
            v.iter().map(|x| format!("k{x}")).collect(),
        ));
    }
    cases
}
fn variant_cases() -> Vec<VariantCase> {
    vec![
        VariantCase {
            label: "CMSHeap<Vector2D<i32>, RegularPath>",
            make: || Variant::V00(CMSHeap::<Vector2D<i32>, RegularPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<Vector2D<i32>, FastPath>",
            make: || Variant::V01(CMSHeap::<Vector2D<i32>, FastPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<Vector2D<i64>, RegularPath>",
            make: || Variant::V02(CMSHeap::<Vector2D<i64>, RegularPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<Vector2D<i64>, FastPath>",
            make: || Variant::V03(CMSHeap::<Vector2D<i64>, FastPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<FixedMatrix, RegularPath>",
            make: || Variant::V04(CMSHeap::<FixedMatrix, RegularPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<FixedMatrix, FastPath>",
            make: || Variant::V05(CMSHeap::<FixedMatrix, FastPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<QuickMatrixI64, RegularPath>",
            make: || Variant::V06(CMSHeap::<QuickMatrixI64, RegularPath>::default()),
        },
        VariantCase {
            label: "CMSHeap<QuickMatrixI64, FastPath>",
            make: || Variant::V07(CMSHeap::<QuickMatrixI64, FastPath>::default()),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i32>, RegularPath>",
            make: || Variant::V08(CSHeap::<Vector2D<i32>, RegularPath>::default()),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i32>, FastPath>",
            make: || Variant::V09(CSHeap::<Vector2D<i32>, FastPath>::default()),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i64>, RegularPath>",
            make: || Variant::V10(CSHeap::<Vector2D<i64>, RegularPath>::default()),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i64>, FastPath>",
            make: || Variant::V11(CSHeap::<Vector2D<i64>, FastPath>::default()),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i128>, RegularPath>",
            make: || Variant::V12(CSHeap::<Vector2D<i128>, RegularPath>::new(3, 4096, 32)),
        },
        VariantCase {
            label: "CSHeap<Vector2D<i128>, FastPath>",
            make: || Variant::V13(CSHeap::<Vector2D<i128>, FastPath>::new(3, 4096, 32)),
        },
        VariantCase {
            label: "CSHeap<FixedMatrix, RegularPath>",
            make: || Variant::V14(CSHeap::<FixedMatrix, RegularPath>::default()),
        },
        VariantCase {
            label: "CSHeap<FixedMatrix, FastPath>",
            make: || Variant::V15(CSHeap::<FixedMatrix, FastPath>::default()),
        },
        VariantCase {
            label: "CSHeap<QuickMatrixI64, RegularPath>",
            make: || Variant::V16(CSHeap::<QuickMatrixI64, RegularPath>::default()),
        },
        VariantCase {
            label: "CSHeap<QuickMatrixI64, FastPath>",
            make: || Variant::V17(CSHeap::<QuickMatrixI64, FastPath>::default()),
        },
        VariantCase {
            label: "CSHeap<QuickMatrixI128, RegularPath>",
            make: || Variant::V18(CSHeap::<QuickMatrixI128, RegularPath>::default()),
        },
        VariantCase {
            label: "CSHeap<QuickMatrixI128, FastPath>",
            make: || Variant::V19(CSHeap::<QuickMatrixI128, FastPath>::default()),
        },
    ]
}
#[test]
fn every_explicit_variant_is_checked_against_every_explicit_data_case() {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let data = data_cases();
            let variants = variant_cases();
            for d in &data {
                let truth = d.truth();
                for v in &variants {
                    let mut sketch = (v.make)();
                    d.insert_all(&mut sketch);
                    let (rows, cols) = sketch.dimensions();
                    if sketch.is_count_min() {
                        CountMinSpec::new(rows, cols).assert_contract(
                            v.label,
                            &truth,
                            |key| d.estimate(&sketch, key),
                            d.label(),
                        )
                    } else {
                        CountSketchSpec::new(rows, cols).assert_contract(
                            v.label,
                            &truth,
                            |key| d.estimate(&sketch, key),
                            d.label(),
                        )
                    }
                }
            }
        })
        .expect("spawn heap matrix")
        .join()
        .unwrap();
}
