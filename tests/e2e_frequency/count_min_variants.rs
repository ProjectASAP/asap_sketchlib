use crate::common::FreqTruth;
use crate::common::specs::CountMinSpec;
use crate::common::storage::*;
use crate::common::streams::{UniformConfig, UniformGenerator, ZipfConfig, ZipfGenerator};
use asap_sketchlib::{CountMin, DataInput, FastPath, RegularPath, Vector2D};

struct VariantCase {
    label: &'static str,
    make: fn() -> Variant,
}

enum Variant {
    V00(CountMin<Vector2D<i32>, RegularPath>),
    V01(CountMin<Vector2D<i32>, FastPath>),
    V02(CountMin<Vector2D<i64>, RegularPath>),
    V03(CountMin<Vector2D<i64>, FastPath>),
    V04(CountMin<Vector2D<i128>, RegularPath>),
    V05(CountMin<Vector2D<i128>, FastPath>),
    V06(CountMin<Vector2D<f64>, RegularPath>),
    V07(CountMin<Vector2D<f64>, FastPath>),
    V08(CountMin<Matrix3X512I32, RegularPath>),
    V09(CountMin<Matrix3X512I32, FastPath>),
    V10(CountMin<Matrix3X512I64, RegularPath>),
    V11(CountMin<Matrix3X512I64, FastPath>),
    V12(CountMin<Matrix3X512I128, RegularPath>),
    V13(CountMin<Matrix3X512I128, FastPath>),
    V14(CountMin<Matrix3X1024I32, RegularPath>),
    V15(CountMin<Matrix3X1024I32, FastPath>),
    V16(CountMin<Matrix3X1024I64, RegularPath>),
    V17(CountMin<Matrix3X1024I64, FastPath>),
    V18(CountMin<Matrix3X1024I128, RegularPath>),
    V19(CountMin<Matrix3X1024I128, FastPath>),
    V20(CountMin<Matrix3X2048I32, RegularPath>),
    V21(CountMin<Matrix3X2048I32, FastPath>),
    V22(CountMin<Matrix3X2048I64, RegularPath>),
    V23(CountMin<Matrix3X2048I64, FastPath>),
    V24(CountMin<Matrix3X2048I128, RegularPath>),
    V25(CountMin<Matrix3X2048I128, FastPath>),
    V26(CountMin<Matrix3X4096I32, RegularPath>),
    V27(CountMin<Matrix3X4096I32, FastPath>),
    V28(CountMin<Matrix3X4096I64, RegularPath>),
    V29(CountMin<Matrix3X4096I64, FastPath>),
    V30(CountMin<Matrix3X4096I128, RegularPath>),
    V31(CountMin<Matrix3X4096I128, FastPath>),
    V32(CountMin<Matrix3X8192I32, RegularPath>),
    V33(CountMin<Matrix3X8192I32, FastPath>),
    V34(CountMin<Matrix3X8192I64, RegularPath>),
    V35(CountMin<Matrix3X8192I64, FastPath>),
    V36(CountMin<Matrix3X8192I128, RegularPath>),
    V37(CountMin<Matrix3X8192I128, FastPath>),
    V38(CountMin<Matrix3X16384I32, RegularPath>),
    V39(CountMin<Matrix3X16384I32, FastPath>),
    V40(CountMin<Matrix3X16384I64, RegularPath>),
    V41(CountMin<Matrix3X16384I64, FastPath>),
    V42(CountMin<Matrix3X16384I128, RegularPath>),
    V43(CountMin<Matrix3X16384I128, FastPath>),
    V44(CountMin<Matrix3X32768I32, RegularPath>),
    V45(CountMin<Matrix3X32768I32, FastPath>),
    V46(CountMin<Matrix3X32768I64, RegularPath>),
    V47(CountMin<Matrix3X32768I64, FastPath>),
    V48(CountMin<Matrix3X32768I128, RegularPath>),
    V49(CountMin<Matrix3X32768I128, FastPath>),
    V50(CountMin<Matrix5X512I32, RegularPath>),
    V51(CountMin<Matrix5X512I32, FastPath>),
    V52(CountMin<Matrix5X512I64, RegularPath>),
    V53(CountMin<Matrix5X512I64, FastPath>),
    V54(CountMin<Matrix5X512I128, RegularPath>),
    V55(CountMin<Matrix5X512I128, FastPath>),
    V56(CountMin<Matrix5X1024I32, RegularPath>),
    V57(CountMin<Matrix5X1024I32, FastPath>),
    V58(CountMin<Matrix5X1024I64, RegularPath>),
    V59(CountMin<Matrix5X1024I64, FastPath>),
    V60(CountMin<Matrix5X1024I128, RegularPath>),
    V61(CountMin<Matrix5X1024I128, FastPath>),
    V62(CountMin<Matrix5X2048I32, RegularPath>),
    V63(CountMin<Matrix5X2048I32, FastPath>),
    V64(CountMin<Matrix5X2048I64, RegularPath>),
    V65(CountMin<Matrix5X2048I64, FastPath>),
    V66(CountMin<Matrix5X2048I128, RegularPath>),
    V67(CountMin<Matrix5X2048I128, FastPath>),
    V68(CountMin<Matrix5X4096I32, RegularPath>),
    V69(CountMin<Matrix5X4096I32, FastPath>),
    V70(CountMin<Matrix5X4096I64, RegularPath>),
    V71(CountMin<Matrix5X4096I64, FastPath>),
    V72(CountMin<Matrix5X4096I128, RegularPath>),
    V73(CountMin<Matrix5X4096I128, FastPath>),
    V74(CountMin<Matrix5X8192I32, RegularPath>),
    V75(CountMin<Matrix5X8192I32, FastPath>),
    V76(CountMin<Matrix5X8192I64, RegularPath>),
    V77(CountMin<Matrix5X8192I64, FastPath>),
    V78(CountMin<Matrix5X8192I128, RegularPath>),
    V79(CountMin<Matrix5X8192I128, FastPath>),
    V80(CountMin<Matrix5X16384I32, RegularPath>),
    V81(CountMin<Matrix5X16384I32, FastPath>),
    V82(CountMin<Matrix5X16384I64, RegularPath>),
    V83(CountMin<Matrix5X16384I64, FastPath>),
    V84(CountMin<Matrix5X16384I128, RegularPath>),
    V85(CountMin<Matrix5X16384I128, FastPath>),
    V86(CountMin<Matrix5X32768I32, RegularPath>),
    V87(CountMin<Matrix5X32768I32, FastPath>),
    V88(CountMin<Matrix5X32768I64, RegularPath>),
    V89(CountMin<Matrix5X32768I64, FastPath>),
    V90(CountMin<Matrix5X32768I128, RegularPath>),
    V91(CountMin<Matrix5X32768I128, FastPath>),
    V92(CountMin<Matrix7X512I32, RegularPath>),
    V93(CountMin<Matrix7X512I32, FastPath>),
    V94(CountMin<Matrix7X512I64, RegularPath>),
    V95(CountMin<Matrix7X512I64, FastPath>),
    V96(CountMin<Matrix7X512I128, RegularPath>),
    V97(CountMin<Matrix7X512I128, FastPath>),
    V98(CountMin<Matrix7X1024I32, RegularPath>),
    V99(CountMin<Matrix7X1024I32, FastPath>),
    V100(CountMin<Matrix7X1024I64, RegularPath>),
    V101(CountMin<Matrix7X1024I64, FastPath>),
    V102(CountMin<Matrix7X1024I128, RegularPath>),
    V103(CountMin<Matrix7X1024I128, FastPath>),
    V104(CountMin<Matrix7X2048I32, RegularPath>),
    V105(CountMin<Matrix7X2048I32, FastPath>),
    V106(CountMin<Matrix7X2048I64, RegularPath>),
    V107(CountMin<Matrix7X2048I64, FastPath>),
    V108(CountMin<Matrix7X2048I128, RegularPath>),
    V109(CountMin<Matrix7X2048I128, FastPath>),
    V110(CountMin<Matrix7X4096I32, RegularPath>),
    V111(CountMin<Matrix7X4096I32, FastPath>),
    V112(CountMin<Matrix7X4096I64, RegularPath>),
    V113(CountMin<Matrix7X4096I64, FastPath>),
    V114(CountMin<Matrix7X4096I128, RegularPath>),
    V115(CountMin<Matrix7X4096I128, FastPath>),
    V116(CountMin<Matrix7X8192I32, RegularPath>),
    V117(CountMin<Matrix7X8192I32, FastPath>),
    V118(CountMin<Matrix7X8192I64, RegularPath>),
    V119(CountMin<Matrix7X8192I64, FastPath>),
    V120(CountMin<Matrix7X8192I128, RegularPath>),
    V121(CountMin<Matrix7X8192I128, FastPath>),
    V122(CountMin<Matrix7X16384I32, RegularPath>),
    V123(CountMin<Matrix7X16384I32, FastPath>),
    V124(CountMin<Matrix7X16384I64, RegularPath>),
    V125(CountMin<Matrix7X16384I64, FastPath>),
    V126(CountMin<Matrix7X16384I128, RegularPath>),
    V127(CountMin<Matrix7X16384I128, FastPath>),
    V128(CountMin<Matrix7X32768I32, RegularPath>),
    V129(CountMin<Matrix7X32768I32, FastPath>),
    V130(CountMin<Matrix7X32768I64, RegularPath>),
    V131(CountMin<Matrix7X32768I64, FastPath>),
    V132(CountMin<Matrix7X32768I128, RegularPath>),
    V133(CountMin<Matrix7X32768I128, FastPath>),
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
            Variant::V33(sketch) => sketch.insert(input),
            Variant::V34(sketch) => sketch.insert(input),
            Variant::V35(sketch) => sketch.insert(input),
            Variant::V36(sketch) => sketch.insert(input),
            Variant::V37(sketch) => sketch.insert(input),
            Variant::V38(sketch) => sketch.insert(input),
            Variant::V39(sketch) => sketch.insert(input),
            Variant::V40(sketch) => sketch.insert(input),
            Variant::V41(sketch) => sketch.insert(input),
            Variant::V42(sketch) => sketch.insert(input),
            Variant::V43(sketch) => sketch.insert(input),
            Variant::V44(sketch) => sketch.insert(input),
            Variant::V45(sketch) => sketch.insert(input),
            Variant::V46(sketch) => sketch.insert(input),
            Variant::V47(sketch) => sketch.insert(input),
            Variant::V48(sketch) => sketch.insert(input),
            Variant::V49(sketch) => sketch.insert(input),
            Variant::V50(sketch) => sketch.insert(input),
            Variant::V51(sketch) => sketch.insert(input),
            Variant::V52(sketch) => sketch.insert(input),
            Variant::V53(sketch) => sketch.insert(input),
            Variant::V54(sketch) => sketch.insert(input),
            Variant::V55(sketch) => sketch.insert(input),
            Variant::V56(sketch) => sketch.insert(input),
            Variant::V57(sketch) => sketch.insert(input),
            Variant::V58(sketch) => sketch.insert(input),
            Variant::V59(sketch) => sketch.insert(input),
            Variant::V60(sketch) => sketch.insert(input),
            Variant::V61(sketch) => sketch.insert(input),
            Variant::V62(sketch) => sketch.insert(input),
            Variant::V63(sketch) => sketch.insert(input),
            Variant::V64(sketch) => sketch.insert(input),
            Variant::V65(sketch) => sketch.insert(input),
            Variant::V66(sketch) => sketch.insert(input),
            Variant::V67(sketch) => sketch.insert(input),
            Variant::V68(sketch) => sketch.insert(input),
            Variant::V69(sketch) => sketch.insert(input),
            Variant::V70(sketch) => sketch.insert(input),
            Variant::V71(sketch) => sketch.insert(input),
            Variant::V72(sketch) => sketch.insert(input),
            Variant::V73(sketch) => sketch.insert(input),
            Variant::V74(sketch) => sketch.insert(input),
            Variant::V75(sketch) => sketch.insert(input),
            Variant::V76(sketch) => sketch.insert(input),
            Variant::V77(sketch) => sketch.insert(input),
            Variant::V78(sketch) => sketch.insert(input),
            Variant::V79(sketch) => sketch.insert(input),
            Variant::V80(sketch) => sketch.insert(input),
            Variant::V81(sketch) => sketch.insert(input),
            Variant::V82(sketch) => sketch.insert(input),
            Variant::V83(sketch) => sketch.insert(input),
            Variant::V84(sketch) => sketch.insert(input),
            Variant::V85(sketch) => sketch.insert(input),
            Variant::V86(sketch) => sketch.insert(input),
            Variant::V87(sketch) => sketch.insert(input),
            Variant::V88(sketch) => sketch.insert(input),
            Variant::V89(sketch) => sketch.insert(input),
            Variant::V90(sketch) => sketch.insert(input),
            Variant::V91(sketch) => sketch.insert(input),
            Variant::V92(sketch) => sketch.insert(input),
            Variant::V93(sketch) => sketch.insert(input),
            Variant::V94(sketch) => sketch.insert(input),
            Variant::V95(sketch) => sketch.insert(input),
            Variant::V96(sketch) => sketch.insert(input),
            Variant::V97(sketch) => sketch.insert(input),
            Variant::V98(sketch) => sketch.insert(input),
            Variant::V99(sketch) => sketch.insert(input),
            Variant::V100(sketch) => sketch.insert(input),
            Variant::V101(sketch) => sketch.insert(input),
            Variant::V102(sketch) => sketch.insert(input),
            Variant::V103(sketch) => sketch.insert(input),
            Variant::V104(sketch) => sketch.insert(input),
            Variant::V105(sketch) => sketch.insert(input),
            Variant::V106(sketch) => sketch.insert(input),
            Variant::V107(sketch) => sketch.insert(input),
            Variant::V108(sketch) => sketch.insert(input),
            Variant::V109(sketch) => sketch.insert(input),
            Variant::V110(sketch) => sketch.insert(input),
            Variant::V111(sketch) => sketch.insert(input),
            Variant::V112(sketch) => sketch.insert(input),
            Variant::V113(sketch) => sketch.insert(input),
            Variant::V114(sketch) => sketch.insert(input),
            Variant::V115(sketch) => sketch.insert(input),
            Variant::V116(sketch) => sketch.insert(input),
            Variant::V117(sketch) => sketch.insert(input),
            Variant::V118(sketch) => sketch.insert(input),
            Variant::V119(sketch) => sketch.insert(input),
            Variant::V120(sketch) => sketch.insert(input),
            Variant::V121(sketch) => sketch.insert(input),
            Variant::V122(sketch) => sketch.insert(input),
            Variant::V123(sketch) => sketch.insert(input),
            Variant::V124(sketch) => sketch.insert(input),
            Variant::V125(sketch) => sketch.insert(input),
            Variant::V126(sketch) => sketch.insert(input),
            Variant::V127(sketch) => sketch.insert(input),
            Variant::V128(sketch) => sketch.insert(input),
            Variant::V129(sketch) => sketch.insert(input),
            Variant::V130(sketch) => sketch.insert(input),
            Variant::V131(sketch) => sketch.insert(input),
            Variant::V132(sketch) => sketch.insert(input),
            Variant::V133(sketch) => sketch.insert(input),
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
            Variant::V08(sketch) => sketch.estimate(input) as f64,
            Variant::V09(sketch) => sketch.estimate(input) as f64,
            Variant::V10(sketch) => sketch.estimate(input) as f64,
            Variant::V11(sketch) => sketch.estimate(input) as f64,
            Variant::V12(sketch) => sketch.estimate(input) as f64,
            Variant::V13(sketch) => sketch.estimate(input) as f64,
            Variant::V14(sketch) => sketch.estimate(input) as f64,
            Variant::V15(sketch) => sketch.estimate(input) as f64,
            Variant::V16(sketch) => sketch.estimate(input) as f64,
            Variant::V17(sketch) => sketch.estimate(input) as f64,
            Variant::V18(sketch) => sketch.estimate(input) as f64,
            Variant::V19(sketch) => sketch.estimate(input) as f64,
            Variant::V20(sketch) => sketch.estimate(input) as f64,
            Variant::V21(sketch) => sketch.estimate(input) as f64,
            Variant::V22(sketch) => sketch.estimate(input) as f64,
            Variant::V23(sketch) => sketch.estimate(input) as f64,
            Variant::V24(sketch) => sketch.estimate(input) as f64,
            Variant::V25(sketch) => sketch.estimate(input) as f64,
            Variant::V26(sketch) => sketch.estimate(input) as f64,
            Variant::V27(sketch) => sketch.estimate(input) as f64,
            Variant::V28(sketch) => sketch.estimate(input) as f64,
            Variant::V29(sketch) => sketch.estimate(input) as f64,
            Variant::V30(sketch) => sketch.estimate(input) as f64,
            Variant::V31(sketch) => sketch.estimate(input) as f64,
            Variant::V32(sketch) => sketch.estimate(input) as f64,
            Variant::V33(sketch) => sketch.estimate(input) as f64,
            Variant::V34(sketch) => sketch.estimate(input) as f64,
            Variant::V35(sketch) => sketch.estimate(input) as f64,
            Variant::V36(sketch) => sketch.estimate(input) as f64,
            Variant::V37(sketch) => sketch.estimate(input) as f64,
            Variant::V38(sketch) => sketch.estimate(input) as f64,
            Variant::V39(sketch) => sketch.estimate(input) as f64,
            Variant::V40(sketch) => sketch.estimate(input) as f64,
            Variant::V41(sketch) => sketch.estimate(input) as f64,
            Variant::V42(sketch) => sketch.estimate(input) as f64,
            Variant::V43(sketch) => sketch.estimate(input) as f64,
            Variant::V44(sketch) => sketch.estimate(input) as f64,
            Variant::V45(sketch) => sketch.estimate(input) as f64,
            Variant::V46(sketch) => sketch.estimate(input) as f64,
            Variant::V47(sketch) => sketch.estimate(input) as f64,
            Variant::V48(sketch) => sketch.estimate(input) as f64,
            Variant::V49(sketch) => sketch.estimate(input) as f64,
            Variant::V50(sketch) => sketch.estimate(input) as f64,
            Variant::V51(sketch) => sketch.estimate(input) as f64,
            Variant::V52(sketch) => sketch.estimate(input) as f64,
            Variant::V53(sketch) => sketch.estimate(input) as f64,
            Variant::V54(sketch) => sketch.estimate(input) as f64,
            Variant::V55(sketch) => sketch.estimate(input) as f64,
            Variant::V56(sketch) => sketch.estimate(input) as f64,
            Variant::V57(sketch) => sketch.estimate(input) as f64,
            Variant::V58(sketch) => sketch.estimate(input) as f64,
            Variant::V59(sketch) => sketch.estimate(input) as f64,
            Variant::V60(sketch) => sketch.estimate(input) as f64,
            Variant::V61(sketch) => sketch.estimate(input) as f64,
            Variant::V62(sketch) => sketch.estimate(input) as f64,
            Variant::V63(sketch) => sketch.estimate(input) as f64,
            Variant::V64(sketch) => sketch.estimate(input) as f64,
            Variant::V65(sketch) => sketch.estimate(input) as f64,
            Variant::V66(sketch) => sketch.estimate(input) as f64,
            Variant::V67(sketch) => sketch.estimate(input) as f64,
            Variant::V68(sketch) => sketch.estimate(input) as f64,
            Variant::V69(sketch) => sketch.estimate(input) as f64,
            Variant::V70(sketch) => sketch.estimate(input) as f64,
            Variant::V71(sketch) => sketch.estimate(input) as f64,
            Variant::V72(sketch) => sketch.estimate(input) as f64,
            Variant::V73(sketch) => sketch.estimate(input) as f64,
            Variant::V74(sketch) => sketch.estimate(input) as f64,
            Variant::V75(sketch) => sketch.estimate(input) as f64,
            Variant::V76(sketch) => sketch.estimate(input) as f64,
            Variant::V77(sketch) => sketch.estimate(input) as f64,
            Variant::V78(sketch) => sketch.estimate(input) as f64,
            Variant::V79(sketch) => sketch.estimate(input) as f64,
            Variant::V80(sketch) => sketch.estimate(input) as f64,
            Variant::V81(sketch) => sketch.estimate(input) as f64,
            Variant::V82(sketch) => sketch.estimate(input) as f64,
            Variant::V83(sketch) => sketch.estimate(input) as f64,
            Variant::V84(sketch) => sketch.estimate(input) as f64,
            Variant::V85(sketch) => sketch.estimate(input) as f64,
            Variant::V86(sketch) => sketch.estimate(input) as f64,
            Variant::V87(sketch) => sketch.estimate(input) as f64,
            Variant::V88(sketch) => sketch.estimate(input) as f64,
            Variant::V89(sketch) => sketch.estimate(input) as f64,
            Variant::V90(sketch) => sketch.estimate(input) as f64,
            Variant::V91(sketch) => sketch.estimate(input) as f64,
            Variant::V92(sketch) => sketch.estimate(input) as f64,
            Variant::V93(sketch) => sketch.estimate(input) as f64,
            Variant::V94(sketch) => sketch.estimate(input) as f64,
            Variant::V95(sketch) => sketch.estimate(input) as f64,
            Variant::V96(sketch) => sketch.estimate(input) as f64,
            Variant::V97(sketch) => sketch.estimate(input) as f64,
            Variant::V98(sketch) => sketch.estimate(input) as f64,
            Variant::V99(sketch) => sketch.estimate(input) as f64,
            Variant::V100(sketch) => sketch.estimate(input) as f64,
            Variant::V101(sketch) => sketch.estimate(input) as f64,
            Variant::V102(sketch) => sketch.estimate(input) as f64,
            Variant::V103(sketch) => sketch.estimate(input) as f64,
            Variant::V104(sketch) => sketch.estimate(input) as f64,
            Variant::V105(sketch) => sketch.estimate(input) as f64,
            Variant::V106(sketch) => sketch.estimate(input) as f64,
            Variant::V107(sketch) => sketch.estimate(input) as f64,
            Variant::V108(sketch) => sketch.estimate(input) as f64,
            Variant::V109(sketch) => sketch.estimate(input) as f64,
            Variant::V110(sketch) => sketch.estimate(input) as f64,
            Variant::V111(sketch) => sketch.estimate(input) as f64,
            Variant::V112(sketch) => sketch.estimate(input) as f64,
            Variant::V113(sketch) => sketch.estimate(input) as f64,
            Variant::V114(sketch) => sketch.estimate(input) as f64,
            Variant::V115(sketch) => sketch.estimate(input) as f64,
            Variant::V116(sketch) => sketch.estimate(input) as f64,
            Variant::V117(sketch) => sketch.estimate(input) as f64,
            Variant::V118(sketch) => sketch.estimate(input) as f64,
            Variant::V119(sketch) => sketch.estimate(input) as f64,
            Variant::V120(sketch) => sketch.estimate(input) as f64,
            Variant::V121(sketch) => sketch.estimate(input) as f64,
            Variant::V122(sketch) => sketch.estimate(input) as f64,
            Variant::V123(sketch) => sketch.estimate(input) as f64,
            Variant::V124(sketch) => sketch.estimate(input) as f64,
            Variant::V125(sketch) => sketch.estimate(input) as f64,
            Variant::V126(sketch) => sketch.estimate(input) as f64,
            Variant::V127(sketch) => sketch.estimate(input) as f64,
            Variant::V128(sketch) => sketch.estimate(input) as f64,
            Variant::V129(sketch) => sketch.estimate(input) as f64,
            Variant::V130(sketch) => sketch.estimate(input) as f64,
            Variant::V131(sketch) => sketch.estimate(input) as f64,
            Variant::V132(sketch) => sketch.estimate(input) as f64,
            Variant::V133(sketch) => sketch.estimate(input) as f64,
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
            Variant::V20(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V21(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V22(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V23(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V24(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V25(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V26(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V27(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V28(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V29(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V30(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V31(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V32(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V33(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V34(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V35(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V36(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V37(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V38(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V39(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V40(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V41(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V42(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V43(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V44(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V45(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V46(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V47(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V48(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V49(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V50(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V51(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V52(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V53(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V54(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V55(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V56(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V57(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V58(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V59(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V60(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V61(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V62(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V63(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V64(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V65(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V66(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V67(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V68(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V69(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V70(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V71(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V72(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V73(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V74(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V75(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V76(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V77(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V78(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V79(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V80(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V81(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V82(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V83(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V84(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V85(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V86(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V87(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V88(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V89(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V90(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V91(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V92(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V93(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V94(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V95(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V96(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V97(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V98(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V99(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V100(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V101(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V102(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V103(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V104(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V105(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V106(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V107(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V108(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V109(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V110(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V111(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V112(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V113(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V114(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V115(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V116(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V117(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V118(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V119(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V120(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V121(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V122(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V123(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V124(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V125(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V126(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V127(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V128(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V129(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V130(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V131(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V132(sketch) => (sketch.rows(), sketch.cols()),
            Variant::V133(sketch) => (sketch.rows(), sketch.cols()),
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
            Self::I64(label, _)
            | Self::U64(label, _)
            | Self::F64(label, _)
            | Self::Str(label, _) => label,
        }
    }
    fn truth(&self) -> FreqTruth {
        match self {
            Self::I64(_, values) => FreqTruth::from_data(values),
            Self::U64(_, values) => {
                FreqTruth::from_data(&values.iter().map(|v| *v as i64).collect::<Vec<_>>())
            }
            Self::F64(_, values) => {
                FreqTruth::from_data(&values.iter().map(|v| *v as i64).collect::<Vec<_>>())
            }
            Self::Str(_, values) => FreqTruth::from_data(
                &values
                    .iter()
                    .map(|v| v[1..].parse().unwrap())
                    .collect::<Vec<_>>(),
            ),
        }
    }
    fn insert_all(&self, sketch: &mut Variant) {
        match self {
            Self::I64(_, values) => values
                .iter()
                .for_each(|v| sketch.insert(&DataInput::I64(*v))),
            Self::U64(_, values) => values
                .iter()
                .for_each(|v| sketch.insert(&DataInput::U64(*v))),
            Self::F64(_, values) => values
                .iter()
                .for_each(|v| sketch.insert(&DataInput::F64(*v))),
            Self::Str(_, values) => values
                .iter()
                .for_each(|v| sketch.insert(&DataInput::Str(v))),
        }
    }
    fn estimate(&self, sketch: &Variant, key: i64) -> f64 {
        match self {
            Self::I64(..) => sketch.estimate(&DataInput::I64(key)),
            Self::U64(..) => sketch.estimate(&DataInput::U64(key as u64)),
            Self::F64(..) => sketch.estimate(&DataInput::F64(key as f64)),
            Self::Str(..) => sketch.estimate(&DataInput::Str(&format!("k{key}"))),
        }
    }
}

fn data_cases() -> Vec<DataCase> {
    vec![
        DataCase::I64(
            "zipf(0.7)/i64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 0.7,
                seed: 0x10BE_C700_0001_0001,
            }),
        ),
        DataCase::U64(
            "zipf(0.7)/u64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 0.7,
                seed: 0x10BE_C700_0001_0001,
            }),
        ),
        DataCase::F64(
            "zipf(0.7)/f64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 0.7,
                seed: 0x10BE_C700_0001_0001,
            }),
        ),
        DataCase::Str(
            "zipf(0.7)/str",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 0.7,
                seed: 0x10BE_C700_0001_0001,
            })
            .into_iter()
            .map(|value: u64| format!("k{value}"))
            .collect(),
        ),
        DataCase::I64(
            "zipf(1.1)/i64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.1,
                seed: 0x10BE_C700_0001_0002,
            }),
        ),
        DataCase::U64(
            "zipf(1.1)/u64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.1,
                seed: 0x10BE_C700_0001_0002,
            }),
        ),
        DataCase::F64(
            "zipf(1.1)/f64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.1,
                seed: 0x10BE_C700_0001_0002,
            }),
        ),
        DataCase::Str(
            "zipf(1.1)/str",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.1,
                seed: 0x10BE_C700_0001_0002,
            })
            .into_iter()
            .map(|value: u64| format!("k{value}"))
            .collect(),
        ),
        DataCase::I64(
            "zipf(1.5)/i64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.5,
                seed: 0x10BE_C700_0001_0003,
            }),
        ),
        DataCase::U64(
            "zipf(1.5)/u64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.5,
                seed: 0x10BE_C700_0001_0003,
            }),
        ),
        DataCase::F64(
            "zipf(1.5)/f64",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.5,
                seed: 0x10BE_C700_0001_0003,
            }),
        ),
        DataCase::Str(
            "zipf(1.5)/str",
            ZipfGenerator::generate(&ZipfConfig {
                count: 10_000,
                domain: 1_024,
                exponent: 1.5,
                seed: 0x10BE_C700_0001_0003,
            })
            .into_iter()
            .map(|value: u64| format!("k{value}"))
            .collect(),
        ),
        DataCase::I64(
            "uniform/i64",
            UniformGenerator::generate(&UniformConfig {
                count: 10_000,
                domain: 1_024,
                seed: 0x10BE_C700_0001_0004,
            }),
        ),
        DataCase::U64(
            "uniform/u64",
            UniformGenerator::generate(&UniformConfig {
                count: 10_000,
                domain: 1_024,
                seed: 0x10BE_C700_0001_0004,
            }),
        ),
        DataCase::F64(
            "uniform/f64",
            UniformGenerator::generate(&UniformConfig {
                count: 10_000,
                domain: 1_024,
                seed: 0x10BE_C700_0001_0004,
            }),
        ),
        DataCase::Str(
            "uniform/str",
            UniformGenerator::generate(&UniformConfig {
                count: 10_000,
                domain: 1_024,
                seed: 0x10BE_C700_0001_0004,
            })
            .into_iter()
            .map(|value: u64| format!("k{value}"))
            .collect(),
        ),
    ]
}

fn variant_cases() -> Vec<VariantCase> {
    vec![
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x512",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x512",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x512",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x512",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x512",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x512",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x512",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x512",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x1024",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x1024",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x1024",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x1024",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x1024",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x1024",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x1024",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x1024",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x2048",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x2048",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x2048",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x2048",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x2048",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x2048",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x2048",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x2048",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x4096",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x4096",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x4096",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x4096",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x4096",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x4096",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x4096",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x4096",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x8192",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x8192",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x8192",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x8192",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x8192",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x8192",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x8192",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x8192",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x16384",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x16384",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x16384",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x16384",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x16384",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x16384",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x16384",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x16384",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x32768",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x32768",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x32768",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x32768",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x32768",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x32768",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x32768",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x32768",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x512",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x512",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x512",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x512",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x512",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x512",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x512",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x512",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x1024",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x1024",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x1024",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x1024",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x1024",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x1024",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x1024",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x1024",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x2048",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x2048",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x2048",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x2048",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x2048",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x2048",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x2048",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x2048",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x4096",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x4096",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x4096",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x4096",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x4096",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x4096",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x4096",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x4096",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x8192",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x8192",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x8192",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x8192",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x8192",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x8192",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x8192",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x8192",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x16384",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x16384",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x16384",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x16384",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x16384",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x16384",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x16384",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x16384",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x32768",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x32768",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x32768",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x32768",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x32768",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x32768",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x32768",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x32768",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x512",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x512",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x512",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x512",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x512",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x512",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x512",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x512",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x1024",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x1024",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x1024",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x1024",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x1024",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x1024",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x1024",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x1024",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x2048",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x2048",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x2048",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x2048",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x2048",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x2048",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x2048",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x2048",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x4096",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x4096",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x4096",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x4096",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x4096",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x4096",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x4096",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x4096",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x8192",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x8192",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x8192",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x8192",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x8192",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x8192",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x8192",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x8192",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x16384",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x16384",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x16384",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x16384",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x16384",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x16384",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x16384",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x16384",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x32768",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x32768",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x32768",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x32768",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x32768",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x32768",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x32768",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x32768",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I32, RegularPath>",
            make: || {
                Variant::V08(CountMin::<Matrix3X512I32, RegularPath>::from_storage(
                    Matrix3X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I32, FastPath>",
            make: || {
                Variant::V09(CountMin::<Matrix3X512I32, FastPath>::from_storage(
                    Matrix3X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I64, RegularPath>",
            make: || {
                Variant::V10(CountMin::<Matrix3X512I64, RegularPath>::from_storage(
                    Matrix3X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I64, FastPath>",
            make: || {
                Variant::V11(CountMin::<Matrix3X512I64, FastPath>::from_storage(
                    Matrix3X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I128, RegularPath>",
            make: || {
                Variant::V12(CountMin::<Matrix3X512I128, RegularPath>::from_storage(
                    Matrix3X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X512I128, FastPath>",
            make: || {
                Variant::V13(CountMin::<Matrix3X512I128, FastPath>::from_storage(
                    Matrix3X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I32, RegularPath>",
            make: || {
                Variant::V14(CountMin::<Matrix3X1024I32, RegularPath>::from_storage(
                    Matrix3X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I32, FastPath>",
            make: || {
                Variant::V15(CountMin::<Matrix3X1024I32, FastPath>::from_storage(
                    Matrix3X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I64, RegularPath>",
            make: || {
                Variant::V16(CountMin::<Matrix3X1024I64, RegularPath>::from_storage(
                    Matrix3X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I64, FastPath>",
            make: || {
                Variant::V17(CountMin::<Matrix3X1024I64, FastPath>::from_storage(
                    Matrix3X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I128, RegularPath>",
            make: || {
                Variant::V18(CountMin::<Matrix3X1024I128, RegularPath>::from_storage(
                    Matrix3X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X1024I128, FastPath>",
            make: || {
                Variant::V19(CountMin::<Matrix3X1024I128, FastPath>::from_storage(
                    Matrix3X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I32, RegularPath>",
            make: || {
                Variant::V20(CountMin::<Matrix3X2048I32, RegularPath>::from_storage(
                    Matrix3X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I32, FastPath>",
            make: || {
                Variant::V21(CountMin::<Matrix3X2048I32, FastPath>::from_storage(
                    Matrix3X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I64, RegularPath>",
            make: || {
                Variant::V22(CountMin::<Matrix3X2048I64, RegularPath>::from_storage(
                    Matrix3X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I64, FastPath>",
            make: || {
                Variant::V23(CountMin::<Matrix3X2048I64, FastPath>::from_storage(
                    Matrix3X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I128, RegularPath>",
            make: || {
                Variant::V24(CountMin::<Matrix3X2048I128, RegularPath>::from_storage(
                    Matrix3X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X2048I128, FastPath>",
            make: || {
                Variant::V25(CountMin::<Matrix3X2048I128, FastPath>::from_storage(
                    Matrix3X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I32, RegularPath>",
            make: || {
                Variant::V26(CountMin::<Matrix3X4096I32, RegularPath>::from_storage(
                    Matrix3X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I32, FastPath>",
            make: || {
                Variant::V27(CountMin::<Matrix3X4096I32, FastPath>::from_storage(
                    Matrix3X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I64, RegularPath>",
            make: || {
                Variant::V28(CountMin::<Matrix3X4096I64, RegularPath>::from_storage(
                    Matrix3X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I64, FastPath>",
            make: || {
                Variant::V29(CountMin::<Matrix3X4096I64, FastPath>::from_storage(
                    Matrix3X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I128, RegularPath>",
            make: || {
                Variant::V30(CountMin::<Matrix3X4096I128, RegularPath>::from_storage(
                    Matrix3X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X4096I128, FastPath>",
            make: || {
                Variant::V31(CountMin::<Matrix3X4096I128, FastPath>::from_storage(
                    Matrix3X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I32, RegularPath>",
            make: || {
                Variant::V32(CountMin::<Matrix3X8192I32, RegularPath>::from_storage(
                    Matrix3X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I32, FastPath>",
            make: || {
                Variant::V33(CountMin::<Matrix3X8192I32, FastPath>::from_storage(
                    Matrix3X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I64, RegularPath>",
            make: || {
                Variant::V34(CountMin::<Matrix3X8192I64, RegularPath>::from_storage(
                    Matrix3X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I64, FastPath>",
            make: || {
                Variant::V35(CountMin::<Matrix3X8192I64, FastPath>::from_storage(
                    Matrix3X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I128, RegularPath>",
            make: || {
                Variant::V36(CountMin::<Matrix3X8192I128, RegularPath>::from_storage(
                    Matrix3X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X8192I128, FastPath>",
            make: || {
                Variant::V37(CountMin::<Matrix3X8192I128, FastPath>::from_storage(
                    Matrix3X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I32, RegularPath>",
            make: || {
                Variant::V38(CountMin::<Matrix3X16384I32, RegularPath>::from_storage(
                    Matrix3X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I32, FastPath>",
            make: || {
                Variant::V39(CountMin::<Matrix3X16384I32, FastPath>::from_storage(
                    Matrix3X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I64, RegularPath>",
            make: || {
                Variant::V40(CountMin::<Matrix3X16384I64, RegularPath>::from_storage(
                    Matrix3X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I64, FastPath>",
            make: || {
                Variant::V41(CountMin::<Matrix3X16384I64, FastPath>::from_storage(
                    Matrix3X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I128, RegularPath>",
            make: || {
                Variant::V42(CountMin::<Matrix3X16384I128, RegularPath>::from_storage(
                    Matrix3X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X16384I128, FastPath>",
            make: || {
                Variant::V43(CountMin::<Matrix3X16384I128, FastPath>::from_storage(
                    Matrix3X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I32, RegularPath>",
            make: || {
                Variant::V44(CountMin::<Matrix3X32768I32, RegularPath>::from_storage(
                    Matrix3X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I32, FastPath>",
            make: || {
                Variant::V45(CountMin::<Matrix3X32768I32, FastPath>::from_storage(
                    Matrix3X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I64, RegularPath>",
            make: || {
                Variant::V46(CountMin::<Matrix3X32768I64, RegularPath>::from_storage(
                    Matrix3X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I64, FastPath>",
            make: || {
                Variant::V47(CountMin::<Matrix3X32768I64, FastPath>::from_storage(
                    Matrix3X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I128, RegularPath>",
            make: || {
                Variant::V48(CountMin::<Matrix3X32768I128, RegularPath>::from_storage(
                    Matrix3X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix3X32768I128, FastPath>",
            make: || {
                Variant::V49(CountMin::<Matrix3X32768I128, FastPath>::from_storage(
                    Matrix3X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I32, RegularPath>",
            make: || {
                Variant::V50(CountMin::<Matrix5X512I32, RegularPath>::from_storage(
                    Matrix5X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I32, FastPath>",
            make: || {
                Variant::V51(CountMin::<Matrix5X512I32, FastPath>::from_storage(
                    Matrix5X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I64, RegularPath>",
            make: || {
                Variant::V52(CountMin::<Matrix5X512I64, RegularPath>::from_storage(
                    Matrix5X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I64, FastPath>",
            make: || {
                Variant::V53(CountMin::<Matrix5X512I64, FastPath>::from_storage(
                    Matrix5X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I128, RegularPath>",
            make: || {
                Variant::V54(CountMin::<Matrix5X512I128, RegularPath>::from_storage(
                    Matrix5X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X512I128, FastPath>",
            make: || {
                Variant::V55(CountMin::<Matrix5X512I128, FastPath>::from_storage(
                    Matrix5X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I32, RegularPath>",
            make: || {
                Variant::V56(CountMin::<Matrix5X1024I32, RegularPath>::from_storage(
                    Matrix5X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I32, FastPath>",
            make: || {
                Variant::V57(CountMin::<Matrix5X1024I32, FastPath>::from_storage(
                    Matrix5X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I64, RegularPath>",
            make: || {
                Variant::V58(CountMin::<Matrix5X1024I64, RegularPath>::from_storage(
                    Matrix5X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I64, FastPath>",
            make: || {
                Variant::V59(CountMin::<Matrix5X1024I64, FastPath>::from_storage(
                    Matrix5X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I128, RegularPath>",
            make: || {
                Variant::V60(CountMin::<Matrix5X1024I128, RegularPath>::from_storage(
                    Matrix5X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X1024I128, FastPath>",
            make: || {
                Variant::V61(CountMin::<Matrix5X1024I128, FastPath>::from_storage(
                    Matrix5X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I32, RegularPath>",
            make: || {
                Variant::V62(CountMin::<Matrix5X2048I32, RegularPath>::from_storage(
                    Matrix5X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I32, FastPath>",
            make: || {
                Variant::V63(CountMin::<Matrix5X2048I32, FastPath>::from_storage(
                    Matrix5X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I64, RegularPath>",
            make: || {
                Variant::V64(CountMin::<Matrix5X2048I64, RegularPath>::from_storage(
                    Matrix5X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I64, FastPath>",
            make: || {
                Variant::V65(CountMin::<Matrix5X2048I64, FastPath>::from_storage(
                    Matrix5X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I128, RegularPath>",
            make: || {
                Variant::V66(CountMin::<Matrix5X2048I128, RegularPath>::from_storage(
                    Matrix5X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X2048I128, FastPath>",
            make: || {
                Variant::V67(CountMin::<Matrix5X2048I128, FastPath>::from_storage(
                    Matrix5X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I32, RegularPath>",
            make: || {
                Variant::V68(CountMin::<Matrix5X4096I32, RegularPath>::from_storage(
                    Matrix5X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I32, FastPath>",
            make: || {
                Variant::V69(CountMin::<Matrix5X4096I32, FastPath>::from_storage(
                    Matrix5X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I64, RegularPath>",
            make: || {
                Variant::V70(CountMin::<Matrix5X4096I64, RegularPath>::from_storage(
                    Matrix5X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I64, FastPath>",
            make: || {
                Variant::V71(CountMin::<Matrix5X4096I64, FastPath>::from_storage(
                    Matrix5X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I128, RegularPath>",
            make: || {
                Variant::V72(CountMin::<Matrix5X4096I128, RegularPath>::from_storage(
                    Matrix5X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X4096I128, FastPath>",
            make: || {
                Variant::V73(CountMin::<Matrix5X4096I128, FastPath>::from_storage(
                    Matrix5X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I32, RegularPath>",
            make: || {
                Variant::V74(CountMin::<Matrix5X8192I32, RegularPath>::from_storage(
                    Matrix5X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I32, FastPath>",
            make: || {
                Variant::V75(CountMin::<Matrix5X8192I32, FastPath>::from_storage(
                    Matrix5X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I64, RegularPath>",
            make: || {
                Variant::V76(CountMin::<Matrix5X8192I64, RegularPath>::from_storage(
                    Matrix5X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I64, FastPath>",
            make: || {
                Variant::V77(CountMin::<Matrix5X8192I64, FastPath>::from_storage(
                    Matrix5X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I128, RegularPath>",
            make: || {
                Variant::V78(CountMin::<Matrix5X8192I128, RegularPath>::from_storage(
                    Matrix5X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X8192I128, FastPath>",
            make: || {
                Variant::V79(CountMin::<Matrix5X8192I128, FastPath>::from_storage(
                    Matrix5X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I32, RegularPath>",
            make: || {
                Variant::V80(CountMin::<Matrix5X16384I32, RegularPath>::from_storage(
                    Matrix5X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I32, FastPath>",
            make: || {
                Variant::V81(CountMin::<Matrix5X16384I32, FastPath>::from_storage(
                    Matrix5X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I64, RegularPath>",
            make: || {
                Variant::V82(CountMin::<Matrix5X16384I64, RegularPath>::from_storage(
                    Matrix5X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I64, FastPath>",
            make: || {
                Variant::V83(CountMin::<Matrix5X16384I64, FastPath>::from_storage(
                    Matrix5X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I128, RegularPath>",
            make: || {
                Variant::V84(CountMin::<Matrix5X16384I128, RegularPath>::from_storage(
                    Matrix5X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X16384I128, FastPath>",
            make: || {
                Variant::V85(CountMin::<Matrix5X16384I128, FastPath>::from_storage(
                    Matrix5X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I32, RegularPath>",
            make: || {
                Variant::V86(CountMin::<Matrix5X32768I32, RegularPath>::from_storage(
                    Matrix5X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I32, FastPath>",
            make: || {
                Variant::V87(CountMin::<Matrix5X32768I32, FastPath>::from_storage(
                    Matrix5X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I64, RegularPath>",
            make: || {
                Variant::V88(CountMin::<Matrix5X32768I64, RegularPath>::from_storage(
                    Matrix5X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I64, FastPath>",
            make: || {
                Variant::V89(CountMin::<Matrix5X32768I64, FastPath>::from_storage(
                    Matrix5X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I128, RegularPath>",
            make: || {
                Variant::V90(CountMin::<Matrix5X32768I128, RegularPath>::from_storage(
                    Matrix5X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix5X32768I128, FastPath>",
            make: || {
                Variant::V91(CountMin::<Matrix5X32768I128, FastPath>::from_storage(
                    Matrix5X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I32, RegularPath>",
            make: || {
                Variant::V92(CountMin::<Matrix7X512I32, RegularPath>::from_storage(
                    Matrix7X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I32, FastPath>",
            make: || {
                Variant::V93(CountMin::<Matrix7X512I32, FastPath>::from_storage(
                    Matrix7X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I64, RegularPath>",
            make: || {
                Variant::V94(CountMin::<Matrix7X512I64, RegularPath>::from_storage(
                    Matrix7X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I64, FastPath>",
            make: || {
                Variant::V95(CountMin::<Matrix7X512I64, FastPath>::from_storage(
                    Matrix7X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I128, RegularPath>",
            make: || {
                Variant::V96(CountMin::<Matrix7X512I128, RegularPath>::from_storage(
                    Matrix7X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X512I128, FastPath>",
            make: || {
                Variant::V97(CountMin::<Matrix7X512I128, FastPath>::from_storage(
                    Matrix7X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I32, RegularPath>",
            make: || {
                Variant::V98(CountMin::<Matrix7X1024I32, RegularPath>::from_storage(
                    Matrix7X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I32, FastPath>",
            make: || {
                Variant::V99(CountMin::<Matrix7X1024I32, FastPath>::from_storage(
                    Matrix7X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I64, RegularPath>",
            make: || {
                Variant::V100(CountMin::<Matrix7X1024I64, RegularPath>::from_storage(
                    Matrix7X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I64, FastPath>",
            make: || {
                Variant::V101(CountMin::<Matrix7X1024I64, FastPath>::from_storage(
                    Matrix7X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I128, RegularPath>",
            make: || {
                Variant::V102(CountMin::<Matrix7X1024I128, RegularPath>::from_storage(
                    Matrix7X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X1024I128, FastPath>",
            make: || {
                Variant::V103(CountMin::<Matrix7X1024I128, FastPath>::from_storage(
                    Matrix7X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I32, RegularPath>",
            make: || {
                Variant::V104(CountMin::<Matrix7X2048I32, RegularPath>::from_storage(
                    Matrix7X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I32, FastPath>",
            make: || {
                Variant::V105(CountMin::<Matrix7X2048I32, FastPath>::from_storage(
                    Matrix7X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I64, RegularPath>",
            make: || {
                Variant::V106(CountMin::<Matrix7X2048I64, RegularPath>::from_storage(
                    Matrix7X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I64, FastPath>",
            make: || {
                Variant::V107(CountMin::<Matrix7X2048I64, FastPath>::from_storage(
                    Matrix7X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I128, RegularPath>",
            make: || {
                Variant::V108(CountMin::<Matrix7X2048I128, RegularPath>::from_storage(
                    Matrix7X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X2048I128, FastPath>",
            make: || {
                Variant::V109(CountMin::<Matrix7X2048I128, FastPath>::from_storage(
                    Matrix7X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I32, RegularPath>",
            make: || {
                Variant::V110(CountMin::<Matrix7X4096I32, RegularPath>::from_storage(
                    Matrix7X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I32, FastPath>",
            make: || {
                Variant::V111(CountMin::<Matrix7X4096I32, FastPath>::from_storage(
                    Matrix7X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I64, RegularPath>",
            make: || {
                Variant::V112(CountMin::<Matrix7X4096I64, RegularPath>::from_storage(
                    Matrix7X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I64, FastPath>",
            make: || {
                Variant::V113(CountMin::<Matrix7X4096I64, FastPath>::from_storage(
                    Matrix7X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I128, RegularPath>",
            make: || {
                Variant::V114(CountMin::<Matrix7X4096I128, RegularPath>::from_storage(
                    Matrix7X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X4096I128, FastPath>",
            make: || {
                Variant::V115(CountMin::<Matrix7X4096I128, FastPath>::from_storage(
                    Matrix7X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I32, RegularPath>",
            make: || {
                Variant::V116(CountMin::<Matrix7X8192I32, RegularPath>::from_storage(
                    Matrix7X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I32, FastPath>",
            make: || {
                Variant::V117(CountMin::<Matrix7X8192I32, FastPath>::from_storage(
                    Matrix7X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I64, RegularPath>",
            make: || {
                Variant::V118(CountMin::<Matrix7X8192I64, RegularPath>::from_storage(
                    Matrix7X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I64, FastPath>",
            make: || {
                Variant::V119(CountMin::<Matrix7X8192I64, FastPath>::from_storage(
                    Matrix7X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I128, RegularPath>",
            make: || {
                Variant::V120(CountMin::<Matrix7X8192I128, RegularPath>::from_storage(
                    Matrix7X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X8192I128, FastPath>",
            make: || {
                Variant::V121(CountMin::<Matrix7X8192I128, FastPath>::from_storage(
                    Matrix7X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I32, RegularPath>",
            make: || {
                Variant::V122(CountMin::<Matrix7X16384I32, RegularPath>::from_storage(
                    Matrix7X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I32, FastPath>",
            make: || {
                Variant::V123(CountMin::<Matrix7X16384I32, FastPath>::from_storage(
                    Matrix7X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I64, RegularPath>",
            make: || {
                Variant::V124(CountMin::<Matrix7X16384I64, RegularPath>::from_storage(
                    Matrix7X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I64, FastPath>",
            make: || {
                Variant::V125(CountMin::<Matrix7X16384I64, FastPath>::from_storage(
                    Matrix7X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I128, RegularPath>",
            make: || {
                Variant::V126(CountMin::<Matrix7X16384I128, RegularPath>::from_storage(
                    Matrix7X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X16384I128, FastPath>",
            make: || {
                Variant::V127(CountMin::<Matrix7X16384I128, FastPath>::from_storage(
                    Matrix7X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I32, RegularPath>",
            make: || {
                Variant::V128(CountMin::<Matrix7X32768I32, RegularPath>::from_storage(
                    Matrix7X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I32, FastPath>",
            make: || {
                Variant::V129(CountMin::<Matrix7X32768I32, FastPath>::from_storage(
                    Matrix7X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I64, RegularPath>",
            make: || {
                Variant::V130(CountMin::<Matrix7X32768I64, RegularPath>::from_storage(
                    Matrix7X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I64, FastPath>",
            make: || {
                Variant::V131(CountMin::<Matrix7X32768I64, FastPath>::from_storage(
                    Matrix7X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I128, RegularPath>",
            make: || {
                Variant::V132(CountMin::<Matrix7X32768I128, RegularPath>::from_storage(
                    Matrix7X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Matrix7X32768I128, FastPath>",
            make: || {
                Variant::V133(CountMin::<Matrix7X32768I128, FastPath>::from_storage(
                    Matrix7X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x100",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x100",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x100",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x100",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x100",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x100",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x100",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x100",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x1000",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x1000",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x1000",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x1000",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x1000",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x1000",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x1000",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x1000",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 3x4095",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 3x4095",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 3x4095",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 3x4095",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 3x4095",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 3x4095",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 3x4095",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 3x4095",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x100",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x100",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x100",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x100",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x100",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x100",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x100",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x100",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x1000",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x1000",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x1000",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x1000",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x1000",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x1000",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x1000",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x1000",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 5x4095",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 5x4095",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 5x4095",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 5x4095",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 5x4095",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 5x4095",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 5x4095",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 5x4095",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x100",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x100",
            make: || Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x100",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x100",
            make: || Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x100",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x100",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x100",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x100",
            make: || Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x1000",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x1000",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x1000",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x1000",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x1000",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x1000",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x1000",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x1000",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, RegularPath> 7x4095",
            make: || {
                Variant::V00(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i32>, FastPath> 7x4095",
            make: || {
                Variant::V01(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, RegularPath> 7x4095",
            make: || {
                Variant::V02(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i64>, FastPath> 7x4095",
            make: || {
                Variant::V03(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, RegularPath> 7x4095",
            make: || {
                Variant::V04(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<i128>, FastPath> 7x4095",
            make: || {
                Variant::V05(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, RegularPath> 7x4095",
            make: || {
                Variant::V06(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "CountMin<Vector2D<f64>, FastPath> 7x4095",
            make: || {
                Variant::V07(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                    7, 4095,
                ))
            },
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
            for data_case in &data {
                let truth = data_case.truth();
                for variant_case in &variants {
                    let mut sketch = (variant_case.make)();
                    data_case.insert_all(&mut sketch);
                    let (rows, cols) = sketch.dimensions();
                    CountMinSpec::new(rows, cols).assert_contract(
                        variant_case.label,
                        &truth,
                        |key| data_case.estimate(&sketch, key),
                        data_case.label(),
                    );
                }
            }
        })
        .expect("spawn variant matrix test")
        .join()
        .unwrap();
}
