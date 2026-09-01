use crate::common::storage::*;
use asap_sketchlib::{Count, DataInput, FastPath, RegularPath, Vector2D};

struct VariantCase {
    label: &'static str,
    make: fn() -> Variant,
}

enum Variant {
    V00(Count<Vector2D<i32>, RegularPath>),
    V01(Count<Vector2D<i32>, FastPath>),
    V02(Count<Vector2D<i64>, RegularPath>),
    V03(Count<Vector2D<i64>, FastPath>),
    V04(Count<Vector2D<i128>, RegularPath>),
    V05(Count<Vector2D<i128>, FastPath>),
    V06(Count<Matrix3X512I32, RegularPath>),
    V07(Count<Matrix3X512I32, FastPath>),
    V08(Count<Matrix3X512I64, RegularPath>),
    V09(Count<Matrix3X512I64, FastPath>),
    V10(Count<Matrix3X512I128, RegularPath>),
    V11(Count<Matrix3X512I128, FastPath>),
    V12(Count<Matrix3X1024I32, RegularPath>),
    V13(Count<Matrix3X1024I32, FastPath>),
    V14(Count<Matrix3X1024I64, RegularPath>),
    V15(Count<Matrix3X1024I64, FastPath>),
    V16(Count<Matrix3X1024I128, RegularPath>),
    V17(Count<Matrix3X1024I128, FastPath>),
    V18(Count<Matrix3X2048I32, RegularPath>),
    V19(Count<Matrix3X2048I32, FastPath>),
    V20(Count<Matrix3X2048I64, RegularPath>),
    V21(Count<Matrix3X2048I64, FastPath>),
    V22(Count<Matrix3X2048I128, RegularPath>),
    V23(Count<Matrix3X2048I128, FastPath>),
    V24(Count<Matrix3X4096I32, RegularPath>),
    V25(Count<Matrix3X4096I32, FastPath>),
    V26(Count<Matrix3X4096I64, RegularPath>),
    V27(Count<Matrix3X4096I64, FastPath>),
    V28(Count<Matrix3X4096I128, RegularPath>),
    V29(Count<Matrix3X4096I128, FastPath>),
    V30(Count<Matrix3X8192I32, RegularPath>),
    V31(Count<Matrix3X8192I32, FastPath>),
    V32(Count<Matrix3X8192I64, RegularPath>),
    V33(Count<Matrix3X8192I64, FastPath>),
    V34(Count<Matrix3X8192I128, RegularPath>),
    V35(Count<Matrix3X8192I128, FastPath>),
    V36(Count<Matrix3X16384I32, RegularPath>),
    V37(Count<Matrix3X16384I32, FastPath>),
    V38(Count<Matrix3X16384I64, RegularPath>),
    V39(Count<Matrix3X16384I64, FastPath>),
    V40(Count<Matrix3X16384I128, RegularPath>),
    V41(Count<Matrix3X16384I128, FastPath>),
    V42(Count<Matrix3X32768I32, RegularPath>),
    V43(Count<Matrix3X32768I32, FastPath>),
    V44(Count<Matrix3X32768I64, RegularPath>),
    V45(Count<Matrix3X32768I64, FastPath>),
    V46(Count<Matrix3X32768I128, RegularPath>),
    V47(Count<Matrix3X32768I128, FastPath>),
    V48(Count<Matrix5X512I32, RegularPath>),
    V49(Count<Matrix5X512I32, FastPath>),
    V50(Count<Matrix5X512I64, RegularPath>),
    V51(Count<Matrix5X512I64, FastPath>),
    V52(Count<Matrix5X512I128, RegularPath>),
    V53(Count<Matrix5X512I128, FastPath>),
    V54(Count<Matrix5X1024I32, RegularPath>),
    V55(Count<Matrix5X1024I32, FastPath>),
    V56(Count<Matrix5X1024I64, RegularPath>),
    V57(Count<Matrix5X1024I64, FastPath>),
    V58(Count<Matrix5X1024I128, RegularPath>),
    V59(Count<Matrix5X1024I128, FastPath>),
    V60(Count<Matrix5X2048I32, RegularPath>),
    V61(Count<Matrix5X2048I32, FastPath>),
    V62(Count<Matrix5X2048I64, RegularPath>),
    V63(Count<Matrix5X2048I64, FastPath>),
    V64(Count<Matrix5X2048I128, RegularPath>),
    V65(Count<Matrix5X2048I128, FastPath>),
    V66(Count<Matrix5X4096I32, RegularPath>),
    V67(Count<Matrix5X4096I32, FastPath>),
    V68(Count<Matrix5X4096I64, RegularPath>),
    V69(Count<Matrix5X4096I64, FastPath>),
    V70(Count<Matrix5X4096I128, RegularPath>),
    V71(Count<Matrix5X4096I128, FastPath>),
    V72(Count<Matrix5X8192I32, RegularPath>),
    V73(Count<Matrix5X8192I32, FastPath>),
    V74(Count<Matrix5X8192I64, RegularPath>),
    V75(Count<Matrix5X8192I64, FastPath>),
    V76(Count<Matrix5X8192I128, RegularPath>),
    V77(Count<Matrix5X8192I128, FastPath>),
    V78(Count<Matrix5X16384I32, RegularPath>),
    V79(Count<Matrix5X16384I32, FastPath>),
    V80(Count<Matrix5X16384I64, RegularPath>),
    V81(Count<Matrix5X16384I64, FastPath>),
    V82(Count<Matrix5X16384I128, RegularPath>),
    V83(Count<Matrix5X16384I128, FastPath>),
    V84(Count<Matrix5X32768I32, RegularPath>),
    V85(Count<Matrix5X32768I32, FastPath>),
    V86(Count<Matrix5X32768I64, RegularPath>),
    V87(Count<Matrix5X32768I64, FastPath>),
    V88(Count<Matrix5X32768I128, RegularPath>),
    V89(Count<Matrix5X32768I128, FastPath>),
    V90(Count<Matrix7X512I32, RegularPath>),
    V91(Count<Matrix7X512I32, FastPath>),
    V92(Count<Matrix7X512I64, RegularPath>),
    V93(Count<Matrix7X512I64, FastPath>),
    V94(Count<Matrix7X512I128, RegularPath>),
    V95(Count<Matrix7X512I128, FastPath>),
    V96(Count<Matrix7X1024I32, RegularPath>),
    V97(Count<Matrix7X1024I32, FastPath>),
    V98(Count<Matrix7X1024I64, RegularPath>),
    V99(Count<Matrix7X1024I64, FastPath>),
    V100(Count<Matrix7X1024I128, RegularPath>),
    V101(Count<Matrix7X1024I128, FastPath>),
    V102(Count<Matrix7X2048I32, RegularPath>),
    V103(Count<Matrix7X2048I32, FastPath>),
    V104(Count<Matrix7X2048I64, RegularPath>),
    V105(Count<Matrix7X2048I64, FastPath>),
    V106(Count<Matrix7X2048I128, RegularPath>),
    V107(Count<Matrix7X2048I128, FastPath>),
    V108(Count<Matrix7X4096I32, RegularPath>),
    V109(Count<Matrix7X4096I32, FastPath>),
    V110(Count<Matrix7X4096I64, RegularPath>),
    V111(Count<Matrix7X4096I64, FastPath>),
    V112(Count<Matrix7X4096I128, RegularPath>),
    V113(Count<Matrix7X4096I128, FastPath>),
    V114(Count<Matrix7X8192I32, RegularPath>),
    V115(Count<Matrix7X8192I32, FastPath>),
    V116(Count<Matrix7X8192I64, RegularPath>),
    V117(Count<Matrix7X8192I64, FastPath>),
    V118(Count<Matrix7X8192I128, RegularPath>),
    V119(Count<Matrix7X8192I128, FastPath>),
    V120(Count<Matrix7X16384I32, RegularPath>),
    V121(Count<Matrix7X16384I32, FastPath>),
    V122(Count<Matrix7X16384I64, RegularPath>),
    V123(Count<Matrix7X16384I64, FastPath>),
    V124(Count<Matrix7X16384I128, RegularPath>),
    V125(Count<Matrix7X16384I128, FastPath>),
    V126(Count<Matrix7X32768I32, RegularPath>),
    V127(Count<Matrix7X32768I32, FastPath>),
    V128(Count<Matrix7X32768I64, RegularPath>),
    V129(Count<Matrix7X32768I64, FastPath>),
    V130(Count<Matrix7X32768I128, RegularPath>),
    V131(Count<Matrix7X32768I128, FastPath>),
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
        }
    }

    fn estimate(&self, input: &DataInput) -> f64 {
        match self {
            Variant::V00(sketch) => sketch.estimate(input),
            Variant::V01(sketch) => sketch.estimate(input),
            Variant::V02(sketch) => sketch.estimate(input),
            Variant::V03(sketch) => sketch.estimate(input),
            Variant::V04(sketch) => sketch.estimate(input),
            Variant::V05(sketch) => sketch.estimate(input),
            Variant::V06(sketch) => sketch.estimate(input),
            Variant::V07(sketch) => sketch.estimate(input),
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
            Variant::V20(sketch) => sketch.estimate(input),
            Variant::V21(sketch) => sketch.estimate(input),
            Variant::V22(sketch) => sketch.estimate(input),
            Variant::V23(sketch) => sketch.estimate(input),
            Variant::V24(sketch) => sketch.estimate(input),
            Variant::V25(sketch) => sketch.estimate(input),
            Variant::V26(sketch) => sketch.estimate(input),
            Variant::V27(sketch) => sketch.estimate(input),
            Variant::V28(sketch) => sketch.estimate(input),
            Variant::V29(sketch) => sketch.estimate(input),
            Variant::V30(sketch) => sketch.estimate(input),
            Variant::V31(sketch) => sketch.estimate(input),
            Variant::V32(sketch) => sketch.estimate(input),
            Variant::V33(sketch) => sketch.estimate(input),
            Variant::V34(sketch) => sketch.estimate(input),
            Variant::V35(sketch) => sketch.estimate(input),
            Variant::V36(sketch) => sketch.estimate(input),
            Variant::V37(sketch) => sketch.estimate(input),
            Variant::V38(sketch) => sketch.estimate(input),
            Variant::V39(sketch) => sketch.estimate(input),
            Variant::V40(sketch) => sketch.estimate(input),
            Variant::V41(sketch) => sketch.estimate(input),
            Variant::V42(sketch) => sketch.estimate(input),
            Variant::V43(sketch) => sketch.estimate(input),
            Variant::V44(sketch) => sketch.estimate(input),
            Variant::V45(sketch) => sketch.estimate(input),
            Variant::V46(sketch) => sketch.estimate(input),
            Variant::V47(sketch) => sketch.estimate(input),
            Variant::V48(sketch) => sketch.estimate(input),
            Variant::V49(sketch) => sketch.estimate(input),
            Variant::V50(sketch) => sketch.estimate(input),
            Variant::V51(sketch) => sketch.estimate(input),
            Variant::V52(sketch) => sketch.estimate(input),
            Variant::V53(sketch) => sketch.estimate(input),
            Variant::V54(sketch) => sketch.estimate(input),
            Variant::V55(sketch) => sketch.estimate(input),
            Variant::V56(sketch) => sketch.estimate(input),
            Variant::V57(sketch) => sketch.estimate(input),
            Variant::V58(sketch) => sketch.estimate(input),
            Variant::V59(sketch) => sketch.estimate(input),
            Variant::V60(sketch) => sketch.estimate(input),
            Variant::V61(sketch) => sketch.estimate(input),
            Variant::V62(sketch) => sketch.estimate(input),
            Variant::V63(sketch) => sketch.estimate(input),
            Variant::V64(sketch) => sketch.estimate(input),
            Variant::V65(sketch) => sketch.estimate(input),
            Variant::V66(sketch) => sketch.estimate(input),
            Variant::V67(sketch) => sketch.estimate(input),
            Variant::V68(sketch) => sketch.estimate(input),
            Variant::V69(sketch) => sketch.estimate(input),
            Variant::V70(sketch) => sketch.estimate(input),
            Variant::V71(sketch) => sketch.estimate(input),
            Variant::V72(sketch) => sketch.estimate(input),
            Variant::V73(sketch) => sketch.estimate(input),
            Variant::V74(sketch) => sketch.estimate(input),
            Variant::V75(sketch) => sketch.estimate(input),
            Variant::V76(sketch) => sketch.estimate(input),
            Variant::V77(sketch) => sketch.estimate(input),
            Variant::V78(sketch) => sketch.estimate(input),
            Variant::V79(sketch) => sketch.estimate(input),
            Variant::V80(sketch) => sketch.estimate(input),
            Variant::V81(sketch) => sketch.estimate(input),
            Variant::V82(sketch) => sketch.estimate(input),
            Variant::V83(sketch) => sketch.estimate(input),
            Variant::V84(sketch) => sketch.estimate(input),
            Variant::V85(sketch) => sketch.estimate(input),
            Variant::V86(sketch) => sketch.estimate(input),
            Variant::V87(sketch) => sketch.estimate(input),
            Variant::V88(sketch) => sketch.estimate(input),
            Variant::V89(sketch) => sketch.estimate(input),
            Variant::V90(sketch) => sketch.estimate(input),
            Variant::V91(sketch) => sketch.estimate(input),
            Variant::V92(sketch) => sketch.estimate(input),
            Variant::V93(sketch) => sketch.estimate(input),
            Variant::V94(sketch) => sketch.estimate(input),
            Variant::V95(sketch) => sketch.estimate(input),
            Variant::V96(sketch) => sketch.estimate(input),
            Variant::V97(sketch) => sketch.estimate(input),
            Variant::V98(sketch) => sketch.estimate(input),
            Variant::V99(sketch) => sketch.estimate(input),
            Variant::V100(sketch) => sketch.estimate(input),
            Variant::V101(sketch) => sketch.estimate(input),
            Variant::V102(sketch) => sketch.estimate(input),
            Variant::V103(sketch) => sketch.estimate(input),
            Variant::V104(sketch) => sketch.estimate(input),
            Variant::V105(sketch) => sketch.estimate(input),
            Variant::V106(sketch) => sketch.estimate(input),
            Variant::V107(sketch) => sketch.estimate(input),
            Variant::V108(sketch) => sketch.estimate(input),
            Variant::V109(sketch) => sketch.estimate(input),
            Variant::V110(sketch) => sketch.estimate(input),
            Variant::V111(sketch) => sketch.estimate(input),
            Variant::V112(sketch) => sketch.estimate(input),
            Variant::V113(sketch) => sketch.estimate(input),
            Variant::V114(sketch) => sketch.estimate(input),
            Variant::V115(sketch) => sketch.estimate(input),
            Variant::V116(sketch) => sketch.estimate(input),
            Variant::V117(sketch) => sketch.estimate(input),
            Variant::V118(sketch) => sketch.estimate(input),
            Variant::V119(sketch) => sketch.estimate(input),
            Variant::V120(sketch) => sketch.estimate(input),
            Variant::V121(sketch) => sketch.estimate(input),
            Variant::V122(sketch) => sketch.estimate(input),
            Variant::V123(sketch) => sketch.estimate(input),
            Variant::V124(sketch) => sketch.estimate(input),
            Variant::V125(sketch) => sketch.estimate(input),
            Variant::V126(sketch) => sketch.estimate(input),
            Variant::V127(sketch) => sketch.estimate(input),
            Variant::V128(sketch) => sketch.estimate(input),
            Variant::V129(sketch) => sketch.estimate(input),
            Variant::V130(sketch) => sketch.estimate(input),
            Variant::V131(sketch) => sketch.estimate(input),
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
        }
    }
}

fn variant_cases() -> Vec<VariantCase> {
    vec![
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x512",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x512",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x512",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x512",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x512",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 512,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x512",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x1024",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x1024",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x1024",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x1024",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x1024",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x1024",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x2048",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x2048",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x2048",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x2048",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x2048",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x2048",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x4096",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x4096",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x4096",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x4096",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x4096",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x4096",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x8192",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x8192",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x8192",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x8192",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x8192",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x8192",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x16384",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x16384",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x16384",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x16384",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x16384",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x16384",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x32768",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x32768",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x32768",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x32768",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x32768",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x32768",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x512",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x512",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x512",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x512",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x512",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 512,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x512",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x1024",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x1024",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x1024",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x1024",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x1024",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x1024",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x2048",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x2048",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x2048",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x2048",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x2048",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x2048",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x4096",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x4096",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x4096",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x4096",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x4096",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x4096",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x8192",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x8192",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x8192",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x8192",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x8192",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x8192",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x16384",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x16384",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x16384",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x16384",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x16384",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x16384",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x32768",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x32768",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x32768",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x32768",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x32768",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x32768",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x512",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x512",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x512",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x512",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x512",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 512,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x512",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 512)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x1024",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x1024",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x1024",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x1024",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x1024",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 1024,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x1024",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 1024)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x2048",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x2048",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x2048",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x2048",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x2048",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 2048,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x2048",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 2048)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x4096",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x4096",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x4096",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x4096",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x4096",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 4096,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x4096",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 4096)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x8192",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x8192",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x8192",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x8192",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x8192",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 8192,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x8192",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 8192)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x16384",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x16384",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x16384",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x16384",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x16384",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 16384,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x16384",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 16384)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x32768",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x32768",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x32768",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x32768",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 32768)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x32768",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 32768,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x32768",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 32768)),
        },
        VariantCase {
            label: "Count<Matrix3X512I32, RegularPath>",
            make: || {
                Variant::V06(Count::<Matrix3X512I32, RegularPath>::from_storage(
                    Matrix3X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X512I32, FastPath>",
            make: || {
                Variant::V07(Count::<Matrix3X512I32, FastPath>::from_storage(
                    Matrix3X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X512I64, RegularPath>",
            make: || {
                Variant::V08(Count::<Matrix3X512I64, RegularPath>::from_storage(
                    Matrix3X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X512I64, FastPath>",
            make: || {
                Variant::V09(Count::<Matrix3X512I64, FastPath>::from_storage(
                    Matrix3X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X512I128, RegularPath>",
            make: || {
                Variant::V10(Count::<Matrix3X512I128, RegularPath>::from_storage(
                    Matrix3X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X512I128, FastPath>",
            make: || {
                Variant::V11(Count::<Matrix3X512I128, FastPath>::from_storage(
                    Matrix3X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I32, RegularPath>",
            make: || {
                Variant::V12(Count::<Matrix3X1024I32, RegularPath>::from_storage(
                    Matrix3X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I32, FastPath>",
            make: || {
                Variant::V13(Count::<Matrix3X1024I32, FastPath>::from_storage(
                    Matrix3X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I64, RegularPath>",
            make: || {
                Variant::V14(Count::<Matrix3X1024I64, RegularPath>::from_storage(
                    Matrix3X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I64, FastPath>",
            make: || {
                Variant::V15(Count::<Matrix3X1024I64, FastPath>::from_storage(
                    Matrix3X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I128, RegularPath>",
            make: || {
                Variant::V16(Count::<Matrix3X1024I128, RegularPath>::from_storage(
                    Matrix3X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X1024I128, FastPath>",
            make: || {
                Variant::V17(Count::<Matrix3X1024I128, FastPath>::from_storage(
                    Matrix3X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I32, RegularPath>",
            make: || {
                Variant::V18(Count::<Matrix3X2048I32, RegularPath>::from_storage(
                    Matrix3X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I32, FastPath>",
            make: || {
                Variant::V19(Count::<Matrix3X2048I32, FastPath>::from_storage(
                    Matrix3X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I64, RegularPath>",
            make: || {
                Variant::V20(Count::<Matrix3X2048I64, RegularPath>::from_storage(
                    Matrix3X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I64, FastPath>",
            make: || {
                Variant::V21(Count::<Matrix3X2048I64, FastPath>::from_storage(
                    Matrix3X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I128, RegularPath>",
            make: || {
                Variant::V22(Count::<Matrix3X2048I128, RegularPath>::from_storage(
                    Matrix3X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X2048I128, FastPath>",
            make: || {
                Variant::V23(Count::<Matrix3X2048I128, FastPath>::from_storage(
                    Matrix3X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I32, RegularPath>",
            make: || {
                Variant::V24(Count::<Matrix3X4096I32, RegularPath>::from_storage(
                    Matrix3X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I32, FastPath>",
            make: || {
                Variant::V25(Count::<Matrix3X4096I32, FastPath>::from_storage(
                    Matrix3X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I64, RegularPath>",
            make: || {
                Variant::V26(Count::<Matrix3X4096I64, RegularPath>::from_storage(
                    Matrix3X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I64, FastPath>",
            make: || {
                Variant::V27(Count::<Matrix3X4096I64, FastPath>::from_storage(
                    Matrix3X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I128, RegularPath>",
            make: || {
                Variant::V28(Count::<Matrix3X4096I128, RegularPath>::from_storage(
                    Matrix3X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X4096I128, FastPath>",
            make: || {
                Variant::V29(Count::<Matrix3X4096I128, FastPath>::from_storage(
                    Matrix3X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I32, RegularPath>",
            make: || {
                Variant::V30(Count::<Matrix3X8192I32, RegularPath>::from_storage(
                    Matrix3X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I32, FastPath>",
            make: || {
                Variant::V31(Count::<Matrix3X8192I32, FastPath>::from_storage(
                    Matrix3X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I64, RegularPath>",
            make: || {
                Variant::V32(Count::<Matrix3X8192I64, RegularPath>::from_storage(
                    Matrix3X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I64, FastPath>",
            make: || {
                Variant::V33(Count::<Matrix3X8192I64, FastPath>::from_storage(
                    Matrix3X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I128, RegularPath>",
            make: || {
                Variant::V34(Count::<Matrix3X8192I128, RegularPath>::from_storage(
                    Matrix3X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X8192I128, FastPath>",
            make: || {
                Variant::V35(Count::<Matrix3X8192I128, FastPath>::from_storage(
                    Matrix3X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I32, RegularPath>",
            make: || {
                Variant::V36(Count::<Matrix3X16384I32, RegularPath>::from_storage(
                    Matrix3X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I32, FastPath>",
            make: || {
                Variant::V37(Count::<Matrix3X16384I32, FastPath>::from_storage(
                    Matrix3X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I64, RegularPath>",
            make: || {
                Variant::V38(Count::<Matrix3X16384I64, RegularPath>::from_storage(
                    Matrix3X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I64, FastPath>",
            make: || {
                Variant::V39(Count::<Matrix3X16384I64, FastPath>::from_storage(
                    Matrix3X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I128, RegularPath>",
            make: || {
                Variant::V40(Count::<Matrix3X16384I128, RegularPath>::from_storage(
                    Matrix3X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X16384I128, FastPath>",
            make: || {
                Variant::V41(Count::<Matrix3X16384I128, FastPath>::from_storage(
                    Matrix3X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I32, RegularPath>",
            make: || {
                Variant::V42(Count::<Matrix3X32768I32, RegularPath>::from_storage(
                    Matrix3X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I32, FastPath>",
            make: || {
                Variant::V43(Count::<Matrix3X32768I32, FastPath>::from_storage(
                    Matrix3X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I64, RegularPath>",
            make: || {
                Variant::V44(Count::<Matrix3X32768I64, RegularPath>::from_storage(
                    Matrix3X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I64, FastPath>",
            make: || {
                Variant::V45(Count::<Matrix3X32768I64, FastPath>::from_storage(
                    Matrix3X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I128, RegularPath>",
            make: || {
                Variant::V46(Count::<Matrix3X32768I128, RegularPath>::from_storage(
                    Matrix3X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix3X32768I128, FastPath>",
            make: || {
                Variant::V47(Count::<Matrix3X32768I128, FastPath>::from_storage(
                    Matrix3X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I32, RegularPath>",
            make: || {
                Variant::V48(Count::<Matrix5X512I32, RegularPath>::from_storage(
                    Matrix5X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I32, FastPath>",
            make: || {
                Variant::V49(Count::<Matrix5X512I32, FastPath>::from_storage(
                    Matrix5X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I64, RegularPath>",
            make: || {
                Variant::V50(Count::<Matrix5X512I64, RegularPath>::from_storage(
                    Matrix5X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I64, FastPath>",
            make: || {
                Variant::V51(Count::<Matrix5X512I64, FastPath>::from_storage(
                    Matrix5X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I128, RegularPath>",
            make: || {
                Variant::V52(Count::<Matrix5X512I128, RegularPath>::from_storage(
                    Matrix5X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X512I128, FastPath>",
            make: || {
                Variant::V53(Count::<Matrix5X512I128, FastPath>::from_storage(
                    Matrix5X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I32, RegularPath>",
            make: || {
                Variant::V54(Count::<Matrix5X1024I32, RegularPath>::from_storage(
                    Matrix5X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I32, FastPath>",
            make: || {
                Variant::V55(Count::<Matrix5X1024I32, FastPath>::from_storage(
                    Matrix5X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I64, RegularPath>",
            make: || {
                Variant::V56(Count::<Matrix5X1024I64, RegularPath>::from_storage(
                    Matrix5X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I64, FastPath>",
            make: || {
                Variant::V57(Count::<Matrix5X1024I64, FastPath>::from_storage(
                    Matrix5X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I128, RegularPath>",
            make: || {
                Variant::V58(Count::<Matrix5X1024I128, RegularPath>::from_storage(
                    Matrix5X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X1024I128, FastPath>",
            make: || {
                Variant::V59(Count::<Matrix5X1024I128, FastPath>::from_storage(
                    Matrix5X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I32, RegularPath>",
            make: || {
                Variant::V60(Count::<Matrix5X2048I32, RegularPath>::from_storage(
                    Matrix5X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I32, FastPath>",
            make: || {
                Variant::V61(Count::<Matrix5X2048I32, FastPath>::from_storage(
                    Matrix5X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I64, RegularPath>",
            make: || {
                Variant::V62(Count::<Matrix5X2048I64, RegularPath>::from_storage(
                    Matrix5X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I64, FastPath>",
            make: || {
                Variant::V63(Count::<Matrix5X2048I64, FastPath>::from_storage(
                    Matrix5X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I128, RegularPath>",
            make: || {
                Variant::V64(Count::<Matrix5X2048I128, RegularPath>::from_storage(
                    Matrix5X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X2048I128, FastPath>",
            make: || {
                Variant::V65(Count::<Matrix5X2048I128, FastPath>::from_storage(
                    Matrix5X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I32, RegularPath>",
            make: || {
                Variant::V66(Count::<Matrix5X4096I32, RegularPath>::from_storage(
                    Matrix5X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I32, FastPath>",
            make: || {
                Variant::V67(Count::<Matrix5X4096I32, FastPath>::from_storage(
                    Matrix5X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I64, RegularPath>",
            make: || {
                Variant::V68(Count::<Matrix5X4096I64, RegularPath>::from_storage(
                    Matrix5X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I64, FastPath>",
            make: || {
                Variant::V69(Count::<Matrix5X4096I64, FastPath>::from_storage(
                    Matrix5X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I128, RegularPath>",
            make: || {
                Variant::V70(Count::<Matrix5X4096I128, RegularPath>::from_storage(
                    Matrix5X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X4096I128, FastPath>",
            make: || {
                Variant::V71(Count::<Matrix5X4096I128, FastPath>::from_storage(
                    Matrix5X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I32, RegularPath>",
            make: || {
                Variant::V72(Count::<Matrix5X8192I32, RegularPath>::from_storage(
                    Matrix5X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I32, FastPath>",
            make: || {
                Variant::V73(Count::<Matrix5X8192I32, FastPath>::from_storage(
                    Matrix5X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I64, RegularPath>",
            make: || {
                Variant::V74(Count::<Matrix5X8192I64, RegularPath>::from_storage(
                    Matrix5X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I64, FastPath>",
            make: || {
                Variant::V75(Count::<Matrix5X8192I64, FastPath>::from_storage(
                    Matrix5X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I128, RegularPath>",
            make: || {
                Variant::V76(Count::<Matrix5X8192I128, RegularPath>::from_storage(
                    Matrix5X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X8192I128, FastPath>",
            make: || {
                Variant::V77(Count::<Matrix5X8192I128, FastPath>::from_storage(
                    Matrix5X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I32, RegularPath>",
            make: || {
                Variant::V78(Count::<Matrix5X16384I32, RegularPath>::from_storage(
                    Matrix5X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I32, FastPath>",
            make: || {
                Variant::V79(Count::<Matrix5X16384I32, FastPath>::from_storage(
                    Matrix5X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I64, RegularPath>",
            make: || {
                Variant::V80(Count::<Matrix5X16384I64, RegularPath>::from_storage(
                    Matrix5X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I64, FastPath>",
            make: || {
                Variant::V81(Count::<Matrix5X16384I64, FastPath>::from_storage(
                    Matrix5X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I128, RegularPath>",
            make: || {
                Variant::V82(Count::<Matrix5X16384I128, RegularPath>::from_storage(
                    Matrix5X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X16384I128, FastPath>",
            make: || {
                Variant::V83(Count::<Matrix5X16384I128, FastPath>::from_storage(
                    Matrix5X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I32, RegularPath>",
            make: || {
                Variant::V84(Count::<Matrix5X32768I32, RegularPath>::from_storage(
                    Matrix5X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I32, FastPath>",
            make: || {
                Variant::V85(Count::<Matrix5X32768I32, FastPath>::from_storage(
                    Matrix5X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I64, RegularPath>",
            make: || {
                Variant::V86(Count::<Matrix5X32768I64, RegularPath>::from_storage(
                    Matrix5X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I64, FastPath>",
            make: || {
                Variant::V87(Count::<Matrix5X32768I64, FastPath>::from_storage(
                    Matrix5X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I128, RegularPath>",
            make: || {
                Variant::V88(Count::<Matrix5X32768I128, RegularPath>::from_storage(
                    Matrix5X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix5X32768I128, FastPath>",
            make: || {
                Variant::V89(Count::<Matrix5X32768I128, FastPath>::from_storage(
                    Matrix5X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I32, RegularPath>",
            make: || {
                Variant::V90(Count::<Matrix7X512I32, RegularPath>::from_storage(
                    Matrix7X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I32, FastPath>",
            make: || {
                Variant::V91(Count::<Matrix7X512I32, FastPath>::from_storage(
                    Matrix7X512I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I64, RegularPath>",
            make: || {
                Variant::V92(Count::<Matrix7X512I64, RegularPath>::from_storage(
                    Matrix7X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I64, FastPath>",
            make: || {
                Variant::V93(Count::<Matrix7X512I64, FastPath>::from_storage(
                    Matrix7X512I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I128, RegularPath>",
            make: || {
                Variant::V94(Count::<Matrix7X512I128, RegularPath>::from_storage(
                    Matrix7X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X512I128, FastPath>",
            make: || {
                Variant::V95(Count::<Matrix7X512I128, FastPath>::from_storage(
                    Matrix7X512I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I32, RegularPath>",
            make: || {
                Variant::V96(Count::<Matrix7X1024I32, RegularPath>::from_storage(
                    Matrix7X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I32, FastPath>",
            make: || {
                Variant::V97(Count::<Matrix7X1024I32, FastPath>::from_storage(
                    Matrix7X1024I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I64, RegularPath>",
            make: || {
                Variant::V98(Count::<Matrix7X1024I64, RegularPath>::from_storage(
                    Matrix7X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I64, FastPath>",
            make: || {
                Variant::V99(Count::<Matrix7X1024I64, FastPath>::from_storage(
                    Matrix7X1024I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I128, RegularPath>",
            make: || {
                Variant::V100(Count::<Matrix7X1024I128, RegularPath>::from_storage(
                    Matrix7X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X1024I128, FastPath>",
            make: || {
                Variant::V101(Count::<Matrix7X1024I128, FastPath>::from_storage(
                    Matrix7X1024I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I32, RegularPath>",
            make: || {
                Variant::V102(Count::<Matrix7X2048I32, RegularPath>::from_storage(
                    Matrix7X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I32, FastPath>",
            make: || {
                Variant::V103(Count::<Matrix7X2048I32, FastPath>::from_storage(
                    Matrix7X2048I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I64, RegularPath>",
            make: || {
                Variant::V104(Count::<Matrix7X2048I64, RegularPath>::from_storage(
                    Matrix7X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I64, FastPath>",
            make: || {
                Variant::V105(Count::<Matrix7X2048I64, FastPath>::from_storage(
                    Matrix7X2048I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I128, RegularPath>",
            make: || {
                Variant::V106(Count::<Matrix7X2048I128, RegularPath>::from_storage(
                    Matrix7X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X2048I128, FastPath>",
            make: || {
                Variant::V107(Count::<Matrix7X2048I128, FastPath>::from_storage(
                    Matrix7X2048I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I32, RegularPath>",
            make: || {
                Variant::V108(Count::<Matrix7X4096I32, RegularPath>::from_storage(
                    Matrix7X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I32, FastPath>",
            make: || {
                Variant::V109(Count::<Matrix7X4096I32, FastPath>::from_storage(
                    Matrix7X4096I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I64, RegularPath>",
            make: || {
                Variant::V110(Count::<Matrix7X4096I64, RegularPath>::from_storage(
                    Matrix7X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I64, FastPath>",
            make: || {
                Variant::V111(Count::<Matrix7X4096I64, FastPath>::from_storage(
                    Matrix7X4096I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I128, RegularPath>",
            make: || {
                Variant::V112(Count::<Matrix7X4096I128, RegularPath>::from_storage(
                    Matrix7X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X4096I128, FastPath>",
            make: || {
                Variant::V113(Count::<Matrix7X4096I128, FastPath>::from_storage(
                    Matrix7X4096I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I32, RegularPath>",
            make: || {
                Variant::V114(Count::<Matrix7X8192I32, RegularPath>::from_storage(
                    Matrix7X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I32, FastPath>",
            make: || {
                Variant::V115(Count::<Matrix7X8192I32, FastPath>::from_storage(
                    Matrix7X8192I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I64, RegularPath>",
            make: || {
                Variant::V116(Count::<Matrix7X8192I64, RegularPath>::from_storage(
                    Matrix7X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I64, FastPath>",
            make: || {
                Variant::V117(Count::<Matrix7X8192I64, FastPath>::from_storage(
                    Matrix7X8192I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I128, RegularPath>",
            make: || {
                Variant::V118(Count::<Matrix7X8192I128, RegularPath>::from_storage(
                    Matrix7X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X8192I128, FastPath>",
            make: || {
                Variant::V119(Count::<Matrix7X8192I128, FastPath>::from_storage(
                    Matrix7X8192I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I32, RegularPath>",
            make: || {
                Variant::V120(Count::<Matrix7X16384I32, RegularPath>::from_storage(
                    Matrix7X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I32, FastPath>",
            make: || {
                Variant::V121(Count::<Matrix7X16384I32, FastPath>::from_storage(
                    Matrix7X16384I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I64, RegularPath>",
            make: || {
                Variant::V122(Count::<Matrix7X16384I64, RegularPath>::from_storage(
                    Matrix7X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I64, FastPath>",
            make: || {
                Variant::V123(Count::<Matrix7X16384I64, FastPath>::from_storage(
                    Matrix7X16384I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I128, RegularPath>",
            make: || {
                Variant::V124(Count::<Matrix7X16384I128, RegularPath>::from_storage(
                    Matrix7X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X16384I128, FastPath>",
            make: || {
                Variant::V125(Count::<Matrix7X16384I128, FastPath>::from_storage(
                    Matrix7X16384I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I32, RegularPath>",
            make: || {
                Variant::V126(Count::<Matrix7X32768I32, RegularPath>::from_storage(
                    Matrix7X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I32, FastPath>",
            make: || {
                Variant::V127(Count::<Matrix7X32768I32, FastPath>::from_storage(
                    Matrix7X32768I32::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I64, RegularPath>",
            make: || {
                Variant::V128(Count::<Matrix7X32768I64, RegularPath>::from_storage(
                    Matrix7X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I64, FastPath>",
            make: || {
                Variant::V129(Count::<Matrix7X32768I64, FastPath>::from_storage(
                    Matrix7X32768I64::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I128, RegularPath>",
            make: || {
                Variant::V130(Count::<Matrix7X32768I128, RegularPath>::from_storage(
                    Matrix7X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Matrix7X32768I128, FastPath>",
            make: || {
                Variant::V131(Count::<Matrix7X32768I128, FastPath>::from_storage(
                    Matrix7X32768I128::default(),
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x100",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x100",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x100",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x100",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x100",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 100,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x100",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x1000",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x1000",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x1000",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x1000",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x1000",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x1000",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 3x4095",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 3x4095",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 3x4095",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 3x4095",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 3x4095",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    3, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 3x4095",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x100",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x100",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x100",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x100",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x100",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 100,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x100",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x1000",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x1000",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x1000",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x1000",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x1000",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x1000",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 5x4095",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 5x4095",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 5x4095",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 5x4095",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 5x4095",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    5, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 5x4095",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x100",
            make: || Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x100",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x100",
            make: || Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x100",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x100",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 100,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x100",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 100)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x1000",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x1000",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x1000",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x1000",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x1000",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 1000,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x1000",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 1000)),
        },
        VariantCase {
            label: "Count<Vector2D<i32>, RegularPath> 7x4095",
            make: || {
                Variant::V00(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i32>, FastPath> 7x4095",
            make: || Variant::V01(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i64>, RegularPath> 7x4095",
            make: || {
                Variant::V02(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i64>, FastPath> 7x4095",
            make: || Variant::V03(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 4095)),
        },
        VariantCase {
            label: "Count<Vector2D<i128>, RegularPath> 7x4095",
            make: || {
                Variant::V04(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                    7, 4095,
                ))
            },
        },
        VariantCase {
            label: "Count<Vector2D<i128>, FastPath> 7x4095",
            make: || Variant::V05(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 4095)),
        },
    ]
}
