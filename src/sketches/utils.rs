use ahash::RandomState;

use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SketchInput<'a> {
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(&'a str),
    String(String),
    Bytes(&'a [u8]),
}

// this is from datafusion
// using their seed for easiness and better comparison
pub const SEED: RandomState = RandomState::with_seeds(
    0x885f6cab121d01a3_u64,
    0x71e4379f2976ad8f_u64,
    0xbf30173dd28a8816_u64,
    0x0eaea5d736d733a4_u64,
);

pub const STATE1: RandomState =
    RandomState::with_seeds(0x9e3779b9, 0x85ebca6b, 0xc2b2ae35, 0x27d4eb2f);
pub const STATE2: RandomState =
    RandomState::with_seeds(0x165667b1, 0x9e3779b5, 0x61c88647, 0x85a308d3);
pub const STATE3: RandomState =
    RandomState::with_seeds(0x5bd1e995, 0x3c6ef372, 0xa54ff53a, 0x510e527f);
pub const STATE4: RandomState =
    RandomState::with_seeds(0x9b05688c, 0x1f83d9ab, 0x5be0cd19, 0x137e2179);
pub const STATE5: RandomState =
    RandomState::with_seeds(0x8cc70208, 0x2f024b2b, 0x451a3df5, 0x6a09e667);
pub const STATE6: RandomState =
    RandomState::with_seeds(0x5942cafe, 0xbeef5757, 0xceec3737, 0xadefbe33);

/// the first 5 can be used for rows
/// support 5 rows with fixed hash seed for now
/// the last 6-th can be used as sign hash for count-sketch
pub const STATELIST: [RandomState; 6] = [STATE1, STATE2, STATE3, STATE4, STATE5, STATE6];
pub const LASTSTATE: usize = 5;
/// use d in 0..=4 as hash for rows; use d = 5 as hash for sign
// pub fn hash_it<T: Hash+?Sized>(d: usize, key: &T) -> u64 {
//     STATELIST[d].hash_one(key)
// }

pub const SEEDLIST: [u64; 6] = [
    0xcafe3553,
    0xade3415118,
    0x8cc70208,
    0x2f024b2b,
    0x451a3df5,
    0x6a09e667,
];

/// I32, U32, F32 will all be treated as 64-bit value
pub fn hash_it(d: usize, key: &SketchInput) -> u64 {
    match key {
        SketchInput::I32(i) => XxHash64::oneshot(SEEDLIST[d], &(*i as u64).to_ne_bytes()),
        SketchInput::I64(i) => XxHash64::oneshot(SEEDLIST[d], &(*i as u64).to_ne_bytes()),
        SketchInput::U32(u) => XxHash64::oneshot(SEEDLIST[d], &(*u as u64).to_ne_bytes()),
        SketchInput::U64(u) => XxHash64::oneshot(SEEDLIST[d], &(*u as u64).to_ne_bytes()),
        SketchInput::F32(f) => XxHash64::oneshot(SEEDLIST[d], &(*f as u64).to_ne_bytes()),
        SketchInput::F64(f) => XxHash64::oneshot(SEEDLIST[d], &(*f as u64).to_ne_bytes()),
        SketchInput::Str(s) => XxHash64::oneshot(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::String(s) => XxHash64::oneshot(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::Bytes(items) => XxHash64::oneshot(SEEDLIST[d], *items),
    }
}

/// this should be a temporary function
/// modify KLL to remove this function
pub fn iv_to_f64(i: &SketchInput) -> f64 {
    match i {
        SketchInput::I32(x) => *x as f64,
        SketchInput::I64(x) => *x as f64,
        SketchInput::U32(x) => *x as f64,
        SketchInput::U64(x) => *x as f64,
        SketchInput::F32(x) => *x as f64,
        SketchInput::F64(f) => *f,
        SketchInput::Str(_) => todo!(),
        SketchInput::String(_) => todo!(),
        SketchInput::Bytes(_) => todo!(),
    }
}
