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

pub const LASTSTATE: usize = 5;

pub const SEEDLIST: [u64; 6] = [
    0xcafe3553,
    0xade3415118,
    0x8cc70208,
    0x2f024b2b,
    0x451a3df5,
    0x6a09e667,
];

/// I32, U32, F32 will all be treated as 64-bit value.
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
