use twox_hash::{XxHash3_64, XxHash3_128, XxHash32};

use super::SketchInput;

pub const LASTSTATE: usize = 5;
pub const BOTTOM_LAYER_FINDER: usize = 19;

const MASK_32BITS: u64 = (1 << 32) - 1;

pub const SEEDLIST: [u64; 20] = [
    0xcafe3553,
    0xade3415118,
    0x8cc70208,
    0x2f024b2b,
    0x451a3df5,
    0x6a09e667,
    0xbb67ae85,
    0x3c6ef372,
    0xa54ff53a,
    0x510e527f,
    0x9b05688c,
    0x1f83d9ab,
    0x5be0cd19,
    0xcbbb9d5d,
    0x629a292a,
    0x9159015a,
    0x152fecd8,
    0x67332667,
    0x8eb44a87,
    0xdb0c2e0d,
];

/// I32, U32, F32 will all be treated as 64-bit value.
pub fn hash_it(d: usize, key: &SketchInput) -> u64 {
    match key {
        SketchInput::I32(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I64(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::U32(u) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U64(u) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::F32(f) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*f as u64).to_ne_bytes())
        }
        SketchInput::F64(f) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*f as u64).to_ne_bytes())
        }
        SketchInput::Str(s) => XxHash3_64::oneshot_with_seed(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::String(s) => XxHash3_64::oneshot_with_seed(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::Bytes(items) => XxHash3_64::oneshot_with_seed(SEEDLIST[d], *items),
        SketchInput::I8(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I16(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I128(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u128).to_ne_bytes())
        }
        SketchInput::ISIZE(i) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::U8(u) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U16(u) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U128(u) => XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u).to_ne_bytes()),
        SketchInput::USIZE(u) => {
            XxHash3_64::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
    }
}

pub fn hash_it_to_128(d: usize, key: &SketchInput) -> u128 {
    match key {
        SketchInput::I32(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I64(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::U32(u) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U64(u) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::F32(f) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*f as u64).to_ne_bytes())
        }
        SketchInput::F64(f) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*f as u64).to_ne_bytes())
        }
        SketchInput::Str(s) => XxHash3_128::oneshot_with_seed(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::String(s) => XxHash3_128::oneshot_with_seed(SEEDLIST[d], (*s).as_bytes()),
        SketchInput::Bytes(items) => XxHash3_128::oneshot_with_seed(SEEDLIST[d], *items),
        SketchInput::I8(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I16(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::I128(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u128).to_ne_bytes())
        }
        SketchInput::ISIZE(i) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*i as u64).to_ne_bytes())
        }
        SketchInput::U8(u) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U16(u) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
        SketchInput::U128(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u).to_ne_bytes()),
        SketchInput::USIZE(u) => {
            XxHash3_128::oneshot_with_seed(SEEDLIST[d], &(*u as u64).to_ne_bytes())
        }
    }
}

/// idx: index of the hash seed
/// key: wrapper of the input to be hashed
/// requirement: bits requirement, only 32, 64, 128 allowed
/// utilizing different xxHash algorithm, such that
/// only hash by need: if only need 64 bits, will not hash 128 bits
pub fn hash_for_enough_bits(idx: usize, key: &SketchInput, bits_expectation: usize) -> u128 {
    assert!(
        bits_expectation == 32 || bits_expectation == 64 || bits_expectation == 128,
        "bits_expectation should be 32 or 64 or 128"
    );
    if bits_expectation == 32 {
        match key {
            SketchInput::I32(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::I64(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::U32(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
            SketchInput::U64(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
            SketchInput::F32(f) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &f.to_ne_bytes()) as u128
            }
            SketchInput::F64(f) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &f.to_ne_bytes()) as u128
            }
            SketchInput::Str(s) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, (*s).as_bytes()) as u128
            }
            SketchInput::String(s) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, (*s).as_bytes()) as u128
            }
            SketchInput::Bytes(items) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, *items) as u128
            }
            SketchInput::I8(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::I16(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::I128(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::ISIZE(i) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &i.to_ne_bytes()) as u128
            }
            SketchInput::U8(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
            SketchInput::U16(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
            SketchInput::U128(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
            SketchInput::USIZE(u) => {
                XxHash32::oneshot((SEEDLIST[idx] & MASK_32BITS) as u32, &u.to_ne_bytes()) as u128
            }
        }
    } else if bits_expectation == 64 {
        match key {
            SketchInput::I32(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::I64(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::U32(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
            SketchInput::U64(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
            SketchInput::F32(f) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &f.to_ne_bytes()) as u128
            }
            SketchInput::F64(f) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &f.to_ne_bytes()) as u128
            }
            SketchInput::Str(s) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], (*s).as_bytes()) as u128
            }
            SketchInput::String(s) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], (*s).as_bytes()) as u128
            }
            SketchInput::Bytes(items) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], *items) as u128
            }
            SketchInput::I8(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::I16(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::I128(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::ISIZE(i) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()) as u128
            }
            SketchInput::U8(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
            SketchInput::U16(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
            SketchInput::U128(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
            SketchInput::USIZE(u) => {
                XxHash3_64::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()) as u128
            }
        }
    } else {
        match key {
            SketchInput::I32(i) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()),
            SketchInput::I64(i) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()),
            SketchInput::U32(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()),
            SketchInput::U64(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()),
            SketchInput::F32(f) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &f.to_ne_bytes()),
            SketchInput::F64(f) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &f.to_ne_bytes()),
            SketchInput::Str(s) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], (*s).as_bytes()),
            SketchInput::String(s) => {
                XxHash3_128::oneshot_with_seed(SEEDLIST[idx], (*s).as_bytes())
            }
            SketchInput::Bytes(items) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], *items),
            SketchInput::I8(i) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()),
            SketchInput::I16(i) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()),
            SketchInput::I128(i) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes()),
            SketchInput::ISIZE(i) => {
                XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &i.to_ne_bytes())
            }
            SketchInput::U8(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()),
            SketchInput::U16(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()),
            SketchInput::U128(u) => XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes()),
            SketchInput::USIZE(u) => {
                XxHash3_128::oneshot_with_seed(SEEDLIST[idx], &u.to_ne_bytes())
            }
        }
    }
}
