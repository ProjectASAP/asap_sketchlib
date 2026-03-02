//! Delta entry types for OctoSketch-style multi-threaded sketch updates.
//!
//! Each delta represents an accumulated counter change emitted by a child
//! worker sketch when a local counter overflows the promotion threshold (PROMASK).

/// CountMin promotion threshold: emit delta when u8 counter >= 127.
pub const CM_PROMASK: u8 = 0x7f;

/// Count sketch promotion threshold: emit delta when |i8 counter| >= 63.
pub const COUNT_PROMASK: u8 = 0x3f;

/// HLL promotion threshold: 0 means every register improvement is emitted.
pub const HLL_PROMASK: u8 = 0;

/// Delta emitted by a CountMin child worker.
/// Represents an accumulated unsigned count for a single cell.
#[derive(Clone, Copy, Debug)]
pub struct CmDelta {
    pub row: u16,
    pub col: u16,
    pub value: u8,
}

/// Delta emitted by a Count sketch child worker.
/// Represents a signed accumulated count for a single cell.
#[derive(Clone, Copy, Debug)]
pub struct CountDelta {
    pub row: u16,
    pub col: u16,
    pub value: i8,
}

/// Delta emitted by an HLL child worker.
/// Represents a register improvement (max-register semantics).
#[derive(Clone, Copy, Debug)]
pub struct HllDelta {
    pub pos: u16,
    pub value: u8,
}
