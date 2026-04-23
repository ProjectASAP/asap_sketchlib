use serde::{Deserialize, Serialize};

/// Standard row-by-row hashing mode for matrix-backed sketches.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct RegularPath;

/// Packed fast-path hashing mode for matrix-backed sketches.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct FastPath;
