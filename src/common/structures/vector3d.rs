use serde::{Deserialize, Serialize};

/// Shared thin wrapper over `Vec<T>` tailored for sketches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vector3D<T> {
    data: Vec<T>,
    layer: usize,
    row: usize,
    col: usize,
}

impl<T> Vector3D<T> {
    pub fn init(layer: usize, row: usize, col: usize) -> Self {
        Self {
            data: Vec::with_capacity(layer * row * col),
            layer,
            row,
            col,
        }
    }
}
