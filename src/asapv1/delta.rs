//! The rules every ASAPv1 delta kind shares (`docs/asapv1_wire_format.md`
//! §3.23): the index and value checks, and the all-or-nothing add of a matrix
//! delta.

use std::fmt;
use std::ops::RangeBounds;

/// A counter a delta adds to. An integer add that overflows has no result; a
/// float one always has.
///
/// Integer adds are exact, so a receiver of `i32` or `i64` deltas equals the
/// producer cell for cell. An `f64` add rounds once to nearest, off by at most
/// `2^-53 * |result|`, so after `n` applies a cell differs from the producer's
/// by at most the sum of `e_i + 2^-53 * |r_i|` (finite values), where `r_i` is
/// the receiver's cell after apply `i` and `e_i` is how far delta `i` is from
/// the producer's true change. A producer forming each delta as `new - old` of
/// its own `f64` cells has `e_i <= 2^-53 * |delta_i|`; one summing updates in a
/// separate buffer has that buffer's summation error. Not guaranteed equal
/// (`docs/asapv1_wire_format.md` §3.23).
pub trait DeltaCounter: Copy + PartialEq + Default {
    /// `self + delta`.
    fn checked_add(self, delta: Self) -> Option<Self>;
}

macro_rules! integer_counter {
    ($($t:ty),*) => {$(
        impl DeltaCounter for $t {
            fn checked_add(self, delta: Self) -> Option<Self> {
                <$t>::checked_add(self, delta)
            }
        }
    )*};
}
integer_counter!(i32, i64);

impl DeltaCounter for f64 {
    fn checked_add(self, delta: Self) -> Option<Self> {
        Some(self + delta)
    }
}

/// Checks one index array and its value array: equal lengths, indices strictly
/// ascending and inside `range`, no value equal to zero.
pub(crate) fn check_cells<I, V>(
    indices: &[I],
    values: &[V],
    range: impl RangeBounds<I>,
) -> Result<(), String>
where
    I: Copy + Ord + fmt::Display,
    V: PartialEq + Default,
{
    if indices.len() != values.len() {
        return Err(format!(
            "{} indices but {} values",
            indices.len(),
            values.len()
        ));
    }
    if let Some(pair) = indices.windows(2).find(|pair| pair[0] >= pair[1]) {
        return Err(format!(
            "index {} follows {}; indices must be strictly ascending",
            pair[1], pair[0]
        ));
    }
    if let Some(index) = indices.iter().find(|index| !range.contains(index)) {
        return Err(format!("index {index} is out of range"));
    }
    if values.iter().any(|value| *value == V::default()) {
        return Err("a value is zero".to_string());
    }
    Ok(())
}

/// Adds `deltas[i]` to `cells[indices[i]]` for every `i`. When an index is past
/// the end of `cells` or an add overflows, no cell changes.
pub(crate) fn add_cells<C: DeltaCounter>(
    cells: &mut [C],
    indices: &[u64],
    deltas: &[C],
) -> Result<(), String> {
    let mut sums = Vec::with_capacity(indices.len());
    for (&index, &delta) in indices.iter().zip(deltas) {
        let cell = usize::try_from(index)
            .ok()
            .and_then(|at| cells.get(at))
            .ok_or_else(|| format!("index {index} is past the {} cells", cells.len()))?;
        let sum = cell
            .checked_add(delta)
            .ok_or_else(|| format!("adding to cell {index} overflows"))?;
        sums.push(sum);
    }
    for (&index, sum) in indices.iter().zip(sums) {
        cells[index as usize] = sum;
    }
    Ok(())
}
