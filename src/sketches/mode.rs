/// Hashing mode of a matrix-backed sketch in which row `r` hashes the key on
/// its own, at seed-table index `r`. ASAPv1 `mode` `"regular"`.
#[derive(Clone, Copy, Debug, Default)]
pub struct RegularPath;

/// Hashing mode of a matrix-backed sketch in which the key is hashed once, at
/// seed-table index 0, and every row reads its column (and Count Sketch sign)
/// from that one matrix hash. The hash is one packed 64- or 128-bit value when
/// the rows' bits fit, else one hash per row (`MatrixHashLayout`). ASAPv1
/// `mode` `"fast"`.
#[derive(Clone, Copy, Debug, Default)]
pub struct FastPath;
