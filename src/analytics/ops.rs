//! Op counting behind the `op-count` feature. One op is one read or write of
//! a cell, counted as `docs/sketch_analytics.md` counts them. Without the
//! feature every counting call compiles to nothing.

#[cfg(feature = "op-count")]
std::thread_local! {
    static OPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Adds `n` ops to this thread's count.
#[inline(always)]
pub fn add(n: usize) {
    #[cfg(feature = "op-count")]
    OPS.with(|ops| ops.set(ops.get() + n as u64));
    #[cfg(not(feature = "op-count"))]
    let _ = n;
}

/// Adds the cost of sorting `p` items or taking their median, `p·⌈log2 p⌉`.
#[inline(always)]
pub fn add_sort(p: usize) {
    if p > 1 {
        add(p * (usize::BITS - (p - 1).leading_zeros()) as usize);
    }
}

/// Runs `f` and returns its result with the ops it performed on this thread.
#[cfg(feature = "op-count")]
pub fn count<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = OPS.with(std::cell::Cell::get);
    let result = f();
    (result, OPS.with(std::cell::Cell::get) - before)
}

#[cfg(all(test, feature = "op-count"))]
mod tests {
    use super::*;

    #[test]
    fn count_sees_only_the_ops_inside_the_closure() {
        add(5);
        let ((), ops) = count(|| {
            add(3);
            add_sort(8);
        });
        assert_eq!(ops, 3 + 8 * 3);
    }

    #[test]
    fn add_sort_uses_the_ceiling_of_log2() {
        let ((), ops) = count(|| add_sort(5));
        assert_eq!(ops, 15);
        let ((), ops) = count(|| add_sort(1));
        assert_eq!(ops, 0);
    }
}
