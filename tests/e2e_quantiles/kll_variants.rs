//! `KLL` and `KLLDynamic` over the shared input table.
//!
//! The rank batteries in the parent file run KLL against the *quantile*
//! shapes — `normal`, `monotonic`, `outside-in`, `duplicate-heavy` — which are
//! chosen for what they do to a compactor: a monotone arrival order and an
//! outside-in order are its worst inputs, and neither is a bounded key draw
//! that the shared table can express. Those stay where they are.
//!
//! What the table adds here is the other two axes. Every case below is one
//! cell of `InputGrid`, so KLL is scored on the same distributions, stream
//! lengths and key domains as the cardinality and frequency matrices, and on
//! three of its four encodings.
//!
//! `Str` is excluded: `KLL<T>` is generic over `NumericalValue` and has no
//! string encoding at all, so there is nothing for that column to test.
//!
//! The three numeric columns are *not* the same test three times. `KLL<i64>`
//! and `KLL<u64>` order their items with `Ord::cmp` while `KLL<f64>` uses
//! `f64::total_cmp`; those are different comparison paths, and KLL's whole
//! correctness rests on ordering. Before this file only the float path was
//! exercised end to end.

use asap_sketchlib::{KLL, KLLDynamic};

fn kll_instances() -> Vec<KLL<f64>> {
    vec![
        KLL::init_kll(20),
        KLL::init_kll(100),
        KLL::init_kll(200),
        KLL::init_kll(400),
        KLL::init_kll(800),
    ]
}

fn kll_dynamic_instances() -> Vec<KLLDynamic<f64>> {
    vec![
        KLLDynamic::init_kll(20),
        KLLDynamic::init_kll(100),
        KLLDynamic::init_kll(200),
        KLLDynamic::init_kll(400),
        KLLDynamic::init_kll(800),
    ]
}
