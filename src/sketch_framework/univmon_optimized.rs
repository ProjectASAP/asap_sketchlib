//! UnivSketchPool: Object pool for `UnivMon` sketch reuse.
//!
//! Maintains a free-list of pre-allocated `UnivMon` sketches. Callers take
//! ownership via `take()` and return sketches via `put()`, which resets and
//! recycles them. This avoids repeated heap allocation/deallocation for
//! large sketch matrices during promotion, merge, and expiration cycles.

use crate::UnivMon;

/// Object pool for `UnivMon` sketches.
///
/// Maintains a free-list of pre-allocated sketches. Callers take ownership
/// via `take()` and return sketches via `put()`, which resets and recycles
/// them. This avoids repeated heap allocation/deallocation for large sketch
/// matrices during promotion, merge, and expiration cycles.
pub struct UnivSketchPool {
    free_list: Vec<UnivMon>,
    total_allocated: usize,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
}

impl UnivSketchPool {
    /// Creates a new pool with `cap` pre-allocated sketches.
    pub fn new(
        cap: usize,
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
    ) -> Self {
        let free_list: Vec<UnivMon> = (0..cap)
            .map(|_| UnivMon::init_univmon(heap_size, sketch_row, sketch_col, layer_size))
            .collect();
        UnivSketchPool {
            free_list,
            total_allocated: cap,
            heap_size,
            sketch_row,
            sketch_col,
            layer_size,
        }
    }

    /// Takes ownership of a clean sketch from the pool.
    ///
    /// Pops a recycled sketch from the free-list if available, otherwise
    /// allocates a fresh one.
    pub fn take(&mut self) -> UnivMon {
        if let Some(sketch) = self.free_list.pop() {
            sketch
        } else {
            self.total_allocated += 1;
            UnivMon::init_univmon(
                self.heap_size,
                self.sketch_row,
                self.sketch_col,
                self.layer_size,
            )
        }
    }

    /// Returns a sketch to the pool for reuse. Resets all internal state.
    pub fn put(&mut self, mut sketch: UnivMon) {
        sketch.free();
        self.free_list.push(sketch);
    }

    /// Number of sketches currently available in the pool.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Total number of sketches ever allocated by this pool.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    #[test]
    fn pool_basic_take_put() {
        let mut pool = UnivSketchPool::new(2, 16, 2, 5, 2);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.total_allocated(), 2);

        let s0 = pool.take();
        assert_eq!(pool.available(), 1);

        let s1 = pool.take();
        assert_eq!(pool.available(), 0);

        // Pool is empty — next take allocates a new one
        let s2 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3);

        // Return one
        pool.put(s1);
        assert_eq!(pool.available(), 1);

        // Should reuse the returned sketch
        let s3 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3); // no new allocation

        // Return all
        pool.put(s0);
        pool.put(s2);
        pool.put(s3);
        assert_eq!(pool.available(), 3);
    }

    #[test]
    fn pool_free_resets_sketch() {
        let mut pool = UnivSketchPool::new(1, 16, 2, 5, 2);

        // Take a sketch, insert some data
        let mut sketch = pool.take();
        sketch.insert(&SketchInput::I64(42), 100);
        assert!(sketch.bucket_size > 0);

        // Return it — should reset
        pool.put(sketch);

        // Take it back — should be clean
        let sketch2 = pool.take();
        assert_eq!(sketch2.bucket_size, 0);
        assert!((sketch2.l2_sketch_layers[0].get_l2()).abs() < 1e-9);
    }
}
