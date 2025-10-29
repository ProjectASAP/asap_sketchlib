use crate::common::{SketchInput, Vector2D, hash_it};
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Coco bucket implemented on top of the shared `Vector2D` abstraction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bucket<'sketch> {
    #[serde(borrow)]
    pub full_key: Option<SketchInput<'sketch>>,
    pub value: u64,
}

impl<'sketch> Bucket<'sketch> {
    pub fn new() -> Self {
        Self {
            full_key: None,
            value: 0,
        }
    }

    pub fn add(&mut self, delta: u64) {
        self.value += delta;
    }

    pub fn set_key(&mut self, key: &SketchInput<'sketch>) {
        self.full_key = Some(key.clone());
    }

    pub fn is_partial_key(&self, partial_key: &SketchInput) -> bool {
        match (&self.full_key, partial_key) {
            (Some(SketchInput::String(full)), SketchInput::String(partial)) => {
                full.contains(partial)
            }
            (Some(SketchInput::Str(full)), SketchInput::Str(partial)) => full.contains(partial),
            (Some(SketchInput::String(full)), SketchInput::Str(partial)) => full.contains(partial),
            (Some(SketchInput::Str(full)), SketchInput::String(partial)) => full.contains(partial),
            _ => false,
        }
    }

    pub fn is_partial_key_with_udf<'query, F>(
        &self,
        partial_key: &SketchInput<'query>,
        udf: F,
    ) -> bool
    where
        F: Fn(&SketchInput<'sketch>, &SketchInput<'query>) -> bool,
    {
        match &self.full_key {
            Some(full) => udf(full, partial_key),
            None => false,
        }
    }
}

impl<'sketch> Default for Bucket<'sketch> {
    fn default() -> Self {
        Self::new()
    }
}

/// Coco sketch backed by the shared `Vector2D` abstraction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Coco<'sketch> {
    pub width: usize,
    pub depth: usize,
    #[serde(borrow)]
    pub table: Vector2D<Bucket<'sketch>>,
}

impl<'sketch> Default for Coco<'sketch> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'sketch> Coco<'sketch> {
    pub fn new() -> Self {
        Self::init_with_size(64, 5)
    }

    pub fn init_with_size(width: usize, depth: usize) -> Self {
        let table = Vector2D::from_fn(depth, width, |_, _| Bucket::default());
        Self {
            width,
            depth,
            table,
        }
    }

    pub fn debug(&self) {
        println!("width: {}", self.width);
        println!("depth: {}", self.depth);
        for row in 0..self.depth {
            print!("[ ");
            for bucket in self.table.row_slice(row) {
                match &bucket.full_key {
                    Some(key) => print!("{:?}:{}, ", key, bucket.value),
                    None => print!("None:{}, ", bucket.value),
                }
            }
            println!("]");
        }
    }

    pub fn insert(&mut self, key: &SketchInput<'sketch>, value: u64) {
        let mut target: Option<(usize, usize)> = None;
        let mut min_value = u64::MAX;

        for row in 0..self.depth {
            let idx = hash_it(row, key) as usize % self.width;
            let bucket = self.table.get_mut(row, idx).expect("index validated above");
            match (bucket.full_key.as_ref(), key) {
                (Some(SketchInput::String(existing)), SketchInput::String(current)) => {
                    if existing == current {
                        bucket.add(value);
                        return;
                    }
                    if bucket.value < min_value {
                        min_value = bucket.value;
                        target = Some((row, idx));
                    }
                }
                (Some(SketchInput::Str(existing)), SketchInput::Str(current)) => {
                    if existing == current {
                        bucket.add(value);
                        return;
                    }
                    if bucket.value < min_value {
                        min_value = bucket.value;
                        target = Some((row, idx));
                    }
                }
                (Some(_), _) => {
                    if bucket.value < min_value {
                        min_value = bucket.value;
                        target = Some((row, idx));
                    }
                }
                (None, _) => {
                    bucket.add(value);
                    bucket.set_key(key);
                    return;
                }
            }
        }

        let (row, col) = target.unwrap_or_else(|| (0, hash_it(0, key) as usize % self.width));
        let bucket = self.table.get_mut(row, col).expect("in bounds");
        bucket.add(value);

        match bucket.full_key.as_ref() {
            Some(_) => {
                let mut rng = rand::rng();
                let random_value: f64 = rng.random_range(0.0..=1.0);
                if (value as f64 / bucket.value as f64) < random_value {
                    bucket.set_key(key);
                }
            }
            None => bucket.set_key(key),
        }
    }

    pub fn estimate(&self, partial_key: &SketchInput<'_>) -> u64 {
        let mut total = 0;
        for row in 0..self.depth {
            for bucket in self.table.row_slice(row) {
                if bucket.is_partial_key(partial_key) {
                    total += bucket.value;
                }
            }
        }
        total
    }

    pub fn estimate_with_udf<'query, F>(&self, partial_key: &SketchInput<'query>, udf: F) -> u64
    where
        F: Fn(&SketchInput<'sketch>, &SketchInput<'query>) -> bool,
    {
        let mut total = 0;
        for row in 0..self.depth {
            for bucket in self.table.row_slice(row) {
                if bucket.is_partial_key_with_udf(partial_key, &udf) {
                    total += bucket.value;
                }
            }
        }
        total
    }

    pub fn merge(&mut self, other: &Coco<'sketch>) {
        assert_eq!(self.depth, other.depth, "Different depth, cannot merge");
        assert_eq!(self.width, other.width, "Different width, cannot merge");

        for row in 0..other.depth {
            for bucket in other.table.row_slice(row) {
                if let Some(key) = &bucket.full_key {
                    self.insert(key, bucket.value);
                }
            }
        }
    }
}
