// Code translated from PromSketch
// this time, the translation is done by me
// could be useful... hopefully
// I use String.clone() many time, could be a performance problem
// needs to be fixed sometime
// oh! the lifetime!

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Item {
    pub key: String,
    pub count: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TopKHeap {
    pub heap: Vec<Item>,
    pub k: u32,         // consider this be usize at some time
    pub total_mem: f64, // don't see why this should be f64
}

impl TopKHeap {
    pub fn init_heap(k: u32) -> Self {
        TopKHeap {
            heap: Vec::with_capacity(k as usize),
            k,
            total_mem: 0.0,
        }
    }

    pub fn get_memory_bytes(&self) -> f64 {
        self.total_mem
    }

    pub fn clean(&mut self) -> () {
        self.heap.clear();
    }

    pub fn init_heap_from_heap(other: &TopKHeap) -> Self {
        let mut h = TopKHeap {
            heap: Vec::with_capacity(other.k as usize),
            k: other.k,
            total_mem: other.total_mem,
        };
        for item in &other.heap {
            let new_item = Item {
                key: item.key.clone(),
                count: item.count,
            };
            h.heap.push(new_item);
        }
        h
    }

    pub fn print_heap(&self) -> () {
        println!("======== Beginning of Heap ========");
        for item in &self.heap {
            item.print_item();
        }
        println!("============ Heap Ends ============");
    }

    pub fn find(&self, k: &str) -> Option<usize> {
        for (idx, item) in self.heap.iter().enumerate() {
            if item.key == k {
                return Some(idx);
            }
        }
        return None;
    }

    pub fn left_child(i: i32) -> i32 {
        2 * i + 1
    }

    pub fn right_child(i: i32) -> i32 {
        2 * i + 2
    }

    pub fn parent(i: i32) -> i32 {
        (i - 1) / 2
    }

    pub fn swap(&mut self, i: i32, j: i32) {
        self.heap.swap(i as usize, j as usize);
    }

    pub fn update_count(&mut self, key: &str, count: i64) -> bool {
        match self.find(key) {
            Some(idx) => {
                self.heap[idx].count += 1;
                self.update_order(idx as i32);
                true
            }
            None => {
                self.insert(key, count);
                true
            }
        }
    }

    pub fn update(&mut self, k: &str, c: i64) -> bool {
        match self.find(k) {
            Some(idx) => {
                self.heap[idx].count = c;
                self.update_order(idx as i32);
                true
            }
            None => {
                self.insert(k, c);
                true
            }
        }
    }

    fn insert(&mut self, k: &str, c: i64) -> () {
        if self.heap.len() < self.k as usize {
            self.heap.push(Item {
                key: k.to_string(),
                count: c,
            });
            self.total_mem += k.len() as f64 + 8.0;
            self.update_order_up(self.heap.len() as i32 - 1);
            // ()
        } else {
            if self.heap[0].count < c {
                self.heap[0].count = c;
                self.heap[0].key = k.to_string();
                self.update_order_down(0);
                // ()
            }
        }
    }

    pub fn update_order(&mut self, i: i32) -> () {
        if !self.update_order_down(i) {
            self.update_order_up(i);
        }
    }

    pub fn update_order_down(&mut self, mut i: i32) -> bool {
        let n = self.heap.len();
        let i0 = i;
        while (i as usize) < n {
            let l = TopKHeap::left_child(i) as usize;
            let r = TopKHeap::right_child(i) as usize;
            let mut smallest = i as usize;

            if l < n && self.heap[smallest].count > self.heap[l].count {
                smallest = l;
            }
            if r < n && self.heap[smallest].count > self.heap[r].count {
                smallest = r;
            }

            if smallest != i as usize {
                self.swap(smallest as i32, i);
            } else {
                break;
            }
            i = smallest as i32;
        }
        i > i0
    }

    pub fn update_order_up(&mut self, mut i: i32) -> () {
        while i > 0 {
            let par = TopKHeap::parent(i);
            if self.heap[par as usize].count > self.heap[i as usize].count {
                self.swap(par, i);
                i = par;
            } else {
                break;
            }
        }
    }
}

impl Item {
    pub fn init_item(k: String, c: i64) -> Self {
        Item { key: k, count: c }
    }

    pub fn print_item(&self) -> () {
        println!("key: {} with count: {}", self.key, self.count);
    }
}
