use querysimulation::sketches::TopKHeap;

pub fn main() {
    equal_value_string_multi_insert();
}

pub fn equal_value_string_multi_insert() {
    let mut h = TopKHeap::init_heap(3);
    let a = &1.to_string();
    let b = &1.to_string();
    let c = &1.to_string();
    h.update(a, 1);
    h.update(b, 2);
    h.update(c, 3);
    match h.find(a) {
        Some(i) => {
            println!("index is: {}", i);
            println!("Count for {} is: {}", a, h.heap[i].count);
        }
        None => println!("None???"),
    }
    match h.find(b) {
        Some(i) => {
            println!("index is: {}", i);
            println!("Count for {} is: {}", a, h.heap[i].count);
        }
        None => println!("None???"),
    }
    match h.find(c) {
        Some(i) => {
            println!("index is: {}", i);
            println!("Count for {} is: {}", a, h.heap[i].count);
        }
        None => println!("None???"),
    }
    h.print_heap();
}
