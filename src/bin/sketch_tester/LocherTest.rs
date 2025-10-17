use querysimulation::sketches::locher::LocherSketch;

fn main() {
    basic_test();
}

pub fn basic_test() {
    let mut sk = LocherSketch::new(5, 1024, 4);
    println!("===Basic Test for LocherSketch===");
    for v in 0..100u64 {
        sk.insert(&0.to_string(), v);
    }
    for v in 0..10u64 {
        sk.insert(&1.to_string(), v);
    }
    for v in 100..200u64 {
        sk.insert(&2.to_string(), v);
    }

    let e0 = sk.estimate(&0.to_string());
    let e1 = sk.estimate(&1.to_string());
    let e2 = sk.estimate(&2.to_string());

    println!("estimate for 0 is {}, while inserted 100", e0);
    println!("estimate for 1 is {}, while inserted 10", e1);
    println!("estimate for 2 is {}, while inserted 100", e2);
}
