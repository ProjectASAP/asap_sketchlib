use querysimulation::{
    sketches::univmon::UnivMon,
    utils::{LASTSTATE, SketchInput, hash_it},
};
// use std::collections::HashMap;

fn main() {
    println!("=== UnivMon Test Suite ===\n");

    test_basic_operations();
    // test_random_seed_operations();
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

fn test_basic_operations() {
    let cases: Vec<(String, i64)> = vec![
        ("notfound", 1),
        ("hello", 1),
        ("count", 3),
        ("min", 4),
        ("world", 10),
        ("cheatcheat", 3),
        ("cheatcheat", 7),
        ("min", 2),
        ("hello", 2),
        ("tigger", 34),
        ("flow", 9),
        ("miss", 4),
        ("hello", 30),
        ("world", 10),
        ("hello", 10),
        ("mom", 1),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut um = UnivMon::init_univmon(100, 3, 2048, 16, -1);
    for case in cases {
        let h = hash_it(LASTSTATE, &SketchInput::Str(&case.0));
        let bln = um.find_bottom_layer_num(h, 16);
        um.univmon_processing(&case.0, case.1, bln);
    }

    println!("cardinality estimation: {}", um.calc_card());
}
