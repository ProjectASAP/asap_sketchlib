use std::fs::File;
use std::io::BufReader;
use serde::{Deserialize};
use rmp_serde::{Deserializer};
use querysimulation::sketches::{countmin::CountMin, utils::InsertableValue};
use std::io::Cursor;

#[derive(Debug, Deserialize)]
struct Record {
    se: String,        // directly parse as byte array
    ha: Vec<String>,
}

pub fn parse_and_print_bytes(path: &str) {
    let file = File::open(path).expect("Failed to open file");
    let reader = BufReader::new(file);

    let record: Record = serde_json::from_reader(reader).expect("Failed to parse JSON");

    println!("{:?}", record.se);
    println!("{:?}", record.ha);
    
    let bytes = hex::decode(&record.se).expect("Failed to decode hex string");
    let mut de = Deserializer::new(Cursor::new(bytes));
    let sketch: CountMin = Deserialize::deserialize(&mut de).expect("Failed to deserialize MsgPack");
    sketch.debug();
    for s in record.ha.iter() {
        match s.parse::<u32>() {
            Ok(n) => println!("estimate result is: {}", sketch.get_est(&InsertableValue::U32(n))),
            Err(e) => println!("whatever {}", e),
        }
    }

    // Print the result
    // println!("{:#?}", sketch);
}

fn main() {
    parse_and_print_bytes("/tmp/testcase/00000-000.json");
}