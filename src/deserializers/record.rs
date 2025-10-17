// use serde::{Deserialize, Deserializer};
// use chrono::DateTime;

use serde::Deserialize;

fn hex_to_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    hex::decode(s).map_err(serde::de::Error::custom)
}

// fn timestamp_to_nanos<'de, D>(deserializer: D) -> Result<i64, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let s = String::deserialize(deserializer)?;
//     // Parse once during deserialization
//     DateTime::parse_from_rfc3339(&s)
//         .map_err(serde::de::Error::custom)?
//         .timestamp_nanos_opt()
//         .ok_or_else(|| serde::de::Error::custom("timestamp out of range"))
// }

#[derive(Debug, Deserialize)]
pub struct Record {
    // se: String,        // directly parse as byte array
    #[serde(deserialize_with = "hex_to_bytes")]
    pub se_cm: Vec<u8>,
    #[serde(deserialize_with = "hex_to_bytes")]
    pub se_hll: Vec<u8>,
    #[serde(deserialize_with = "hex_to_bytes")]
    pub se_kll: Vec<u8>,
    pub ha: Vec<String>,
    // #[serde(deserialize_with = "timestamp_to_nanos")]
    // pub ts: i64,
    pub ts: String,
}

#[derive(Debug, Deserialize)]
pub struct PMetricRecord {
    #[serde(deserialize_with = "hex_to_bytes")]
    pub se_hll: Vec<u8>,
    pub ips: Vec<i64>,
    pub ts: String,
}

#[derive(Debug, Deserialize)]
pub struct PMetricRecordV3 {
    #[serde(deserialize_with = "hex_to_bytes")]
    pub se_hll: Vec<u8>,
    pub ts: String,
}
