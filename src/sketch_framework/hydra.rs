//! Hydra hierarchical sketch framework.
//!
//! A Hydra is an `r x w` grid of counters over a fixed set of named *key
//! columns*. Each incoming record supplies one value per column and fans out
//! into the `2^D - 1` non-empty subpopulations it belongs to; each subpopulation
//! is hashed to one column per row, and a query takes the **median** of the `r`
//! row estimates.
//!
//! Following the paper, a subpopulation is a *set of attribute-value
//! equalities*, `Q_i = {D_i1 = d_i1 AND ... AND D_il = d_il}`, so the column
//! identity is part of the subpopulation's identity and is therefore part of the
//! hashed subkey. See [`KeySchema`] for the canonical encoding.
//!
//! Accuracy note: every record writes `2^D - 1` subkeys into the *same* grid, so
//! the collision noise a cell sees is drawn from `N * (2^D - 1)` units of mass,
//! not from `N`. Theorem 2's additive `eps * G_s` term should be read against
//! that post-fan-out mass; the `2^D` factor lives inside its `O(1/eps)` columns.
//!
//! Reference:
//! - Manousis et al., VLDB 2022.
//!   <https://vldb.org/pvldb/vol15/p3249-manousis.pdf>

use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

use crate::Vector2D;
use crate::input::{HydraCounter, HydraQuery};
use crate::{DataInput, HYDRA_SEED, hash_for_matrix_seeded};

/// Maximum number of key columns. Each record fans out into `2^D - 1` subkeys,
/// so `D` is small by construction.
pub const MAX_KEY_COLUMNS: usize = 16;

/// Appends `s` to `out`, escaping the structural characters (`:` and `;`) and
/// the escape character itself, so that the subkey encoding stays injective for
/// labels and values that contain them.
fn push_escaped(out: &mut String, s: &str) {
    for ch in s.chars() {
        if matches!(ch, '\\' | ':' | ';') {
            out.push('\\');
        }
        out.push(ch);
    }
}

/// The key columns of a Hydra, plus the pre-escaped label forms used to build
/// canonical subkeys.
///
/// A subpopulation over the constrained column set `M` encodes, in **declaration
/// order**, as `label_i ":" value_i` joined by `";"` — e.g. `region:us;os:ios`.
/// Labels and values are escaped (`\` -> `\\`, `:` -> `\:`, `;` -> `\;`), so the
/// encoding is injective: `("x;y", "z")` and `("x", "y;z")` produce
/// `a:x\;y;b:z` and `a:x;b:y\;z` respectively.
///
/// Because the column is named in the subkey, two columns sharing a value domain
/// no longer collide, and a projection of a wide row can no longer alias the full
/// key of a narrow one.
///
/// Serializes as a plain array of labels; the escaped cache is rebuilt and the
/// labels re-validated on decode via `TryFrom<Vec<String>>`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct KeySchema {
    labels: Vec<String>,
    escaped_labels: Vec<String>,
}

impl TryFrom<Vec<String>> for KeySchema {
    type Error = String;

    fn try_from(labels: Vec<String>) -> Result<Self, Self::Error> {
        if labels.is_empty() {
            return Err("Hydra schema must declare at least one key column".to_string());
        }
        if labels.len() > MAX_KEY_COLUMNS {
            return Err(format!(
                "Hydra schema supports at most {MAX_KEY_COLUMNS} key columns, got {}",
                labels.len()
            ));
        }
        let mut sorted: Vec<&str> = labels.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        if let Some(pair) = sorted.windows(2).find(|w| w[0] == w[1]) {
            return Err(format!(
                "Hydra schema contains duplicate column label '{}'",
                pair[0]
            ));
        }

        let escaped_labels = labels
            .iter()
            .map(|label| {
                let mut escaped = String::with_capacity(label.len());
                push_escaped(&mut escaped, label);
                escaped
            })
            .collect();

        Ok(KeySchema {
            labels,
            escaped_labels,
        })
    }
}

impl From<KeySchema> for Vec<String> {
    fn from(schema: KeySchema) -> Self {
        schema.labels
    }
}

impl KeySchema {
    /// Number of key columns.
    #[inline]
    pub fn arity(&self) -> usize {
        self.labels.len()
    }

    /// Key-column labels, in declaration order.
    #[inline]
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Upper bound on the encoded length of any subkey over `values`.
    #[inline]
    fn encoded_capacity(&self, values: &[&str]) -> usize {
        self.escaped_labels
            .iter()
            .map(|label| label.len() + 2)
            .sum::<usize>()
            + values.iter().map(|value| 2 * value.len()).sum::<usize>()
    }

    /// Rejects a key whose width does not match the schema.
    #[inline]
    fn check_arity(&self, got: usize) -> Result<(), String> {
        if got != self.arity() {
            return Err(format!(
                "Hydra key arity mismatch: schema declares {} columns, got {got}",
                self.arity()
            ));
        }
        Ok(())
    }

    /// Validates a positional query key and folds it into `(mask, values)`.
    /// Unconstrained columns are excluded by the mask, so their slot is unused.
    fn resolve_query<'a>(&self, key: &[Option<&'a str>]) -> Result<(u32, Vec<&'a str>), String> {
        self.check_arity(key.len())?;
        let mut mask = 0u32;
        let mut values = vec![""; key.len()];
        for (col, slot) in key.iter().enumerate() {
            if let Some(value) = slot {
                mask |= 1 << col;
                values[col] = value;
            }
        }
        if mask == 0 {
            return Err("Hydra query must constrain at least one column".to_string());
        }
        Ok((mask, values))
    }

    /// Writes the canonical encoding of the subpopulation
    /// `{ labels[i] = values[i] : bit i of mask is set }` into `buf`.
    #[inline]
    fn encode_subkey_into(&self, values: &[&str], mask: u32, buf: &mut String) {
        debug_assert_eq!(values.len(), self.labels.len());
        buf.clear();
        let mut first = true;
        for (col, label) in self.escaped_labels.iter().enumerate() {
            if (mask >> col) & 1 == 1 {
                if !first {
                    buf.push(';');
                }
                buf.push_str(label);
                buf.push(':');
                push_escaped(buf, values[col]);
                first = false;
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Hierarchical sketch grid for subpopulation queries.
pub struct Hydra {
    /// Number of rows in the sketch grid.
    pub row_num: usize,
    /// Number of columns in the sketch grid.
    pub col_num: usize,
    /// Backing grid of per-cell sketches.
    pub sketches: Vector2D<HydraCounter>,
    /// Prototype sketch cloned for new cells.
    pub type_to_clone: HydraCounter,
    /// Key columns. Private because changing it would invalidate every subkey
    /// already hashed into the grid.
    schema: KeySchema,
}

impl Hydra {
    /// Creates a Hydra grid over the named key columns.
    ///
    /// Every [`update`](Hydra::update) supplies one value per column,
    /// positionally; every query constrains a subset of those columns.
    pub fn with_schema<S, I>(
        r: usize,
        c: usize,
        schema: I,
        sketch_type: HydraCounter,
    ) -> Result<Self, String>
    where
        S: Into<String>,
        I: IntoIterator<Item = S>,
    {
        let labels: Vec<String> = schema.into_iter().map(Into::into).collect();
        let schema = KeySchema::try_from(labels)?;
        let mut h = Hydra {
            row_num: r,
            col_num: c,
            sketches: Vector2D::init(r, c),
            type_to_clone: sketch_type.clone(),
            schema,
        };
        h.sketches.fill(sketch_type);
        Ok(h)
    }

    /// Key-column labels, in declaration order.
    pub fn schema(&self) -> &[String] {
        self.schema.labels()
    }

    /// Records one row of the stream.
    ///
    /// `key` supplies one value per schema column, positionally. The row fans
    /// out into all `2^D - 1` non-empty subpopulations it belongs to, each
    /// hashed with its column labels attached.
    pub fn update(
        &mut self,
        key: &[&str],
        value: &DataInput,
        count: Option<i32>,
    ) -> Result<(), String> {
        self.schema.check_arity(key.len())?;

        // Reuse a single buffer to minimize allocations
        let mut buffer = String::with_capacity(self.schema.encoded_capacity(key));

        for mask in 1u32..(1u32 << key.len()) {
            self.schema.encode_subkey_into(key, mask, &mut buffer);

            // Insert immediately instead of collecting all combinations first
            // Use Str(&str) variant to avoid cloning the buffer
            let hash = hash_for_matrix_seeded(
                HYDRA_SEED,
                self.row_num,
                self.col_num,
                &DataInput::Str(&buffer),
            );
            self.sketches
                .fast_insert(|a, b, _| a.insert(b, count), value, &hash);
        }
        Ok(())
    }

    /// Merge another Hydra sketch into this one.
    pub fn merge(&mut self, other: &Hydra) -> Result<(), String> {
        if self.row_num != other.row_num || self.col_num != other.col_num {
            return Err("Hydra dimension mismatch while merging".to_string());
        }
        if std::mem::discriminant(&self.type_to_clone)
            != std::mem::discriminant(&other.type_to_clone)
        {
            return Err("Hydra counter type mismatch while merging".to_string());
        }
        // Declaration order matters, not just the label set: the encoding is
        // positional, so after a permuted merge there would be no unambiguous
        // positional order for the result. Do not relax this to a set compare.
        if self.schema.labels() != other.schema.labels() {
            return Err(format!(
                "Hydra schema mismatch while merging: {:?} vs {:?}",
                self.schema.labels(),
                other.schema.labels()
            ));
        }
        let self_cells = self.sketches.as_mut_slice();
        let other_cells = other.sketches.as_slice();
        if self_cells.len() != other_cells.len() {
            return Err("Hydra storage length mismatch while merging".to_string());
        }
        for (self_counter, other_counter) in self_cells.iter_mut().zip(other_cells.iter()) {
            self_counter.merge(other_counter)?;
        }
        Ok(())
    }

    /// Query the Hydra sketch for a specific subpopulation.
    ///
    /// # Arguments
    /// * `key` - Positional and full width, one entry per schema column, in
    ///   schema order. `Some(v)` constrains that column to `v`; `None` leaves it
    ///   unconstrained. At least one column must be constrained.
    /// * `query` - The query type (Frequency, Quantile, Cardinality, etc.)
    ///
    /// # Returns
    /// The estimated statistic (median of r row estimates)
    pub fn query_key(&self, key: &[Option<&str>], query: &HydraQuery) -> Result<f64, String> {
        let (mask, values) = self.schema.resolve_query(key)?;
        // Probe the pristine template so an incompatible counter/query pair is a
        // clean error rather than a panic once per row inside the median closure.
        self.type_to_clone.query(query)?;

        let mut buffer = String::with_capacity(self.schema.encoded_capacity(&values));
        self.schema.encode_subkey_into(&values, mask, &mut buffer);
        let hashed_val = hash_for_matrix_seeded(
            HYDRA_SEED,
            self.row_num,
            self.col_num,
            &DataInput::Str(&buffer),
        );
        Ok(self
            .sketches
            .fast_query_median_with_key(&hashed_val, query, |counter, q, _, _| {
                counter.query(q).unwrap_or(0.0)
            }))
    }

    /// Convenience method for querying frequency (for CountMin-based Hydra)
    /// This is a wrapper around query_key with HydraQuery::Frequency
    pub fn query_frequency(&self, key: &[Option<&str>], value: &DataInput) -> Result<f64, String> {
        self.query_key(key, &HydraQuery::Frequency(value.clone()))
    }

    /// Convenience method for querying cumulative distribution for a tracked metric
    /// This is a wrapper around query_key with HydraQuery::Cdf
    pub fn query_quantile(&self, key: &[Option<&str>], threshold: f64) -> Result<f64, String> {
        self.query_key(key, &HydraQuery::Cdf(threshold))
    }

    /// Serializes the Hydra sketch (including all counters) into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a Hydra sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Multi-head Hydra with one sketch family per named measure head.
///
/// Note the two distinct naming concepts: [`key_schema`](MultiHeadHydra::key_schema)
/// names the *key columns* that define a subpopulation, while `heads` names the
/// *measures* tracked per cell. They are unrelated.
pub struct MultiHeadHydra {
    /// Number of rows in the sketch grid.
    pub row_num: usize,
    /// Number of columns in the sketch grid.
    pub col_num: usize,
    /// Backing grid of per-cell sketch vectors.
    pub sketches: Vector2D<Vec<HydraCounter>>,
    /// Named measure heads and their prototype sketches.
    pub heads: Vec<(String, HydraCounter)>,
    /// Key columns. Private for the same reason as [`Hydra`]'s.
    key_schema: KeySchema,
}

impl MultiHeadHydra {
    /// Returns the index of a named measure head.
    pub fn head_index(&self, head: &str) -> Option<usize> {
        self.heads.iter().position(|(name, _)| name == head)
    }

    /// Key-column labels, in declaration order.
    pub fn schema(&self) -> &[String] {
        self.key_schema.labels()
    }

    /// Creates a multi-head Hydra over the named key columns, with one sketch
    /// family per named measure head.
    pub fn with_schema<S, I>(
        r: usize,
        c: usize,
        schema: I,
        heads: Vec<(String, HydraCounter)>,
    ) -> Result<Self, String>
    where
        S: Into<String>,
        I: IntoIterator<Item = S>,
    {
        let labels: Vec<String> = schema.into_iter().map(Into::into).collect();
        let key_schema = KeySchema::try_from(labels)?;
        let template: Vec<HydraCounter> =
            heads.iter().map(|(_, counter)| counter.clone()).collect();
        let sketches = Vector2D::from_fn(r, c, |_, _| template.clone());
        Ok(MultiHeadHydra {
            row_num: r,
            col_num: c,
            sketches,
            heads,
            key_schema,
        })
    }

    /// Single fan-out, insert multiple values to different measure heads.
    ///
    /// `key` supplies one value per key-schema column, positionally.
    pub fn update(
        &mut self,
        key: &[&str],
        values: &[(&DataInput, &[&str])],
        count: Option<i32>,
    ) -> Result<(), String> {
        self.key_schema.check_arity(key.len())?;

        // An unknown head name is a caller error, not something to skip: silently
        // dropping it would make the whole update a no-op with no signal.
        let precomputed: Vec<Vec<usize>> = values
            .iter()
            .map(|(_, heads)| {
                heads
                    .iter()
                    .map(|head| {
                        self.head_index(head)
                            .ok_or_else(|| format!("MultiHeadHydra has no measure head '{head}'"))
                    })
                    .collect::<Result<Vec<usize>, String>>()
            })
            .collect::<Result<Vec<Vec<usize>>, String>>()?;
        let updates = (values, &precomputed);

        // Reuse a single buffer to minimize allocations
        let mut buffer = String::with_capacity(self.key_schema.encoded_capacity(key));
        for mask in 1u32..(1u32 << key.len()) {
            self.key_schema.encode_subkey_into(key, mask, &mut buffer);

            // Insert immediately instead of collecting all combinations first
            // Use Str(&str) variant to avoid cloning the buffer
            let hash = hash_for_matrix_seeded(
                HYDRA_SEED,
                self.row_num,
                self.col_num,
                &DataInput::Str(&buffer),
            );
            self.sketches.fast_insert(
                |cell_vec, dim_values, _| {
                    let (values, precomputed) = dim_values;
                    for ((value, _), indices) in values.iter().zip(precomputed.iter()) {
                        for &idx in indices.iter() {
                            if let Some(counter) = cell_vec.get_mut(idx) {
                                if let Some(hash) = counter.hash_for_value(value) {
                                    counter.insert_with_hash(value, &hash, count);
                                } else {
                                    counter.insert(value, count);
                                }
                            }
                        }
                    }
                },
                updates,
                &hash,
            );
        }
        Ok(())
    }

    /// Merge another MultiHeadHydra into this one.
    pub fn merge(&mut self, other: &MultiHeadHydra) -> Result<(), String> {
        if self.row_num != other.row_num || self.col_num != other.col_num {
            return Err("MultiHeadHydra dimension mismatch while merging".to_string());
        }
        if self.key_schema.labels() != other.key_schema.labels() {
            return Err(format!(
                "MultiHeadHydra key schema mismatch while merging: {:?} vs {:?}",
                self.key_schema.labels(),
                other.key_schema.labels()
            ));
        }
        if self.heads.len() != other.heads.len() {
            return Err("MultiHeadHydra head list mismatch while merging".to_string());
        }
        for (idx, (name, counter)) in self.heads.iter().enumerate() {
            let (other_name, other_counter) = other
                .heads
                .get(idx)
                .ok_or_else(|| "MultiHeadHydra head list mismatch while merging".to_string())?;
            if name != other_name {
                return Err(format!("MultiHeadHydra head order mismatch at index {idx}"));
            }
            if std::mem::discriminant(counter) != std::mem::discriminant(other_counter) {
                return Err(format!(
                    "MultiHeadHydra counter type mismatch for head '{name}'"
                ));
            }
        }

        let self_cells = self.sketches.as_mut_slice();
        let other_cells = other.sketches.as_slice();
        if self_cells.len() != other_cells.len() {
            return Err("MultiHeadHydra storage length mismatch while merging".to_string());
        }
        for (self_cell, other_cell) in self_cells.iter_mut().zip(other_cells.iter()) {
            if self_cell.len() != self.heads.len() || other_cell.len() != other.heads.len() {
                return Err("MultiHeadHydra cell head mismatch while merging".to_string());
            }
            for idx in 0..self.heads.len() {
                let self_counter = self_cell
                    .get_mut(idx)
                    .ok_or_else(|| "MultiHeadHydra missing head in target cell".to_string())?;
                let other_counter = other_cell
                    .get(idx)
                    .ok_or_else(|| "MultiHeadHydra missing head in source cell".to_string())?;
                self_counter.merge(other_counter)?;
            }
        }

        Ok(())
    }

    /// Query one measure head over a subpopulation.
    ///
    /// `key` is positional and full width, in key-schema order; `None` leaves a
    /// column unconstrained.
    pub fn query_key(
        &self,
        key: &[Option<&str>],
        head: &str,
        query: &HydraQuery,
    ) -> Result<f64, String> {
        let (mask, values) = self.key_schema.resolve_query(key)?;
        // An unknown head name is a caller error; returning 0.0 would be
        // indistinguishable from a genuinely empty subpopulation.
        let head_idx = self
            .head_index(head)
            .ok_or_else(|| format!("MultiHeadHydra has no measure head '{head}'"))?;
        self.heads[head_idx].1.query(query)?;

        let mut buffer = String::with_capacity(self.key_schema.encoded_capacity(&values));
        self.key_schema
            .encode_subkey_into(&values, mask, &mut buffer);
        let hashed_val = hash_for_matrix_seeded(
            HYDRA_SEED,
            self.row_num,
            self.col_num,
            &DataInput::Str(&buffer),
        );

        Ok(self
            .sketches
            .fast_query_median_with_key(&hashed_val, query, |cell_vec, q, _, _| {
                cell_vec
                    .get(head_idx)
                    .map(|counter| counter.query(q).unwrap_or(0.0))
                    .unwrap_or(0.0)
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Count, CountMin, ErtlMLE, FastPath, HyperLogLog, KLL, UnivMon, Vector2D};
    use std::collections::HashMap;

    const EPSILON: f64 = 1e-6;

    /// Three generic key columns, matching the `keyN;keyM;keyP` fixtures below.
    const K3: [&str; 3] = ["c0", "c1", "c2"];

    fn query_cdf(hydra: &Hydra, key_parts: &[Option<&str>], threshold: f64) -> f64 {
        hydra
            .query_quantile(key_parts, threshold)
            .expect("well-formed quantile query")
    }

    fn build_kll_test_hydra() -> Hydra {
        let template = HydraCounter::KLL(KLL::default());
        let mut hydra = Hydra::with_schema(3, 1024, K3, template).expect("valid schema");

        let dataset = [
            (["key1", "key2", "key3"], 10.0),
            (["key1", "key2", "key3"], 20.0),
            (["key1", "key2", "key3"], 30.0),
            (["key4", "key5", "key6"], 40.0),
            (["key4", "key5", "key6"], 50.0),
            (["key4", "key5", "key6"], 60.0),
            (["key7", "key8", "key9"], 70.0),
            (["key7", "key8", "key9"], 80.0),
            (["key7", "key8", "key9"], 90.0),
        ];

        for (key, value) in dataset {
            let input = DataInput::F64(value);
            hydra.update(&key, &input, None).expect("schema arity");
        }

        hydra
    }

    #[test]
    fn hydra_updates_countmin_frequency() {
        let mut hydra = Hydra::with_schema(
            3,
            32,
            ["user", "session"],
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        )
        .expect("valid schema");
        let value = DataInput::String("event".to_string());

        for _ in 0..5 {
            hydra
                .update(&["alice", "s1"], &value, None)
                .expect("schema arity");
        }

        let combined = hydra
            .query_frequency(&[Some("alice"), Some("s1")], &value)
            .expect("well-formed query");
        assert!(
            combined >= 5.0,
            "expected frequency at least 5, got {combined}"
        );

        let unrelated = hydra
            .query_frequency(&[Some("other"), None], &value)
            .expect("well-formed query");
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_updates_countmin_frequency_multiple_values() {
        let mut hydra = Hydra::with_schema(
            3,
            32,
            K3,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        )
        .expect("valid schema");

        for i in 0..5 {
            for _ in 0..i {
                let value = DataInput::I64(i as i64);
                hydra
                    .update(&["key1", "key2", "key3"], &value, None)
                    .expect("schema arity");
            }
        }

        for i in 0..5 {
            let query_value = DataInput::I64(i as i64);
            let combined = hydra
                .query_frequency(&[Some("key1"), None, Some("key3")], &query_value)
                .expect("well-formed query");
            assert!(
                combined >= i as f64,
                "expected frequency at least {i}, got {combined}"
            );
        }

        let unrelated_value = DataInput::I64(0);
        let unrelated = hydra
            .query_frequency(&[Some("other"), None, None], &unrelated_value)
            .expect("well-formed query");
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_round_trip_serialization() {
        let template =
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 64));
        let mut hydra = Hydra::with_schema(3, 64, ["city", "device", "country"], template)
            .expect("valid schema");

        // A fixed schema requires uniform arity, so every row carries all three
        // columns; what used to be a shorter key is now a wildcard at query time.
        let dataset = [
            (["nyc", "phone", "us"], "event_a"),
            (["nyc", "phone", "us"], "event_a"),
            (["nyc", "browser", "us"], "event_b"),
            (["sfo", "phone", "us"], "event_c"),
            (["nyc", "phone", "ca"], "event_a"),
        ];

        for (key, value) in dataset {
            hydra
                .update(&key, &DataInput::String(value.to_string()), None)
                .expect("schema arity");
        }

        let hot_value = DataInput::String("event_a".to_string());
        let cold_value = DataInput::String("event_c".to_string());

        let freq_before = hydra
            .query_frequency(&[Some("nyc"), Some("phone"), None], &hot_value)
            .expect("well-formed query");
        let region_before = hydra
            .query_frequency(&[Some("sfo"), None, None], &cold_value)
            .expect("well-formed query");

        let encoded = hydra
            .serialize_to_bytes()
            .expect("serialize Hydra into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded =
            Hydra::deserialize_from_bytes(&data).expect("deserialize Hydra from MessagePack");

        assert_eq!(hydra.row_num, decoded.row_num);
        assert_eq!(hydra.col_num, decoded.col_num);
        assert_eq!(hydra.sketches.rows(), decoded.sketches.rows());
        assert_eq!(hydra.sketches.cols(), decoded.sketches.cols());
        // The key schema is part of the payload: without it the decoded sketch
        // could not reproduce a single subkey.
        assert_eq!(hydra.schema(), decoded.schema());
        match &decoded.type_to_clone {
            HydraCounter::CM(_) => {}
            other => panic!("expected CM template, got {other:?}"),
        }

        let freq_after = decoded
            .query_frequency(&[Some("nyc"), Some("phone"), None], &hot_value)
            .expect("well-formed query");
        let region_after = decoded
            .query_frequency(&[Some("sfo"), None, None], &cold_value)
            .expect("well-formed query");

        assert_eq!(freq_before, freq_after, "frequency changed after serde");
        assert_eq!(
            region_before, region_after,
            "region frequency changed after serde"
        );
    }

    #[test]
    fn multihead_hydra_updates_multiple_dimensions() {
        let heads = vec![
            (
                "events".to_string(),
                HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
            ),
            (
                "latency".to_string(),
                HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
            ),
        ];
        let mut hydra =
            MultiHeadHydra::with_schema(3, 32, ["user", "session"], heads).expect("valid schema");

        let event_value = DataInput::String("event_a".to_string());
        let latency_value = DataInput::I64(120);

        for _ in 0..3 {
            hydra
                .update(
                    &["alice", "s1"],
                    &[(&event_value, &["events"]), (&latency_value, &["latency"])],
                    None,
                )
                .expect("schema arity and known heads");
        }

        let events_full = hydra
            .query_key(
                &[Some("alice"), Some("s1")],
                "events",
                &HydraQuery::Frequency(event_value.clone()),
            )
            .expect("well-formed query");
        assert!(
            events_full >= 3.0,
            "expected events count at least 3, got {events_full}"
        );

        let events_fanout = hydra
            .query_key(
                &[Some("alice"), None],
                "events",
                &HydraQuery::Frequency(event_value.clone()),
            )
            .expect("well-formed query");
        assert!(
            events_fanout >= 3.0,
            "expected fan-out events count at least 3, got {events_fanout}"
        );

        let latency_full = hydra
            .query_key(
                &[Some("alice"), Some("s1")],
                "latency",
                &HydraQuery::Frequency(latency_value.clone()),
            )
            .expect("well-formed query");
        assert!(
            latency_full >= 3.0,
            "expected latency count at least 3, got {latency_full}"
        );

        // An unknown head is a caller error, not a silent zero / silent no-op.
        assert!(
            hydra
                .query_key(
                    &[Some("alice"), None],
                    "nope",
                    &HydraQuery::Frequency(event_value.clone()),
                )
                .is_err()
        );
        assert!(
            hydra
                .update(&["alice", "s1"], &[(&event_value, &["nope"])], None)
                .is_err()
        );
    }

    #[test]
    fn hydra_subpopulation_frequency_test() {
        // Build test dataset using CountMin for frequency queries
        let mut hydra = Hydra::with_schema(
            3,
            64,
            K3,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        )
        .expect("valid schema");

        let dataset = [
            (["key1", "key2", "key3"], 10.0),
            (["key1", "key2", "key4"], 10.0),
            (["key1", "key2", "key3"], 20.0),
            (["key1", "key2", "key3"], 30.0),
            (["key4", "key5", "key6"], 40.0),
            (["key4", "key5", "key6"], 50.0),
            (["key4", "key5", "key6"], 60.0),
            (["key7", "key8", "key9"], 70.0),
            (["key7", "key8", "key9"], 80.0),
            (["key7", "key8", "key9"], 90.0),
        ];

        // Insert all data points
        for (key, value) in dataset {
            let input = DataInput::F64(value);
            hydra.update(&key, &input, None).expect("schema arity");
        }

        let freq = |key: &[Option<&str>], value: f64| {
            hydra
                .query_frequency(key, &DataInput::F64(value))
                .expect("well-formed query")
        };

        // Test single label subpopulation queries
        // key1 appears in 3 entries with values 10.0, 20.0, 30.0
        let freq_10 = freq(&[Some("key1"), None, None], 10.0);
        assert_eq!(
            freq_10, 2.0,
            "expected frequency of 10.0 for key1 to be 2, got {freq_10}"
        );

        let freq_20 = freq(&[Some("key1"), None, None], 20.0);
        assert_eq!(
            freq_20, 1.0,
            "expected frequency of 20.0 for key1 to be 1, got {freq_20}"
        );

        let freq_30 = freq(&[Some("key1"), None, None], 30.0);
        assert_eq!(
            freq_30, 1.0,
            "expected frequency of 30.0 for key1 to be 1, got {freq_30}"
        );

        // key4 appears in 3 entries with values 40.0, 50.0, 60.0
        let freq_40 = freq(&[Some("key4"), None, None], 40.0);
        assert_eq!(
            freq_40, 1.0,
            "expected frequency of 40.0 for key4 to be 1, got {freq_40}"
        );

        // Test multi-label subpopulation queries
        let freq_multi = freq(&[Some("key1"), None, Some("key3")], 10.0);
        assert_eq!(
            freq_multi, 1.0,
            "expected frequency of 10.0 for c0=key1,c2=key3 to be 1, got {freq_multi}"
        );

        // (key1, key2, key3) is the full key appearing 3 times
        let freq_full = freq(&[Some("key1"), Some("key2"), Some("key3")], 20.0);
        assert_eq!(
            freq_full, 1.0,
            "expected frequency of 20.0 for the full key to be 1, got {freq_full}"
        );

        // Test cross-population queries (key1 in c0 and key8 in c1 never co-occur)
        let freq_cross = freq(&[Some("key1"), Some("key8"), None], 10.0);
        assert_eq!(
            freq_cross, 0.0,
            "expected frequency of 10.0 for c0=key1,c1=key8 to be 0/empty, got {freq_cross}"
        );
    }

    #[test]
    fn hydra_subpopulation_cardinality_test() {
        use crate::sketches::hll::{ErtlMLE, HyperLogLog};

        // Build test dataset using HyperLogLog for cardinality queries
        let mut hydra =
            Hydra::with_schema(5, 128, K3, HydraCounter::HLL(HyperLogLog::<ErtlMLE>::new()))
                .expect("valid schema");

        let dataset = [
            (["key1", "key2", "key3"], 10.0),
            (["key1", "key2", "key3"], 20.0),
            (["key1", "key2", "key3"], 30.0),
            (["key4", "key5", "key6"], 40.0),
            (["key4", "key5", "key6"], 50.0),
            (["key4", "key5", "key6"], 60.0),
            (["key7", "key8", "key9"], 70.0),
            (["key7", "key8", "key9"], 80.0),
            (["key7", "key8", "key9"], 90.0),
        ];

        // Insert all data points (HLL tracks distinct values)
        for (key, value) in dataset {
            let input = DataInput::F64(value);
            hydra.update(&key, &input, None).expect("schema arity");
        }

        let card = |key: &[Option<&str>]| {
            hydra
                .query_key(key, &HydraQuery::Cardinality)
                .expect("well-formed query")
        };

        // Test single label cardinality
        // key1 appears with 3 distinct values: 10.0, 20.0, 30.0
        let card_key1 = card(&[Some("key1"), None, None]);
        assert!(
            (card_key1 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key1, got {card_key1}"
        );

        // key4 appears with 3 distinct values: 40.0, 50.0, 60.0
        let card_key4 = card(&[Some("key4"), None, None]);
        assert!(
            (card_key4 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key4, got {card_key4}"
        );

        // key7 appears with 3 distinct values: 70.0, 80.0, 90.0
        let card_key7 = card(&[Some("key7"), None, None]);
        assert!(
            (card_key7 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key7, got {card_key7}"
        );

        // Test multi-label cardinality
        // (c0=key1, c1=key2) appears together with 3 distinct values
        let card_multi = card(&[Some("key1"), Some("key2"), None]);
        assert!(
            (card_multi - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for c0=key1,c1=key2, got {card_multi}"
        );

        // The full key has 3 distinct values
        let card_full = card(&[Some("key1"), Some("key2"), Some("key3")]);
        assert!(
            (card_full - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for the full key, got {card_full}"
        );

        // Cross-population query. Note key1 and key7 are both c0 values, so under
        // a schema they are mutually exclusive by construction rather than merely
        // absent; the meaningful cross-population probe pairs distinct columns.
        let card_cross = card(&[Some("key1"), Some("key8"), None]);
        assert_eq!(
            card_cross, 0.0,
            "expected cardinality 0 for non-overlapping keys"
        );

        // Test unrelated key (never inserted)
        let card_unrelated = card(&[Some("unknown"), None, None]);
        assert_eq!(
            card_unrelated, 0.0,
            "expected cardinality 0 for unknown key"
        );
    }

    #[test]
    fn hydra_tracks_kll_quantiles() {
        let mut hydra = Hydra::with_schema(
            3,
            64,
            ["metric", "stage"],
            HydraCounter::KLL(KLL::default()),
        )
        .expect("valid schema");
        let samples = [
            DataInput::F64(10.0),
            DataInput::F64(20.0),
            DataInput::F64(30.0),
            DataInput::F64(40.0),
            DataInput::F64(50.0),
        ];

        for sample in &samples {
            hydra
                .update(&["metrics", "latency"], sample, None)
                .expect("schema arity");
        }

        // let query_value = DataInput::F64(35.0);
        let quantile = hydra
            .query_key(&[Some("metrics"), Some("latency")], &HydraQuery::Cdf(30.0))
            .expect("well-formed query");
        assert!(
            (quantile - 0.6).abs() < 1e-9,
            "expected CDF near 0.6, got {quantile}"
        );

        let empty_bucket = hydra
            .query_key(&[Some("other"), Some("key")], &HydraQuery::Cdf(50.0))
            .expect("well-formed query");
        assert_eq!(empty_bucket, 0.0);
    }

    #[test]
    fn hydra_kll_single_label_cdfs() {
        let hydra = build_kll_test_hydra();

        assert!(
            (query_cdf(&hydra, &[Some("key1"), None, None], 15.0) - (1.0 / 3.0)).abs() < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key1"), None, None], 25.0) - (2.0 / 3.0)).abs() < EPSILON
        );
        assert!((query_cdf(&hydra, &[Some("key1"), None, None], 35.0) - 1.0).abs() < EPSILON);

        assert!(
            (query_cdf(&hydra, &[Some("key4"), None, None], 45.0) - (1.0 / 3.0)).abs() < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key4"), None, None], 55.0) - (2.0 / 3.0)).abs() < EPSILON
        );
        assert!((query_cdf(&hydra, &[Some("key4"), None, None], 65.0) - 1.0).abs() < EPSILON);

        assert!(
            (query_cdf(&hydra, &[Some("key7"), None, None], 75.0) - (1.0 / 3.0)).abs() < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key7"), None, None], 85.0) - (2.0 / 3.0)).abs() < EPSILON
        );
        assert!((query_cdf(&hydra, &[Some("key7"), None, None], 95.0) - 1.0).abs() < EPSILON);
    }

    #[test]
    fn hydra_kll_multi_label_cdfs() {
        let hydra = build_kll_test_hydra();

        assert!(
            (query_cdf(&hydra, &[Some("key1"), None, Some("key3")], 25.0) - (2.0 / 3.0)).abs()
                < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key1"), Some("key2"), Some("key3")], 30.0) - 1.0).abs()
                < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key4"), Some("key5"), None], 55.0) - (2.0 / 3.0)).abs()
                < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key4"), Some("key5"), Some("key6")], 60.0) - 1.0).abs()
                < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key7"), Some("key8"), Some("key9")], 85.0) - (2.0 / 3.0))
                .abs()
                < EPSILON
        );
        // key1 and key7 are both c0 values, so pair distinct columns that never
        // co-occur instead: c0=key1 with c1=key5.
        assert!(
            (query_cdf(&hydra, &[Some("key1"), Some("key5"), None], 50.0) - 0.0).abs() < EPSILON
        );
    }

    #[test]
    fn hydra_kll_extreme_queries() {
        let hydra = build_kll_test_hydra();

        assert!((query_cdf(&hydra, &[Some("key1"), None, None], 0.0) - 0.0).abs() < EPSILON);
        assert!((query_cdf(&hydra, &[Some("key1"), None, None], 100.0) - 1.0).abs() < EPSILON);

        assert!(
            (query_cdf(&hydra, &[Some("key4"), Some("key5"), Some("key6")], 35.0) - 0.0).abs()
                < EPSILON
        );
        assert!(
            (query_cdf(&hydra, &[Some("key4"), Some("key5"), Some("key6")], 100.0) - 1.0).abs()
                < EPSILON
        );

        assert!((query_cdf(&hydra, &[Some("unknown"), None, None], 50.0) - 0.0).abs() < EPSILON);
    }

    // Helper to generate a default CountMin counter
    fn cm_counter() -> HydraCounter {
        HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default())
    }

    /// A deliberately tiny per-cell counter. The default CountMin is 3x4096 i32
    /// (~49 KB) *per grid cell*, which is fine for a 3x64 grid and ruinous for a
    /// large one.
    fn small_cm_counter() -> HydraCounter {
        HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(2, 64))
    }

    #[test]
    fn key_schema_encoding_names_its_columns() {
        let s3 = KeySchema::try_from(vec!["a".to_string(), "b".to_string(), "c".to_string()])
            .expect("valid schema");
        let s2 = KeySchema::try_from(vec!["x".to_string(), "y".to_string()]).expect("valid schema");

        let mut buf = String::new();

        // The column is named in the subkey, so the same value in two different
        // columns can never share a cell.
        s3.encode_subkey_into(&["p", "q", "r"], 0b001, &mut buf);
        assert_eq!(buf, "a:p");
        s3.encode_subkey_into(&["p", "q", "r"], 0b010, &mut buf);
        assert_eq!(buf, "b:q");
        s3.encode_subkey_into(&["p", "q", "r"], 0b101, &mut buf);
        assert_eq!(buf, "a:p;c:r");

        // Arity ambiguity: the {a,c} projection of a 3-column row no longer
        // aliases the full key of a 2-column row over the same values.
        let mut wide = String::new();
        s3.encode_subkey_into(&["p", "q", "r"], 0b101, &mut wide);
        let mut narrow = String::new();
        s2.encode_subkey_into(&["p", "r"], 0b11, &mut narrow);
        assert_ne!(wide, narrow);

        // Separators inside values stay unambiguous.
        let mut first = String::new();
        s2.encode_subkey_into(&["x;y", "z"], 0b11, &mut first);
        let mut second = String::new();
        s2.encode_subkey_into(&["x", "y;z"], 0b11, &mut second);
        assert_eq!(first, "x:x\\;y;y:z");
        assert_eq!(second, "x:x;y:y\\;z");
        assert_ne!(first, second);

        // Colons and backslashes are escaped too.
        let mut colon = String::new();
        s2.encode_subkey_into(&["a:b", "c"], 0b11, &mut colon);
        assert_eq!(colon, "x:a\\:b;y:c");
        let mut backslash = String::new();
        s2.encode_subkey_into(&["a\\b", "c"], 0b11, &mut backslash);
        assert_eq!(backslash, "x:a\\\\b;y:c");

        // Labels are escaped at construction.
        let odd =
            KeySchema::try_from(vec!["a;b".to_string(), "c:d".to_string()]).expect("valid schema");
        let mut odd_buf = String::new();
        odd.encode_subkey_into(&["p", "q"], 0b11, &mut odd_buf);
        assert_eq!(odd_buf, "a\\;b:p;c\\:d:q");

        // An empty value is legal and distinct from an unconstrained column.
        let mut empty = String::new();
        s2.encode_subkey_into(&["", "c"], 0b11, &mut empty);
        assert_eq!(empty, "x:;y:c");
        let mut unconstrained = String::new();
        s2.encode_subkey_into(&["", "c"], 0b10, &mut unconstrained);
        assert_eq!(unconstrained, "y:c");
        assert_ne!(empty, unconstrained);
    }

    #[test]
    fn key_schema_rejects_invalid_column_lists() {
        assert!(KeySchema::try_from(Vec::<String>::new()).is_err());
        assert!(KeySchema::try_from(vec!["a".to_string(), "a".to_string()]).is_err());
        let too_many: Vec<String> = (0..=MAX_KEY_COLUMNS).map(|i| format!("c{i}")).collect();
        assert!(KeySchema::try_from(too_many).is_err());
    }

    /// Before the fix, `update` hashed only the concatenated *values*, so every
    /// column shared one subkey namespace. This pins each consequence of that.
    #[test]
    fn hydra_subkeys_are_labelled_by_column() {
        let value = DataInput::Str("pkt");
        let freq = |h: &Hydra, key: &[Option<&str>]| {
            h.query_frequency(key, &value).expect("well-formed query")
        };

        // 1. Cross-column collision: `alice` in `src` is not visible as a `dst`.
        let mut h =
            Hydra::with_schema(3, 512, ["src", "dst"], small_cm_counter()).expect("valid schema");
        for _ in 0..10 {
            h.update(&["alice", "bob"], &value, None).expect("arity");
        }
        assert_eq!(freq(&h, &[Some("alice"), None]), 10.0);
        assert_eq!(freq(&h, &[None, Some("alice")]), 0.0);
        assert_eq!(freq(&h, &[None, Some("bob")]), 10.0);
        assert_eq!(freq(&h, &[Some("bob"), None]), 0.0);

        // 2. Structural characters inside values are escaped, not conflated.
        let mut h2 =
            Hydra::with_schema(3, 512, ["a", "b"], small_cm_counter()).expect("valid schema");
        h2.update(&["x;y", "z"], &value, None).expect("arity");
        h2.update(&["x", "y;z"], &value, None).expect("arity");
        assert_eq!(freq(&h2, &[Some("x;y"), None]), 1.0);
        assert_eq!(freq(&h2, &[Some("x"), None]), 1.0);
        assert_eq!(freq(&h2, &[None, Some("y;z")]), 1.0);
        assert_eq!(freq(&h2, &[None, Some("z")]), 1.0);

        // 3. An interior column can be left unconstrained.
        let mut h3 = Hydra::with_schema(3, 512, K3, small_cm_counter()).expect("valid schema");
        h3.update(&["p", "q", "r"], &value, None).expect("arity");
        h3.update(&["p", "other", "r"], &value, None)
            .expect("arity");
        assert_eq!(freq(&h3, &[Some("p"), None, Some("r")]), 2.0);
        assert_eq!(freq(&h3, &[Some("p"), Some("q"), Some("r")]), 1.0);
        assert_eq!(freq(&h3, &[None, Some("q"), None]), 1.0);

        // 4. Misuse is rejected rather than silently coerced.
        assert!(h3.update(&["p", "q"], &value, None).is_err());
        assert!(
            h3.query_key(&[Some("p")], &HydraQuery::Frequency(value.clone()))
                .is_err()
        );
        assert!(
            h3.query_key(&[None, None, None], &HydraQuery::Frequency(value.clone()))
                .is_err()
        );
        // A counter/query mismatch is an error, not a panic.
        assert!(
            h3.query_key(&[Some("p"), None, None], &HydraQuery::Cardinality)
                .is_err()
        );
        assert!(Hydra::with_schema(3, 64, ["a", "a"], small_cm_counter()).is_err());
        assert!(Hydra::with_schema(3, 64, Vec::<String>::new(), small_cm_counter()).is_err());
    }

    /// Exact probability that a *median* of `rows` row-estimates violates the
    /// bound, given per-row failure probability `p_row`. The median fails only
    /// when a strict majority of rows fail.
    ///
    /// This is deliberately not the `e^-rows` used by the Count-Min bound tests:
    /// that is the *min*-estimator bound, where all rows must fail at once.
    /// Hydra combines rows by median, so reusing it would understate delta by
    /// more than an order of magnitude.
    fn median_failure_probability(rows: usize, p_row: f64) -> f64 {
        let need = rows / 2 + 1;
        let mut total = 0.0;
        for k in need..=rows {
            let mut binom = 1.0_f64;
            for t in 0..k {
                binom = binom * (rows - t) as f64 / (t + 1) as f64;
            }
            total += binom * p_row.powi(k as i32) * (1.0 - p_row).powi((rows - k) as i32);
        }
        total
    }

    #[test]
    fn median_failure_probability_matches_binomial_tail() {
        // P[Bin(5, 1/4) >= 3] = 10*(1/4)^3*(3/4)^2 + 5*(1/4)^4*(3/4) + (1/4)^5
        assert!((median_failure_probability(5, 0.25) - 0.103_515_625).abs() < 1e-12);
        // P[Bin(3, 1/4) >= 2] = 3*(1/4)^2*(3/4) + (1/4)^3
        assert!((median_failure_probability(3, 0.25) - 0.156_25).abs() < 1e-12);
        assert!((median_failure_probability(5, 0.0) - 0.0).abs() < 1e-12);
        assert!((median_failure_probability(5, 1.0) - 1.0).abs() < 1e-12);
    }

    /// Resolves a ground-truth column slot into a query entry.
    /// `usize::MAX` marks a column the subpopulation does not constrain.
    fn pick(slot: usize, values: &[String]) -> Option<&str> {
        if slot == usize::MAX {
            None
        } else {
            Some(values[slot].as_str())
        }
    }

    /// Drives one `(rows, cols)` configuration of the Theorem 2 check.
    fn run_hydra_bound_config(rows: usize, cols: usize) {
        const N: usize = 100_000;
        const REGIONS: usize = 8;
        const DEVICES: usize = 4;
        const OSES: usize = 3;
        const COMBOS: usize = REGIONS * DEVICES * OSES;
        const D: usize = 3;
        const FANOUT: usize = (1 << D) - 1;

        let region_values: Vec<String> = (0..REGIONS).map(|i| format!("region-{i}")).collect();
        let device_values: Vec<String> = (0..DEVICES).map(|i| format!("device-{i}")).collect();
        let os_values: Vec<String> = (0..OSES).map(|i| format!("os-{i}")).collect();

        let mut hydra =
            Hydra::with_schema(rows, cols, ["region", "device", "os"], small_cm_counter())
                .expect("valid schema");

        // A single distinct measure value means every per-cell CountMin holds
        // exactly one item and is therefore *exact*: eps_us = 0. That isolates
        // the Hydra grid, which is what the additive eps*G_s term describes.
        let measure = DataInput::Str("hit");
        let stream = crate::test_utils::sample_zipf_u64(COMBOS, 1.1, N, 0x5eed_c0de);

        // Exact ground truth for every fanned-out subpopulation, computed
        // independently of the sketch.
        let mut truth: HashMap<[usize; D], u64> = HashMap::new();

        for &combo in &stream {
            let combo = combo as usize;
            let idx = [
                combo % REGIONS,
                (combo / REGIONS) % DEVICES,
                combo / (REGIONS * DEVICES),
            ];
            let key = [
                region_values[idx[0]].as_str(),
                device_values[idx[1]].as_str(),
                os_values[idx[2]].as_str(),
            ];
            hydra.update(&key, &measure, None).expect("schema arity");

            for mask in 1..=FANOUT {
                let mut projected = [usize::MAX; D];
                for (col, slot) in projected.iter_mut().enumerate() {
                    if (mask >> col) & 1 == 1 {
                        *slot = idx[col];
                    }
                }
                *truth.entry(projected).or_insert(0) += 1;
            }
        }

        // G_s is the *post-fan-out* mass actually resident in the shared grid:
        // each record writes FANOUT subkeys into the same r x w grid, so the
        // collision noise a cell sees is drawn from N * FANOUT units, not N.
        // (The paper's literal eps*G_s assumes a sketch per dimension-subset;
        // with one shared grid the 2^D factor lives inside its O(1/eps).)
        let g_s = (N * FANOUT) as f64;
        assert_eq!(
            truth.values().sum::<u64>(),
            (N * FANOUT) as u64,
            "ground truth must account for exactly the mass written to the grid"
        );

        // Markov on the per-row noise: E[noise] <= G_s / w, so
        // P[noise > eps * G_s] <= 1 / (eps * w). Setting eps = 4/w pins the
        // per-row failure probability at 1/4.
        let epsilon = 4.0 / cols as f64;
        let p_row = 1.0 / (epsilon * cols as f64);
        let delta = median_failure_probability(rows, p_row);
        let error_bound = epsilon * g_s;

        let mut within_count = 0usize;
        let mut max_over = 0.0f64;
        let mut sum_over = 0.0f64;
        for (projected, &g_i) in &truth {
            let key = [
                pick(projected[0], &region_values),
                pick(projected[1], &device_values),
                pick(projected[2], &os_values),
            ];
            let est = hydra
                .query_frequency(&key, &measure)
                .expect("well-formed query");

            // Lower bound with eps_us = 0: CountMin never under-counts, and the
            // median of non-negative-noise estimates is >= the truth. A failure
            // here is lost mass, not a probabilistic miss.
            assert!(
                est >= g_i as f64,
                "Theorem 2 lower bound violated for {key:?}: est {est} < truth {g_i}"
            );

            let over = est - g_i as f64;
            max_over = max_over.max(over);
            sum_over += over;
            if over <= error_bound {
                within_count += 1;
            }
        }

        let total = truth.len();
        let correct_lower_bound = total as f64 * (1.0 - delta);
        eprintln!(
            "[hydra_error_bound] rows={rows} cols={cols} G_s={g_s} eps={epsilon:.6} \
             bound={error_bound:.1} | within={within_count}/{total} \
             (required>{correct_lower_bound:.1}) max_overshoot={max_over:.1} \
             mean_overshoot={:.1} delta={delta:.6}",
            sum_over / total as f64
        );

        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound subpopulations {within_count} not greater than expected amount \
             {correct_lower_bound} (of {total}, rows={rows}, cols={cols}, eps={epsilon}, \
             delta={delta}, bound={error_bound}, max_overshoot={max_over})"
        );
    }

    /// Theorem 2 (Manousis et al., VLDB 2022, arXiv:2208.04927): a Hydra-sketch
    /// with `w = O(1/eps)` columns and `r` rows combined by median satisfies,
    /// with probability `1 - delta`,
    ///
    /// ```text
    ///     G_i * (1 - eps_us)  <=  Ghat_i  <=  G_i * (1 + eps_us) + eps * G_s
    /// ```
    ///
    /// Here the per-cell counter is exact (`eps_us = 0`), so the lower bound
    /// degenerates to `Ghat_i >= G_i` and is asserted for *every* subpopulation,
    /// and the substantive claim is the additive upper bound.
    ///
    /// The second configuration deliberately overloads the grid (700k units of
    /// mass over 256 columns) so the in-bound fraction is genuinely exercised
    /// rather than trivially saturated.
    #[test]
    fn hydra_error_bound_zipf_subpopulations() {
        run_hydra_bound_config(5, 4096);
        run_hydra_bound_config(5, 256);
    }

    #[test]
    fn hydra_merge_rejects_schema_mismatch() {
        let value = DataInput::Str("pkt");

        let mut a =
            Hydra::with_schema(3, 64, ["src", "dst"], small_cm_counter()).expect("valid schema");
        let mut reordered =
            Hydra::with_schema(3, 64, ["dst", "src"], small_cm_counter()).expect("valid schema");
        let different =
            Hydra::with_schema(3, 64, ["src", "port"], small_cm_counter()).expect("valid schema");
        let narrower =
            Hydra::with_schema(3, 64, ["src"], small_cm_counter()).expect("valid schema");

        a.update(&["alice", "bob"], &value, None).expect("arity");
        reordered
            .update(&["bob", "alice"], &value, None)
            .expect("arity");

        // Same label *set*, different declaration order: the API is positional,
        // so the merged result would have no unambiguous column order.
        assert!(a.merge(&reordered).is_err());
        assert!(a.merge(&different).is_err());
        assert!(a.merge(&narrower).is_err());

        // Identical schemas still merge.
        let mut same =
            Hydra::with_schema(3, 64, ["src", "dst"], small_cm_counter()).expect("valid schema");
        same.update(&["alice", "bob"], &value, None).expect("arity");
        assert!(a.merge(&same).is_ok());
        assert_eq!(
            a.query_frequency(&[Some("alice"), None], &value)
                .expect("well-formed query"),
            2.0
        );
    }

    // Helper to generate a default Count Sketch counter
    fn count_counter() -> HydraCounter {
        HydraCounter::CS(Count::<Vector2D<i32>, FastPath>::default())
    }

    // Helper to generate a default UnivMon counter
    fn univmon_counter() -> HydraCounter {
        HydraCounter::UNIVERSAL(UnivMon::default())
    }

    #[test]
    fn test_count_min_frequency_query() {
        let mut counter = cm_counter();
        let key = DataInput::I64(42);

        // 1. Insert data
        counter.insert(&key, None);
        counter.insert(&key, None);
        counter.insert(&key, None);

        // 2. Query Frequency (Valid)
        let query = HydraQuery::Frequency(key);
        let result = counter.query(&query);

        assert!(result.is_ok());
        // CountMin isn't always exact, but for small inputs/defaults it usually is
        assert_eq!(result.unwrap(), 3.0);
    }

    #[test]
    fn test_count_min_invalid_query_types() {
        let counter = cm_counter();

        // 1. Test Quantile query (Invalid for CM)
        let result = counter.query(&HydraQuery::Quantile(0.5));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Count-Min Sketch Counter does not support Quantile Query"
        );

        // 2. Test Cardinality query (Invalid for CM)
        let result = counter.query(&HydraQuery::Cardinality);
        assert!(result.is_err());
    }

    #[test]
    fn test_hll_cardinality_query() {
        let mut counter = HydraCounter::HLL(HyperLogLog::<ErtlMLE>::default());

        // 1. Insert unique items
        for i in 0..100 {
            counter.insert(&DataInput::I64(i), None);
        }
        // Duplicate insertions shouldn't affect cardinality
        counter.insert(&DataInput::I64(0), None);

        // 2. Query Cardinality (Valid)
        let result = counter.query(&HydraQuery::Cardinality);
        assert!(result.is_ok());

        // HLL is probabilistic, check for reasonable error margin (e.g., +/- 5%)
        let card = result.unwrap();
        assert!(
            card > 90.0 && card < 110.0,
            "Expected approx 100, got {card}"
        );
    }

    #[test]
    fn test_kll_quantile_query() {
        // Assuming KLL has a default implementation
        let mut counter = HydraCounter::KLL(KLL::default());

        // Insert numbers 1 to 100
        for i in 1..=100 {
            counter.insert(&DataInput::F64(i as f64), None);
        }

        // Query Median (0.5)
        let result = counter.query(&HydraQuery::Quantile(0.5));
        assert!(result.is_ok());

        // Median of 1..100 is approx 50
        let median = result.unwrap();
        assert!(
            (median - 50.0).abs() < 5.0,
            "Expected approx 50, got {median}"
        );
    }

    #[test]
    fn test_univmon_universal_queries() {
        let mut counter = univmon_counter();

        // Insert distribution:
        // Item "A": 10 times
        // Item "B": 20 times
        let key_a = DataInput::Str("A");
        let key_b = DataInput::Str("B");

        for _ in 0..10 {
            counter.insert(&key_a, None);
        }
        for _ in 0..20 {
            counter.insert(&key_b, None);
        }

        // 1. Test L1 Norm (Total Sum of Weights)
        // Should be 10 + 20 = 30
        let l1 = counter.query(&HydraQuery::L1Norm).unwrap();
        assert_eq!(l1, 30.0);

        // 2. Test Cardinality
        // Should be 2 ("A" and "B")
        let card = counter.query(&HydraQuery::Cardinality).unwrap();
        assert!((card - 2.0).abs() < 0.5, "Cardinality should be approx 2");

        // 3. Test Entropy
        // UnivMon calculates entropy, should be > 0 for this distribution
        let entropy = counter.query(&HydraQuery::Entropy).unwrap();
        assert!(entropy > 0.0);
    }

    #[test]
    fn test_merge_counters() {
        // Test merging two CountMin sketches via the Hydra wrapper
        let mut c1 = cm_counter();
        let mut c2 = cm_counter();

        c1.insert(&DataInput::I64(1), None);
        c2.insert(&DataInput::I64(1), None);

        // Valid merge
        assert!(c1.merge(&c2).is_ok());

        let count = c1.query(&HydraQuery::Frequency(DataInput::I64(1))).unwrap();
        assert_eq!(count, 2.0, "Merge should sum the counts");

        // Invalid merge (Different types)
        let hll = HydraCounter::HLL(HyperLogLog::<ErtlMLE>::default());
        assert!(c1.merge(&hll).is_err());
    }

    #[test]
    fn test_count_frequency_query() {
        let mut counter = count_counter();
        let key = DataInput::I64(7);

        for _ in 0..4 {
            counter.insert(&key, None);
        }

        let query = HydraQuery::Frequency(key);
        let result = counter.query(&query);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            4.0,
            "Count Sketch should track all inserts"
        );
    }

    #[test]
    fn test_count_invalid_query_types() {
        let counter = count_counter();

        let quantile = counter.query(&HydraQuery::Quantile(0.5));
        assert!(quantile.is_err());
        assert_eq!(
            quantile.unwrap_err(),
            "Count Sketch Counter does not support Quantile Query"
        );

        let cardinality = counter.query(&HydraQuery::Cardinality);
        assert!(cardinality.is_err());
    }
}
