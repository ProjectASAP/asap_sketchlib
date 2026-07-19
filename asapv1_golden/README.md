# ASAPv1 cross-language golden byte-vectors

These `.hex` files pin the exact bytes the ASAPv1 wire format (see
`docs/asapv1_wire_format.md`) emits for a set of fixed, known sketch states. They
are the machine-checked proof that the Rust (`asap_sketchlib`) and Go
(`sketchlib-go`) implementations serialize **byte-identically**.

**The copy here and the copy in `sketchlib-go/asapv1_golden/` MUST stay
byte-identical.** They are the same fixtures, checked into both repos so each
side's test suite is self-contained. The bytes are authored by the Rust side
(rmp_serde is the reference encoder); Go conforms to them, never the reverse.

Each file is one line of lowercase hex (no `0x`, no whitespace) = the complete
ASAPv1 envelope `[ magic | version | kind_id | metadata_len | payload_len |
metadata | payload ]`.

## Design principle: state is fixed, not hashed

Every fixture is built from a **known raw sketch state** (specific register
bytes / matrix values set directly), never by hashing input values. So the
golden tests the **wire encoding**, isolated from the hash functions.

## Fixtures

| File | Sketch | kind_id | State |
| ---- | ------ | ------- | ----- |
| `hll_classic_p12` | HLL Classic, P12 | `01 01` | 4096 registers, set: `[0]=1, [1]=7, [100]=42, [4095]=3` |
| `hll_ertl_mle_p12` | HLL Ertl-MLE, P12 | `01 02` | same register pattern |
| `hll_hip_p12` | HLL HIP, P12 | `01 03` | same registers + `hip_kxq0=1.5, hip_kxq1=2.5, hip_est=3.0` |
| `cms_i64_regular_2x3` | Count-Min i64, RegularPath | `02 00` | 2×3 row-major `[[0,1,127],[128,300,65536]]` |
| `cms_f64_fast_2x3` | Count-Min f64, FastPath | `02 00` | 2×3 row-major `[[0.0,1.5,2.25],[3.75,4.125,5.0625]]` |
| `kll_f64_k200` | KLL f64, k=200 | `06 00` | integers `1..=50`, compaction seed 42 |
| `kll_i64_k200` | KLL i64, k=200 | `06 00` | integers `1..=50`, compaction seed 42 |

The i64 fixture deliberately spans the msgpack integer width boundaries
(positive fixint / uint8 / uint16 / uint32) to lock the "non-negative integer →
uint family, minimal width" rule (`docs/asapv1_wire_format.md` §5).

The KLL fixtures are a special case of "state is fixed, not hashed": KLL never
hashes — it orders raw numeric values — so inserting `1..=50` places exactly
those retained samples. `k=200` keeps the input below the level-0 capacity, so no
compaction fires (`num_levels = 1`, one level `[1..50]`) and the state is fully
deterministic. The fixed compaction seed (42) pins the carried coin state, which
must match `sketchlib-go`'s coin for the same input. Only the compact KLL
(`06 00`) has a golden; the dynamic variant (`06 01`) shares the payload shape but
lacks a seeded constructor, so its cross-language golden is deferred.

## Tests that consume these

- Rust: `tests/asapv1_golden.rs` — builds each fixture from known state,
  serializes, asserts `== golden`; and asserts `deserialize(golden)` round-trips.
- Go: `wire/asapmsgpack/golden_test.go` — `Unmarshal(golden)`→re-`Marshal` ==
  golden, **and** `Marshal(equivalent known state) == golden` (the cross-language
  parity proof).

## Regenerating

If the wire format intentionally changes, regenerate from the Rust side (the
reference encoder) and copy the files into both repos. Both test suites must then
pass unchanged.
