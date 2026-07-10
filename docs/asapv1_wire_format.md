# ASAPv1 Wire Format — Design Doc

Status: **implemented (Rust)**. HLL and Count-Min serialization use the shared
`message_pack_format::envelope` module per this spec; the byte-level encoding is
pinned in "Wire encoding rules" and decisions are summarized at the bottom. The
hash-spec metadata is **derived from the hasher's `HashProfile`** (not hardcoded),
so the bytes truthfully describe how the sketch was hashed and custom hash
profiles are supported (see Section 2). The `sketchlib-go` side is not yet updated
(see Cross-language contract).

## Guiding principle

The envelope carries `kind_id` and `metadata` **before** the payload, so by the
time the decoder reaches the payload it already knows both — and together they
fix the payload's structure completely. That gives a clean three-way split:

- **kind_id** = the sketch's **algorithm identity** (which decoder + which
  estimator): HLL-Classic, HLL-Ertl-MLE, HLL-HIP, Count-Min. The coarse dispatch
  key. It does *not* carry parameters.
- **Metadata** = the **descriptor**: how it was hashed (seeds / algorithm) *plus*
  the structural parameters needed to interpret the payload (HLL precision, CMS
  counter type, CMS column-derivation mode). Self-describing (msgpack map).
- **Payload** = the **raw state** only (registers, matrix): a positional msgpack
  array parameterized by kind_id + metadata. No field names, no tag the kind_id
  or metadata already carries, no derived quantities.

If a payload looks complicated, either the sketch genuinely has that much state,
or something derivable/redundant leaked in and should be removed.

## Layering

| Layer | Scope | Self-describing? | Owner | Changes when |
| ------- | ------- | ------------------ | ------- | -------------- |
| **Envelope** | frame | yes | one shared module | the framing changes (rare) |
| **Metadata** | descriptor (hash spec + structural params) | yes | one shared module | the hash profile or a sketch's params change |
| **Payload** | one per sketch | **no** | each sketch | that sketch's raw encoding changes |

Envelope and Metadata are **not** per-sketch — they live in one shared module
every sketch calls into. Only the **Payload** is authored per sketch. Today's
code duplicates the envelope into each sketch file; this doc exists to undo that.

```md
┌───────────────────────────────┐
│ Envelope | Metadata | Payload │
└───────────────────────────────┘
```

---

## Section 1 — Envelope

A flat, sketch-agnostic frame. It answers, with zero knowledge of the sketch:
*is this ours?* (magic), *how do I parse the frame?* (version), *what algorithm?*
(kind_id). The envelope is essentially **constant** across sketches — only
`kind_id` and the two length fields differ.

### Layout

```md
[ magic:6 | version:u8 | kind_id_len:u8 | kind_id:bytes
          | metadata_len:u32_be | payload_len:u32_be
          | metadata:msgpack | payload:msgpack ]
```

| Field | Type | Value / range | Notes |
| ------- | ------ | --------------- | ------- |
| `magic` | 6 bytes | `41 53 41 50 76 31` = `b"ASAPv1"` | fixed sentinel |
| `version` | u8 | `0x01` | envelope layout version; this doc = `0x01` |
| `kind_id_len` | u8 | `2` today (≤255) | length of `kind_id` |
| `kind_id` | bytes | see registry | which algorithm |
| `metadata_len` | u32 be | varies | byte length of the metadata block |
| `payload_len` | u32 be | varies | byte length of the payload |
| `metadata` | msgpack map | — | Section 2 |
| `payload` | msgpack array | — | Section 3 |

**`payload_len`** makes the envelope a self-delimiting record (needed to ever
place a sketch inside a larger container). `metadata_len` is variable only because
the metadata is a variable-length msgpack map (Section 2), not because it depends
on the sketch — the length fields are pure framing.

### The `kind_id` scheme

`kind_id` is `[family, variant]` and names the sketch's **algorithm**, not its
parameters:

- **family** (byte 1) picks the sketch type — `0x01` = HLL, `0x02` = Count-Min, …
- **variant** (byte 2) picks the algorithm within that family — for HLL, Classic
  vs Ertl-MLE vs HIP.

Structural parameters (HLL precision, CMS counter type, CMS mode) are **not** in
`kind_id` — they live in the metadata, which the decoder has already read before
it reaches the payload. So the payload structure is fixed by `kind_id` + metadata
together. The registry below is our **master list of algorithms we still have to
design payloads for**.

### kind_id registry (single source of truth — mirrored verbatim in `sketchlib-go`)

The **family** bytes below now match `sketchlib-go`'s
`wire/asapmsgpack/magic_ids.go` verbatim. An earlier draft of this doc used
*speculative* family bytes (`0x03` KLL, `0x04` DDSketch, `0x05` KMV, `0x06`
CountSketch) that conflicted with the ids Go had already committed to; those have
been **corrected to align with Go** (`0x03` = Count-Min-with-heap, `0x04` =
Count-Sketch, `0x05` = DDSketch, `0x06` = KLL). Family bytes `0x0a`+ are new
allocations for the remaining sketches in [`apis.md`](./apis.md) that Go has not
assigned yet.

Only the HLL variants and Count-Min have designed payloads today; every other row
lists the family byte with variant `0x00` reserved and payload **TBD**. Variant
sub-ids are **not** invented ahead of a payload design — a family that later needs
several algorithms allocates its variants when it is designed (as HLL did).

| kind_id | Sketch | Algorithm / variant | Payload | Status |
| --------- | -------- | --------- | --------- | -------- |
| `0x01 0x00` | HLL | Unspecified | — | reserved |
| `0x01 0x01` | HLL | Classic ("Regular") | §3.1 | implemented |
| `0x01 0x02` | HLL | Ertl-MLE ("Datafusion") | §3.1 | implemented |
| `0x01 0x03` | HLL | HIP | §3.1 | implemented |
| `0x02 0x00` | Count-Min | Count-Min | §3.2 | implemented |
| `0x03 0x00` | Count-Min-with-heap (CMSHeap) | — | TBD | assigned in Go / payload not designed |
| `0x04 0x00` | Count Sketch | — | TBD | assigned in Go / payload not designed |
| `0x05 0x00` | DDSketch | — | TBD | assigned in Go / payload not designed |
| `0x06 0x00` | KLL | — | TBD | assigned in Go / payload not designed |
| `0x07 0x00` | Hydra-KLL | — | TBD | assigned in Go / payload not designed |
| `0x08 0x00` | SetAggregator | — | TBD | assigned in Go / payload not designed |
| `0x09 0x00` | DeltaResult | — | TBD | assigned in Go / payload not designed |
| `0x0a 0x00` | Count-Sketch-with-heap (CSHeap) | — | TBD | reserved / not designed |
| `0x0b 0x00` | Elastic (`Unstable`) | — | TBD | reserved / not designed |
| `0x0c 0x00` | Coco (`Unstable`) | — | TBD | reserved / not designed |
| `0x0d 0x00` | UniformSampling (`Unstable`) | — | TBD | reserved / not designed |
| `0x0e 0x00` | KMV (`Unstable`) | — | TBD | reserved / not designed |
| `0x0f 0x00` | HashSketchEnsemble | — | TBD | reserved / not designed |
| `0x10 0x00` | UnivMon | — | TBD | reserved / not designed |
| `0x11 0x00` | UnivMon Optimized | — | TBD | reserved / not designed |
| `0x12 0x00` | NitroBatch | — | TBD | reserved / not designed |
| `0x13 0x00` | ExponentialHistogram | — | TBD | reserved / not designed |
| `0x14 0x00` | EHSketchList | — | TBD | reserved / not designed |
| `0x15 0x00` | EHUnivOptimized (`Unstable`) | — | TBD | reserved / not designed |
| `0x16 0x00` | OctoSketch | — | TBD | reserved / not designed |

Count-Min is **one** kind_id: its counter type (i64/f64) and mode (fast/regular)
are metadata, not separate ids. Classic and Ertl-MLE have byte-identical payloads
but are separate ids because `kind_id` also selects the *estimator* to apply.

**Mapping notes** (where `apis.md` and Go's `magic_ids.go` don't line up 1:1):

- **CMSHeap vs CSHeap.** Go's `MagicCountMinSketchWithHeap` (`0x03`) is the
  Count-*Min*-with-heap sketch (`apis.md` → CMSHeap). The Count-*Sketch*-with-heap
  sketch (`apis.md` → CSHeap) is a distinct family and gets a fresh byte (`0x0a`);
  it is **not** a variant of `0x03`.
- **Hydra.** `apis.md` lists the "Hydra" framework; Go's only Hydra id is
  `MagicHydraKLLSketch` (`0x07`), so Hydra maps here to the Hydra-KLL id. If Hydra
  is later wrapped around a non-KLL base sketch, that combination gets its own id.
- **SetAggregator / DeltaResult** (`0x08` / `0x09`) come from Go's `magic_ids.go`
  and are **not** listed as sketches in `apis.md` (they are aggregation / delta
  result envelopes, not stand-alone sketches). They are kept here so the family
  space stays mirrored verbatim with Go.
- **`Unstable`** rows mirror the `Unstable` status those sketches carry in
  `apis.md`; their kind_id is reserved but the payload (and the sketch API) may
  still change.

**Allocation rules:**

- `kind_id` is **variable-length** (`kind_id_len` is a u8), so the id space is
  effectively unbounded — it can keep growing forever; we will never run out.
- A `kind_id` is **allocated once and never recycled.** When an algorithm is
  retired, its id stays reserved permanently — reusing a retired number would
  make a new decoder silently misread old bytes.
- A **new incompatible payload encoding gets a new `kind_id`**, not a version
  field inside the payload (Q-VER — versioning lives in the id, keeping payloads
  minimal).

### Decoder rules

1. `len >= 6+1+1+0+4+4` before reading anything.
2. `magic` matches, else reject.
3. `version` is known, else reject (no best-effort parse).
4. Read `kind_id`; the per-sketch decoder rejects any `kind_id` it does not own.
5. Read `metadata`, validate per Section 2.
6. Cross-check metadata against `kind_id` and the payload (structural params
   consistent — see Section 2 validation).
7. Read exactly `payload_len` bytes; hand to the per-sketch payload decoder.
8. Fail **closed** on any inconsistency — never merge/query a sketch whose hash
   spec did not validate.

> Implementation note: the shared envelope module
> (`src/message_pack_format/envelope.rs`) owns rules 1–3 and the byte framing
> (`encode` / `split`); it is sketch-agnostic and does **not** know the registry.
> Rule 4 (and metadata/kind_id validation) happens in each sketch's decoder,
> which checks the `kind_id` against the ones it owns.

---

## Section 2 — Metadata

The **descriptor**: everything the decoder needs to interpret and merge the
payload beyond the algorithm named by `kind_id`. Two groups of fields:

- **Hash spec** — how keys were hashed (so two sketches can be checked
  mergeable and a query key hashed the same way). Profile-derived.
- **Structural params** — parameters that shape the payload (HLL precision, CMS
  counter type, CMS mode). Per-sketch, per-algorithm.

### Encoding: msgpack **map** keyed by field name

Metadata is a **msgpack map**. A map is self-describing — a consumer reads
`"hash_profile_id"` without knowing the schema, unknown keys are skippable, and
**optional / not-applicable fields are just omitted keys** (no null placeholders).
That "omit the key" property is what lets each sketch carry only what it uses.

Two consequences:

1. **`seed_list` is inlined.** The full 20-seed list is carried in every sketch's
   metadata, so the bytes are **self-describing** — a consumer can read the exact
   seeds (and algorithm) straight from the binary without any registry. It costs
   ~130 bytes; the alternative (carry only `hash_profile_id` and resolve the seeds
   from a registry) is a v2 space optimization once many sketches share one spec.
   `deny_unknown_fields` still rejects any key beyond the fixed set, so v1 accepts
   exactly that field set (the values may be the standard profile or a custom
   `HashProfile` — see "Custom hash profiles").
2. **Each sketch carries only the fields it uses.** HLL includes
   `canonical_seed_index` and `precision`; Count-Min includes `matrix_seed_index`,
   `counter_type`, `mode`. Nobody carries fields for seed roles or params they
   don't use.

### Fields

**Hash spec**

| Key | Type | Required | Meaning |
| ------- | ------ | -------- | --------- |
| `metadata_version` | u8 | yes | schema version of *this block* (`1`). Independent of envelope `version`. |
| `hash_profile_id` | string | yes | stable global id, `"projectasap.xxh3.seedlist.v1"` — authoritative |
| `hash_algorithm` | string | yes | `"xxh3_64_128"` |
| `seed_derivation` | string | yes | `"seed_list_index_wrap"` |
| `input_encoding` | string | yes | `"projectasap.input.v1"` |
| `seed_list` | `array<u64>` | **yes (inlined)** | the 20 seeds, carried inline so the bytes self-describe the hash |
| `canonical_seed_index` | u32 | **per-sketch** | index into `seed_list` (`5`); HLL uses it |
| `matrix_seed_index` | u32 | **per-sketch** | `0`; Count-Min uses it |
| `hydra_seed_index` | u32 | **per-sketch** | `6`; include only if used |
| `univmon_bottom_layer_seed_index` | u32 | **per-sketch** | `19`; include only if used |

**Structural params**

| Key | Type | Applies to | Meaning |
| ------- | ------ | -------- | --------- |
| `precision` | u8 | HLL | `12` / `14` / `16`; register count = `2^precision` |
| `counter_type` | string | Count-Min | `"i64"` or `"f64"` — element type of `counts` |
| `mode` | string | Count-Min | `"fast"` or `"regular"` — key→column derivation |

### Standard ProjectASAP profile (reference values)

The hash-spec field *values* are sourced from the hasher's `HashProfile`, not
hardcoded — `hll_metadata::<H>` / `cms_metadata::<H>` read `PROFILE_ID`,
`ALGORITHM`, `SEED_DERIVATION`, `INPUT_ENCODING`, `seed_list()`, and the seed
index straight off `H`. The block below is the **standard profile**, the one
`DefaultXxHasher` declares (the single source of truth for these values); it is
also what the registry resolves `hash_profile_id` to. A single sketch's metadata
carries `hash_profile_id` plus only the subset of indices/params it uses.

```md
metadata_version = 1
hash_profile_id  = "projectasap.xxh3.seedlist.v1"
hash_algorithm   = "xxh3_64_128"
seed_list        = [0xcafe3553, 0xade3415118, 0x8cc70208, 0x2f024b2b, 0x451a3df5,
                    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f,
                    0x9b05688c, 0x1f83d9ab, 0x5be0cd19, 0xcbbb9d5d, 0x629a292a,
                    0x9159015a, 0x152fecd8, 0x67332667, 0x8eb44a87, 0xdb0c2e0d]
canonical_seed_index            = 5
matrix_seed_index               = 0
hydra_seed_index                = 6
univmon_bottom_layer_seed_index = 19
seed_derivation  = "seed_list_index_wrap"
input_encoding   = "projectasap.input.v1"
```

### Custom hash profiles

Because the metadata is `HashProfile`-derived, a hasher that declares its own
profile (a different `PROFILE_ID` / `seed_list()` / seed index) serializes
**truthfully** — its own values land in the metadata. Since `seed_list` is
inlined, those bytes are **fully self-describing**: a consumer reads the exact
seeds and algorithm straight from the binary, with no registry, even for a hash
it has never seen. This is safe on both ends because serialization **fails
closed**:

- **Encode side (compile-time).** `serialize_to_bytes` is bounded on
  `H: HashProfile`, so a hasher that does *not* declare a profile simply cannot
  serialize — mislabeled bytes are impossible by construction.
- **Decode side (runtime).** Decode validates the metadata against the *target*
  type's `HashProfile` (`meta == hll_metadata::<H>(precision)` /
  `cms_metadata::<H>(..)`), so bytes hashed under profile A cannot be decoded into
  a profile-B–typed sketch — they are rejected.
- **Merge.** Merge compatibility is hash-spec equality (same `hash_profile_id` +
  seeds). A custom-profile sketch is **not** mergeable with a standard-profile one.

### Validation

Fail **closed** on any mismatch (a wrong hash spec produces silently-wrong merges,
worse than a hard error):

1. `kind_id` is in the registry.
2. Every hash-spec field matches the **target hasher's** `HashProfile` (exact
   equality — decode compares the read metadata against `hll_metadata::<H>` /
   `cms_metadata::<H>` for the type being decoded into, not merely "the standard
   profile"). Bytes carrying a different profile are rejected.
3. Structural params are consistent with `kind_id` and the payload:
   - HLL: `registers.len() == 2^precision ==` the target storage's register count.
   - Count-Min: `counts` element type matches `counter_type`; `counts.len() == rows*cols`.

---

## Section 3 — Payload

Per sketch. **Raw state only**, a **positional msgpack array** in the order its
kind_id implies. Rules:

- No field that `kind_id` or the metadata already determines (no variant tag, no
  precision, no counter type, no mode).
- No field derivable from another (no HLL `precision`; no CMS `l1`/`l2` — those
  are `Σ count` / `Σ count²`, recomputed on decode).
- msgpack array (positional), never a keyed map. The exact msgpack types are in
  "Wire encoding rules".

> Note: derived summaries like CMS `l1`/`l2` and `sum_counts`/`sum2_counts` live
> in the **delta / error-accounting** format (proto `CountMinState`), a separate
> wire format. They do **not** belong in the self-contained sketch payload.

### 3.1 — HLL payload (`0x01 0x01` / `0x01 0x02` / `0x01 0x03`)

The variant is in `kind_id`, precision is in the metadata (and equals
`log2(register count)`), so the only real state is the register bytes — plus, for
HIP, three running scalars.

**Classic / Ertl-MLE** (`0x01 0x01`, `0x01 0x02`) — identical layout:

| Pos | Field | Type | Notes |
| ----- | ------- | ------ | ------- |
| 0 | `registers` | bin | one byte per register; length is `2^precision` |

**HIP** (`0x01 0x03`):

| Pos | Field | Type | Notes |
| ----- | ------- | ------ | ------- |
| 0 | `registers` | bin | one byte per register |
| 1 | `hip_kxq0` | f64 | HIP running estimate state |
| 2 | `hip_kxq1` | f64 | |
| 3 | `hip_est` | f64 | |

### 3.2 — Count-Min payload (`0x02 0x00`)

The `CountMin` struct is generic in memory (counter `i32`/`i64`/`i128`/`f64`,
`RegularPath`/`FastPath`, Nitro, …). **That freedom is kept in memory; nothing is
forbidden.** The wire supports a fixed set, and the two parameters that shape it —
**counter type** (`"i64"`/`"f64"`) and **mode** (`"fast"`/`"regular"`) — live in
the metadata, so the payload itself is just shape + counters:

| Pos | Field | Type | Notes |
| ----- | ------- | ------ | ------- |
| 0 | `rows` | u32 | matrix depth |
| 1 | `cols` | u32 | matrix width |
| 2 | `counts` | array | packed **row-major**, `rows*cols` cells; element type = `counter_type` |

Wire counter types are `i64` and `f64` only (`i32` widens to `i64`; `i128` and
exotic counters are not wire types). `mode` records `RegularPath` vs `FastPath`
because they place a key in different columns (compare `cm_regular_path_correctness`
vs `cm_fast_path_correctness`), so a reader must know which to reproduce a query.

#### Converting an exotic in-memory sketch to a wire form (user-side)

The library provides no free wire serialization for exotic counters — only the
owner knows if the mapping is lossless. Convert to a canonical counter type, then
serialize. Doable **today** with existing public API (the pattern `SketchlibCms`
already uses):

```rust
// e.g. a u64-counter FastPath CMS → the i64 wire form
let (rows, cols) = (src.rows(), src.cols());
let converted: CountMin<Vector2D<i64>, FastPath> = CountMin::from_storage(
    Vector2D::from_fn(rows, cols, |r, c| src.as_storage().query_one_counter(r, c) as i64),
);
let bytes = converted.serialize_to_bytes()?; // wire-eligible type
```

Converts the **counter type** only (cell-for-cell). It does **not** convert the
mode (Regular↔Fast) — that would need re-inserting the original data.

#### Rust-side changes (as implemented)

- Removed `serialize_to_bytes`/`deserialize_from_bytes` from the blanket
  `impl<S: MatrixStorage + Serialize>` — no "serialize anything" surface. They now
  exist only on `CountMin<Vector2D<T>, Mode, H>` for wire-eligible `T`/`Mode`.
- Two marker traits carry the structural params into the metadata:
  `CmsWireCounter` (`i64` → `"i64"`, `f64` → `"f64"`) and `CmsWireMode`
  (`FastPath` → `"fast"`, `RegularPath` → `"regular"`). The native
  `MessagePackCodec` impl is narrowed to the same bounds.
- The `(rows, cols, counts)` payload is a `CmsPayload<T>` struct serialized with
  `rmp_serde::to_vec` (positional array); `rows`/`cols` come from the storage at
  encode time (the struct's redundant `row`/`col` fields are not serialized).
- The envelope framing + hash-profile constants are the shared
  `message_pack_format::envelope` module (same one HLL uses).

#### Go-side TODOs (tradeoffs)

- Implement whichever `mode` derivations it must read (FastPath at least),
  bit-for-bit with Rust.
- Support i64 and f64 wire counter types. int64-only vs adding f64 is the
  precision-vs-simplicity tradeoff.
- No need for i128 / Nitro — not wire types.

---

## Section 4 — Wire coverage

The in-memory sketch types are deliberately **freer** than what the wire
serializes. That is the same framing as §3.2 — *in-memory is free; the wire is a
small fixed set* — stated once, in full, so the coverage decision is explicit and
so a user knows exactly **what to implement (or convert)** when they want
something the wire does not cover out of the box.

A config is **wire-eligible** iff `serialize_to_bytes` is defined for it. That is
enforced by the trait bounds on each sketch's serialization impl — nothing else
is a wire type, and the compiler says so.

### 4.1 — HLL coverage

HLL has three degrees of freedom, and the wire covers the full cross product:

| Freedom | In-memory choices | Wire-eligible |
| --------- | ------------------- | --------------- |
| estimator variant | `Classic`, `ErtlMLE`, HIP | **all three** — each maps to its own `kind_id` via `HllWireVariant` (`Classic` → `0x01 0x01`, `ErtlMLE` → `0x01 0x02`) or `HLL_KIND_HIP` (`0x01 0x03`) |
| precision | `HllBucketListP12` / `P14` / `P16` | **all three** — carried as `precision` (`12`/`14`/`16`) in the metadata |
| hasher `H` | any `H: SketchHasher` | **any `H: HashProfile`** — the metadata is `hll_metadata::<H>` (profile-derived, self-describing) |

`HyperLogLogImpl<Variant, Registers, H>::serialize_to_bytes` is bounded on
`Variant: HllWireVariant, H: HashProfile`, so every (variant × precision × hasher)
combination serializes. **HLL is fully covered** — there is no in-memory HLL shape
that the wire cannot represent.

One nuance for accuracy: the HIP estimator is a standalone struct
(`HyperLogLogHIPImpl<Registers>`) that is **not** parameterized by a hasher — it
hashes through the `DefaultXxHasher` free functions and its
`serialize_to_bytes` uses `standard_hll_metadata`. So HIP is wire-eligible only
under the **standard profile**; the custom-hasher freedom above applies to the
`Classic` / `ErtlMLE` family.

### 4.2 — Count-Min coverage

`CountMin<S, Mode, H>` is generic in memory over counter type, storage, mode, and
hasher, and also supports Nitro sampling and delta emission. The wire supports a
**fixed subset** — the serialization impl exists only for
`CountMin<Vector2D<T>, Mode, H>` where `T: CmsWireCounter`, `Mode: CmsWireMode`,
`H: HashProfile`:

| Freedom | In-memory choices | Wire-eligible? |
| --------- | ------------------- | ---------------- |
| counter type | `i32`, `i64`, `i128`, `f64`, … | `i64` ✓, `f64` ✓ (`CmsWireCounter`); `i32` / `i128` / other ✗ — convert first |
| mode | `RegularPath`, `FastPath` | `RegularPath` ✓ (`"regular"`), `FastPath` ✓ (`"fast"`) (`CmsWireMode`) |
| storage `S` | `Vector2D<T>`, `FixedMatrix`, `DefaultMatrixI32/I64/I128`, `QuickMatrixI64/I128` | `Vector2D<i64>` / `Vector2D<f64>` ✓; `FixedMatrix` / `DefaultMatrix*` / `QuickMatrix*` ✗ — rebuild into `Vector2D` |
| hasher `H` | any `H: SketchHasher` | any `H: HashProfile` ✓; a hasher without `HashProfile` ✗ (compile error, by design) |
| Nitro / delta emission | `enable_nitro`, `insert_emit_delta` (i32-only) | **n/a** — in-memory-only machinery, never part of the sketch wire payload |

Note the **default** `CountMin` storage is `Vector2D<i32>` (see the struct
default), which is **not** wire-eligible: a `CountMin::default()` must be built as
(or converted to) `Vector2D<i64>` / `Vector2D<f64>` before it can serialize. Two
sketches that differ only in `mode` place a key in different columns (compare
`cm_regular_path_correctness` vs `cm_fast_path_correctness`), which is why `mode`
is recorded rather than assumed.

### 4.3 — "If you want X, do Y"

The actionable part. Each row is a config the wire does not cover directly and the
concrete step that makes it serializable.

| You have | Do this |
| ---------- | --------- |
| An **exotic counter type** (e.g. `u64`, `i128`) | Convert cell-by-cell to `i64` or `f64` and serialize the result — the exact `Vector2D::from_fn` + `CountMin::from_storage` recipe in §3.2. Only you know if the mapping is lossless. **Or**, if it deserves to be a first-class wire type, implement `CmsWireCounter` for it (giving its `COUNTER_TYPE` string) **and** add the matching Go decode support **and** check in a golden byte-vector — do all three, not just the Rust trait. |
| **Non-`Vector2D` storage** (`FixedMatrix` / `DefaultMatrix*` / `QuickMatrix*`) | Rebuild it into a `Vector2D<i64>` / `Vector2D<f64>` via `CountMin::from_storage(Vector2D::from_fn(rows, cols, |r, c| src.as_storage().query_one_counter(r, c) as i64))`, then serialize. |
| A **custom hash function** | Implement `HashProfile` for the hasher (a distinct `PROFILE_ID`, its own `seed_list()` and seed index). It then serializes **truthfully** and, because `seed_list` is inlined, **self-describes** on the wire — no registry needed to read it (see §2, "Custom hash profiles"). |
| An **unprofiled hasher** (impls `SketchHasher` but not `HashProfile`) | Nothing — it **cannot** serialize, and that is a **compile error by design** (`serialize_to_bytes` is bounded on `H: HashProfile`). This is the intended fail-closed behavior — mislabeled bytes are impossible by construction — not a bug to work around. |
| A **brand-new sketch algorithm** | Allocate a fresh `kind_id` in the registry (§1 — allocated once, never recycled), define its metadata fields (§2), and author its payload (§3). Mirror the `kind_id` and add goldens on the Go side. |

### 4.4 — Why the wire is a small fixed set

Every wire-eligible config is a **cross-language surface** (Go must decode it
bit-for-bit) plus a **golden byte-vector** to maintain forever. Keeping that set
small and fixed keeps both bounded — a handful of counter types and modes instead
of the open-ended in-memory matrix. The in-memory types stay maximally free for
performance and experimentation; serialization is the deliberately narrow gate
where that freedom is pinned down to a canonical, portable form.

---

## Section 5 — Wire encoding rules (byte-level)

This is what makes two languages emit **identical bytes**. msgpack fixes
endianness and float format; these rules fix the family/width choices that
libraries otherwise make differently.

**Integer family + width rule (applies to every integer below).** This is the
single biggest cross-language trap — some Go msgpack libraries emit a *signed*
`int` family for a positive `int64` while Rust's `rmp_serde` narrows it to the
`uint` family. Pin it:

- A **non-negative** integer is encoded in the msgpack **uint** family, at the
  **minimal width** for its value (e.g. `300` → `cd 01 2c`, uint16; `1` →
  positive fixint `01`).
- A **negative** integer is encoded in the msgpack **int** family, minimal width.
- `f64` is always full **float64** (`0xcb`), never narrowed to float32.

The Go side MUST configure its encoder to match (uint-narrowing on, minimal
width). Golden byte-vectors lock it.

**Metadata (msgpack map)**

- Keys are the exact ASCII strings in Section 2.
- **Canonical key order** = the order fields are listed in Section 2 (hash-spec
  group, then structural-params group). Encoders MUST write in this order.
  (Order is irrelevant to decoding but required for byte-identical output.)
- Decoders reject **unknown keys** (Rust uses `#[serde(deny_unknown_fields)]`) —
  v1 carries exactly the fixed field set (its values are the hasher's
  `HashProfile`: the standard profile or a custom one).
- Values: strings as msgpack `str`; `seed_list` as a msgpack array of integers
  (each per the family/width rule); all other integers per the family/width rule.

**Payload (msgpack array)**

- A msgpack **array**, elements in the Section 3 position order.
- `registers` → msgpack `bin` (one byte per register; matches Go's `[]byte`).
- `rows` / `cols` → integers per the family/width rule.
- `counts` → msgpack array; each element is an integer (per the family/width
  rule) when `counter_type == "i64"`, a **float64** when `"f64"`.
- HLL HIP `hip_*` → **float64**.

Golden byte-vectors lock all of the above; any encoder that deviates fails them.

---

## Cross-language contract

Direction: **custom per-sketch payload replaces the `portable` types, and
`sketchlib-go` mirrors each payload.** Good direction (more compact, higher
fidelity, less Rust-internal duplication), but it moves the contract from shared
code to discipline. To keep it safe:

1. **This spec** — byte-level, language-neutral, per sketch.
2. **Golden byte-vector fixtures** checked into both repos; both languages
   decode→re-encode them byte-identically. These replace the `portable`-as-oracle
   round-trip test that guards drift today.
3. **This registry**, mirrored, never independently allocated.

**Hash profile on the Go side.** Rust derives the hash spec from a generic
`HashProfile` bound on the hasher type; Go has no generic hasher type, so there is
nothing to derive from. On the Go side the profile is simply **written into** the
metadata on encode and **read from** it on decode. Go MUST validate the profile it
reads (same fail-closed intent as Rust): a sketch is only mergeable/queryable if
its `hash_profile_id` + seeds match the profile Go is prepared to reproduce.
Because `seed_list` is inlined, Go can read a custom profile's seeds without any
registry, but it must still reject a profile it cannot reproduce rather than
merge/query under the wrong hash.

Sequencing: do **not** delete `portable` until (2) exists — the current
`native bytes == portable bytes` test is the only drift guard right now. Keep it
through the transition, retire `portable` once goldens are in place.

---

## Decisions (resolved)

- **kind_id = algorithm identity**, not parameters. Structural params (HLL
  precision, CMS counter type + mode) live in metadata, which is read before the
  payload. Payload structure = kind_id + metadata.
- **Q-META** — metadata is a msgpack **map**; canonical key order per Section 5;
  optional fields are omitted keys.
- **Q-SEEDS** — `seed_list` is **inlined** in v1 so the bytes self-describe the
  hash (a consumer needs no registry to read the seeds). Resolving seeds from
  `hash_profile_id` alone is a v2 space optimization. Each sketch still carries
  only the seed *index* it uses.
- **Q-PROFILE** — the hash-spec metadata is **derived from the hasher's
  `HashProfile`** (`hll_metadata::<H>` / `cms_metadata::<H>`), not hardcoded, so it
  is always truthful to the hasher. Custom hash profiles are **supported and
  self-describing**. Fail-closed on both ends: `serialize_to_bytes` requires
  `H: HashProfile` (an unprofiled hasher can't serialize — compile-time), and
  decode validates the metadata against the *target* type's profile (profile-A
  bytes won't decode into a profile-B sketch). Merge compatibility is hash-spec
  equality, so a custom-profile sketch is not mergeable with a standard one.
- **Q-CMS** — Count-Min is one `kind_id` (`0x02 0x00`); counter type and mode are
  metadata, not the id.
- **Q-VER** — no payload version field. A new incompatible encoding gets a **new
  `kind_id`**; retired ids are reserved forever and never recycled.
- **Encoding** — metadata + payload are both msgpack; payload is a positional
  array. Byte-level rules in Section 5.
