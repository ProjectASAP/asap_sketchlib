# Message Pack Format

`src/message_pack_format/` holds the Rust-side serialization plumbing. The
current, self-describing wire format is **ASAPv1**; its byte-level layout is the
single source of truth in
[**`asapv1_wire_format.md`**](./asapv1_wire_format.md) — read that for the
authoritative spec. This page describes how the code is organized.

## The ASAPv1 model

Every serialized sketch is one self-delimiting envelope:

```md
[ magic:6 | version:u8 | kind_id_len:u8 | kind_id:bytes
          | metadata_len:u32_be | payload_len:u32_be
          | metadata:msgpack-map | payload:msgpack-array ]
```

- **Envelope** — the sketch-agnostic frame (magic `b"ASAPv1"`, version,
  `kind_id`, and the two length prefixes). It answers *is this ours?*, *how do I
  parse the frame?*, and *what algorithm?* with zero knowledge of any sketch.
- **Metadata** — a msgpack **map** (self-describing) carrying the hash spec plus
  the structural params needed to interpret the payload (HLL `precision`;
  Count-Min `rows`/`cols`/`counter_type`/`mode`). The hash-spec values are
  **derived from the hasher's [`HashProfile`](../src/common/hash.rs)** trait, so
  the bytes truthfully describe how the sketch was hashed and custom hash
  profiles serialize self-describingly. Each sketch has its own fixed metadata
  schema with `#[serde(deny_unknown_fields)]` (fail-closed on unknown/missing
  keys).
- **Payload** — a positional msgpack **array** of the raw sketch state only
  (registers, counter matrix). No field names, and nothing the `kind_id` or
  metadata already determines.

`kind_id` + metadata together fix the payload structure completely. See
[`asapv1_wire_format.md`](./asapv1_wire_format.md) for the `kind_id` registry,
the metadata field tables, and the byte-level encoding rules.

## Code organization

### Shared framing — `envelope.rs`

`src/message_pack_format/envelope.rs` is the **one shared, sketch-agnostic**
module every sketch calls into. It owns the magic sentinel, the layout version,
and the byte framing (`encode` / `split`). It validates only the magic, version,
and framing — it does **not** know the `kind_id` registry or any sketch. Rule
"which `kind_id` do I own?" and all metadata validation happen in each sketch's
decoder.

### Per-sketch serialization — `src/sketches/<sketch>/wire.rs`

Serialization now lives **with each sketch**, split from the algorithm:

- `src/sketches/<sketch>.rs` — the **algorithm** (struct, marker types, aliases,
  insert/estimate/merge). It declares `mod wire;`.
- `src/sketches/<sketch>/wire.rs` — the **serialization** (metadata/payload DTOs,
  `kind_id` consts, wire-variant/counter/mode marker traits, and the
  `serialize_to_bytes` / `deserialize_from_bytes` impls). Because `wire` is a
  child submodule of the sketch, it reads the struct's **private** fields
  (`self.registers`, `self.counts`) directly — no field is widened for
  serialization.

Converted today:

- **HLL** (`src/sketches/hll/wire.rs`) — all variants (Classic → `0x01 0x01`,
  Ertl-MLE → `0x01 0x02`, HIP → `0x01 0x03`) × precisions (P12/P14/P16) ×
  `H: HashProfile`. HLL is **fully wire-covered**. (HIP is a non-generic struct
  hashed through the default functions, so it is wire-eligible under the standard
  profile only.)
- **Count-Min** (`src/sketches/countminsketch/wire.rs`, kind `0x02 0x00`) —
  restricted to wire-eligible configs:
  `CountMin<Vector2D<T>, Mode, H>` where `T` is `i64` or `f64` (`CmsWireCounter`),
  `Mode` is `FastPath` or `RegularPath` (`CmsWireMode`), and `H: HashProfile`.
  The default `Vector2D<i32>` CMS is **not** wire-eligible; convert first. `rows`
  and `cols` live in the metadata; the payload is just `[counts]`.

Other sketches are **not yet converted** to `wire.rs`.

## `portable/` and `native/` are being retired

The `portable/` and `native/` sub-modules (and the `MessagePackCodec` trait /
unified `Error` at the module root) are the **older** serialization path and are
being **phased out** in favor of the per-sketch `wire.rs` + the shared
`envelope.rs`.

- `portable/` was previously described as "the cross-language wire format shared
  with Go." That is no longer the direction: **ASAPv1 (per-sketch `wire.rs`) is
  what `sketchlib-go` mirrors.** The custom per-sketch payload replaces the
  `portable` types.
- `native/` was a set of thin `MessagePackCodec` shims over the sketches'
  `serialize_to_bytes` / `deserialize_from_bytes`. Those methods now emit the
  ASAPv1 envelope directly.

Sequencing note (from the spec): `portable` is not deleted until the golden
byte-vector fixtures are the drift guard on both sides.

## Cross-language parity

Cross-language parity with `sketchlib-go` is proven by **golden byte-vectors** in
[`asapv1_golden/`](../asapv1_golden) (exercised by
[`tests/asapv1_golden.rs`](../tests/asapv1_golden.rs)): both languages must
decode → re-encode them byte-identically. The `kind_id` registry is mirrored
verbatim with Go's `wire/asapmsgpack/magic_ids.go`, never independently
allocated. These goldens replace the old `portable`-as-oracle round-trip test.

## Cross-Reference

- [`asapv1_wire_format.md`](./asapv1_wire_format.md) — the authoritative
  byte-level spec (envelope, metadata schema, per-sketch payloads, encoding
  rules, wire coverage).
- Generated rustdoc: `cargo doc --no-deps --all-features --open`.
</content>
</invoke>
