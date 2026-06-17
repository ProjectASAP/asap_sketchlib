# MessagePack Magic IDs

Every serialised sketch binary produced by this library starts with a single
type-discriminant byte called the **magic ID**. Reading the first byte of any
blob is enough to identify what sketch type it contains — no full decode
required.

**Portable** (cross-language):
```
┌─────────────┬───────────────────────────────────────┐
│ magic_id: u8 │  rmp_serde / msgpack payload …        │
└─────────────┴───────────────────────────────────────┘
```

**Native** (Rust-internal):
```
┌─────────────┬──────────────┬───────────────────────────────────┐
│ magic_id: u8 │ hasher_id: u8 │  rmp_serde / msgpack payload …   │
└─────────────┴──────────────┴───────────────────────────────────┘
```

The `hasher_id` byte is `0xFF` (`HASHER_UNKNOWN`) for types without an `H` parameter.
Custom hashers that do not register an ID also store `0xFF` — the mismatch check
is skipped on both sides when either value is `0xFF`.

Magic IDs are **stable** — once assigned, a value is never reused or
reassigned. Adding a new sketch type requires a new constant; removing or
repurposing an existing constant is a **breaking protocol change**.

The single source of truth in code is
[`src/message_pack_format/magic_ids.rs`](../src/message_pack_format/magic_ids.rs).
The Go mirror lives in
[`sketchlib-go/wire/asapmsgpack/magic_ids.go`](https://github.com/ProjectASAP/sketchlib-go/blob/main/wire/asapmsgpack/magic_ids.go).

---

## Portable IDs (0x01 – 0x09)

These IDs identify the **cross-language wire format** shared with
`sketchlib-go`. Any byte blob with a portable ID can be decoded by either
the Rust or Go implementation.

| ID     | Rust type / entry point                          | Go entry point                         |
|--------|--------------------------------------------------|----------------------------------------|
| `0x01` | `portable::HllSketch::to_msgpack`                | `HLL.SerializeMsgpack`                 |
| `0x02` | `portable::CountMinSketch::to_msgpack`           | `CountMinSketch.SerializeMsgpack`      |
| `0x03` | `portable::CountMinSketchWithHeap::to_msgpack`   | `CountSketch.SerializeMsgpackWithHeap` |
| `0x04` | `portable::CountSketch::to_msgpack`              | `CountSketch.SerializeMsgpack`         |
| `0x05` | `portable::DdSketch::to_msgpack`                 | `DDSketch.SerializeMsgpack`            |
| `0x06` | `portable::KllSketch::to_msgpack`                | _(Rust-only path; no Go equivalent)_   |
| `0x07` | `portable::HydraKllSketch::to_msgpack`           | _(Rust-only path; no Go equivalent)_   |
| `0x08` | `portable::SetAggregator::to_msgpack`            | _(Rust-only path; no Go equivalent)_   |
| `0x09` | `portable::DeltaResult::to_msgpack`              | _(Rust-only path; no Go equivalent)_   |

> **Note on `0x03`:** The Go producer calls this format "CountSketchWithHeap";
> the Rust consumer knows it as `CountMinSketchWithHeap`. The delta-heap frame
> (`SerializeMsgpackWithHeapDelta`) shares the same magic ID because the Rust
> consumer uses the same `from_msgpack` path for both the full and delta shapes.

---

## Native IDs (0x81 – 0x89)

These IDs identify the **Rust-internal** format produced by
`serialize_to_bytes` / `deserialize_from_bytes` on the generic sketch types
in `src/sketches/`. Go never reads these bytes directly.

The native wire format uses `rmp_serde`'s **named** (map) encoding
(`to_vec_named`), whereas the portable format uses **compact** (array)
encoding. The two are not interchangeable even for logically equivalent types
(e.g., `sketches::DDSketch` vs `portable::DdSketch`).

| ID     | Rust type / method                              | Notes |
|--------|--------------------------------------------------|-------|
| `0x81` | `CountMin<_, RegularPath, _>::serialize_to_bytes` | Named map format |
| `0x82` | `CountMin<_, FastPath, _>::serialize_to_bytes`   | Named map format |
| `0x83` | `Count<_, RegularPath, _>::serialize_to_bytes`   | Named map format |
| `0x84` | `Count<_, FastPath, _>::serialize_to_bytes`      | Named map format |
| `0x85` | `CountL2HH::serialize_to_bytes`                  | Named map format (CMSHeap / heavy-hitter) |
| `0x86` | `HyperLogLogImpl<Classic, _, _>::serialize_to_bytes`   | Named map format |
| `0x87` | `HyperLogLogImpl<ErtlMLE, _, _>::serialize_to_bytes`   | Named map format |
| `0x88` | `HyperLogLogHIPImpl<_>::serialize_to_bytes`      | Named map; always `HASHER_DEFAULT_XX` as second byte |
| `0x89` | `sketches::DDSketch::serialize_to_bytes`         | Named map; distinct from portable `0x05` |
| `0x8a` | `sketches::KLL::serialize_to_bytes`              | Compact array format |
| `0x8b` | `sketches::KLLDynamic::serialize_to_bytes`       | Compact array format |
| `0x8c` | `sketches::KMV::serialize_to_bytes`              | Named map format (experimental feature) |
| `0x8d` | `sketch_framework::Hydra::serialize_to_bytes`    | Named map format |
| `0x8e` | `sketch_framework::UnivMon::serialize_to_bytes`  | Named map format |

### Relationship between native and portable KLL

`portable::KllSketch::to_msgpack` (`0x06`) embeds the raw KLL cell bytes
inside a `KllSketchData { k, sketch_bytes }` msgpack struct. Those embedded
bytes are produced by `KLL::serialize_to_bytes` and therefore carry the native
`0x87` prefix. The portable round-trip is:

```
KllSketch::to_msgpack()               → [ 0x06 | msgpack([k, [0x87 | raw_kll_bytes]]) ]
KllSketch::from_msgpack()             → strips 0x06, decodes struct,
                                        calls KLL::deserialize_from_bytes([0x87 | …])
                                        → strips 0x87, decodes KLL
```

The same pattern applies to `HydraKllSketch` (`0x07`), which contains a grid
of KLL cells.

---

## Adding a new sketch type

1. Add a constant to `src/message_pack_format/magic_ids.rs` (choose the next
   available value in the appropriate range).
2. If the type has a **portable** wire format shared with Go, also add the
   constant to `sketchlib-go/wire/asapmsgpack/magic_ids.go` with the same
   value, and update `SerializeMsgpack` / `DeserializeMsgpack` in the
   corresponding Go sketch package.
3. Implement `MessagePackCodec::to_msgpack` / `from_msgpack` (or
   `serialize_to_bytes` / `deserialize_from_bytes` for native types) to
   prepend / validate the new constant.
4. Add or update round-trip tests.
5. Update this document.
