# Vector BLOB Format

This document describes the binary encoding format for vectors stored
in Litebase.

## Format Specification

### Version 1 Format

Vectors are encoded as binary BLOBs with a 6-byte header followed by
type-specific payload:

```text
┌─────────┬────────┬──────────────┬────────────────────────┐
│ Version │ Type   │ Dimensions   │ Vector Data            │
│ 1 byte  │ 1 byte │ 4 bytes      │ type-dependent         │
└─────────┴────────┴──────────────┴────────────────────────┘
```

### Header Fields

| Field      | Offset | Size   | Type   | Description                          |
| ---------- | ------ | ------ | ------ | ------------------------------------ |
| Version    | 0      | 1 byte | uint8  | Format version (`0x01`)              |
| Type       | 1      | 1 byte | uint8  | Data type identifier (see below)     |
| Dimensions | 2      | 4 byte | uint32 | Number of dimensions, little-endian  |
| Data       | 6      | varies | —      | Type-specific payload                |

### Vector Type Codes

| Type    | Code   | Storage        | Bytes/Dimension           |
| ------- | ------ | -------------- | ------------------------- |
| Float32 | `0x01` | IEEE 754 f32   | 4                         |
| Float64 | `0x02` | IEEE 754 f64   | 8                         |
| Int8    | `0x03` | Signed integer | 1                         |
| Int16   | `0x04` | Signed integer | 2                         |
| Float16 | `0x05` | IEEE 754 f16   | 2                         |
| Bit     | `0x06` | Bit-packed     | 1 bit (ceil(dims/8) bytes)|
| Sparse  | `0x07` | Index-value    | Variable                  |

### Example Encoding

For a Float32 vector `[1.0, 2.0, 3.0]`:

```text
Offset  Bytes         Description
------  ----------    --------------------------------
0x00    01            Version = 1
0x01    01            Type = Float32 (0x01)
0x02    03 00 00 00   Dimensions = 3 (little-endian)
0x06    00 00 80 3F   Value[0] = 1.0 (IEEE 754 f32)
0x0A    00 00 00 40   Value[1] = 2.0 (IEEE 754 f32)
0x0E    00 00 40 40   Value[2] = 3.0 (IEEE 754 f32)

Total: 18 bytes (6-byte header + 3 × 4 bytes)
```

### Type-Specific Payload Sizes

| Type    | Total BLOB size          | Example (3-D) |
| ------- | ------------------------ | ------------- |
| Float32 | `6 + dims × 4`           | 18 bytes      |
| Float64 | `6 + dims × 8`           | 30 bytes      |
| Int8    | `6 + dims`               | 9 bytes       |
| Int16   | `6 + dims × 2`           | 12 bytes      |
| Float16 | `6 + dims × 2`           | 12 bytes      |
| Bit     | `6 + ceil(dims / 8)`     | 7 bytes       |
| Sparse  | `6 + nnz × (4 + 4)`      | Variable      |

## Go API

### Encoding

```go
// Float32 vector
vec32 := []float32{1.0, 2.0, 3.0}
blob, err := vector.EncodeFloat32(vec32) // type 0x01

// Float16 vector (precision-reduced)
vec16, err := vector.EncodeFloat16(vec32) // type 0x05

// Int8 quantized vector
vec8 := []int8{120, -45, 0}
blob8, err := vector.EncodeInt8(vec8) // type 0x03
```

### Decoding

```go
// Parse BLOB to VectorBlob struct
vecBlob, err := vector.ParseVectorBlob(blob)

// Access header fields
version    := vecBlob.Version    // 0x01
vectorType := vecBlob.Type       // e.g. 0x01 = Float32
dims       := vecBlob.Dimensions // e.g. 3
rawData    := vecBlob.Data       // raw payload bytes

// Type-safe data access
floats := vecBlob.GetFloat32Slice()  // nil if not Float32

// Decode any supported type to float32 for distance computation
f32 := vecBlob.GetFloat32Decoded() // float32, float16, or int8
```

## SQL Usage

### Encoding in SQL

```sql
-- Full precision (float32)
INSERT INTO embeddings (vector)
VALUES (vector_f32('[1.0, 2.0, 3.0]'));

-- Half precision (float16, 2 bytes/dim)
INSERT INTO embeddings (vector)
VALUES (vector_f16('[1.0, 2.0, 3.0]'));

-- Quantized integer (int8, 1 byte/dim)
INSERT INTO embeddings (vector)
VALUES (vector_int8('[120, -45, 0]'));

-- Binary (1 bit/dim)
INSERT INTO embeddings (vector)
VALUES (vector_bit('[1, 0, 1, 1, 0, 1, 0, 0]'));
```

### Retrieving Vectors

```sql
-- Returns raw BLOB (6-byte header + payload)
SELECT id, vector FROM embeddings WHERE id = 1;

-- Float32 [1.0, 2.0, 3.0] returns:
-- 0x01 01 03000000 0000803F 00000040 00004040
--  ^   ^  ^------  ^-----float data---------
-- ver type dims
```

## Validation

### Constraints

1. **Version byte must be `0x01`** — only version 1 is supported
2. **Type byte must be `0x01`-`0x07`** — unknown types are rejected
3. **Dimensions must be 1–4096** — enforced by `MaxDimensions`
4. **BLOB length must match the type formula** — e.g. Float32
   requires exactly `6 + dims × 4` bytes

### Error Handling

```go
vecBlob, err := vector.ParseVectorBlob(blob)

if err != nil {
    switch err {
    case vector.ErrInvalidBlobFormat:
        // Wrong length or structure
    case vector.ErrUnsupportedVersion:
        // Version byte is not 0x01
    case vector.ErrInvalidDimensions:
        // Dimensions == 0 or > 4096
    }
}
```

## Memory Efficiency

### Size Calculation

All types share the same 6-byte header; only the payload size differs:

```text
Float32  = 6 + (dims × 4)
Float64  = 6 + (dims × 8)
Int8     = 6 + dims
Int16    = 6 + (dims × 2)
Float16  = 6 + (dims × 2)
Bit      = 6 + ceil(dims / 8)
Sparse   = 6 + (nnz × 4) + (nnz × 4)  [indices + values]
```

Examples for a 1536-D embedding:

| Type    | Size     | vs Float32 |
| ------- | -------- | ---------- |
| Float32 | 6,150 B  | 100%       |
| Float16 | 3,078 B  | 50%        |
| Int8    | 1,542 B  | 25%        |
| Bit     | 198 B    | 3%         |

## Implementation Notes

- Little-endian byte order throughout (version, type, dimensions, data)
- IEEE 754 encoding for Float32, Float64, and Float16 payloads
- Bit vectors pack 8 dimensions per byte, LSB first
- Sparse vectors encode non-zero indices as `uint32` followed by
  `float32` values, both little-endian
- Direct memory mapping possible for SIMD operations
- No padding or alignment bytes (packed format)
