# Vector BLOB Format

This document describes the binary encoding format for vectors stored in Litebase.

## Format Specification

### Version 1 Format

Vectors are encoded as binary BLOBs with the following structure:

```text
┌─────────┬──────────────┬───────────────────────┐
│ Version │ Dimensions   │ Vector Data           │
│ 1 byte  │ 4 bytes      │ dimensions × 4 bytes  │
└─────────┴──────────────┴───────────────────────┘
```

### Field Details

| Field      | Offset | Size        | Type      | Description                            |
| ---------- | ------ | ----------- | --------- | -------------------------------------- |
| Version    | 0      | 1 byte      | uint8     | Format version (0x01)                  |
| Dimensions | 1      | 4 bytes     | uint32    | Number of dimensions (little-endian)   |
| Data       | 5      | N × 4 bytes | float32[] | Vector values (little-endian IEEE 754) |

### Example Encoding

For vector `[1.0, 2.0, 3.0]`:

```text
Offset  Bytes                   Description
------  ----------------------  ---------------------------
0x00    01                      Version = 1
0x01    03 00 00 00             Dimensions = 3 (little-endian)
0x05    00 00 80 3F             Value[0] = 1.0 (IEEE 754)
0x09    00 00 00 40             Value[1] = 2.0 (IEEE 754)
0x0D    00 00 40 40             Value[2] = 3.0 (IEEE 754)

Total: 17 bytes
```

## Go API

### Encoding

```go
// From float32 slice
vec := []float32{1.0, 2.0, 3.0}
blob, err := vector.EncodeFloat32(vec)

// From JSON string
jsonStr := "[1.0, 2.0, 3.0]"
values, err := vector.ParseJSONArray(jsonStr)
blob, err := vector.EncodeFloat32(values)
```

### Decoding

```go
// Parse BLOB to VectorBlob struct
vecBlob, err := vector.ParseVectorBlob(blob)

// Access properties
version := vecBlob.Version      // 0x01
dims := vecBlob.Dimensions      // 3
data := vecBlob.Data            // Raw []byte

// Get as float32 slice
floats := vecBlob.GetFloat32Slice()  // []float32{1.0, 2.0, 3.0}
```

## SQL Usage

### Encoding in SQL

```sql
-- Direct JSON array encoding
INSERT INTO embeddings (vector) 
VALUES (vector_f32('[1.0, 2.0, 3.0]'));

-- From variable
INSERT INTO embeddings (vector) 
VALUES (vector_f32(?));  -- Pass "[1.0, 2.0, 3.0]" as parameter
```

### Retrieving Vectors

```sql
-- Get raw BLOB
SELECT id, vector FROM embeddings WHERE id = 1;

-- Returns BLOB: 0x01 03000000 0000803F 00000040 00004040
```

## Validation

### Constraints

1. **Version byte must be 0x01**

   ```go
   if blob[0] != 0x01 {
       return ErrUnsupportedVersion
   }
   ```

2. **Dimensions must be valid**

   ```go
   if dims <= 0 || dims > 4096 {
       return ErrInvalidDimensions
   }
   ```

3. **BLOB size must match**

   ```go
   expectedSize := 5 + (dims * 4)

   if len(blob) != expectedSize {
       return ErrInvalidBlobFormat
   }
   ```

### Error Handling

```go
vecBlob, err := vector.ParseVectorBlob(blob)

if err != nil {
    switch err {
    case vector.ErrInvalidBlobFormat:
        // Invalid structure or size
    case vector.ErrUnsupportedVersion:
        // Version byte not 0x01
    case vector.ErrInvalidDimensions:
        // Dimensions out of range
    }
}
```

## Memory Efficiency

### Size Calculation

```text
Size (bytes) = 1 + 4 + (dimensions × 4)
             = 5 + (dimensions × 4)
```

Examples:

- 128-D vector: 5 + (128 × 4) = **517 bytes**
- 384-D vector: 5 + (384 × 4) = **1,541 bytes**
- 1536-D vector: 5 + (1536 × 4) = **6,149 bytes**

### Storage Comparison

| Format      | 128-D  | 384-D   | 1536-D  |
| ----------- | ------ | ------- | ------- |
| Binary BLOB | 517 B  | 1.5 KB  | 6.0 KB  |
| JSON Array  | ~640 B | ~1.9 KB | ~7.6 KB |
| Space Saved | 19%    | 21%     | 21%     |

## Future Versions

### Planned Extensions

- **Version 2**: Compressed vectors (quantization)
- **Version 3**: Sparse vector support
- **Version 4**: Multi-precision (float16, int8)

### Backwards Compatibility

The version byte ensures future formats can coexist:

```go
switch blob[0] {
case 0x01:
    return parseV1(blob)
case 0x02:
    return parseV2(blob)  // Future
default:
    return ErrUnsupportedVersion
}
```

## Best Practices

1. **Always validate BLOBs** before processing
2. **Use provided APIs** instead of manual parsing
3. **Check dimensions** match your model requirements
4. **Handle errors gracefully** with proper error types
5. **Consider memory alignment** for SIMD operations

## Implementation Notes

- Little-endian byte order for cross-platform compatibility
- IEEE 754 float32 format (widely supported)
- Header overhead minimal (5 bytes fixed)
- Direct memory mapping possible for SIMD operations
- No padding or alignment bytes (packed format)
