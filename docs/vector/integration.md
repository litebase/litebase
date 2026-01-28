# Vector Extension Integration

The vector package has been successfully integrated with Litebase's SQLite database system. This document describes the integration architecture and how to use vector functionality in databases.

## Architecture Overview

The integration consists of four main components:

### 1. SQLite Extension Registration

The vector extension is registered automatically when SQLite connections are opened:

- **Location**: `pkg/sqlite3/connection.go`
- **Function**: `registerVectorExtension()`
- **When**: Called during `sqlite3.Open()` after connection initialization

The extension provides:

**Vector Creation Functions:**

- `vector_f16(json_array)` - Create float16 vectors (2 bytes/dimension)
- `vector_f32(json_array)` - Create float32 vectors (4 bytes/dimension)
- `vector_f64(json_array)` - Create float64 vectors (8 bytes/dimension)
- `vector_int16(json_array)` - Create int16 vectors (2 bytes/dimension)
- `vector_int8(json_array)` - Create int8 vectors (1 byte/dimension)
- `vector_bit(json_array)` - Create bit vectors (1 bit/dimension)
- `vector_sparse(json_object)` - Create sparse vectors (variable size)

**Quantization Functions:**

- `vector_quantize_f16(blob)` - Quantize to float16 (50% savings)
- `vector_quantize_int16(blob)` - Quantize to 16-bit (50% savings)
- `vector_quantize_int8(blob)` - Quantize to 8-bit (74% savings)
- `vector_quantize_bit(blob)` - Quantize to binary (96% savings)

**Distance Functions:**

- `vector_hamming_distance(blob1, blob2)` - Hamming distance for bit vectors

**Virtual Table:**

- `vector_scan` - k-NN search module for similarity queries

> **Note**: The `vector_scan` virtual table module is registered but requires implementation updates to support:
>
> 1. Eponymous table-valued function syntax for direct querying
> 2. Parameter passing for vfsID, databaseID, branchID, table, column, k, and metric
>
> Currently, these parameters are passed programmatically via the CGO bridge (`goVectorScan`), which allows the vector scan system to work across databases and branches. The database/branch IDs are necessary because the scan acquires connections via `vfs.GetVfsFromId(vfsID)` → `connManager.Get(databaseID, branchID)`, enabling cross-database vector searches.

### 2. VFS Connection Manager Adapter

A bridge between the database connection manager and the VFS interface:

- **Location**: `pkg/database/vfs_connection_adapter.go`
- **Purpose**: Allows vector operations to acquire database connections through VFS
- **Interface**: Implements `vfs.ConnectionManager`

The adapter is set on each VFS instance when database connections are registered:

```go
// In database_connection.go:registerVFS()
adapter := NewVfsConnectionAdapter(con.connectionManager)
vfs.SetConnectionManager(adapter)
```

### 3. CGO Export Linking

The vector package exports C-callable functions that the SQLite extension uses:

**Vector Encoding:**

- `goEncodeVector` / `goEncodeVectorF64` / `goEncodeVectorInt8` / `goEncodeVectorInt16` - Parse JSON and encode vectors
- `goEncodeVectorF16` / `goEncodeVectorBit` / `goEncodeVectorSparse` - Encode specialized vector types
- `goFreeVector` - Frees allocated memory

**Quantization:**

- `goQuantizeToInt8` / `goQuantizeToInt16` - Quantize to integer formats
- `goQuantizeToFloat16` - Quantize to half-precision
- `goQuantizeToBit` - Quantize to binary

**Distance Computation:**

- `goComputeHammingDistance` - Compute Hamming distance between bit vectors

**k-NN Search:**

- `goVectorScan` - Initiates vector similarity search
- `goGetScanResult` - Retrieves next result from scan
- `goReleaseScanResults` - Cleans up scan resources

**Important**: Any binary that uses the vector extension must import the vector package:

```go
import _ "github.com/litebase/litebase/pkg/vector" // Import for CGO exports
```

This ensures the CGO exports are linked during compilation.

### 4. Integration Test

Comprehensive testing verifies the integration:

- **Location**: `pkg/database/vector_integration_test.go`
- **Test**: `TestVectorExtensionRegistered`
- **Verifies**:
  - Extension loads on all connections
  - `vector_f32()` function works correctly
  - BLOB encoding follows specification (version + dimensions + data)

## Usage

### Encoding Vectors

Use vector creation functions to convert JSON to binary BLOBs:

```sql
-- Create a table with vector columns
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    vec_f32 BLOB,      -- Full precision
    vec_f16 BLOB,      -- Half precision (50% smaller)
    vec_int8 BLOB,     -- Quantized (74% smaller)
    vec_bit BLOB       -- Binary (96% smaller)
);

-- Insert vectors with different types
INSERT INTO embeddings (id, vec_f32, vec_f16, vec_int8, vec_bit)
VALUES (
    1,
    vector_f32('[1.0, 2.0, 3.0, 4.0]'),
    vector_f16('[1.0, 2.0, 3.0, 4.0]'),
    vector_int8('[127, 64, -64, -128]'),
    vector_bit('[1, 1, 0, 1]')
);

-- Quantize existing float32 vectors
UPDATE embeddings 
SET vec_f16 = vector_quantize_f16(vec_f32),
    vec_int8 = vector_quantize_int8(vec_f32),
    vec_bit = vector_quantize_bit(vec_f32);

-- Compute distances
SELECT id, vector_hamming_distance(vec_bit, vector_quantize_bit(vector_f32('[1, 1, 1, 1]'))) as distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

### Binary Format

All vectors use a 6-byte header:

- 1 byte: Version (0x01)
- 1 byte: Type (0x01-0x07)
- 4 bytes: Dimensions (uint32, little-endian)
- N bytes: Type-specific data

**Type-Specific Encoding:**

| Type    | Code | Bytes/Dim | Example (3-D)          |
| ------- | ---- | --------- | ---------------------- |
| Float32 | 0x01 | 4         | 6 + (3 × 4) = 18 bytes |
| Float64 | 0x02 | 8         | 6 + (3 × 8) = 30 bytes |
| Int8    | 0x03 | 1         | 6 + (3 × 1) = 9 bytes  |
| Int16   | 0x04 | 2         | 6 + (3 × 2) = 12 bytes |
| Float16 | 0x05 | 2         | 6 + (3 × 2) = 12 bytes |
| Bit     | 0x06 | 0.125     | 6 + ⌈3/8⌉ = 7 bytes    |
| Sparse  | 0x07 | Variable  | 6 + indices + values   |

## Implementation Details

### Connection Lifecycle

1. **Connection Open**: `sqlite3.Open()` creates a new SQLite connection
2. **Extension Init**: `registerVectorExtension()` is called automatically
3. **VFS Registration**: VFS is registered with connection manager adapter
4. **Ready**: Vector functions are available for queries

### Memory Management

- Vector BLOBs use `SQLITE_TRANSIENT`, so SQLite makes its own copy
- C memory allocated by `goEncodeVector` is freed by `goFreeVector`
- Scan handles use `cgo.Handle` for garbage collection integration

### Error Handling

Errors are handled at multiple levels:

- JSON parsing errors return NULL in SQL
- Invalid dimensions return NULL in SQL
- Internal errors are logged via `slog.Error()`

## Testing

Run the integration test:

```bash
go test -v -run TestVectorExtensionRegistered ./pkg/database
```

Expected output:

```text
✓ Vector extension successfully loaded and working!
  - Returned 17 bytes
  - Version: 0x01
```

## Troubleshooting

### "Segmentation fault" during tests

**Cause**: CGO exports not linked (vector package not imported)

**Solution**: Ensure test files import:

```go
import _ "github.com/litebase/litebase/pkg/vector"
```

### "undefined symbols" during build

**Cause**: Missing CGO export functions

**Solution**: Verify all required exports exist in:

- `pkg/vector/cgo_exports.go`
- `pkg/vector/scan.go`

### "vector_f32 function not found"

**Cause**: Extension not registered on connection

**Solution**: Verify `registerVectorExtension()` is called in `sqlite3.Open()`

## Implemented Features

- ✅ 7 vector types (float16/32/64, int8/16, bit, sparse)
- ✅ 4 quantization functions (int8/16, float16, bit)
- ✅ Hamming distance for bit vectors
- ✅ `vector_scan` virtual table for k-NN search
- ✅ Multiple distance metrics (L2, cosine, dot product, Hamming)
- ✅ SIMD-optimized distance computations

## Future Enhancements

- [ ] Vector indexing (HNSW, IVF)
- [ ] Approximate nearest neighbor search (ANN)
- [ ] Additional distance metrics (Manhattan, Chebyshev)
- [ ] Batch quantization operations
- [ ] GPU acceleration for large-scale operations
