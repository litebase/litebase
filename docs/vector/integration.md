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

- `vector_f32(json_array)` - Scalar function to encode vectors as BLOBs
- `vector_scan` - Virtual table module for k-NN search (future implementation)

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

- `goEncodeVector` - Parses JSON and encodes as binary BLOB
- `goFreeVector` - Frees allocated memory
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

Use the `vector_f32()` function to convert JSON arrays to binary BLOBs:

```sql
-- Create a table with a vector column
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    vector BLOB
);

-- Insert vectors
INSERT INTO embeddings (id, vector) 
VALUES (1, vector_f32('[1.0, 2.0, 3.0, 4.0]'));

-- Query vectors
SELECT id, vector FROM embeddings;
```

### Binary Format

Vectors are encoded as:

- 1 byte: Version (0x01 for VectorVersion1)
- 4 bytes: Dimensions (uint32, little-endian)
- N × 4 bytes: Float32 values (IEEE 754, little-endian)

Example for `[1.0, 2.0, 3.0]`:

- Total size: 1 + 4 + (3 × 4) = 17 bytes
- Version: `0x01`
- Dimensions: `0x03000000` (3 in little-endian)
- Data: 12 bytes of float32 values

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

## Future Enhancements

- [ ] Implement `vector_scan` virtual table for k-NN search
- [ ] Add vector indexing support
- [ ] Support additional distance metrics
- [ ] Optimize BLOB encoding for different vector types
- [ ] Add batch vector operations
