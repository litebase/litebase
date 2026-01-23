# Compression and Encryption Layering

## Overview

Litebase implements a two-layer storage architecture with encryption and compression. The correct order of operations is critical for efficiency: **Compress → Encrypt → Upload**.

## Architecture

### Data Flow

```text
SQLite Write
    ↓
WAL (Write-Ahead Log)
    ↓ [Checkpoint]
PageLog
    ↓ [Compaction]
Range Files (EncryptedStreamFile)
    ↓ [TieredFS Flush]
ObjectStorage (S3)
```

### Layering Principles

1. **Compression BEFORE Encryption**: Encrypted data has high entropy and doesn't compress well. Compressing before encryption maximizes storage efficiency.

2. **No Double Compression**: If data is already compressed (e.g., in EncryptedAuthenticatedFile), skip compression at the object storage layer.

3. **Magic Header Detection**: Use file headers to identify encrypted files:
   - `LSTR` (4 bytes): EncryptedStreamFile
   - `LENC` (4 bytes): EncryptedAuthenticatedFile

## Implementation

### EncryptedStreamFile

Used for: **Range files** (final durable storage)

**Characteristics**:

- Stream-based encryption (CTR mode)
- Supports arbitrary-size reads/writes
- Does NOT compress (high-entropy encrypted data)
- Magic header: `LSTR` (hex: `4c535452`)
- 64-byte header containing encryption metadata

**Path Strategy**:

- Portable paths for branch copying: `database/{databaseId}/range/{rangeNumber}`
- Not tied to specific branch paths
- Allows encrypted ranges to be copied between branches

### EncryptedAuthenticatedFile

Used for: **Legacy or block-based storage** (not currently used for ranges)

**Characteristics**:

- Block-based encryption (GCM mode) with authentication
- Compresses BEFORE encrypting ✅
- Requires full 4096-byte page reads
- Magic header: `LENC` (hex: `4c454e43`)
- Variable-size encrypted pages

### ObjectFileSystemDriver

Handles uploads to S3-compatible object storage.

**Compression Logic (Write)**:

```go
func (fs *ObjectFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
    // Check for encryption magic headers
    isEncrypted := len(data) >= 4 && (string(data[:4]) == "LSTR" || string(data[:4]) == "LENC")
    
    if isEncrypted {
        // Skip compression for encrypted files
        // EncryptedStreamFile: high-entropy data won't compress
        // EncryptedAuthenticatedFile: already compressed
        upload(data)
    } else {
        // Compress unencrypted files with s2
        compressed := s2.Encode(data)
        upload(compressed)
    }
}
```

**Decompression Logic (Read)**:

```go
func (fs *ObjectFileSystemDriver) ReadFile(path string) ([]byte, error) {
    body := download()
    
    // Check for encryption magic headers
    isEncrypted := len(body) >= 4 && (string(body[:4]) == "LSTR" || string(body[:4]) == "LENC")
    
    if isEncrypted {
        // Skip decompression for encrypted files
        // They were not compressed during write
        return body
    } else {
        // Decompress unencrypted files
        return s2.Decode(body)
    }
}
```

**Round-Trip Integrity**:

Encrypted files maintain their exact byte content through the write/read cycle:

1. **Write**: Detect "LSTR"/"LENC" → Skip compression → Upload raw encrypted bytes
2. **Read**: Detect "LSTR"/"LENC" → Skip decompression → Return raw encrypted bytes
3. **Result**: No data corruption, perfect round-trip

## Why This Matters

### Without Optimization (Before)

```text
Range Data → Encrypt → High Entropy → Compress (wasteful!) → Upload
```

**Problems**:

- CPU cycles wasted compressing high-entropy encrypted data
- Minimal compression ratio (near 1:1)
- Confusing that EncryptedAuthenticatedFile compresses but EncryptedStreamFile doesn't

### With Optimization (After)

```text
Encrypted Files:
Range Data → Encrypt → ObjectFS (skip compression) → Upload

Unencrypted Files:
Data → ObjectFS → Compress → Upload
```

**Benefits**:

- Saves CPU cycles on encrypted files
- Consistent behavior across encryption types
- Clear separation of concerns

## Configuration

No configuration needed. The optimization is automatic based on magic headers.

## Testing

### Encryption Verification

```bash
go test ./pkg/database -run TestEncryptedDatabase/FileEncryption
```

Verifies:

- Files are actually encrypted (not plaintext "SQLite format 3")
- Files have correct magic headers ("LSTR")
- Encryption works end-to-end

### Compression Optimization

```bash
go test ./pkg/storage -run TestObjectFileSystemCompression
```

Verifies:

- Encrypted files are detected by magic headers
- Both "LSTR" and "LENC" headers are recognized
- Optimization logic is correct

## Troubleshooting

### File Not Encrypted

**Symptom**: Range file starts with "SQLite format 3" instead of "LSTR"

**Causes**:

1. Encryption configured AFTER FileSystem initialization
2. Branch encryption settings not loaded
3. Wrong encryption wrapper type

**Fix**: Ensure `NewDurableDatabaseFileSystemWithEncryption()` is called with encryption params BEFORE `init()`.

### Poor Compression Ratios

**Symptom**: Object storage files are nearly same size as source

**Causes**:

1. Files are encrypted (correct behavior - skip compression)
2. Magic header detection not working

**Check**: Verify file starts with "LSTR" or "LENC" - if so, skipping compression is expected.

### Type Mismatch Panic

**Symptom**: `panic: interface conversion: storage.File is *storage.EncryptedAuthenticatedFile, not *storage.EncryptedStreamFile`

**Cause**: Using `NewEncryptedAuthenticatedFile()` but casting to `EncryptedStreamFile`

**Fix**: Use correct constructor and type:

```go
// Correct
encryptedFile, err = NewEncryptedStreamFile(file, dataKey, keyHash, 0, encryptionPath)
err = encryptedFile.(*EncryptedStreamFile).WriteHeader()
```

## Related Files

- [pkg/storage/object_file_system_driver.go](../pkg/storage/object_file_system_driver.go) - Compression optimization
- [pkg/storage/range.go](../pkg/storage/range.go) - EncryptedStreamFile usage
- [pkg/storage/encrypted_stream_file.go](../pkg/storage/encrypted_stream_file.go) - Stream encryption
- [pkg/storage/encrypted_authenticated_file.go](../pkg/storage/encrypted_authenticated_file.go) - Block encryption with compression
- [pkg/database/encrypted_database_test.go](../pkg/database/encrypted_database_test.go) - Encryption tests
- [pkg/storage/object_file_system_compression_test.go](../pkg/storage/object_file_system_compression_test.go) - Compression optimization tests
