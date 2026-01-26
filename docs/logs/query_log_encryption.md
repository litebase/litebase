# Query Log Encryption

## Overview

Query logs support optional encryption, similar to other sensitive data components in Litebase. This allows query statement indexes to be encrypted when databases are encrypted.

## Architecture

### Components

1. **QueryLog** - Main query logging component
   - Can be configured with encryption via `ConfigureEncryption(dataKey []byte, keyHash [32]byte)`
   - Stores encryption settings: `dataKey`, `keyHash`, and `encrypted` flag
   - Automatically configures the statement index with encryption when needed

2. **QueryStatementIndex** - Index mapping query checksums to SQL statements
   - Can be created encrypted via `GetQueryStatementIndexWithEncryption()`
   - Can be configured after creation via `ConfigureEncryption()`
   - Supports querying encryption status via `IsEncrypted()`

3. **LogManager** - Manages query logs for multiple databases
   - Initializes QueryLog instances
   - Callers can configure encryption by calling `queryLog.ConfigureEncryption()` after getting the log

## Usage

### Enabling Encryption

Query logs are encrypted by default when associated with an encrypted database. The encryption configuration happens in the HTTP request handler when logging queries:

```go
// In query log controller or handler
queryLog := logManager.GetQueryLog(cluster, databaseHash, databaseId, branchId)

// If database is encrypted, configure encryption on the query log
if database.Settings.Encrypted && database.Settings.DataEncryptionKeyHash != "" {
    dataKey, keyHash, err := matchEncryptionKey(config, database.Settings.DataEncryptionKeyHash)
    if err == nil {
        err = queryLog.ConfigureEncryption(dataKey, keyHash)
        if err != nil {
            slog.Error("Failed to configure query log encryption", "error", err)
        }
    }
}
```

### Creating Encrypted Statement Index

```go
// Create an encrypted statement index directly
dataKey := make([]byte, 32)
rand.Read(dataKey)
keyHash := sha256.Sum256(dataKey)

statementIndex, err := GetQueryStatementIndexWithEncryption(
    tieredFS,
    "/logs/query",
    "QUERY_STATEMENT_INDEX_node1",
    timestamp,
    dataKey,
    keyHash,
)
```

### Configuring Encryption After Creation

```go
// Create non-encrypted index
statementIndex, err := GetQueryStatementIndex(
    tieredFS,
    "/logs/query",
    "QUERY_STATEMENT_INDEX_node1",
    timestamp,
)

// Later, configure encryption
dataKey := make([]byte, 32)
rand.Read(dataKey)
keyHash := sha256.Sum256(dataKey)

err = statementIndex.ConfigureEncryption(dataKey, keyHash)
```

## API Reference

### QueryLog Methods

- `ConfigureEncryption(dataKey []byte, keyHash [32]byte) error`
  - Sets encryption parameters for the query log
  - Must be 32 bytes for dataKey
  - Also configures the statement index if already created
  - Returns error if dataKey is invalid length

### QueryStatementIndex Methods

- `ConfigureEncryption(dataKey []byte, keyHash [32]byte) error`
  - Sets encryption parameters for the statement index
  - Must be 32 bytes for dataKey
  - Returns error if dataKey is invalid length

- `IsEncrypted() bool`
  - Returns whether the statement index is encrypted
  - Thread-safe access to encryption flag

### QueryStatementIndex Functions

- `GetQueryStatementIndexWithEncryption(tieredFS, path, name, timestamp, dataKey, keyHash) (*QueryStatementIndex, error)`
  - Creates an encrypted statement index directly
  - Validates dataKey is 32 bytes
  - Same behavior as `GetQueryStatementIndex()` but with encryption enabled

## Data Security

### What Is Encrypted

- **Statement Index**: SQL statements mapped to checksums are encrypted
- **File Storage**: Index files are stored encrypted when encryption is configured

### What Is Not Encrypted (Currently)

- **Query Metrics Files (QUERY_LOG_*)**: Raw query metrics are not encrypted
  - Contains only aggregated statistics (counts, latencies)
  - Does not contain actual query text (only checksums)

## Thread Safety

All encryption-related methods are thread-safe:

- `ConfigureEncryption()` uses mutex to protect state
- `IsEncrypted()` uses mutex for safe read access
- Statement index reads/writes are protected by existing locks

## Performance Considerations

- Encryption configuration is typically done once per query log
- Encryption overhead is minimal since statement index is only written occasionally
- Decryption happens transparently when reading statements

## Integration Points

Query log encryption should be configured in:

1. **HTTP Query Log Controller** - When logging query metrics
2. **Query Log Initialization** - If query logs need to start encrypted
3. **Database Handler** - After branch encryption settings are loaded

