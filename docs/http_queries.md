# HTTP Queries in Litebase

This document describes how to execute SQL queries against Litebase databases using the HTTP JSON API.

## Table of Contents

- [Overview](#overview)
- [Query Endpoint](#query-endpoint)
- [Request Format](#request-format)
- [Data Types](#data-types)
- [Response Format](#response-format)
- [Examples](#examples)
- [Error Handling](#error-handling)

## Overview

Litebase Server provides an HTTP JSON API endpoint for executing SQL queries against databases. Queries are sent as JSON payloads to the query endpoint and return results in JSON format. This approach is straightforward and works with any HTTP client.

For high-performance scenarios with frequent queries, consider using the [Litebase Query Transfer Protocol (LQTP)](./litebase_query_transfer_protocol.md) which offers a more efficient binary protocol over streaming connections.

## Query Endpoint

```http
POST /v1/databases/{databaseName}/branches/{branchName}/query HTTP/1.1
```

### Path Parameters

- `databaseName` - The name of the database
- `branchName` - The name of the database branch

### Authentication

The endpoint supports three authentication methods:

- **Access Key Authentication** - Using HMAC-SHA256 signatures (recommended for production)
- **Token Authentication** - Using bearer tokens
- **Basic Authentication** - Using username and password

## Request Format

### Request Body Structure

The request body must be a JSON object with the following structure:

```json
{
  "queries": [
    {
      "id": "unique-query-id",
      "statement": "SQL statement with optional ? placeholders",
      "parameters": [
        {
          "type": "TEXT|INTEGER|FLOAT|BLOB|NULL",
          "value": "parameter value"
        }
      ],
      "transactionId": "optional-transaction-id"
    }
  ]
}
```

### Query Object Fields

| Field           | Type   | Required | Description                                                                                 |
| --------------- | ------ | -------- | ------------------------------------------------------------------------------------------- |
| `id`            | string | Yes      | Unique identifier for the query. Used to match responses to requests.                       |
| `statement`     | string | Yes      | SQL statement to execute. Use `?` as placeholders for parameters.                           |
| `parameters`    | array  | Yes      | Array of parameter objects to bind to the statement. Use empty array `[]` if no parameters. |
| `transactionId` | string | No       | Transaction ID if this query is part of a transaction. Omit for auto-commit queries.        |

### Parameter Object Structure

Each parameter in the `parameters` array must have:

| Field   | Type   | Required    | Description                                              |
| ------- | ------ | ----------- | -------------------------------------------------------- |
| `type`  | string | Yes         | Data type: `TEXT`, `INTEGER`, `FLOAT`, `BLOB`, or `NULL` |
| `value` | any    | Conditional | The parameter value. Required unless type is `NULL`.     |

## Data Types

Litebase supports five data types for query parameters and result columns, corresponding to SQLite's type system.

### TEXT

String values stored as text.

**Request Format:**

```json
{
  "type": "TEXT",
  "value": "Hello, World!"
}
```

### INTEGER

64-bit signed integer values (range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807).

**Request Format (as number):**

```json
{
  "type": "INTEGER",
  "value": 42
}
```

**Request Format (as string for large integers):**

```json
{
  "type": "INTEGER",
  "value": "9007199254740993"
}
```

**JavaScript Compatibility:**

JavaScript's `Number` type uses IEEE 754 double-precision floating-point format, which can only safely represent integers between `-(2^53 - 1)` and `2^53 - 1` (±9,007,199,254,740,991).

For integers outside this safe range, **use string format** to avoid precision loss:

```javascript
// UNSAFE - Will lose precision for large integers
{
  type: "INTEGER",
  value: 9007199254740993  // Beyond Number.MAX_SAFE_INTEGER
}

// SAFE - Use string format
{
  type: "INTEGER",
  value: "9007199254740993"
}

// Helper function to check if string format is needed
function needsStringFormat(value) {
  return value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER;
}
```

**Python Example:**

```python
# Python integers have arbitrary precision, so you can use either format
{
  "type": "INTEGER",
  "value": 42  # Small integers as numbers
}

{
  "type": "INTEGER",
  "value": "9223372036854775807"  # Large integers as strings
}
```

**JSON Response Handling:**

Litebase automatically handles large integers in responses to prevent precision loss:

- **Small integers** (within ±2^53-1): Returned as JSON numbers for backward compatibility
- **Large integers** (beyond ±2^53-1): Automatically returned as JSON strings to preserve full precision

This means JavaScript clients can safely handle all integer values without precision loss:

```javascript
// Response with mixed integer sizes
{
  "columns": [
    {"name": "small_id", "type": 1},      // INTEGER (within safe range)
    {"name": "large_id", "type": 101}     // INTEGER_LARGE (beyond safe range)
  ],
  "rows": [
    [42, "9007199254740993"]  // Small int as number, large int as string
  ]
}

// Client handling - check column types to determine how to parse values
response.data[0].rows.forEach(row => {
  response.data[0].columns.forEach((col, idx) => {
    if (col.type === 101) {  // INTEGER_LARGE
      const largeInt = BigInt(row[idx]);  // Parse string to BigInt
      console.log(`${col.name}: ${largeInt}`);
    } else if (col.type === 1) {  // INTEGER
      const smallInt = row[idx];  // Use as regular number
      console.log(`${col.name}: ${smallInt}`);
    }
  });
});
```

**Important:** For applications that need consistent handling of all integers (always as numbers or always as strings), or require exact large integer handling without automatic conversion, use the [LQTP binary protocol](./litebase_query_transfer_protocol.md) which preserves full 64-bit integer precision without any string conversion.

### FLOAT

64-bit floating point values (IEEE 754 double precision).

**Request Format:**

```json
{
  "type": "FLOAT",
  "value": 3.14159
}
```

### BLOB

Binary data stored as a byte array. **Blob values must be base64-encoded in the HTTP request.**

**Request Format:**

```json
{
  "type": "BLOB",
  "value": "SGVsbG8sIFdvcmxkIQ=="
}
```

**Important Notes:**

- The server automatically base64-decodes blob values before binding them to SQL statements
- In responses, blob values are returned as base64-encoded strings
- Empty blobs should be sent as empty strings: `"value": ""`

### NULL

Represents a NULL value in SQL.

**Request Format:**

```json
{
  "type": "NULL"
}
```

**Note:** The `value` field should be omitted for NULL parameters.

## Response Format

### Success Response Structure

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "unique-query-id",
      "changes": 0,
      "lastInsertRowId": 0,
      "rowCount": 2,
      "latency": 0.001234,
      "columns": [
        {
          "name": "id",
          "type": 1
        },
        {
          "name": "name",
          "type": 3
        }
      ],
      "rows": [
        [1, "Alice"],
        [2, "Bob"]
      ],
      "transactionId": ""
    }
  ]
}
```

### Response Fields

| Field     | Type   | Description                                                    |
| --------- | ------ | -------------------------------------------------------------- |
| `status`  | string | `"success"` or `"error"`                                       |
| `message` | string | Human-readable status message                                  |
| `data`    | array  | Array of query response objects (one per query in the request) |

### Query Response Object

| Field             | Type    | Description                                                        |
| ----------------- | ------- | ------------------------------------------------------------------ |
| `id`              | string  | The query ID from the request                                      |
| `changes`         | integer | Number of rows modified by DML statements (INSERT, UPDATE, DELETE) |
| `lastInsertRowId` | integer | The ROWID of the last row inserted (0 if not applicable)           |
| `rowCount`        | integer | Number of rows returned by SELECT queries                          |
| `latency`         | float   | Query execution time in seconds                                    |
| `columns`         | array   | Column definitions (name and type)                                 |
| `rows`            | array   | Array of row arrays containing the query results                   |
| `transactionId`   | string  | Transaction ID if the query started or is part of a transaction    |

### Column Types in Responses

Column types are represented as integers corresponding to SQLite's internal types:

| Type Code | Data Type     | Description                                                                  |
| --------- | ------------- | ---------------------------------------------------------------------------- |
| 1         | INTEGER       | 64-bit signed integer (returned as JSON number)                              |
| 2         | FLOAT         | 64-bit floating point                                                        |
| 3         | TEXT          | UTF-8 text string                                                            |
| 4         | BLOB          | Binary data (base64-encoded in JSON)                                         |
| 5         | NULL          | NULL value                                                                   |
| 101       | INTEGER_LARGE | 64-bit signed integer beyond ±2^53-1 (returned as JSON string for precision) |

**Note on INTEGER vs INTEGER_LARGE:**

- Type `1` (INTEGER): Values within JavaScript's safe integer range (±9,007,199,254,740,991) are returned as JSON numbers
- Type `101` (INTEGER_LARGE): Values beyond the safe range are returned as JSON strings to preserve full precision

This allows clients to handle large integers appropriately:

```javascript
columns.forEach((col, idx) => {
  if (col.type === 101) {  // INTEGER_LARGE
    // Parse as BigInt
    const value = BigInt(row[idx]);
  } else if (col.type === 1) {  // INTEGER
    // Use as regular number
    const value = row[idx];
  }
});
```

## Examples

### Basic SELECT Query

**Request:**

```json
{
  "queries": [
    {
      "id": "query-1",
      "statement": "SELECT * FROM users WHERE age > ?",
      "parameters": [
        {
          "type": "INTEGER",
          "value": 25
        }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "query-1",
      "changes": 0,
      "lastInsertRowId": 0,
      "rowCount": 2,
      "latency": 0.002,
      "columns": [
        {"name": "id", "type": 1},
        {"name": "name", "type": 3},
        {"name": "age", "type": 1}
      ],
      "rows": [
        [1, "Alice", 30],
        [2, "Bob", 28]
      ],
      "transactionId": ""
    }
  ]
}
```

### INSERT with Multiple Parameters

**Request:**

```json
{
  "queries": [
    {
      "id": "insert-1",
      "statement": "INSERT INTO products (name, price, description) VALUES (?, ?, ?)",
      "parameters": [
        {
          "type": "TEXT",
          "value": "Widget"
        },
        {
          "type": "FLOAT",
          "value": 19.99
        },
        {
          "type": "TEXT",
          "value": "A useful widget"
        }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "insert-1",
      "changes": 1,
      "lastInsertRowId": 42,
      "rowCount": 0,
      "latency": 0.001,
      "columns": [],
      "rows": [],
      "transactionId": ""
    }
  ]
}
```

### Working with BLOB Data

**Request:**

```json
{
  "queries": [
    {
      "id": "blob-insert",
      "statement": "INSERT INTO files (name, content) VALUES (?, ?)",
      "parameters": [
        {
          "type": "TEXT",
          "value": "document.pdf"
        },
        {
          "type": "BLOB",
          "value": "JVBERi0xLjQKJeLjz9MKMSAwIG9iago8PAov..."
        }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "blob-insert",
      "changes": 1,
      "lastInsertRowId": 1,
      "rowCount": 0,
      "latency": 0.003,
      "columns": [],
      "rows": [],
      "transactionId": ""
    }
  ]
}
```

### Query with NULL Parameter

**Request:**

```json
{
  "queries": [
    {
      "id": "null-update",
      "statement": "UPDATE users SET email = ? WHERE id = ?",
      "parameters": [
        {
          "type": "NULL"
        },
        {
          "type": "INTEGER",
          "value": 5
        }
      ]
    }
  ]
}
```

### Multiple Queries in One Request

**Request:**

```json
{
  "queries": [
    {
      "id": "query-1",
      "statement": "SELECT COUNT(*) as count FROM users",
      "parameters": []
    },
    {
      "id": "query-2",
      "statement": "SELECT * FROM users WHERE active = ?",
      "parameters": [
        {
          "type": "INTEGER",
          "value": 1
        }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "query-1",
      "rowCount": 1,
      "columns": [{"name": "count", "type": 1}],
      "rows": [[150]],
      "latency": 0.001
    },
    {
      "id": "query-2",
      "rowCount": 45,
      "columns": [
        {"name": "id", "type": 1},
        {"name": "name", "type": 3},
        {"name": "active", "type": 1}
      ],
      "rows": [
        [1, "Alice", 1],
        [2, "Bob", 1]
      ],
      "latency": 0.002
    }
  ]
}
```

### Transaction Example

**Request 1 - Start Transaction:**

```json
{
  "queries": [
    {
      "id": "tx-begin",
      "statement": "BEGIN",
      "parameters": []
    }
  ]
}
```

**Response 1:**

```json
{
  "status": "success",
  "message": "Queries executed successfully",
  "data": [
    {
      "id": "tx-begin",
      "transactionId": "tx-12345-67890-abcdef",
      "latency": 0.0001
    }
  ]
}
```

**Request 2 - Execute Query in Transaction:**

```json
{
  "queries": [
    {
      "id": "tx-query",
      "statement": "INSERT INTO accounts (user_id, balance) VALUES (?, ?)",
      "parameters": [
        {"type": "INTEGER", "value": 100},
        {"type": "FLOAT", "value": 500.00}
      ],
      "transactionId": "tx-12345-67890-abcdef"
    }
  ]
}
```

**Request 3 - Commit Transaction:**

```json
{
  "queries": [
    {
      "id": "tx-commit",
      "statement": "COMMIT",
      "parameters": [],
      "transactionId": "tx-12345-67890-abcdef"
    }
  ]
}
```

## Error Handling

### Validation Errors

When the request format is invalid, the server returns a 400 Bad Request with validation errors:

```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": {
    "queries[0].statement": ["The SQL statement field is required"],
    "queries[0].parameters[0].type": ["The parameter type field must be one of the allowed values"]
  }
}
```

### Query Execution Errors

When a query fails to execute, the error is included in the response:

```json
{
  "status": "error",
  "message": "Query execution failed",
  "code": "SQL_ERROR"
}
```

### Common Error Codes

| Code                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `INVALID_INPUT`      | Request body validation failed                           |
| `INVALID_CREDENTIAL` | Authentication failed                                    |
| `FORBIDDEN`          | User lacks required permissions                          |
| `DATABASE_NOT_FOUND` | Database or branch does not exist                        |
| `SQL_ERROR`          | SQL syntax or execution error                            |
| `TRANSACTION_ERROR`  | Transaction-related error (e.g., invalid transaction ID) |

## Best Practices

### 1. Always Use Parameterized Queries

**Bad:**

```json
{
  "statement": "SELECT * FROM users WHERE name = 'Alice'"
}
```

**Good:**

```json
{
  "statement": "SELECT * FROM users WHERE name = ?",
  "parameters": [{"type": "TEXT", "value": "Alice"}]
}
```

### 2. Generate Unique Query IDs

Use UUIDs or other unique identifiers for query IDs to avoid conflicts:

```javascript
const queryId = crypto.randomUUID();
```

### 3. Handle Blob Data Correctly

Always base64-encode blob data before sending:

```javascript
// Browser
const blob = new Blob([data]);
const base64 = await blob.arrayBuffer()
  .then(buffer => btoa(String.fromCharCode(...new Uint8Array(buffer))));

// Node.js
const base64 = Buffer.from(data).toString('base64');
```

### 4. Batch Multiple Queries

When possible, batch related queries in a single request to reduce HTTP overhead:

```json
{
  "queries": [
    {"id": "1", "statement": "INSERT INTO ...", "parameters": [...]},
    {"id": "2", "statement": "INSERT INTO ...", "parameters": [...]},
    {"id": "3", "statement": "INSERT INTO ...", "parameters": [...]}
  ]
}
```

### 5. Use Transactions for Related Writes

Wrap related write operations in transactions to ensure atomicity:

```javascript
// 1. Begin transaction
// 2. Execute queries with transactionId
// 3. Commit or rollback
```

### 6. Monitor Query Latency

Use the `latency` field in responses to monitor query performance and identify slow queries.

## Performance Considerations

### HTTP REST API vs LQTP

The HTTP REST API is suitable for:

- Occasional queries
- Simple CRUD operations
- Browser-based applications
- Integration with existing HTTP clients

Consider using [LQTP](./litebase_query_transfer_protocol.md) for:

- High-frequency query execution
- Low-latency requirements
- Streaming large result sets
- Applications executing hundreds or thousands of queries per second

### Connection Pooling

While the HTTP REST API doesn't maintain persistent connections, you can still benefit from HTTP keep-alive connections at the HTTP client level. Configure your HTTP client library to reuse connections:

```javascript
// Example with axios in Node.js
const axios = require('axios');
const http = require('http');
const https = require('https');

const instance = axios.create({
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true })
});
```

## See Also

- [Litebase Query Transfer Protocol (LQTP)](./litebase_query_transfer_protocol.md) - Binary streaming protocol for high-performance queries
- [OpenAPI Specification](../api/generated_open_api.json) - Complete API reference
- [Server Documentation](./server.md) - Server configuration and deployment
