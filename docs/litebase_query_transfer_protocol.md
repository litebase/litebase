# Litebase Query Transfer Protocol

Litebase Query Transfer Protocol (LQTP) is a binary protocol designed to work on top of HTTP for efficient communication between clients and the Litebase Server. While Litebase Server generally performs operations using an HTTP JSON API, this protocol is provided specifically for executing database queries. LQTP offers more efficient communication with the ability to stream queries using bi-directional communication over a persistent TCP connection.

## Why LQTP?

By default, clients can send queries to Litebase Server via JSON via an HTTP API endpoint.

**HTTP JSON Example:**

```http
POST /v1/databases/<database>/<branch>/query HTTP/1.1 
Authorization: Bearer <token>
Content-Type: application/json

{
    "queries": [
        {
            "id": "<unique-query-id>",
            "transaction_id": "<optional-transaction-id>",
            "statement": "SELECT * FROM users WHERE age > ?",
            "parameters": [30]
        }
    ]
}
```

This method is straightforward and easy to implement, but it has some limitations, especially when dealing with high-frequency query execution. For every query, a new HTTP request must be made, sending the full HTTP headers and body each time. In addition, using JSON as a transport format introduces additional overhead due to serialization and deserialization processes which can be quite wasteful for large volumes of data. Together, these factors can introduce significant overhead, particularly in scenarios where low latency and high throughput are required.

LQTP is designed to address these limitations by offering a well-defined protocol for executing database queries.

### Key Benefits

- Lowers latency by minimizing the overhead of HTTP.
- Reduces bandwidth usage with a compact binary format and avoidance of repetitive HTTP headers.
- Avoids costly JSON serialization/deserialization.
- Enables asynchronous communication and bidirectional streaming.
- Supports multiplexing of multiple queries over a single connection.

## Protocol Overview

LQTP runs on top of HTTP and implements a simple binary format to encode messages. Unlike traditional HTTP APIs that require a separate request for each query, LQTP uses a single HTTP request that is upgraded to a persistent connection using TCP keep-alive. This allows multiple queries to be sent and received over the same connection without the overhead of establishing new HTTP requests for each operation. By using a binary format and this persistent connection approach, LQTP significantly reduces the overhead associated with HTTP request/response cycles, enabling faster and more efficient communication between clients and the Litebase Server.

To start using LQTP, clients must establish a connection to the Litebase Server's LQTP endpoint, typically at `/v1/databases/<database>/<branch>/query/stream`.

**Create Connection:**

```http
POST /v1/databases/<database>/<branch>/query/stream HTTP/1.1 
Authorization: Bearer <token>
Content-Type: application/octet-stream
Upgrade: lqtp
Connection: Upgrade

...
```

Once the request is authenticated and authorized the server will respond with a `101 Switching Protocols` status code, indicating that the connection has been upgraded to support LQTP.

**Connection Response:**

```http
HTTP/1.1 101 Switching Protocols
Upgrade: lqtp
Connection: Upgrade
```

Following these headers, the server will also send a `QueryStreamOpenConnection` message to indicate that the connection is ready to accept queries. Once the client receives this message, it can start sending a query stream. A query stream consists of one or more `QueryStreamFrame` messages, each containing one or more individual queries.


<!-- markdownlint-disable MD033 MD041 -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/litebase-lqtp-diagram-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/litebase-lqtp-diagram-light.svg">
  <img alt="Fallback image description" src="docs/images/litebase-lqtp-diagram-light.svg">
</picture>

## Protocol Message Format

All LQTP messages follow a consistent binary format:

| Offset | Length | Description                    |
| ------ | ------ | ------------------------------ |
| 0      | 1      | Message Type                   |
| 1      | 4      | Message Length (Little Endian) |
| 5      | n      | Message Data                   |

### Message Types

LQTP defines the following message types:

- `0x01` - **QueryStreamOpenConnection**: Sent by server to indicate connection is ready
- `0x02` - **QueryStreamCloseConnection**: Sent by client to close the connection
- `0x03` - **QueryStreamError**: Sent by server to indicate an error occurred
- `0x04` - **QueryStreamFrame**: Contains one or more query frame entries
- `0x05` - **QueryStreamFrameEntry**: Individual query response within a frame

## Query Stream

The query stream allows clients to send multiple queries efficiently over a single connection. Queries are sent as frames, where each frame can contain multiple individual queries.

### Connection Establishment

When a connection is established, the server sends a `QueryStreamOpenConnection` message:

| Field        | Type   | Length | Description                |
| ------------ | ------ | ------ | -------------------------- |
| Message Type | uint8  | 1      | `0x01`                     |
| Length       | uint32 | 4      | Length of message data (9) |
| Data         | string | 9      | "connected"                |

### Sending Queries

Clients send queries using `QueryStreamFrame` messages. Each frame contains one or more individual queries.

#### Frame Format

| Field        | Type   | Length | Description            |
| ------------ | ------ | ------ | ---------------------- |
| Message Type | uint8  | 1      | `0x04`                 |
| Length       | uint32 | 4      | Length of frame data   |
| Frame Data   | bytes  | n      | Multiple query entries |

#### Frame Data Structure

Within the frame data, each query follows this format:

| Field        | Type   | Length | Description               |
| ------------ | ------ | ------ | ------------------------- |
| Query Length | uint32 | 4      | Length of query data      |
| Query Data   | bytes  | n      | Encoded query (see below) |

### Query Encoding

Individual queries are encoded using a binary format defined by the `QueryInput` structure:

| Offset         | Length | Description                    |
| -------------- | ------ | ------------------------------ |
| 0              | 4      | Length of the query ID         |
| 4              | n      | Query ID (unique identifier)   |
| 4 + n          | 4      | Length of the transaction ID   |
| 8 + n          | m      | Transaction ID (optional)      |
| 8 + n + m      | 4      | Length of the SQL statement    |
| 12 + n + m     | p      | SQL statement                  |
| 12 + n + m + p | 4      | Length of the parameters array |
| 16 + n + m + p | q      | Encoded parameters             |

**Notes:**

- All multi-byte integers use Little Endian encoding
- If transaction ID is not provided, its length is set to 0
- Parameters are encoded using SQLite3 parameter encoding format

### Server Responses

The server responds with `QueryStreamFrame` messages containing `QueryStreamFrameEntry` responses:

#### Response Frame Format

| Field         | Type   | Length | Description             |
| ------------- | ------ | ------ | ----------------------- |
| Message Type  | uint8  | 1      | `0x04`                  |
| Length        | uint32 | 4      | Length of response data |
| Response Data | bytes  | n      | Multiple frame entries  |

#### Frame Entry Structure

Each response entry within the frame:

| Field         | Type   | Length | Description                        |
| ------------- | ------ | ------ | ---------------------------------- |
| Entry Type    | uint8  | 1      | `0x05` (success) or `0x03` (error) |
| Entry Length  | uint32 | 4      | Length of response data            |
| Response Data | bytes  | n      | Encoded query response             |

### Error Handling

When an error occurs, the server sends a `QueryStreamError` message:

| Field         | Type   | Length | Description             |
| ------------- | ------ | ------ | ----------------------- |
| Message Type  | uint8  | 1      | `0x03`                  |
| Length        | uint32 | 4      | Length of error message |
| Error Message | string | n      | Human-readable error    |

### Connection Termination

To close the connection, the client sends a `QueryStreamCloseConnection` message:

| Field        | Type   | Length | Description |
| ------------ | ------ | ------ | ----------- |
| Message Type | uint8  | 1      | `0x02`      |
| Length       | uint32 | 4      | 0 (no data) |

## Limitations

- **Single Database per Connection**: Each LQTP connection is bound to a specific database and branch
- **Authentication Required**: All connections must be authenticated with valid access keys
- **HTTP Dependency**: The protocol runs over HTTP and requires HTTP/1.1 upgrade support
- **Connection State**: Connections maintain state and must be properly closed to avoid resource leaks

## Implementation Notes

- Messages are read in chunks of up to 1024 bytes for efficiency
- The server uses buffer pools to minimize memory allocations
- All operations are protected by mutexes for thread safety
- Context cancellation is supported for graceful connection termination
- The protocol supports full-duplex communication for bidirectional streaming
