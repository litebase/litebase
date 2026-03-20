# vector_index Virtual Table

`vector_index` is a SQLite virtual table that provides k-nearest
neighbour (k-NN) similarity search using hierarchical Inverted File
(IVF) indexing. It supports multiple vector columns, configurable
distance metrics, and automatic quantization for compressed storage.

## Contents

- [Overview](#overview)
- [Creating a Vector Index](#creating-a-vector-index)
- [Column Definitions](#column-definitions)
- [Index Parameters](#index-parameters)
- [Shadow Tables](#shadow-tables)
- [Inserting Vectors](#inserting-vectors)
- [Querying](#querying)
- [Multi-Column Indexes](#multi-column-indexes)
- [Architecture](#architecture)
- [Insert Buffer](#insert-buffer)
- [Cluster Management](#cluster-management)
- [Storage Types](#storage-types)

## Overview

`vector_index` wraps one or more vector columns from a base table
and builds an IVF cluster tree on top of them. Insertions are
buffered in memory and flushed together for efficiency. Cluster
splits occur asynchronously after each commit.

The virtual table is backed by C code in `virtual_table_index.c`
with Go callbacks for the clustering logic.

## Creating a Vector Index

```sql
CREATE VIRTUAL TABLE docs_idx USING vector_index(
    embedding BLOB,
    dimensions = 1536,
    distance_metric = cosine
);
```

This statement:

1. Creates the virtual table `docs_idx`
2. Creates shadow tables for vectors, cluster tree, and metadata
3. Initialises a root cluster (id = 1) with no centroid

The `BLOB` keyword in the column definition marks `embedding` as a
vector column. The index is associated with the table name derived
from the virtual table name (with the `_idx` suffix stripped if
present, otherwise used verbatim).

## Column Definitions

A column definition in `CREATE VIRTUAL TABLE` can be either:

- **Vector column**: `col_name BLOB` — indexed vector column
- **Non-vector column**: `col_name TYPE` (any non-BLOB type) —
  pass-through column stored in the shadow `_vectors` table

Example with mixed columns:

```sql
CREATE VIRTUAL TABLE docs_idx USING vector_index(
    id INTEGER,
    title TEXT,
    embedding BLOB,
    dimensions = 1536,
    distance_metric = cosine
);
```

## Index Parameters

All parameters are specified as `key = value` in the column list.
Parameters are not real columns and are stripped from the schema.

### Global Parameters

| Parameter         | Default   | Description                          |
| ----------------- | --------- | ------------------------------------ |
| `dimensions`      | required  | Vector dimensions (max 4096)         |
| `distance_metric` | `l2`      | `cosine`, `l2`, or `dot`             |
| `max_cluster_size`| `5000`    | Max vectors per leaf before split    |
| `min_cluster_size`| `200`     | Min vectors for a valid split        |
| `storage_type`    | `float32` | `float32`, `float16`, or `int8`      |

### Per-Column Parameters

When using multiple vector columns with different configurations:

```sql
CREATE VIRTUAL TABLE multi_idx USING vector_index(
    embedding_en BLOB,
    embedding_fr BLOB,
    dimensions = 768,
    embedding_fr_dimensions = 512,
    distance_metric = cosine,
    embedding_fr_distance_metric = l2
);
```

The pattern is `{col_name}_{parameter_name}`.

## Shadow Tables

`vector_index` creates four shadow tables automatically.

### `{name}_vectors`

Stores the raw vector data and any non-vector metadata columns.

```sql
CREATE TABLE IF NOT EXISTS {name}_vectors (
    rowid    INTEGER PRIMARY KEY,
    {col1}   BLOB,   -- vector blobs
    {col2}   TEXT,   -- non-vector pass-through columns
    ...
);
```

### `{name}_{col}_cluster_tree`

Stores the IVF cluster hierarchy for each vector column.

```sql
CREATE TABLE IF NOT EXISTS {name}_{col}_cluster_tree (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id    INTEGER,
    centroid_blob BLOB,
    is_leaf      INTEGER NOT NULL DEFAULT 1,
    cluster_size INTEGER NOT NULL DEFAULT 0,
    radius       REAL
);
```

The root node always has `id = 1` and `parent_id = NULL`.

### `{name}_{col}_cluster_vector_map`

Maps each vector to its assigned leaf cluster.

```sql
CREATE TABLE IF NOT EXISTS {name}_{col}_cluster_vector_map (
    vector_id  INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    distance   REAL
);
```

### `{name}_metadata`

Stores index configuration as key-value pairs.

```sql
CREATE TABLE IF NOT EXISTS {name}_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT
);
```

## Inserting Vectors

Insert into the virtual table exactly like a regular table:

```sql
INSERT INTO docs_idx (rowid, embedding)
VALUES (42, vector_f32('[0.1, 0.2, ...]'));
```

Inserts are collected in an in-memory buffer. The buffer is flushed:

- When it reaches capacity (dimension-adaptive, up to 10,922 rows)
- At `xSync` (SAVEPOINT) time
- At `xCommit` time

After a commit, cluster split detection runs via `goTriggerClusterSplits`.

## Querying

Query the virtual table using standard `SELECT` with a `MATCH`
clause or a `WHERE` equality on a vector column. Pass the encoded
query vector as a BLOB parameter:

```sql
-- Find 10 most similar documents
SELECT rowid
FROM docs_idx
WHERE embedding MATCH vector_f32('[0.1, 0.2, ...]')
LIMIT 10;
```

Results are returned ordered by distance (ascending).

## Multi-Column Indexes

A single `vector_index` can cover several vector columns. Each
column gets its own cluster tree and vector map:

```sql
CREATE VIRTUAL TABLE product_idx USING vector_index(
    image_vec   BLOB,
    text_vec    BLOB,
    image_vec_dimensions   = 512,
    text_vec_dimensions    = 768,
    image_vec_distance_metric = cosine,
    text_vec_distance_metric  = cosine
);

INSERT INTO product_idx (rowid, image_vec, text_vec)
VALUES (
    1,
    vector_f32('[...]'),  -- 512-D
    vector_f32('[...]')   -- 768-D
);
```

## Architecture

`vector_index` uses a hierarchical IVF structure:

```text
Root cluster (id = 1)
├── Cluster 2 (leaf)
│     ├── vector 10
│     └── vector 11
├── Cluster 3 (non-leaf)
│     ├── Cluster 5 (leaf)
│     │     └── vector 20
│     └── Cluster 6 (leaf)
│           └── vector 21
└── Cluster 4 (leaf)
      └── vector 12
```

**Assignment**: When flushing the insert buffer, `goAssignVectorsInBatch`
traverses the cluster tree from the root with `findBestCluster`,
greedily descending to the leaf cluster whose centroid is nearest
to each vector.

**Centroid update**: After assignment, `goUpdateClusterStats`
recomputes each touched leaf's centroid as the running mean of all
its vectors.

**Splitting**: After commit, `goTriggerClusterSplits` checks all
leaves whose `cluster_size > max_cluster_size`. Oversized leaves
are split by k-means into two children.

## Insert Buffer

The insert buffer avoids per-row I/O overhead. Its capacity is
computed at virtual table creation time by
`compute_insert_buffer_capacity()`:

```text
target_bytes = 128 MiB
row_bytes    = dimensions × 4   (float32 storage before quantization)
capacity     = target_bytes / row_bytes
capacity     = clamp(capacity, 64, 10922)
```

10,922 is `SQLITE_MAX_VARIABLE_NUMBER / 3`, which prevents exceeding
the SQLite bind-parameter limit in the batch INSERT statement.

**Example capacities:**

| Dimensions | Row size | Buffer rows |
| ---------- | -------- | ----------- |
| 128        | 512 B    | 10,922      |
| 768        | 3,072 B  | 10,922      |
| 1,536      | 6,144 B  | 10,922      |
| 3,072      | 12,288 B | 10,922      |
| 4,096      | 16,384 B | 8,192       |

At 4,096 dimensions the capacity drops below 10,922 because
`128 MiB / 16,384 = 8,192`.

During `xBegin`, the SQLite cache is increased to 64 MiB
(`PRAGMA cache_size = -65536`) and restored at the end of `xCommit`.

## Cluster Management

### Finding the Best Cluster

`findBestCluster` in `cluster.go` traverses the tree top-down:

1. Start at the root (id = 1).
2. At a non-leaf node, compare the vector's distance to each child
   centroid; descend into the nearest child.
3. Stop at a leaf node — that is the assigned cluster.

### Statement Caching

Prepared SQLite statements for cluster-tree reads and centroid
updates are cached in `sync.Map` structures keyed by the database
pointer + table/column name pair. This avoids re-preparation on
every flush.

### Post-Commit Splits

After each commit the database calls `goTriggerClusterSplits` via
the `VectorIndexManagerInterface`. The manager schedules a
background job that:

1. Reads all leaf nodes with `cluster_size > max_cluster_size`.
2. Splits each oversize leaf into two children using k-means
   (k = 2).
3. Reassigns vectors from the old leaf to the new children.
4. Marks the old leaf as non-leaf and inserts the two new leaves.

## Storage Types

The index can store vectors in a compressed format set by
`storage_type`. Quantization happens during `flush_insert_buffer`
in C via `quantize_vector_blob()`.

| `storage_type` | Format  | Size vs Float32 |
| -------------- | ------- | --------------- |
| `float32`      | Float32 | 1× (baseline)   |
| `float16`      | Float16 | 0.5×            |
| `int8`         | Int8    | 0.25×           |

Quantization from Float32 to Float16 or Int8 is lossy. Distance
computations use the decoded float32 values internally.

## See Also

- [Type Support](type-support.md) - Vector data types and storage
- [Distance Metrics](distance-metrics.md) - Supported distance
  algorithms
- [BLOB Format](blob-format.md) - Binary encoding specification
- [Integration](integration.md) - SQLite extension wiring
- [Performance](performance.md) - Tuning tips
