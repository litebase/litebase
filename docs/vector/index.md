# Vector Package Documentation

The vector package provides high-performance vector operations for
Litebase, enabling semantic search, similarity matching, and k-NN
queries directly in SQLite.

## Contents

- [Overview](#overview) - Architecture and capabilities
- [Type Support](./type-support.md) - Float32, Float16, Float64,
  Int8, Int16, Bit, Sparse vector types
- [Vector Index](./vector-index.md) - k-NN index and similarity
  search with `vector_index`
- [Integration](./integration.md) - Database integration details
- [BLOB Format](./blob-format.md) - Binary vector encoding
  specification
- [Distance Metrics](./distance-metrics.md) - Supported similarity
  functions
- [Performance](./performance.md) - Optimization techniques
- [API Reference](./api-reference.md) - Go package API

## Overview

### Key Features

- **SIMD-Optimized Distance Calculations** - AVX2/NEON acceleration
  for L2, cosine, and dot product
- **SQLite Integration** - `vector_f32()`, `vector_f16()`,
  `vector_f64()`, `vector_int8()`, `vector_int16()`,
  `vector_bit()`, `vector_sparse()` SQL functions
- **k-NN Search** - `vector_index` virtual table with hierarchical
  IVF clustering for approximate nearest-neighbor queries
- **Efficient Storage** - Compact binary BLOB format with 6-byte
  header supporting 7 data types
- **Quantization** - Built-in compression to Float16, Int8, or Bit
  via SQL functions
- **Multi-column Indexes** - A single `vector_index` virtual table
  can index multiple vector columns simultaneously

### Architecture

```text
┌─────────────────────────────────────────────┐
│           SQLite Layer                      │
│  vector_*() SQL functions                   │
│  vector_index virtual table (k-NN)          │
└─────────────┬───────────────────────────────┘
              │
┌─────────────▼───────────────────────────────┐
│         Vector Package (Go)                 │
│  ┌────────────┐  ┌──────────────────────┐   │
│  │ Encoding   │  │ Distance Metrics     │   │
│  │ (BLOB)     │  │ (SIMD-optimized)     │   │
│  └────────────┘  └──────────────────────┘   │
│  ┌────────────┐  ┌──────────────────────┐   │
│  │ Cluster    │  │ k-NN Search          │   │
│  │ Tree (IVF) │  │ (Heap + IVF)         │   │
│  └────────────┘  └──────────────────────┘   │
└─────────────┬───────────────────────────────┘
              │
┌─────────────▼───────────────────────────────┐
│      C Extensions (CGO)                     │
│  AVX2/NEON SIMD Distance Functions          │
│  vector_index Virtual Table (SQLite)        │
└─────────────────────────────────────────────┘
```

### Use Cases

1. **Semantic Search** - Find documents with similar meaning
2. **Image Similarity** - Match visually similar images
3. **Recommendation Systems** - Suggest related items
4. **Anomaly Detection** - Identify outliers in vector space
5. **Clustering** - Group similar vectors together

## Quick Start

### Storing Vectors

```sql
-- Create a vector index for semantic search
-- (dimensions=1536 for OpenAI text-embedding-3-small)
CREATE VIRTUAL TABLE doc_embeddings USING vector_index(
    embedding BLOB,
    embedding_dimensions=1536,
    distance_metric=cosine
);

-- Insert row with vector
INSERT INTO doc_embeddings (embedding)
VALUES (vector_f32('[0.1, 0.5, 0.9, ...]'));
```

### Searching

```sql
-- Find 10 nearest neighbors
SELECT id, distance
FROM doc_embeddings
WHERE embedding MATCH vector_f32('[0.2, 0.4, 0.8, ...]')
ORDER BY distance
LIMIT 10;
```

### Computing Distances

```go
// Parse vectors from BLOBs
vec1, _ := vector.ParseVectorBlob(blob1)
vec2, _ := vector.ParseVectorBlob(blob2)

// Compute distance (SIMD-accelerated)
distance, _ := vector.DistanceL2(vec1, vec2)
cosineDist, _ := vector.DistanceCosine(vec1, vec2)
dotProduct, _ := vector.DistanceDot(vec1, vec2)
```

## Performance Characteristics

| Operation        | Complexity   | SIMD Speedup      |
| ---------------- | ------------ | ----------------- |
| L2 Distance      | O(n)         | 4–8x faster       |
| Cosine Distance  | O(n)         | 4–8x faster       |
| Dot Product      | O(n)         | 4–8x faster       |
| Vector Encoding  | O(n)         | Linear            |
| k-NN Search      | O(log n)     | IVF approximation |

Where n = vector dimensions

## Limitations

- Maximum dimensions: 4,096 per vector
- Supported storage types: Float32, Float64, Float16, Int8, Int16,
  Bit, Sparse
- k-NN precision: approximate (hierarchical IVF); recall depends on
  cluster quality
- `vector_index` supports inserts and deletes; in-place updates are
  handled as a delete followed by an insert

## Next Steps

- Read [Vector Index Guide](./vector-index.md) for k-NN search setup
- Review [Integration Guide](./integration.md) for database setup
- Read [BLOB Format](./blob-format.md) for storage details
- Check [Distance Metrics](./distance-metrics.md) for algorithm
  details
- See [Performance Guide](./performance.md) for optimization tips
