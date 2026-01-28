# Vector Package Documentation

The vector package provides high-performance vector operations for Litebase, enabling semantic search, similarity matching, and k-NN queries directly in SQLite.

## Contents

- [Overview](#overview) - Architecture and capabilities
- [Type Support](./type-support.md) - Float32, Float16, Float64, Int8, Int16 vector types
- [Integration](./integration.md) - Database integration details
- [BLOB Format](./blob-format.md) - Binary vector encoding specification
- [Distance Metrics](./distance-metrics.md) - Supported similarity functions
- [Performance](./performance.md) - Optimization techniques
- [API Reference](./api-reference.md) - Go package API

## Overview

### Key Features

- **SIMD-Optimized Distance Calculations** - AVX2/NEON acceleration for L2, cosine, and dot product
- **SQLite Integration** - `vector_f32()`, `vector_f16()`, `vector_f64()`, `vector_int8()`, `vector_int16()` functions for vector encoding
- **Efficient Storage** - Compact binary BLOB format with version headers
- **Parallel Processing** - Worker pool for batch operations
- **k-NN Search** - Virtual table interface for nearest neighbor queries (planned)

### Architecture

```text
┌─────────────────────────────────────────┐
│           SQLite Layer                  │
│  vector_*() | vector_scan vtable        │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│         Vector Package (Go)             │
│  ┌────────────┐  ┌──────────────────┐   │
│  │ Encoding   │  │ Distance Metrics │   │
│  │ (BLOB)     │  │  (SIMD-optimized)│   │
│  └────────────┘  └──────────────────┘   │
│  ┌────────────┐  ┌──────────────────┐   │
│  │ Worker     │  │ k-NN Search      │   │
│  │ Pool       │  │ (Partitions/Heap)│   │
│  └────────────┘  └──────────────────┘   │
└─────────────┬───────────────────────────┘
              │
┌─────────────▼───────────────────────────┐
│      C Extensions (CGO)                 │
│  AVX2/NEON SIMD Distance Functions      │
└─────────────────────────────────────────┘
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
-- Create table with vector column
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    text TEXT,
    embedding BLOB
);

-- Insert vector (3-dimensional for example)
INSERT INTO embeddings (text, embedding) 
VALUES ('hello world', vector_f32('[0.1, 0.5, 0.9]'));
```

### Computing Distances

```go
// Parse vectors from BLOBs
vec1, _ := vector.ParseVectorBlob(blob1)
vec2, _ := vector.ParseVectorBlob(blob2)

// Calculate L2 distance
distance, _ := vector.DistanceL2(vec1, vec2)

// Calculate cosine similarity (distance)
cosineDist, _ := vector.DistanceCosine(vec1, vec2)

// Calculate dot product
dotProduct, _ := vector.DistanceDot(vec1, vec2)
```

### Batch Processing

```go
// Get worker pool
pool := vector.GetWorkerPool()

// Process vectors in parallel
results := pool.ProcessVectors(vectors, func(v *VectorBlob) float64 {
    return vector.ComputeDistance(v, queryVector, "l2")
})
```

## Performance Characteristics

| Operation       | Complexity | SIMD Speedup      |
| --------------- | ---------- | ----------------- |
| L2 Distance     | O(n)       | 4-8x faster       |
| Cosine Distance | O(n)       | 4-8x faster       |
| Dot Product     | O(n)       | 4-8x faster       |
| Vector Encoding | O(n)       | Linear            |
| k-NN Search     | O(n log k) | With partitioning |

Where n = vector dimensions, k = number of neighbors

## Limitations

- Maximum dimensions: 4,096 per vector
- Supported data type: `float32` only
- Memory footprint: 4 bytes × dimensions per vector
- Virtual table k-NN search: Not yet implemented

## Next Steps

- Read [Integration Guide](./integration.md) for database setup
- Review [BLOB Format](./blob-format.md) for storage details
- Check [Distance Metrics](./distance-metrics.md) for algorithm details
- See [Performance Guide](./performance.md) for optimization tips
