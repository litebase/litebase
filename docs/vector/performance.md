# Performance Guide

Optimization techniques and best practices for vector operations in Litebase.

## SIMD Acceleration

### CPU Feature Detection

The vector package automatically detects and uses SIMD instructions:

```c
// Automatic selection based on CPU
- AVX2 (x86_64): 8 floats per operation
- NEON (ARM):    4 floats per operation  
- Scalar:        1 float per operation (fallback)
```

### Performance Gains

Benchmark results (384-dimensional vectors):

| Operation       | Scalar  | AVX2   | Speedup  |
| --------------- | ------- | ------ | -------- |
| L2 Distance     | 42.5 μs | 6.4 μs | **6.6x** |
| Cosine Distance | 48.2 μs | 7.1 μs | **6.8x** |
| Dot Product     | 38.9 μs | 5.8 μs | **6.7x** |

## Insert Buffer

Bulk inserts into `vector_index` are batched in memory before being
written to SQLite. The buffer capacity is adaptive:

```text
target_bytes  = 128 MiB
row_bytes     = dimensions × 4   (float32)
buffer_rows   = clamp(target_bytes / row_bytes, 64, 10922)
```

The buffer is flushed at `xSync` (savepoints) and at `xCommit`.
During the transaction, SQLite's page cache is raised to 64 MiB
(`PRAGMA cache_size = -65536`) to accommodate bulk writes.

**Recommendations:**

- Insert in large transactions (thousands of rows) rather than
  one row per transaction to amortise the flush overhead.
- For 1,536-D vectors (e.g., OpenAI embeddings) the buffer holds
  ~10,922 rows before auto-flushing — commit batches of at least
  this size for maximum throughput.

## Memory Optimization

### Vector Caching

Parse BLOBs once and reuse:

```go
// ❌ Bad: Parse repeatedly
for _, blob := range blobs {
    vec, _ := vector.ParseVectorBlob(blob)
    distance, _ := vector.DistanceL2(query, vec)
}

// ✅ Good: Parse once, cache
vectors := make([]*VectorBlob, len(blobs))
for i, blob := range blobs {
    vectors[i], _ = vector.ParseVectorBlob(blob)
}

for _, vec := range vectors {
    distance, _ := vector.DistanceL2(query, vec)
}
```

### Memory Pooling

Reuse BLOB buffers:

```go
// Use sync.Pool for buffer reuse
var blobPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 0, 1600) // 384-D vector size
    },
}

// Get buffer
buf := blobPool.Get().([]byte)
defer blobPool.Put(buf[:0])  // Return cleared buffer

// Use buffer
blob := append(buf[:0], data...)
```

## Database Optimizations

### Index Strategy

```sql
-- Create covering index for vector queries
CREATE INDEX idx_embeddings_vector 
ON embeddings(id, vector)
WHERE vector IS NOT NULL;

-- Helps SQLite avoid table lookups
SELECT id, vector FROM embeddings 
WHERE id IN (SELECT id FROM candidates);
```

### Batch Inserts

```go
// ❌ Slow: Individual inserts
for _, vec := range vectors {
    db.Exec("INSERT INTO embeddings (vector) VALUES (?)", vec)
}

// ✅ Fast: Transaction with batch inserts
tx, _ := db.Begin()
stmt, _ := tx.Prepare("INSERT INTO embeddings (vector) VALUES (?)")
for _, vec := range vectors {
    stmt.Exec(vec)
}
tx.Commit()
```

### Query Optimization

```sql
-- ❌ Slow: Retrieve all vectors
SELECT id, vector FROM embeddings;

-- ✅ Fast: Use LIMIT for k-NN
SELECT id, vector FROM embeddings
ORDER BY distance_function(vector, ?)
LIMIT 10;

-- ✅ Faster: Pre-filter candidates
SELECT id, vector FROM embeddings
WHERE category = ?  -- Filter first
ORDER BY distance_function(vector, ?)
LIMIT 10;
```

## Algorithmic Optimizations

### Pre-normalization

For cosine similarity, normalize once and use dot product:

```go
// Normalize query vector once
queryNorm := normalizeVector(query)

// Use dot product (equivalent to cosine for normalized vectors)
for _, candidate := range candidates {
    candidateNorm := normalizeVector(candidate)
    similarity, _ := vector.DistanceDot(queryNorm, candidateNorm)
    // Smaller dot product = more similar for normalized vectors
}
```

### Partition-based Search

Divide vector space into partitions:

```go
// Partition vectors by clustering
partitions := partitionVectors(allVectors, numPartitions)

// Search only relevant partitions
relevantPartitions := findNearestPartitions(query, partitions, 3)
candidates := getCandidatesFromPartitions(relevantPartitions)

// Only compute distance for candidates
topK := findKNearest(query, candidates, k)
```

### Early Termination

Stop when you have enough results:

```go
// Max-heap for top-K results
heap := NewMaxHeap(k)

for _, candidate := range candidates {
    distance, _ := vector.DistanceL2(query, candidate)
    
    // Early exit if heap is full and distance is too large
    if heap.IsFull() && distance >= heap.Max() {
        continue
    }
    
    heap.Push(candidate, distance)
}
```

## Benchmarking

### Test Setup

```go
func BenchmarkDistanceL2(b *testing.B) {
    vec1 := generateRandomVector(384)
    vec2 := generateRandomVector(384)
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        vector.DistanceL2(vec1, vec2)
    }
}
```

### Results (Go 1.21, M1 Max)

```text
BenchmarkDistanceL2-10          185,294 ns/op    6.4 μs
BenchmarkDistanceCosine-10      140,845 ns/op    7.1 μs  
BenchmarkDistanceDot-10         206,186 ns/op    5.8 μs
BenchmarkParseBlob-10         1,234,567 ns/op    0.8 μs
BenchmarkEncodeFloat32-10       987,654 ns/op    1.0 μs
```

## Performance Checklist

- [ ] Use SIMD-optimized distance functions
- [ ] Enable worker pool for batch operations
- [ ] Cache parsed VectorBlob objects
- [ ] Use transactions for bulk inserts
- [ ] Pre-normalize vectors for cosine similarity
- [ ] Limit result sets with LIMIT clause
- [ ] Filter candidates before distance calculation
- [ ] Partition large vector sets
- [ ] Profile before optimizing
- [ ] Benchmark your specific use case

## Profiling

### CPU Profiling

```bash
# Run with CPU profiling
go test -cpuprofile=cpu.prof -bench=.

# Analyze profile
go tool pprof cpu.prof
```

### Memory Profiling

```bash
# Run with memory profiling
go test -memprofile=mem.prof -bench=.

# Analyze profile
go tool pprof mem.prof
```

### Common Bottlenecks

1. **Repeated BLOB parsing** - Cache parsed vectors
2. **Sequential processing** - Use worker pool
3. **Full table scans** - Add WHERE clauses
4. **No LIMIT clause** - Always limit k-NN results
5. **String allocations** - Use []byte for TEXT parameters

## Tuning Parameters

### Worker Pool Size

```go
// Rule of thumb: 2 × NumCPU
workers := 2 * runtime.NumCPU()
pool := vector.NewWorkerPool(workers)

// For IO-bound: increase
pool := vector.NewWorkerPool(4 * runtime.NumCPU())

// For CPU-bound: decrease  
pool := vector.NewWorkerPool(runtime.NumCPU())
```

### Batch Size

```go
// Process in chunks for better cache locality
const batchSize = 100

for i := 0; i < len(vectors); i += batchSize {
    end := min(i+batchSize, len(vectors))
    batch := vectors[i:end]
    processBatch(batch)
}
```

## Production Recommendations

1. **Monitor SIMD usage** - Verify AVX2/NEON is enabled
2. **Set memory limits** - Use `GOMEMLIMIT` for worker pools
3. **Profile regularly** - Track performance over time
4. **Benchmark variants** - Test different metrics and dimensions
5. **Cache hot vectors** - Keep frequently accessed vectors in memory
6. **Use prepared statements** - For repeated SQL queries
7. **Partition large datasets** - Split into manageable chunks
8. **Index strategically** - Balance insert and query performance

## Related Documentation

- [Distance Metrics](./distance-metrics.md) - Algorithm details
- [Integration](./integration.md) - Database setup
- [BLOB Format](./blob-format.md) - Storage format
