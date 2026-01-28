# Distance Metrics

Litebase supports three distance metrics for vector similarity, all SIMD-optimized for performance.

## Supported Metrics

### 1. L2 (Euclidean) Distance

Measures straight-line distance between two points in vector space.

**Formula:**

```math
d(a, b) = \sqrt{\sum_i (a_i - b_i)^2}
```

**Properties:**

- Range: [0, ∞)
- Smaller values = more similar
- Sensitive to magnitude
- Most intuitive distance metric

**Use Cases:**

- Image similarity
- Spatial data
- General-purpose similarity

**Example:**

```go
distance, err := vector.DistanceL2(vec1, vec2)
// For [1,0,0] and [0,1,0]: distance = √2 ≈ 1.414
```

**SQL Usage:**

```sql
SELECT id, vector_f32('[1.0, 0.0, 0.0]') as query
FROM embeddings
ORDER BY distance_l2(vector, query) ASC
LIMIT 10;
```

---

### 2. Cosine Distance

Measures angle between vectors, ignoring magnitude.

**Formula:**

```math
similarity(a, b) = \frac{a \cdot b}{\|a\| \times \|b\|}
distance(a, b) = 1 - similarity(a, b)
```

**Properties:**

- Range: [0, 2]
- 0 = identical direction
- 1 = orthogonal
- 2 = opposite direction
- Magnitude-independent

**Use Cases:**

- Text embeddings (word2vec, BERT)
- Document similarity
- Recommendation systems
- When magnitude is not meaningful

**Example:**

```go
distance, err := vector.DistanceCosine(vec1, vec2)
// For [1,0,0] and [0,1,0]: distance = 1.0 (orthogonal)
// For [1,0,0] and [2,0,0]: distance = 0.0 (same direction)
```

---

### 3. Dot Product

Measures alignment and magnitude together.

**Formula:**

```math
\text{dot}(a, b) = \sum_i a_i \times b_i
\text{distance}(a, b) = -\text{dot}(a, b)  \quad \text{(Negated for sorting)}
```

**Properties:**

- Range: (-∞, ∞)
- More negative = more similar
- Considers both direction and magnitude
- Fastest to compute

**Use Cases:**

- Pre-normalized vectors
- Maximum Inner Product Search (MIPS)
- Neural network layers
- When magnitude matters

**Example:**

```go
distance, err := vector.DistanceDot(vec1, vec2)
// For [1,0,0] and [0,1,0]: distance = 0.0 (orthogonal)
// For [1,0,0] and [2,0,0]: distance = -2.0 (aligned)
```

## Comparison Table

| Metric      | Best For           | Normalized Vectors | Time Complexity | SIMD Speedup |
| ----------- | ------------------ | ------------------ | --------------- | ------------ |
| L2          | General similarity | No                 | O(n)            | 4-8x         |
| Cosine      | Text embeddings    | Optional           | O(n)            | 4-8x         |
| Dot Product | Pre-normalized     | Yes                | O(n)            | 4-8x         |

## SIMD Optimization

All distance functions use SIMD instructions when available:

- **x86_64**: AVX2 (256-bit)
- **ARM**: NEON (128-bit)
- **Fallback**: Scalar implementation

### Performance Comparison

Test: 1000 distance calculations, 384-D vectors

| Implementation | Time (ms) | Speedup |
| -------------- | --------- | ------- |
| Scalar C       | 45.2      | 1.0x    |
| SIMD (AVX2)    | 6.8       | 6.6x    |
| SIMD (NEON)    | 11.3      | 4.0x    |

## API Reference

### Go Functions

```go
// Compute distance with automatic metric selection
distance, err := vector.ComputeDistance(vec1, vec2, "l2")
distance, err := vector.ComputeDistance(vec1, vec2, "cosine")
distance, err := vector.ComputeDistance(vec1, vec2, "dot")

// Direct metric calls
l2Dist, err := vector.DistanceL2(vec1, vec2)
cosDist, err := vector.DistanceCosine(vec1, vec2)
dotDist, err := vector.DistanceDot(vec1, vec2)
```

### C Functions (Internal)

```c
// Called via CGO from Go
float compute_distance_l2(const float* a, const float* b, int dims);
float compute_distance_cosine(const float* a, const float* b, int dims);
float compute_distance_dot(const float* a, const float* b, int dims);
```

## Choosing a Metric

### Decision Tree

```text
Are vectors pre-normalized to unit length?
├─ YES → Use Dot Product (fastest)
└─ NO
    ├─ Does magnitude matter?
    │  ├─ YES → Use L2 Distance
    │  └─ NO → Use Cosine Distance
    └─ What data type?
        ├─ Text embeddings → Cosine Distance
        ├─ Images → L2 Distance
        └─ Audio → L2 Distance
```

### Recommendations

#### Text/Document Embeddings (BERT, OpenAI, etc.)

```go
distance, _ := vector.DistanceCosine(doc1, doc2)
```

#### Image Embeddings (ResNet, CLIP, etc.)

```go
distance, _ := vector.DistanceL2(img1, img2)
```

#### Pre-normalized Vectors

```go
distance, _ := vector.DistanceDot(vec1, vec2)
```

## Mathematical Properties

### Triangle Inequality

Only L2 distance satisfies the triangle inequality:

```math
d(a, c) \leq d(a, b) + d(b, c)
```

This property enables indexing structures like IVF (Inverted File Index).

### Symmetry

All metrics are symmetric:

```math
d(a, b) = d(b, a)
```

### Non-negativity

L2 and Cosine distances are always non-negative:

```math
d(a, b) \geq 0
```

Dot product can be negative.

## Implementation Details

### SIMD Processing

```c
// AVX2 example (8 floats at a time)
__m256 a_vec = _mm256_loadu_ps(&a[i]);
__m256 b_vec = _mm256_loadu_ps(&b[i]);
__m256 diff = _mm256_sub_ps(a_vec, b_vec);
__m256 sq = _mm256_mul_ps(diff, diff);
sum = _mm256_add_ps(sum, sq);
```

### Initialization

SIMD functions are initialized once:

```go
func InitDistanceFunctions() {
    C.init_distance_functions()  // Detects CPU features
}
```

### Error Handling

```go
distance, err := vector.DistanceL2(vec1, vec2)

if err == vector.ErrDimensionMismatch {
    // Vectors must have same dimensions
}
if err == vector.ErrUnsupportedMetric {
    // Invalid metric name
}
```

## Performance Tips

1. **Pre-normalize for cosine** if doing many comparisons

   ```go
   // Normalize once
   normalized := normalizeVector(vec)
   // Then use dot product (equivalent to cosine)
   for _, other := range candidates {
       dist, _ := vector.DistanceDot(normalized, other)
   }
   ```

2. **Use worker pool** for batch operations

   ```go
   pool := vector.GetWorkerPool()

   results := pool.Map(vectors, func(v *VectorBlob) float64 {
       return vector.DistanceL2(query, v)
   })
   ```

3. **Cache frequently used vectors** in parsed form

   ```go
   // Parse once
   queryBlob, _ := vector.ParseVectorBlob(queryData)
   
   // Reuse many times
   for _, candidateData := range candidates {
       candidate, _ := vector.ParseVectorBlob(candidateData)
       dist, _ := vector.DistanceL2(queryBlob, candidate)
   }
   ```

## References

- [SIMD Distance Calculations](https://www.intel.com/content/www/us/en/developer/articles/technical/vectorization-with-avx-512.html)
- [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity)
- [Euclidean Distance](https://en.wikipedia.org/wiki/Euclidean_distance)
- [Maximum Inner Product Search](https://arxiv.org/abs/1412.6576)
