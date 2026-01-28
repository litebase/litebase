# Vector Package API Reference

Complete reference for the Go vector package API.

## Package: `github.com/litebase/litebase/pkg/vector`

### Constants

```go
const (
    VectorVersion1 byte = 0x01  // BLOB format version
    
    // Vector data types
    VectorTypeFloat32 byte = 0x01  // float32 - 4 bytes/dimension
    VectorTypeFloat64 byte = 0x02  // float64 - 8 bytes/dimension
    VectorTypeInt8    byte = 0x03  // int8 - 1 byte/dimension
    VectorTypeInt16   byte = 0x04  // int16 - 2 bytes/dimension
    VectorTypeFloat16 byte = 0x05  // float16 - 2 bytes/dimension (IEEE 754 binary16)
    VectorTypeBit     byte = 0x06  // bit - 1 bit/dimension (binary quantization)
    VectorTypeSparse  byte = 0x07  // sparse - variable size (index-value pairs)
    
    MaxDimensions = 4096  // Maximum vector dimensions
)

const (
    MetricL2      = "l2"      // Euclidean distance
    MetricCosine  = "cosine"  // Cosine distance
    MetricDot     = "dot"     // Dot product
    MetricHamming = "hamming" // Hamming distance (for bit vectors)
)
```

### Types

#### VectorBlob

```go
type VectorBlob struct {
    Version    byte    // Format version (0x01)
    Type       byte    // Data type (0x01-0x07)
    Dimensions int     // Number of dimensions
    Data       []byte  // Raw vector data
}
```

**Methods:**

```go
// Type-specific getters (return nil if type doesn't match)

// GetFloat32Slice returns float32 vector data
func (vb *VectorBlob) GetFloat32Slice() []float32

// GetFloat64Slice returns float64 vector data
func (vb *VectorBlob) GetFloat64Slice() []float64

// GetInt8Slice returns int8 vector data
func (vb *VectorBlob) GetInt8Slice() []int8

// GetInt16Slice returns int16 vector data
func (vb *VectorBlob) GetInt16Slice() []int16

// GetFloat16Slice returns raw uint16 float16 data
func (vb *VectorBlob) GetFloat16Slice() []uint16

// GetFloat16AsFloat32 returns float16 data converted to float32
func (vb *VectorBlob) GetFloat16AsFloat32() []float32

// GetBitSlice returns packed bit data (8 bits per byte)
func (vb *VectorBlob) GetBitSlice() []byte

// GetBitVector returns unpacked bit vector as []bool
func (vb *VectorBlob) GetBitVector() []bool

// GetSparseVector returns sparse vector with index-value pairs
func (vb *VectorBlob) GetSparseVector() *SparseVector
```

**Example:**

```go
// Float32 vector
blob32, _ := vector.ParseVectorBlob(data)
floats := blob32.GetFloat32Slice()  // []float32{1.0, 2.0, 3.0}

// Float16 vector (auto-converted to float32)
blob16, _ := vector.ParseVectorBlob(data)
converted := blob16.GetFloat16AsFloat32()  // []float32{1.0, 2.5, 3.0}

// Bit vector
blobBit, _ := vector.ParseVectorBlob(data)
bits := blobBit.GetBitVector()  // []bool{true, false, true, ...}

// Sparse vector
blobSparse, _ := vector.ParseVectorBlob(data)
sparse := blobSparse.GetSparseVector()
// sparse.Indices: []uint32{0, 100, 500}
// sparse.Values: []float32{0.5, 0.8, 0.3}
```

#### SparseVector

```go
type SparseVector struct {
    Indices []uint32   // Non-zero indices
    Values  []float32  // Non-zero values
}
```

**Description:** Represents a sparse vector storing only non-zero elements. Useful for high-dimensional vectors with few non-zero values (e.g., BM25 term vectors).

**Example:**

```go
// Create sparse 1000-D vector with 3 non-zero values
sparse := &vector.SparseVector{
    Indices: []uint32{0, 500, 999},
    Values:  []float32{0.5, 0.8, 0.3},
}

blob, _ := vector.EncodeSparse(1000, sparse.Indices, sparse.Values)
```

#### WorkerPool

```go
type WorkerPool struct {
    // Internal fields (not exported)
}
```

**Methods:**

```go
// NewWorkerPool creates a pool with specified worker count
func NewWorkerPool(numWorkers int) *WorkerPool

// Shutdown stops all workers gracefully
func (wp *WorkerPool) Shutdown()

// ProcessVectors applies function to each vector in parallel
func (wp *WorkerPool) ProcessVectors(
    vectors []*VectorBlob,
    fn func(*VectorBlob) float64,
) []float64
```

### Functions

#### Encoding/Decoding

```go
// EncodeFloat32 encodes float32 slice to BLOB
func EncodeFloat32(vec []float32) ([]byte, error)

// EncodeFloat64 encodes float64 slice to BLOB
func EncodeFloat64(vec []float64) ([]byte, error)

// EncodeInt8 encodes int8 slice to BLOB
func EncodeInt8(vec []int8) ([]byte, error)

// EncodeInt16 encodes int16 slice to BLOB
func EncodeInt16(vec []int16) ([]byte, error)

// EncodeFloat16 encodes float32 slice to float16 BLOB (with precision loss)
func EncodeFloat16(vec []float32) ([]byte, error)

// EncodeBit encodes bool slice to bit-packed BLOB
func EncodeBit(vec []bool) ([]byte, error)

// EncodeBitFromFloat32 quantizes float32 values to bits using threshold
func EncodeBitFromFloat32(vec []float32, threshold float32) ([]byte, error)

// EncodeSparse encodes sparse vector to BLOB
// dims: total dimensions, indices: non-zero positions, values: non-zero values
func EncodeSparse(dims int, indices []uint32, values []float32) ([]byte, error)
```

**Parameters:**

**EncodeFloat32/Float64:**

- `vec`: Float slice (1 to 4096 elements)
- Returns: Binary BLOB in Version 1 format
- Error: `ErrInvalidDimensions` if vec is empty or too large

**EncodeInt8/Int16:**

- `vec`: Integer slice (1 to 4096 elements)
- Returns: Binary BLOB with quantized values
- Error: `ErrInvalidDimensions` if vec is empty or too large

**EncodeFloat16:**

- `vec`: Float32 slice to convert to float16 (1 to 4096 elements)
- Returns: Binary BLOB with IEEE 754 binary16 values
- Note: Precision loss due to 16-bit representation
- Error: `ErrInvalidDimensions` if vec is empty or too large

**EncodeBit:**

- `vec`: Boolean slice (1 to 32768 elements)
- Returns: Bit-packed BLOB (8 bits per byte)
- Error: `ErrInvalidDimensions` if vec is empty or too large

**EncodeBitFromFloat32:**

- `vec`: Float32 slice to quantize
- `threshold`: Value above which bits are set to 1
- Returns: Bit-packed BLOB
- Use case: Convert dense embeddings to binary for 96% storage savings

**EncodeSparse:**

- `dims`: Total vector dimensions (e.g., 10000)
- `indices`: Positions of non-zero values (must be sorted)
- `values`: Non-zero values (same length as indices)
- Returns: Sparse BLOB format
- Error: `ErrInvalidDimensions`, `ErrInvalidSparseVector` if indices/values mismatch
- Use case: Hybrid BM25+semantic search with high-dimensional sparse vectors

**Example:**

```go
// Float32
vec32 := []float32{1.0, 2.0, 3.0}
blob32, _ := vector.EncodeFloat32(vec32)

// Float16 (50% smaller)
blob16, _ := vector.EncodeFloat16(vec32)

// Bit vector (96% smaller)
bits := []bool{true, false, true}
blobBit, _ := vector.EncodeBit(bits)

// Or quantize from float32
blobBit2, _ := vector.EncodeBitFromFloat32(vec32, 0.5)

// Sparse vector (variable size)
blobSparse, _ := vector.EncodeSparse(
    1000,                        // Total dimensions
    []uint32{0, 100, 999},       // Non-zero indices
    []float32{0.5, 0.8, 0.3},    // Non-zero values
)
```

---

```go
// ParseVectorBlob decodes BLOB to VectorBlob
func ParseVectorBlob(blob []byte) (*VectorBlob, error)
```

**Parameters:**

- `blob`: Binary BLOB data

**Returns:**

- Parsed `VectorBlob` struct
- Errors: `ErrInvalidBlobFormat`, `ErrUnsupportedVersion`, `ErrInvalidDimensions`

**Example:**

```go
vecBlob, err := vector.ParseVectorBlob(blob)
if err != nil {
    log.Fatal(err)
}

fmt.Println(vecBlob.Dimensions)  // 3
```

---

#### JSON Parsing

```go
// ParseJSONArray parses JSON array string to float32 slice
func ParseJSONArray(jsonStr string) ([]float32, error)

// ParseJSONArrayFloat16 parses JSON array and encodes as float16 BLOB
func ParseJSONArrayFloat16(jsonStr string) ([]byte, error)

// ParseJSONArrayBit parses JSON boolean array and encodes as bit BLOB
func ParseJSONArrayBit(jsonStr string) ([]byte, error)

// ParseJSONSparse parses JSON object with sparse vector format
// Expected format: {"dim": 1000, "indices": [0, 100], "values": [0.5, 0.8]}
func ParseJSONSparse(jsonStr string) ([]byte, error)
```

**Parameters:**

- `jsonStr`: JSON string representation

**Returns:**

- Encoded BLOB (for Float16, Bit, Sparse variants)
- Float32 slice (for ParseJSONArray)
- `ErrTooManyDimensions` if exceeds `MaxDimensions`

**Example:**

```go
// Float32 array
values, _ := vector.ParseJSONArray("[1.0, 2.0, 3.0]")
// values: []float32{1.0, 2.0, 3.0}

// Float16 BLOB
blobF16, _ := vector.ParseJSONArrayFloat16("[1.0, 2.5, 3.14]")

// Bit BLOB
blobBit, _ := vector.ParseJSONArrayBit("[1, 0, 1, 1, 0]")

// Sparse BLOB
blobSparse, _ := vector.ParseJSONSparse(`{
    "dim": 1000,
    "indices": [0, 500, 999],
    "values": [0.5, 0.8, 0.3]
}`)
```

#### Distance Functions

```go
// ComputeDistance calculates distance using specified metric
func ComputeDistance(a, b *VectorBlob, metric string) (float64, error)
```

**Parameters:**

- `a`, `b`: Vector BLOBs (must have matching dimensions and compatible types)
- `metric`: Distance metric
  - `"l2"` - Euclidean distance (float types)
  - `"cosine"` - Cosine distance (float types)
  - `"dot"` - Dot product (float types)
  - `"hamming"` - Hamming distance (bit vectors only)

**Returns:**

- Distance value
- Errors: `ErrDimensionMismatch`, `ErrUnsupportedMetric`, `ErrTypeMismatch`

**Type Compatibility:**

- Float32, Float64, Float16: All float metrics (l2, cosine, dot)
- Int8, Int16: L2 metric only
- Bit: Hamming metric only
- Sparse: Dot product only (cosine distance can be computed from dot)

**Example:**

```go
// Euclidean distance between float vectors
dist, _ := vector.ComputeDistance(vec1, vec2, vector.MetricL2)

// Hamming distance between bit vectors
hamming, _ := vector.ComputeDistance(bitVec1, bitVec2, vector.MetricHamming)

// Cosine similarity for sparse vectors
// Note: Compute dot product, then use sqrt(dot(a,a)) * sqrt(dot(b,b))
dotProd, _ := vector.ComputeDistance(sparse1, sparse2, vector.MetricDot)
```

---

```go
// DistanceL2 calculates Euclidean distance
func DistanceL2(a, b *VectorBlob) (float64, error)
```

**Formula:** `√(Σ(aᵢ - bᵢ)²)`

**Example:**

```go
// [1,0,0] and [0,1,0]
dist, _ := vector.DistanceL2(vec1, vec2)
// dist ≈ 1.414 (√2)
```

---

```go
// DistanceCosine calculates cosine distance
func DistanceCosine(a, b *VectorBlob) (float64, error)
```

**Formula:** `1 - (a·b) / (||a|| × ||b||)`

**Range:** [0, 2]

**Example:**

```go
// [1,0,0] and [0,1,0]
dist, _ := vector.DistanceCosine(vec1, vec2)
// dist = 1.0 (orthogonal)
```

---

```go
// DistanceDot calculates negative dot product
func DistanceDot(a, b *VectorBlob) (float64, error)
```

**Formula:** `-Σ(aᵢ × bᵢ)`

**Note:** Negated for similarity ordering (smaller = more similar)

**Example:**

```go
// [1,0,0] and [2,0,0]
dist, _ := vector.DistanceDot(vec1, vec2)
// dist = -2.0 (aligned)
```

#### Validation

```go
// ValidateDimensions checks if vectors have matching dimensions
func ValidateDimensions(a, b *VectorBlob) error
```

**Returns:**

- `nil` if dimensions match
- `ErrDimensionMismatch` if different

**Example:**

```go
if err := vector.ValidateDimensions(vec1, vec2); err != nil {
    log.Fatal("Dimension mismatch")
}
```

#### Worker Pool

```go
// GetWorkerPool returns global worker pool instance
func GetWorkerPool() *WorkerPool
```

**Returns:**

- Global worker pool (created if needed)
- Default size: `2 × NumCPU`

**Example:**

```go
pool := vector.GetWorkerPool()
results := pool.ProcessVectors(vectors, computeFunc)
```

---

```go
// ShutdownWorkerPool stops global worker pool
func ShutdownWorkerPool()
```

**Example:**

```go
defer vector.ShutdownWorkerPool()
```

#### Initialization

```go
// InitDistanceFunctions initializes SIMD distance functions
func InitDistanceFunctions()
```

**Note:** Called automatically, but can be called explicitly

**Example:**

```go
vector.InitDistanceFunctions()  // Detects AVX2/NEON
```

### Errors

```go
var (
    ErrInvalidBlobFormat  = errors.New("invalid vector blob format")
    ErrUnsupportedVersion = errors.New("unsupported vector version")
    ErrInvalidDimensions  = errors.New("invalid dimensions")
    ErrDimensionMismatch  = errors.New("dimension mismatch")
    ErrTooManyDimensions  = errors.New("too many dimensions")
    ErrUnsupportedMetric  = errors.New("unsupported distance metric")
)
```

### Complete Example

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/litebase/litebase/pkg/vector"
)

func main() {
    // Parse JSON to vectors
    vec1Data, _ := vector.ParseJSONArray("[1.0, 2.0, 3.0]")
    vec2Data, _ := vector.ParseJSONArray("[4.0, 5.0, 6.0]")
    
    // Encode to BLOBs
    blob1, _ := vector.EncodeFloat32(vec1Data)
    blob2, _ := vector.EncodeFloat32(vec2Data)
    
    // Parse BLOBs (simulating database retrieval)
    vec1, _ := vector.ParseVectorBlob(blob1)
    vec2, _ := vector.ParseVectorBlob(blob2)
    
    // Calculate distances
    l2, _ := vector.DistanceL2(vec1, vec2)
    cosine, _ := vector.DistanceCosine(vec1, vec2)
    dot, _ := vector.DistanceDot(vec1, vec2)
    
    fmt.Printf("L2 Distance: %.4f\n", l2)
    fmt.Printf("Cosine Distance: %.4f\n", cosine)
    fmt.Printf("Dot Product: %.4f\n", dot)
    
    // Batch processing with worker pool
    pool := vector.GetWorkerPool()
    defer pool.Shutdown()
    
    vectors := []*vector.VectorBlob{vec1, vec2}
    results := pool.ProcessVectors(vectors, func(v *vector.VectorBlob) float64 {
        dist, _ := vector.DistanceL2(v, vec1)
        return dist
    })
    
    fmt.Printf("Batch results: %v\n", results)
}
```

## CGO Exports

These functions are exported for SQLite extension use (not typically called directly):

```go
//export goEncodeVector
func goEncodeVector(jsonStr *C.char, blobLen *C.size_t) *C.uint8_t

//export goFreeVector
func goFreeVector(ptr *C.uint8_t)

//export goVectorScan
func goVectorScan(
    databaseID, branchID, tableName, columnName *C.char,
    queryVector *C.uint8_t, queryLen C.size_t,
    k C.int, metric *C.char,
) C.int64_t

//export goGetScanResult
func goGetScanResult(handle C.int64_t, rowid *C.int64_t, distance *C.double) C.int

//export goReleaseScanResults
func goReleaseScanResults(handle C.int64_t)
```

## Thread Safety

- ✅ **Thread-safe:** `ComputeDistance`, `DistanceL2`, `DistanceCosine`, `DistanceDot`
- ✅ **Thread-safe:** `ParseVectorBlob`, `EncodeFloat32`, `ParseJSONArray`
- ✅ **Thread-safe:** `WorkerPool` methods
- ⚠️ **Not thread-safe:** Individual `VectorBlob` mutations (read-only usage is safe)

## SQL Functions

The vector extension provides SQL functions for creating and querying vectors directly in SQLite.

### Vector Creation Functions

#### `vector_f32(json_array TEXT) → BLOB`

Creates a float32 vector from JSON array.

**Parameters:**

- `json_array`: JSON array string (e.g., `'[1.0, 2.0, 3.0]'`)

**Returns:** BLOB (type 0x01, 4 bytes/dimension)

**Example:**

```sql
SELECT vector_f32('[1.0, 2.0, 3.0]') AS embedding;
-- Returns BLOB: 01 01 03 00 00 00 00 00 80 3f 00 00 00 40 00 00 40 40
```

---

#### `vector_f64(json_array TEXT) → BLOB`

Creates a float64 vector from JSON array.

**Parameters:**

- `json_array`: JSON array string

**Returns:** BLOB (type 0x02, 8 bytes/dimension)

**Example:**

```sql
SELECT vector_f64('[1.0, 2.0, 3.0]') AS embedding;
```

---

#### `vector_int8(json_array TEXT) → BLOB`

Creates an int8 vector from JSON array. Values quantized to -128 to 127 range.

**Parameters:**

- `json_array`: JSON array string

**Returns:** BLOB (type 0x03, 1 byte/dimension)

**Example:**

```sql
SELECT vector_int8('[127, 0, -128]') AS quantized;
```

---

#### `vector_int16(json_array TEXT) → BLOB`

Creates an int16 vector from JSON array. Values quantized to -32768 to 32767 range.

**Parameters:**

- `json_array`: JSON array string

**Returns:** BLOB (type 0x04, 2 bytes/dimension)

**Example:**

```sql
SELECT vector_int16('[1000, 2000, 3000]') AS quantized;
```

---

#### `vector_f16(json_array TEXT) → BLOB`

Creates a float16 vector from JSON array. Uses IEEE 754 binary16 encoding (50% storage savings).

**Parameters:**

- `json_array`: JSON array string

**Returns:** BLOB (type 0x05, 2 bytes/dimension)

**Example:**

```sql
-- Store production embeddings with 50% less space
INSERT INTO embeddings (id, vector)
VALUES (1, vector_f16('[0.5, 0.8, 0.3, ...]'));

-- Size comparison
SELECT 
    length(vector_f32('[' || repeat('1.0,', 767) || '1.0]')) as f32_size,
    length(vector_f16('[' || repeat('1.0,', 767) || '1.0]')) as f16_size;
-- f32_size: 3078 bytes (6 header + 768*4)
-- f16_size: 1542 bytes (6 header + 768*2) → 50% savings!
```

---

#### `vector_bit(json_array TEXT) → BLOB`

Creates a bit vector from JSON boolean array. Binary quantization (96% storage savings).

**Parameters:**

- `json_array`: JSON array of 1/0 or true/false values

**Returns:** BLOB (type 0x06, 1 bit/dimension, packed 8 per byte)

**Example:**

```sql
-- Binary quantization for billion-scale search
INSERT INTO binary_embeddings (id, vector)
VALUES (1, vector_bit('[1, 0, 1, 1, 0, 0, 1, 0]'));

-- Size comparison for 768-D
SELECT 
    length(vector_f32('[' || repeat('1.0,', 767) || '1.0]')) as f32_size,
    length(vector_bit('[' || repeat('1,', 767) || '1]')) as bit_size;
-- f32_size: 3078 bytes
-- bit_size: 102 bytes (6 header + 96 data) → 96% savings!
```

---

#### `vector_sparse(json_object TEXT) → BLOB`

Creates a sparse vector from JSON object with indices and values. Ideal for hybrid search.

**Parameters:**

- `json_object`: JSON with `dim`, `indices`, `values` fields

**Returns:** BLOB (type 0x07, variable size)

**Example:**

```sql
-- Hybrid BM25 + semantic search
INSERT INTO hybrid_vectors (id, sparse_bm25, dense_semantic)
VALUES (
    1,
    vector_sparse('{
        "dim": 10000,
        "indices": [42, 1337, 9999],
        "values": [0.5, 0.8, 0.3]
    }'),
    vector_f32('[0.1, 0.2, ...]')  -- 768-D dense vector
);

-- Sparse is efficient: 10000-D with 3 non-zero = only 34 bytes!
-- vs dense float32 10000-D = 40006 bytes → 99.9% savings
```

---

### Vector Quantization Functions

Quantization functions convert float32 vectors to compressed formats. Use these for production workloads with millions of vectors.

#### `vector_quantize_int8(vec BLOB) → BLOB`

Quantizes float32 vector to 8-bit integers (74% storage savings).

**Parameters:**

- `vec`: Float32 vector BLOB (type 0x01)

**Returns:** Int8 vector BLOB (type 0x03, 1 byte/dimension)

**Use case:** Medium datasets (1M-10M vectors) requiring good accuracy with significant storage savings.

**Example:**

```sql
-- Quantize during insert
INSERT INTO embeddings (id, quantized_vec)
SELECT id, vector_quantize_int8(vector_f32(raw_json))
FROM source_data;

-- Storage comparison (768-D vector)
SELECT 
    length(vector_f32('[' || repeat('1.0,', 767) || '1.0]')) as original,
    length(vector_quantize_int8(vector_f32('[' || repeat('1.0,', 767) || '1.0]'))) as quantized;
-- original: 3078 bytes (6 + 768*4)
-- quantized: 774 bytes (6 + 768*1) → 74.9% savings!
```

---

#### `vector_quantize_int16(vec BLOB) → BLOB`

Quantizes float32 vector to 16-bit integers (50% storage savings).

**Parameters:**

- `vec`: Float32 vector BLOB (type 0x01)

**Returns:** Int16 vector BLOB (type 0x04, 2 bytes/dimension)

**Use case:** High-precision quantization when int8 loses too much accuracy.

**Example:**

```sql
-- Balance between size and accuracy
CREATE TABLE vectors_int16 AS
SELECT id, vector_quantize_int16(embedding) as quantized
FROM vectors_f32;

-- 768-D: 3078 → 1542 bytes (49.9% savings)
```

---

#### `vector_quantize_f16(vec BLOB) → BLOB`

Quantizes float32 vector to float16 (IEEE 754 binary16, 50% storage savings).

**Parameters:**

- `vec`: Float32 vector BLOB (type 0x01)

**Returns:** Float16 vector BLOB (type 0x05, 2 bytes/dimension)

**Use case:** Production ML workloads requiring minimal accuracy loss (< 0.01% error).

**Example:**

```sql
-- Best option for production (minimal accuracy loss)
CREATE TABLE production_vectors (
    id INTEGER PRIMARY KEY,
    embedding BLOB  -- float16 quantized
);

INSERT INTO production_vectors
SELECT id, vector_quantize_f16(original_f32_embedding)
FROM source_vectors;

-- Quality comparison
WITH original AS (
    SELECT embedding as f32_vec FROM vectors WHERE id = 1
),
quantized AS (
    SELECT vector_quantize_f16(f32_vec) as f16_vec FROM original
)
SELECT 
    vector_distance_cosine(o.f32_vec, o.f32_vec) as f32_self_distance,
    vector_distance_cosine(q.f16_vec, q.f16_vec) as f16_self_distance
FROM original o, quantized q;
-- Both ≈ 0.0 (near-zero difference)
```

---

#### `vector_quantize_bit(vec BLOB) → BLOB`

Quantizes float32 vector to binary (1 bit/dimension, 96% storage savings).

**Parameters:**

- `vec`: Float32 vector BLOB (type 0x01)

**Returns:** Bit vector BLOB (type 0x06, 1 bit/dimension)

**Use case:** Billion-scale filtering (stage 1), rerank with float32 (stage 2).

**Example:**

```sql
-- Billion-scale two-stage search
CREATE TABLE billion_vectors (
    id INTEGER PRIMARY KEY,
    binary_vec BLOB,    -- vector_bit for fast filtering
    precise_vec BLOB    -- vector_f32 for reranking
);

-- Insert with dual representation
INSERT INTO billion_vectors
SELECT 
    id,
    vector_quantize_bit(embedding) as binary_vec,
    embedding as precise_vec
FROM embeddings;

-- Fast filtering + precise reranking
WITH binary_candidates AS (
    SELECT id
    FROM billion_vectors
    WHERE vector_hamming_distance(binary_vec, 
            vector_quantize_bit(vector_f32('[...]'))) < 100
    LIMIT 1000
)
SELECT id, vector_distance_cosine(precise_vec, vector_f32('[...]')) as score
FROM billion_vectors
WHERE id IN (SELECT id FROM binary_candidates)
ORDER BY score
LIMIT 10;

-- 768-D storage: 3078 → 102 bytes (96.7% savings!)
```

---

### SQL Distance Functions

#### `vector_distance_l2(vec1 BLOB, vec2 BLOB) → REAL`

Calculates Euclidean (L2) distance between vectors.

**Formula:** `√(Σ(aᵢ - bᵢ)²)`

**Example:**

```sql
SELECT id, vector_distance_l2(embedding, vector_f32('[1.0, 0.0, 0.0]')) as distance
FROM vectors
ORDER BY distance
LIMIT 10;
```

---

#### `vector_distance_cosine(vec1 BLOB, vec2 BLOB) → REAL`

Calculates cosine distance (1 - similarity).

**Formula:** `1 - (a·b) / (||a|| × ||b||)`

**Example:**

```sql
SELECT id, vector_distance_cosine(embedding, query_vec) as distance
FROM vectors
ORDER BY distance
LIMIT 10;
```

---

#### `vector_distance_dot(vec1 BLOB, vec2 BLOB) → REAL`

Calculates negative dot product (for similarity ranking).

**Formula:** `-Σ(aᵢ × bᵢ)`

**Example:**

```sql
SELECT id, -vector_distance_dot(embedding, query_vec) as similarity
FROM vectors
ORDER BY similarity DESC
LIMIT 10;
```

---

#### `vector_hamming_distance(vec1 BLOB, vec2 BLOB) → INTEGER`

Calculates Hamming distance between bit vectors (XOR + POPCNT).

**Formula:** Number of differing bits

**Example:**

```sql
-- Billion-scale approximate search
SELECT id, vector_hamming_distance(binary_vec, query_bit_vec) as distance
FROM binary_embeddings
WHERE vector_hamming_distance(binary_vec, query_bit_vec) < 100
ORDER BY distance
LIMIT 100;

-- Then rerank top candidates with float32 precision
```

### Complete SQL Examples

#### Float16 Production Setup

```sql
-- Create table with f16 embeddings (50% storage savings)
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    embedding BLOB  -- vector_f16, 768-D = 1542 bytes vs 3078
);

-- Insert embeddings
INSERT INTO products (name, embedding)
VALUES 
    ('Product A', vector_f16('[0.1, 0.2, ..., 0.8]')),
    ('Product B', vector_f16('[0.3, 0.4, ..., 0.9]'));

-- Semantic search (works same as f32!)
SELECT id, name, 
       vector_distance_cosine(embedding, vector_f16('[0.2, 0.3, ..., 0.7]')) as score
FROM products
ORDER BY score
LIMIT 10;
```

#### Binary Quantization Billion-Scale

```sql
-- Two-stage search: binary filter → precision rerank
CREATE TABLE billion_vectors (
    id INTEGER PRIMARY KEY,
    binary_vec BLOB,    -- vector_bit for filtering (96% smaller)
    precise_vec BLOB    -- vector_f32 for reranking
);

-- Fast binary search
WITH binary_candidates AS (
    SELECT id, vector_hamming_distance(binary_vec, ?) as hamming
    FROM billion_vectors
    WHERE vector_hamming_distance(binary_vec, ?) < 100  -- Hamming threshold
    ORDER BY hamming
    LIMIT 1000  -- Top 1000 candidates
)
-- Precision rerank
SELECT v.id, vector_distance_cosine(v.precise_vec, ?) as score
FROM billion_vectors v
JOIN binary_candidates bc ON v.id = bc.id
ORDER BY score
LIMIT 10;
```

#### Hybrid BM25 + Semantic Search

```sql
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    text TEXT,
    bm25_sparse BLOB,    -- vector_sparse: term frequencies
    semantic_dense BLOB  -- vector_f32: semantic embedding
);

-- Hybrid search
WITH bm25_scores AS (
    SELECT id, vector_distance_dot(bm25_sparse, ?) * -1 as bm25
    FROM documents
),
semantic_scores AS (
    SELECT id, 1 - vector_distance_cosine(semantic_dense, ?) as semantic
    FROM documents
)
SELECT d.id, d.text,
       (b.bm25 * 0.7 + s.semantic * 0.3) as hybrid_score  -- Weighted fusion
FROM documents d
JOIN bm25_scores b ON d.id = b.id
JOIN semantic_scores s ON d.id = s.id
ORDER BY hybrid_score DESC
LIMIT 20;
```

### Type Selection Guide

| Type    | SQL Function      | Size/Dim    | Use Case               | Trade-off                     |
| ------- | ----------------- | ----------- | ---------------------- | ----------------------------- |
| Float32 | `vector_f32()`    | 4 bytes     | General purpose        | Baseline                      |
| Float64 | `vector_f64()`    | 8 bytes     | High precision science | 2× space                      |
| Int8    | `vector_int8()`   | 1 byte      | Quantized (PQ)         | 75% savings, accuracy loss    |
| Int16   | `vector_int16()`  | 2 bytes     | Medium quantization    | 50% savings                   |
| Float16 | `vector_f16()`    | 2 bytes     | Production ML          | **50% savings, minimal loss** |
| Bit     | `vector_bit()`    | 0.125 bytes | Billion-scale filter   | **96% savings, approximate**  |
| Sparse  | `vector_sparse()` | Variable    | Hybrid search          | **99%+ savings for sparse**   |

**Recommendation:**

- **Standard**: Use `vector_f32()` for < 10M vectors
- **Production**: Use `vector_f16()` for 10M-100M vectors (best balance)
- **Billion-scale**: Use `vector_bit()` for filtering + `vector_f32()` for reranking
- **Hybrid search**: Use `vector_sparse()` for BM25 + `vector_f32()` for semantic

## Performance Notes

- **SIMD acceleration** enabled automatically for AVX2/NEON
- **Memory allocation**: Minimize by reusing `VectorBlob` objects
- **Worker pool**: Use for batch operations (>100 vectors)
- **Dimension limit**: 4,096 enforced for memory safety

## See Also

- [Integration Guide](./integration.md)
- [Distance Metrics](./distance-metrics.md)
- [Performance Guide](./performance.md)
- [BLOB Format](./blob-format.md)
