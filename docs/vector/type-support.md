# Vector Type Support

Litebase's vector extension supports multiple data types to
accommodate different use cases and memory/performance trade-offs.

## Supported Vector Types

### Float32 (Default)

- **SQL Function**: `vector_f32(json_array)`
- **Storage**: 4 bytes per element
- **Precision**: Single-precision floating point (~7 decimal digits)
- **Range**: ±3.4 × 10³⁸
- **Use Cases**:
  - General-purpose embeddings (most common)
  - OpenAI, Cohere, Anthropic embeddings
  - Balanced precision and memory usage

```sql
INSERT INTO documents (embedding)
VALUES (vector_f32('[0.1, 0.2, 0.3]'));
```

### Float64 (High Precision)

- **SQL Function**: `vector_f64(json_array)`
- **Storage**: 8 bytes per element
- **Precision**: Double-precision floating point (~15 decimal digits)
- **Range**: ±1.7 × 10³⁰⁸
- **Use Cases**:
  - Scientific computing requiring high precision
  - When precision loss from Float32 is unacceptable

```sql
INSERT INTO scientific_data (vector)
VALUES (vector_f64('[3.141592653589793, 2.718281828459045]'));
```

### Float16 (Half Precision)

- **SQL Function**: `vector_f16(json_array)`
- **Quantize Function**: `vector_quantize_f16(blob)`
- **Storage**: 2 bytes per element (IEEE 754 binary16)
- **Precision**: Half-precision floating point (~3 decimal digits)
- **Use Cases**:
  - 50% storage reduction vs Float32 with acceptable precision loss
  - LLM embeddings where slight precision loss is tolerable

```sql
-- Directly from JSON
INSERT INTO documents (vec16)
VALUES (vector_f16('[0.1, 0.2, 0.3]'));

-- Downgrade an existing float32 column
UPDATE documents
SET vec16 = vector_quantize_f16(embedding);
```

### Int8 (Quantized, Memory-Efficient)

- **SQL Function**: `vector_int8(json_array)`
- **Quantize Function**: `vector_quantize_int8(blob)`
- **Storage**: 1 byte per element
- **Range**: -128 to 127
- **Use Cases**:
  - 75% storage reduction vs Float32
  - Post-training quantized embeddings

```sql
INSERT INTO quantized_embeddings (vector)
VALUES (vector_int8('[120, -45, 0, 89, -127]'));
```

### Int16 (Medium Precision)

- **SQL Function**: `vector_int16(json_array)`
- **Quantize Function**: `vector_quantize_int16(blob)`
- **Storage**: 2 bytes per element
- **Range**: -32,768 to 32,767
- **Use Cases**:
  - Better precision than Int8 with 50% savings vs Float32

```sql
INSERT INTO medium_precision (vector)
VALUES (vector_int16('[15000, -8000, 0, 20000]'));
```

### Bit (Binary Quantization)

- **SQL Function**: `vector_bit(json_array)`
- **Quantize Function**: `vector_quantize_bit(blob)`
- **Storage**: 1 bit per element (packed 8 per byte)
- **Use Cases**:
  - Extreme compression (96% smaller than Float32)
  - Hamming-distance similarity search
  - Pre-filtering before exact re-ranking

```sql
-- From binary values
INSERT INTO bin_embeddings (vector)
VALUES (vector_bit('[1, 0, 1, 1, 0, 1, 0, 0]'));

-- Quantize an existing float32 column (sign-based)
UPDATE documents
SET vec_bit = vector_quantize_bit(embedding);

-- Hamming distance search
SELECT id,
  vector_hamming_distance(vec_bit,
    vector_quantize_bit(vector_f32('[...]'))
  ) AS dist
FROM bin_embeddings
ORDER BY dist
LIMIT 20;
```

### Sparse

- **SQL Function**: `vector_sparse(json_object)`
- **Storage**: Variable — only non-zero (index, value) pairs
- **Input Format**: `{"indices": [i0, i1, ...], "values": [v0, v1, ...]}`
- **Use Cases**:
  - High-dimensional sparse representations (BM25 term vectors)
  - TF-IDF vectors with most dimensions zero

```sql
INSERT INTO sparse_docs (vector)
VALUES (vector_sparse(
  '{"indices": [0, 500, 999], "values": [0.5, 0.8, 0.3]}'
));
```

## BLOB Format

All vector types share the same 6-byte header:

```text
Byte 0:     Version (0x01)
Byte 1:     Type code (0x01–0x07)
Bytes 2–5:  Dimensions (uint32, little-endian)
Bytes 6+:   Payload (type-specific)
```

### Storage Calculations

For a vector with `D` dimensions (and `nnz` non-zero elements for
Sparse):

| Type    | Total bytes        | Example (128-D)  |
| ------- | ------------------ | ---------------- |
| Float32 | `6 + D × 4`        | 518 bytes        |
| Float64 | `6 + D × 8`        | 1,030 bytes      |
| Float16 | `6 + D × 2`        | 262 bytes        |
| Int8    | `6 + D`            | 134 bytes        |
| Int16   | `6 + D × 2`        | 262 bytes        |
| Bit     | `6 + ⌈D / 8⌉`      | 22 bytes         |
| Sparse  | `6 + nnz × 4 × 2`  | Varies           |

## Type Safety

The Go API provides type-safe access to vector data:

```go
blob, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
vecBlob, _ := vector.ParseVectorBlob(blob)

// Returns []float32 only if type matches; nil otherwise
floats := vecBlob.GetFloat32Slice()

// Returns nil — blob is Float32, not Float64
wrongType := vecBlob.GetFloat64Slice() // nil

// Decode any supported type to float32 for distance computation:
// works for Float32, Float16, and Int8 without extra allocation
// for Float32
f32 := vecBlob.GetFloat32Decoded()
```

## Performance Characteristics

| Type    | Memory  | Precision | Speed     | Best For               |
| ------- | ------- | --------- | --------- | ---------------------- |
| Float32 | 4B/dim  | Medium    | Fast      | General embeddings     |
| Float64 | 8B/dim  | Highest   | Slower    | High-precision         |
| Float16 | 2B/dim  | Low-Med   | Fast      | Balanced compression   |
| Int8    | 1B/dim  | Lowest    | Fastest   | Quantized embeddings   |
| Int16   | 2B/dim  | Low       | Very Fast | Medium quantization    |
| Bit     | 0.125B  | Binary    | Fastest   | Coarse pre-filter      |
| Sparse  | Varies  | Float32   | Varies    | Sparse representations |

## Choosing a Type

1. **Use Float32** (default) for:
   - Standard ML embeddings (OpenAI, Cohere, Anthropic)
   - When unsure — provides the best compatibility

2. **Use Float64** when:
   - Precision is critical
   - Accumulating many vector operations

3. **Use Float16** when:
   - Memory is constrained but Float32 semantics are needed
   - 50% storage reduction with minimal quality loss

4. **Use Int8** when:
   - Memory is very constrained (4× smaller than Float32)
   - Using quantized/compressed models

5. **Use Bit** when:
   - Extreme compression is needed (~32× smaller than Float32)
   - Using binary or sign-quantized embeddings
   - Performing Hamming-distance pre-filtering

6. **Use Sparse** when:
   - Most dimensions are zero (e.g., BM25, TF-IDF)
   - Working with large vocabulary representations

## Quantization via SQL

Quantization functions convert an existing Float32 BLOB column to a
smaller type:

```sql
-- Float32 → Float16 (50% savings)
UPDATE documents SET vec16 = vector_quantize_f16(embedding);

-- Float32 → Int16 (50% savings, integer range)
UPDATE documents SET vec16i = vector_quantize_int16(embedding);

-- Float32 → Int8 (75% savings)
UPDATE documents SET vec8 = vector_quantize_int8(embedding);

-- Float32 → Bit (96% savings, sign-based)
UPDATE documents SET vec_bit = vector_quantize_bit(embedding);
```

## See Also

- [BLOB Format](blob-format.md) - Detailed binary format specification
- [Vector Index](vector-index.md) - k-NN search with vector_index
- [Distance Metrics](distance-metrics.md) - Distance calculation
  algorithms
- [API Reference](api-reference.md) - Complete Go API documentation
