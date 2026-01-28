# Vector Type Support

Litebase's vector extension supports multiple data types to accommodate different use cases and memory/performance trade-offs.

## Supported Vector Types

### Float32 (Default)

- **SQL Function**: `vector_f32(json_array)`
- **Storage**: 4 bytes per element
- **Precision**: Single-precision floating point (≈7 decimal digits)
- **Range**: ±3.4 × 10³⁸
- **Use Cases**:
  - General-purpose embeddings (most common)
  - OpenAI embeddings
  - Hugging Face model outputs
  - Balanced precision and memory usage

```sql
INSERT INTO documents (embedding) 
VALUES (vector_f32('[0.1, 0.2, 0.3]'));
```

### Float64 (High Precision)

- **SQL Function**: `vector_f64(json_array)`
- **Storage**: 8 bytes per element
- **Precision**: Double-precision floating point (≈15 decimal digits)
- **Range**: ±1.7 × 10³⁰⁸
- **Use Cases**:
  - Scientific computing requiring high precision
  - Financial data
  - Astronomical calculations
  - When precision loss is unacceptable

```sql
INSERT INTO scientific_data (vector) 
VALUES (vector_f64('[3.141592653589793, 2.718281828459045]'));
```

### Int8 (Quantized, Memory-Efficient)

- **SQL Function**: `vector_int8(json_array)`
- **Storage**: 1 byte per element
- **Precision**: Integer values
- **Range**: -128 to 127
- **Use Cases**:
  - Product quantization
  - Binary/ternary quantized embeddings
  - Extreme memory efficiency (8x smaller than float32)
  - Post-training quantization

```sql
-- Store quantized embeddings (e.g., scaled to -128..127)
INSERT INTO quantized_embeddings (vector) 
VALUES (vector_int8('[120, -45, 0, 89, -127]'));
```

### Int16 (Medium Precision)

- **SQL Function**: `vector_int16(json_array)`
- **Storage**: 2 bytes per element
- **Precision**: Integer values
- **Range**: -32,768 to 32,767
- **Use Cases**:
  - Medium-precision quantization
  - Fixed-point arithmetic
  - Moderate memory savings (2x smaller than float32)
  - Better precision than int8 with reasonable memory usage

```sql
INSERT INTO medium_precision (vector) 
VALUES (vector_int16('[15000, -8000, 0, 20000]'));
```

## BLOB Format

All vector types share the same BLOB format structure:

```text
Byte 0:     Version (0x01)
Byte 1:     Type (0x01=float32, 0x02=float64, 0x03=int8, 0x04=int16)
Bytes 2-5:  Dimensions (uint32, little-endian)
Bytes 6+:   Data (type-specific)
```

### Storage Calculations

For a vector with `D` dimensions:

- **Float32**: `6 + (D × 4)` bytes
  - Example: 128-D vector = 518 bytes
- **Float64**: `6 + (D × 8)` bytes
  - Example: 128-D vector = 1,030 bytes
- **Int8**: `6 + D` bytes
  - Example: 128-D vector = 134 bytes (74% smaller than float32!)
- **Int16**: `6 + (D × 2)` bytes
  - Example: 128-D vector = 262 bytes (49% smaller than float32)

## Type Safety

The Go API provides type-safe access to vector data:

```go
blob, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
vecBlob, _ := vector.ParseVectorBlob(blob)

// Returns []float32 if type matches, nil otherwise
floats := vecBlob.GetFloat32Slice()

// Returns nil because blob is Float32, not Float64
wrongType := vecBlob.GetFloat64Slice() // nil
```

## Performance Characteristics

| Type    | Memory | Precision | Speed       | Best For                |
|---------|--------|-----------|-------------|-------------------------|
| Float32 | 4B/dim | Medium    | Fast        | General embeddings      |
| Float64 | 8B/dim | Highest   | Slower      | High-precision needs    |
| Int8    | 1B/dim | Lowest    | Fastest     | Quantized embeddings    |
| Int16   | 2B/dim | Low       | Very Fast   | Medium quantization     |

## Choosing a Type

1. **Use Float32** (default) for:
   - Standard ML embeddings
   - OpenAI, Cohere, Anthropic vectors
   - When you're unsure

2. **Use Float64** when:
   - Precision is critical
   - Dealing with scientific data
   - Accumulating many operations

3. **Use Int8** when:
   - Memory is extremely constrained
   - Using quantized models
   - Speed is critical and precision loss is acceptable
   - Working with binary/ternary embeddings

4. **Use Int16** when:
   - Need better precision than int8
   - Still want memory savings
   - Implementing fixed-point arithmetic

## Distance Metrics

All distance functions work with all vector types:

```sql
-- L2 (Euclidean) distance
SELECT vector_l2_distance(
    vector_int8('[120, -45, 0]'),
    vector_int8('[100, -30, 5]')
);

-- Cosine distance
SELECT vector_cosine_distance(
    vector_f64('[1.0, 2.0, 3.0]'),
    vector_f64('[4.0, 5.0, 6.0]')
);

-- Inner product (dot product)
SELECT vector_dot_distance(
    vector_f32('[0.5, 0.3, 0.2]'),
    vector_f32('[0.7, 0.2, 0.1]')
);
```

**Note**: Mixing types in distance calculations is supported (e.g., comparing float32 to int8), but the types are converted internally which may impact performance.

## Migration Between Types

You can convert between types in SQL:

```sql
-- Convert float32 to int8 (with scaling)
UPDATE embeddings 
SET quantized = vector_int8(
    json_group_array(
        CAST(value * 127 AS INTEGER)
    )
)
FROM json_each(embedding);

-- Convert int8 back to float32 (with rescaling)
SELECT vector_f32(
    json_group_array(
        CAST(value AS REAL) / 127.0
    )
)
FROM json_each(quantized);
```

## Example: Product Quantization

Here's how to implement product quantization using int8:

```sql
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    -- Original embedding
    embedding BLOB,
    -- Quantized to int8 for faster search
    quantized BLOB,
    -- Store scaling factors for reconstruction
    scale_factor REAL,
    min_value REAL
);

-- Insert with quantization
INSERT INTO embeddings (embedding, quantized, scale_factor, min_value)
SELECT 
    original,
    vector_int8(
        json_group_array(
            CAST((value - min_val) * 255.0 / (max_val - min_val) - 128 AS INTEGER)
        )
    ),
    (max_val - min_val) / 255.0,
    min_val
FROM (
    SELECT 
        vector_f32('[0.1, 0.5, 0.9, 0.3]') AS original,
        MIN(value) AS min_val,
        MAX(value) AS max_val
    FROM json_each(vector_f32('[0.1, 0.5, 0.9, 0.3]'))
);

-- Fast approximate search using int8
SELECT id, vector_l2_distance(quantized, vector_int8('[...query...]'))
FROM embeddings
ORDER BY 2
LIMIT 10;
```

## See Also

- [BLOB Format](blob-format.md) - Detailed binary format specification
- [Distance Metrics](distance-metrics.md) - Distance calculation algorithms
- [Performance Guide](performance.md) - Optimization techniques
- [API Reference](api-reference.md) - Complete Go API documentation
