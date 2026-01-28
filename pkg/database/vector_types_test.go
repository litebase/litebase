package database_test

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

func TestVectorTypes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("Float32", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_f32('[1.0, 2.5, 3.14159]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			if len(result.Rows[0]) == 0 {
				t.Fatal("Expected at least one column")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeFloat32 {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeFloat32, vecBlob.Type)
			}

			if vecBlob.Dimensions != 3 {
				t.Fatalf("Expected 3 dimensions, got %d", vecBlob.Dimensions)
			}

			floats := vecBlob.GetFloat32Slice()

			if floats == nil {
				t.Fatal("Expected float32 slice, got nil")
			}

			expected := []float32{1.0, 2.5, 3.14159}

			for i, v := range floats {
				if math.Abs(float64(v-expected[i])) > 0.0001 {
					t.Errorf("Value[%d]: expected %.5f, got %.5f", i, expected[i], v)
				}
			}

			t.Logf("✓ Float32 vector working! - Dimensions: %d, Values: %v", vecBlob.Dimensions, floats)
		})

		t.Run("Float64", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_f64('[1.0, 2.5, 3.141592653589793]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeFloat64 {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeFloat64, vecBlob.Type)
			}

			if vecBlob.Dimensions != 3 {
				t.Fatalf("Expected 3 dimensions, got %d", vecBlob.Dimensions)
			}

			floats := vecBlob.GetFloat64Slice()

			if floats == nil {
				t.Fatal("Expected float64 slice, got nil")
			}

			expected := []float64{1.0, 2.5, 3.141592653589793}

			for i, v := range floats {
				if math.Abs(v-expected[i]) > 0.000000000001 {
					t.Errorf("Value[%d]: expected %.15f, got %.15f", i, expected[i], v)
				}
			}

			t.Logf("✓ Float64 vector working! - Dimensions: %d, Values: %v", vecBlob.Dimensions, floats)
		})

		t.Run("Int8", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_int8('[-128, -1, 0, 1, 127]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeInt8 {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeInt8, vecBlob.Type)
			}

			if vecBlob.Dimensions != 5 {
				t.Fatalf("Expected 5 dimensions, got %d", vecBlob.Dimensions)
			}

			ints := vecBlob.GetInt8Slice()

			if ints == nil {
				t.Fatal("Expected int8 slice, got nil")
			}

			expected := []int8{-128, -1, 0, 1, 127}

			for i, v := range ints {
				if v != expected[i] {
					t.Errorf("Value[%d]: expected %d, got %d", i, expected[i], v)
				}
			}

			t.Logf("✓ Int8 vector working! - Dimensions: %d, Values: %v", vecBlob.Dimensions, ints)
		})

		t.Run("Int16", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_int16('[-32768, -1000, 0, 1000, 32767]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeInt16 {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeInt16, vecBlob.Type)
			}

			if vecBlob.Dimensions != 5 {
				t.Fatalf("Expected 5 dimensions, got %d", vecBlob.Dimensions)
			}

			ints := vecBlob.GetInt16Slice()

			if ints == nil {
				t.Fatal("Expected int16 slice, got nil")
			}

			expected := []int16{-32768, -1000, 0, 1000, 32767}

			for i, v := range ints {
				if v != expected[i] {
					t.Errorf("Value[%d]: expected %d, got %d", i, expected[i], v)
				}
			}

			t.Logf("✓ Int16 vector working! - Dimensions: %d, Values: %v", vecBlob.Dimensions, ints)
		})

		t.Run("Float16", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_f16('[1.0, 2.5, 3.14159]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeFloat16 {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeFloat16, vecBlob.Type)
			}

			if vecBlob.Dimensions != 3 {
				t.Fatalf("Expected 3 dimensions, got %d", vecBlob.Dimensions)
			}

			floats := vecBlob.GetFloat16AsFloat32()

			if floats == nil {
				t.Fatal("Expected float32 slice from f16, got nil")
			}

			expected := []float32{1.0, 2.5, 3.14159}

			for i, v := range floats {
				// Float16 has less precision, allow larger tolerance
				if math.Abs(float64(v-expected[i])) > 0.01 {
					t.Errorf("Value[%d]: expected %.5f, got %.5f", i, expected[i], v)
				}
			}

			// Verify BLOB size: 6 header + (3 dims × 2 bytes) = 12 bytes
			expectedSize := 6 + (3 * 2)

			if len(blob) != expectedSize {
				t.Errorf("Expected BLOB size %d, got %d", expectedSize, len(blob))
			}

			t.Logf("✓ Float16 vector working! - Dimensions: %d, Values: %v", vecBlob.Dimensions, floats)
			t.Logf("  BLOB size: %d bytes (50%% savings vs float32)", len(blob))
		})

		t.Run("BitVector", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			// Test with 10 bits
			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_bit('[1, 0, 1, 1, 0, 0, 1, 0, 1, 1]')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeBit {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeBit, vecBlob.Type)
			}

			if vecBlob.Dimensions != 10 {
				t.Fatalf("Expected 10 dimensions, got %d", vecBlob.Dimensions)
			}

			bits := vecBlob.GetBitVector()

			if bits == nil {
				t.Fatal("Expected bit vector, got nil")
			}

			expected := []bool{true, false, true, true, false, false, true, false, true, true}

			for i, v := range bits {
				if v != expected[i] {
					t.Errorf("Bit[%d]: expected %v, got %v", i, expected[i], v)
				}
			}

			// Verify BLOB size: 6 header + ((10+7)/8 = 2 bytes) = 8 bytes
			expectedSize := 6 + 2

			if len(blob) != expectedSize {
				t.Errorf("Expected BLOB size %d, got %d", expectedSize, len(blob))
			}

			t.Logf("✓ Bit vector working! - Dimensions: %d, Bits: %v", vecBlob.Dimensions, bits)
			t.Logf("  BLOB size: %d bytes (96%% savings vs float32!)", len(blob))
		})

		t.Run("SparseVector", func(t *testing.T) {
			db := test.MockDatabase(app)

			conn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			ctx := context.Background()

			// Sparse vector: dimension 1000, only 3 non-zero values
			stmt, err := conn.GetConnection().Prepare(ctx, "SELECT vector_sparse('{\"dim\": 1000, \"indices\": [0, 500, 999], \"values\": [0.5, 0.8, 0.3]}')")

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			err = stmt.Sqlite3Statement.Exec(result)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("Expected row from query")
			}

			blob := result.Rows[0][0].ColumnValue

			vecBlob, err := vector.ParseVectorBlob(blob)

			if err != nil {
				t.Fatal(err)
			}

			if vecBlob.Type != vector.VectorTypeSparse {
				t.Fatalf("Expected type %d, got %d", vector.VectorTypeSparse, vecBlob.Type)
			}

			if vecBlob.Dimensions != 1000 {
				t.Fatalf("Expected 1000 dimensions, got %d", vecBlob.Dimensions)
			}

			sparse := vecBlob.GetSparseVector()

			if sparse == nil {
				t.Fatal("Expected sparse vector, got nil")
			}

			if len(sparse.Indices) != 3 {
				t.Fatalf("Expected 3 indices, got %d", len(sparse.Indices))
			}

			expectedIndices := []uint32{0, 500, 999}
			expectedValues := []float32{0.5, 0.8, 0.3}

			for i := range sparse.Indices {
				if sparse.Indices[i] != expectedIndices[i] {
					t.Errorf("Index[%d]: expected %d, got %d", i, expectedIndices[i], sparse.Indices[i])
				}

				if math.Abs(float64(sparse.Values[i]-expectedValues[i])) > 0.0001 {
					t.Errorf("Value[%d]: expected %.2f, got %.2f", i, expectedValues[i], sparse.Values[i])
				}
			}

			// Sparse BLOB: 6 header + 4 (num_elements) + 3*(4+4) = 34 bytes
			// vs dense float32 1000-D would be 6 + 4000 = 4006 bytes!
			t.Logf("✓ Sparse vector working! - Dimensions: %d, Non-zero: %d", vecBlob.Dimensions, len(sparse.Indices))
			t.Logf("  BLOB size: %d bytes (vs %d for dense float32 = 99%% savings!)", len(blob), 6+1000*4)
		})

		t.Run("BLOBSizesComparison", func(t *testing.T) {
			dims := 128

			// Compare all types for same dimension count
			vec32 := make([]float32, dims)

			for i := range vec32 {
				vec32[i] = float32(i) / float32(dims)
			}

			blob32, _ := vector.EncodeFloat32(vec32)
			blob16, _ := vector.EncodeFloat16(vec32)

			bits := make([]bool, dims)

			for i := range bits {
				bits[i] = vec32[i] > 0.5
			}

			blobBit, _ := vector.EncodeBit(bits)

			// Sparse: only store non-zero values (simulate 10% sparsity)
			indices := []uint32{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110}
			values := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.5, 0.3}
			blobSparse, _ := vector.EncodeSparse(dims, indices, values)

			t.Logf("✓ Storage comparison for 128-D vector:")
			t.Logf("  Float32: %d bytes (baseline)", len(blob32))
			t.Logf("  Float16: %d bytes (%.0f%% savings)", len(blob16), 100.0*(1.0-float64(len(blob16))/float64(len(blob32))))
			t.Logf("  Bit:     %d bytes (%.0f%% savings)", len(blobBit), 100.0*(1.0-float64(len(blobBit))/float64(len(blob32))))
			t.Logf("  Sparse:  %d bytes (%.0f%% savings with 10%% density)", len(blobSparse), 100.0*(1.0-float64(len(blobSparse))/float64(len(blob32))))
		})

		t.Run("BLOBFormat", func(t *testing.T) {
			// Verify BLOB format for each type
			vec := []float32{42.5}
			blob, _ := vector.EncodeFloat32(vec)

			if blob[0] != 0x01 {
				t.Errorf("Version byte: expected 0x01, got 0x%02x", blob[0])
			}

			if blob[1] != vector.VectorTypeFloat32 {
				t.Errorf("Type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeFloat32, blob[1])
			}

			dims := binary.LittleEndian.Uint32(blob[2:6])

			if dims != 1 {
				t.Errorf("Dimensions: expected 1, got %d", dims)
			}

			// Verify float64 type byte
			vec64 := []float64{42.5}
			blob64, _ := vector.EncodeFloat64(vec64)

			if blob64[1] != vector.VectorTypeFloat64 {
				t.Errorf("Float64 type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeFloat64, blob64[1])
			}

			// Verify int8 type byte
			vec8 := []int8{42}
			blob8, _ := vector.EncodeInt8(vec8)

			if blob8[1] != vector.VectorTypeInt8 {
				t.Errorf("Int8 type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeInt8, blob8[1])
			}

			// Verify int16 type byte
			vec16 := []int16{42}
			blob16, _ := vector.EncodeInt16(vec16)

			if blob16[1] != vector.VectorTypeInt16 {
				t.Errorf("Int16 type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeInt16, blob16[1])
			}

			// Verify float16 type byte
			vecF16 := []float32{42.5}
			blobF16, _ := vector.EncodeFloat16(vecF16)

			if blobF16[1] != vector.VectorTypeFloat16 {
				t.Errorf("Float16 type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeFloat16, blobF16[1])
			}

			// Verify bit type byte
			vecBit := []bool{true, false, true}
			blobBit, _ := vector.EncodeBit(vecBit)

			if blobBit[1] != vector.VectorTypeBit {
				t.Errorf("Bit type byte: expected 0x%02x, got 0x%02x", vector.VectorTypeBit, blobBit[1])
			}

			t.Logf("✓ BLOB format verified!")
		})

		t.Run("TypeSafety", func(t *testing.T) {
			// Create a float32 vector
			vec32 := []float32{1.0, 2.0, 3.0}
			blob32, _ := vector.EncodeFloat32(vec32)

			parsed, _ := vector.ParseVectorBlob(blob32)

			// Verify that getting wrong type returns nil
			if parsed.GetFloat64Slice() != nil {
				t.Error("GetFloat64Slice should return nil for Float32 blob")
			}

			if parsed.GetInt8Slice() != nil {
				t.Error("GetInt8Slice should return nil for Float32 blob")
			}

			if parsed.GetInt16Slice() != nil {
				t.Error("GetInt16Slice should return nil for Float32 blob")
			}

			if parsed.GetFloat16Slice() != nil {
				t.Error("GetFloat16Slice should return nil for Float32 blob")
			}

			if parsed.GetFloat16AsFloat32() != nil {
				t.Error("GetFloat16AsFloat32 should return nil for Float32 blob")
			}

			if parsed.GetBitVector() != nil {
				t.Error("GetBitVector should return nil for Float32 blob")
			}

			if parsed.GetSparseVector() != nil {
				t.Error("GetSparseVector should return nil for Float32 blob")
			}

			// But correct type should work
			if parsed.GetFloat32Slice() == nil {
				t.Error("GetFloat32Slice should not return nil for Float32 blob")
			}

			// Test Float16 type safety
			vecF16 := []float32{1.0, 2.0, 3.0}
			blobF16, _ := vector.EncodeFloat16(vecF16)

			parsedF16, _ := vector.ParseVectorBlob(blobF16)

			// Should return nil for wrong types
			if parsedF16.GetFloat32Slice() != nil {
				t.Error("GetFloat32Slice should return nil for Float16 blob")
			}

			if parsedF16.GetBitVector() != nil {
				t.Error("GetBitVector should return nil for Float16 blob")
			}

			if parsedF16.GetSparseVector() != nil {
				t.Error("GetSparseVector should return nil for Float16 blob")
			}

			// But correct accessors should work
			if parsedF16.GetFloat16Slice() == nil {
				t.Error("GetFloat16Slice should not return nil for Float16 blob")
			}

			if parsedF16.GetFloat16AsFloat32() == nil {
				t.Error("GetFloat16AsFloat32 should not return nil for Float16 blob")
			}

			t.Logf("✓ Type safety working!")
		})

		t.Run("Float16Precision", func(t *testing.T) {
			// Test precision limits of float16
			testValues := []float32{
				0.0,
				1.0,
				-1.0,
				0.5,
				3.14159,
				65504.0,    // Max float16
				0.00006104, // Min positive normal float16
			}

			for _, original := range testValues {
				blob, _ := vector.EncodeFloat16([]float32{original})
				parsed, _ := vector.ParseVectorBlob(blob)
				converted := parsed.GetFloat16AsFloat32()

				// Float16 has limited precision
				tolerance := math.Abs(float64(original)) * 0.001 // 0.1% tolerance

				if tolerance < 0.0001 {
					tolerance = 0.0001
				}

				if math.Abs(float64(converted[0]-original)) > tolerance {
					t.Errorf("Float16 conversion: %.6f -> %.6f (error: %.6f)",
						original, converted[0], math.Abs(float64(converted[0]-original)))
				}
			}

			t.Logf("✓ Float16 precision verified")
		})
	})
}
