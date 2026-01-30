package vector_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestVectorHammingDistance(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		t.Run("IdenticalVectors", func(t *testing.T) {
			// Create two identical bit vectors
			var vec1, vec2 []byte

			result1, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, -1.0, 1.0, -1.0, 1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			vec1 = result1.Rows[0][0].Blob()

			result2, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, -1.0, 1.0, -1.0, 1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			vec2 = result2.Rows[0][0].Blob()

			// Compute Hamming distance
			var distance int

			res, err := db.GetConnection().Exec("SELECT vector_hamming_distance(?, ?)", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}, {Type: sqlite3.ParameterTypeBlob, Value: vec2}})

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			distance = int(res.Rows[0][0].Int64())

			if distance != 0 {
				t.Errorf("Expected hamming distance of 0 for identical vectors, got %d", distance)
			}

			t.Logf("✓ Identical vectors: hamming distance = %d", distance)
		})

		t.Run("CompletelyDifferent", func(t *testing.T) {
			// All positive vs all negative
			var vec1, vec2 []byte

			res1, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			vec1 = res1.Rows[0][0].Blob()

			res2, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			vec2 = res2.Rows[0][0].Blob()

			var distance int

			res, err := db.GetConnection().Exec("SELECT vector_hamming_distance(?, ?)", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}, {Type: sqlite3.ParameterTypeBlob, Value: vec2}})

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			distance = int(res.Rows[0][0].Int64())

			// All 8 bits should differ
			if distance != 8 {
				t.Errorf("Expected hamming distance of 8, got %d", distance)
			}

			t.Logf("✓ Completely different vectors (8-D): hamming distance = %d", distance)
		})

		t.Run("PartialDifference", func(t *testing.T) {
			var vec1, vec2 []byte

			res1, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			vec1 = res1.Rows[0][0].Blob()

			res2, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			vec2 = res2.Rows[0][0].Blob()

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			var distance int

			res, err := db.GetConnection().Exec("SELECT vector_hamming_distance(?, ?)", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}, {Type: sqlite3.ParameterTypeBlob, Value: vec2}})

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			distance = int(res.Rows[0][0].Int64())

			// 2 bits differ (positions 2 and 3)
			if distance != 2 {
				t.Errorf("Expected hamming distance of 2, got %d", distance)
			}

			t.Logf("✓ Partial difference (8-D): hamming distance = %d", distance)
		})

		t.Run("LargeVector768D", func(t *testing.T) {
			// Build 768-dimensional vectors - one with alternating pattern
			var vec1JSON, vec2JSON string

			// vec1: all 1.0 (all bits set)
			// vec2: alternating 1.0, -1.0 (half bits set)
			vec1JSON = "["

			for i := range 768 {
				if i > 0 {
					vec1JSON += ", "
				}

				vec1JSON += "1.0"
			}

			vec1JSON += "]"

			vec2JSON = "["

			for i := range 768 {
				if i > 0 {
					vec2JSON += ", "
				}

				if i%2 == 0 {
					vec2JSON += "1.0"
				} else {
					vec2JSON += "-1.0"
				}
			}

			vec2JSON += "]"

			var vec1, vec2 []byte

			res1, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32(?))", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeText, Value: []byte(vec1JSON)}})

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			vec1 = res1.Rows[0][0].Blob()

			res2, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32(?))", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeText, Value: []byte(vec2JSON)}})

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			vec2 = res2.Rows[0][0].Blob()

			var distance int

			res, err := db.GetConnection().Exec("SELECT vector_hamming_distance(?, ?)", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: vec1}, {Type: sqlite3.ParameterTypeBlob, Value: vec2}})

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			distance = int(res.Rows[0][0].Int64())

			// Half the bits should differ (384 out of 768)
			if distance != 384 {
				t.Errorf("Expected hamming distance of 384, got %d", distance)
			}

			t.Logf("✓ Large vector (768-D): hamming distance = %d (50%% difference)", distance)
		})

		t.Run("SimilaritySearch", func(t *testing.T) {
			// Create a table with bit vectors
			_, err := db.GetConnection().Exec(`
				CREATE TEMP TABLE IF NOT EXISTS bit_vectors (
					id INTEGER PRIMARY KEY,
					vec BLOB
				)
			`, nil)

			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Insert some bit vectors
			vectors := []string{
				"[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]",         // 8 bits set
				"[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]",     // 4 bits set
				"[1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0]",     // 4 bits set (different pattern)
				"[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]", // 0 bits set
			}

			for i, vec := range vectors {
				var bitVec []byte

				res, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32(?))", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeText, Value: []byte(vec)}})

				if err != nil {
					t.Fatalf("Failed to quantize vector %d: %v", i, err)
				}

				bitVec = res.Rows[0][0].Blob()

				_, err = db.GetConnection().Exec("INSERT INTO bit_vectors (id, vec) VALUES (?, ?)", []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeInteger, Value: int64(i + 1)}, {Type: sqlite3.ParameterTypeBlob, Value: bitVec}})

				if err != nil {
					t.Fatalf("Failed to insert vector %d: %v", i, err)
				}
			}

			// Query vector (same as first vector)
			var queryVec []byte

			res, err := db.GetConnection().Exec("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]'))", nil)

			if err != nil {
				t.Fatalf("Failed to create query vector: %v", err)
			}

			queryVec = res.Rows[0][0].Blob()

			// Find nearest neighbors by Hamming distance
			res, err = db.GetConnection().Exec(`
				SELECT id, vector_hamming_distance(vec, ?) as distance
				FROM bit_vectors
				ORDER BY distance
				LIMIT 3
			`, []sqlite3.StatementParameter{{Type: sqlite3.ParameterTypeBlob, Value: queryVec}})

			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}

			var results []struct {
				id       int
				distance int
			}

			for _, row := range res.Rows {
				var id, distance int

				id = int(row[0].Int64())
				distance = int(row[1].Int64())

				results = append(results, struct {
					id       int
					distance int
				}{id, distance})
			}

			// First result should be id=1 (exact match) with distance 0
			if len(results) < 1 || results[0].id != 1 || results[0].distance != 0 {
				t.Errorf("Expected first result to be id=1 with distance=0, got id=%d distance=%d",
					results[0].id, results[0].distance)
			}

			t.Logf("✓ Similarity search results:")

			for _, r := range results {
				t.Logf("  - ID %d: hamming distance = %d", r.id, r.distance)
			}
		})
	})
}
