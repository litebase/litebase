package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestVectorHammingDistance(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		t.Run("IdenticalVectors", func(t *testing.T) {
			// Create two identical bit vectors
			var vec1, vec2 []byte

			err := db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, -1.0, 1.0, -1.0, 1.0]'))").Scan(&vec1)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			err = db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, -1.0, 1.0, -1.0, 1.0]'))").Scan(&vec2)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			// Compute Hamming distance
			var distance int

			err = db.QueryRow("SELECT vector_hamming_distance(?, ?)", vec1, vec2).Scan(&distance)

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			if distance != 0 {
				t.Errorf("Expected hamming distance of 0 for identical vectors, got %d", distance)
			}

			t.Logf("✓ Identical vectors: hamming distance = %d", distance)
		})

		t.Run("CompletelyDifferent", func(t *testing.T) {
			// All positive vs all negative
			var vec1, vec2 []byte

			err := db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]'))").Scan(&vec1)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			err = db.QueryRow("SELECT vector_quantize_bit(vector_f32('[-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]'))").Scan(&vec2)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			var distance int

			err = db.QueryRow("SELECT vector_hamming_distance(?, ?)", vec1, vec2).Scan(&distance)

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			// All 8 bits should differ
			if distance != 8 {
				t.Errorf("Expected hamming distance of 8, got %d", distance)
			}

			t.Logf("✓ Completely different vectors (8-D): hamming distance = %d", distance)
		})

		t.Run("PartialDifference", func(t *testing.T) {
			var vec1, vec2 []byte

			err := db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0]'))").Scan(&vec1)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			err = db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]'))").Scan(&vec2)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			var distance int

			err = db.QueryRow("SELECT vector_hamming_distance(?, ?)", vec1, vec2).Scan(&distance)

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

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

			err := db.QueryRow("SELECT vector_quantize_bit(vector_f32(?))", vec1JSON).Scan(&vec1)

			if err != nil {
				t.Fatalf("Failed to create vector 1: %v", err)
			}

			err = db.QueryRow("SELECT vector_quantize_bit(vector_f32(?))", vec2JSON).Scan(&vec2)

			if err != nil {
				t.Fatalf("Failed to create vector 2: %v", err)
			}

			var distance int

			err = db.QueryRow("SELECT vector_hamming_distance(?, ?)", vec1, vec2).Scan(&distance)

			if err != nil {
				t.Fatalf("Failed to compute hamming distance: %v", err)
			}

			// Half the bits should differ (384 out of 768)
			if distance != 384 {
				t.Errorf("Expected hamming distance of 384, got %d", distance)
			}

			t.Logf("✓ Large vector (768-D): hamming distance = %d (50%% difference)", distance)
		})

		t.Run("SimilaritySearch", func(t *testing.T) {
			// Create a table with bit vectors
			_, err := db.Exec(`
				CREATE TEMP TABLE IF NOT EXISTS bit_vectors (
					id INTEGER PRIMARY KEY,
					vec BLOB
				)
			`)

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

				err := db.QueryRow("SELECT vector_quantize_bit(vector_f32(?))", vec).Scan(&bitVec)

				if err != nil {
					t.Fatalf("Failed to quantize vector %d: %v", i, err)
				}

				_, err = db.Exec("INSERT INTO bit_vectors (id, vec) VALUES (?, ?)", i+1, bitVec)

				if err != nil {
					t.Fatalf("Failed to insert vector %d: %v", i, err)
				}
			}

			// Query vector (same as first vector)
			var queryVec []byte

			err = db.QueryRow("SELECT vector_quantize_bit(vector_f32('[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]'))").Scan(&queryVec)

			if err != nil {
				t.Fatalf("Failed to create query vector: %v", err)
			}

			// Find nearest neighbors by Hamming distance
			rows, err := db.Query(`
				SELECT id, vector_hamming_distance(vec, ?) as distance
				FROM bit_vectors
				ORDER BY distance
				LIMIT 3
			`, queryVec)

			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}

			defer rows.Close()

			var results []struct {
				id       int
				distance int
			}

			for rows.Next() {
				var id, distance int

				err := rows.Scan(&id, &distance)

				if err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}

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
