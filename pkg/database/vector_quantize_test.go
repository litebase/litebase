package database_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestVectorQuantizeFunctions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		t.Run("QuantizeToInt8", func(t *testing.T) {
			// Create a float32 vector
			var originalBlob []byte

			err := db.QueryRow("SELECT vector_f32('[1.0, 2.5, -3.0, 0.5, 1.8]')").Scan(&originalBlob)

			if err != nil {
				t.Fatalf("Failed to create vector: %v", err)
			}

			// Quantize to int8
			var quantizedBlob []byte

			err = db.QueryRow("SELECT vector_quantize_int8(?)", originalBlob).Scan(&quantizedBlob)

			if err != nil {
				t.Fatalf("Failed to quantize vector: %v", err)
			}

			// Verify the result is a valid BLOB
			if len(quantizedBlob) == 0 {
				t.Fatal("Quantized BLOB is empty")
			}

			// Check that the quantized BLOB is smaller (int8 header + 5 bytes vs float32 header + 20 bytes)
			if len(quantizedBlob) >= len(originalBlob) {
				t.Errorf("Expected quantized BLOB to be smaller: got %d bytes, original %d bytes",
					len(quantizedBlob), len(originalBlob))
			}

			t.Logf("✓ Int8 quantization: %d bytes → %d bytes (%.1f%% of original)",
				len(originalBlob), len(quantizedBlob), float64(len(quantizedBlob))/float64(len(originalBlob))*100)
		})

		t.Run("QuantizeToInt16", func(t *testing.T) {
			// Create a float32 vector
			var originalBlob []byte

			err := db.QueryRow("SELECT vector_f32('[1.0, 2.5, -3.0, 0.5, 1.8]')").Scan(&originalBlob)

			if err != nil {
				t.Fatalf("Failed to create vector: %v", err)
			}

			// Quantize to int16
			var quantizedBlob []byte

			err = db.QueryRow("SELECT vector_quantize_int16(?)", originalBlob).Scan(&quantizedBlob)

			if err != nil {
				t.Fatalf("Failed to quantize vector: %v", err)
			}

			if len(quantizedBlob) == 0 {
				t.Fatal("Quantized BLOB is empty")
			}

			t.Logf("✓ Int16 quantization: %d bytes → %d bytes (%.1f%% of original)",
				len(originalBlob), len(quantizedBlob), float64(len(quantizedBlob))/float64(len(originalBlob))*100)
		})

		t.Run("QuantizeToFloat16", func(t *testing.T) {
			// Create a float32 vector
			var originalBlob []byte

			err := db.QueryRow("SELECT vector_f32('[1.0, 2.5, -3.0, 0.5, 1.8]')").Scan(&originalBlob)

			if err != nil {
				t.Fatalf("Failed to create vector: %v", err)
			}

			// Quantize to float16
			var quantizedBlob []byte

			err = db.QueryRow("SELECT vector_quantize_f16(?)", originalBlob).Scan(&quantizedBlob)

			if err != nil {
				t.Fatalf("Failed to quantize vector: %v", err)
			}

			if len(quantizedBlob) == 0 {
				t.Fatal("Quantized BLOB is empty")
			}

			// Float16 should be exactly half the size of float32
			expectedSize := 6 + (5 * 2) // header + 5 float16 values
			if len(quantizedBlob) != expectedSize {
				t.Errorf("Expected float16 BLOB to be %d bytes, got %d bytes",
					expectedSize, len(quantizedBlob))
			}

			t.Logf("✓ Float16 quantization: %d bytes → %d bytes (50%% of original)",
				len(originalBlob), len(quantizedBlob))
		})

		t.Run("QuantizeToBit", func(t *testing.T) {
			// Create a float32 vector
			var originalBlob []byte

			err := db.QueryRow("SELECT vector_f32('[1.0, -2.5, 3.0, -0.5, 1.8]')").Scan(&originalBlob)

			if err != nil {
				t.Fatalf("Failed to create vector: %v", err)
			}

			// Quantize to bit
			var quantizedBlob []byte

			err = db.QueryRow("SELECT vector_quantize_bit(?)", originalBlob).Scan(&quantizedBlob)

			if err != nil {
				t.Fatalf("Failed to quantize vector: %v", err)
			}

			if len(quantizedBlob) == 0 {
				t.Fatal("Quantized BLOB is empty")
			}

			// Bit quantization should be extremely small (1 byte for 5 dimensions)
			expectedSize := 6 + 1 // header + 1 byte (8 bits)
			if len(quantizedBlob) != expectedSize {
				t.Errorf("Expected bit BLOB to be %d bytes, got %d bytes",
					expectedSize, len(quantizedBlob))
			}

			t.Logf("✓ Bit quantization: %d bytes → %d bytes (%.1f%% of original)",
				len(originalBlob), len(quantizedBlob), float64(len(quantizedBlob))/float64(len(originalBlob))*100)
		})

		t.Run("QuantizeLargeVector", func(t *testing.T) {
			// Test with a large 768-dimensional vector (common embedding size)
			values := make([]float64, 768)

			for i := range values {
				values[i] = float64(i%10) / 10.0 // Values between 0.0 and 0.9
			}

			// Build JSON array string
			jsonArray := "["

			for i, v := range values {
				if i > 0 {
					jsonArray += ", "
				}

				jsonArray += fmt.Sprintf("%.1f", v)
			}

			jsonArray += "]"

			// Create float32 vector
			var originalBlob []byte

			err := db.QueryRow("SELECT vector_f32(?)", jsonArray).Scan(&originalBlob)

			if err != nil {
				t.Fatalf("Failed to create large vector: %v", err)
			}

			// Test all quantization methods
			tests := []struct {
				name     string
				query    string
				expected int
			}{
				{"int8", "SELECT vector_quantize_int8(?)", 6 + 768},
				{"int16", "SELECT vector_quantize_int16(?)", 6 + (768 * 2)},
				{"float16", "SELECT vector_quantize_f16(?)", 6 + (768 * 2)},
				{"bit", "SELECT vector_quantize_bit(?)", 6 + 96}, // 768/8 = 96 bytes
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					var quantizedBlob []byte

					err := db.QueryRow(tc.query, originalBlob).Scan(&quantizedBlob)

					if err != nil {
						t.Fatalf("Failed to quantize with %s: %v", tc.name, err)
					}

					if len(quantizedBlob) == 0 {
						t.Fatal("Quantized BLOB is empty")
					}

					if len(quantizedBlob) != tc.expected {
						t.Errorf("%s: expected %d bytes, got %d bytes",
							tc.name, tc.expected, len(quantizedBlob))
					}

					savings := float64(len(originalBlob)-len(quantizedBlob)) / float64(len(originalBlob)) * 100
					t.Logf("✓ %s quantization (768-D): %d → %d bytes (%.1f%% savings)",
						tc.name, len(originalBlob), len(quantizedBlob), savings)
				})
			}
		})
	})
}
