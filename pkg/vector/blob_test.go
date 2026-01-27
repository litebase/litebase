package vector_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestBlobEncoding(t *testing.T) {
	t.Run("EncodeDecodeFloat32", func(t *testing.T) {
		original := []float32{0.1, 0.2, 0.3, 0.4, 0.5}

		blob, err := vector.EncodeFloat32(original)

		if err != nil {
			t.Fatalf("Failed to encode: %v", err)
		}

		parsed, err := vector.ParseVectorBlob(blob)

		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}

		if parsed.Version != vector.VectorVersion1 {
			t.Errorf("Version mismatch: got %d, want %d", parsed.Version, vector.VectorVersion1)
		}

		if parsed.Dimensions != len(original) {
			t.Errorf("Dimensions mismatch: got %d, want %d", parsed.Dimensions, len(original))
		}

		decoded := parsed.GetFloat32Slice()

		for i, v := range original {
			if decoded[i] != v {
				t.Errorf("Value mismatch at index %d: got %f, want %f", i, decoded[i], v)
			}
		}
	})

	t.Run("ParseJSONArray", func(t *testing.T) {
		result, err := vector.ParseJSONArray("[0.1, 0.2, 0.3]")

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expected := []float32{0.1, 0.2, 0.3}

		if len(result) != len(expected) {
			t.Errorf("Length mismatch: got %d, want %d", len(result), len(expected))
		}

		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("Value mismatch at index %d: got %f, want %f", i, result[i], expected[i])
			}
		}
	})
}

func TestDimensionValidation(t *testing.T) {
	blob1, _ := vector.EncodeFloat32([]float32{1, 2, 3})
	blob2, _ := vector.EncodeFloat32([]float32{4, 5, 6})
	blob3, _ := vector.EncodeFloat32([]float32{7, 8, 9, 10})

	v1, _ := vector.ParseVectorBlob(blob1)
	v2, _ := vector.ParseVectorBlob(blob2)
	v3, _ := vector.ParseVectorBlob(blob3)

	t.Run("MatchingDimensions", func(t *testing.T) {
		err := vector.ValidateDimensions(v1, v2)

		if err != nil {
			t.Errorf("Expected no error for matching dimensions, got: %v", err)
		}
	})

	t.Run("MismatchedDimensions", func(t *testing.T) {
		err := vector.ValidateDimensions(v1, v3)

		if err == nil {
			t.Error("Expected error for mismatched dimensions")
		}
	})
}
