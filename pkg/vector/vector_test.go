package vector_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestConstants(t *testing.T) {
	t.Run("VectorVersion", func(t *testing.T) {
		if vector.VectorVersion1 != 0x01 {
			t.Errorf("Expected VectorVersion1 to be 0x01, got 0x%02x", vector.VectorVersion1)
		}
	})

	t.Run("VectorType", func(t *testing.T) {
		if vector.VectorTypeFloat32 != 0x01 {
			t.Errorf("Expected VectorTypeFloat32 to be 0x01, got 0x%02x", vector.VectorTypeFloat32)
		}
	})

	t.Run("MaxDimensions", func(t *testing.T) {
		if vector.MaxDimensions != 4096 {
			t.Errorf("Expected MaxDimensions to be 4096, got %d", vector.MaxDimensions)
		}

		// Test that we can create a vector with max dimensions
		values := make([]float32, vector.MaxDimensions)

		for i := range values {
			values[i] = float32(i) * 0.001
		}

		blob, err := vector.EncodeFloat32(values)

		if err != nil {
			t.Errorf("Failed to encode max dimensions vector: %v", err)
		}

		parsed, err := vector.ParseVectorBlob(blob)

		if err != nil {
			t.Errorf("Failed to parse max dimensions vector: %v", err)
		}

		if parsed.Dimensions != vector.MaxDimensions {
			t.Errorf("Expected %d dimensions, got %d", vector.MaxDimensions, parsed.Dimensions)
		}
	})

	t.Run("ExceedMaxDimensions", func(t *testing.T) {
		values := make([]float32, vector.MaxDimensions+1)

		_, err := vector.EncodeFloat32(values)

		if err == nil {
			t.Error("Expected error when exceeding max dimensions")
		}

		if err != vector.ErrInvalidDimensions {
			t.Errorf("Expected ErrInvalidDimensions, got %v", err)
		}
	})
}

func TestDistanceMetricConstants(t *testing.T) {
	t.Run("MetricValues", func(t *testing.T) {
		if vector.MetricL2 != "l2" {
			t.Errorf("Expected MetricL2 to be 'l2', got '%s'", vector.MetricL2)
		}

		if vector.MetricCosine != "cosine" {
			t.Errorf("Expected MetricCosine to be 'cosine', got '%s'", vector.MetricCosine)
		}

		if vector.MetricDot != "dot" {
			t.Errorf("Expected MetricDot to be 'dot', got '%s'", vector.MetricDot)
		}
	})
}
