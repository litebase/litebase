package vector_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestDecodeFloat32Vector(t *testing.T) {
	t.Run("ValidVector", func(t *testing.T) {
		// Create a blob with 4 float32 values: [1.0, 2.5, -3.7, 0.0]
		blob := make([]byte, 16)
		binary.LittleEndian.PutUint32(blob[0:4], math.Float32bits(1.0))
		binary.LittleEndian.PutUint32(blob[4:8], math.Float32bits(2.5))
		binary.LittleEndian.PutUint32(blob[8:12], math.Float32bits(-3.7))
		binary.LittleEndian.PutUint32(blob[12:16], math.Float32bits(0.0))

		result := vector.DecodeFloat32Vector(blob)

		if len(result) != 4 {
			t.Errorf("Expected 4 dimensions, got %d", len(result))
		}

		expected := []float32{1.0, 2.5, -3.7, 0.0}

		for i, val := range expected {
			if result[i] != val {
				t.Errorf("At index %d: expected %f, got %f", i, val, result[i])
			}
		}
	})

	t.Run("EmptyBlob", func(t *testing.T) {
		result := vector.DecodeFloat32Vector([]byte{})

		if len(result) != 0 {
			t.Errorf("Expected empty vector, got length %d", len(result))
		}
	})

	t.Run("InvalidLength", func(t *testing.T) {
		// Blob length not divisible by 4
		blob := make([]byte, 7)
		result := vector.DecodeFloat32Vector(blob)

		if result != nil {
			t.Errorf("Expected nil for invalid blob length, got %v", result)
		}
	})

	t.Run("SingleValue", func(t *testing.T) {
		blob := make([]byte, 4)
		binary.LittleEndian.PutUint32(blob, math.Float32bits(42.0))

		result := vector.DecodeFloat32Vector(blob)

		if len(result) != 1 {
			t.Errorf("Expected 1 dimension, got %d", len(result))
		}

		if result[0] != 42.0 {
			t.Errorf("Expected 42.0, got %f", result[0])
		}
	})
}

func TestL2Distance(t *testing.T) {
	t.Run("IdenticalVectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}

		dist := vector.L2Distance(a, b)

		if dist != 0.0 {
			t.Errorf("Expected distance 0.0 for identical vectors, got %f", dist)
		}
	})

	t.Run("OrthogonalVectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}

		dist := vector.L2Distance(a, b)
		expected := float32(math.Sqrt(2.0))

		if math.Abs(float64(dist-expected)) > 0.0001 {
			t.Errorf("Expected distance %f, got %f", expected, dist)
		}
	})

	t.Run("KnownDistance", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		// Distance = sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(9+9+9) = sqrt(27) ≈ 5.196
		dist := vector.L2Distance(a, b)
		expected := float32(math.Sqrt(27.0))

		if math.Abs(float64(dist-expected)) > 0.0001 {
			t.Errorf("Expected distance %f, got %f", expected, dist)
		}
	})

	t.Run("NegativeValues", func(t *testing.T) {
		a := []float32{-1.0, -2.0, -3.0}
		b := []float32{1.0, 2.0, 3.0}

		// Distance = sqrt((1-(-1))^2 + (2-(-2))^2 + (3-(-3))^2) = sqrt(4+16+36) = sqrt(56)
		dist := vector.L2Distance(a, b)
		expected := float32(math.Sqrt(56.0))

		if math.Abs(float64(dist-expected)) > 0.0001 {
			t.Errorf("Expected distance %f, got %f", expected, dist)
		}
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0}

		dist := vector.L2Distance(a, b)

		if !math.IsInf(float64(dist), 1) {
			t.Errorf("Expected +Inf for mismatched lengths, got %f", dist)
		}
	})

	t.Run("ZeroVectors", func(t *testing.T) {
		a := []float32{0.0, 0.0, 0.0}
		b := []float32{0.0, 0.0, 0.0}

		dist := vector.L2Distance(a, b)

		if dist != 0.0 {
			t.Errorf("Expected distance 0.0 for zero vectors, got %f", dist)
		}
	})
}

func TestCosineDistance(t *testing.T) {
	t.Run("IdenticalVectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}

		dist := vector.CosineDistance(a, b)

		// Cosine similarity = 1, so distance = 1 - 1 = 0
		if math.Abs(float64(dist)) > 0.0001 {
			t.Errorf("Expected distance ~0.0 for identical vectors, got %f", dist)
		}
	})

	t.Run("OrthogonalVectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}

		dist := vector.CosineDistance(a, b)

		// Cosine similarity = 0, so distance = 1 - 0 = 1
		if math.Abs(float64(dist-1.0)) > 0.0001 {
			t.Errorf("Expected distance 1.0 for orthogonal vectors, got %f", dist)
		}
	})

	t.Run("OppositeVectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{-1.0, -2.0, -3.0}

		dist := vector.CosineDistance(a, b)

		// Cosine similarity = -1, so distance = 1 - (-1) = 2
		if math.Abs(float64(dist-2.0)) > 0.0001 {
			t.Errorf("Expected distance 2.0 for opposite vectors, got %f", dist)
		}
	})

	t.Run("ScaledVectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{2.0, 4.0, 6.0}

		dist := vector.CosineDistance(a, b)

		// Scaled versions have cosine similarity = 1, so distance = 0
		if math.Abs(float64(dist)) > 0.0001 {
			t.Errorf("Expected distance ~0.0 for scaled vectors, got %f", dist)
		}
	})

	t.Run("ZeroVectorA", func(t *testing.T) {
		a := []float32{0.0, 0.0, 0.0}
		b := []float32{1.0, 2.0, 3.0}

		dist := vector.CosineDistance(a, b)

		// Zero vector returns distance 1.0
		if dist != 1.0 {
			t.Errorf("Expected distance 1.0 for zero vector, got %f", dist)
		}
	})

	t.Run("ZeroVectorB", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{0.0, 0.0, 0.0}

		dist := vector.CosineDistance(a, b)

		// Zero vector returns distance 1.0
		if dist != 1.0 {
			t.Errorf("Expected distance 1.0 for zero vector, got %f", dist)
		}
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0}

		dist := vector.CosineDistance(a, b)

		if !math.IsInf(float64(dist), 1) {
			t.Errorf("Expected +Inf for mismatched lengths, got %f", dist)
		}
	})
}

func TestDotDistance(t *testing.T) {
	t.Run("PositiveDotProduct", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}

		// Dot product = 1*1 + 2*2 + 3*3 = 14
		// Distance = -14
		dist := vector.DotDistance(a, b)

		if dist != -14.0 {
			t.Errorf("Expected distance -14.0, got %f", dist)
		}
	})

	t.Run("NegativeDotProduct", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{-1.0, -2.0, -3.0}

		// Dot product = -1 - 4 - 9 = -14
		// Distance = 14
		dist := vector.DotDistance(a, b)

		if dist != 14.0 {
			t.Errorf("Expected distance 14.0, got %f", dist)
		}
	})

	t.Run("OrthogonalVectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}

		// Dot product = 0
		// Distance = 0
		dist := vector.DotDistance(a, b)

		if dist != 0.0 {
			t.Errorf("Expected distance 0.0, got %f", dist)
		}
	})

	t.Run("ZeroVectors", func(t *testing.T) {
		a := []float32{0.0, 0.0, 0.0}
		b := []float32{1.0, 2.0, 3.0}

		dist := vector.DotDistance(a, b)

		if dist != 0.0 {
			t.Errorf("Expected distance 0.0 for zero vector, got %f", dist)
		}
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0}

		dist := vector.DotDistance(a, b)

		if !math.IsInf(float64(dist), 1) {
			t.Errorf("Expected +Inf for mismatched lengths, got %f", dist)
		}
	})
}

func TestHammingDistance(t *testing.T) {
	t.Run("IdenticalVectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}

		dist := vector.HammingDistance(a, b)

		if dist != 0.0 {
			t.Errorf("Expected distance 0.0 for identical vectors, got %f", dist)
		}
	})

	t.Run("CompletelyDifferent", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		dist := vector.HammingDistance(a, b)

		if dist != 3.0 {
			t.Errorf("Expected distance 3.0, got %f", dist)
		}
	})

	t.Run("PartiallyDifferent", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0, 4.0}
		b := []float32{1.0, 0.0, 3.0, 0.0}

		// Positions 1 and 3 differ
		dist := vector.HammingDistance(a, b)

		if dist != 2.0 {
			t.Errorf("Expected distance 2.0, got %f", dist)
		}
	})

	t.Run("BinaryVectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 1.0, 0.0}
		b := []float32{1.0, 1.0, 0.0, 0.0}

		// Positions 1 and 2 differ
		dist := vector.HammingDistance(a, b)

		if dist != 2.0 {
			t.Errorf("Expected distance 2.0, got %f", dist)
		}
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0}

		dist := vector.HammingDistance(a, b)

		if !math.IsInf(float64(dist), 1) {
			t.Errorf("Expected +Inf for mismatched lengths, got %f", dist)
		}
	})
}

func TestGetDistanceFunc(t *testing.T) {
	t.Run("L2Metric", func(t *testing.T) {
		distFunc := vector.GetDistanceFunc(vector.DistanceMetricL2)

		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		dist := distFunc(a, b)
		expected := vector.L2Distance(a, b)

		if dist != expected {
			t.Errorf("L2 metric: expected %f, got %f", expected, dist)
		}
	})

	t.Run("CosineMetric", func(t *testing.T) {
		distFunc := vector.GetDistanceFunc(vector.DistanceMetricCosine)

		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		dist := distFunc(a, b)
		expected := vector.CosineDistance(a, b)

		if dist != expected {
			t.Errorf("Cosine metric: expected %f, got %f", expected, dist)
		}
	})

	t.Run("DotMetric", func(t *testing.T) {
		distFunc := vector.GetDistanceFunc(vector.DistanceMetricDot)

		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		dist := distFunc(a, b)
		expected := vector.DotDistance(a, b)

		if dist != expected {
			t.Errorf("Dot metric: expected %f, got %f", expected, dist)
		}
	})

	t.Run("HammingMetric", func(t *testing.T) {
		distFunc := vector.GetDistanceFunc(vector.DistanceMetricHamming)

		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 0.0, 3.0}

		dist := distFunc(a, b)
		expected := vector.HammingDistance(a, b)

		if dist != expected {
			t.Errorf("Hamming metric: expected %f, got %f", expected, dist)
		}
	})

	t.Run("InvalidMetricDefaultsToL2", func(t *testing.T) {
		distFunc := vector.GetDistanceFunc(999)

		a := []float32{1.0, 2.0, 3.0}
		b := []float32{4.0, 5.0, 6.0}

		dist := distFunc(a, b)
		expected := vector.L2Distance(a, b)

		if dist != expected {
			t.Errorf("Invalid metric should default to L2: expected %f, got %f", expected, dist)
		}
	})
}

func TestDistanceFunctionsEdgeCases(t *testing.T) {
	t.Run("EmptyVectors", func(t *testing.T) {
		a := []float32{}
		b := []float32{}

		// All distance functions should handle empty vectors
		l2 := vector.L2Distance(a, b)
		cosine := vector.CosineDistance(a, b)
		dot := vector.DotDistance(a, b)
		hamming := vector.HammingDistance(a, b)

		if l2 != 0.0 {
			t.Errorf("L2Distance on empty vectors: expected 0.0, got %f", l2)
		}

		if cosine != 1.0 {
			t.Errorf("CosineDistance on empty vectors: expected 1.0, got %f", cosine)
		}

		if dot != 0.0 {
			t.Errorf("DotDistance on empty vectors: expected 0.0, got %f", dot)
		}

		if hamming != 0.0 {
			t.Errorf("HammingDistance on empty vectors: expected 0.0, got %f", hamming)
		}
	})

	t.Run("VeryLargeValues", func(t *testing.T) {
		a := []float32{1e10, 2e10, 3e10}
		b := []float32{4e10, 5e10, 6e10}

		// Functions should handle large values without overflow
		l2 := vector.L2Distance(a, b)
		cosine := vector.CosineDistance(a, b)
		dot := vector.DotDistance(a, b)

		if math.IsNaN(float64(l2)) || math.IsInf(float64(l2), 0) {
			t.Errorf("L2Distance with large values returned invalid: %f", l2)
		}

		if math.IsNaN(float64(cosine)) {
			t.Errorf("CosineDistance with large values returned NaN: %f", cosine)
		}

		if math.IsNaN(float64(dot)) || math.IsInf(float64(dot), 0) {
			t.Errorf("DotDistance with large values returned invalid: %f", dot)
		}
	})

	t.Run("VerySmallValues", func(t *testing.T) {
		a := []float32{1e-10, 2e-10, 3e-10}
		b := []float32{4e-10, 5e-10, 6e-10}

		// Functions should handle small values
		l2 := vector.L2Distance(a, b)
		cosine := vector.CosineDistance(a, b)
		dot := vector.DotDistance(a, b)

		if math.IsNaN(float64(l2)) {
			t.Errorf("L2Distance with small values returned NaN: %f", l2)
		}

		if math.IsNaN(float64(cosine)) {
			t.Errorf("CosineDistance with small values returned NaN: %f", cosine)
		}

		if math.IsNaN(float64(dot)) {
			t.Errorf("DotDistance with small values returned NaN: %f", dot)
		}
	})
}
