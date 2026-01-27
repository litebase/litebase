package vector_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestDistanceFunctions(t *testing.T) {
	// Initialize SIMD functions
	vector.InitDistanceFunctions()

	t.Run("L2Distance", func(t *testing.T) {
		a, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		b, _ := vector.EncodeFloat32([]float32{4.0, 5.0, 6.0})

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)

		dist, err := vector.DistanceL2(va, vb)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Expected: sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(27) ≈ 5.196
		expected := math.Sqrt(27.0)

		if math.Abs(dist-expected) > 1e-6 {
			t.Errorf("L2 distance mismatch: got %f, want %f", dist, expected)
		}

		// Compare with Go implementation
		distGo := vector.DistanceL2Go(va.GetFloat32Slice(), vb.GetFloat32Slice())

		if math.Abs(dist-distGo) > 1e-6 {
			t.Errorf("L2 distance mismatch with Go implementation: SIMD=%f, Go=%f", dist, distGo)
		}
	})

	t.Run("CosineDistance", func(t *testing.T) {
		a, _ := vector.EncodeFloat32([]float32{1.0, 0.0, 0.0})
		b, _ := vector.EncodeFloat32([]float32{0.0, 1.0, 0.0})

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)
		dist, err := vector.DistanceCosine(va, vb)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Orthogonal vectors should have distance = 1.0 (similarity = 0.0)
		if math.Abs(dist-1.0) > 1e-6 {
			t.Errorf("Cosine distance mismatch: got %f, want 1.0", dist)
		}

		// Compare with Go implementation
		distGo := vector.DistanceCosineGo(va.GetFloat32Slice(), vb.GetFloat32Slice())

		if math.Abs(dist-distGo) > 1e-6 {
			t.Errorf("Cosine distance mismatch with Go implementation: SIMD=%f, Go=%f", dist, distGo)
		}
	})

	t.Run("CosineDistanceParallel", func(t *testing.T) {
		a, _ := vector.EncodeFloat32([]float32{1.0, 1.0, 0.0})
		b, _ := vector.EncodeFloat32([]float32{1.0, 1.0, 0.0})

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)

		dist, err := vector.DistanceCosine(va, vb)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Parallel vectors should have distance = 0.0 (similarity = 1.0)
		if math.Abs(dist) > 1e-6 {
			t.Errorf("Cosine distance mismatch for parallel vectors: got %f, want 0.0", dist)
		}
	})

	t.Run("DotProduct", func(t *testing.T) {
		a, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		b, _ := vector.EncodeFloat32([]float32{4.0, 5.0, 6.0})

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)
		dist, err := vector.DistanceDot(va, vb)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Expected: -(1*4 + 2*5 + 3*6) = -(4 + 10 + 18) = -32
		expected := -32.0

		if math.Abs(dist-expected) > 1e-6 {
			t.Errorf("Dot product mismatch: got %f, want %f", dist, expected)
		}

		// Compare with Go implementation
		distGo := vector.DistanceDotGo(va.GetFloat32Slice(), vb.GetFloat32Slice())

		if math.Abs(dist-distGo) > 1e-6 {
			t.Errorf("Dot product mismatch with Go implementation: SIMD=%f, Go=%f", dist, distGo)
		}
	})

	t.Run("LargeVectors", func(t *testing.T) {
		// Test with larger vectors (768 dimensions like common embeddings)
		size := 768
		aValues := make([]float32, size)
		bValues := make([]float32, size)

		for i := 0; i < size; i++ {
			aValues[i] = float32(i) * 0.01
			bValues[i] = float32(i) * 0.02
		}

		a, _ := vector.EncodeFloat32(aValues)
		b, _ := vector.EncodeFloat32(bValues)

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)
		// Test L2
		distL2, _ := vector.DistanceL2(va, vb)
		distL2Go := vector.DistanceL2Go(aValues, bValues)

		if math.Abs(distL2-distL2Go) > 1e-4 {
			t.Errorf("L2 distance mismatch for large vectors: SIMD=%f, Go=%f", distL2, distL2Go)
		}

		// Test Cosine
		distCosine, _ := vector.DistanceCosine(va, vb)
		distCosineGo := vector.DistanceCosineGo(aValues, bValues)

		if math.Abs(distCosine-distCosineGo) > 1e-4 {
			t.Errorf("Cosine distance mismatch for large vectors: SIMD=%f, Go=%f", distCosine, distCosineGo)
		}

		// Test Dot
		distDot, _ := vector.DistanceDot(va, vb)
		distDotGo := vector.DistanceDotGo(aValues, bValues)

		if math.Abs(distDot-distDotGo) > 1e-2 {
			t.Errorf("Dot product mismatch for large vectors: SIMD=%f, Go=%f", distDot, distDotGo)
		}
	})

	t.Run("DimensionMismatch", func(t *testing.T) {
		a, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		b, _ := vector.EncodeFloat32([]float32{4.0, 5.0})

		va, _ := vector.ParseVectorBlob(a)
		vb, _ := vector.ParseVectorBlob(b)
		_, err := vector.DistanceL2(va, vb)

		if err == nil {
			t.Error("Expected error for dimension mismatch")
		}

		if err != vector.ErrDimensionMismatch {
			t.Errorf("Expected ErrDimensionMismatch, got %v", err)
		}
	})
}

func TestComputeDistance(t *testing.T) {
	vector.InitDistanceFunctions()

	a, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
	b, _ := vector.EncodeFloat32([]float32{4.0, 5.0, 6.0})

	va, _ := vector.ParseVectorBlob(a)
	vb, _ := vector.ParseVectorBlob(b)
	t.Run("L2Metric", func(t *testing.T) {
		dist, err := vector.ComputeDistance(va, vb, vector.MetricL2)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expected := math.Sqrt(27.0)

		if math.Abs(dist-expected) > 1e-6 {
			t.Errorf("Distance mismatch: got %f, want %f", dist, expected)
		}
	})

	t.Run("CosineMetric", func(t *testing.T) {
		dist, err := vector.ComputeDistance(va, vb, vector.MetricCosine)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should compute cosine distance
		if dist < 0 || dist > 2 {
			t.Errorf("Cosine distance out of range: %f", dist)
		}
	})

	t.Run("DotMetric", func(t *testing.T) {
		dist, err := vector.ComputeDistance(va, vb, vector.MetricDot)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expected := -32.0

		if math.Abs(dist-expected) > 1e-6 {
			t.Errorf("Distance mismatch: got %f, want %f", dist, expected)
		}
	})

	t.Run("UnsupportedMetric", func(t *testing.T) {
		_, err := vector.ComputeDistance(va, vb, "invalid")

		if err == nil {
			t.Error("Expected error for unsupported metric")
		}

		if err != vector.ErrUnsupportedMetric {
			t.Errorf("Expected ErrUnsupportedMetric, got %v", err)
		}
	})
}

func BenchmarkDistanceFunctions(b *testing.B) {
	vector.InitDistanceFunctions()

	sizes := []int{64, 128, 256, 512, 768, 1024}

	for _, size := range sizes {
		aValues := make([]float32, size)
		bValues := make([]float32, size)

		for i := 0; i < size; i++ {
			aValues[i] = float32(i) * 0.01
			bValues[i] = float32(i) * 0.02
		}

		aBlob, _ := vector.EncodeFloat32(aValues)
		bBlob, _ := vector.EncodeFloat32(bValues)

		va, _ := vector.ParseVectorBlob(aBlob)
		vb, _ := vector.ParseVectorBlob(bBlob)

		b.Run(fmt.Sprintf("L2_SIMD_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = vector.DistanceL2(va, vb)
			}
		})

		b.Run(fmt.Sprintf("L2_Go_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = vector.DistanceL2Go(aValues, bValues)
			}
		})

		b.Run(fmt.Sprintf("Cosine_SIMD_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = vector.DistanceCosine(va, vb)
			}
		})

		b.Run(fmt.Sprintf("Dot_SIMD_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = vector.DistanceDot(va, vb)
			}
		})
	}
}
