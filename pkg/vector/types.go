package vector

import (
	"encoding/binary"
	"math"
)

// Distance metric constants
const (
	DistanceMetricL2      = 0
	DistanceMetricCosine  = 1
	DistanceMetricDot     = 2
	DistanceMetricHamming = 3
)

// DistanceFunc computes distance between two float32 vectors
type DistanceFunc func(a, b []float32) float32

// DecodeFloat32Vector decodes a blob of bytes into a float32 vector
func DecodeFloat32Vector(blob []byte) []float32 {
	if len(blob)%4 != 0 {
		return nil
	}

	dims := len(blob) / 4
	vector := make([]float32, dims)

	for i := 0; i < dims; i++ {
		bits := binary.LittleEndian.Uint32(blob[i*4 : (i+1)*4])
		vector[i] = math.Float32frombits(bits)
	}

	return vector
}

// L2Distance computes the Euclidean (L2) distance between two vectors
func L2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var sum float32

	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}

	return float32(math.Sqrt(float64(sum)))
}

// CosineDistance computes the cosine distance (1 - cosine similarity) between two vectors
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var dotProduct, normA, normB float32

	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	similarity := dotProduct / float32(math.Sqrt(float64(normA*normB)))

	return 1.0 - similarity
}

// DotDistance computes the negative dot product (for use as a distance metric)
func DotDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var dotProduct float32

	for i := range a {
		dotProduct += a[i] * b[i]
	}

	return -dotProduct
}

// HammingDistance computes the Hamming distance between two vectors (treating as binary)
func HammingDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}

	var distance float32

	for i := range a {
		if a[i] != b[i] {
			distance++
		}
	}

	return distance
}

// GetDistanceFunc returns the distance function for a given metric
func GetDistanceFunc(metric int) DistanceFunc {
	switch metric {
	case DistanceMetricL2:
		return L2Distance
	case DistanceMetricCosine:
		return CosineDistance
	case DistanceMetricDot:
		return DotDistance
	case DistanceMetricHamming:
		return HammingDistance
	default:
		return L2Distance
	}
}
