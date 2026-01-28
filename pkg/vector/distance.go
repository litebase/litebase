package vector

/*
#cgo CFLAGS: -O3
#include <stdlib.h>

// Forward declarations from distance.c
void init_distance_functions();
float compute_distance_l2(const float* a, const float* b, int dims);
float compute_distance_cosine(const float* a, const float* b, int dims);
float compute_distance_dot(const float* a, const float* b, int dims);
*/
import "C"

import (
	"errors"
	"math"
	"unsafe"
)

// Distance metric types
const (
	MetricL2      = "l2"
	MetricCosine  = "cosine"
	MetricDot     = "dot"
	MetricHamming = "hamming"
)

var ErrUnsupportedMetric = errors.New("unsupported distance metric")

var distanceFunctionsInitialized = false

// InitDistanceFunctions initializes the SIMD distance functions
func InitDistanceFunctions() {
	if !distanceFunctionsInitialized {
		C.init_distance_functions()
		distanceFunctionsInitialized = true
	}
}

// ComputeDistance computes the distance between two vectors using the specified metric
func ComputeDistance(a, b *VectorBlob, metric string) (float64, error) {
	if err := ValidateDimensions(a, b); err != nil {
		return 0, err
	}

	switch metric {
	case MetricL2:
		return DistanceL2(a, b)
	case MetricCosine:
		return DistanceCosine(a, b)
	case MetricDot:
		return DistanceDot(a, b)
	case MetricHamming:
		distance, err := DistanceHamming(a, b)

		if err != nil {
			return 0, err
		}

		return float64(distance), nil
	default:
		return 0, ErrUnsupportedMetric
	}
}

// DistanceL2 computes the L2 (Euclidean) distance between two vectors
func DistanceL2(a, b *VectorBlob) (float64, error) {
	if err := ValidateDimensions(a, b); err != nil {
		return 0, err
	}

	aPtr := (*C.float)(unsafe.Pointer(&a.Data[0]))
	bPtr := (*C.float)(unsafe.Pointer(&b.Data[0]))
	dims := C.int(a.Dimensions)

	result := float64(C.compute_distance_l2(aPtr, bPtr, dims))

	return result, nil
}

// DistanceCosine computes the cosine distance between two vectors
func DistanceCosine(a, b *VectorBlob) (float64, error) {
	if err := ValidateDimensions(a, b); err != nil {
		return 0, err
	}

	aPtr := (*C.float)(unsafe.Pointer(&a.Data[0]))
	bPtr := (*C.float)(unsafe.Pointer(&b.Data[0]))
	dims := C.int(a.Dimensions)

	result := float64(C.compute_distance_cosine(aPtr, bPtr, dims))

	return result, nil
}

// DistanceDot computes the negative dot product distance between two vectors
func DistanceDot(a, b *VectorBlob) (float64, error) {
	if err := ValidateDimensions(a, b); err != nil {
		return 0, err
	}

	aPtr := (*C.float)(unsafe.Pointer(&a.Data[0]))
	bPtr := (*C.float)(unsafe.Pointer(&b.Data[0]))
	dims := C.int(a.Dimensions)

	result := float64(C.compute_distance_dot(aPtr, bPtr, dims))

	return result, nil
}

// DistanceHamming computes the Hamming distance between two bit vectors
// Returns the number of differing bits
func DistanceHamming(a, b *VectorBlob) (int, error) {
	if err := ValidateDimensions(a, b); err != nil {
		return 0, err
	}

	// Hamming distance only makes sense for bit vectors
	if a.Type != VectorTypeBit || b.Type != VectorTypeBit {
		return 0, errors.New("hamming distance only supported for bit vectors")
	}

	return hammingDistanceGo(a.Data, b.Data), nil
}

// hammingDistanceGo computes Hamming distance in pure Go using POPCNT
func hammingDistanceGo(a, b []byte) int {
	distance := 0

	for i := range a {
		// XOR gives 1 where bits differ
		xor := a[i] ^ b[i]

		// Count set bits (population count)
		distance += popcount(xor)
	}

	return distance
}

// popcount counts the number of set bits in a byte
func popcount(x byte) int {
	// Brian Kernighan's algorithm
	count := 0

	for x != 0 {
		x &= x - 1 // Clear the least significant set bit
		count++
	}

	return count
}

// Pure Go implementations for testing/comparison

// DistanceL2Go computes L2 distance in pure Go
func DistanceL2Go(a, b []float32) float64 {
	var sum float64

	for i := range a {
		diff := float64(a[i] - b[i])
		sum += diff * diff
	}

	return math.Sqrt(sum)
}

// DistanceCosineGo computes cosine distance in pure Go
func DistanceCosineGo(a, b []float32) float64 {
	var dotProduct, normA, normB float64

	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	similarity := dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))

	return 1.0 - similarity
}

// DistanceDotGo computes negative dot product in pure Go
func DistanceDotGo(a, b []float32) float64 {
	var dotProduct float64

	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
	}

	return -dotProduct
}
