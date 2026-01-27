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
	MetricL2     = "l2"
	MetricCosine = "cosine"
	MetricDot    = "dot"
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
