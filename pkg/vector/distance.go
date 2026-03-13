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
	"sync"
	"unsafe"
)

// tempFloat32Pool holds pooled []float32 buffers used when decoding quantized
// (float16 or int8) blobs inside DistanceFromBlob to avoid per-call allocation.
var tempFloat32Pool = sync.Pool{
	New: func() any { b := make([]float32, 0, 512); return &b },
}

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

// DistanceFromBlob computes the distance between a parsed query vector and a
// raw vector blob without allocating a VectorBlob for the raw blob.  The blob
// must use VectorVersion1 format and match the query vector's dimensionality.
// Supported storage types: VectorTypeFloat32, VectorTypeFloat16, VectorTypeInt8.
// metric is a numeric constant (0=L2, 1=Cosine, 2=Dot).
// Returns (distance, ok); ok is false when the blob is malformed, the
// dimensions do not match, or the type is unsupported.
func DistanceFromBlob(query *VectorBlob, rawBlob []byte, metric int) (float64, bool) {
	if len(rawBlob) < 7 || query == nil || len(query.Data) == 0 {
		return 0, false
	}

	if rawBlob[0] != VectorVersion1 {
		return 0, false
	}

	dims := int(uint32(rawBlob[2]) | uint32(rawBlob[3])<<8 | uint32(rawBlob[4])<<16 | uint32(rawBlob[5])<<24)

	if dims != query.Dimensions {
		return 0, false
	}

	aPtr := (*C.float)(unsafe.Pointer(&query.Data[0]))
	cDims := C.int(dims)

	switch rawBlob[1] {
	case VectorTypeFloat32:
		if len(rawBlob) != 6+dims*4 {
			return 0, false
		}

		bPtr := (*C.float)(unsafe.Pointer(&rawBlob[6]))

		switch metric {
		case 0:
			return float64(C.compute_distance_l2(aPtr, bPtr, cDims)), true
		case 1:
			return float64(C.compute_distance_cosine(aPtr, bPtr, cDims)), true
		case 2:
			return float64(C.compute_distance_dot(aPtr, bPtr, cDims)), true
		default:
			return 0, false
		}

	case VectorTypeFloat16:
		if len(rawBlob) != 6+dims*2 {
			return 0, false
		}

		// Decode float16 → temp float32 buffer, then compute via SIMD.
		bp := tempFloat32Pool.Get().(*[]float32)
		buf := *bp

		if cap(buf) < dims {
			buf = make([]float32, dims)
		} else {
			buf = buf[:dims]
		}

		for i := 0; i < dims; i++ {
			h := uint16(rawBlob[6+i*2]) | uint16(rawBlob[7+i*2])<<8
			buf[i] = float16ToFloat32(h)
		}

		bPtr := (*C.float)(unsafe.Pointer(&buf[0]))
		var dist float64

		switch metric {
		case 0:
			dist = float64(C.compute_distance_l2(aPtr, bPtr, cDims))
		case 1:
			dist = float64(C.compute_distance_cosine(aPtr, bPtr, cDims))
		case 2:
			dist = float64(C.compute_distance_dot(aPtr, bPtr, cDims))
		default:
			*bp = buf[:0]
			tempFloat32Pool.Put(bp)

			return 0, false
		}

		*bp = buf[:0]
		tempFloat32Pool.Put(bp)

		return dist, true

	case VectorTypeInt8:
		if len(rawBlob) != 6+dims {
			return 0, false
		}

		// Decode int8 → float32 using global scale (val/127.0), then compute via SIMD.
		bp := tempFloat32Pool.Get().(*[]float32)
		buf := *bp

		if cap(buf) < dims {
			buf = make([]float32, dims)
		} else {
			buf = buf[:dims]
		}

		for i := 0; i < dims; i++ {
			buf[i] = float32(int8(rawBlob[6+i])) / 127.0
		}

		bPtr := (*C.float)(unsafe.Pointer(&buf[0]))
		var dist float64

		switch metric {
		case 0:
			dist = float64(C.compute_distance_l2(aPtr, bPtr, cDims))
		case 1:
			dist = float64(C.compute_distance_cosine(aPtr, bPtr, cDims))
		case 2:
			dist = float64(C.compute_distance_dot(aPtr, bPtr, cDims))
		default:
			*bp = buf[:0]
			tempFloat32Pool.Put(bp)

			return 0, false
		}

		*bp = buf[:0]
		tempFloat32Pool.Put(bp)

		return dist, true

	default:
		return 0, false
	}
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
