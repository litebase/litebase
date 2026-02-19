package vector

import (
	"encoding/binary"
	"sync"
)

// VectorBlobPool is a pool for reusing VectorBlob objects
var vectorBlobPool = sync.Pool{
	New: func() interface{} {
		return &VectorBlob{}
	},
}

// inlineClusterNodePool pools inlineClusterNode objects to reduce per-batch
// heap allocations when loading the cluster tree.
var inlineClusterNodePool = sync.Pool{
	New: func() interface{} {
		return &inlineClusterNode{
			children: make([]int64, 0, 8),
		}
	},
}

// getInlineClusterNode acquires a node from the pool and resets it.
func getInlineClusterNode() *inlineClusterNode {
	n := inlineClusterNodePool.Get().(*inlineClusterNode)
	n.clusterID = 0
	n.parentID = nil
	n.centroid = nil
	n.isLeaf = false
	n.children = n.children[:0]

	return n
}

// putInlineClusterNode returns a node to the pool.
func putInlineClusterNode(n *inlineClusterNode) {
	if n == nil {
		return
	}

	inlineClusterNodePool.Put(n)
}

// float64SlicePool pools []float64 slices used for centroid sum accumulation in
// goUpdateClusterStats. Slices are keyed by capacity bucket (dims).
var float64SlicePool sync.Pool

// getFloat64Slice returns a zeroed []float64 of length dims from the pool.
func getFloat64Slice(dims int) []float64 {
	if v := float64SlicePool.Get(); v != nil {
		s := v.([]float64)

		if cap(s) >= dims {
			s = s[:dims]

			for i := range s {
				s[i] = 0
			}

			return s
		}
	}

	return make([]float64, dims)
}

// putFloat64Slice returns a []float64 slice to the pool.
func putFloat64Slice(s []float64) {
	if s == nil {
		return
	}

	float64SlicePool.Put(s)
}

// encodeBlobPool pools byte slices for centroid blob encoding in
// goUpdateClusterStats to avoid per-cluster heap allocations.
// Each blob is 6 + dims*4 bytes for float32 vectors.
var encodeBlobPool sync.Pool

// getEncodeBlob returns a []byte of length n from the pool.
func getEncodeBlob(n int) []byte {
	if v := encodeBlobPool.Get(); v != nil {
		b := v.([]byte)

		if cap(b) >= n {
			return b[:n]
		}
	}

	return make([]byte, n)
}

// putEncodeBlob returns a blob slice to the pool.
func putEncodeBlob(b []byte) {
	if b == nil {
		return
	}

	encodeBlobPool.Put(b)
}

// GetVectorBlob acquires a VectorBlob from the pool
func GetVectorBlob() *VectorBlob {
	return vectorBlobPool.Get().(*VectorBlob)
}

// PutVectorBlob returns a VectorBlob to the pool
func PutVectorBlob(vb *VectorBlob) {
	if vb == nil {
		return
	}

	// Reset fields before returning to pool
	vb.Version = 0
	vb.Type = 0
	vb.Dimensions = 0
	vb.Data = nil
	vectorBlobPool.Put(vb)
}

// ParseVectorBlobPooled parses a vector BLOB using a pooled VectorBlob object
// Caller must call PutVectorBlob when done
func ParseVectorBlobPooled(blob []byte) (*VectorBlob, error) {
	if len(blob) < 6 {
		return nil, ErrInvalidBlobFormat
	}

	version := blob[0]

	if version != VectorVersion1 {
		return nil, ErrUnsupportedVersion
	}

	vectorType := blob[1]
	dims := int(binary.LittleEndian.Uint32(blob[2:6]))

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	vb := GetVectorBlob()
	vb.Version = version
	vb.Type = vectorType
	vb.Dimensions = dims

	// Calculate expected size based on type
	var bytesPerElement int

	switch vectorType {
	case VectorTypeFloat32:
		bytesPerElement = 4
	case VectorTypeFloat64:
		bytesPerElement = 8
	case VectorTypeInt8:
		bytesPerElement = 1
	case VectorTypeInt16:
		bytesPerElement = 2
	case VectorTypeFloat16:
		bytesPerElement = 2
	case VectorTypeBit:
		// For bit vectors, we pack 8 dimensions per byte
		expectedSize := 6 + ((dims + 7) / 8)

		if len(blob) != expectedSize {
			PutVectorBlob(vb)
			return nil, ErrInvalidBlobFormat
		}

		vb.Data = blob[6:]
		return vb, nil
	case VectorTypeSparse:
		// Sparse vectors have variable size, validate minimum size
		if len(blob) < 6 {
			PutVectorBlob(vb)
			return nil, ErrInvalidBlobFormat
		}

		vb.Data = blob[6:]
		return vb, nil
	default:
		PutVectorBlob(vb)
		return nil, ErrInvalidDimensions
	}

	expectedSize := 6 + (dims * bytesPerElement)

	if len(blob) != expectedSize {
		PutVectorBlob(vb)
		return nil, ErrInvalidBlobFormat
	}

	vb.Data = blob[6:]

	return vb, nil
}
