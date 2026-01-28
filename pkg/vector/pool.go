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
