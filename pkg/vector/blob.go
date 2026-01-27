package vector

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"unsafe"
)

var (
	ErrInvalidBlobFormat  = errors.New("invalid vector blob format")
	ErrUnsupportedVersion = errors.New("unsupported vector version")
	ErrInvalidDimensions  = errors.New("invalid dimensions")
	ErrDimensionMismatch  = errors.New("dimension mismatch")
	ErrTooManyDimensions  = errors.New("too many dimensions")
)

// VectorBlob represents a parsed vector BLOB
type VectorBlob struct {
	Version    byte
	Type       byte
	Dimensions int
	Data       []byte
}

// ParseVectorBlob parses a vector BLOB and returns a VectorBlob struct
func ParseVectorBlob(blob []byte) (*VectorBlob, error) {
	if len(blob) < 6 {
		return nil, ErrInvalidBlobFormat
	}

	version := blob[0]

	if version != VectorVersion1 {
		return nil, ErrUnsupportedVersion
	}

	dims := int(binary.LittleEndian.Uint32(blob[1:5]))

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	expectedSize := 5 + (dims * 4)

	if len(blob) != expectedSize {
		return nil, ErrInvalidBlobFormat
	}

	return &VectorBlob{
		Version:    version,
		Type:       VectorTypeFloat32,
		Dimensions: dims,
		Data:       blob[5:],
	}, nil
}

// GetFloat32Slice returns the vector data as a float32 slice
func (vb *VectorBlob) GetFloat32Slice() []float32 {
	return unsafe.Slice((*float32)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// EncodeFloat32 encodes a float32 slice into a vector BLOB
func EncodeFloat32(vec []float32) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 5+(dims*4))
	blob[0] = VectorVersion1

	binary.LittleEndian.PutUint32(blob[1:5], uint32(dims))

	for i, v := range vec {
		binary.LittleEndian.PutUint32(blob[5+(i*4):], *(*uint32)(unsafe.Pointer(&v)))
	}

	return blob, nil
}

// ParseJSONArray parses a JSON array string into a float32 slice
func ParseJSONArray(jsonStr string) ([]float32, error) {
	var values []float32

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	return values, nil
}

// ValidateDimensions checks that two vectors have the same dimensions
func ValidateDimensions(a, b *VectorBlob) error {
	if a.Dimensions != b.Dimensions {
		return ErrDimensionMismatch
	}

	return nil
}
