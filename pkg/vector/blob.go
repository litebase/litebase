package vector

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

	vectorType := blob[1]
	dims := int(binary.LittleEndian.Uint32(blob[2:6]))

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

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
			return nil, ErrInvalidBlobFormat
		}

		return &VectorBlob{
			Version:    version,
			Type:       vectorType,
			Dimensions: dims,
			Data:       blob[6:],
		}, nil
	case VectorTypeSparse:
		// Sparse vectors have variable size, validate minimum size
		if len(blob) < 6 {
			return nil, ErrInvalidBlobFormat
		}

		return &VectorBlob{
			Version:    version,
			Type:       vectorType,
			Dimensions: dims,
			Data:       blob[6:],
		}, nil
	default:
		return nil, fmt.Errorf("unsupported vector type: %d", vectorType)
	}

	expectedSize := 6 + (dims * bytesPerElement)

	if len(blob) != expectedSize {
		return nil, ErrInvalidBlobFormat
	}

	return &VectorBlob{
		Version:    version,
		Type:       vectorType,
		Dimensions: dims,
		Data:       blob[6:],
	}, nil
}

// GetFloat32Slice returns the vector data as a float32 slice
func (vb *VectorBlob) GetFloat32Slice() []float32 {
	if vb.Type != VectorTypeFloat32 {
		return nil
	}

	return unsafe.Slice((*float32)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// GetFloat64Slice returns the vector data as a float64 slice
func (vb *VectorBlob) GetFloat64Slice() []float64 {
	if vb.Type != VectorTypeFloat64 {
		return nil
	}

	return unsafe.Slice((*float64)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// GetInt8Slice returns the vector data as an int8 slice
func (vb *VectorBlob) GetInt8Slice() []int8 {
	if vb.Type != VectorTypeInt8 {
		return nil
	}

	return unsafe.Slice((*int8)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// GetInt16Slice returns the vector data as an int16 slice
func (vb *VectorBlob) GetInt16Slice() []int16 {
	if vb.Type != VectorTypeInt16 {
		return nil
	}

	return unsafe.Slice((*int16)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// GetFloat16Slice returns the vector data as a []uint16 (packed float16 values)
// Use float16ToFloat32() to convert individual values to float32
func (vb *VectorBlob) GetFloat16Slice() []uint16 {
	if vb.Type != VectorTypeFloat16 {
		return nil
	}

	return unsafe.Slice((*uint16)(unsafe.Pointer(&vb.Data[0])), vb.Dimensions)
}

// GetFloat16AsFloat32 returns the float16 vector converted to float32 slice
func (vb *VectorBlob) GetFloat16AsFloat32() []float32 {
	if vb.Type != VectorTypeFloat16 {
		return nil
	}

	raw := vb.GetFloat16Slice()
	result := make([]float32, len(raw))

	for i, h := range raw {
		result[i] = float16ToFloat32(h)
	}

	return result
}

// GetBitSlice returns the bit vector as a byte slice
// Each byte contains 8 packed bits
func (vb *VectorBlob) GetBitSlice() []byte {
	if vb.Type != VectorTypeBit {
		return nil
	}

	return vb.Data
}

// GetBitVector returns the bit vector as a []bool slice
func (vb *VectorBlob) GetBitVector() []bool {
	if vb.Type != VectorTypeBit {
		return nil
	}

	result := make([]bool, vb.Dimensions)

	for i := 0; i < vb.Dimensions; i++ {
		byteIndex := i / 8
		bitIndex := uint(i % 8)
		result[i] = (vb.Data[byteIndex] & (1 << bitIndex)) != 0
	}

	return result
}

// SparseVector represents a sparse vector with index-value pairs
type SparseVector struct {
	Indices []uint32
	Values  []float32
}

// GetSparseVector returns the sparse vector as index-value pairs
func (vb *VectorBlob) GetSparseVector() *SparseVector {
	if vb.Type != VectorTypeSparse {
		return nil
	}

	// Sparse format: [num_elements (4 bytes)] [index1 (4)] [value1 (4)] [index2 (4)] [value2 (4)] ...
	if len(vb.Data) < 4 {
		return nil
	}

	numElements := int(binary.LittleEndian.Uint32(vb.Data[0:4]))

	if len(vb.Data) < 4+(numElements*8) {
		return nil
	}

	result := &SparseVector{
		Indices: make([]uint32, numElements),
		Values:  make([]float32, numElements),
	}

	offset := 4

	for i := range numElements {
		result.Indices[i] = binary.LittleEndian.Uint32(vb.Data[offset : offset+4])
		result.Values[i] = math.Float32frombits(binary.LittleEndian.Uint32(vb.Data[offset+4 : offset+8]))
		offset += 8
	}

	return result
}

// EncodeFloat32 encodes a float32 slice into a vector BLOB
func EncodeFloat32(vec []float32) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+(dims*4))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeFloat32

	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, v := range vec {
		binary.LittleEndian.PutUint32(blob[6+(i*4):], *(*uint32)(unsafe.Pointer(&v)))
	}

	return blob, nil
}

// EncodeFloat64 encodes a float64 slice into a vector BLOB
func EncodeFloat64(vec []float64) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+(dims*8))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeFloat64

	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, v := range vec {
		binary.LittleEndian.PutUint64(blob[6+(i*8):], *(*uint64)(unsafe.Pointer(&v)))
	}

	return blob, nil
}

// EncodeInt8 encodes an int8 slice into a vector BLOB
func EncodeInt8(vec []int8) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+dims)
	blob[0] = VectorVersion1
	blob[1] = VectorTypeInt8

	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, v := range vec {
		blob[6+i] = byte(v)
	}

	return blob, nil
}

// EncodeInt16 encodes an int16 slice into a vector BLOB
func EncodeInt16(vec []int16) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+(dims*2))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeInt16

	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, v := range vec {
		binary.LittleEndian.PutUint16(blob[6+(i*2):], uint16(v))
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

// ParseJSONArrayFloat64 parses a JSON array string into a float64 slice
func ParseJSONArrayFloat64(jsonStr string) ([]float64, error) {
	var values []float64

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	return values, nil
}

// ParseJSONArrayInt8 parses a JSON array string into an int8 slice
func ParseJSONArrayInt8(jsonStr string) ([]int8, error) {
	var values []int8

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	return values, nil
}

// ParseJSONArrayInt16 parses a JSON array string into an int16 slice
func ParseJSONArrayInt16(jsonStr string) ([]int16, error) {
	var values []int16

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	return values, nil
}

// EncodeFloat16 encodes a float32 slice into a float16 vector BLOB
func EncodeFloat16(vec []float32) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+(dims*2))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeFloat16
	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, val := range vec {
		f16 := float32ToFloat16(val)
		binary.LittleEndian.PutUint16(blob[6+(i*2):], f16)
	}

	return blob, nil
}

// EncodeBit encodes a boolean slice into a bit vector BLOB
func EncodeBit(vec []bool) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	numBytes := (dims + 7) / 8
	blob := make([]byte, 6+numBytes)
	blob[0] = VectorVersion1
	blob[1] = VectorTypeBit
	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, bit := range vec {
		if bit {
			byteIndex := i / 8
			bitIndex := uint(i % 8)
			blob[6+byteIndex] |= (1 << bitIndex)
		}
	}

	return blob, nil
}

// EncodeBitFromFloat32 encodes a float32 slice as binary quantized bit vector
// Values >= threshold are set to 1, otherwise 0
func EncodeBitFromFloat32(vec []float32, threshold float32) ([]byte, error) {
	bits := make([]bool, len(vec))

	for i, v := range vec {
		bits[i] = v >= threshold
	}

	return EncodeBit(bits)
}

// EncodeSparse encodes a sparse vector from indices and values
func EncodeSparse(dimensions int, indices []uint32, values []float32) ([]byte, error) {
	if dimensions <= 0 || dimensions > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	if len(indices) != len(values) {
		return nil, errors.New("indices and values must have same length")
	}

	numElements := len(indices)
	blob := make([]byte, 6+4+(numElements*8))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeSparse
	binary.LittleEndian.PutUint32(blob[2:6], uint32(dimensions))
	binary.LittleEndian.PutUint32(blob[6:10], uint32(numElements))

	offset := 10

	for i := range numElements {
		binary.LittleEndian.PutUint32(blob[offset:offset+4], indices[i])
		binary.LittleEndian.PutUint32(blob[offset+4:offset+8], math.Float32bits(values[i]))
		offset += 8
	}

	return blob, nil
}

// ParseJSONArrayFloat16 parses a JSON array string into a float16 BLOB
func ParseJSONArrayFloat16(jsonStr string) ([]byte, error) {
	var values []float32

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	return EncodeFloat16(values)
}

// ParseJSONArrayBit parses a JSON array of 0/1 into a bit vector
func ParseJSONArrayBit(jsonStr string) ([]byte, error) {
	var values []int

	err := json.Unmarshal([]byte(jsonStr), &values)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	if len(values) > MaxDimensions {
		return nil, ErrTooManyDimensions
	}

	bits := make([]bool, len(values))

	for i, v := range values {
		bits[i] = v != 0
	}

	return EncodeBit(bits)
}

// ParseJSONSparse parses a JSON sparse vector format
// Expected format: {"dim": 1024, "indices": [0, 5, 100], "values": [0.5, 0.3, 0.8]}
func ParseJSONSparse(jsonStr string) ([]byte, error) {
	var data struct {
		Dimensions int       `json:"dim"`
		Indices    []uint32  `json:"indices"`
		Values     []float32 `json:"values"`
	}

	err := json.Unmarshal([]byte(jsonStr), &data)

	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON sparse vector: %w", err)
	}

	if len(data.Indices) != len(data.Values) {
		return nil, errors.New("indices and values must have same length")
	}

	return EncodeSparse(data.Dimensions, data.Indices, data.Values)
}

// EncodeFloat16FromUint16 encodes a uint16 slice (already converted to float16) into a BLOB
func EncodeFloat16FromUint16(vec []uint16) ([]byte, error) {
	dims := len(vec)

	if dims <= 0 || dims > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	blob := make([]byte, 6+(dims*2))
	blob[0] = VectorVersion1
	blob[1] = VectorTypeFloat16
	binary.LittleEndian.PutUint32(blob[2:6], uint32(dims))

	for i, f16 := range vec {
		binary.LittleEndian.PutUint16(blob[6+(i*2):], f16)
	}

	return blob, nil
}

// EncodeBitFromBytes encodes a byte slice (already packed bits) into a BLOB
func EncodeBitFromBytes(packedBits []byte, dimensions int) ([]byte, error) {
	if dimensions <= 0 || dimensions > MaxDimensions {
		return nil, ErrInvalidDimensions
	}

	expectedBytes := (dimensions + 7) / 8

	if len(packedBits) != expectedBytes {
		return nil, fmt.Errorf("packed bits length mismatch: got %d, expected %d", len(packedBits), expectedBytes)
	}

	blob := make([]byte, 6+expectedBytes)
	blob[0] = VectorVersion1
	blob[1] = VectorTypeBit
	binary.LittleEndian.PutUint32(blob[2:6], uint32(dimensions))
	copy(blob[6:], packedBits)

	return blob, nil
}

// ValidateDimensions checks that two vectors have the same dimensions
func ValidateDimensions(a, b *VectorBlob) error {
	if a.Dimensions != b.Dimensions {
		return ErrDimensionMismatch
	}

	return nil
}
