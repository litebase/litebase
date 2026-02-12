package vector

// Vector BLOB format version
const (
	VectorVersion1 byte = 0x01
)

// Vector type identifiers
const (
	VectorTypeFloat32 byte = 0x01
	VectorTypeFloat64 byte = 0x02
	VectorTypeInt8    byte = 0x03
	VectorTypeInt16   byte = 0x04
	VectorTypeFloat16 byte = 0x05 // Half-precision (2 bytes/dim)
	VectorTypeBit     byte = 0x06 // Binary quantization (1 bit/dim)
	VectorTypeSparse  byte = 0x07 // Sparse vectors (index-value pairs)
)

// Maximum dimensions supported
const MaxDimensions = 4096
