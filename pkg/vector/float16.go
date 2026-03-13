package vector

import (
	"math"
)

// float32ToFloat16 converts a float32 to float16 (IEEE 754 binary16)
// Returns uint16 representation
func float32ToFloat16(f float32) uint16 {
	bits := math.Float32bits(f)

	sign := uint32(bits>>31) << 15
	exp := int32((bits>>23)&0xff) - 127 + 15
	mantissa := bits & 0x7fffff

	// Handle special cases
	if exp <= 0 {
		// Subnormal or zero
		if exp < -10 {
			return uint16(sign)
		}

		shift := uint32(1 - exp)
		mantissa = (mantissa | 0x800000) >> shift
		return uint16(sign | (mantissa >> 13))
	}

	if exp >= 0x1f {
		// Infinity or NaN
		if mantissa == 0 {
			return uint16(sign | 0x7c00)
		}

		return uint16(sign | 0x7c00 | (mantissa >> 13))
	}

	// Normalized number
	return uint16(sign | (uint32(exp) << 10) | (mantissa >> 13))
}

// Float16ToFloat32 converts a float16 (uint16) to float32.
func Float16ToFloat32(h uint16) float32 {
	return float16ToFloat32(h)
}

// float16ToFloat32 converts a float16 (uint16) to float32
func float16ToFloat32(h uint16) float32 {
	sign := uint32((h>>15)&1) << 31
	exp := int32((h >> 10) & 0x1f)
	mantissa := uint32(h & 0x03ff)

	switch exp {
	case 0:
		// Subnormal or zero
		if mantissa == 0 {
			return math.Float32frombits(sign)
		}

		// Normalize subnormal
		exp = 1
		for (mantissa & 0x400) == 0 {
			mantissa <<= 1
			exp--
		}

		mantissa &= 0x3ff
	case 0x1f:
		// Infinity or NaN
		return math.Float32frombits(sign | 0x7f800000 | (mantissa << 13))
	}

	// Normalized number
	exp = exp + 127 - 15
	mantissa = mantissa << 13

	return math.Float32frombits(sign | (uint32(exp) << 23) | mantissa)
}
