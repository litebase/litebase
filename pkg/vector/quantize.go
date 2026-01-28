package vector

import (
	"fmt"
	"math"
)

// QuantizeToInt8 quantizes a float32 vector to int8 using min-max scaling
// Returns the quantized vector and the scale/offset needed for dequantization
func QuantizeToInt8(vector []float32) ([]int8, float32, float32, error) {
	if len(vector) == 0 {
		return nil, 0, 0, fmt.Errorf("cannot quantize empty vector")
	}

	if len(vector) > MaxDimensions {
		return nil, 0, 0, fmt.Errorf("vector dimensions %d exceed maximum %d", len(vector), MaxDimensions)
	}

	// Find min and max values
	min := vector[0]
	max := vector[0]

	for _, v := range vector[1:] {
		if v < min {
			min = v
		}

		if v > max {
			max = v
		}
	}

	// Calculate scale and offset to map [min, max] to [-128, 127]
	scale := float32(255.0) / (max - min)
	offset := min

	// Quantize each value
	quantized := make([]int8, len(vector))

	for i, v := range vector {
		scaled := (v - offset) * scale
		// Map from [0, 255] to [-128, 127]
		quantized[i] = int8(scaled - 128)
	}

	return quantized, scale, offset, nil
}

// DequantizeFromInt8 converts int8 quantized vector back to float32
func DequantizeFromInt8(quantized []int8, scale float32, offset float32) []float32 {
	result := make([]float32, len(quantized))

	for i, v := range quantized {
		// Map from [-128, 127] back to [0, 255]
		scaled := float32(v) + 128
		result[i] = (scaled / scale) + offset
	}

	return result
}

// QuantizeToInt16 quantizes a float32 vector to int16 using min-max scaling
func QuantizeToInt16(vector []float32) ([]int16, float32, float32, error) {
	if len(vector) == 0 {
		return nil, 0, 0, fmt.Errorf("cannot quantize empty vector")
	}

	if len(vector) > MaxDimensions {
		return nil, 0, 0, fmt.Errorf("vector dimensions %d exceed maximum %d", len(vector), MaxDimensions)
	}

	// Find min and max values
	min := vector[0]
	max := vector[0]

	for _, v := range vector[1:] {
		if v < min {
			min = v
		}

		if v > max {
			max = v
		}
	}

	// Calculate scale and offset to map [min, max] to [-32768, 32767]
	scale := float32(65535.0) / (max - min)
	offset := min

	// Quantize each value
	quantized := make([]int16, len(vector))

	for i, v := range vector {
		scaled := (v - offset) * scale
		// Map from [0, 65535] to [-32768, 32767]
		quantized[i] = int16(scaled - 32768)
	}

	return quantized, scale, offset, nil
}

// DequantizeFromInt16 converts int16 quantized vector back to float32
func DequantizeFromInt16(quantized []int16, scale float32, offset float32) []float32 {
	result := make([]float32, len(quantized))

	for i, v := range quantized {
		// Map from [-32768, 32767] back to [0, 65535]
		scaled := float32(v) + 32768
		result[i] = (scaled / scale) + offset
	}

	return result
}

// QuantizeToFloat16 quantizes a float32 vector to float16
// No scale/offset needed as float16 maintains the range
func QuantizeToFloat16(vector []float32) ([]uint16, error) {
	if len(vector) == 0 {
		return nil, fmt.Errorf("cannot quantize empty vector")
	}

	if len(vector) > MaxDimensions {
		return nil, fmt.Errorf("vector dimensions %d exceed maximum %d", len(vector), MaxDimensions)
	}

	quantized := make([]uint16, len(vector))

	for i, v := range vector {
		quantized[i] = float32ToFloat16(v)
	}

	return quantized, nil
}

// DequantizeFromFloat16 converts float16 vector back to float32
func DequantizeFromFloat16(quantized []uint16) []float32 {
	result := make([]float32, len(quantized))

	for i, v := range quantized {
		result[i] = float16ToFloat32(v)
	}

	return result
}

// QuantizeToBit quantizes a float32 vector to binary using sign bit
// Each dimension becomes 1 (positive/zero) or 0 (negative)
func QuantizeToBit(vector []float32) ([]byte, error) {
	if len(vector) == 0 {
		return nil, fmt.Errorf("cannot quantize empty vector")
	}

	if len(vector) > MaxDimensions {
		return nil, fmt.Errorf("vector dimensions %d exceed maximum %d", len(vector), MaxDimensions)
	}

	// Calculate number of bytes needed (8 bits per byte)
	numBytes := (len(vector) + 7) / 8
	quantized := make([]byte, numBytes)

	for i, v := range vector {
		byteIdx := i / 8
		bitIdx := uint(i % 8)

		if v >= 0 {
			quantized[byteIdx] |= (1 << bitIdx)
		}
	}

	return quantized, nil
}

// DequantizeFromBit converts binary vector back to float32 (-1 or 1)
func DequantizeFromBit(quantized []byte, dimensions int) []float32 {
	result := make([]float32, dimensions)

	for i := 0; i < dimensions; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)

		if (quantized[byteIdx] & (1 << bitIdx)) != 0 {
			result[i] = 1.0
		} else {
			result[i] = -1.0
		}
	}

	return result
}

// QuantizeToBitCentered quantizes using mean-centered binary quantization
// More accurate than simple sign-based quantization
func QuantizeToBitCentered(vector []float32) ([]byte, float32, error) {
	if len(vector) == 0 {
		return nil, 0, fmt.Errorf("cannot quantize empty vector")
	}

	if len(vector) > MaxDimensions {
		return nil, 0, fmt.Errorf("vector dimensions %d exceed maximum %d", len(vector), MaxDimensions)
	}

	// Calculate mean
	var sum float32

	for _, v := range vector {
		sum += v
	}

	mean := sum / float32(len(vector))

	// Quantize based on comparison to mean
	numBytes := (len(vector) + 7) / 8
	quantized := make([]byte, numBytes)

	for i, v := range vector {
		byteIdx := i / 8
		bitIdx := uint(i % 8)

		if v >= mean {
			quantized[byteIdx] |= (1 << bitIdx)
		}
	}

	return quantized, mean, nil
}

// DequantizeFromBitCentered converts centered binary vector back to float32
func DequantizeFromBitCentered(quantized []byte, dimensions int, mean float32) []float32 {
	result := make([]float32, dimensions)

	for i := range dimensions {
		byteIdx := i / 8
		bitIdx := uint(i % 8)

		if (quantized[byteIdx] & (1 << bitIdx)) != 0 {
			result[i] = mean + 1.0
		} else {
			result[i] = mean - 1.0
		}
	}

	return result
}

// CalculateQuantizationError returns the mean absolute error between original and quantized vectors
func CalculateQuantizationError(original, dequantized []float32) float32 {
	if len(original) != len(dequantized) {
		return math.MaxFloat32
	}

	var totalError float32

	for i := range original {
		diff := original[i] - dequantized[i]

		if diff < 0 {
			diff = -diff
		}

		totalError += diff
	}

	return totalError / float32(len(original))
}
