package vector_test

import (
	"math"
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestQuantizeToInt8(t *testing.T) {
	original := []float32{-1.0, 0.0, 1.0, 2.0, 3.0}

	quantized, scale, offset, err := vector.QuantizeToInt8(original)

	if err != nil {
		t.Fatalf("QuantizeToInt8 failed: %v", err)
	}

	if len(quantized) != len(original) {
		t.Errorf("Expected %d quantized values, got %d", len(original), len(quantized))
	}

	// Dequantize and check error
	dequantized := vector.DequantizeFromInt8(quantized, scale, offset)
	error := vector.CalculateQuantizationError(original, dequantized)

	if error > 0.1 {
		t.Errorf("Quantization error too high: %f", error)
	}

	t.Logf("✓ Int8 quantization: error = %.6f, scale = %.4f, offset = %.4f", error, scale, offset)
}

func TestQuantizeToInt16(t *testing.T) {
	original := []float32{-10.5, -5.0, 0.0, 5.0, 10.5}

	quantized, scale, offset, err := vector.QuantizeToInt16(original)

	if err != nil {
		t.Fatalf("QuantizeToInt16 failed: %v", err)
	}

	if len(quantized) != len(original) {
		t.Errorf("Expected %d quantized values, got %d", len(original), len(quantized))
	}

	dequantized := vector.DequantizeFromInt16(quantized, scale, offset)
	error := vector.CalculateQuantizationError(original, dequantized)

	if error > 0.01 {
		t.Errorf("Quantization error too high: %f", error)
	}

	t.Logf("✓ Int16 quantization: error = %.6f, scale = %.4f, offset = %.4f", error, scale, offset)
}

func TestQuantizeToBit(t *testing.T) {
	original := []float32{-1.0, 1.0, -0.5, 0.5, 0.0}

	quantized, err := vector.QuantizeToBit(original)

	if err != nil {
		t.Fatalf("QuantizeToBit failed: %v", err)
	}

	expectedBytes := (len(original) + 7) / 8

	if len(quantized) != expectedBytes {
		t.Errorf("Expected %d bytes, got %d", expectedBytes, len(quantized))
	}

	// Verify bit pattern
	if (quantized[0] & 0x01) != 0 { // -1.0 should be 0
		t.Error("Negative value should map to 0 bit")
	}

	if (quantized[0] & 0x02) == 0 { // 1.0 should be 1
		t.Error("Positive value should map to 1 bit")
	}

	if (quantized[0] & 0x10) == 0 { // 0.0 should be 1
		t.Error("Zero should map to 1 bit")
	}

	t.Logf("✓ Bit quantization: %d dimensions → %d bytes", len(original), len(quantized))
}

func TestQuantizeToBitCentered(t *testing.T) {
	original := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}

	quantized, mean, err := vector.QuantizeToBitCentered(original)

	if err != nil {
		t.Fatalf("QuantizeToBitCentered failed: %v", err)
	}

	expectedMean := float32(0.45) // (0.1+0.2+...+0.8)/8
	diff := math.Abs(float64(mean - expectedMean))

	if diff > 0.01 {
		t.Errorf("Mean calculation incorrect: expected %.2f, got %.2f", expectedMean, mean)
	}

	// Values below mean should be 0, above should be 1
	if (quantized[0] & 0x01) != 0 { // 0.1 < 0.45
		t.Error("Value below mean should be 0")
	}

	if (quantized[0] & 0x80) == 0 { // 0.8 > 0.45
		t.Error("Value above mean should be 1")
	}

	t.Logf("✓ Centered bit quantization: mean = %.4f", mean)
}

func TestQuantizeToFloat16(t *testing.T) {
	original := []float32{1.0, 2.0, 3.0, -1.0, -2.0}

	quantized, err := vector.QuantizeToFloat16(original)

	if err != nil {
		t.Fatalf("QuantizeToFloat16 failed: %v", err)
	}

	dequantized := vector.DequantizeFromFloat16(quantized)

	for i := range original {
		diff := math.Abs(float64(original[i] - dequantized[i]))

		if diff > 0.001 {
			t.Errorf("Float16 conversion error at index %d: %f vs %f (diff: %f)",
				i, original[i], dequantized[i], diff)
		}
	}

	error := vector.CalculateQuantizationError(original, dequantized)
	t.Logf("✓ Float16 quantization: error = %.6f", error)
}

func TestQuantizationStorageSavings(t *testing.T) {
	dims := 128
	original := make([]float32, dims)

	for i := range original {
		original[i] = float32(i) / float32(dims)
	}

	// Float32: 4 bytes per dimension
	float32Size := dims * 4

	// Int8: 1 byte per dimension + 8 bytes metadata
	quantizedInt8, _, _, _ := vector.QuantizeToInt8(original)
	int8Size := len(quantizedInt8) + 8

	// Int16: 2 bytes per dimension + 8 bytes metadata
	quantizedInt16, _, _, _ := vector.QuantizeToInt16(original)
	int16Size := len(quantizedInt16)*2 + 8

	// Float16: 2 bytes per dimension
	quantizedF16, _ := vector.QuantizeToFloat16(original)
	f16Size := len(quantizedF16) * 2

	// Bit: 1 bit per dimension
	quantizedBit, _ := vector.QuantizeToBit(original)
	bitSize := len(quantizedBit)

	t.Logf("✓ Storage comparison for %d dimensions:", dims)
	t.Logf("  Float32: %d bytes (baseline)", float32Size)
	t.Logf("  Int8:    %d bytes (%.1f%% of float32)", int8Size, float64(int8Size)/float64(float32Size)*100)
	t.Logf("  Int16:   %d bytes (%.1f%% of float32)", int16Size, float64(int16Size)/float64(float32Size)*100)
	t.Logf("  Float16: %d bytes (%.1f%% of float32)", f16Size, float64(f16Size)/float64(float32Size)*100)
	t.Logf("  Bit:     %d bytes (%.1f%% of float32)", bitSize, float64(bitSize)/float64(float32Size)*100)
}

func TestQuantizeEmptyVector(t *testing.T) {
	empty := []float32{}

	_, _, _, err := vector.QuantizeToInt8(empty)

	if err == nil {
		t.Error("Expected error for empty vector")
	}

	_, err = vector.QuantizeToBit(empty)

	if err == nil {
		t.Error("Expected error for empty vector")
	}
}

func TestQuantizeLargeVector(t *testing.T) {
	dims := 768 // Common embedding size

	original := make([]float32, dims)

	for i := range original {
		original[i] = float32(i%100) / 100.0
	}

	// Test each quantization method
	quantizedInt8, scale8, offset8, err := vector.QuantizeToInt8(original)

	if err != nil {
		t.Fatalf("Failed to quantize to int8: %v", err)
	}

	dequantizedInt8 := vector.DequantizeFromInt8(quantizedInt8, scale8, offset8)
	error8 := vector.CalculateQuantizationError(original, dequantizedInt8)

	quantizedF16, err := vector.QuantizeToFloat16(original)

	if err != nil {
		t.Fatalf("Failed to quantize to float16: %v", err)
	}

	dequantizedF16 := vector.DequantizeFromFloat16(quantizedF16)
	errorF16 := vector.CalculateQuantizationError(original, dequantizedF16)

	quantizedBit, err := vector.QuantizeToBit(original)

	if err != nil {
		t.Fatalf("Failed to quantize to bit: %v", err)
	}

	t.Logf("✓ Large vector (%d dims) quantization errors:", dims)
	t.Logf("  Int8:    %.6f", error8)
	t.Logf("  Float16: %.6f", errorF16)
	t.Logf("  Bit:     %d bytes (deterministic binary)", len(quantizedBit))
}
