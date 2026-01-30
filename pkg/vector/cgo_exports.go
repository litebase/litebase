package vector

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"log/slog"
	"unsafe"
)

//export goEncodeVector
func goEncodeVector(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	return GoEncodeVector(jsonStr, blobLen)
}

// GoEncodeVector is the exported Go function that can be called from other packages
func GoEncodeVector(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	values, err := ParseJSONArray(jsonString)

	if err != nil {
		slog.Error("Failed to parse JSON array", "error", err)
		*blobLen = 0

		return nil
	}

	blob, err := EncodeFloat32(values)

	if err != nil {
		slog.Error("Failed to encode vector", "error", err)
		*blobLen = 0

		return nil
	}

	*blobLen = C.int(len(blob))

	// Allocate C memory and copy blob
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0

		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goFreeVector
func goFreeVector(ptr unsafe.Pointer) {
	GoFreeVector(ptr)
}

// GoFreeVector is the exported Go function that can be called from other packages
func GoFreeVector(ptr unsafe.Pointer) {
	C.free(ptr)
}

//export goEncodeVectorF64
func goEncodeVectorF64(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	values, err := ParseJSONArrayFloat64(jsonString)

	if err != nil {
		slog.Error("Failed to parse JSON array", "error", err)
		*blobLen = 0

		return nil
	}

	blob, err := EncodeFloat64(values)

	if err != nil {
		slog.Error("Failed to encode vector", "error", err)
		*blobLen = 0

		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goEncodeVectorInt8
func goEncodeVectorInt8(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	values, err := ParseJSONArrayInt8(jsonString)

	if err != nil {
		slog.Error("Failed to parse JSON array", "error", err)
		*blobLen = 0
		return nil
	}

	blob, err := EncodeInt8(values)

	if err != nil {
		slog.Error("Failed to encode vector", "error", err)
		*blobLen = 0
		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goEncodeVectorInt16
func goEncodeVectorInt16(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	values, err := ParseJSONArrayInt16(jsonString)

	if err != nil {
		slog.Error("Failed to parse JSON array", "error", err)
		*blobLen = 0
		return nil
	}

	blob, err := EncodeInt16(values)

	if err != nil {
		slog.Error("Failed to encode vector", "error", err)
		*blobLen = 0
		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goEncodeVectorF16
func goEncodeVectorF16(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	blob, err := ParseJSONArrayFloat16(jsonString)

	if err != nil {
		slog.Error("Failed to parse/encode float16 vector", "error", err)
		*blobLen = 0
		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goEncodeVectorBit
func goEncodeVectorBit(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	blob, err := ParseJSONArrayBit(jsonString)

	if err != nil {
		slog.Error("Failed to parse/encode bit vector", "error", err)
		*blobLen = 0
		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goEncodeVectorSparse
func goEncodeVectorSparse(jsonStr *C.char, blobLen *C.int) unsafe.Pointer {
	jsonString := C.GoString(jsonStr)

	blob, err := ParseJSONSparse(jsonString)

	if err != nil {
		slog.Error("Failed to parse/encode sparse vector", "error", err)
		*blobLen = 0
		return nil
	}

	*blobLen = C.int(len(blob))
	cBlob := C.malloc(C.size_t(len(blob)))

	if cBlob == nil {
		*blobLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&blob[0]), C.size_t(len(blob)))

	return cBlob
}

//export goQuantizeToInt8
func goQuantizeToInt8(blobPtr unsafe.Pointer, blobLen C.int, resultLen *C.int, scaleOut *C.float, offsetOut *C.float) unsafe.Pointer {
	blob := C.GoBytes(blobPtr, blobLen)

	vb, err := ParseVectorBlob(blob)

	if err != nil {
		slog.Error("Failed to parse vector blob", "error", err)
		*resultLen = 0

		return nil
	}

	vec := vb.GetFloat32Slice()

	if vec == nil {
		slog.Error("Vector is not float32 type")
		*resultLen = 0

		return nil
	}

	quantized, scale, offset, err := QuantizeToInt8(vec)

	if err != nil {
		slog.Error("Failed to quantize vector", "error", err)
		*resultLen = 0

		return nil
	}

	resultBlob, err := EncodeInt8(quantized)

	if err != nil {
		slog.Error("Failed to encode quantized vector", "error", err)
		*resultLen = 0

		return nil
	}

	*scaleOut = C.float(scale)
	*offsetOut = C.float(offset)
	*resultLen = C.int(len(resultBlob))

	cBlob := C.malloc(C.size_t(len(resultBlob)))

	if cBlob == nil {
		*resultLen = 0

		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&resultBlob[0]), C.size_t(len(resultBlob)))

	return cBlob
}

//export goQuantizeToInt16
func goQuantizeToInt16(blobPtr unsafe.Pointer, blobLen C.int, resultLen *C.int, scaleOut *C.float, offsetOut *C.float) unsafe.Pointer {
	blob := C.GoBytes(blobPtr, blobLen)

	vb, err := ParseVectorBlob(blob)

	if err != nil {
		slog.Error("Failed to parse vector blob", "error", err)
		*resultLen = 0
		return nil
	}

	vec := vb.GetFloat32Slice()

	if vec == nil {
		slog.Error("Vector is not float32 type")
		*resultLen = 0

		return nil
	}

	quantized, scale, offset, err := QuantizeToInt16(vec)

	if err != nil {
		slog.Error("Failed to quantize vector", "error", err)
		*resultLen = 0
		return nil
	}

	resultBlob, err := EncodeInt16(quantized)

	if err != nil {
		slog.Error("Failed to encode quantized vector", "error", err)
		*resultLen = 0

		return nil
	}

	*scaleOut = C.float(scale)
	*offsetOut = C.float(offset)
	*resultLen = C.int(len(resultBlob))

	cBlob := C.malloc(C.size_t(len(resultBlob)))

	if cBlob == nil {
		*resultLen = 0

		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&resultBlob[0]), C.size_t(len(resultBlob)))

	return cBlob
}

//export goQuantizeToFloat16
func goQuantizeToFloat16(blobPtr unsafe.Pointer, blobLen C.int, resultLen *C.int) unsafe.Pointer {
	blob := C.GoBytes(blobPtr, blobLen)

	vb, err := ParseVectorBlob(blob)

	if err != nil {
		slog.Error("Failed to parse vector blob", "error", err)
		*resultLen = 0

		return nil
	}

	vec := vb.GetFloat32Slice()

	if vec == nil {
		slog.Error("Vector is not float32 type")
		*resultLen = 0

		return nil
	}

	quantized, err := QuantizeToFloat16(vec)

	if err != nil {
		slog.Error("Failed to quantize vector", "error", err)
		*resultLen = 0

		return nil
	}

	resultBlob, err := EncodeFloat16FromUint16(quantized)

	if err != nil {
		slog.Error("Failed to encode quantized vector", "error", err)
		*resultLen = 0

		return nil
	}

	*resultLen = C.int(len(resultBlob))

	cBlob := C.malloc(C.size_t(len(resultBlob)))

	if cBlob == nil {
		*resultLen = 0

		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&resultBlob[0]), C.size_t(len(resultBlob)))

	return cBlob
}

//export goQuantizeToBit
func goQuantizeToBit(blobPtr unsafe.Pointer, blobLen C.int, resultLen *C.int) unsafe.Pointer {
	blob := C.GoBytes(blobPtr, blobLen)

	vb, err := ParseVectorBlob(blob)

	if err != nil {
		slog.Error("Failed to parse vector blob", "error", err)
		*resultLen = 0

		return nil
	}

	vec := vb.GetFloat32Slice()

	if vec == nil {
		slog.Error("Vector is not float32 type")
		*resultLen = 0

		return nil
	}

	quantized, err := QuantizeToBit(vec)

	if err != nil {
		slog.Error("Failed to quantize vector", "error", err)
		*resultLen = 0

		return nil
	}

	resultBlob, err := EncodeBitFromBytes(quantized, len(vec))

	if err != nil {
		slog.Error("Failed to encode quantized vector", "error", err)
		*resultLen = 0

		return nil
	}

	*resultLen = C.int(len(resultBlob))

	cBlob := C.malloc(C.size_t(len(resultBlob)))

	if cBlob == nil {
		*resultLen = 0
		return nil
	}

	C.memcpy(cBlob, unsafe.Pointer(&resultBlob[0]), C.size_t(len(resultBlob)))

	return cBlob
}

//export goComputeHammingDistance
func goComputeHammingDistance(blobPtr1 unsafe.Pointer, blobLen1 C.int, blobPtr2 unsafe.Pointer, blobLen2 C.int) C.int {
	blob1 := C.GoBytes(blobPtr1, blobLen1)
	blob2 := C.GoBytes(blobPtr2, blobLen2)

	vb1, err := ParseVectorBlob(blob1)

	if err != nil {
		slog.Error("Failed to parse first vector blob", "error", err)

		return -1
	}

	vb2, err := ParseVectorBlob(blob2)

	if err != nil {
		slog.Error("Failed to parse second vector blob", "error", err)

		return -1
	}

	distance, err := DistanceHamming(vb1, vb2)

	if err != nil {
		slog.Error("Failed to compute hamming distance", "error", err)

		return -1
	}

	return C.int(distance)
}

//export goVectorIndexStats
func goVectorIndexStats(dbPtr unsafe.Pointer, tableName *C.char) *C.char {
	// Stub implementation
	return C.CString("{}")
}

//export goNotifyVectorInsert
func goNotifyVectorInsert(databaseID, branchID, tableName *C.char) {
	NotifyVectorInsert(
		C.GoString(databaseID),
		C.GoString(branchID),
		C.GoString(tableName),
	)
}
