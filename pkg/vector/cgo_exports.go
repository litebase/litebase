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
