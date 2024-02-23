package vfs

import "C"
import (
	"unsafe"
)

type P2Cache struct {
}

//export goSpillCachePage
func goSpillCachePage(cacheId *C.char, page C.int, data unsafe.Pointer) C.int {
	cacheIdStr := C.GoString((*C.char)(cacheId))
	offset := int64(page-1) * 4096

	goBuffer := (*[1 << 28]byte)(data)[:int(4096):int(4096)]

	vfsMap[cacheIdStr].storage.Cache().Put(offset, goBuffer)

	return C.int(0)
}
