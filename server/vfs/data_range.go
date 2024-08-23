package vfs

/*
#include "./data_range.h"
*/
import "C"
import (
	"litebase/server/file"
	"unsafe"
)

func CNewDataRange(path string, number int, pageSize int) *C.DataRange {
	CPath := C.CString(path)
	CNumber := C.int(number)
	CPageSize := C.int(pageSize)
	defer C.free(unsafe.Pointer(CPath))

	cDataRange := C.NewDataRange(CPath, CNumber, CPageSize)
	// print the memory address of the data range

	return cDataRange
}

func CDataRangeReadAt(dataRange unsafe.Pointer, data []byte, offset int64) (int, error) {
	CBuffer := C.CBytes(data)
	CIamt := C.int(len(data))
	CPageNumber := C.int(offset)
	CReadBytes := C.int(0)
	defer C.free(CBuffer)

	C.DataRangeReadAt((*C.DataRange)(dataRange), CBuffer, CIamt, CPageNumber, &CReadBytes)

	return int(CReadBytes), nil
}

func CDataRangeWriteAt(dataRange *C.DataRange, data []byte, offset int64) int {
	CBuffer := C.CBytes(data)
	CPageNumber := C.int(file.PageNumber(offset, 4096))
	defer C.free(CBuffer)

	return int(C.DataRangeWriteAt(dataRange, CBuffer, CPageNumber))
}

func CDataRangeClose(dataRange *C.DataRange) int {
	return int(C.DataRangeClose(dataRange))
}

func CDataRangeRemove(dataRange *C.DataRange) int {
	return int(C.DataRangeRemove(dataRange))
}

func CDataRangeSize(dataRange *C.DataRange) int {
	sizerPtr := 0

	// Pass *C.int as a pointer to the sizerPtr
	C.DataRangeSize(dataRange, (*C.int)(unsafe.Pointer(&sizerPtr)))

	return sizerPtr
}

func CDataRangeTruncate(dataRange *C.DataRange, size int) int {
	return int(C.DataRangeTruncate(dataRange, C.int(size)))
}
