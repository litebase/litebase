package utils

/*
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"fmt"
)

// SafeCString safely converts a Go string to a C string with validation
// to avoid integer overflow issues in cgo generated code
func SafeCString(s string) (*C.char, error) {
	if len(s) == 0 {
		// For empty strings, allocate a single null byte
		ptr := C.malloc(1)
		if ptr == nil {
			return nil, errors.New("malloc failed")
		}
		(*[1]byte)(ptr)[0] = 0
		return (*C.char)(ptr), nil
	}

	// Validate string length is reasonable (less than 1GB)
	if len(s) > 1<<30 {
		return nil, errors.New("string too large")
	}

	// Use C.size_t explicitly and validate the conversion
	strLen := len(s)
	if strLen < 0 {
		return nil, errors.New("invalid string length")
	}

	uint64StrLen, err := SafeInt64ToUint64(int64(strLen))

	if err != nil {
		return nil, fmt.Errorf("invalid string length: %v", err)
	}

	size := C.size_t(uint64StrLen + 1)
	ptr := C.malloc(size)
	if ptr == nil {
		return nil, errors.New("malloc failed")
	}

	// Copy string data manually
	if strLen > 0 {
		copy((*[1 << 30]byte)(ptr)[:strLen], []byte(s))
	}
	// Add null terminator
	(*[1 << 30]byte)(ptr)[strLen] = 0

	return (*C.char)(ptr), nil
}

func StaticSafeCString(s string) *C.char {
	cStr, err := SafeCString(s)

	if err != nil {
		panic(fmt.Sprintf("StaticSafeCString failed: %v", err))
	}

	return cStr
}
