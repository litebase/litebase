package vfs

import "C"
import (
	"sync"
	"unsafe"
)

var vfsShmMutex = &sync.Mutex{}
var VfsShmMap = make(map[string]*ShmMemory)

type ShmMemory struct {
	locks   map[int]int
	mutex   *sync.Mutex
	regions []*ShmRegion
	walHash string
}

type ShmRegion struct {
	id    int
	pData unsafe.Pointer
	size  C.size_t
}
