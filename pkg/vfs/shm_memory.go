package vfs

/*
#cgo linux LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup

#include <stdlib.h>
#include <string.h>
#include <vfs.h>
*/
import "C"
import (
	"sync"
	"unsafe"
)

var vfsShmMutex = &sync.Mutex{}
var VfsShmMap = make(map[string]*ShmMemory)

type ShmMemory struct {
	locks    map[int]int
	mutex    *sync.Mutex
	regions  []*ShmRegion
	nodeHash string
}

type ShmRegion struct {
	id    int
	pData unsafe.Pointer
	size  uintptr // Using uintptr instead of C.size_t for now
}

func (shm *ShmMemory) GetWALIndexHeader() []byte {
	shm.mutex.Lock()
	defer shm.mutex.Unlock()

	// Find page 0 (WAL index header page)
	for _, region := range shm.regions {
		if region.id == 0 && region.pData != nil {
			// Ensure we have enough bytes for the WAL index header
			if region.size >= 95 {
				return (*[1 << 28]byte)(region.pData)[:95:95]
			}
		}
	}

	return nil
}
