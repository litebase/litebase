package vfs

/*
#cgo linux LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <vfs.h>

extern void go_write_hook(uintptr_t vfsHandle, int iAmt, sqlite3_int64 iOfst, void* zBuf);
*/
import "C"

import (
	"errors"
	"litebase/server/storage"
	"runtime/cgo"
	"sync"
	"unsafe"
)

var VfsMap = make(map[string]*LitebaseVFS)
var vfsMutex = &sync.RWMutex{}

type LitebaseVFS struct {
	fileSystem storage.DatabaseFileSystem
	id         string
}

func RegisterVFS(
	connectionId string,
	vfsId string,
	dataPath string,
	pageSize int64,
	fileSystem storage.DatabaseFileSystem,
) error {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if connectionId == "" {
		return errors.New("connectionId cannot be empty")
	}

	if vfsId == "" {
		return errors.New("vfsId cannot be empty")
	}

	if dataPath == "" {
		return errors.New("dataPath cannot be empty")
	}

	if pageSize < 512 {
		return errors.New("pageSize must be at least 512")
	}

	// Only register the VFS if it doesn't already exist
	if _, ok := VfsMap[vfsId]; ok {
		return nil
	}

	cZvfsId := C.CString(vfsId)
	defer C.free(unsafe.Pointer(cZvfsId))

	cDataPath := C.CString(dataPath)
	defer C.free(unsafe.Pointer(cDataPath))

	C.newVfs(cZvfsId, cDataPath, C.int(pageSize))

	l := &LitebaseVFS{
		fileSystem: fileSystem,
		id:         vfsId,
	}

	l.writeHook()

	VfsMap[vfsId] = l

	return nil
}

func UnregisterVFS(conId, vfsId string) error {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	vfs := VfsMap[vfsId]

	if vfs != nil {
		cvfsId := C.CString(vfsId)
		defer C.free(unsafe.Pointer(cvfsId))

		C.unregisterVfs(cvfsId)
	}

	delete(VfsMap, vfsId)

	return nil
}

func VFSIsRegistered(vfsId string) bool {
	cVfsId := C.CString(vfsId)
	defer C.free(unsafe.Pointer(cVfsId))

	vfsPointer := C.sqlite3_vfs_find(C.CString(vfsId))

	return vfsPointer != nil
}

// Setup the write hook for the VFS to receive write events from SQLite.
func (l *LitebaseVFS) writeHook() error {
	vfsHandle := cgo.NewHandle(l)

	cVfsId := C.CString(l.id)
	defer C.free(unsafe.Pointer(cVfsId))

	C.litebase_vfs_write_hook(
		cVfsId,
		(*[0]byte)(C.go_write_hook),
		unsafe.Pointer(vfsHandle),
	)

	return nil
}

//export go_write_hook
func go_write_hook(vfsHandle C.uintptr_t, iAmt C.int, iOfst C.sqlite3_int64, zBuf unsafe.Pointer) {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	handle := cgo.Handle(vfsHandle)
	l := handle.Value().(*LitebaseVFS)

	l.fileSystem.WriteHook(int64(iOfst), goBuffer)
}
