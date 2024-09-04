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
	"fmt"
	"io"
	"litebase/server/storage"
	"log"
	"runtime/cgo"
	"strings"
	"sync"
	"unsafe"
)

var VfsMap = make(map[string]*LitebaseVFS)
var vfsMutex = &sync.RWMutex{}

type LitebaseVFS struct {
	filename   string
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

	log.Println("Registered VFS", vfsId)
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

func getVfsFromFile(pFile *C.sqlite3_file) (*LitebaseVFS, error) {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	vfsId := C.GoString(file.pVfsId)

	vfsMutex.RLock()
	defer vfsMutex.RUnlock()

	vfs, ok := VfsMap[vfsId]

	if !ok {
		return nil, fmt.Errorf("vfs not found")
	}

	return vfs, nil
}

//export goXOpen
func goXOpen(zVfs *C.sqlite3_vfs, zName *C.char, pFile *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {
	vfsId := C.GoString(zVfs.zName)
	name := C.GoString(zName)
	filename := name[strings.LastIndex(name, "/")+1:]

	vfsMutex.RLock()
	vfs, ok := VfsMap[vfsId]
	vfsMutex.RUnlock()

	if !ok {
		return C.SQLITE_IOERR
	}

	// fileType := getFileType(name)

	// switch fileType {
	// case "journal", "wal":
	// 	vfs.tempStorage.Open(filename)
	// default:
	vfs.fileSystem.Open(filename)
	vfs.filename = filename
	// }

	return C.SQLITE_OK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR_READ
	}

	// Get just the file name from the path
	vfsFile := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	name := C.GoString(vfsFile.pName)
	name = name[strings.LastIndex(name, "/")+1:]

	n, err := vfs.fileSystem.ReadAt(
		name,
		goBuffer,
		int64(iOfst),
		int64(iAmt),
	)

	if err != nil && err != io.EOF {
		return C.SQLITE_IOERR_READ
	}

	if n < len(goBuffer) && err == io.EOF {
		for i := n; i < len(goBuffer); i++ {
			goBuffer[i] = 0
		}

		log.Println("Short read", n, len(goBuffer))
		// return errToC(IOErrorShortRead)
	}

	return C.SQLITE_OK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.fileSystem.WriteAt(vfs.filename, goBuffer, int64(iOfst))

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	return C.SQLITE_OK
}

//export goXFileSize
func goXFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_FSTAT
	}

	size, err := vfs.fileSystem.Size(vfs.filename)

	if err != nil {
		log.Println("Error getting file size", err)
		return C.SQLITE_IOERR_FSTAT
	}

	*pSize = C.sqlite3_int64(size)

	return C.SQLITE_OK
}

//export goXTruncate
func goXTruncate(pFile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	err = vfs.fileSystem.Truncate(vfs.filename, int64(size))

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	return C.SQLITE_OK
}

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}
