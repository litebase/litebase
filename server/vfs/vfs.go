package vfs

/*
#cgo linux LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <vfs.h>
*/
import "C"

import (
	"fmt"
	"io"
	"litebase/server/storage"
	"log"
	"os"
	"strings"
	"sync"
	"unsafe"
)

var vfsMap = make(map[string]*LitebaseVFS)
var vfsLocks = make(map[string]*VfsLock)
var vfsMutex = &sync.RWMutex{}

type LitebaseVFS struct {
	filename    string
	id          string
	Lock        *VfsLock
	name        string
	storage     storage.DatabaseFileSystem
	tempStorage storage.DatabaseFileSystem
}

type GoLitebaseVFSFile struct {
	fileType string
	name     string
}

func RegisterVFS(
	connectionId string,
	vfsId string,
	storage storage.DatabaseFileSystem,
	tempStorage storage.DatabaseFileSystem,
) (*LitebaseVFS, error) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	// Only register the VFS if it doesn't already exist
	if vfs, ok := vfsMap[vfsId]; ok {
		return vfs, nil
	}

	if _, ok := vfsLocks[connectionId]; !ok {
		vfsLocks[connectionId] = NewVfsLock()
	}

	cZvsId := C.CString(vfsId)
	defer C.free(unsafe.Pointer(cZvsId))
	rc := C.newVfs(cZvsId)

	if rc != C.SQLITE_OK {
		return nil, errFromCode(int(rc))
	}

	vfsMap[vfsId] = &LitebaseVFS{
		id:          vfsId,
		Lock:        vfsLocks[connectionId],
		storage:     storage,
		tempStorage: tempStorage,
	}

	// log.Println("Registered VFS", vfsId, len(vfsMap))

	return vfsMap[vfsId], nil
}

func UnregisterVFS(conId, vfsId string) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	vfs := vfsMap[vfsId]

	if vfs != nil {
		cvfsId := C.CString(vfsId)
		defer C.free(unsafe.Pointer(cvfsId))

		C.unregisterVfs(cvfsId)
	}

	delete(vfsMap, vfsId)

	removeConnectionLock := false

	for _, vfs := range vfsMap {
		if vfs.id == vfsId {
			removeConnectionLock = true
			break
		}
	}

	// Remove vfs lock for the connection id if there are no more VFSs registered
	if removeConnectionLock {
		delete(vfsLocks, conId)
	}
}

//export goXOpen
func goXOpen(zVfs *C.sqlite3_vfs, zName *C.char, pFile *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {
	pVfs := (*C.LitebaseVFS)(unsafe.Pointer(zVfs))
	vfsId := C.GoString(pVfs.vfsId)
	name := C.GoString(zName)
	filename := name[strings.LastIndex(name, "/")+1:]
	vfsMutex.RLock()
	vfs, ok := vfsMap[vfsId]
	vfsMutex.RUnlock()

	if !ok {
		return C.SQLITE_IOERR
	}

	fileType := getFileType(name)

	switch fileType {
	case "journal", "wal":
		vfs.tempStorage.Open(filename)
	default:
		vfs.storage.Open(filename)
		vfs.filename = filename
	}

	return C.SQLITE_OK
}

//export goXDelete
func goXDelete(zVfs *C.sqlite3_vfs, zName *C.char, syncDir C.int) C.int {
	pVfs := (*C.LitebaseVFS)(unsafe.Pointer(zVfs))
	vfsId := C.GoString(pVfs.vfsId)
	name := C.GoString(zName)
	fileType := getFileType(name)
	vfsMutex.RLock()
	vfs := vfsMap[vfsId]
	vfsMutex.RUnlock()

	filename := name[strings.LastIndex(name, "/")+1:]

	if vfs == nil {
		log.Println("VFS not found")
		return C.SQLITE_OK
	}

	switch fileType {
	case "journal", "wal":
		vfs.tempStorage.Delete(filename)
	default:
		vfs.storage.Delete(filename)
	}

	return C.SQLITE_OK
}

//export goXAccess
func goXAccess(zVfs *C.sqlite3_vfs, zName *C.char, zFlags C.int, resOut *C.int) C.int {
	cVfs := (*C.LitebaseVFS)(unsafe.Pointer(zVfs))
	vfsId := C.GoString(cVfs.vfsId)
	fileType := getFileType(C.GoString(zName))

	vfsMutex.RLock()
	goVfs := vfsMap[vfsId]
	vfsMutex.RUnlock()

	// Check for the existence of the file
	if fileType == "journal" || fileType == "wal" {
		exists := goVfs.tempStorage.Exists()

		if exists {
			*resOut = C.int(0)
		} else {
			*resOut = C.int(1)
		}
	} else {
		*resOut = C.int(1)
	}

	return C.SQLITE_OK
}

//export goXClose
func goXClose(pFile *C.sqlite3_file) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_CLOSE
	}

	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	switch vfsFile.fileType {
	case "journal", "wal":
		vfs.tempStorage.Close(filename)
	default:
		vfs.storage.Close(filename)
	}

	return C.SQLITE_OK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_READ
	}

	// Get just the file name from the path
	vfsFile := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	name := C.GoString(vfsFile.pName)
	name = name[strings.LastIndex(name, "/")+1:]

	n, err := vfs.storage.ReadAt(
		name,
		goBuffer,
		int64(iOfst),
		int64(iAmt),
	)

	if err != nil && err != io.EOF {
		return errToC(err)
	}

	// n := copy(goBuffer, data)

	if n < len(goBuffer) && err == io.EOF {
		for i := n; i < len(goBuffer); i++ {
			goBuffer[i] = 0
		}
		log.Println("Short read", n, len(goBuffer))
		// return errToC(IOErrorShortRead)
	}

	return sqliteOK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.storage.WriteAt(vfs.filename, goBuffer, int64(iOfst))

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	return sqliteOK
}

//export goXTruncate
func goXTruncate(pFile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	switch vfsFile.fileType {
	case "journal", "wal":
		err = vfs.tempStorage.Truncate(filename, int64(size))
	default:
		err = vfs.storage.Truncate(filename, int64(size))
	}

	if err != nil {
		log.Println("Truncate error", err)

		if !os.IsNotExist(err) {
			return C.SQLITE_IOERR_TRUNCATE
		}
	}

	return sqliteOK
}

//export goXFileSize
func goXFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_FSTAT
	}

	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	size, err := vfs.storage.Size(filename)

	if err != nil {
		log.Println("Error getting file size", err)
		return C.SQLITE_IOERR_FSTAT
	}

	*pSize = C.sqlite3_int64(size)

	return sqliteOK
}

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}

func getVfsFromFile(pFile *C.sqlite3_file) (*LitebaseVFS, error) {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	vfsId := C.GoString(file.pVfsId)

	vfsMutex.RLock()
	defer vfsMutex.RUnlock()

	vfs, ok := vfsMap[vfsId]

	if !ok {
		return nil, fmt.Errorf("vfs not found")
	}

	return vfs, nil
}

func getFile(pFile *C.sqlite3_file) GoLitebaseVFSFile {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	name := C.GoString(file.pName)

	return GoLitebaseVFSFile{
		fileType: getFileType(name),
		name:     name,
	}
}

func getFileType(name string) string {
	if strings.HasSuffix(name, "-journal") {
		return "journal"
	}

	if strings.HasSuffix(name, "-wal") {
		return "wal"
	}

	return "main"
}
