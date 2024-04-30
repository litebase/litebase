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
	"io"
	"litebasedb/server/storage"
	"log"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var vfsMap = make(map[string]*LitebaseVFS)

// var vfsLocks = make(map[string]*sync.RWMutex)
var vfsMutex = &sync.RWMutex{}

type LitebaseVFS struct {
	id              string
	journalFile     *C.LitebaseVFSFile
	lockType        int
	mainFile        *C.LitebaseVFSFile
	storage         *storage.LocalDatabaseFileSystem
	tempStorage     *storage.LocalDatabaseFileSystem
	TransactionLock *sync.RWMutex
	shmPointer      unsafe.Pointer
	walFile         *C.LitebaseVFSFile
}

type GoLitebaseVFSFile struct {
	fileType string
	name     string
}

func RegisterVFS(
	id string,
	storage *storage.LocalDatabaseFileSystem,
	tempStorage *storage.LocalDatabaseFileSystem,
) (*LitebaseVFS, error) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	// Only register the VFS if it doesn't already exist
	// TODO: Need to remove vfs if all database connections are closed.
	if vfs, ok := vfsMap[id]; ok {
		return vfs, nil
	}

	cZvsId := C.CString(id)
	defer C.free(unsafe.Pointer(cZvsId))
	rc := C.newVfs(cZvsId)

	if rc != C.SQLITE_OK {
		return nil, errFromCode(int(rc))
	}

	vfsMap[id] = &LitebaseVFS{
		id:              id,
		storage:         storage,
		tempStorage:     tempStorage,
		TransactionLock: sync.RWMutex{},
	}

	log.Println("Registered VFS", id, len(vfsMap))

	return vfsMap[id], nil
}

func UnregisterVFS(id string) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	delete(vfsMap, id)
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
		panic("VFS not found")
	}

	fileType := getFileType(zName)
	// log.Println("Open", filename)
	switch fileType {
	case "journal":
		vfs.tempStorage.Open(filename)
		vfs.journalFile = (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	case "wal":
		vfs.tempStorage.Open(filename)
		vfs.walFile = (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	default:
		vfs.storage.Open(filename)
		vfs.mainFile = (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	}

	// Set the id to the fileID pointer
	// goBuffer := (*[1 << 28]byte)(zId)[:len(vfsId):len(vfsId)]

	// copy(goBuffer, vfsId)

	return C.SQLITE_OK
}

//export goXDelete
func goXDelete(zVfs *C.sqlite3_vfs, zName *C.char, syncDir C.int) C.int {
	pVfs := (*C.LitebaseVFS)(unsafe.Pointer(zVfs))
	vfsId := C.GoString(pVfs.vfsId)
	fileType := getFileType(zName)
	name := C.GoString(zName)
	vfsMutex.RLock()
	vfs := vfsMap[vfsId]
	vfsMutex.RUnlock()

	filename := name[strings.LastIndex(name, "/")+1:]

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
	log.Println("Access", vfsId)
	fileType := getFileType(zName)

	vfsMutex.RLock()
	goVfs := vfsMap[vfsId]
	vfsMutex.RUnlock()

	// Check for the existence of the file
	if fileType == "journal" {
		if goVfs.journalFile == nil {
			*resOut = C.int(0)
		} else {
			*resOut = C.int(1)
		}
	} else if fileType == "wal" {
		if goVfs.walFile == nil {
			*resOut = C.int(0)
		} else {
			*resOut = C.int(1)
		}
	} else {
		*resOut = C.int(1)
	}

	return C.SQLITE_OK
}

//export goXFullPathname
func goXFullPathname(zVfs *C.sqlite3_vfs, zName *C.char, nOut C.int, zOut *C.char) C.int {
	cVfs := (*C.LitebaseVFS)(unsafe.Pointer(zVfs))
	vfsId := C.GoString(cVfs.vfsId)

	log.Println("FullPathname", vfsId)
	vfsMutex.RLock()
	vfs := vfsMap[vfsId]
	vfsMutex.RUnlock()

	fileType := getFileType(zName)
	name := C.GoString(zName)
	filename := name[strings.LastIndex(name, "/")+1:]

	var s string

	switch fileType {
	case "journal":
	case "wal":
		s = vfs.tempStorage.Path() + "/" + filename
	default:
		s = vfs.storage.Path() + "/" + filename
	}

	path := C.CString(s)

	defer C.free(unsafe.Pointer(path))

	if len(s)+1 >= int(nOut) {
		return C.SQLITE_TOOBIG
	}

	C.memcpy(unsafe.Pointer(zOut), unsafe.Pointer(path), C.size_t(len(s)+1))

	return sqliteOK
}

//export goXSleep
func goXSleep(cvfs *C.sqlite3_vfs, microseconds C.int) C.int {
	d := time.Duration(microseconds) * time.Microsecond

	time.Sleep(d)

	return sqliteOK
}

//export goXClose
func goXClose(pFile *C.sqlite3_file) C.int {
	vfs := getVfsFromFile(pFile)
	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	switch vfsFile.fileType {
	case "journal":
		vfs.tempStorage.Close(filename)
		vfs.journalFile = nil
	case "wal":
		vfs.tempStorage.Close(filename)
		vfs.walFile = nil
	default:
		vfs.storage.Close(filename)
	}

	return C.SQLITE_OK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	vfs := getVfsFromFile(pFile)
	vfsFile := getFile(pFile)
	var data []byte
	var err error
	// Get just the file name from the path
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	switch vfsFile.fileType {
	case "journal":
	case "wal":
		data, err = vfs.tempStorage.ReadAt(filename, int64(iOfst), int64(iAmt))
	default:
		data, err = vfs.storage.ReadAt(filename, int64(iOfst), int64(iAmt))
	}

	if err != nil && err != io.EOF {
		return errToC(err)
	}

	n := copy(goBuffer, data)

	if n < len(goBuffer) && err == io.EOF {
		for i := n; i < len(goBuffer); i++ {
			goBuffer[i] = 0
		}

		return errToC(IOErrorShortRead)
	}

	return sqliteOK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	vfs := getVfsFromFile(pFile)
	vfsFile := getFile(pFile)

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	switch vfsFile.fileType {
	case "journal":
	case "wal":
		vfs.tempStorage.WriteAt(filename, goBuffer, int64(iOfst))
	default:
		_, err := vfs.storage.WriteAt(filename, goBuffer, int64(iOfst))

		if err != nil {
			return C.SQLITE_IOERR_WRITE
		}
	}

	return sqliteOK
}

//export goXTruncate
func goXTruncate(pFile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	vfs := getVfsFromFile(pFile)
	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	// log.Println("Truncate", file.fileType)
	var err error

	switch vfsFile.fileType {
	case "journal":
	case "wal":
		err = vfs.tempStorage.Truncate(filename, int64(size))
	default:
		err = vfs.storage.Truncate(filename, int64(size))
	}

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	return sqliteOK
}

//export goXFileSize
func goXFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	vfs := getVfsFromFile(pFile)
	vfsFile := getFile(pFile)
	filename := vfsFile.name[strings.LastIndex(vfsFile.name, "/")+1:]

	var size int64
	var err error

	// log.Println("FileSize", file.fileType)

	switch vfsFile.fileType {
	case "journal":
	case "wal":
		size, err = vfs.tempStorage.Size(filename)

		if err != nil {
			return C.SQLITE_IOERR_FSTAT
		}
	default:
		size, err = vfs.storage.Size(filename)

		if err != nil {
			return C.SQLITE_IOERR_FSTAT
		}
	}

	*pSize = C.sqlite3_int64(size)

	return sqliteOK
}

//export goXLock
func goXLock(pFile *C.sqlite3_file, lockType C.int) C.int {
	vfs := getVfsFromFile(pFile)

	// If the lock type is already at the requested level, return SQLITE_OK
	if vfs.lockType >= int(lockType) {
		return C.SQLITE_OK
	}

	vfs.lockType = int(lockType)

	return C.SQLITE_OK
}

//export goXUnlock
func goXUnlock(pFile *C.sqlite3_file, lockType C.int) C.int {
	vfs := getVfsFromFile(pFile)

	// If the lock type is lower or equal to the requested level, return SQLITE_OK
	if vfs.lockType <= int(lockType) {
		return C.SQLITE_OK
	}

	vfs.lockType = int(lockType)

	return C.SQLITE_OK
}

//export goXCheckReservedLock
func goXCheckReservedLock(pFile *C.sqlite3_file, pResOut *C.int) C.int {
	vfs := getVfsFromFile(pFile)

	if vfs.lockType >= int(C.SQLITE_LOCK_RESERVED) {
		*pResOut = 1
	} else {
		*pResOut = 0
	}

	return C.SQLITE_OK
}

//export goXShmMap
// func goXShmMap(pFile *C.sqlite3_file, iPage C.int, pgsz C.int, bExtend C.int, pp uint32) C.int {
// 	// log.Println("ShmMap", iPage, pgsz, bExtend)
// 	vfs := getVfsFromFile(pFile)
// 	// ptr := C.malloc(C.size_t(pgsz))
// 	// pp = ptr
// 	vfs.sharedMemory.Map(int(iPage), int(pgsz), int(bExtend), pp)

// 	return C.SQLITE_OK
// }

//export goXShmLock
// func goXShmLock(pFile *C.sqlite3_file, offset C.int, n C.int, flags C.int) C.int {
// 	vfs := getVfsFromFile(pFile)

// 	// If SQLITE_SHM_UNLOCK is set, unlock the shared memory segment
// 	if flags&C.SQLITE_SHM_UNLOCK != 0 {
// 		vfs.sharedMemory.Unlock(int64(offset), int64(n))
// 		// If SQLITE_SHM_SHARED is set, apply a shared lock
// 	} else if flags&C.SQLITE_SHM_SHARED != 0 {
// 		rc := vfs.sharedMemory.SharedLock(int64(offset), int64(n))

// 		if rc != 0 {
// 			return C.SQLITE_BUSY
// 		}
// 		// If SQLITE_SHM_EXCLUSIVE is set, apply an exclusive lock
// 	} else if flags&C.SQLITE_SHM_EXCLUSIVE != 0 {
// 		rc := vfs.sharedMemory.ExclusiveLock(int64(offset), int64(n))

// 		if rc != 0 {
// 			return C.SQLITE_BUSY
// 		}
// 	}

// 	return C.SQLITE_OK
// }

//export goXShmBarrier
// func goXShmBarrier(pFile *C.sqlite3_file) {
// 	// log.Println("ShmBarrier")

// 	// Implement a memory barrier by using atomic operations
// 	// var val int32
// 	// atomic.StoreInt32(&val, 1)
// 	// atomic.LoadInt32(&val)
// }

//export goXShmUnmap
// func goXShmUnmap(pFile *C.sqlite3_file, deleteFlag C.int) C.int {
// 	log.Println("ShmUnmap", deleteFlag)
// 	vfs := getVfsFromFile(pFile)

// 	vfs.sharedMemory.Unmap(0)

// 	return C.SQLITE_OK
// }

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}

func getVfsFromFile(pFile *C.sqlite3_file) *LitebaseVFS {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	vfsId := C.GoString(file.pVfsId)

	vfsMutex.RLock()
	defer vfsMutex.RUnlock()

	vfs, ok := vfsMap[vfsId]

	if !ok {
		panic("VFS not found")
	}

	return vfs

}

// func getVfsId(name *C.char) string {
// 	var id string

// 	// The id is the string before .db
// 	id = C.GoString(name)
// 	// Get the string after the last slash
// 	id = id[strings.LastIndex(id, "/")+1:]
// 	id = strings.TrimSuffix(id, ".db")
// 	id = strings.TrimSuffix(id, ".db-journal")
// 	id = strings.TrimSuffix(id, ".db-wal")

// 	return fmt.Sprintf("litebase:%s", id)
// }

func getFile(pFile *C.sqlite3_file) GoLitebaseVFSFile {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))

	return GoLitebaseVFSFile{
		fileType: getFileType(file.pName),
		name:     C.GoString(file.pName),
		// id:       C.GoString(file.id),
	}
}

func getFileType(name *C.char) string {
	nameStr := C.GoString(name)

	if strings.HasSuffix(nameStr, "-journal") {
		return "journal"
	}

	if strings.HasSuffix(nameStr, "-wal") {
		return "wal"
	}

	return "main"
}
