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
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"strings"
	"sync"
	"unsafe"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/storage"
)

var vfsMutex = &sync.RWMutex{}
var VfsMap = make(map[string]*LitebaseVFS)

var vfsBuffers = &sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 4096))
	},
}

type LitebaseVFS struct {
	filename   string
	fileSystem *storage.DurableDatabaseFileSystem
	id         string
	timestamp  int64
	vfsIdPtr   uintptr
	wal        WAL
	walHash    string
	shm        *ShmMemory
}

// Register a new VFS instance for a database connection.
func RegisterVFS(
	vfsHash string,
	vfsDatabaseHash string,
	pageSize int64,
	fileSystem *storage.DurableDatabaseFileSystem,
	wal WAL,
) (*LitebaseVFS, error) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if vfsHash == "" {
		return nil, errors.New("vfsHash cannot be empty")
	}

	if pageSize < 512 {
		return nil, errors.New("pageSize must be at least 512")
	}

	// Check for integer overflow when converting int64 to int32
	if pageSize > math.MaxInt32 {
		return nil, errors.New("pageSize exceeds maximum allowed value for int32")
	}

	// Only register the VFS if it doesn't already exist
	if lVfs, ok := VfsMap[vfsHash]; ok {
		return lVfs, nil
	}

	cZvfsId, err := utils.SafeCString(vfsHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert vfsHash to C string: %v", err)
	}
	defer C.free(unsafe.Pointer(cZvfsId))

	int32PageSize, err := utils.SafeInt64ToInt32(pageSize)

	if err != nil {
		return nil, fmt.Errorf("invalid pageSize: %v", err)
	}

	C.newVfs((*C.char)(cZvfsId), C.int(int32PageSize))

	// Check if the WAL is already registered
	if VfsShmMap[vfsDatabaseHash] == nil {
		VfsShmMap[vfsDatabaseHash] = &ShmMemory{
			locks:   make(map[int]int),
			mutex:   &sync.Mutex{},
			regions: make([]*ShmRegion, 0),
			walHash: vfsDatabaseHash,
		}
	}

	l := &LitebaseVFS{
		fileSystem: fileSystem,
		id:         vfsHash,
		wal:        wal,
		walHash:    vfsDatabaseHash,
		shm:        VfsShmMap[vfsDatabaseHash],
	}

	VfsMap[vfsHash] = l

	return l, nil
}

// Remove a VFS instance from the registry by its ID.
func UnregisterVFS(vfsId string) error {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if vfsId == "" {
		return errors.New("vfsId cannot be empty")
	}

	vfs, ok := VfsMap[vfsId]

	if !ok {
		return errors.New("vfsId not found")
	}

	var walHash string

	if vfs == nil {
		delete(VfsMap, vfsId) // Clean up the map entry
		return errors.New("vfs instance is nil")
	}

	cvfsId, err := utils.SafeCString(vfsId)
	if err != nil {
		return fmt.Errorf("failed to convert vfsId to C string: %v", err)
	}
	defer C.free(unsafe.Pointer(cvfsId))

	if cvfsId == nil {
		return errors.New("failed to create C string for vfsId")
	}

	C.unregisterVfs((*C.char)(cvfsId))

	walHash = vfs.walHash

	delete(VfsMap, vfsId)

	var found bool

	for _, vfs := range VfsMap {
		if vfs != nil && vfs.walHash == walHash {
			found = true
			break
		}
	}

	if !found && walHash != "" {
		delete(VfsShmMap, walHash)
	}

	return nil
}

// Check if a VFS is registered by its ID.
func VFSIsRegistered(vfsId string) bool {
	if vfsId == "" {
		return false
	}
	cVfsId, err := utils.SafeCString(vfsId)
	if err != nil {
		return false
	}
	defer C.free(unsafe.Pointer(cVfsId))

	vfsPointer := C.sqlite3_vfs_find((*C.char)(cVfsId))

	return vfsPointer != nil
}

// Set the timestamp for the VFS instance. This timestamp is used to
// consistently interact with the file system and WAL.
func (vfs *LitebaseVFS) SetTimestamp(timestamp int64) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	vfs.timestamp = timestamp
}

func (vfs *LitebaseVFS) Timestamp() int64 {
	return vfs.timestamp
}

func getVfsFromFile(pFile *C.sqlite3_file) (*LitebaseVFS, error) {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))
	vfsIdPtr := uintptr(unsafe.Pointer(file.pVfsId))

	vfsMutex.RLock()
	defer vfsMutex.RUnlock()

	for _, vfs := range VfsMap {
		if vfs.vfsIdPtr == vfsIdPtr {
			return vfs, nil
		}
	}

	vfsId := C.GoString(file.pVfsId)

	if vfs, ok := VfsMap[vfsId]; ok {
		vfs.vfsIdPtr = vfsIdPtr

		return vfs, nil
	}

	return nil, fmt.Errorf("vfs not found")
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

	vfs.filename = filename

	return C.SQLITE_OK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	var err error

	// Validate iAmt is positive to avoid integer overflow issues
	if iAmt < 0 {
		return C.SQLITE_IOERR_READ
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_READ
	}

	_, err = vfs.fileSystem.ReadAt(
		vfs.timestamp,
		goBuffer,
		int64(iOfst),
		int64(iAmt),
	)

	if err != nil && err != io.EOF {
		return C.SQLITE_IOERR_READ
	}

	return C.SQLITE_OK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	// Validate iAmt is positive to avoid integer overflow issues
	if iAmt < 0 {
		return C.SQLITE_IOERR_WRITE
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.fileSystem.WriteAt(vfs.timestamp, goBuffer, int64(iOfst))

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

	size, err := vfs.fileSystem.Size()

	if err != nil {
		log.Println("Error getting file size", err)
		return C.SQLITE_IOERR_FSTAT
	}

	*pSize = C.sqlite3_int64(size)

	return C.SQLITE_OK
}

//export goXSync
// func goXSync(pFile *C.sqlite3_file, flags C.int) C.int {
// 	vfs, err := getVfsFromFile(pFile)

// 	if err != nil {
// 		return C.SQLITE_IOERR_FSYNC
// 	}

// 	err = vfs.fileSystem.Sync()

// 	if err != nil {
// 		log.Println("Error syncing file", err)
// 		return C.SQLITE_IOERR_FSYNC
// 	}

// 	return C.SQLITE_OK
// }

//export goXShmMap
func goXShmMap(pFile *C.sqlite3_file, iPage C.int, pgsz C.int, bExtend C.int, pp *unsafe.Pointer) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_SHMMAP
	}

	vfs.shm.mutex.Lock()
	defer vfs.shm.mutex.Unlock()

	// Check if the shared memory region already exists
	for _, region := range vfs.shm.regions {
		if region.id == int(iPage) {
			*pp = region.pData

			return C.SQLITE_OK
		}
	}

	// Validate pgsz is positive before conversion to avoid integer overflow
	if pgsz <= 0 {
		slog.Error("goXShmMap: Invalid page size", "pgsz", pgsz)
		return C.SQLITE_NOMEM
	}

	uint64Pgsz, err := utils.SafeInt32ToUint64(int32(pgsz))

	if err != nil {
		slog.Error("goXShmMap: Invalid page size", "error", err)
		return C.SQLITE_NOMEM
	}

	// Allocate new shared memory region
	newRegion := &ShmRegion{
		id:    int(iPage),
		pData: C.malloc(C.size_t(uint64Pgsz)),
		size:  C.size_t(uint64Pgsz),
	}

	if newRegion.pData == nil {
		log.Printf("goXShmMap: Failed to allocate shared memory region %d\n", iPage)
		return C.SQLITE_NOMEM
	}

	vfs.shm.regions = append(vfs.shm.regions, newRegion)
	*pp = newRegion.pData

	return C.SQLITE_OK
}

//export goXShmLock
func goXShmLock(pFile *C.sqlite3_file, offset C.int, n C.int, flags C.int) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_SHMLOCK
	}

	vfs.shm.mutex.Lock()
	defer vfs.shm.mutex.Unlock()

	// Validate inputs
	if offset < 0 || int(offset)+int(n) > C.SQLITE_SHM_NLOCK || n < 1 ||
		(flags != (C.SQLITE_SHM_LOCK|C.SQLITE_SHM_SHARED) &&
			flags != (C.SQLITE_SHM_LOCK|C.SQLITE_SHM_EXCLUSIVE) &&
			flags != (C.SQLITE_SHM_UNLOCK|C.SQLITE_SHM_SHARED) &&
			flags != (C.SQLITE_SHM_UNLOCK|C.SQLITE_SHM_EXCLUSIVE)) {
		return C.SQLITE_IOERR_SHMLOCK
	}

	var rc C.int = C.SQLITE_OK

	// Check for unlock
	if flags&C.SQLITE_SHM_UNLOCK != 0 {
		// Unlock logic
		if flags&C.SQLITE_SHM_SHARED != 0 {
			if vfs.shm.locks[int(offset)] > 1 {
				vfs.shm.locks[int(offset)]--
			} else {
				vfs.shm.locks[int(offset)] = 0
			}
		} else {
			for i := int(offset); i < int(offset+n); i++ {
				vfs.shm.locks[i] = 0
			}
		}
	} else if flags&C.SQLITE_SHM_SHARED != 0 {
		// Shared lock logic
		if vfs.shm.locks[int(offset)] < 0 {
			rc = C.SQLITE_BUSY // Exclusive lock already held
		} else {
			vfs.shm.locks[int(offset)]++
		}
	} else {
		// Exclusive lock logic
		for i := int(offset); i < int(offset+n); i++ {
			if vfs.shm.locks[i] != 0 {
				rc = C.SQLITE_BUSY // Lock already held
				break
			}
		}

		if rc == C.SQLITE_OK {
			for i := int(offset); i < int(offset+n); i++ {
				vfs.shm.locks[i] = -1
			}
		}
	}

	return rc
}

//export goXShmUnmap
func goXShmUnmap(pFile *C.sqlite3_file, deleteFlag C.int) C.int {
	vfs, err := getVfsFromFile(pFile)
	if err != nil {
		return C.SQLITE_IOERR_SHMMAP
	}

	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	vfs.shm.mutex.Lock()
	defer vfs.shm.mutex.Unlock()

	var found int

	for _, vfsEntry := range VfsMap {
		if vfsEntry.id != vfs.id && vfsEntry.walHash == vfs.shm.walHash {
			found++
		}
	}

	if found < 1 {
		for _, region := range vfs.shm.regions {
			C.free(region.pData)
		}
	}

	vfs.shm.regions = make([]*ShmRegion, 0)

	return C.SQLITE_OK
}

//export goXShmBarrier
func goXShmBarrier(pFile *C.sqlite3_file) {
	// Implement barrier logic here
}

//export goXTruncate
func goXTruncate(pFile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	err = vfs.fileSystem.Truncate(int64(size))

	if err != nil {
		return C.SQLITE_IOERR_TRUNCATE
	}

	return C.SQLITE_OK
}

//export goXWALFileSize
func goXWALFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	if vfs.wal == nil {
		log.Println("WAL is nil")
		return C.SQLITE_IOERR
	}

	size, err := vfs.wal.Size(vfs.timestamp)

	if err != nil {
		log.Println("Error getting WAL file size", err)
		return C.SQLITE_IOERR
	}

	*pSize = C.sqlite3_int64(size)

	return C.SQLITE_OK
}

//export goXWALRead
func goXWALRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	if vfs.wal == nil {
		log.Println("WAL is nil")
		return C.SQLITE_IOERR
	}

	// buffer := vfsBuffers.Get().(*bytes.Buffer)
	// defer vfsBuffers.Put(buffer)

	// buffer.Reset()

	// if buffer.Len() < int(iAmt) {
	// 	buffer.Grow(int(iAmt))
	// }

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.wal.ReadAt(vfs.timestamp, goBuffer, int64(iOfst))

	if err != nil {
		if err == io.EOF {
			return C.SQLITE_OK
		}

		log.Println("Error reading WAL file", err)
		return C.SQLITE_IOERR
	}

	// if n < len(goBuffer) && err == io.EOF {
	// 	for i := n; i < len(goBuffer); i++ {
	// 		goBuffer[i] = 0
	// 	}

	// }

	return C.SQLITE_OK
}

//export goXWALWrite
func goXWALWrite(pFile *C.sqlite3_file, iAmt C.int, iOfst C.sqlite3_int64, zBuf unsafe.Pointer) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.wal.WriteAt(vfs.timestamp, goBuffer, int64(iOfst))

	if err != nil {
		log.Println("Error writing to WAL file", err)
		return C.SQLITE_IOERR
	}

	return C.SQLITE_OK
}

//export goXWALSync
func goXWALSync(pFile *C.sqlite3_file, flags C.int) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	err = vfs.wal.Sync(vfs.timestamp)

	if err != nil {
		log.Println("Error syncing WAL file", err)
		return C.SQLITE_IOERR
	}

	return C.SQLITE_OK
}

//export goXWALTruncate
func goXWALTruncate(pFile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	err = vfs.wal.Truncate(vfs.timestamp, int64(size))

	if err != nil {
		log.Println("Error truncating WAL file", err)
		return C.SQLITE_IOERR
	}

	return C.SQLITE_OK
}
