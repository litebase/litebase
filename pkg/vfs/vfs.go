package vfs

/*
#cgo linux LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup

#cgo nocallback newVfs
#cgo nocallback sqlite3_vfs_find
#cgo nocallback unregisterVfs

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <vfs.h>
*/
import "C"

import (
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
var VfsMap = make(map[uintptr]*LitebaseVFS)

type LitebaseVFS struct {
	connectionHash         string
	connectionID           string // Extracted from connectionHash for buffer ownership
	connectionManager      ConnectionManager
	databaseHash           string
	filename               string
	fileSystem             *storage.DurableDatabaseFileSystem
	nodeHash               string
	transactionalTimestamp int64
	VfsIdPtr               uintptr
	wal                    WAL
	walTimestamp           int64
	shm                    *ShmMemory
}

// ConnectionManager interface for accessing database connections
type ConnectionManager interface {
	Get(databaseID, branchID string) (any, error)
	Release(conn any)
}

// GetVfsFromId retrieves a VFS instance by its ID string
func GetVfsFromId(vfsId string) (*LitebaseVFS, error) {
	if vfsId == "" {
		return nil, errors.New("vfsId cannot be empty")
	}

	cvfsId, err := utils.SafeCString(vfsId)

	if err != nil {
		return nil, fmt.Errorf("failed to convert vfsId to C string: %v", err)
	}

	defer C.free(unsafe.Pointer(cvfsId))

	pVfs := C.sqlite3_vfs_find((*C.char)(cvfsId))

	if pVfs == nil {
		return nil, fmt.Errorf("VFS not found: %s", vfsId)
	}

	vfsIdPtr := uintptr(unsafe.Pointer(pVfs.zName))

	vfsMutex.RLock()
	defer vfsMutex.RUnlock()

	vfs, ok := VfsMap[vfsIdPtr]

	if !ok {
		return nil, fmt.Errorf("VFS not registered: %s", vfsId)
	}

	return vfs, nil
}

// SetConnectionManager sets the connection manager for this VFS instance
func (l *LitebaseVFS) SetConnectionManager(cm ConnectionManager) {
	l.connectionManager = cm
}

// ConnectionManager returns the connection manager for this VFS
func (l *LitebaseVFS) ConnectionManager() ConnectionManager {
	return l.connectionManager
}

// Register a new VFS instance for a database connection.
func RegisterVFS(
	databaseHash string, // Database ID + Branch ID
	connectionHash string, // Database ID + Branch ID + Connection ID
	nodeHash string, // Node ID + Database ID + Branch ID
	pageSize int64,
	fileSystem *storage.DurableDatabaseFileSystem,
	wal WAL,
	connectionID string, // Connection ID for buffer ownership tracking
) (*LitebaseVFS, error) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if pageSize < 512 {
		return nil, errors.New("pageSize must be at least 512")
	}

	// Check for integer overflow when converting int64 to int32
	if pageSize > math.MaxInt32 {
		return nil, errors.New("pageSize exceeds maximum allowed value for int32")
	}

	cZvfsId, err := utils.SafeCString(connectionHash)

	if err != nil {
		return nil, fmt.Errorf("failed to convert connectionHash to C string: %v", err)
	}

	defer C.free(unsafe.Pointer(cZvfsId))

	int32PageSize, err := utils.SafeInt64ToInt32(pageSize)

	if err != nil {
		return nil, fmt.Errorf("invalid pageSize: %v", err)
	}

	// Register the VFS in C. The C side makes its own copy of the vfsId
	// and stores that pointer in the sqlite3_vfs.zName field. We must
	// retrieve that pointer after registration and use it as our map key
	// to match the pointer used later by sqlite when opening files.
	C.newVfs((*C.char)(cZvfsId), C.int(int32PageSize))

	// Find the registered sqlite3_vfs and read its zName pointer.
	pVfs := C.sqlite3_vfs_find((*C.char)(cZvfsId))

	if pVfs == nil {
		return nil, fmt.Errorf("failed to find registered VFS for id: %s", connectionHash)
	}

	vfsIdPtr := uintptr(unsafe.Pointer(pVfs.zName))

	if lVfs, ok := VfsMap[vfsIdPtr]; ok {
		return lVfs, nil
	}

	// Check if the WAL is already registered
	if VfsShmMap[nodeHash] == nil {
		VfsShmMap[nodeHash] = &ShmMemory{
			locks:    make(map[int]int),
			mutex:    &sync.Mutex{},
			regions:  make([]*ShmRegion, 0),
			nodeHash: nodeHash,
		}
	}

	l := &LitebaseVFS{
		connectionHash: connectionHash,
		connectionID:   connectionID,
		databaseHash:   databaseHash,
		fileSystem:     fileSystem,
		wal:            wal,
		nodeHash:       nodeHash,
		shm:            VfsShmMap[nodeHash],
	}

	l.VfsIdPtr = vfsIdPtr

	VfsMap[vfsIdPtr] = l

	return l, nil
}

// Remove a VFS instance from the registry by its ID.
func UnregisterVFS(vfsId string) error {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if vfsId == "" {
		return errors.New("vfsId cannot be empty")
	}

	// Convert the vfsId string to a C string and find the registered sqlite3_vfs
	cvfsId, err := utils.SafeCString(vfsId)
	if err != nil {
		return fmt.Errorf("failed to convert vfsId to C string: %v", err)
	}
	defer C.free(unsafe.Pointer(cvfsId))

	pVfs := C.sqlite3_vfs_find((*C.char)(cvfsId))

	if pVfs == nil {
		return errors.New("vfsId not found")
	}

	vfsIdPtr := uintptr(unsafe.Pointer(pVfs.zName))

	vfs, ok := VfsMap[vfsIdPtr]

	if !ok {
		return errors.New("vfsId not found")
	}

	if vfs == nil {
		delete(VfsMap, vfsIdPtr)
		return errors.New("vfs instance is nil")
	}

	// Unregister on the C side
	C.unregisterVfs((*C.char)(cvfsId))

	nodeHash := vfs.nodeHash

	delete(VfsMap, vfsIdPtr)

	var found bool
	for _, v := range VfsMap {
		if v.nodeHash == nodeHash {
			found = true
			break
		}
	}

	if !found && nodeHash != "" {
		delete(VfsShmMap, nodeHash)
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
func (vfs *LitebaseVFS) SetTimestamps(walTimestamp, transactionalTimestamp int64) {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	vfs.walTimestamp = walTimestamp
	vfs.transactionalTimestamp = transactionalTimestamp
}

func (vfs *LitebaseVFS) WALTimestamp() int64 {
	return vfs.walTimestamp
}

func getVfsFromFile(pFile *C.sqlite3_file) (*LitebaseVFS, error) {
	file := (*C.LitebaseVFSFile)(unsafe.Pointer(pFile))

	// Use the key stamped into the file struct by goXOpen — a direct map
	// read with a shared lock, no string comparison or fallback scan.
	vfsMapKey := uintptr(file.vfsMapKey)

	vfsMutex.RLock()
	vfs, ok := VfsMap[vfsMapKey]
	vfsMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("vfs not found")
	}

	return vfs, nil
}

//export goXOpen
func goXOpen(zVfs *C.sqlite3_vfs, zName *C.char, pFile *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {
	// Lookup by the sqlite3_vfs.zName pointer to avoid allocating a Go string
	vfsIdPtr := uintptr(unsafe.Pointer(zVfs.zName))
	name := C.GoString(zName)
	filename := name[strings.LastIndex(name, "/")+1:]

	vfsMutex.RLock()
	vfs, ok := VfsMap[vfsIdPtr]
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
		vfs.transactionalTimestamp,
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

	_, err = vfs.fileSystem.WriteAt(vfs.transactionalTimestamp, goBuffer, int64(iOfst))

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
		size:  uintptr(uint64Pgsz),
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
	start := int(offset)
	end := start + int(n)

	// Check for unlock
	if flags&C.SQLITE_SHM_UNLOCK != 0 {
		// Unlock logic (shared/unshared over entire range)
		if flags&C.SQLITE_SHM_SHARED != 0 {
			for i := start; i < end; i++ {
				if vfs.shm.locks[i] > 1 {
					vfs.shm.locks[i]--
				} else {
					vfs.shm.locks[i] = 0
				}
			}
		} else {
			for i := start; i < end; i++ {
				vfs.shm.locks[i] = 0
			}
		}
	} else if flags&C.SQLITE_SHM_SHARED != 0 {
		// Shared lock logic: ensure no exclusive lock in the whole range
		for i := start; i < end; i++ {
			if vfs.shm.locks[i] < 0 {
				rc = C.SQLITE_BUSY // Exclusive lock already held
				break
			}
		}
		if rc == C.SQLITE_OK {
			for i := start; i < end; i++ {
				vfs.shm.locks[i]++
			}
		}
	} else {
		// Exclusive lock logic: ensure entire range is free
		for i := start; i < end; i++ {
			if vfs.shm.locks[i] != 0 {
				rc = C.SQLITE_BUSY // Lock already held
				break
			}
		}

		if rc == C.SQLITE_OK {
			for i := start; i < end; i++ {
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
		if vfsEntry.connectionHash != vfs.connectionHash && vfsEntry.nodeHash == vfs.shm.nodeHash {
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

	size, err := vfs.wal.Size(vfs.walTimestamp)

	if err != nil {
		log.Println("Error getting WAL file size", err)
		return C.SQLITE_IOERR
	}

	*pSize = C.sqlite3_int64(size)

	return C.SQLITE_OK
}

//export goXWALRead
//go:nosplit
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

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.wal.ReadAt(vfs.walTimestamp, vfs.connectionID, goBuffer, int64(iOfst))

	if err != nil {
		if err == io.EOF {
			return C.SQLITE_OK
		}

		return C.SQLITE_IOERR
	}

	return C.SQLITE_OK
}

//export goXWALWrite
//go:nosplit
func goXWALWrite(pFile *C.sqlite3_file, iAmt C.int, iOfst C.sqlite3_int64, zBuf unsafe.Pointer) C.int {
	vfs, err := getVfsFromFile(pFile)

	if err != nil {
		log.Println("Error getting VFS from file", err)
		return C.SQLITE_IOERR
	}

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]

	_, err = vfs.wal.WriteAt(vfs.walTimestamp, vfs.connectionID, goBuffer, int64(iOfst))

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

	err = vfs.wal.Sync(vfs.walTimestamp)

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

	err = vfs.wal.Truncate(vfs.walTimestamp, int64(size))

	if err != nil {
		log.Println("Error truncating WAL file", err)
		return C.SQLITE_IOERR
	}

	return C.SQLITE_OK
}

// UpdateWALSharedMemory updates specific fields in the WAL index header for replicas
// This updates the bytes of the WAL INDEX header.
func UpdateWALSharedMemory(databaseHash string, senderNodeHash string, timestamp int64, headerBytes []byte) error {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if len(headerBytes) == 0 {
		log.Printf("UpdateWALSharedMemory: No header bytes provided")
		return nil
	}

	// Find all VFS instances for this database (excluding the sender)
	for _, vfs := range VfsMap {
		if vfs.databaseHash != databaseHash {
			continue
		}

		if vfs.nodeHash == senderNodeHash {
			continue
		}

		for _, shmMemory := range VfsShmMap {
			// Check if this shared memory belongs to the same database
			shmMemory.mutex.Lock()

			// Update WAL index header (page 0) with current WAL state
			if len(shmMemory.regions) > 0 && shmMemory.regions[0].id == 0 {
				if shmMemory.regions[0].pData != nil {
					// Ensure we don't copy more than the available space
					headerSize := min(len(headerBytes), int(shmMemory.regions[0].size))

					// Debug: Log some key WAL header fields before copying
					// if len(headerBytes) >= 20 {
					// 	mxFrame := binary.LittleEndian.Uint32(headerBytes[16:20])
					// 	iChange := binary.LittleEndian.Uint32(headerBytes[8:12])
					// 	log.Printf("UpdateWALSharedMemory: Header data - mxFrame: %d, iChange: %d", mxFrame, iChange)
					// }

					target := (*[1 << 16]byte)(shmMemory.regions[0].pData)[:headerSize:headerSize]

					copy(target, headerBytes[:headerSize])

					// Debug: Verify the copy worked by reading back some key fields
					// if headerSize >= 20 {
					// 	copiedMxFrame := binary.LittleEndian.Uint32(target[16:20])
					// 	copiedIChange := binary.LittleEndian.Uint32(target[8:12])
					// 	log.Printf("UpdateWALSharedMemory: After copy - mxFrame: %d, iChange: %d", copiedMxFrame, copiedIChange)
					// }
				}
			}

			shmMemory.mutex.Unlock()
		}
	}

	return nil
}

// Mark the WAL as updated.
func (vfs *LitebaseVFS) WALUpdated() {
	// Notify the WAL about the write so it can broadcast to replicas
	vfs.wal.OnWALUpdate(
		vfs.walTimestamp,
		vfs.shm.GetWALIndexHeader(),
	)
}
