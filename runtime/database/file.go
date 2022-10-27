package database

// #include <../../dist/sqlite3.h>
import "C"

import (
	"fmt"
	"os"
	"time"

	"github.com/psanford/sqlite3vfs"
)

type File struct {
	closed     bool
	connection *Connection
	locked     bool
	name       string
	path       string
	pointer    *os.File
	size       int64
}

func (file *File) Close() error {
	file.closed = true
	delete(VFSFiles, file.name)
	err := file.pointer.Close()

	return err
}

// ReadAt reads len(p) bytes into p starting at offset off in the underlying input source.
// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
// If n < len(p), SQLITE_IOERR_SHORT_READ will be returned to sqlite.
func (file *File) ReadAt(data []byte, offset int64) (n int, err error) {
	// start := time.Now()
	size := len(data)

	// TODO: Read from the WAL instead of the main file if the read occurs within
	// an explicit transaction that has not been committed.
	// if Operator.InTransaction() && WAL.HasPage(offset) {
	// 	return WAL.ReadAt(data, offset)
	// }

	n, e := file.pointer.ReadAt(data, offset)

	// Capture the database header as it is read
	if e == nil && offset == 0 && size == 100 && !file.connection.WAL.CheckPointing() {
		file.connection.WAL.SetHeaderHash(data[:100])
	}

	// fmt.Println("ReadAt", file.name, offset, len(data), time.Since(start))

	return n, e
}

// WriteAt writes len(p) bytes from p to the underlying data stream at offset off.
// It returns the number of bytes written from p (0 <= n <= len(p)) and any error encountered that caused the write to stop early.
// WriteAt must return a non-nil error if it returns n < len(p).
func (file *File) WriteAt(data []byte, offset int64) (int, error) {
	// start := time.Now()
	var bytesWritten int
	var err error

	if file.connection.Operator.Transmitting() {
		bytesWritten, err = file.pointer.WriteAt(data, offset)
	} else {
		bytesWritten, err = file.connection.WAL.WriteAt(data, offset)
	}

	// fmt.Println("WriteAt", file.name, len(data), time.Since(start))

	return bytesWritten, err
}

func (file *File) Truncate(size int64) error {
	return file.pointer.Truncate(size)
}

func (file *File) Sync(flag sqlite3vfs.SyncType) error {
	start := time.Now()
	file.pointer.Sync()
	fmt.Println("Sync", file.name, time.Since(start))

	return nil
}

func (file *File) FileSize() (int64, error) {
	// This is a no-op for now. We don't need to track the file size as we are
	// not using rollback journaling. This value represents the max size of the
	// database file, considering the default page size and  max number number
	// of pages allowed in the databse.

	// We need to return 0 here when creating a new database file. Otherwise
	// sqlite will try to read the file size and fail.
	if file.connection.WAL.HeaderEmpty() {
		return 0 * 4096, nil
	}

	return 4294967294 * 4096, nil
}

// Acquire or upgrade a lock.
// elock can be one of the following:
// LockShared, LockReserved, LockPending, LockExclusive.
//
// Additional states can be inserted between the current lock level
// and the requested lock level. The locking might fail on one of the later
// transitions leaving the lock state different from what it started but
// still short of its goal.  The following chart shows the allowed
// transitions and the inserted intermediate states:
//
//	UNLOCKED -> SHARED
//	SHARED -> RESERVED
//	SHARED -> (PENDING) -> EXCLUSIVE
//	RESERVED -> (PENDING) -> EXCLUSIVE
//	PENDING -> EXCLUSIVE
//
// This function should only increase a lock level.
// See the sqlite source documentation for unixLock for more details.
func (file *File) Lock(elock sqlite3vfs.LockType) error {
	// fmt.Println("LOCK", file.name, elock)
	// if elock == sqlite3vfs.LockExclusive {
	// 	start := time.Now()
	// 	file.locked = true
	// 	syscall.Flock(int(file.pointer.Fd()), syscall.LOCK_EX)
	// 	fmt.Println("LOCK: ", file.name, time.Since(start))
	// 	return nil
	// }

	return nil
}

// Lower the locking level on file to eFileLock. eFileLock must be
// either NO_LOCK or SHARED_LOCK. If the locking level of the file
// descriptor is already at or below the requested locking level,
// this routine is a no-op.
func (file *File) Unlock(elock sqlite3vfs.LockType) error {
	// fmt.Println("Unlock", file.name, elock)

	// if file.locked {
	// 	start := time.Now()
	// 	file.locked = false
	// 	syscall.Flock(int(file.pointer.Fd()), syscall.LOCK_UN)
	// 	fmt.Println("UNLOCK: ", file.name, time.Since(start))

	// }

	return nil
}

// Check whether any database connection, either in this process or
// in some other process, is holding a RESERVED, PENDING, or
// EXCLUSIVE lock on the file. It returns true if such a lock exists
// and false otherwise.
func (file *File) CheckReservedLock() (bool, error) {
	return false, nil
}

// SectorSize returns the sector size of the device that underlies
// the file. The sector size is the minimum write that can be
// performed without disturbing other bytes in the file.
func (file *File) SectorSize() int64 {
	return 0
}

// DeviceCharacteristics returns a bit vector describing behaviors
// of the underlying device.
func (file *File) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapAtomic64K | sqlite3vfs.IocapSafeAppend | sqlite3vfs.IocapSequential
}
