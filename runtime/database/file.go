package database

import (
	"litebasedb/runtime/sqlite3_vfs"
	"os"
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

func (file *File) ReadAt(data []byte, offset int64) (n int, err error) {
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

	return n, e
}

func (file *File) WriteAt(data []byte, offset int64) (int, error) {
	var bytesWritten int
	var err error

	if file.connection.Operator.Transmitting() {
		bytesWritten, err = file.pointer.WriteAt(data, offset)
	} else {
		bytesWritten, err = file.connection.WAL.WriteAt(data, offset)
	}

	return bytesWritten, err
}

func (file *File) Truncate(size int64) error {
	return file.pointer.Truncate(size)
}

func (file *File) Sync(flag sqlite3_vfs.SyncType) error {

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

func (file *File) Lock(elock sqlite3_vfs.LockType) error {
	return nil
}

func (file *File) Unlock(elock sqlite3_vfs.LockType) error {
	return nil
}

func (file *File) CheckReservedLock() (bool, error) {
	return false, nil
}

func (file *File) SectorSize() int64 {
	return 0
}

func (file *File) DeviceCharacteristics() sqlite3_vfs.DeviceCharacteristic {
	return sqlite3_vfs.IocapAtomic64K | sqlite3_vfs.IocapSafeAppend | sqlite3_vfs.IocapSequential
}
