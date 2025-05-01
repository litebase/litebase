package storage

import (
	"container/list"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// The TieredFile stores a reference of a single File that is stored durably in
// two different locations for the purpose of latency and cost. The File is
// stored on a distributed file system and eventually stored durably to
// another file system, typically object storage.
type TieredFile struct {

	// Closed is a boolean value that determines if the File has been Closed
	// by local storage. If the File has been Closed, the File will be marked
	// for release, which means the File will be removed from local storage
	// and the TieredFileSystemDriver will no longer be able to access it.
	Closed bool

	// CreatedAt stores the time the TieredFile struct was creeated. This
	// value will be used in correlation with the updatedAt and writtenAt
	// values to determine how long the File has been open.
	CreatedAt time.Time

	// Element is a pointer to the list.Element that is used to store the File
	// in the LRU cache. The Element is used to determine the position of the
	// File in the LRU cache and to remove the File from the LRU cache.
	Element *list.Element

	// File is the internalStorage.File object that is used to read and write
	// data to the File. The File is a instance of *os.File which points to a
	// local File. If the File is nil, the File has not been opened yet.
	File internalStorage.File

	// Flags that defined the file permissions used for access. This value will
	// be used to determine if the File should be written to durable storage.
	Flag int

	// Used to identify the File in the durable storage and local storage.
	Key string

	// Mutex is a pointer to a sync.Mutex that is used to lock the file when
	// reading or writing to the file. This is used to prevent multiple
	// concurrent operations from occurring at the same time.
	mutex *sync.Mutex

	// The pointer to the FileSystemDriver that created the File.
	TieredFileSystemDriver *TieredFileSystemDriver

	// UpdatedAt stores the time the File was last updated. This value will be
	// used in correlation with the CreatedAt and writtenAt values to determine
	// how long the File has been open and if the File should be written to
	// durable storage.
	UpdatedAt time.Time

	// WrittenAt stores the time the File was last written to durable storage.
	// This value will be used in correlation with the CreatedAt and updatedAt
	// values to determine how long the File has been open and if the File
	// should be written to durable storage.
	WrittenAt time.Time
}

// Create a new instance of a TieredFile.
func NewTieredFile(
	tieredFileSystemDriver *TieredFileSystemDriver,
	key string,
	file internalStorage.File,
	flag int,
) *TieredFile {
	return &TieredFile{
		CreatedAt:              time.Now(),
		File:                   file,
		Flag:                   flag,
		Key:                    key,
		mutex:                  &sync.Mutex{},
		TieredFileSystemDriver: tieredFileSystemDriver,
		UpdatedAt:              time.Time{},
		WrittenAt:              time.Time{},
	}
}

// Close the file and release it from the TieredFileSystemDriver.
func (f *TieredFile) Close() error {
	if f.Closed {
		return nil
	}

	f.Closed = true

	f.TieredFileSystemDriver.WithLock(func() {
		if f.shouldBeWrittenToDurableStorage() {
			f.TieredFileSystemDriver.flushFileToDurableStorage(f, true)
		}

		f.TieredFileSystemDriver.ReleaseFile(f)
	})

	return nil
}

// Close the file without locking the mutex.
func (f *TieredFile) closeFile() error {
	f.Closed = true

	if f.File == nil {
		return nil
	}

	return f.File.Close()
}

// Indicate that the file has been updated so that they TieredFileSystemDriver
// knows to write the file to durable storage.
func (f *TieredFile) MarkUpdated() {
	if f.Closed {
		return
	}

	f.UpdatedAt = time.Now()

	if f.Element != nil {
		f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)
	}
}

// Read reads up to len(b) bytes from the File and stores them in b.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
//
// It is important that consumers of this function Seek to the appropriate place
// in the file before calling Read. This is because the File is not
// automatically seeked to the beginning of the file when it is opened and the
// its position may be moved by another goroutine.
func (f *TieredFile) Read(b []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.Read(b)
}

// ReadAt reads up to len(p) bytes from the File starting at offset off.
// It returns the number of bytes read and any error encountered.
func (f *TieredFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.ReadAt(p, off)
}

// Seek sets the offset for the next Read or Write on the File to offset,
// interpreted according to whence: 0 means relative to the origin of
// the File, 1 means relative to the current offset, 2 means relative
// to the end. Seek returns the new offset and an error, if any.
func (f *TieredFile) Seek(offset int64, whence int) (n int64, err error) {
	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	return f.File.Seek(offset, whence)
}

// This operation checks if the File should be written to durable storage. The File
// should be written to durable storage if it has been updated and the last write
// to durable storage was more than a minute ago.
func (f *TieredFile) shouldBeWrittenToDurableStorage() bool {
	return f.UpdatedAt.After(f.WrittenAt) &&
		(time.Since(f.WrittenAt) >= f.TieredFileSystemDriver.WriteInterval)
}

// Stat returns the FileInfo structure describing the File. If the File is
// nil, the File will be opened and the FileInfo structure will be returned.
func (f *TieredFile) Stat() (fs.FileInfo, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return nil, err
		}

		f.File = file
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.Stat()
}

// Sync flushes the File's contents to the underlying storage. It is important
// to note that this does not guarantee that the File is written to the
// durable file system. However, the File will still be synced to the
// distributed file system and eventually synced to the durable
// file system.
func (f *TieredFile) Sync() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return err
		}

		f.File = file
	}

	err := f.File.Sync()

	if err != nil {
		return err
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	f.UpdatedAt = time.Now()

	return nil
}

// Truncate changes the size of the File to the specified size. If the
// File is larger than the specified size, the File will be truncated to
// the specified size.
func (f *TieredFile) Truncate(size int64) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return err
		}

		f.File = file
	}

	err := f.File.Truncate(size)

	if err != nil {
		return err
	}

	f.MarkUpdated()

	return nil
}

// Write writes len(p) bytes from p to the File. It returns the number of
// bytes written and any error encountered. If the File is nil, the
// File will be opened and the bytes will be written to the File. If
// the File is not opened in write mode, an error will be returned.
//
// It is important to note that this does not guarantee that the File is
// written to the durable file system. However, the File will still be
// written to the distributed file system and eventually written to
// the durable file system.
//
// It is important that consumers of this function Seek to the appropriate place
// in the file before calling Write. The File is not guaranteed to be
// positioned at any particular location before the write occurs.
// Caution should be taken to avoid data corruption.
func (f *TieredFile) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	if f.Flag&os.O_RDONLY != 0 {
		return 0, fs.ErrInvalid
	}

	if f.Flag&os.O_WRONLY == 0 && f.Flag&os.O_RDWR == 0 {
		return 0, fs.ErrInvalid
	}

	n, err = f.File.Write(p)

	if err == nil {
		f.MarkUpdated()
	}

	return n, err
}

// WriteAt writes len(p) bytes from p to the File at offset off. It returns
// the number of bytes written and any error encountered.
func (f *TieredFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	n, err = f.File.WriteAt(p, off)

	if err == nil {
		f.MarkUpdated()
	}

	return n, err
}

// WriteTo writes the contents of the File to w. It returns the number of
// bytes written and any error encountered.
func (f *TieredFile) WriteTo(w io.Writer) (n int64, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	defer f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.WriteTo(w)

}

// WriteString writes the string s to the File. It returns the number of
// bytes written and any error encountered.
func (f *TieredFile) WriteString(s string) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.Closed || f.File == nil {
		file, err := f.TieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		f.File = file
	}

	n, err = f.File.WriteString(s)

	if err == nil {
		f.MarkUpdated()
	}

	return n, err
}
