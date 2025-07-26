package storage

import (
	"container/list"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// The TieredFile stores a reference of a single File that is stored durably in
// two different locations for the purpose of latency and cost. The File is
// stored on a shared file system and eventually stored durably to
// another file system, typically object storage.
type TieredFile struct {
	// Closed is a boolean value that determines if the File has been Closed
	// by local storage. If the File has been Closed, the File will be marked
	// for release, which means the File will be removed from local storage
	// and the TieredFileSystemDriver will no longer be able to access it.
	Closed bool

	// CreatedAt stores the time the TieredFile struct was created. This
	// value will be used in correlation with the updatedAt and writtenAt
	// values to determine how long the File has been open.
	CreatedAt time.Time

	// Map to track open descriptors
	descriptors map[string]*TieredFileDescriptor

	// Mutex to protect the descriptors map
	descriptorsMutex sync.Mutex

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

	// The tiered files system log that was used to capture file modifications.
	LogKey int64

	// Mutex is a pointer to a sync.Mutex that is used to lock the file when
	// reading or writing to the file. This is used to prevent multiple
	// concurrent operations from occurring at the same time.
	mutex *sync.Mutex

	// Position tracks the current file position to preserve it across file reopens
	position int64

	// Released indicates that the file was released from memory for resource management.
	// Descriptors can still be "open" but will need to reopen the file when accessed.
	Released bool

	// Mutex that needs to be checked when flushing the file to durable storage
	// to prevent multiple goroutines from flushing the file at the same time.
	syncMutex *sync.Mutex

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
		CreatedAt:              time.Now().UTC(),
		File:                   file,
		Flag:                   flag,
		Key:                    key,
		mutex:                  &sync.Mutex{},
		position:               0,
		syncMutex:              &sync.Mutex{},
		TieredFileSystemDriver: tieredFileSystemDriver,
		UpdatedAt:              time.Time{},
		WrittenAt:              time.Time{},
		descriptors:            make(map[string]*TieredFileDescriptor),
	}
}

// Get access to the tiered file with the sync mutex locked. This is useful
// for performing multiple operations on the file without having to lock and
// unlock the mutex each time or worry about the file being modified during flush.
func (f *TieredFile) AccessBarrier(fn func() error) error {
	f.syncMutex.Lock()
	defer f.syncMutex.Unlock()

	return fn()
}

// AddDescriptor registers a new descriptor with this file
func (f *TieredFile) AddDescriptor(descriptor *TieredFileDescriptor) {
	f.descriptorsMutex.Lock()
	defer f.descriptorsMutex.Unlock()

	f.descriptors[descriptor.ID()] = descriptor
}

// Close the file and release it from the TieredFileSystemDriver.
// Note: This method is now primarily used internally. External callers
// should close TieredFileDescriptor instances instead.
func (f *TieredFile) Close() error {
	// Check if there are still open descriptors
	f.descriptorsMutex.Lock()
	hasDescriptors := len(f.descriptors) > 0
	f.descriptorsMutex.Unlock()

	if hasDescriptors {
		// Don't actually close if there are still descriptors open
		return nil
	}

	// Always use the driver to release the file with proper locking
	err := f.TieredFileSystemDriver.ReleaseFileWithLock(f)

	if err != nil {
		slog.Error("Error releasing file", "error", err)
	}

	return err
}

// CloseDescriptor is called when a descriptor is closed
func (f *TieredFile) CloseDescriptor(descriptor *TieredFileDescriptor) error {
	var shouldRemoveFromMap bool

	f.descriptorsMutex.Lock()
	// Remove the descriptor from our tracking
	delete(f.descriptors, descriptor.ID())

	// If no more descriptors are using this file, we can potentially release it
	// Note: We don't check shouldBeWrittenToDurableStorage() here because flushing
	// should happen independently from descriptor lifecycle. The background flush
	// process will handle writing files to durable storage even if they have
	// open descriptors, and release will only happen when both conditions are met:
	// 1. No open descriptors AND 2. No need to flush
	if len(f.descriptors) == 0 {
		shouldRemoveFromMap = true
	}

	f.descriptorsMutex.Unlock()

	// Handle file release outside of the descriptors mutex to avoid deadlock
	if shouldRemoveFromMap {
		// Try to release the file, but if it can't be released (e.g., needs flushing),
		// that's OK - just leave it in the driver for later flushing
		err := f.TieredFileSystemDriver.ReleaseFileWithLock(f) // We know descriptor count is 0

		if err != nil && err != ErrTieredFileCannotBeReleased {
			// Log non-expected errors, but don't fail the close operation
			slog.Warn("Unexpected error trying to release file after descriptor close", "error", err, "file", f.Key)
		} else if err == ErrTieredFileCannotBeReleased {
			slog.Debug("File cannot be released yet, keeping for later flush", "file", f.Key)
		} else {
			// File was successfully released, now remove it from the driver's map
			// This needs to be done while holding the driver's mutex to avoid concurrent map access
			f.TieredFileSystemDriver.ReleaseFileFromMap(f)
		}
	}

	// Always return nil - closing a descriptor should not fail even if file can't be released yet
	return nil
}

// Close the file without locking the mutex.
func (f *TieredFile) closeFile() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.Closed = true

	if f.File == nil {
		return nil
	}

	return f.File.Close()
}

// Return the number of open descriptors
func (f *TieredFile) GetDescriptorCount() int {
	f.descriptorsMutex.Lock()
	defer f.descriptorsMutex.Unlock()

	return len(f.descriptors)
}

// HasOpenDescriptors returns true if there are any open descriptors
func (f *TieredFile) HasOpenDescriptors() bool {
	f.descriptorsMutex.Lock()
	defer f.descriptorsMutex.Unlock()

	return len(f.descriptors) > 0
}

// Indicate that the file has been updated so that they TieredFileSystemDriver
// knows to write the file to durable storage.
func (f *TieredFile) MarkUpdated() {
	if f.Closed {
		return
	}

	f.UpdatedAt = time.Now().UTC()

	err := f.TieredFileSystemDriver.MarkFileUpdated(f)

	if err != nil {
		slog.Error("Error marking file as updated", "error", err)
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

	if f.File == nil || f.Released {
		err := f.reopenFile()

		if err != nil {
			return 0, err
		}
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	n, err = f.File.Read(b)

	if err == nil {
		f.position += int64(n)
	}

	return n, err
}

// ReadAt reads up to len(p) bytes from the File starting at offset off.
// It returns the number of bytes read and any error encountered.
func (f *TieredFile) ReadAt(p []byte, off int64) (n int, err error) {
	err = f.AccessBarrier(func() error {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		if f.File == nil || f.Released {
			err := f.reopenFile()
			if err != nil {
				return err
			}
		}

		f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

		n, err = f.File.ReadAt(p, off)

		// Update position only if this read extends beyond current position
		if off+int64(n) > f.position {
			f.position = off + int64(n)
		}

		return err
	})

	return n, err
}

// Seek sets the offset for the next Read or Write on the File to offset,
// interpreted according to whence: 0 means relative to the origin of
// the File, 1 means relative to the current offset, 2 means relative
// to the end. Seek returns the new offset and an error, if any.
func (f *TieredFile) Seek(offset int64, whence int) (n int64, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.File == nil || f.Released {
		err := f.reopenFile()
		if err != nil {
			return 0, err
		}
	}

	n, err = f.File.Seek(offset, whence)

	if err == nil {
		f.position = n
	}

	return n, err
}

// This operation checks if the File should be written to durable storage. The File
// should be written to durable storage if it has been updated and the last write
// to durable storage was more than a minute ago.
func (f *TieredFile) shouldBeWrittenToDurableStorage() bool {
	if f.UpdatedAt.IsZero() {
		return false
	}

	return f.UpdatedAt.After(f.WrittenAt) &&
		(time.Since(f.WrittenAt) >= f.TieredFileSystemDriver.WriteInterval)
}

// Stat returns the FileInfo structure describing the File. If the File is
// nil, the File will be opened and the FileInfo structure will be returned.
func (f *TieredFile) Stat() (fs.FileInfo, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.File == nil {
		err := f.reopenFile()

		if err != nil {
			return nil, err
		}
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.Stat()
}

// Sync flushes the File's contents to the underlying storage. It is important
// to note that this does not guarantee that the File is written to the
// durable file system. However, the File will still be synced to the
// shared file system and eventually synced to the durable
// file system.
func (f *TieredFile) Sync() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.File == nil {
		err := f.reopenFile()
		if err != nil {
			return err
		}
	}

	err := f.File.Sync()

	if err != nil {
		return err
	}

	// Sync the position after file sync to ensure consistency
	if f.File != nil {
		currentPos, seekErr := f.File.Seek(0, io.SeekCurrent)

		if seekErr == nil {
			f.position = currentPos
		}
	}

	f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	f.UpdatedAt = time.Now().UTC()

	return nil
}

// Truncate changes the size of the File to the specified size. If the
// File is larger than the specified size, the File will be truncated to
// the specified size.
func (f *TieredFile) Truncate(size int64) error {
	err := f.AccessBarrier(func() error {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		if f.File == nil {
			err := f.reopenFile()

			if err != nil {
				return err
			}
		}

		err := f.File.Truncate(size)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		slog.Error("Error truncating file", "error", err)

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
// written to the shared file system and eventually written to
// the durable file system.
//
// It is important that consumers of this function Seek to the appropriate place
// in the file before calling Write. The File is not guaranteed to be
// positioned at any particular location before the write occurs.
// Caution should be taken to avoid data corruption.
func (f *TieredFile) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.File == nil {
		err := f.reopenFile()

		if err != nil {
			return 0, err
		}
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
		f.position += int64(n)
	}

	return n, err
}

// WriteAt writes len(p) bytes from p to the File at offset off. It returns
// the number of bytes written and any error encountered.
func (f *TieredFile) WriteAt(p []byte, off int64) (n int, err error) {
	err = f.AccessBarrier(func() error {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		if f.File == nil {
			err := f.reopenFile()

			if err != nil {
				return err
			}
		}

		n, err = f.File.WriteAt(p, off)

		if err == nil {
			f.MarkUpdated()

			if off+int64(n) > f.position {
				f.position = off + int64(n)
			}
		}

		return err
	})

	return n, err
}

// WriteTo writes the contents of the File to w. It returns the number of
// bytes written and any error encountered.
func (f *TieredFile) WriteTo(w io.Writer) (n int64, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.File == nil {
		err := f.reopenFile()

		if err != nil {
			return 0, err
		}
	}

	defer f.TieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	n, err = f.File.WriteTo(w)

	if err == nil {
		// WriteTo reads from current position to EOF, so update position accordingly
		f.position += n
	}

	return n, err
}

// WriteString writes the string s to the File. It returns the number of
// bytes written and any error encountered.
func (f *TieredFile) WriteString(s string) (n int, err error) {
	err = f.AccessBarrier(func() error {
		f.mutex.Lock()
		defer f.mutex.Unlock()

		if f.File == nil {
			err := f.reopenFile()
			if err != nil {
				return err
			}
		}

		n, err = f.File.WriteString(s)

		if err == nil {
			f.MarkUpdated()
			f.position += int64(n)
		}

		return err
	})

	return n, err
}

// reopenFile reopens the file directly from the high tier file system
// without going through the TieredFileSystemDriver to avoid recursion
func (f *TieredFile) reopenFile() error {
	if f.File != nil {
		return nil
	}

	// Try to open the file directly from the high tier file system first
	// Always open with RDWR since descriptors handle access control
	file, err := f.TieredFileSystemDriver.highTierFileSystemDriver.OpenFile(f.Key, os.O_RDWR, 0600)
	if err != nil {
		// If file doesn't exist on high tier, try to copy it from low tier
		if os.IsNotExist(err) {
			// Read from low tier storage
			lowTierFile, err := f.TieredFileSystemDriver.lowTierFileSystemDriver.OpenFile(f.Key, os.O_RDONLY, 0600)

			if err != nil {
				return err
			}

			defer lowTierFile.Close()

			// Create on high tier storage
			file, err = f.TieredFileSystemDriver.highTierFileSystemDriver.OpenFile(f.Key, os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				return err
			}

			// Copy data from low tier to high tier
			_, err = f.TieredFileSystemDriver.CopyFile(file, lowTierFile)

			if err != nil {
				file.Close()

				return err
			}
		} else {
			return err
		}
	}

	f.File = file
	f.Closed = false

	// Restore the file position
	if f.position > 0 {
		actualPos, err := f.File.Seek(f.position, io.SeekStart)

		if err != nil {
			f.File.Close()

			return err
		}

		// Verify the position was set correctly
		if actualPos != f.position {
			f.File.Close()

			return fmt.Errorf("failed to restore file position: expected %d, got %d", f.position, actualPos)
		}
	}

	return nil
}
