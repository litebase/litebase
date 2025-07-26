package storage

import (
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"sync"
)

type TieredFileDescriptor struct {
	// Track if this descriptor has been closed
	closed bool

	// File is the underlying TieredFile that this descriptor operates on.
	File *TieredFile

	// Flags that defined the file permissions used for access. This value will
	// be used to determine if the File should be written to durable storage.
	Flag int

	// Unique identifier for this descriptor
	id string

	// Key is the unique identifier for this file in the tiered storage system.
	Key string

	// Mutex to protect descriptor-specific state
	mutex sync.Mutex

	// Position tracks the current file position to preserve it across file reopens
	position int64
}

// Create a new TieredFileDescriptor for a given TieredFile
func NewTieredFileDescriptor(file *TieredFile, key string, flag int, id string) *TieredFileDescriptor {
	descriptor := &TieredFileDescriptor{
		closed:   false,
		File:     file,
		Flag:     flag,
		id:       id,
		Key:      key,
		position: 0,
	}

	// Register this descriptor with the TieredFile
	file.AddDescriptor(descriptor)

	return descriptor
}

// Close releases the descriptor and notifies the TieredFile
func (tfd *TieredFileDescriptor) Close() error {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return nil
	}

	tfd.closed = true

	// Notify the TieredFile that this descriptor is closing
	return tfd.File.CloseDescriptor(tfd)
}

// Checks if the underlying file was released and reopens it if needed
func (tfd *TieredFileDescriptor) ensureFileAvailable() error {
	// Lock the TieredFile's mutex to ensure thread-safe reopening
	tfd.File.mutex.Lock()
	defer tfd.File.mutex.Unlock()

	if tfd.File.Released {
		// File was released from memory, need to reopen it
		slog.Debug("Reopening released file", "file", tfd.File.Key)

		// Use the TieredFile's own reopenFile method which handles position restoration
		err := tfd.File.reopenFile()

		if err != nil {
			return fmt.Errorf("failed to reopen file %s: %w", tfd.File.Key, err)
		}

		tfd.File.Released = false
	}

	return nil
}

// ID returns the unique identifier for this descriptor
func (tfd *TieredFileDescriptor) ID() string {
	return tfd.id
}

// Read data from the file starting at the current position
func (tfd *TieredFileDescriptor) Read(p []byte) (n int, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	if err := tfd.ensureFileAvailable(); err != nil {
		return 0, err
	}

	// Use ReadAt to avoid position conflicts
	n, err = tfd.File.ReadAt(p, tfd.position)

	if err == nil {
		tfd.position += int64(n)
	}

	return n, err
}

// Read data from the file at a specific offset
func (tfd *TieredFileDescriptor) ReadAt(p []byte, off int64) (n int, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	if err := tfd.ensureFileAvailable(); err != nil {
		return 0, err
	}

	return tfd.File.ReadAt(p, off)
}

// Move the file position to a new location
func (tfd *TieredFileDescriptor) Seek(offset int64, whence int) (int64, error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = tfd.position + offset
	case io.SeekEnd:
		// Get file size for SeekEnd
		if err := tfd.ensureFileAvailable(); err != nil {
			return 0, err
		}

		stat, err := tfd.File.Stat()

		if err != nil {
			return 0, err
		}

		newPos = stat.Size() + offset
	default:
		return 0, fs.ErrInvalid
	}

	if newPos < 0 {
		return 0, fs.ErrInvalid
	}

	tfd.position = newPos
	return newPos, nil
}

// Return the file information for this descriptor
func (tfd *TieredFileDescriptor) Stat() (fs.FileInfo, error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return nil, fs.ErrClosed
	}

	if err := tfd.ensureFileAvailable(); err != nil {
		return nil, err
	}

	return tfd.File.Stat()
}

// Sync flushes the file contents to storage
func (tfd *TieredFileDescriptor) Sync() error {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return fs.ErrClosed
	}

	return tfd.File.Sync()
}

// Truncate the file to a specific size
func (tfd *TieredFileDescriptor) Truncate(size int64) error {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return fs.ErrClosed
	}

	if tfd.Flag&os.O_RDONLY != 0 {
		return fs.ErrInvalid
	}

	err := tfd.File.Truncate(size)

	if err == nil && tfd.position > size {
		tfd.position = size
	}

	return err
}

// Write data to the file starting at the current position
func (tfd *TieredFileDescriptor) Write(p []byte) (n int, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	if tfd.Flag&os.O_RDONLY != 0 {
		return 0, fs.ErrInvalid
	}

	if tfd.Flag&os.O_WRONLY == 0 && tfd.Flag&os.O_RDWR == 0 {
		return 0, fs.ErrInvalid
	}

	if err := tfd.ensureFileAvailable(); err != nil {
		return 0, err
	}

	// Use WriteAt to avoid position conflicts
	n, err = tfd.File.WriteAt(p, tfd.position)

	if err == nil {
		tfd.position += int64(n)
	}

	return n, err
}

// Write data to the file at a specific offset
func (tfd *TieredFileDescriptor) WriteAt(p []byte, off int64) (n int, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	if tfd.Flag&os.O_RDONLY != 0 {
		return 0, fs.ErrInvalid
	}

	if err := tfd.ensureFileAvailable(); err != nil {
		return 0, err
	}

	n, err = tfd.File.WriteAt(p, off)

	// Update our position if this write extends beyond current position
	if err == nil && off+int64(n) > tfd.position {
		tfd.position = off + int64(n)
	}

	return n, err
}

func (tfd *TieredFileDescriptor) WriteTo(w io.Writer) (n int64, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	// Read from current position to end and write to w
	for {
		buf := make([]byte, 32*1024)
		readN, readErr := tfd.File.ReadAt(buf, tfd.position)

		if readN > 0 {
			writeN, writeErr := w.Write(buf[:readN])
			n += int64(writeN)
			tfd.position += int64(readN)

			if writeErr != nil {
				return n, writeErr
			}
		}

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return n, readErr
		}
	}

	return n, err
}

// Write a string to the file starting at the current position
func (tfd *TieredFileDescriptor) WriteString(s string) (ret int, err error) {
	tfd.mutex.Lock()
	defer tfd.mutex.Unlock()

	if tfd.closed {
		return 0, fs.ErrClosed
	}

	if tfd.Flag&os.O_RDONLY != 0 {
		return 0, fs.ErrInvalid
	}

	if tfd.Flag&os.O_WRONLY == 0 && tfd.Flag&os.O_RDWR == 0 {
		return 0, fs.ErrInvalid
	}

	// Use WriteAt to avoid position conflicts
	ret, err = tfd.File.WriteAt([]byte(s), tfd.position)

	if err == nil {
		tfd.position += int64(ret)
	}

	return ret, err
}
