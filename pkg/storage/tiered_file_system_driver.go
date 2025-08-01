package storage

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// ReleaseFileWithLock acquires the mutex and calls releaseFile. Use this for any external calls.
func (fsd *TieredFileSystemDriver) ReleaseFileWithLock(file *TieredFile) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()
	return fsd.releaseFile(file)
}

const (
	DefaultWriteInterval         = 10 * time.Second
	TieredFileTTL                = 1 * time.Hour
	TieredFileSystemMaxOpenFiles = 10000
)

var (
	ErrTieredFileCannotBeReleased = errors.New("tiered file cannot be released")
	ErrNoTieredFilesToRemove      = errors.New("no tiered files to remove")
)

// Data in this driver is stored on disk in a high tier then eventually pushed
// up down to a low tier. The high tier is typically a local file system that
// transiently stores files while the low tier durably stores files with
// storage that have S3 compatibility. This provides fast read access
// performance with scalable and cost-effective long-term storage.
type TieredFileSystemDriver struct {
	buffers                  sync.Pool
	CanSyncDirtyFiles        func() bool // Function to check if dirty files can be synced
	context                  context.Context
	logger                   *TieredFileSystemLogger
	lowTierFileSystemDriver  FileSystemDriver
	FileOrder                *list.List
	Files                    map[string]*TieredFile
	highTierFileSystemDriver FileSystemDriver
	MaxFilesOpened           int
	mutex                    *sync.Mutex
	releasingOldestFile      atomic.Bool
	shuttingDown             bool
	WriteInterval            time.Duration
	watchTicker              *time.Ticker
}

type TieredFileSystemNewFunc func(context.Context, *TieredFileSystemDriver)

// Generate a unique ID for a file descriptor
func generateDescriptorID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)

	return hex.EncodeToString(bytes)
}

// Create a new instance of a tiered file system driver. This driver will manage
// files that are stored on the high and low tier file system.
func NewTieredFileSystemDriver(
	context context.Context,
	highTierFileSystemDriver *FileSystem,
	lowTierFileSystemDriver *FileSystem,
	f ...TieredFileSystemNewFunc,
) *TieredFileSystemDriver {
	fsd := &TieredFileSystemDriver{
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
		context:                  context,
		FileOrder:                list.New(),
		Files:                    map[string]*TieredFile{},
		highTierFileSystemDriver: highTierFileSystemDriver,
		lowTierFileSystemDriver:  lowTierFileSystemDriver,
		logger:                   nil,
		MaxFilesOpened:           TieredFileSystemMaxOpenFiles,
		mutex:                    &sync.Mutex{},
		WriteInterval:            DefaultWriteInterval,
	}

	if len(f) > 0 {
		for _, fn := range f {
			fn(context, fsd)
		}
	}

	var fileSystemDriver FileSystemDriver

	if _, ok := highTierFileSystemDriver.Driver().(*LocalFileSystemDriver); ok {
		fileSystemDriver = NewLocalFileSystemDriver(
			highTierFileSystemDriver.Path("/"),
		)
	} else {
		slog.Error("High tier file system driver must be a local file system driver")
	}

	logger, err := NewTieredFileSystemLogger(fileSystemDriver, "/_fslogs/tiered-files")

	if err != nil {
		log.Println("Error creating logger", err)
		return nil
	}

	fsd.logger = logger

	if fsd.CanSyncDirtyFiles != nil && fsd.CanSyncDirtyFiles() {
		err = fsd.SyncDirtyFiles()

		if err != nil {
			log.Println("Error syncing dirty files", err)
		}
	}

	go fsd.watchForFileChanges()

	return fsd
}

// Adding a file to the driver involves creating a new file that will be used
// to manage the state of the file on the high tier file system. When the file
// is written to it will be marked to be flushed to the low tier file system.
func (fsd *TieredFileSystemDriver) addFile(path string, file internalStorage.File, flag int) *TieredFile {
	if fsd.fileCountInternal() >= fsd.MaxFilesOpened {
		err := fsd.releaseOldestFileInternal()

		if err != nil {
			slog.Error("Error releasing oldest file", "error", err)
		}
	}

	fsd.Files[path] = NewTieredFile(
		fsd,
		path,
		file,
		flag,
	)

	element := fsd.FileOrder.PushBack(fsd.Files[path])
	fsd.Files[path].Element = element

	return fsd.Files[path]
}

// clearDirtyLog clears all dirty log entries after successful flush
func (fsd *TieredFileSystemDriver) clearDirtyLog() error {
	logs := make(map[int64]struct{})

	// Collect all log entries to clear
	for entry := range fsd.logger.DirtyKeys() {
		// Only clear entries for files that exist in the opened files
		if file, ok := fsd.Files[entry.Key]; ok {
			if err := fsd.logger.Remove(entry.Key, file.LogKey); err != nil {
				slog.Error("Error removing log entry:", "error", err)
			}
		}

		logs[entry.Log] = struct{}{}
	}

	// Remove the log files
	for log := range logs {
		if err := fsd.logger.removeLogFile(log); err != nil {
			slog.Error("Error removing log file:", "error", err)
		}
	}

	return nil
}

// ClearFiles clears all files from the tiered file system driver. This operation
// will flush any dirty files to durable storage before clearing the files from
// the storage device.
func (fsd *TieredFileSystemDriver) ClearFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	// Do not clear files if we cannot sync dirty files
	if !fsd.CanSyncDirtyFiles() {
		return nil
	}

	for path, file := range fsd.Files {
		if file.shouldBeWrittenToDurableStorage() {
			fsd.flushFileToDurableStorage(file, true)
		}

		err := fsd.releaseFile(file) // already holding lock
		if err != nil {
			log.Println("Error releasing file", err)
		} else {
			delete(fsd.Files, path)
		}
	}

	entries, err := fsd.highTierFileSystemDriver.ReadDir("/")

	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			err = fsd.highTierFileSystemDriver.RemoveAll(entry.Name())

			if err != nil {
				return err
			}
		} else {
			err = fsd.highTierFileSystemDriver.Remove(entry.Name())

			if err != nil {
				return err
			}
		}
	}

	return fsd.logger.Restart()
}

// CopyFile copies data from src to dst using a buffer pool to minimize memory allocations.
func (fsd *TieredFileSystemDriver) CopyFile(dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024) // Use a fixed-size buffer (32 KB in this example)

	var totalBytes int64

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			totalBytes += int64(written)

			if writeErr != nil {
				return totalBytes, writeErr
			}

			if written != n {
				return totalBytes, io.ErrShortWrite
			}
		}

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return totalBytes, readErr
		}
	}

	return totalBytes, nil
}

// Creating a new file instantiates a new file that will be used to manage
// the file on the high tier file system. When the file is closed, or written
// to, it will be pushed down to the low tier file system.
func (fsd *TieredFileSystemDriver) Create(path string) (internalStorage.File, error) {
	// Create the file in the high tier file system first
	highTierFile, err := fsd.highTierFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	// Also create the file in the low tier file system to ensure it exists
	lowTierFile, err := fsd.lowTierFileSystemDriver.Create(path)

	if err != nil {
		// If low tier creation fails, clean up the high tier file
		highTierFile.Close()
		fsd.highTierFileSystemDriver.Remove(path)

		return nil, err
	}

	lowTierFile.Close()

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	tieredFile := fsd.addFile(path, highTierFile, os.O_CREATE|os.O_RDWR)
	tieredFile.MarkUpdated()

	// Create and return a descriptor - addFile already increments usage, so don't double-count
	descriptor := NewTieredFileDescriptor(tieredFile, path, os.O_CREATE|os.O_RDWR, generateDescriptorID())

	return descriptor, nil
}

// FileCount returns the number of files currently loaded in memory
func (fsd *TieredFileSystemDriver) FileCount() int {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	return fsd.fileCountInternal()
}

// fileCountInternal returns the number of files currently loaded in memory (without acquiring mutex)
func (fsd *TieredFileSystemDriver) fileCountInternal() int {
	count := 0

	for _, file := range fsd.Files {
		if !file.Released {
			count++
		}
	}

	return count
}

// Flush all files to the low tier file system
func (fsd *TieredFileSystemDriver) Flush() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if !fsd.CanSyncDirtyFiles() {
		return nil
	}

	err := fsd.flushFiles()

	if err != nil {
		return err
	}

	// Clear the dirty log after successful flush
	return fsd.clearDirtyLog()
}

// Force flushing all files to durable storage. This operation is typically
// performed when the driver is being closed.
func (fsd *TieredFileSystemDriver) flushFiles() error {
	for _, file := range fsd.Files {
		fsd.flushFileToDurableStorage(file, false)

		// Remove the file from dirty log after successful flush
		if file.LogKey != 0 {
			if err := fsd.logger.Remove(file.Key, file.LogKey); err != nil {
				slog.Error("Error removing log entry after flush:", "error", err)
			}
		}
	}

	return nil
}

// Flushing a file to durable storage involves writing the file to the durable file
// system. This operation is typically performed when the file has been updated
// and has not been written to durable storage in the last minute.
func (fsd *TieredFileSystemDriver) flushFileToDurableStorage(file *TieredFile, force bool) {
	if !file.shouldBeWrittenToDurableStorage() && !force {
		return
	}

	if file.File == nil {
		slog.Debug("File is not open, skipping flush", "file", file.Key)
		return
	}

	err := file.AccessBarrier(func() error {
		_, err := file.File.Seek(0, io.SeekStart)

		if err != nil {
			slog.Error("Error seeking to start of file", "error", err)
			return err
		}

		buffer := fsd.buffers.Get().(*bytes.Buffer)

		buffer.Reset()

		defer fsd.buffers.Put(buffer)

		_, err = buffer.ReadFrom(file.File)

		if err != nil {
			log.Println("Error reading file from high tier storage", err)
			return err
		}

		err = fsd.lowTierFileSystemDriver.WriteFile(file.Key, buffer.Bytes(), 0600)

		if err != nil {
			// Check if context is done
			if fsd.context.Err() != nil {
				slog.Debug("Context is done, skipping write to durable storage", "error", fsd.context.Err())
				return fsd.context.Err()
			}

			return err
		}

		// Update the last written time to indicate the file is synced
		file.WrittenAt = time.Now().UTC()

		return nil
	})

	if err != nil {
		slog.Error("Error accessing file for flush", "error", err)
		return
	}
}

func (fsd *TieredFileSystemDriver) GetTieredFile(path string) (*TieredFile, bool) {
	// First, try with a read lock for the common case where no modification is needed
	fsd.mutex.Lock()
	file, ok := fsd.Files[path]

	if !ok {
		fsd.mutex.Unlock()
		return nil, false
	}

	// Check if file needs to be released without holding the lock
	needsRelease := file.Closed ||
		(file.UpdatedAt != (time.Time{}) && file.UpdatedAt.Add(TieredFileTTL).Before(time.Now().UTC())) ||
		(file.UpdatedAt.Equal((time.Time{})) && file.CreatedAt.Add(TieredFileTTL).Before(time.Now().UTC()))

	if !needsRelease {
		// Common case: file is valid, return it with read lock
		fsd.mutex.Unlock()
		return file, true
	}

	// Need to release file, upgrade to write lock
	fsd.mutex.Unlock()

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	// Re-check after acquiring write lock (double-checked locking pattern)
	file, ok = fsd.Files[path]

	if !ok {
		return nil, false
	}

	// Re-check conditions after acquiring write lock
	if file.Closed ||
		(file.UpdatedAt != (time.Time{}) && file.UpdatedAt.Add(TieredFileTTL).Before(time.Now().UTC())) ||
		(file.UpdatedAt.Equal((time.Time{})) && file.CreatedAt.Add(TieredFileTTL).Before(time.Now().UTC())) {

		err := fsd.releaseFile(file) // already holding lock

		if err != nil {
			slog.Error("Error releasing file", "error", err)
		}

		return nil, false
	}

	return file, true
}

// Mkdir creates a new directory on the low and high tier file systems.
func (fsd *TieredFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	err := fsd.highTierFileSystemDriver.Mkdir(path, perm)

	if err != nil {
		return err
	}

	return fsd.lowTierFileSystemDriver.Mkdir(path, perm)
}

// MkdirAll creates a new directory on all tiers of the file system.
func (fsd *TieredFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	err := fsd.highTierFileSystemDriver.MkdirAll(path, perm)

	if err != nil {
		return err
	}

	return fsd.lowTierFileSystemDriver.MkdirAll(path, perm)
}

// Mark a file as updated. This operation is typically performed when the
// file has been modified and needs to be flushed to durable storage. This will
// move the file to the back of the file order list and write the change to
// the log file. The log file is used to track changes to the file system so
// that in the event of a crash, updated files can be flushed to durable
// storage once a primary node assumes control of the cluster.
func (fsd *TieredFileSystemDriver) MarkFileUpdated(f *TieredFile) error {
	if f.Element != nil {
		fsd.FileOrder.MoveToBack(f.Element)
	}

	logKey, err := fsd.logger.Put(f.Key)

	if err != nil {
		log.Println("Error writing to log file", err)

		return err
	}

	f.LogKey = logKey

	return nil
}

// See OpenFile
func (fsd *TieredFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return fsd.OpenFile(path, os.O_RDWR, 0)
}

// Opening a file in the tiered file system driver involves reading a file from
// the low tier file system. If the file does not exist on the low tier file
// system, this operation will create a new file on the high tier file system
// and then create a new tiered file that will be used to manage the file.
func (fsd *TieredFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	if file, ok := fsd.GetTieredFile(path); ok {
		// Create and return a new descriptor for this open
		file.mutex.Lock()
		descriptor := NewTieredFileDescriptor(file, path, flag, generateDescriptorID())
		file.mutex.Unlock()

		return descriptor, nil
	}

	// If the file is write only, we need to add file flags to durable storage
	// that allow the file to be created and read.
	durableFlag := flag

	if flag&os.O_WRONLY == os.O_WRONLY {
		durableFlag &= ^os.O_WRONLY
		durableFlag |= os.O_RDWR
	}

	// To open a file, we need to first try and read the file from the durable storage
	f, err := fsd.lowTierFileSystemDriver.OpenFile(path, durableFlag, perm)

	if err != nil {
		return nil, err
	}

tryOpen:
	file, err := fsd.highTierFileSystemDriver.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = fsd.highTierFileSystemDriver.MkdirAll(filepath.Dir(path), 0750)

			if err != nil {
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	_, err = fsd.CopyFile(file, f)

	if err != nil {
		log.Println("Error writing to high tier file", err)
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	tieredFile := fsd.addFile(path, file, flag)

	// Create and return a descriptor instead of the TieredFile directly
	descriptor := NewTieredFileDescriptor(tieredFile, path, flag, generateDescriptorID())

	return descriptor, nil
}

func (fsd *TieredFileSystemDriver) Path(path string) string {
	return fsd.highTierFileSystemDriver.Path(path)
}

// Reading a directory only occurs on the low tier file system. This is because
// the high tier file system is only used for temporary storage and does not
// contain a complete copy of the data. However, the file will be tracked in
// the driver to keep track of its state for future use that may require the file.
func (fsd *TieredFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := fsd.lowTierFileSystemDriver.ReadDir(path)

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Reading a file in the tiered file system driver involves reading the file from
// the high tier file system. If the file does not exist on the high tier file
// system, the operation will be attempted on the low tier file system. If the
// file is found on the low tier file system, it will be copied to the high tier
// file system for future use and an entry will be created in the driver to
// track the file.
func (fsd *TieredFileSystemDriver) ReadFile(path string) ([]byte, error) {
	if file, ok := fsd.GetTieredFile(path); ok && file.File != nil {
		// Save current position
		currentPos, err := file.Seek(0, io.SeekCurrent)

		if err != nil {
			return nil, err
		}

		// Seek to start for reading entire file
		_, err = file.Seek(0, io.SeekStart)

		if err != nil {
			return nil, err
		}

		data, err := io.ReadAll(file)

		if err != nil {
			return nil, err
		}

		// Restore original position
		_, err = file.Seek(currentPos, io.SeekStart)

		if err != nil {
			return nil, err
		}

		return data, nil
	}

	data, err := fsd.lowTierFileSystemDriver.ReadFile(path)

	if err != nil {
		return nil, err
	}

	file, err := fsd.highTierFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)

	if err != nil {
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.addFile(path, file, os.O_RDONLY)

	return data, nil
}

// Releasing a file involves closing the file and removing it from the driver.
// This operation is typically performed when the file is no longer needed.
func (fsd *TieredFileSystemDriver) releaseFile(file *TieredFile) error {
	descriptorCount := file.GetDescriptorCount()

	// If file needs flushing, flush it first before release (regardless of open descriptors)
	if file.shouldBeWrittenToDurableStorage() {
		fsd.flushFileToDurableStorage(file, true) // force=true to flush even if interval hasn't passed
	}

	// Always close and clean up the actual file handle when releasing - descriptors will reopen if needed
	if file.File != nil {
		file.AccessBarrier(func() error {
			err := file.File.Close()

			if err != nil {
				return err
			}

			err = fsd.highTierFileSystemDriver.Remove(file.Key)

			if err != nil && !os.IsNotExist(err) {
				slog.Error("Error removing file from high tier file system", "error", err)
				return err
			}

			file.File = nil

			return nil
		})

	}

	// Mark the file as released (descriptors will need to reopen when accessed)
	file.Released = true

	// If the file should be removed from the map (no descriptors), do it here while holding the lock
	if descriptorCount == 0 {
		delete(fsd.Files, file.Key)
	}

	return nil
}

// releaseFileFromMap removes a file from the Files map - should be called while holding driver mutex
func (fsd *TieredFileSystemDriver) ReleaseFileFromMap(file *TieredFile) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	delete(fsd.Files, file.Key)
}

// Removing a file included removing the file from the high tier file system
// and also removing the file from the low tier file system immediately after.
func (fsd *TieredFileSystemDriver) Remove(path string) error {
	if file, ok := fsd.GetTieredFile(path); ok {
		err := fsd.ReleaseFileWithLock(file)

		if err != nil {
			slog.Error("Error releasing file:", "error", err)
		}
	}

	// OPTIMIZE: Run concurrently
	err := fsd.highTierFileSystemDriver.Remove(path)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.lowTierFileSystemDriver.Remove(path)
}

// Removing all files from the tiered file system path involves removing all
// files from the high tier file system and also removing all files from the
// low tier file system immediately after.
func (fsd *TieredFileSystemDriver) RemoveAll(path string) error {
	// Remove any files that are under the path
	var filesToClose []*TieredFile
	fsd.mutex.Lock()

	for key, file := range fsd.Files {
		if key == path || (len(key) > len(path) && key[:len(path)] == path) {
			filesToClose = append(filesToClose, file)
			delete(fsd.Files, key)
		}
	}

	fsd.mutex.Unlock()

	// Close files outside the lock
	for _, file := range filesToClose {
		go file.closeFile()
	}

	// OPTIMIZE: Run concurrently
	err := fsd.highTierFileSystemDriver.RemoveAll(path)

	if err != nil {
		return err
	}

	return fsd.lowTierFileSystemDriver.RemoveAll(path)
}

// Release the oldest file in the tiered file system driver. This operation will
// remove the oldest file from the driver and close the file. If the TieredFile
// is still open, it will reopen the file resource.
func (fsd *TieredFileSystemDriver) ReleaseOldestFile() error {
	if fsd.releasingOldestFile.Load() {
		return nil
	}

	fsd.releasingOldestFile.Store(true)
	defer fsd.releasingOldestFile.Store(false)

	return fsd.releaseOldestFileWithLock()
}

// releaseOldestFileInternal releases the oldest file in the tiered file system
// driver without acquiring the lock. This is useful for operations that need to
// release the oldest file without blocking other operations.
func (fsd *TieredFileSystemDriver) releaseOldestFileInternal() error {
	// Assumes lock is already held
	element := fsd.FileOrder.Front()

	if element == nil {
		return ErrNoTieredFilesToRemove
	}

	file := element.Value.(*TieredFile)

	// Skip files that need to be written to durable storage
	// Files with open descriptors can be released if they're already flushed
	for file.shouldBeWrittenToDurableStorage() {
		element = element.Next()

		if element == nil {
			return ErrNoTieredFilesToRemove
		}

		file = element.Value.(*TieredFile)
	}

	if element == nil || file == nil {
		return ErrNoTieredFilesToRemove
	}

	fsd.FileOrder.Remove(element)

	return fsd.releaseFile(file)
}

func (fsd *TieredFileSystemDriver) releaseOldestFileWithLock() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	return fsd.releaseOldestFileInternal()
}

// Renaming a file in the tiered file system driver involves renaming the file
// on the high tier file system and then renaming the file on the low tier file
// system immediately after.
func (fsd *TieredFileSystemDriver) Rename(oldpath, newpath string) error {
	if file, ok := fsd.GetTieredFile(oldpath); ok {
		err := fsd.ReleaseFileWithLock(file)
		if err != nil {
			slog.Error("Error releasing file:", "error", err)
		}
	}

	err := fsd.highTierFileSystemDriver.Rename(oldpath, newpath)

	if err != nil && !os.IsNotExist(err) {
		slog.Error("Error renaming file on high tier file system:", "error", err)
		return err
	}

	return fsd.lowTierFileSystemDriver.Rename(oldpath, newpath)
}

// Shutting down the tiered file system driver involves stopping the watch ticker
// and flushing all files to durable storage.
func (fsd *TieredFileSystemDriver) Shutdown() error {
	if fsd.shuttingDown {
		return nil
	}

	fsd.shuttingDown = true

	if fsd.watchTicker != nil {
		fsd.watchTicker.Stop()
	}

	if !fsd.CanSyncDirtyFiles() {
		return nil
	}

	return fsd.Flush()
}

// Statting a file in the tiered file system driver involves statting the file
// on the high tier file system and then returning the file information. If the
// file does not exist on the high tier file system, the operation will be
// attempted on the low tier file system.
func (fsd *TieredFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	isDir := path[len(path)-1] == '/'

	// Path ends with a slash, so it is a directory
	if isDir {
		return fsd.lowTierFileSystemDriver.Stat(path)
	} else if file, ok := fsd.GetTieredFile(path); ok {
		return file.Stat()
	}

	info, err := fsd.lowTierFileSystemDriver.Stat(path)

	if err != nil {
		return nil, err
	}

	return info, nil
}

// HasDirtyLogs checks if there are any dirty logs
func (fsd *TieredFileSystemDriver) HasDirtyLogs() bool {
	return fsd.logger.HasDirtyLogs()
}

// Logger returns the logger for testing purposes
func (fsd *TieredFileSystemDriver) Logger() *TieredFileSystemLogger {
	return fsd.logger
}

// SyncDirtyFiles checks if there are any dirty files in the logger and
// flushes them to durable storage. This operation is typically performed
// when the driver is initially opened and there may be files that have not
// been flushed to durable storage due to a crash or other error.
func (fsd *TieredFileSystemDriver) SyncDirtyFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if fsd.logger.HasDirtyLogs() {
		slog.Info("Syncing dirty files to durable storage")

		logs := make(map[int64]struct{})

		for entry := range fsd.logger.DirtyKeys() {
			if file, ok := fsd.Files[entry.Key]; ok {
				fsd.flushFileToDurableStorage(file, true)
			} else {
				// Check if the file exists on high tier before trying to sync
				if fileInfo, err := fsd.highTierFileSystemDriver.Stat(entry.Key); err != nil {
					if os.IsNotExist(err) {
						slog.Debug("File does not exist on high tier, skipping sync", "file", entry.Key)

						// Still remove the log entry since the file doesn't exist
						if err := fsd.logger.Remove(entry.Key, entry.Log); err != nil {
							slog.Error("Error removing log entry for non-existent file:", "error", err)
						}

						logs[entry.Log] = struct{}{}
						continue
					}

					slog.Error("Error checking file existence on high tier file system", "error", err)

					continue
				} else if fileInfo.Size() == 0 {
					slog.Debug("File is empty on high tier, skipping sync", "file", entry.Key)

					// Still remove the log entry since empty files shouldn't be synced
					if err := fsd.logger.Remove(entry.Key, entry.Log); err != nil {
						slog.Error("Error removing log entry for empty file:", "error", err)
					}

					logs[entry.Log] = struct{}{}

					continue
				}

				file, err := fsd.highTierFileSystemDriver.OpenFile(entry.Key, os.O_RDWR|os.O_CREATE, 0600)

				if err != nil {
					slog.Error("Error opening file on high tier file system", "error", err)
					continue
				}

				tieredFile := fsd.addFile(entry.Key, file, os.O_RDWR)

				fsd.flushFileToDurableStorage(tieredFile, true)

				if err := fsd.logger.Remove(entry.Key, tieredFile.LogKey); err != nil {
					slog.Error("Error removing log entry:", "error", err)
				}
			}

			logs[entry.Log] = struct{}{}
		}

		for log := range logs {
			err := fsd.logger.removeLogFile(log)

			if err != nil {
				slog.Error("Error removing log file:", "error", err)
			}
		}
	}

	return nil
}

// Truncating a file in the tiered file system driver involves truncating the file
// on the high tier file system and then truncating the file on the low tier
// file system immediately after.
func (fsd *TieredFileSystemDriver) Truncate(path string, size int64) error {
	err := fsd.highTierFileSystemDriver.Truncate(path, size)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.lowTierFileSystemDriver.Truncate(path, size)
}

// Watching for file changes involves periodically checking the state of all files
// in the driver. If a file has been updated and has not been written to durable
// storage in the last minute, the file will be written to durable storage. If a
// file has been closed, the file will be released.
func (fsd *TieredFileSystemDriver) watchForFileChanges() {
	if fsd.WriteInterval == 0 {
		return
	}

	if fsd.watchTicker != nil {
		fsd.watchTicker.Stop()
	}

	fsd.watchTicker = time.NewTicker(fsd.WriteInterval)

	for {
		select {
		case <-fsd.context.Done():
			// Force flush all files to durable storage
			err := fsd.Shutdown()

			if err != nil {
				slog.Error("Error shutting down tiered file system driver:", "error", err)
			}

			return
		case <-fsd.watchTicker.C:
			if !fsd.CanSyncDirtyFiles() {
				continue
			}

			// Take a snapshot of all files under lock to avoid concurrent map write during iteration
			fsd.mutex.Lock()
			fileSnapshot := make([]*TieredFile, 0, len(fsd.Files))
			for _, file := range fsd.Files {
				fileSnapshot = append(fileSnapshot, file)
			}
			fsd.mutex.Unlock()

			// Now filter for files to flush outside the lock
			filesToFlush := make([]*TieredFile, 0, len(fileSnapshot))
			for _, file := range fileSnapshot {
				if file.shouldBeWrittenToDurableStorage() {
					filesToFlush = append(filesToFlush, file)
				}
			}

			if len(filesToFlush) == 0 {
				continue
			}

			// Use a semaphore to limit concurrency to 10
			semaphore := make(chan struct{}, 10)
			var wg sync.WaitGroup

			// Process files concurrently
			for _, file := range filesToFlush {
				wg.Add(1)
				semaphore <- struct{}{} // Acquire semaphore slot

				go func(f *TieredFile) {
					defer wg.Done()
					defer func() { <-semaphore }() // Release semaphore slot
					fsd.flushFileToDurableStorage(f, false)
				}(file)
			}

			// Wait for all active flush operations to finish
			wg.Wait()

			// Attempt to remove files to ensure we do not exceed the max
			// number of files opened
			for fsd.FileCount() > fsd.MaxFilesOpened {
				err := fsd.ReleaseOldestFile()

				if err != nil {
					log.Println("Error removing oldest file", err)
					break
				}
			}
		}
	}
}

// Writing a file in the tiered file system driver involves writing the file on
// the high tier file system. Writing the file to low tier storage will take
// place immmediately after.
func (fsd *TieredFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	var err error
	var file *TieredFile
	var ok bool

	// If the file is not found, create a new file on the high tier file system
	if file, ok = fsd.GetTieredFile(path); !ok {
		f, err := fsd.highTierFileSystemDriver.Create(path)

		if err != nil {
			return err
		}

		fsd.mutex.Lock()
		file = fsd.addFile(path, f, os.O_CREATE|os.O_RDWR)
		fsd.mutex.Unlock()
	} else {
		err = file.Truncate(0)
	}

	if err != nil {
		return err
	}

	// Write the data to the file
	_, err = file.Write(data)

	if err != nil {
		return err
	}

	// Mark the file as updated
	file.MarkUpdated()

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.flushFileToDurableStorage(file, true)

	return nil
}
