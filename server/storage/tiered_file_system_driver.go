package storage

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

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
	context                  context.Context
	lowTierFileSystemDriver  FileSystemDriver
	FileCount                int
	FileOrder                *list.List
	Files                    map[string]*TieredFile
	highTierFileSystemDriver FileSystemDriver
	MaxFilesOpened           int
	mutex                    *sync.RWMutex
	releasingOldestFile      atomic.Bool
	shuttingDown             bool
	WriteInterval            time.Duration
	watchTicker              *time.Ticker
}

type TieredFileSystemNewFunc func(context.Context, *TieredFileSystemDriver)

// Create a new instance of a tiered file system driver. This driver will manage
// files that are stored on the high and low tier file system.
func NewTieredFileSystemDriver(
	context context.Context,
	highTierFileSystemDriver FileSystemDriver,
	lowTierFileSystemDriver FileSystemDriver,
	f ...TieredFileSystemNewFunc,
) *TieredFileSystemDriver {
	fsd := &TieredFileSystemDriver{
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
		context:                  context,
		FileCount:                0,
		FileOrder:                list.New(),
		Files:                    map[string]*TieredFile{},
		highTierFileSystemDriver: highTierFileSystemDriver,
		MaxFilesOpened:           TieredFileSystemMaxOpenFiles,
		lowTierFileSystemDriver:  lowTierFileSystemDriver,
		mutex:                    &sync.RWMutex{},
		WriteInterval:            DefaultWriteInterval,
	}

	if len(f) > 0 {
		for _, fn := range f {
			fn(context, fsd)
		}
	}

	go fsd.watchForFileChanges()

	return fsd
}

// Adding a file to the driver involves creating a new file that will be used
// to manage the state of the file on the high tier file system. When the file
// is written to it will be marked to be flushed to the low tier file system.
func (fsd *TieredFileSystemDriver) addFile(path string, file internalStorage.File, flag int) *TieredFile {
	if fsd.FileCount >= fsd.MaxFilesOpened {
		fsd.ReleaseOldestFile()
	}

	fsd.Files[path] = NewTieredFile(
		fsd,
		path,
		file,
		flag,
	)

	element := fsd.FileOrder.PushBack(fsd.Files[path])
	fsd.Files[path].Element = element
	fsd.FileCount++

	return fsd.Files[path]
}

func (fsd *TieredFileSystemDriver) ClearFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for path, file := range fsd.Files {
		if file.shouldBeWrittenToDurableStorage() {
			fsd.flushFileToDurableStorage(file, true)
		}

		err := fsd.releaseFile(file)

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

	return nil
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

// Creating a new file istantiates a new file durable that will be used to manage
// the file on the high tier file system. When the file is closed, or written
// to, it will be pushed down to the low tier file system.
func (fsd *TieredFileSystemDriver) Create(path string) (internalStorage.File, error) {
	file, err := fsd.lowTierFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	newFile := fsd.addFile(path, file, os.O_CREATE|os.O_RDWR)

	newFile.MarkUpdated()

	return newFile, nil
}

func (fsd *TieredFileSystemDriver) Flush() error {
	// fsd.mutex.Lock()
	// defer fsd.mutex.Unlock()

	// Flush all files to durable storage
	return fsd.flushFiles()
}

// Force flushing all files to durable storage. This operation is typically
// performed when the driver is being closed.
func (fsd *TieredFileSystemDriver) flushFiles() error {
	for _, file := range fsd.Files {
		fsd.flushFileToDurableStorage(file, false)
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

	// file.mutex.Lock()
	// defer file.mutex.Unlock()

	_, err := file.File.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to start of file", err)
		return
	}

	buffer := fsd.buffers.Get().(*bytes.Buffer)

	buffer.Reset()

	defer fsd.buffers.Put(buffer)

	_, err = buffer.ReadFrom(file.File)

	if err != nil {
		log.Println("Error reading file from high tier storage", err)
		return
	}

	err = fsd.lowTierFileSystemDriver.WriteFile(file.Key, buffer.Bytes(), 0644)

	if err != nil {
		// Handle error (retry, log, etc.)
		// Check if context is done
		if fsd.context.Err() != nil {
			log.Println("Context is done, skipping write to durable storage", fsd.context.Err())
			return
		}

		log.Println("Error writing file to durable storage", err)
		return
	}

	// Update the last written time to indicate the file is synced
	file.WrittenAt = time.Now()
}

func (fsd *TieredFileSystemDriver) GetTieredFile(path string) (*TieredFile, bool) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.Files[path]; ok {
		if file.Closed {
			fsd.releaseFile(file)
			return nil, false
		}

		// Do not return the file if it is stale
		if file.UpdatedAt != (time.Time{}) && file.UpdatedAt.Add(TieredFileTTL).Before(time.Now()) ||
			(file.UpdatedAt == (time.Time{}) && file.CreatedAt.Add(TieredFileTTL).Before(time.Now())) {
			fsd.releaseFile(file)

			return nil, false
		}

		return file, true
	}

	return nil, false
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

// See OpenFile
func (fsd *TieredFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return fsd.OpenFile(path, os.O_RDWR, 0)
}

// Opening a file in the tiered file system driver involves reading a file from
// the low tier file system. If the file does not exist on the low tier file
// system, this operation will create a new file on the high tier file system
// and then create a new tiered file that will be used to manage the file.
func (fsd *TieredFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	// start := time.Now()
	// defer func() {
	// 	log.Printf("OpenFile %s took %s", strings.Split(path, "/")[len(strings.Split(path, "/"))-1], time.Since(start))
	// }()

	if file, ok := fsd.GetTieredFile(path); ok {
		// Compare the flags to ensure they match
		if file.Flag&flag == flag {
			_, err := file.Seek(0, io.SeekStart)

			if err != nil {
				log.Println("Error seeking to start of file", err)

				return nil, err
			}

			return file, nil
		}

		fsd.releaseFile(file)
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

	file, err := fsd.highTierFileSystemDriver.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	_, err = fsd.CopyFile(file, f)

	if err != nil {
		log.Println("Error writing to high tier file", err)
		return nil, err
	}

	newFile := fsd.addFile(path, file, flag)

	return newFile, nil
}

func (fsd *TieredFileSystemDriver) OpenFileDirect(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	if file, ok := fsd.GetTieredFile(path); ok {
		// Compare the flags to ensure they match
		if file.Flag&flag == flag {
			_, err := file.Seek(0, io.SeekStart)

			if err != nil {
				log.Println("Error seeking to start of file", err)

				return nil, err
			}

			return file, nil
		}

		fsd.releaseFile(file)
	}

	// If the file is write only, we need to add file flags to durable storage
	// that allow the file to be created and read.
	durableFlag := flag

	if flag&os.O_WRONLY == os.O_WRONLY {
		durableFlag &= ^os.O_WRONLY
		durableFlag |= os.O_RDWR
	}

	// To open a file, we need to first try and read the file from the durable storage
	f, err := fsd.lowTierFileSystemDriver.OpenFileDirect(path, durableFlag, perm)

	if err != nil {
		return nil, err
	}

	file, err := fsd.highTierFileSystemDriver.OpenFileDirect(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	_, err = fsd.CopyFile(file, f)

	if err != nil {
		log.Println("Error writing to high tier file", err)
		return nil, err
	}

	newFile := fsd.addFile(path, file, flag)

	return newFile, nil
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
		file.Seek(0, io.SeekStart)

		return io.ReadAll(file)
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

	fsd.addFile(path, file, os.O_RDONLY)

	return data, nil
}

// Releasing a file involves closing the file and removing it from the driver.
// This operation is typically performed when the file is no longer needed.
func (fsd *TieredFileSystemDriver) releaseFile(file *TieredFile) error {
	// Files should not be released if their changes are pending to be written
	if file.shouldBeWrittenToDurableStorage() {
		return ErrTieredFileCannotBeReleased
	}

	if file.File != nil {
		err := file.File.Close()

		if err != nil {
			log.Println("Error closing file", err)
			return err
		}

		err = fsd.highTierFileSystemDriver.Remove(file.Key)

		if err != nil && !os.IsNotExist(err) {
			log.Println("Error removing file from high tier file system", err)
			return err
		}

		file.File = nil
	}

	if _, ok := fsd.Files[file.Key]; ok {
		delete(fsd.Files, file.Key)
		fsd.FileCount--
	}

	return nil
}

// Removing a file included removing the file from the high tier file system
// and also removing the file from the low tier file system immediately after.
func (fsd *TieredFileSystemDriver) Remove(path string) error {
	if file, ok := fsd.GetTieredFile(path); ok {
		fsd.mutex.Lock()
		fsd.releaseFile(file)
		fsd.mutex.Unlock()
	}

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
	fsd.mutex.Lock()

	for key, file := range fsd.Files {
		if key == path || key[:len(path)] == path {
			go file.closeFile()
			delete(fsd.Files, key)
			fsd.FileCount--
		}
	}

	fsd.mutex.Unlock()

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

	element := fsd.FileOrder.Front()

	if element == nil {
		return ErrNoTieredFilesToRemove
	}

	file := element.Value.(*TieredFile)

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

// Renaming a file in the tiered file system driver involves renaming the file
// on the high tier file system and then renaming the file on the low tier file
// system immediately after.
func (fsd *TieredFileSystemDriver) Rename(oldpath, newpath string) error {
	if file, ok := fsd.GetTieredFile(oldpath); ok {
		fsd.mutex.Lock()
		fsd.releaseFile(file)
		fsd.mutex.Unlock()
	}

	err := fsd.highTierFileSystemDriver.Rename(oldpath, newpath)

	if err != nil && !os.IsNotExist(err) {
		log.Println("Error renaming file on high tier file system", err)
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

	return info, err
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
			fsd.Shutdown()
			return
		case <-fsd.watchTicker.C:
			fsd.mutex.RLock()

			// Use a semaphore to limit concurrency to 10
			semaphore := make(chan struct{}, 10)
			filesToFlush := make([]*TieredFile, 0)

			for _, file := range fsd.Files {
				if file.shouldBeWrittenToDurableStorage() {
					filesToFlush = append(filesToFlush, file)
				}
			}

			fsd.mutex.RUnlock()

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
			for fsd.FileCount > fsd.MaxFilesOpened {
				err := fsd.ReleaseOldestFile()

				if err != nil {
					log.Println("Error removing oldest file", err)
					break
				}

			}
		default:
			if fsd.context.Err() != nil {
				return
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

	fsd.mutex.RLock()
	defer fsd.mutex.RUnlock()

	fsd.flushFileToDurableStorage(fsd.Files[path], true)

	return nil
}
