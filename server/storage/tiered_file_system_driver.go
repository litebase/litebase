package storage

import (
	"bytes"
	"container/list"
	"context"
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"sync"
	"time"
)

const (
	DefaultWriteInterval = 10 * time.Second
)

// Data in this driver is stored locally on disk then eventually pushed up to
// durable storage with S3 compatability. This provides fast read access
// performance with scalable and cost-effective long-term storage.
type TieredFileSystemDriver struct {
	buffers                 sync.Pool
	context                 context.Context
	durableFileSystemDriver FileSystemDriver
	FileCount               int
	FileOrder               *list.List
	Files                   map[string]*TieredFile
	localFileSystemDriver   FileSystemDriver
	MaxFilesOpened          int
	mutex                   *sync.RWMutex
	WriteInterval           time.Duration
	watchTicker             *time.Ticker
}

const (
	TieredFileTTL                = 1 * time.Hour
	TieredFileSystemMaxOpenFiles = 10000
)

type TieredFileSystemNewFunc func(context.Context, *TieredFileSystemDriver)

// Create a new instance of a tiered file system driver. This driver will manage
// files that are stored on the local file system and durable file system.
func NewTieredFileSystemDriver(context context.Context, localFileSystemDriver FileSystemDriver, durableFileSystemDriver FileSystemDriver, f ...TieredFileSystemNewFunc) *TieredFileSystemDriver {
	fsd := &TieredFileSystemDriver{
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
		context:                 context,
		FileCount:               0,
		FileOrder:               list.New(),
		Files:                   map[string]*TieredFile{},
		localFileSystemDriver:   localFileSystemDriver,
		MaxFilesOpened:          TieredFileSystemMaxOpenFiles,
		mutex:                   &sync.RWMutex{},
		durableFileSystemDriver: durableFileSystemDriver,
		WriteInterval:           DefaultWriteInterval,
	}

	if len(f) > 0 {
		for _, fn := range f {
			fn(context, fsd)
		}
	}

	go fsd.watchForFileChanges()

	return fsd
}

// Adding a file to the driver involves creating a new file durable that will be
// used to manage the file on the local file system. When the file is closed, or
// written to, it will be pushed to the durable file system.
func (fsd *TieredFileSystemDriver) AddFile(path string, file internalStorage.File, flag int) *TieredFile {
	if fsd.FileCount >= fsd.MaxFilesOpened {
		fsd.RemoveOldestFile()
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

// CopyFile copies data from src to dst using a buffer pool to minimize memory allocations.
func (fsd *TieredFileSystemDriver) CopyFile(dst io.Writer, src io.Reader) (int64, error) {
	buf := fsd.buffers.Get().(*bytes.Buffer)
	defer fsd.buffers.Put(buf)

	buf.Reset()

	var totalBytes int64

	for {
		n, readErr := buf.ReadFrom(src)

		if n == 0 {
			break
		}

		if n > 0 {
			written, writeErr := dst.Write(buf.Bytes()[:n])
			totalBytes += int64(written)

			if writeErr != nil {
				return totalBytes, writeErr
			}

			if written != int(n) {
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
// the file on the local file system. When the file is closed, or written to, it
// will be pushed to the durable file system.
func (fsd *TieredFileSystemDriver) Create(path string) (internalStorage.File, error) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	newFile := fsd.AddFile(path, file, os.O_CREATE|os.O_RDWR)

	newFile.MarkUpdated()

	fsd.flushFileToDurableStorage(newFile, false)

	return newFile, nil
}

// Force flushing all files to durable storage. This operation is typically
// performed when the driver is being closed.
func (fsd *TieredFileSystemDriver) flushFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for _, file := range fsd.Files {
		fsd.flushFileToDurableStorage(file, true)
	}

	return nil
}

// Flushing a file to durable storage involves writing the file to the durable file
// system. This operation is typically performed when the file has been updated
// and has not been written to durable storage in the last minute.
func (fsd *TieredFileSystemDriver) flushFileToDurableStorage(file *TieredFile, force bool) {
	if !file.shouldBeWrittenToDurableStorage() && !force {
		log.Println("File does not need to be written to durable storage", file.Key)
		return
	}

	file.mutex.Lock()

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
		// Handle error (retry, log, etc.)
		log.Println("Error reading file from local storage", err)
		return
	}

	err = fsd.durableFileSystemDriver.WriteFile(file.Key, buffer.Bytes(), 0644)

	if err != nil {
		// Handle error (retry, log, etc.)
		// log.Println("Error writing file to durable storage", err)
		return
	}

	// Update the last written time to indicate the file is synced
	file.WrittenAt = time.Now()

	file.mutex.Unlock()
}

func (fsd *TieredFileSystemDriver) GetLocalFile(path string) (*TieredFile, bool) {
	if file, ok := fsd.Files[path]; ok {
		if file.Closed {
			fsd.ReleaseFile(file)
			return nil, false
		}

		// Do not return the file if it is stale
		if file.UpdatedAt != (time.Time{}) && file.UpdatedAt.Add(TieredFileTTL).Before(time.Now()) ||
			(file.UpdatedAt == (time.Time{}) && file.CreatedAt.Add(TieredFileTTL).Before(time.Now())) {
			fsd.ReleaseFile(file)

			return nil, false
		}

		return file, true
	}

	return nil, false
}

// Mkdir creates a new directory on the local file system. This has no effect on
// the durable file system.
func (fsd *TieredFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	err := fsd.localFileSystemDriver.Mkdir(path, perm)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.Mkdir(path, perm)
}

// MkdirAll creates a new directory on the local file system, along with any
// parents directories. This has no effect on the durable file system.
func (fsd *TieredFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	err := fsd.localFileSystemDriver.MkdirAll(path, perm)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.MkdirAll(path, perm)
}

// See OpenFile
func (fsd *TieredFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return fsd.OpenFile(path, os.O_RDWR, 0)
}

// Opening a file in the tiered file system driver involves reading a file from
// the durable file system. If the file does not exist on the durable file system,
// this operation will create a new file on the local file system and then create
// a new tiered file durable that will be used to manage the file.
func (fsd *TieredFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(path); ok {
		// Compare the flags to ensure they match
		if file.Flag&flag == flag {
			_, err := file.Seek(0, io.SeekStart)

			if err != nil {
				log.Println("Error seeking to start of file", err)

				return nil, err
			}

			return file, nil
		}

		fsd.ReleaseFile(file)
	}

	// To open a file, we need to first try and read the file from the durable storage
	f, err := fsd.durableFileSystemDriver.OpenFile(path, flag, perm)

	// If the file does not exist in durable storage, we will create a new file
	// on the local file system.
	if err != nil {
		return nil, err
	}

	// Open the file on the local file system
	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	// Write the file data to the local file system
	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to start of file", err)

		return nil, err
	}

	_, err = fsd.CopyFile(file, f)

	if err != nil {
		log.Println("Error writing to local file", err)
		return nil, err
	}

	newFile := fsd.AddFile(path, file, flag)

	// Write the file data to the local file system
	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to start of file", err)
		return nil, err
	}

	return newFile, nil
}

// Reading a directory only occurs on the durable file system. This is because the
// local file system is only used for temporary storage and does not contain a
// complete copy of the data. However, the file will be tracked in the driver
// to keep track of its state for future use that may require the file.
func (fsd *TieredFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := fsd.durableFileSystemDriver.ReadDir(path)

	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Reading a file in the tiered file system driver involves reading the file from
// the local file system. If the file does not exist on the local file system, the
// operation will be attempted on the durable file system. If the file is found on
// the durable file system, it will be copied to the local file system for future
// use and an entry will be created in the driver to track the file.
func (fsd *TieredFileSystemDriver) ReadFile(path string) ([]byte, error) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(path); ok && file.File != nil {
		file.Seek(0, io.SeekStart)

		return io.ReadAll(file)
	}

	data, err := fsd.durableFileSystemDriver.ReadFile(path)

	if err != nil {
		return nil, err
	}

	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)

	if err != nil {
		return nil, err
	}

	fsd.AddFile(path, file, os.O_RDONLY)

	return data, nil
}

// Releasing a file involves closing the file and removing it from the driver. This
// operation is typically performed when the file is no longer needed.
func (fsd *TieredFileSystemDriver) ReleaseFile(file *TieredFile) {
	file.closeFile()
	delete(fsd.Files, file.Key)
	fsd.FileCount--
}

// Removing a file included removing the file from the local file system and also
// removing the file from the durable file system immediately after.
func (fsd *TieredFileSystemDriver) Remove(path string) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(path); ok {
		fsd.ReleaseFile(file)
	}

	err := fsd.localFileSystemDriver.Remove(path)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.durableFileSystemDriver.Remove(path)
}

// Removing all files from the tiered file system path involves removing all files
// from the local file system and also removing all files from the durable file
// system immediately after.
func (fsd *TieredFileSystemDriver) RemoveAll(path string) error {
	// Remove any files that are under the path
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for key, file := range fsd.Files {
		if key == path || key[:len(path)] == path {
			file.closeFile()
			delete(fsd.Files, key)
			fsd.FileCount--
		}
	}

	err := fsd.localFileSystemDriver.RemoveAll(path)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.RemoveAll(path)
}

// TODO: This needs to be tested and verified. How does it impact opened files
// that are used throughout the application?
func (fsd *TieredFileSystemDriver) RemoveOldestFile() {
	element := fsd.FileOrder.Front()

	if element == nil {
		return
	}

	file := element.Value.(*TieredFile)

	// Remove the file if it does not need to be written to durable storage or
	// else find the next file that is ready to be released.
	if !file.shouldBeWrittenToDurableStorage() {
		for file.shouldBeWrittenToDurableStorage() {
			element = element.Next()

			if element == nil {
				return
			}

			file = element.Value.(*TieredFile)
		}
	}

	if element == nil || file == nil {
		return
	}

	fsd.FileOrder.Remove(element)
	fsd.ReleaseFile(file)
}

// Renaming a file in the tiered file system driver involves renaming the file on
// the local file system and then renaming the file on the durable file system
// immediately after.
func (fsd *TieredFileSystemDriver) Rename(oldpath, newpath string) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(oldpath); ok {
		fsd.ReleaseFile(file)
	}

	err := fsd.localFileSystemDriver.Rename(oldpath, newpath)

	if err != nil && !os.IsNotExist(err) {
		log.Println("Error FOOOO file on local file system", err)
		return err
	}

	return fsd.durableFileSystemDriver.Rename(oldpath, newpath)
}

// Statting a file in the tiered file system driver involves statting the file on
// the local file system and then returning the file information. If the file does
// not exist on the local file system, the operation will be attempted on the
// durable file system.
func (fsd *TieredFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	isDir := path[len(path)-1] == '/'

	// Path ends with a slash, so it is a directory
	if isDir {
		return fsd.durableFileSystemDriver.Stat(path)
	} else if file, ok := fsd.GetLocalFile(path); ok {
		return file.Stat()
	}

	info, err := fsd.durableFileSystemDriver.Stat(path)

	if err != nil {
		return nil, err
	}

	return info, err
}

// Truncating a file in the tiered file system driver involves truncating the file
// on the local file system and then truncating the file on the durable file system
// immediately after.
func (fsd *TieredFileSystemDriver) Truncate(path string, size int64) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	err := fsd.localFileSystemDriver.Truncate(path, size)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.durableFileSystemDriver.Truncate(path, size)
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
			err := fsd.flushFiles()

			if err != nil {
				log.Println("Error flushing files to durable storage", err)
			}
			return
		case <-fsd.watchTicker.C:
			fsd.mutex.Lock()

			for _, file := range fsd.Files {
				if file.shouldBeWrittenToDurableStorage() {
					fsd.flushFileToDurableStorage(file, false)
				}
			}

			fsd.mutex.Unlock()
		}
	}
}

func (fsd *TieredFileSystemDriver) WithLock(fn func()) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fn()
}

// Writing a file in the tiered file system driver involves writing the file on
// the local file system. Writing the file to durable storage will take place
// immmediately after.
func (fsd *TieredFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	var err error
	var file *TieredFile
	var ok bool

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	// If the file is already open, mark it as updated
	if file, ok = fsd.GetLocalFile(path); !ok {
		f, err := fsd.localFileSystemDriver.Create(path)

		if err != nil {
			return err
		}

		file = fsd.AddFile(path, f, os.O_CREATE|os.O_RDWR)
	} else {
		err = file.Truncate(0)
	}

	if err != nil {
		return err
	}

	_, err = file.Write(data)

	if err != nil {
		return err
	}

	file.MarkUpdated()

	fsd.flushFileToDurableStorage(fsd.Files[path], true)

	return nil
}
