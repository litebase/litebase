package storage

import (
	"context"
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// TODO: Limit the number of files that can be open at once. Should be configurable.

/*
Data in this driver is stored locally on disk then eventually pushed up to
durable storage with S3 compatability. This provides fast read access
performance with scalable and cost-effective long-term storage.
*/
type TieredFileSystemDriver struct {
	context                 context.Context
	Files                   map[string]*TieredFile
	localFileSystemDriver   FileSystemDriver
	mutex                   *sync.RWMutex
	durableFileSystemDriver FileSystemDriver
	watchTicker             *time.Ticker
}

const (
	TieredFileTTL = 1 * time.Hour
)

/*
Create a new instance of a tiered file system driver. This driver will manage
files that are stored on the local file system and durable file system.
*/
func NewTieredFileSystemDriver(context context.Context, localFileSystemDriver FileSystemDriver, durableFileSystemDriver FileSystemDriver) *TieredFileSystemDriver {
	fsd := &TieredFileSystemDriver{
		context:                 context,
		Files:                   map[string]*TieredFile{},
		localFileSystemDriver:   localFileSystemDriver,
		mutex:                   &sync.RWMutex{},
		durableFileSystemDriver: durableFileSystemDriver,
	}

	go fsd.watchForFileChanges()

	return fsd
}

/*
Creating a new file istantiates a new file durable that will be used to manage
the file on the local file system. When the file is closed, or written to, it
will be pushed to the durable file system.
*/
func (fsd *TieredFileSystemDriver) Create(path string) (internalStorage.File, error) {
	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.Files[path] = NewTieredFile(
		fsd,
		path,
		file,
		os.O_CREATE|os.O_RDWR,
	)

	fsd.Files[path].MarkUpdated()

	fsd.flushFileToDurableStorage(fsd.Files[path])

	return fsd.Files[path], nil
}

/*
Flushing a file to durable storage involves writing the file to the durable file
system. This operation is typically performed when the file has been updated
and has not been written to durable storage in the last minute.
*/
func (fsd *TieredFileSystemDriver) flushFileToDurableStorage(file *TieredFile) {
	if !file.shouldBeWrittenToDurableStorage() {
		return
	}

	file.File.Seek(0, io.SeekStart)

	// Perform the actual upload to durable storage here
	data, err := io.ReadAll(file.File)

	if err != nil {
		// Handle error (retry, log, etc.)
		log.Println("Error reading file from local storage", err)
		return
	}

	err = fsd.durableFileSystemDriver.WriteFile(file.Key, data, 0644)

	if err != nil {
		// Handle error (retry, log, etc.)
		log.Println("Error writing file to durable storage", err)
		return
	}

	// Update the last written time to indicate the file is synced
	file.WrittenAt = time.Now()
	if strings.Contains(file.Key, "_METADATA") {
		log.Println("File flushed to durable storage", file.Key)
	}
}

func (fsd *TieredFileSystemDriver) GetLocalFile(path string) (*TieredFile, bool) {
	if file, ok := fsd.Files[path]; ok {
		if file.Closed {
			fsd.releaseFile(path)
			return nil, false
		}

		// Do not return the file if it is stale
		if file.UpdatedAt != (time.Time{}) && file.UpdatedAt.Add(TieredFileTTL).Before(time.Now()) ||
			(file.UpdatedAt == (time.Time{}) && file.CreatedAt.Add(TieredFileTTL).Before(time.Now())) {
			fsd.releaseFile(path)

			return nil, false
		}

		return file, true
	}

	return nil, false
}

/*
Mkdir creates a new directory on the local file system. This has no effect on
the durable file system.
*/
func (fsd *TieredFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	err := fsd.localFileSystemDriver.Mkdir(path, perm)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.Mkdir(path, perm)
}

/*
MkdirAll creates a new directory on the local file system, along with any
parents directories. This has no effect on the durable file system.
*/
func (fsd *TieredFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	err := fsd.localFileSystemDriver.MkdirAll(path, perm)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.MkdirAll(path, perm)
}

/*
See OpenFile
*/
func (fsd *TieredFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return fsd.OpenFile(path, os.O_RDWR, 0)
}

/*
Opening a file in the tiered file system driver involves reading a file from
the durable file system. If the file does not exist on the durable file system,
this operation will create a new file on the local file system and then create
a new tiered file durable that will be used to manage the file.
*/
func (fsd *TieredFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	if file, ok := fsd.GetLocalFile(path); ok {
		// TODO: Compare the flags and permissions to ensure they match
		return file, nil
	}

	// To open a file, we need to first try and read the file from the durable storage
	data, err := fsd.durableFileSystemDriver.ReadFile(path)

	// If the file does not exist in durable storage, we will create a new file
	// on the local file system.
	if err != nil {
		if os.IsNotExist(err) && (flag&os.O_CREATE) != os.O_CREATE {
			log.Println("File does not exist in durable storage", path)
			return nil, err
		}
	}

	// Open the file on the local file system
	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		log.Println("Error opening file on local filesystem", err)

		return nil, err
	}

	// Write the file data to the local file system
	_, err = file.Write(data)

	if err != nil {
		log.Println("Error writing to local file", err)
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.Files[path] = NewTieredFile(
		fsd,
		path,
		file,
		flag,
	)

	return fsd.Files[path], nil
}

/*
Remove closed files from the driver. This operation is typically performed when
resources are being cleaned up.
*/
func (fsd *TieredFileSystemDriver) PurgeClosedFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for _, file := range fsd.Files {
		if file.Closed {
			delete(fsd.Files, file.Key)
		}
	}

	return nil
}

/*
Reading a directory only occurs on the durable file system. This is because the
local file system is only used for temporary storage and does not contain a
complete copy of the data. However, the file will be tracked in the driver
to keep track of its state for future use that may require the file.
*/
func (fsd *TieredFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := fsd.durableFileSystemDriver.ReadDir(path)

	if err != nil {
		return nil, err
	}

	dirEntries := make([]internalStorage.DirEntry, len(entries))

	for i, entry := range entries {
		dirEntries[i] = internalStorage.DirEntry{
			Name:  entry.Name,
			IsDir: entry.IsDir,
		}

		if !entry.IsDir {
			fsd.Files[entry.Name] = NewTieredFile(
				fsd,
				path+"/"+entry.Name,
				nil,
				os.O_RDONLY,
			)
		}
	}

	return dirEntries, nil
}

/*
Reading a file in the tiered file system driver involves reading the file from
the local file system. If the file does not exist on the local file system, the
operation will be attempted on the durable file system. If the file is found on
the durable file system, it will be copied to the local file system for future
use and an entry will be created in the driver to track the file.
*/
func (fsd *TieredFileSystemDriver) ReadFile(path string) ([]byte, error) {
	fsd.mutex.RLock()

	if file, ok := fsd.GetLocalFile(path); ok && file.File != nil {
		fsd.mutex.RUnlock()

		file.Seek(0, io.SeekStart)
		log.Println("Reading file from local storage", path)
		return io.ReadAll(file)
	}

	fsd.mutex.RUnlock()

	data, err := fsd.localFileSystemDriver.ReadFile(path)

	if err != nil {
		if os.IsNotExist(err) {
			data, err = fsd.durableFileSystemDriver.ReadFile(path)

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

			fsd.mutex.Lock()
			defer fsd.mutex.Unlock()

			fsd.Files[path] = NewTieredFile(
				fsd,
				path,
				file,
				os.O_RDONLY,
			)

			return data, nil
		}

		return nil, err
	}

	return data, nil
}

/*
Releasing a file involves closing the file and removing it from the driver. This
operation is typically performed when the file is no longer needed.
*/
func (fsd *TieredFileSystemDriver) releaseFile(path string) {
	if file, ok := fsd.Files[path]; ok {
		file.Close()
		delete(fsd.Files, path)
	}
}

/*
Removing a file included removing the file from the local file system and also
removing the file from the durable file system immediately after.
*/
func (fsd *TieredFileSystemDriver) Remove(path string) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(path); ok {
		file.Close()
		delete(fsd.Files, path)
	}

	err := fsd.localFileSystemDriver.Remove(path)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.durableFileSystemDriver.Remove(path)
}

/*
Removing all files from the tiered file system path involves removing all files
from the local file system and also removing all files from the durable file
system immediately after.
*/
func (fsd *TieredFileSystemDriver) RemoveAll(path string) error {
	// Remove any files that are under the path
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for key, file := range fsd.Files {
		if key == path || key[:len(path)] == path {
			file.Close()
			delete(fsd.Files, key)
		}
	}

	err := fsd.localFileSystemDriver.RemoveAll(path)

	if err != nil {
		return err
	}

	return fsd.durableFileSystemDriver.RemoveAll(path)
}

/*
Renaming a file in the tiered file system driver involves renaming the file on
the local file system and then renaming the file on the durable file system
immediately after.
*/
func (fsd *TieredFileSystemDriver) Rename(oldpath, newpath string) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.GetLocalFile(oldpath); ok {
		file.Close()
		delete(fsd.Files, oldpath)
		log.Println("DELETE")
	}

	err := fsd.localFileSystemDriver.Rename(oldpath, newpath)

	if err != nil && !os.IsNotExist(err) {
		log.Println("Error FOOOO file on local file system", err)
		return err
	}

	return fsd.durableFileSystemDriver.Rename(oldpath, newpath)
}

/*
Statting a file in the tiered file system driver involves statting the file on
the local file system and then returning the file information. If the file does
not exist on the local file system, the operation will be attempted on the
durable file system.
*/
func (fsd *TieredFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
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

/*
Truncating a file in the tiered file system driver involves truncating the file
on the local file system and then truncating the file on the durable file system
immediately after.
*/
func (fsd *TieredFileSystemDriver) Truncate(path string, size int64) error {
	err := fsd.localFileSystemDriver.Truncate(path, size)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return fsd.durableFileSystemDriver.Truncate(path, size)
}

/*
Watching for file changes involves periodically checking the state of all files
in the driver. If a file has been updated and has not been written to durable
storage in the last minute, the file will be written to durable storage. If a
file has been closed, the file will be released.
*/
func (fsd *TieredFileSystemDriver) watchForFileChanges() {
	fsd.watchTicker = time.NewTicker(1 * time.Second)

	for {
		select {
		case <-fsd.context.Done():
			return
		case <-fsd.watchTicker.C:
			fsd.mutex.Lock()

			for path, file := range fsd.Files {
				if file.shouldBeWrittenToDurableStorage() {
					fsd.flushFileToDurableStorage(file)
				} else if file.Closed {
					go func() {
						fsd.mutex.Lock()
						defer fsd.mutex.Unlock()

						fsd.releaseFile(path)
					}()
				}
			}

			fsd.mutex.Unlock()
		}
	}
}

/*
Writing a file in the tiered file system driver involves writing the file on
the local file system. Writing the file to durable storage will be deferred.
*/
func (fsd *TieredFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	err := fsd.localFileSystemDriver.WriteFile(path, data, perm)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if file, ok := fsd.GetLocalFile(path); ok {
		file.UpdatedAt = time.Now()
	} else {
		fsd.Files[path] = NewTieredFile(
			fsd,
			path,
			nil,
			os.O_RDWR,
		)
	}

	fsd.flushFileToDurableStorage(fsd.Files[path])

	return nil
}
