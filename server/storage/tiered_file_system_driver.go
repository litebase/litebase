package storage

import (
	"context"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"sync"
	"time"
)

/*
Data in this driver is stored locally on disk then eventually pushed up to
object storage with S3 compatability. This provides fast read access
performance with scalable and cost-effective long-term storage.
*/
type TieredFileSystemDriver struct {
	context                context.Context
	files                  map[string]*TieredFile
	localFileSystemDriver  *LocalFileSystemDriver
	mutex                  *sync.Mutex
	objectFileSystemDriver *ObjectFileSystemDriver
	watchTicker            *time.Ticker
}

/*
Create a new instance of a tiered file system driver. This driver will manage
files that are stored on the local file system and object file system.
*/
func NewTieredFileSystemDriver(context context.Context, localFileSystemDriver *LocalFileSystemDriver, objectFileSystemDriver *ObjectFileSystemDriver) *TieredFileSystemDriver {
	fsd := &TieredFileSystemDriver{
		context:                context,
		files:                  map[string]*TieredFile{},
		localFileSystemDriver:  localFileSystemDriver,
		mutex:                  &sync.Mutex{},
		objectFileSystemDriver: objectFileSystemDriver,
	}

	go fsd.watchForFileChanges()

	return fsd
}

/*
Creating a new file istantiates a new file object that will be used to manage
the file on the local file system. When the file is closed, or written to, it
will be pushed to the object file system.
*/
func (fsd *TieredFileSystemDriver) Create(path string) (internalStorage.File, error) {
	file, err := fsd.localFileSystemDriver.Create(path)

	if err != nil {
		return nil, err
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.files[path] = NewTieredFile(
		fsd,
		path,
		file,
	)

	return fsd.files[path], nil
}

/*
Flushing a file to object storage involves writing the file to the object file
system. This operation is typically performed when the file has been updated
and has not been written to object storage in the last minute.
*/
func (fsd *TieredFileSystemDriver) flushFileToObjectStorage(file *TieredFile) {
	if !file.shouldBeWrittenToObjectStorage() {
		return
	}

	// Perform the actual upload to object storage here
	data, err := fsd.localFileSystemDriver.ReadFile(file.key)

	if err != nil {
		// Handle error (retry, log, etc.)
		log.Println("Error reading file from local storage", err)
		return
	}

	err = fsd.objectFileSystemDriver.WriteFile(file.key, data, 0644)

	if err != nil {
		// Handle error (retry, log, etc.)
		log.Println("Error writing file to object storage", err)
		return
	}

	// Update the last written time to indicate the file is synced
	file.writtenAt = time.Now()
}

func (fsd *TieredFileSystemDriver) getFile(path string) (*TieredFile, bool) {
	if file, ok := fsd.files[path]; ok {
		if file.closed {
			fsd.releaseFile(path)
			return nil, false
		}

		return file, true
	}

	return nil, false
}

/*
Mkdir creates a new directory on the local file system. This has no effect on
the object file system.
*/
func (fsd *TieredFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	return fsd.localFileSystemDriver.Mkdir(path, perm)
}

/*
MkdirAll creates a new directory on the local file system, along with any
parents directories. This has no effect on the object file system.
*/
func (fsd *TieredFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	return fsd.localFileSystemDriver.MkdirAll(path, perm)
}

/*
See OpenFile
*/
func (fsd *TieredFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return fsd.OpenFile(path, os.O_RDWR, 0)
}

/*
Opening a file in the tiered file system driver involves reading a file from
the object file system. If the file does not exist on the object file system,
this operation will create a new file on the local file system and then create
a new tiered file object that will be used to manage the file.
*/
func (fsd *TieredFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	if file, ok := fsd.getFile(path); ok {
		return file, nil
	}

	file, err := fsd.localFileSystemDriver.OpenFile(path, flag, perm)

	if err != nil {
		log.Println("Error opening file", err)

		if !os.IsNotExist(err) {
			return nil, err
		}

		log.Println("File does not exist on local file system PULLING FROM OBJECT", path)
		// Pull the file from object storage
		data, err := fsd.objectFileSystemDriver.ReadFile(path)

		if err != nil {
			return nil, err
		}

		file, err = fsd.localFileSystemDriver.OpenFile(path, flag, perm)

		if err != nil {
			log.Println("Error opening file", err)
			return nil, err
		}

		_, err = file.Write(data)

		if err != nil {
			log.Println("Error writing file", err)
			return nil, err
		}
	}

	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	fsd.files[path] = NewTieredFile(
		fsd,
		path,
		file,
	)

	return fsd.files[path], nil
}

/*
Remove closed files from the driver. This operation is typically performed when
resources are being cleaned up.
*/
func (fsd *TieredFileSystemDriver) PurgeClosedFiles() error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for _, file := range fsd.files {
		if file.closed {
			delete(fsd.files, file.key)
		}
	}

	return nil
}

/*
Reading a directory only occurs on the object file system. This is because the
local file system is only used for temporary storage and does not contain a
complete copy of the data. However, the file will be tracked in the driver
to keep track of its state for future use that may require the file.
*/
func (fsd *TieredFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := fsd.localFileSystemDriver.ReadDir(path)

	if err != nil {
		return nil, err
	}

	var dirEntries []internalStorage.DirEntry

	for _, entry := range entries {
		dirEntries = append(dirEntries, internalStorage.DirEntry{
			Name:  entry.Name,
			IsDir: entry.IsDir,
		})

		if !entry.IsDir {
			fsd.files[entry.Name] = NewTieredFile(
				fsd,
				path+"/"+entry.Name,
				nil,
			)
		}
	}

	return dirEntries, nil
}

/*
Reading a file in the tiered file system driver involves reading the file from
the local file system. If the file does not exist on the local file system, the
operation will be attempted on the object file system. If the file is found on
the object file system, it will be copied to the local file system for future
use and an entry will be created in the driver to track the file.
*/
func (fsd *TieredFileSystemDriver) ReadFile(path string) ([]byte, error) {
	data, err := fsd.localFileSystemDriver.ReadFile(path)

	if err != nil {
		if os.IsNotExist(err) {
			data, err = fsd.objectFileSystemDriver.ReadFile(path)

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

			fsd.files[path] = NewTieredFile(
				fsd,
				path,
				file,
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
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.files[path]; ok {
		file.Close()
		delete(fsd.files, path)
	}
}

/*
Removing a file included removing the file from the local file system and also
removing the file from the object file system immediately after.
*/
func (fsd *TieredFileSystemDriver) Remove(path string) error {
	// Remove the path from the local file system
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if file, ok := fsd.getFile(path); ok {
		file.Close()
		delete(fsd.files, path)
	}

	err := fsd.localFileSystemDriver.Remove(path)

	if err != nil {
		return err
	}

	return fsd.objectFileSystemDriver.Remove(path)
}

/*
Removing all files from the tiered file system path involves removing all files
from the local file system and then removing all files from the object file
system immediately after.
*/
func (fsd *TieredFileSystemDriver) RemoveAll(path string) error {
	// Remove any files that are under the path
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for key, file := range fsd.files {
		if key == path || key == path+"/" {
			file.Close()
			delete(fsd.files, key)
		}
	}

	err := fsd.localFileSystemDriver.RemoveAll(path)

	if err != nil {
		return err
	}

	return fsd.objectFileSystemDriver.RemoveAll(path)
}

/*
Renaming a file in the tiered file system driver involves renaming the file on
the local file system and then renaming the file on the object file system
immediately after.
*/
func (fsd *TieredFileSystemDriver) Rename(oldpath, newpath string) error {
	err := fsd.localFileSystemDriver.Rename(oldpath, oldpath)

	if err != nil {
		return err
	}

	return fsd.objectFileSystemDriver.Rename(oldpath, newpath)
}

/*
Statting a file in the tiered file system driver involves statting the file on
the local file system and then returning the file information. If the file does
not exist on the local file system, the operation will be attempted on the
object file system.
*/
func (fsd *TieredFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	info, err := fsd.localFileSystemDriver.Stat(path)

	if err != nil {
		if os.IsNotExist(err) {
			info, err = fsd.objectFileSystemDriver.Stat(path)

			if err != nil {
				return nil, err
			}

			return info, nil
		}
	}

	return info, err
}

/*
Truncating a file in the tiered file system driver involves truncating the file
on the local file system and then truncating the file on the object file system
immediately after.
*/
func (fsd *TieredFileSystemDriver) Truncate(path string, size int64) error {
	err := fsd.localFileSystemDriver.Truncate(path, size)

	if err != nil {
		return err
	}

	return fsd.objectFileSystemDriver.Truncate(path, size)
}

/*
Watching for file changes involves periodically checking the state of all files
in the driver. If a file has been updated and has not been written to object
storage in the last minute, the file will be written to object storage. If a
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

			for path, file := range fsd.files {
				if file.shouldBeWrittenToObjectStorage() {
					// log.Println("Flushing file to object storage", path)
					go fsd.flushFileToObjectStorage(file)
				} else if file.closed {
					// log.Println("Releasing file", path)
					go fsd.releaseFile(path)
				}
			}

			fsd.mutex.Unlock()
		}
	}
}

/*
Writing a file in the tiered file system driver involves writing the file on
the local file system. Writing the file to object storage will be deferred.
*/
func (fsd *TieredFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	err := fsd.localFileSystemDriver.WriteFile(path, data, perm)

	if err != nil {
		return err
	}

	if file, ok := fsd.getFile(path); ok {
		file.updatedAt = time.Now()
	} else {
		fsd.files[path] = NewTieredFile(
			fsd,
			path,
			nil,
		)
	}

	go fsd.flushFileToObjectStorage(fsd.files[path])

	return nil
}
