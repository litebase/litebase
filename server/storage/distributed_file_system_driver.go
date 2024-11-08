package storage

import (
	"container/list"
	"context"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	DistributedFileSystemMaxOpenFiles = 1000
)

// The DistributedFileSystemDriver allows the server to interact with a group of
// storage nodes that are responsible for storing and retrieving files. This allows
// the current server to operate without having to store files locally, and instead
// rely on the storage nodes to handle the file operations by making HTTP requests.
type DistributedFileSystemDriver struct {
	FileCount                int
	FileOrder                *list.List
	Files                    map[string]*DistributedFile
	localFileSystemDriver    *LocalFileSystemDriver
	MaxFilesOpened           int
	mutex                    *sync.Mutex
	storageConnectionManager *StorageConnectionManager
}

// Create a new instance of the DistributedFileSystemDriver.
func NewDistributedFileSystemDriver(
	context context.Context,
	localFileSystemDriver *LocalFileSystemDriver,
	storageConnectionManager *StorageConnectionManager,
) *DistributedFileSystemDriver {
	return &DistributedFileSystemDriver{
		FileCount:                0,
		FileOrder:                list.New(),
		Files:                    make(map[string]*DistributedFile),
		localFileSystemDriver:    localFileSystemDriver,
		MaxFilesOpened:           DistributedFileSystemMaxOpenFiles,
		mutex:                    &sync.Mutex{},
		storageConnectionManager: storageConnectionManager,
	}
}

func (fsd *DistributedFileSystemDriver) AddFile(
	path string,
	file internalStorage.File,
	flag int,
	perm fs.FileMode,
) *DistributedFile {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	if fsd.FileCount >= fsd.MaxFilesOpened {
		fsd.RemoveOldestFile()
	}

	// Check if the file already exists
	if _, ok := fsd.Files[path]; ok {
		return fsd.Files[path]
	}

	fsd.Files[path] = NewDistributedFile(
		fsd,
		path,
		file,
		flag,
		perm,
	)

	element := fsd.FileOrder.PushBack(fsd.Files[path])
	fsd.Files[path].Element = element
	fsd.FileCount++

	return fsd.Files[path]
}

func (fsd *DistributedFileSystemDriver) ClearFiles() {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	for path, file := range fsd.Files {
		fsd.ReleaseFile(file)
		delete(fsd.Files, path)
	}
}

// Create a new file at the specified path.
func (fsd *DistributedFileSystemDriver) Create(path string) (internalStorage.File, error) {
	fsd.mutex.Lock()
	defer fsd.mutex.Unlock()

	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: CreateStorageCommand,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	newFile := fsd.AddFile(path, nil, 0, 0)

	return newFile, nil
}

func (fsd *DistributedFileSystemDriver) GetLocalFile(path string) (*DistributedFile, bool) {
	if file, ok := fsd.Files[path]; ok {
		return file, true
	}

	return nil, false
}

// Create a new directory at the specified path with the specified permissions.
func (fsd *DistributedFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: MkdirStorageCommand,
		Path:    path,
		Perm:    perm,
	})

	if err != nil {
		return err
	}

	return nil
}

// Create a new directory and any necessary parents at the specified path with the
// specified permissions.
func (fsd *DistributedFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: MkdirAllStorageCommand,
		Path:    path,
		Perm:    perm,
	})

	if err != nil {
		return err
	}

	return nil
}

// Open the file at the specified path. This will return a DistributedFile instance
// that can be used to read and write to the file.
func (fsd *DistributedFileSystemDriver) Open(path string) (internalStorage.File, error) {
	response, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: OpenStorageCommand,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

tryOpen:
	file, err := fsd.localFileSystemDriver.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		if err != os.ErrNotExist {
			err = fsd.localFileSystemDriver.MkdirAll(filepath.Dir(path), 0755)

			if err != nil {
				log.Println("error", err)
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	file.Truncate(0)

	_, err = file.Write(response.Data)

	if err != nil {
		log.Println("error", err)
		return nil, err
	}

	newFile := fsd.AddFile(path, file, os.O_RDWR, 0644)

	return newFile, nil
}

// Open the file at the specified path with the specified flags and permissions.
// This will return a DistributedFile instance that can be used to read and write
// to the file.
func (fsd *DistributedFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	response, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: OpenFileStorageCommand,
		Flag:    flag,
		Path:    path,
		Perm:    perm,
	})

	if err != nil {
		return nil, err
	}

tryOpen:
	file, err := fsd.localFileSystemDriver.OpenFile(path, flag, perm)

	if err != nil {
		if err != os.ErrNotExist {
			err = fsd.localFileSystemDriver.MkdirAll(filepath.Dir(path), 0755)

			if err != nil {
				return nil, err
			}

			goto tryOpen
		}

		return nil, err
	}

	file.Truncate(0)

	_, err = file.Write(response.Data)

	if err != nil {
		return nil, err
	}

	newFile := fsd.AddFile(path, file, flag, perm)

	return newFile, nil
}

// Read the directory at the specified path and return a list of DirEntry instances
// representing the contents of the directory.
func (fsd *DistributedFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	response, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: ReadDirStorageCommand,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	return response.Entries, nil
}

// Read the file at the specified path and return the contents as a byte slice.
func (fsd *DistributedFileSystemDriver) ReadFile(path string) ([]byte, error) {
	response, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: ReadFileStorageCommand,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// Releasing a file involves closing the file and removing it from the driver. This
// operation is typically performed when the file is no longer needed.
func (fsd *DistributedFileSystemDriver) ReleaseFile(file *DistributedFile) {
	if file.File != nil {
		file.File.Close()
		err := fsd.localFileSystemDriver.Remove(file.Path)

		if err != nil {
			log.Println("Error removing file from local file system", err)
		}

		file.File = nil
	}

	if _, ok := fsd.Files[file.Path]; ok {
		delete(fsd.Files, file.Path)
		fsd.FileCount--
	}
}

// Remove the file at the specified path.
func (fsd *DistributedFileSystemDriver) Remove(path string) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: RemoveStorageCommand,
		Path:    path,
	})

	if err != nil {
		return err
	}

	return nil
}

func (fsd *DistributedFileSystemDriver) RemoveOldestFile() {
	element := fsd.FileOrder.Front()

	if element == nil {
		return
	}

	file, ok := element.Value.(*DistributedFile)

	if !ok {
		return
	}

	fsd.FileOrder.Remove(element)
	fsd.ReleaseFile(file)
}

// Remove the directory at the specified path and any children it contains.
func (fsd *DistributedFileSystemDriver) RemoveAll(path string) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: RemoveAllStorageCommand,
		Path:    path,
	})

	if err != nil {
		return err
	}

	return nil
}

// Rename the file at the old path to the new path.
func (fsd *DistributedFileSystemDriver) Rename(oldpath, newPath string) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: RenameStorageCommand,
		OldPath: oldpath,
		Path:    newPath,
	})

	if err != nil {
		return err
	}

	return nil
}

// Shutdown the driver and release any resources that are being used.
func (fsd *DistributedFileSystemDriver) Shutdown() error {
	fsd.ClearFiles()

	return nil
}

// Stat the file at the specified path and return information about the file.
func (fsd *DistributedFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	response, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: StatStorageCommand,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	return response.FileInfo, nil
}

// Truncate the file at the specified path to the specified size.
func (fsd *DistributedFileSystemDriver) Truncate(path string, size int64) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: TruncateStorageCommand,
		Path:    path,
	})

	if err != nil {
		return err
	}

	return nil
}

// Write the data to the file at the specified path with the specified permissions.
func (fsd *DistributedFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	_, err := fsd.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: WriteFileStorageCommand,
		Path:    path,
		Data:    data,
		Perm:    perm,
	})

	if err != nil {
		return err
	}

	if file, ok := fsd.GetLocalFile(path); !ok {
		fsd.ReleaseFile(file)
	}

	return nil
}
