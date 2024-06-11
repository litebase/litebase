package storage

import (
	"io/fs"
	"litebase/internal/storage"
	"os"
	"sync"
)

// The FileSystem struct is used to abstract the underlying file system
// implementation. This allows us to use a local file and other remote file
// systems.
type FileSystem struct {
	driver FileSystemDriver
	mutex  *sync.Mutex
}

// The FileSystemDriver interface defines the methods that must be implemented
// by a file system driver.
type FileSystemDriver interface {
	Create(path string) (storage.File, error)
	Mkdir(path string, perm fs.FileMode) error
	MkdirAll(path string, perm fs.FileMode) error
	Open(path string) (storage.File, error)
	OpenFile(path string, flag int, perm fs.FileMode) (storage.File, error)
	ReadDir(path string) ([]os.DirEntry, error)
	ReadFile(path string) ([]byte, error)
	Remove(path string) error
	RemoveAll(path string) error
	Rename(oldpath, newPath string) error
	Stat(path string) (os.FileInfo, error)
	WriteFile(path string, data []byte, perm fs.FileMode) error
}

var fileSystem *FileSystem
var fileSystemMutex = &sync.Mutex{}

func NewFileSystem() *FileSystem {
	return &FileSystem{
		mutex:  &sync.Mutex{},
		driver: NewLocalFileSystemDriver(),
	}
}

func FS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if fileSystem == nil {
		fileSystem = NewFileSystem()
	}

	return fileSystem
}

func (fs *FileSystem) Create(path string) (storage.File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Create(path)
}

func (fs *FileSystem) Mkdir(path string, perm fs.FileMode) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Mkdir(path, perm)
}

func (fs *FileSystem) MkdirAll(path string, perm fs.FileMode) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.MkdirAll(path, perm)
}

func (fs *FileSystem) Open(path string) (storage.File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Open(path)
}

func (fs *FileSystem) OpenFile(path string, flag int, perm fs.FileMode) (storage.File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.OpenFile(path, flag, perm)
}

func (fs *FileSystem) ReadDir(path string) ([]os.DirEntry, error) {
	return fs.driver.ReadDir(path)
}

func (fs *FileSystem) ReadFile(path string) ([]byte, error) {
	return fs.driver.ReadFile(path)
}

func (fs *FileSystem) Remove(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Remove(path)
}

func (fs *FileSystem) RemoveAll(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.RemoveAll(path)
}

func (fs *FileSystem) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *FileSystem) Stat(path string) (os.FileInfo, error) {
	return fs.driver.Stat(path)
}

func (fs *FileSystem) WriteFile(path string, data []byte, perm fs.FileMode) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.WriteFile(path, data, perm)
}
