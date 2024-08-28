package storage

import (
	"io/fs"
	internalStorage "litebase/internal/storage"
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
	Create(path string) (internalStorage.File, error)
	Mkdir(path string, perm fs.FileMode) error
	MkdirAll(path string, perm fs.FileMode) error
	Open(path string) (internalStorage.File, error)
	OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error)
	ReadDir(path string) ([]internalStorage.DirEntry, error)
	ReadFile(path string) ([]byte, error)
	Remove(path string) error
	RemoveAll(path string) error
	Rename(oldpath, newPath string) error
	Stat(path string) (internalStorage.FileInfo, error)
	Truncate(path string, size int64) error
	WriteFile(path string, data []byte, perm fs.FileMode) error
}

func NewFileSystem(fsd FileSystemDriver) *FileSystem {
	return &FileSystem{
		mutex:  &sync.Mutex{},
		driver: fsd,
	}
}

func (fs *FileSystem) Create(path string) (internalStorage.File, error) {
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

func (fs *FileSystem) Open(path string) (internalStorage.File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Open(path)
}

func (fs *FileSystem) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.OpenFile(path, flag, perm)
}

func (fs *FileSystem) ReadDir(path string) ([]internalStorage.DirEntry, error) {
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
	return fs.driver.Rename(oldpath, newpath)
}

func (fs *FileSystem) Stat(path string) (internalStorage.FileInfo, error) {
	return fs.driver.Stat(path)
}

func (fs *FileSystem) Truncate(path string, size int64) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.Truncate(path, size)
}

func (fs *FileSystem) WriteFile(path string, data []byte, perm fs.FileMode) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.driver.WriteFile(path, data, perm)
}
