package storage

import (
	"io/fs"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// The FileSystemDriver interface defines the methods that must be implemented
// by a file system driver.
type FileSystemDriver interface {
	ClearFiles() error
	Create(path string) (internalStorage.File, error)
	Flush() error // Flush any buffered data to the underlying storage
	Mkdir(path string, perm fs.FileMode) error
	MkdirAll(path string, perm fs.FileMode) error
	Open(path string) (internalStorage.File, error)
	OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error)
	OpenFileDirect(path string, flag int, perm fs.FileMode) (internalStorage.File, error)
	Path(string) string
	ReadDir(path string) ([]internalStorage.DirEntry, error)
	ReadFile(path string) ([]byte, error)
	Remove(path string) error
	RemoveAll(path string) error
	Rename(oldpath, newPath string) error
	Shutdown() error
	Stat(path string) (internalStorage.FileInfo, error)
	Truncate(path string, size int64) error
	WriteFile(path string, data []byte, perm fs.FileMode) error
}

// The FileSystem struct is used to abstract the underlying file system
// implementation. This allows us to use a local file and other remote file
// systems.
type FileSystem struct {
	driver FileSystemDriver
	lock   *FileSystemLock
	mutex  sync.Mutex
}

func NewFileSystem(fsd FileSystemDriver) *FileSystem {
	return &FileSystem{
		driver: fsd,
		lock:   NewFileSystemLock(),
		mutex:  sync.Mutex{},
	}
}

func (fs *FileSystem) ClearFiles() error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	accessLocks := fs.lock.acquireAccessLocks("/")
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock("/")
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.ClearFiles()
}

func (fs *FileSystem) Create(path string) (internalStorage.File, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.Create(path)
}

func (fs *FileSystem) Driver() FileSystemDriver {
	return fs.driver
}

func (fs *FileSystem) Flush() error {
	deleteLocks := fs.lock.acquireDeleteLocks("/")
	defer fs.lock.releaseDeleteLocks(deleteLocks)

	return fs.driver.Flush()
}

func (fs *FileSystem) Mkdir(path string, perm fs.FileMode) error {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	return fs.driver.Mkdir(path, perm)
}

func (fs *FileSystem) MkdirAll(path string, perm fs.FileMode) error {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	return fs.driver.MkdirAll(path, perm)
}

func (fs *FileSystem) Open(path string) (internalStorage.File, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.Open(path)
}

func (fs *FileSystem) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.OpenFile(path, flag, perm)
}

func (fs *FileSystem) OpenFileDirect(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.OpenFileDirect(path, flag, perm)
}

func (fs *FileSystem) Path(path string) string {
	return fs.driver.Path(path)
}

func (fs *FileSystem) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	return fs.driver.ReadDir(path)
}

func (fs *FileSystem) ReadFile(path string) ([]byte, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathReadLock(path)
	defer fs.lock.releasePathReadLock(pathLock)

	return fs.driver.ReadFile(path)
}

func (fs *FileSystem) Remove(path string) error {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.Remove(path)
}

func (fs *FileSystem) RemoveAll(path string) error {
	accessLocks := fs.lock.acquireDeleteLocks(path)
	defer fs.lock.releaseDeleteLocks(accessLocks)

	return fs.driver.RemoveAll(path)
}

func (fs *FileSystem) Rename(oldpath, newpath string) error {
	accessLocks := fs.lock.acquireAccessLocks(oldpath)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(oldpath)
	defer fs.lock.releasePathWriteLock(pathLock)

	pathLock = fs.lock.acquirePathWriteLock(newpath)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.Rename(oldpath, newpath)
}

func (fs *FileSystem) Shutdown() error {
	accessLocks := fs.lock.acquireAccessLocks("/")
	defer fs.lock.releaseAccessLocks(accessLocks)

	return fs.driver.Shutdown()
}

func (fs *FileSystem) Stat(path string) (internalStorage.FileInfo, error) {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathReadLock(path)
	defer fs.lock.releasePathReadLock(pathLock)

	return fs.driver.Stat(path)
}

func (fs *FileSystem) Truncate(path string, size int64) error {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathReadLock(path)
	defer fs.lock.releasePathReadLock(pathLock)

	return fs.driver.Truncate(path, size)
}

func (fs *FileSystem) WriteFile(path string, data []byte, perm fs.FileMode) error {
	accessLocks := fs.lock.acquireAccessLocks(path)
	defer fs.lock.releaseAccessLocks(accessLocks)

	pathLock := fs.lock.acquirePathWriteLock(path)
	defer fs.lock.releasePathWriteLock(pathLock)

	return fs.driver.WriteFile(path, data, perm)
}
