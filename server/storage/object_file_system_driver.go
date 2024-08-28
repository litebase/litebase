package storage

import (
	"io/fs"
	"os"

	internalStorage "litebase/internal/storage"
)

type ObjectFileSystemDriver struct{}

func NewObjectFileSystemDriver() *ObjectFileSystemDriver {
	return &ObjectFileSystemDriver{}
}

func (fs *ObjectFileSystemDriver) Create(path string) (internalStorage.File, error) {
	return os.Create(path)
}

func (fs *ObjectFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	return os.Mkdir(path, perm)
}

func (fs *ObjectFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *ObjectFileSystemDriver) Open(name string) (internalStorage.File, error) {
	return os.Open(name)
}

func (fs *ObjectFileSystemDriver) OpenFile(name string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *ObjectFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := os.ReadDir(path)

	if err != nil {
		return nil, err
	}

	var dirEntries []internalStorage.DirEntry

	for _, entry := range entries {
		dirEntries = append(dirEntries, internalStorage.DirEntry{
			Name:  entry.Name(),
			IsDir: entry.IsDir(),
			Type:  entry.Type(),
		})
	}

	return dirEntries, nil
}

func (fs *ObjectFileSystemDriver) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (fs *ObjectFileSystemDriver) Remove(path string) error {
	return os.Remove(path)
}

func (fs *ObjectFileSystemDriver) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *ObjectFileSystemDriver) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *ObjectFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	info, err := os.Stat(path)

	if err != nil {
		return nil, err
	}

	return info, err
}

func (fs *ObjectFileSystemDriver) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

func (fs *ObjectFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(path, data, perm)
}
