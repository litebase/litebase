package storage

import (
	"io/fs"
	"os"

	internalStorage "litebase/internal/storage"
)

type LocalFileSystemDriver struct{}

func NewLocalFileSystemDriver() *LocalFileSystemDriver {
	return &LocalFileSystemDriver{}
}

func (fs *LocalFileSystemDriver) Create(path string) (internalStorage.File, error) {
	return os.Create(path)
}

func (fs *LocalFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	return os.Mkdir(path, perm)
}

func (fs *LocalFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *LocalFileSystemDriver) Open(name string) (internalStorage.File, error) {
	return os.Open(name)
}

func (fs *LocalFileSystemDriver) OpenFile(name string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (fs *LocalFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
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

func (fs *LocalFileSystemDriver) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (fs *LocalFileSystemDriver) Remove(path string) error {
	return os.Remove(path)
}

func (fs *LocalFileSystemDriver) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *LocalFileSystemDriver) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (fs *LocalFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	info, err := os.Stat(path)

	if err != nil {
		return internalStorage.FileInfo{}, err
	}

	return internalStorage.FileInfo{
		Name:    info.Name(),
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
	}, err
}

func (fs *LocalFileSystemDriver) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

func (fs *LocalFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(path, data, perm)
}
