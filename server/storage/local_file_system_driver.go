package storage

import (
	"fmt"
	"io/fs"
	"os"

	internalStorage "litebase/internal/storage"
)

type LocalFileSystemDriver struct {
	basePath string
}

func NewLocalFileSystemDriver(basePath string) *LocalFileSystemDriver {
	return &LocalFileSystemDriver{
		basePath: basePath,
	}
}

func (fs *LocalFileSystemDriver) Create(path string) (internalStorage.File, error) {
	return os.Create(fs.path(path))
}

func (fs *LocalFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	return os.Mkdir(fs.path(path), perm)
}

func (fs *LocalFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(fs.path(path), perm)
}

func (fs *LocalFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return os.Open(fs.path(path))
}

func (fs *LocalFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	return os.OpenFile(fs.path(path), flag, perm)
}

func (fs *LocalFileSystemDriver) path(path string) string {
	return fmt.Sprintf("%s/%s", fs.basePath, path)
}

func (fs *LocalFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := os.ReadDir(fs.path(path))

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
	return os.ReadFile(fs.path(path))
}

func (fs *LocalFileSystemDriver) Remove(path string) error {
	return os.Remove(fs.path(path))
}

func (fs *LocalFileSystemDriver) RemoveAll(path string) error {
	return os.RemoveAll(fs.path(path))
}

func (fs *LocalFileSystemDriver) Rename(oldpath, newpath string) error {
	return os.Rename(fs.path(oldpath), fs.path(oldpath))
}

func (fs *LocalFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	info, err := os.Stat(fs.path(path))

	if err != nil {
		return nil, err
	}

	return info, err
}

func (fs *LocalFileSystemDriver) Truncate(path string, size int64) error {
	return os.Truncate(fs.path(path), size)
}

func (fs *LocalFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(fs.path(path), data, perm)
}
