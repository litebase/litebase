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

func (fs *LocalFileSystemDriver) Create(name string) (internalStorage.File, error) {
	return os.Create(name)
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

func (fs *LocalFileSystemDriver) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
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

func (fs *LocalFileSystemDriver) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *LocalFileSystemDriver) WriteFile(name string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(name, data, perm)
}
