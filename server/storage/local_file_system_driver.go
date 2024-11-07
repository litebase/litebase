package storage

import (
	"bytes"
	"io/fs"
	"log"
	"os"
	"strings"
	"sync"

	internalStorage "litebase/internal/storage"
)

type LocalFileSystemDriver struct {
	basePath string
	buffers  sync.Pool
}

func NewLocalFileSystemDriver(basePath string) *LocalFileSystemDriver {
	return &LocalFileSystemDriver{
		basePath: basePath,
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
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
	file, err := os.Open(fs.path(path))
	log.Println(file, err)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs *LocalFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	file, err := os.OpenFile(fs.path(path), flag, perm)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs *LocalFileSystemDriver) path(path string) string {
	var builder strings.Builder

	builder.Grow(len(fs.basePath) + 1 + len(path)) // Preallocate memory
	builder.WriteString(fs.basePath)
	builder.WriteString("/")
	builder.WriteString(path)

	return builder.String()
}

func (fs *LocalFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := os.ReadDir(fs.path(path))

	if err != nil {
		return nil, err
	}

	var dirEntries []internalStorage.DirEntry

	for _, entry := range entries {
		info, err := entry.Info()

		if err != nil {
			return nil, err
		}

		dirEntries = append(dirEntries, internalStorage.NewDirEntry(
			entry.Name(),
			entry.IsDir(),
			NewStaticFileInfo(
				entry.Name(),
				info.Size(),
				info.ModTime(),
			),
		))
	}

	return dirEntries, nil
}

func (fs *LocalFileSystemDriver) ReadFile(path string) ([]byte, error) {
	data, err := os.ReadFile(fs.path(path))

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (fs *LocalFileSystemDriver) Remove(path string) error {
	return os.Remove(fs.path(path))
}

func (fs *LocalFileSystemDriver) RemoveAll(path string) error {
	return os.RemoveAll(fs.path(path))
}

func (fs *LocalFileSystemDriver) Rename(oldpath, newpath string) error {
	return os.Rename(fs.path(oldpath), fs.path(newpath))
}

func (fs *LocalFileSystemDriver) Shutdown() error {
	return nil
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
