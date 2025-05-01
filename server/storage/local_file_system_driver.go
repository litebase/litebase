package storage

import (
	"bytes"
	"io/fs"
	"os"
	"strings"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

type LocalFileSystemDriver struct {
	basePath string
	buffers  sync.Pool
}

func NewLocalFileSystemDriver(basePath string) *LocalFileSystemDriver {
	return &LocalFileSystemDriver{
		basePath: basePath,
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
	}
}

func (fs *LocalFileSystemDriver) Create(path string) (internalStorage.File, error) {
	return os.Create(fs.Path(path))
}

func (fs *LocalFileSystemDriver) Flush() error {
	return nil
}

func (fs *LocalFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	return os.Mkdir(fs.Path(path), perm)
}

func (fs *LocalFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(fs.Path(path), perm)
}

func (fs *LocalFileSystemDriver) Open(path string) (internalStorage.File, error) {
	file, err := os.Open(fs.Path(path))

	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs *LocalFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	file, err := os.OpenFile(fs.Path(path), flag, perm)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs *LocalFileSystemDriver) Path(path string) string {
	var builder strings.Builder

	builder.Grow(len(fs.basePath) + 1 + len(path)) // Preallocate memory
	builder.WriteString(fs.basePath)
	builder.WriteString("/")
	builder.WriteString(path)

	return builder.String()
}

func (fs *LocalFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	entries, err := os.ReadDir(fs.Path(path))

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
	data, err := os.ReadFile(fs.Path(path))

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (fs *LocalFileSystemDriver) Remove(path string) error {
	return os.Remove(fs.Path(path))
}

func (fs *LocalFileSystemDriver) RemoveAll(path string) error {
	return os.RemoveAll(fs.Path(path))
}

func (fs *LocalFileSystemDriver) Rename(oldpath, newpath string) error {
	return os.Rename(fs.Path(oldpath), fs.Path(newpath))
}

func (fs *LocalFileSystemDriver) Shutdown() error {
	return nil
}

func (fs *LocalFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	info, err := os.Stat(fs.Path(path))

	if err != nil {
		return nil, err
	}

	return info, err
}

func (fs *LocalFileSystemDriver) Truncate(path string, size int64) error {
	return os.Truncate(fs.Path(path), size)
}

func (fs *LocalFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(fs.Path(path), data, perm)
}
