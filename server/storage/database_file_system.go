package storage

import internalStorage "litebase/internal/storage"

type DatabaseFileSystem interface {
	Close(path string) error
	Delete(path string) error
	Exists() bool
	Open(path string) (internalStorage.File, error)
	Path() string
	ReadAt(path string, offset, len int64) ([]byte, error)
	WithWriteHook(hook func(offset int64)) DatabaseFileSystem
	WriteAt(path string, data []byte, offset int64) (int, error)
	Size(path string) (int64, error)
	Truncate(path string, size int64) error
}
