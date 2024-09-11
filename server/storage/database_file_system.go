package storage

import internalStorage "litebase/internal/storage"

type DatabaseFileSystem interface {
	Delete(path string) error
	Exists() bool
	FileSystem() *FileSystem
	Open(path string) (internalStorage.File, error)
	Metadata() *DatabaseMetadata
	PageSize() int64
	Path() string
	ReadAt(path string, data []byte, offset, len int64) (int, error)
	Shutdown() error
	Size(path string) (int64, error)
	Truncate(path string, size int64) error
	WalPath(path string) string
	WithWriteHook(hook func(offset int64, data []byte)) DatabaseFileSystem
	WriteAt(path string, data []byte, offset int64) (int, error)
	WriteHook(offset int64, data []byte)
}
