package storage

import internalStorage "litebase/internal/storage"

type DatabaseFileSystem interface {
	FileSystem() *FileSystem
	Open(path string) (internalStorage.File, error)
	Metadata() *DatabaseMetadata
	PageSize() int64
	Path() string
	ReadAt(path string, data []byte, offset, len int64) (int, error)
	SetWriteHook(hook func(offset int64, data []byte)) DatabaseFileSystem
	Shutdown() error
	Size(path string) (int64, error)
	Truncate(path string, size int64) error
	WriteAt(path string, data []byte, offset int64) (int, error)
	WriteHook(offset int64, data []byte)
}
