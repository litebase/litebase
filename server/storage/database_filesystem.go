package storage

import internalStorage "litebasedb/internal/storage"

type DatabaseFileSystem interface {
	Close(path string) error
	Delete(path string) error
	FetchPage(pageNumber int64) ([]byte, error)
	Open(path string) (internalStorage.File, error)
	PageCache() *PageCache
	PageSize() int64
	Path() string
	ReadAt(path string, offset, len int64) ([]byte, error)
	WriteAt(path string, data []byte, offset int64) (int, error)
	Size(path string) (int64, error)
	Truncate(path string, size int64) error
}
