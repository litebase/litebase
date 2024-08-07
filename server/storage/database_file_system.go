package storage

import internalStorage "litebase/internal/storage"

type DatabaseFileSystem interface {
	Close(path string) error
	Delete(path string) error
	Exists() bool
	Open(path string) (internalStorage.File, error)
	Path() string
	ReadAt(path string, data []byte, offset, len int64) (int, error)
	Size(path string) (int64, error)
	SetTransactionTimestamp(timestamp int64)
	TransactionTimestamp() int64
	Truncate(path string, size int64) error
	WalPath(path string) string
	WithTransactionTimestamp(timestamp int64) DatabaseFileSystem
	WithWriteHook(hook func(path string, offset int64, data []byte)) DatabaseFileSystem
	WriteAt(path string, data []byte, offset int64) (int, error)
}
