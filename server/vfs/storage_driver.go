package vfs

type StorageDriver interface {
	Cache() StorageDriverCache
	ReadAt(offset int64) (data []byte, err error)
	WriteAt(data []byte, offset int64) (n int, err error)
	Size() (int64, error)
}

type StorageDriverCache interface {
	Get(key int64) (data []byte, err error)
	Put(key int64, data []byte) error
	Delete(key int64) error
	Flush() error
}
