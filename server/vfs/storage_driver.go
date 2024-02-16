package vfs

type StorageDriver interface {
	ReadAt(offset int64) (data []byte, err error)
	WriteAt(data []byte, offset int64) (n int, err error)
	Size() (int64, error)
}
