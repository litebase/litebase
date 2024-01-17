package vfs

type StorageDriver interface {
	ReadAt(data []byte, offset int64) (n int, err error)
	WriteAt(data []byte, offset int64) (n int, err error)
	Size() (int64, error)
}
