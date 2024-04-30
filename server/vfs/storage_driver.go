package vfs

type StorageDriver interface {
	Delete(file string) error
	ReadAt(file string, offset int64, len int64) (data []byte, err error)
	WriteAt(file string, data []byte, offset int64) (n int, err error)
	Size(file string) (int64, error)
	Truncate(file string, size int64) error
}
