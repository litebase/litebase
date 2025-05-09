package vfs

type WAL interface {
	IsLatestVersion(timestamp int64) bool
	ReadAt(timestamp int64, p []byte, off int64) (n int, err error)
	Size(timestamp int64) (int64, error)
	Sync(timestamp int64) error
	Truncate(timestamp, size int64) error
	WriteAt(timestamp int64, p []byte, off int64) (n int, err error)
}
