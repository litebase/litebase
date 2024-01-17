package storage

type File interface {
	Close() error
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Write(p []byte) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	WriteString(s string) (ret int, err error)
}
