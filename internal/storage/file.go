package storage

import (
	"io"
	"io/fs"
)

type File interface {
	Close() error
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Seek(offset int64, whence int) (int64, error)
	Stat() (fs.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Write(p []byte) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	WriteTo(w io.Writer) (n int64, err error)
	WriteString(s string) (ret int, err error)
}
