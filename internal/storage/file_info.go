package storage

import (
	"io/fs"
	"time"
)

type FileInfo interface {
	Name() string
	Size() int64
	Mode() fs.FileMode
	ModTime() time.Time
	IsDir() bool
	Sys() interface{}
}
