package storage

import (
	"io/fs"
)

type DirEntry struct {
	Name  string
	IsDir bool
	Type  fs.FileMode
}
