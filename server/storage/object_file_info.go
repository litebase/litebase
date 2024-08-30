package storage

import (
	"io/fs"
	"strings"
	"time"
)

type ObjectFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi *ObjectFileInfo) IsDir() bool {
	// Check if name ends with a slash
	return strings.HasSuffix(fi.name, "/")
}

func (fi *ObjectFileInfo) Name() string {
	return fi.name
}

func (fi *ObjectFileInfo) Size() int64 {
	return fi.size
}

func (fi *ObjectFileInfo) Mode() fs.FileMode {
	return 0
}

func (fi *ObjectFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *ObjectFileInfo) Sys() interface{} {
	return nil
}
