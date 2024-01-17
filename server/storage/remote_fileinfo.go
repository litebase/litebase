package storage

import (
	"io/fs"
	"time"
)

type RemoteFileInfo struct {
	isDir   bool
	name    string
	mode    int32
	modTime time.Time
	size    int64
}

func (fi *RemoteFileInfo) Name() string {
	return fi.name
}

func (fi *RemoteFileInfo) Size() int64 {
	return fi.size
}

func (fi *RemoteFileInfo) Mode() fs.FileMode {
	return fs.FileMode(fi.mode)
}

func (fi *RemoteFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *RemoteFileInfo) IsDir() bool {
	return fi.isDir
}

func (fi *RemoteFileInfo) Sys() interface{} {
	return nil
}
