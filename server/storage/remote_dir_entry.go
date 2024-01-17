package storage

import (
	"io/fs"
	"time"
)

type RemoteDirEntry struct {
	fileInfo map[string]interface{}
	isDir    bool
	mode     int32
	name     string
}

func (entry *RemoteDirEntry) Info() (fs.FileInfo, error) {
	return &RemoteFileInfo{
		isDir:   entry.isDir,
		modTime: entry.fileInfo["modTime"].(time.Time),
		mode:    entry.fileInfo["mode"].(int32),
		name:    entry.name,
		size:    entry.fileInfo["size"].(int64),
	}, nil
}

func (entry *RemoteDirEntry) IsDir() bool {
	return entry.isDir
}

func (entry *RemoteDirEntry) Name() string {
	return entry.name
}

func (entry *RemoteDirEntry) Type() fs.FileMode {
	return fs.FileMode(entry.mode)
}
