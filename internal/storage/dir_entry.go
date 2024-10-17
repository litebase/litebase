package storage

import "io/fs"

/*
DirEntry represents a directory entry.

When encoded the entry will have the following binary format:

| Offset | Length | Description                                |
|--------|--------|--------------------------------------------|
| 0      | 1      | 1 if the entry is a directory, 0 otherwise |
| 1      | n      | The name of the entry                      |
*/
type DirEntry struct {
	name     string
	fileInfo fs.FileInfo
	isDir    bool
}

func NewDirEntry(name string, isDir bool, fileInfo fs.FileInfo) DirEntry {
	return DirEntry{
		fileInfo: fileInfo,
		isDir:    isDir,
		name:     name,
	}
}

func DecodeDirEntry(data []byte) DirEntry {
	return DirEntry{
		isDir: data[0] == 1,
		name:  string(data[1:]),
	}
}

func (de DirEntry) Encode() []byte {
	data := make([]byte, 1+len(de.name))

	if de.isDir {
		data[0] = 1
	} else {
		data[0] = 0
	}

	copy(data[1:], de.name)

	return data
}

func (de DirEntry) Info() fs.FileInfo {
	return de.fileInfo
}

func (de DirEntry) IsDir() bool {
	return de.isDir
}

func (de DirEntry) Name() string {
	return de.name
}
