package storage

import (
	"errors"
	"io/fs"
)

const (
	StorageCommandCreate    string = "create"
	StorageCommandMkdir     string = "mkdir"
	StorageCommandMkdirAll  string = "mkdir_all"
	StorageCommandOpen      string = "open"
	StorageCommandOpenFile  string = "open_file"
	StorageCommandReadDir   string = "read_dir"
	StorageCommandReadFile  string = "read_file"
	StorageCommandRemove    string = "remove"
	StorageCommandRemoveAll string = "remove_all"
	StorageCommandRename    string = "rename"
	StorageCommandStat             = "stat"
	StorageCommandTruncate         = "truncate"
	StorageCommandWriteFile        = "write_file"

	StorageCommandFileClose       = "file_close"
	StorageCommandFileRead        = "file_read"
	StorageCommandFileReadAt      = "file_read_at"
	StorageCommandFileSeek        = "file_seek"
	StorageCommandFileWrite       = "file_write"
	StorageCommandFileWriteAt     = "file_write_at"
	StorageCommandFileWriteString = "file_write_string"
)

var (
	ErrFileIsNotOpened = errors.New("file has not been opened")
	ErrInvalidCommand  = errors.New("invalid command")
)

type StorageConnection struct {
	Id  string
	Url string
}

type StorageRequest struct {
	Command string
	Data    []byte
	FileId  string
	Flag    int
	Id      string
	Offset  int64
	Path    string
	Page    int64
	Perm    fs.FileMode
	Size    int64
	Whence  int
}

type StorageResponse struct {
	Error      string
	Exists     bool
	Data       []byte
	DirEntries []DirEntry
	FileId     string
	FileInfo   FileInfo
	Id         string
	Length     int
	Offset     int64
}

func NewStorageResponse() StorageResponse {
	return StorageResponse{
		Exists: true,
	}
}
