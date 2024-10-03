package storage

import (
	"encoding/gob"
)

func init() {
	gob.Register(DistributedFileSystemRequest{})
	gob.Register(DistributedFileSystemResponse{})
	gob.Register(StaticFileInfo{})
}
