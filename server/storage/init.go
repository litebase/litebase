package storage

import (
	"encoding/gob"
	"litebase/internal/storage"
)

var StorageInit = false

func Init() {
	// StorageInit = true

	gob.Register(storage.StorageConnection{})
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})
}
