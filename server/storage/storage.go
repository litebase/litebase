package storage

import (
	"encoding/gob"
)

type StorageDiscoveryInterface interface {
	GetStorageNode(key string) (int, string, error)
}

type StorageEncryptionInterface interface {
	Encrypt(signature string, text string) ([]byte, error)
}

var StorageDiscovery StorageDiscoveryInterface
var StorageEncryption StorageEncryptionInterface
var NodeIPAddress string

func init() {
	gob.Register(DistributedFileSystemRequest{})
	gob.Register(DistributedFileSystemResponse{})
	gob.Register(StaticFileInfo{})
}

func SetDiscoveryProvider(provider StorageDiscoveryInterface) {
	StorageDiscovery = provider
}
