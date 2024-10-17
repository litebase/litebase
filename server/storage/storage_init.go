//go:build production
// +build production

package storage

import "litebase/internal/config"

func Init(
	c *config.Config,
	objectFS *FileSystem,
	ipAddress string,
	encryption StorageEncryptionInterface,
) {
	NodeIPAddress = ipAddress
	StorageEncryption = encryption
}

func Shutdown() {}
