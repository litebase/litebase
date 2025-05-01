//go:build production
// +build production

package storage

import "github.com/litebase/litebase/common/config"

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
