//go:build production
// +build production

package storage

func Init(
	ipAddress string,
	encryption StorageEncryptionInterface,
) {
	NodeIPAddress = ipAddress
	StorageEncryption = encryption
}

func Shutdown() {}
