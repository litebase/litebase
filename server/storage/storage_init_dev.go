//go:build !production
// +build !production

package storage

import (
	"litebase/internal/config"
)

type StorageEncryptionInterface interface {
	Encrypt(signature string, text string) (string, error)
}

var StorageEncryption StorageEncryptionInterface
var NodeIPAddress string

/*
Init initializes the storage package with the given IP address and encryption
implementation. If the storage mode is local, the function returns immediately.
If the storage mode is object and the environment is development or test, the
function starts a test S3 server.
*/
func Init(ipAddress string, encryption StorageEncryptionInterface) {
	NodeIPAddress = ipAddress
	StorageEncryption = encryption

	objectMode := config.Get().StorageObjectMode
	tieredMode := config.Get().StorageTieredMode

	if objectMode == config.STORAGE_MODE_LOCAL && tieredMode == config.STORAGE_MODE_LOCAL {
		return
	}

	if objectMode == config.STORAGE_MODE_OBJECT && (config.Get().Env == config.ENV_DEVELOPMENT || config.Get().Env == config.ENV_TEST) ||
		tieredMode == config.STORAGE_MODE_OBJECT && (config.Get().Env == config.ENV_DEVELOPMENT || config.Get().Env == config.ENV_TEST) {
		StartTestS3Server()
		return
	}
}

/*
Shutdown stops the test S3 server if it is running.
*/
func Shutdown() {
	objectMode := config.Get().StorageObjectMode
	tieredMode := config.Get().StorageTieredMode

	if objectMode == config.STORAGE_MODE_OBJECT && (config.Get().Env == config.ENV_DEVELOPMENT || config.Get().Env == config.ENV_TEST) ||
		tieredMode == config.STORAGE_MODE_OBJECT && (config.Get().Env == config.ENV_DEVELOPMENT || config.Get().Env == config.ENV_TEST) {
		StopTestS3Server()
	}
}
