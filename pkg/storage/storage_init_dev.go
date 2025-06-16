//go:build !production
// +build !production

package storage

import (
	"github.com/litebase/litebase/pkg/config"
)

// Init initializes the storage package with the given IP address and encryption
// implementation. If the storage mode is local, the function returns immediately.
// If the storage mode is object and the environment is development or test, the
// function starts a test S3 server.
func Init(
	c *config.Config,
	objectFS *FileSystem,
) {
	objectMode := c.StorageObjectMode
	tieredMode := c.StorageTieredMode

	if objectMode == config.StorageModeLocal && tieredMode == config.StorageModeLocal {
		return
	}

	if objectMode == config.StorageModeObject && (c.Env == config.EnvTest) ||
		tieredMode == config.StorageModeObject && (c.Env == config.EnvTest) {
		StartTestS3Server(c, objectFS)

		return
	}
}

// Shutdown stops the test S3 server if it is running.
func Shutdown(c *config.Config) {
	objectMode := c.StorageObjectMode
	tieredMode := c.StorageTieredMode

	if objectMode == config.StorageModeObject && (c.Env == config.EnvTest) ||
		tieredMode == config.StorageModeObject && (c.Env == config.EnvTest) {
		StopTestS3Server()
	}
}
