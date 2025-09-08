//go:build production

package storage

import "github.com/litebase/litebase/pkg/config"

func Init(
	c *config.Config,
	objectFS *FileSystem,
) {
}

func Shutdown(c *config.Config) {}
