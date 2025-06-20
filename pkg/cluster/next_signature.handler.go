package cluster

import (
	"github.com/litebase/litebase/pkg/config"
)

func NextKeyHandler(c *config.Config, data any) {
	// TOOD: check if key is valid by using a struct
	c.EncryptionKeyNext = data.(string)
}
