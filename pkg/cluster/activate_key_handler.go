package cluster

import (
	"errors"

	"github.com/litebase/litebase/pkg/config"
)

func ActivateKeyHandler(c *config.Config, data interface{}) error {
	if key, ok := data.(string); ok {
		c.EncryptionKey = key
	} else {
		return errors.New("encryption key is not a string")
	}

	return nil
}
