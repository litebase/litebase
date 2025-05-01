package cluster

import (
	"errors"

	"github.com/litebase/litebase/common/config"
)

func ActivateSignatureHandler(c *config.Config, data interface{}) error {
	if signature, ok := data.(string); ok {
		c.Signature = signature
	} else {
		return errors.New("signature is not a string")
	}

	return nil
}
