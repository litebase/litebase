package cluster

import (
	"litebase/internal/config"
)

func ActivateSignatureHandler(c *config.Config, data interface{}) {
	// TOOD: check if signature is valid by using a struct
	c.Signature = data.(string)
}
