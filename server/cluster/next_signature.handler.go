package cluster

import (
	"litebase/internal/config"
)

func NextSignatureHandler(c *config.Config, data interface{}) {
	// TOOD: check if signature is valid by using a struct
	c.SignatureNext = data.(string)
}
