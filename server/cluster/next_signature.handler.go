package cluster

import (
	"github.com/litebase/litebase/common/config"
)

func NextSignatureHandler(c *config.Config, data any) {
	// TOOD: check if signature is valid by using a struct
	c.SignatureNext = data.(string)
}
