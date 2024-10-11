package cluster

import (
	"litebase/internal/config"
)

func NextSignatureHandler(data interface{}) {
	// TOOD: check if signature is valid by using a struct
	config.Get().SignatureNext = data.(string)
}
