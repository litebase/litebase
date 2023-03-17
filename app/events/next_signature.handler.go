package events

import (
	"litebasedb/internal/config"
)

func NextSignatureHandler(data interface{}) {
	// TOOD: check if signature is valid by using a struct
	config.Set("signature_next", data.(string))
}
