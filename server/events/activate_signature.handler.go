package events

import (
	"litebasedb/internal/config"
)

func ActivateSignatureHandler(data interface{}) {
	// TOOD: check if signature is valid by using a struct
	config.Get().Signature = data.(string)
}
