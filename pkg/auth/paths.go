package auth

import (
	"fmt"

	"github.com/litebase/litebase/pkg/config"
)

func GetDatabaseKeysPath(signature string) string {
	return fmt.Sprintf("%s%s", Path(signature), "DATABASE_KEYS")
}

func Path(signature string) string {
	return config.SignatureHash(signature) + "/"
}
