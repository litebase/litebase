package auth

import (
	"fmt"

	"github.com/litebase/litebase/pkg/config"
)

func GetDatabaseKeysPath(key string) string {
	return fmt.Sprintf("%s%s", Path(key), "DATABASE_KEYS")
}

func Path(key string) string {
	return config.EncryptionKeyHash(key) + "/"
}
