package auth

import (
	"fmt"
)

func GetDatabaseKeysPath(key string) string {
	return fmt.Sprintf("%s%s", Path(key), "DATABASE_KEYS")
}

func Path(key string) string {
	return EncryptionKeyHash(key) + "/"
}
