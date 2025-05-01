package auth

import (
	"fmt"

	"github.com/litebase/litebase/common/config"
)

func GetDatabaseKeyPath(signature, key string) string {
	return fmt.Sprintf("%s%s/%s", Path(signature), "database_keys", key)
}

func GetDatabaseKeysPath(signature string) string {
	return fmt.Sprintf("%s%s/", Path(signature), "database_keys")
}

func Path(signature string) string {
	return config.SignatureHash(signature) + "/"
}
