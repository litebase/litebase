package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
)

func EncryptionKeyHash(encryptionKey string) string {
	hash := hmac.New(sha256.New, []byte(encryptionKey))
	hash.Write([]byte("lbdb_encryption_key"))

	return fmt.Sprintf("%x", hash.Sum(nil))
}
