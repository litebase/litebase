package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
)

func SignatureHash(signature string) string {
	hash := hmac.New(sha256.New, []byte(signature))
	hash.Write([]byte("lbdb_signature"))

	return fmt.Sprintf("%x", hash.Sum(nil))
}
