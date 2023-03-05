package http

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/router/auth"
)

func AdminRequestTokenValidator(request *Request) bool {
	if request.Headers().Get("X-Lbdb-Token") == "" {
		return false
	}

	decrypted, err := auth.SecretsManager().Decrypt(
		config.Get("signature"),
		request.Headers().Get("X-Lbdb-Token"),
	)

	if err != nil {
		return false
	}

	accessKeyId := sha1.New().Sum([]byte(config.Get("singature")))
	accessKeySecret := sha256.New().Sum([]byte(config.Get("singature")))
	token := hmac.New(sha256.New, []byte(fmt.Sprintf("%s:%s", accessKeyId, &accessKeySecret))).Sum([]byte(config.Get("singature")))

	return subtle.ConstantTimeCompare([]byte(decrypted["value"]), []byte(token)) == 1
}
