package http

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
)

func AdminRequestTokenValidator(request *Request) bool {
	if request.Headers().Get("X-Lbdb-Token") == "" {
		return false
	}

	decrypted, err := request.cluster.Auth.SecretsManager.Decrypt(
		request.cluster.Config.Signature,
		[]byte(request.Headers().Get("X-Lbdb-Token")),
	)

	if err != nil {
		return false
	}

	accessKeyId := sha1.New().Sum([]byte(request.cluster.Config.Signature))
	accessKeySecret := sha256.New().Sum([]byte(request.cluster.Config.Signature))
	token := hmac.New(sha256.New, []byte(fmt.Sprintf("%s:%s", accessKeyId, &accessKeySecret))).Sum([]byte(request.cluster.Config.Signature))

	return subtle.ConstantTimeCompare([]byte(decrypted.Value), []byte(token)) == 1
}
