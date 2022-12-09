package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"litebasedb/router/config"

	"github.com/gofiber/fiber/v2"
)

func AdminRequestTokenValidator(c *fiber.Ctx) bool {
	if c.GetReqHeaders()["X-Lbdb-Token"] == "" {
		return false
	}

	decrypted, err := SecretsManager().Decrypt(
		config.Get("signature"),
		c.GetReqHeaders()["X-Lbdb-Token"],
	)

	if err != nil {
		return false
	}

	accessKeyId := sha1.New().Sum([]byte(config.Get("singature")))
	accessKeySecret := sha256.New().Sum([]byte(config.Get("singature")))
	token := hmac.New(sha256.New, []byte(fmt.Sprintf("%s:%s", accessKeyId, &accessKeySecret))).Sum([]byte(config.Get("singature")))

	return subtle.ConstantTimeCompare([]byte(decrypted["value"]), []byte(token)) == 1
}
