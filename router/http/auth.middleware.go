package http

import (
	"litebasedb/router/auth"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
)

func Auth(c *fiber.Ctx) error {
	if !ensureReuestHasAnAuthorizationHeader(c) || !ensureRequestCanAccessDatabase(c) {
		return c.Status(fiber.StatusUnauthorized).JSON(map[string]string{
			"message": "Unauthorized",
		})
	}

	if !ensureAuthRequestIsNotExpired(c) {
		return c.Status(fiber.StatusUnauthorized).JSON(map[string]string{
			"message": "Unauthorized",
		})
	}

	return c.Next()
}

func ensureRequestCanAccessDatabase(c *fiber.Ctx) bool {
	token := auth.CaptureRequestToken(c.GetReqHeaders()["Authorization"])

	if token == nil {
		return false
	}

	databaseKey := c.Subdomains()[0]

	return auth.SecretsManager().HasAccessKey(databaseKey, token.AccessKeyId)
}

func ensureReuestHasAnAuthorizationHeader(c *fiber.Ctx) bool {
	return c.GetReqHeaders()["Authorization"] != ""
}

func ensureAuthRequestIsNotExpired(c *fiber.Ctx) bool {
	dateHeader := c.GetReqHeaders()["X-Lbdb-Date"]

	if dateHeader == "" {
		return false
	}

	date, err := strconv.ParseInt(dateHeader, 10, 64)

	if err != nil {
		return false
	}

	return time.Since(time.Unix(date, 0)) < 10*time.Second
}
