package http

import (
	"litebasedb/router/auth"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
)

func AdminAuth(c *fiber.Ctx) error {
	if !ensureRequestHasAnAuthorizationHeader(c) ||
		!ensureRequestIsProperlySigned(c) ||
		ensureRequestHasAValidToken(c) {
		return c.Status(fiber.StatusUnauthorized).JSON(map[string]string{
			"message": "Unauthorized",
		})
	}

	if !ensureRequestIsNotExpired(c) {
		return c.Status(fiber.StatusUnauthorized).JSON(map[string]string{
			"message": "Unauthorized",
		})
	}

	return c.Next()
}

/**
 *  Ensure that there is an authorization header
 */
func ensureRequestHasAnAuthorizationHeader(c *fiber.Ctx) bool {
	return c.GetReqHeaders()["Authorization"] != ""
}

func ensureRequestIsProperlySigned(c *fiber.Ctx) bool {
	return true
}

func ensureRequestHasAValidToken(c *fiber.Ctx) bool {
	return auth.AdminRequestTokenValidator(c)
}

func ensureRequestIsNotExpired(c *fiber.Ctx) bool {
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
