package http

import (
	"github.com/gofiber/fiber/v2"
)

func RequireHost(host string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Hostname() != host {
			return c.Status(fiber.StatusForbidden).SendString("Forbidden")
		}

		return c.Next()
	}
}
