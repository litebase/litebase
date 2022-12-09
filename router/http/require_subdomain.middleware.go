package http

import (
	"github.com/gofiber/fiber/v2"
)

func RequireSubdomain(c *fiber.Ctx) error {
	if len(c.Subdomains()) <= 1 {
		return c.SendStatus(fiber.StatusNotFound)
	}

	return c.Next()
}
