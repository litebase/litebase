package http

import "github.com/gofiber/fiber/v2"

func Internal(c *fiber.Ctx) error {
	return c.Next()
}
