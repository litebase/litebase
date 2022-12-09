package http

import "github.com/gofiber/fiber/v2"

func RuntimeAuth(c *fiber.Ctx) error {
	return c.Next()
}
