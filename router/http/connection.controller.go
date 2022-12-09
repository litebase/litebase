package http

import "github.com/gofiber/fiber/v2"

func ConnectionController(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
