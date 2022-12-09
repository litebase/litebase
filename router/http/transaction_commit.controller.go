package http

import "github.com/gofiber/fiber/v2"

func TransactionCommitController(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
