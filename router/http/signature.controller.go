package http

import "github.com/gofiber/fiber/v2"

func SingatureStoreController(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
