package http

import "github.com/gofiber/fiber/v2"

func TrasactionControllerStore(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}

func TrasactionControllerUpdate(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}

func TrasactionControllerDestroy(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
