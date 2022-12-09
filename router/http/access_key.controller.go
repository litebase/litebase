package http

import (
	"litebasedb/router/auth"

	"github.com/gofiber/fiber/v2"
)

type AccessKeyController struct{}

func AccessKeyControllerStore(c *fiber.Ctx) error {
	var body map[string]interface{}

	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(map[string]string{
			"error": "Invalid JSON body.",
		})
	}

	if body["access_key_id"] == nil || body["data"] == nil {
		return c.Status(400).JSON(map[string]string{
			"status":  "error",
			"message": "Missing required parameters",
		})
	}
	auth.SecretsManager().StoreAccessKey(
		c.Params("databaseUuid"),
		c.Params("branchUuid"),
		body["access_key_id"].(string),
		body["data"].(string),
	)

	return c.JSON(map[string]string{
		"status":  "success",
		"message": "Access key stored successfully",
	})
}

func AccessKeyControllerDestroy(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
