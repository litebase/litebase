package http

import (
	"litebasedb/router/auth"
	"litebasedb/router/node"

	"github.com/gofiber/fiber/v2"
)

func DatabaseSettingsStoreController(c *fiber.Ctx) error {
	var body map[string]interface{}

	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(map[string]string{
			"error": "Invalid JSON body.",
		})
	}

	auth.SecretsManager().StoreDatabaseKey(
		body["database_key"].(string),
		body["database_uuid"].(string),
	)

	auth.SecretsManager().StoreDatabaseSettings(
		body["database_uuid"].(string),
		body["branch_uuid"].(string),
		body["database_key"].(string),
		body["data"].(string),
	)

	auth.SecretsManager().PurgeDatabaseSettings(
		body["database_uuid"].(string),
		body["branch_uuid"].(string),
	)

	node.PurgeDatabaseSettings(body["database_uuid"].(string))

	return c.JSON(map[string]string{
		"status":  "success",
		"message": "Database settings stored successfully",
	})
}

func DatabaseSettingsDestroyController(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
