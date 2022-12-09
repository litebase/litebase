package http

import (
	"litebasedb/router/auth"
	"litebasedb/router/runtime"

	"github.com/gofiber/fiber/v2"
)

func QueryController(c *fiber.Ctx) error {
	databaseKey := c.Subdomains()[0]

	if databaseKey == "" || len(c.Subdomains()) != 2 {
		return c.JSON(map[string]string{
			"status":  "error",
			"message": "Bad request",
		})
	}

	databaseUuid := auth.SecretsManager().GetDatabaseUuid(databaseKey)

	if databaseUuid == "" {
		return c.JSON(map[string]string{
			"status":  "error",
			"message": "Bad request",
		})
	}

	accessKey := auth.CaptureRequestToken(c.GetReqHeaders()["Authorization"]).AccessKey(databaseUuid)

	if accessKey == nil {
		return c.JSON(map[string]string{
			"status":  "error",
			"message": "Bad request",
		})
	}

	branchUuid := accessKey.GetBranchUuid()

	if branchUuid == "" {
		return c.JSON(map[string]string{
			"status":  "error",
			"message": "Bad request",
		})
	}

	query := make(map[string]string)
	c.QueryParser(query)

	response := runtime.ForwardRequest(
		c,
		databaseUuid,
		branchUuid,
		accessKey.AccessKeyId,
		"",
	)

	return c.Status(response.StatusCode).JSON(response.Body)
}
