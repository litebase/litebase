package http

import (
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"litebasedb/runtime/database"
)

type DatabaseSettingsController struct {
}

func (controller *DatabaseSettingsController) Store(request *Request) *Response {
	auth.SecretsManager().Init()

	auth.SecretsManager().StoreDatabaseSettings(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("database_key").(string),
		request.Get("data").(string),
	)

	auth.SecretsManager().PurgeDatabaseSettings(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
	)

	err := database.EnsureDatabaseExists(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database settings stored successfully",
	}, 200, nil)
}

func (controller *DatabaseSettingsController) Destroy(request *Request) *Response {
	if request.Param("database") != config.Get("database_uuid") {
		return JsonResponse(map[string]interface{}{
			"status": "error",
			"error":  "Database not found",
		}, 404, nil)
	}

	if request.Param("branch") != config.Get("branch_uuid") {
		return JsonResponse(map[string]interface{}{
			"status": "error",
			"error":  "Database not found",
		}, 404, nil)
	}

	auth.SecretsManager().Init()

	auth.SecretsManager().DeleteSettings(
		request.Param("database"),
		request.Param("branch"),
	)

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database settings deleted successfully",
	}, 200, nil)
}
