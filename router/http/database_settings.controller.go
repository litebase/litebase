package http

import (
	"litebasedb/router/auth"
	"litebasedb/router/node"
)

func DatabaseSettingsStoreController(request *Request) *Response {
	auth.SecretsManager().StoreDatabaseKey(
		request.Get("database_key").(string),
		request.Get("database_uuid").(string),
	)

	err := auth.SecretsManager().StoreDatabaseSettings(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("database_key").(string),
		request.Get("data").(string),
	)

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			},
		}
	}

	auth.SecretsManager().PurgeDatabaseSettings(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
	)

	node.PurgeDatabaseSettings(request.Get("database_uuid").(string))

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Database settings stored successfully",
		},
	}
}

func DatabaseSettingsDestroyController(request *Request) *Response {
	key, err := auth.SecretsManager().GetDatabaseKey(
		request.Param("databaseUuid"),
		request.Param("branchUuid"),
	)

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			},
		}
	}

	auth.SecretsManager().DeleteDatabaseKey(key)

	auth.SecretsManager().DeleteSettings(
		request.Param("databaseUuid"),
		request.Param("branchUuid"),
	)

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Database settings deleted successfully",
		},
	}
}
