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
	return &Response{
		StatusCode: 200,
	}
}
