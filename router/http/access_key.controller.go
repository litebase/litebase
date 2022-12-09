package http

import (
	"litebasedb/router/auth"
)

type AccessKeyController struct{}

func AccessKeyControllerStore(request *Request) *Response {

	if request.Get("access_key_id") == nil || request.Get("data") == nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Missing required parameters",
			},
		}
	}
	auth.SecretsManager().StoreAccessKey(
		request.Param("databaseUuid"),
		request.Param("branchUuid"),
		request.Get("access_key_id").(string),
		request.Get("data").(string),
	)

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Access key stored successfully",
		},
	}
}

func AccessKeyControllerDestroy(request *Request) *Response {
	return &Response{
		StatusCode: 200,
	}
}
