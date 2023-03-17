package http

import (
	"litebasedb/app/auth"
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

	auth.SecretsManager().PurgeAccessKey(request.Get("access_key_id").(string))

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Access key stored successfully",
		},
	}
}

func AccessKeyControllerUpdate(request *Request) *Response {
	auth.SecretsManager().Init()

	updated := auth.SecretsManager().UpdateAccessKey(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("access_key_id").(string),
		request.Get("privileges"),
	)

	if !updated {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access could not be updated",
		}, 500, nil)
	}

	auth.SecretsManager().PurgeAccessKey(request.Get("access_key_id").(string))
	// TODO: Close connections using the access key. This needs to affect all functions.

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key updated successfully.",
	}, 200, nil)
}

func AccessKeyControllerDestroy(request *Request) *Response {
	auth.SecretsManager().DeleteAccessKey(request.Param("accessKeyId"))
	auth.SecretsManager().PurgeAccessKey(request.Param("accessKeyId"))

	return JsonResponse(map[string]interface{}{}, 200, nil)
}
