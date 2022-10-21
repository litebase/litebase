package http

import (
	"litebasedb/runtime/app/auth"
)

type AccessKeyController struct {
}

func (controller *AccessKeyController) Store(request *Request) *Response {
	auth.SecretsManager().Init()

	auth.SecretsManager().StoreAccessKey(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("access_key_id").(string),
		request.Get("access_key_secret").(string),
		request.Get("server_access_key_secret").(string),
		request.Get("privileges").(map[string]interface{}),
	)

	auth.SecretsManager().PurgeAccessKey(request.Get("access_key_id").(string))

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key stored successfully.",
	}, 200, nil)
}

func (controller *AccessKeyController) Update(request *Request) *Response {
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

func (controller *AccessKeyController) Destroy(request *Request) *Response {
	auth.SecretsManager().Init()
	auth.SecretsManager().DeleteAccessKey(request.Param("accessKeyId"))
	auth.SecretsManager().PurgeAccessKey(request.Param("accessKeyId"))

	return JsonResponse(map[string]interface{}{}, 200, nil)
}
