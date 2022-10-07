package http

import "litebasedb/runtime/app/secrets"

type AccessKeyController struct {
}

func (controller *AccessKeyController) Store(request *Request) *Response {
	secrets.Manager().Init()

	secrets.Manager().StoreAccessKey(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("access_key_id").(string),
		request.Get("access_key_secret").(string),
		request.Get("server_access_key_secret").(string),
		request.Get("privileges").(map[string][]string),
	)

	secrets.Manager().PurgeAccessKey(request.Get("access_key_id").(string))

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key stored successfully.",
	}, 200, nil)
}

func (controller *AccessKeyController) Update(request *Request) *Response {
	secrets.Manager().Init()

	secrets.Manager().UpdateAccessKey(
		request.Get("database_uuid").(string),
		request.Get("branch_uuid").(string),
		request.Get("access_key_id").(string),
		request.Get("privileges").(map[string][]string),
	)

	secrets.Manager().PurgeAccessKey(request.Get("access_key_id").(string))
	// TODO: Close connections using the access key. This needs to affect all functions.

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key updated successfully.",
	}, 200, nil)
}

func (controller *AccessKeyController) Destroy(request *Request) *Response {
	secrets.Manager().Init()
	secrets.Manager().DeleteAccessKey(request.Get("access_key_id").(string))
	secrets.Manager().PurgeAccessKey(request.Get("access_key_id").(string))

	return JsonResponse(map[string]interface{}{}, 200, nil)
}
