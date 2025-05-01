package http

import (
	"fmt"
	"log"

	"github.com/litebase/litebase/server/auth"
)

func AccessKeyControllerIndex(request *Request) Response {
	accessKeysIds, err := request.accessKeyManager.AllAccessKeyIds()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access keys could not be retrieved",
		}, 500, nil)
	}

	accessKeys := []map[string]any{}

	for _, accessKeyId := range accessKeysIds {
		accessKeys = append(accessKeys, map[string]any{
			"access_key_id": accessKeyId,
		})
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access keys retrieved successfully",
		"data":    accessKeys,
	}, 200, nil)
}

func AccessKeyControllerStore(request *Request) Response {
	accessKey, err := request.accessKeyManager.Create()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Access key could not be created: %s", err.Error()),
		}, 500, nil)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status":  "success",
			"message": "Access key created successfully",
			"data": map[string]any{
				"access_key_id":     accessKey.AccessKeyId,
				"access_key_secret": accessKey.AccessKeySecret,
			},
		},
	}
}

func AccessKeyControllerUpdate(request *Request) Response {
	request.cluster.Auth.SecretsManager.Init()

	accessKey, err := request.accessKeyManager.Get(request.Get("access_key_id").(string))

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	err = accessKey.Update(request.Get("permissions").([]*auth.AccessKeyPermission))

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be updated",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key updated successfully.",
	}, 200, nil)
}

func AccessKeyControllerDestroy(request *Request) Response {
	log.Println("Destroying access key", request.Param("accessKeyId"))
	accessKey, err := request.accessKeyManager.Get(request.Param("accessKeyId"))

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	err = accessKey.Delete()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be deleted",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key deleted successfully.",
	}, 200, nil)
}
