package http

import (
	"litebase/server/auth"
	"log"
)

func AccessKeyControllerIndex(request *Request) Response {
	accessKeysIds, err := auth.AccessKeyManager().AllAccessKeyIds()

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access keys could not be retrieved",
		}, 500, nil)
	}

	accessKeys := []map[string]interface{}{}

	for _, accessKeyId := range accessKeysIds {
		accessKeys = append(accessKeys, map[string]interface{}{
			"access_key_id": accessKeyId,
		})
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access keys retrieved successfully",
		"data":    accessKeys,
	}, 200, nil)
}

func AccessKeyControllerStore(request *Request) Response {
	accessKey, err := auth.AccessKeyManager().Create()

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access key could not be created",
		}, 500, nil)
	}

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Access key created successfully",
			"data": map[string]interface{}{
				"access_key_id":     accessKey.AccessKeyId,
				"access_key_secret": accessKey.AccessKeySecret,
			},
		},
	}
}

func AccessKeyControllerUpdate(request *Request) Response {
	auth.SecretsManager().Init()

	accessKey, err := auth.AccessKeyManager().Get(request.Get("access_key_id").(string))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	updated := accessKey.Update(request.Get("privileges"))

	if !updated {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access could not be updated",
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key updated successfully.",
	}, 200, nil)
}

func AccessKeyControllerDestroy(request *Request) Response {
	log.Println("Destroying access key", request.Param("accessKeyId"))
	accessKey, err := auth.AccessKeyManager().Get(request.Param("accessKeyId"))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	err = accessKey.Delete()

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Access key could not be deleted",
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Access key deleted successfully.",
	}, 200, nil)
}
