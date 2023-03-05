package http

import (
	"litebasedb/runtime/auth"
)

type SignatureController struct {
}

func (controller *SignatureController) Store(request *Request) *Response {
	auth.SecretsManager().Init()

	if request.Get("signature") == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "A signature is required",
		}, 400, nil)
	}

	publicKey, err := auth.NextSignature(request.Get("signature").(string))

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data": map[string]string{
			"public_key": publicKey,
		},
	}, 200, nil)
}
