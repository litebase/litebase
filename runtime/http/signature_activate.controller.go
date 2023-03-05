package http

import (
	"litebasedb/internal/config"
	"litebasedb/runtime/auth"
	"log"
)

type SignatureActivateController struct {
}

func (controller *SignatureActivateController) Store(request *Request) *Response {
	auth.SecretsManager().Init()

	if request.Get("signature") == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "A signature is required",
		}, 422, nil)
	}

	if !config.HasSignature(request.Get("signature").(string)) {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "The signature is invalid.",
			},
		}
	}

	config.StoreSignature(request.Get("signature").(string))

	log.Println("Signature activated: ", request.Get("signature").(string))

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   map[string]string{},
	}, 200, nil)
}
