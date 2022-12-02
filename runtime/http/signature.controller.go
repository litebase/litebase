package http

import (
	"litebasedb/runtime/auth"
)

type SignatureController struct {
}

func (controller *SignatureController) Store(request *Request) *Response {
	if request.Get("signature") == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "A signature is required",
		}, 400, nil)
	}

	var publicKey []byte
	var err error

	publicKey, err = auth.GetRawPublicKey(request.Get("signature").(string))

	if err != nil {
		_, err := auth.GeneratePrivateKey(request.Get("signature").(string))

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"status":  "error",
				"message": "Unable to generate private key",
			}, 500, nil)
		}

		publicKey, err = auth.GetRawPublicKey(request.Get("signature").(string))

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"status":  "error",
				"message": "Unable to generate public key",
			}, 500, nil)
		}
	}

	encrypted, err := auth.EncryptKey(string(publicKey))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Unable to encrypt key",
		}, 500, nil)
	}

	auth.Rotate()

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data": map[string]string{
			"public_key": encrypted,
		},
	}, 200, nil)
}
