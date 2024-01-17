package http

import (
	_auth "litebasedb/internal/auth"
	"litebasedb/server/auth"
)

type DatabasePublicKeyRequest struct {
	PublicKey string `json:"public_key" validate:"required"`
	Signature string `json:"signature" validate:"required"`
}

func DatabasePublicKeyController(request *Request) *Response {
	input, err := request.Input(&DatabasePublicKeyRequest{})

	if err != nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"public_key.required": "The public key field is required.",
		"signature.required":  "The signature field is required.",
	})

	if validationErrors != nil {
		return &Response{
			StatusCode: 422,
			Body: map[string]interface{}{
				"errors": validationErrors,
			},
		}
	}

	var signature string

	if signature = _auth.FindSignature(request.Headers().Get("X-Lbdb-Signature")); signature == "" {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "The signature is invalid.",
			},
		}
	}

	decrypted, err := auth.SecretsManager().Decrypt(
		signature,
		input.(*DatabasePublicKeyRequest).PublicKey,
	)

	if err != nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "The signature is invalid.",
			},
		}
	}

	encrypted, err := auth.SecretsManager().Encrypt(
		input.(*DatabasePublicKeyRequest).Signature,
		decrypted["value"],
	)

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	err = auth.SecretsManager().StoreDatabasePublicKey(
		input.(*DatabasePublicKeyRequest).Signature,
		request.Param("databaseUuid"),
		encrypted,
	)

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status": "success",
		},
	}
}
