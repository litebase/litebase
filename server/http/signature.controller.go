package http

import (
	"litebasedb/server/auth"
)

type SingatureStoreRequest struct {
	Signature string `json:"signature" validate:"required"`
}

func SingatureStoreController(request *Request) *Response {
	input, err := request.Input(&SingatureStoreRequest{})

	if err != nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"Signature.required": "The signature field is required.",
	})

	if validationErrors != nil {
		return &Response{
			StatusCode: 422,
			Body: map[string]interface{}{
				"errors": validationErrors,
			},
		}
	}

	publicKey, err := auth.NextSignature(input.(*SingatureStoreRequest).Signature)

	if err != nil {
		return &Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"data": map[string]interface{}{
				"public_key": publicKey,
			},
		},
	}
}
