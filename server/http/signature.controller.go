package http

import (
	"github.com/litebase/litebase/server/auth"
)

type SingatureStoreRequest struct {
	Signature string `json:"signature" validate:"required"`
}

func SingatureStoreController(request *Request) Response {
	input, err := request.Input(&SingatureStoreRequest{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"Signature.required": "The signature field is required.",
	})

	if validationErrors != nil {
		return Response{
			StatusCode: 422,
			Body: map[string]any{
				"errors": validationErrors,
			},
		}
	}

	publicKey, err := auth.NextSignature(
		request.cluster.Config,
		request.cluster.Auth.SecretsManager,
		input.(*SingatureStoreRequest).Signature,
	)

	if err != nil {
		return Response{
			StatusCode: 500,
			Body: map[string]any{
				"error": err.Error(),
			},
		}
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"data": map[string]any{
				"public_key": publicKey,
			},
		},
	}
}
