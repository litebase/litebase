package http

import (
	"github.com/litebase/litebase/server/auth"

	"github.com/litebase/litebase/common/config"
)

type SingatureActivateRequest struct {
	Signature string `json:"signature" validate:"required"`
}

func SingatureActivateController(request *Request) Response {
	input, err := request.Input(&SingatureActivateRequest{})

	if err != nil {
		return Response{
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
		return Response{
			StatusCode: 422,
			Body: map[string]interface{}{
				"errors": validationErrors,
			},
		}
	}

	if !config.HasSignature(request.cluster.Config, input.(*SingatureActivateRequest).Signature) {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "The signature is invalid.",
			},
		}
	}

	auth.StoreSignature(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		input.(*SingatureActivateRequest).Signature,
	)

	request.cluster.Broadcast("activate_signature", input.(*SingatureActivateRequest).Signature)

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"data": map[string]any{},
		},
	}
}
