package http

import (
	"litebase/internal/config"
	"litebase/server/events"
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

	if !config.HasSignature(input.(*SingatureActivateRequest).Signature) {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": "The signature is invalid.",
			},
		}
	}

	config.StoreSignature(input.(*SingatureActivateRequest).Signature)
	events.Broadcast("activate_signature", input.(*SingatureActivateRequest).Signature)

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"data": map[string]interface{}{},
		},
	}
}
