package http

import (
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

type KeyActivateRequest struct {
	EncryptionKey string `json:"encryption_key" validate:"required"`
}

func KeyActivateController(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&KeyActivateRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"encryption_key.required": "The encryption key field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	if !auth.HasKey(input.(*KeyActivateRequest).EncryptionKey, request.cluster.ObjectFS()) {
		return BadRequestResponse(errors.New("the encryption key is invalid"))
	}

	err = auth.StoreEncryptionKey(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		input.(*KeyActivateRequest).EncryptionKey,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	err = request.cluster.Broadcast("key:activate", input.(*KeyActivateRequest).EncryptionKey)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse("encryption key activated successfully", map[string]any{}, 200)
}
