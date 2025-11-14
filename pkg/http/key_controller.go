package http

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

type KeyStoreRequest struct {
	EncryptionKey string `json:"encryptionKey" validate:"required"`
	Signature     string `json:"signature" validate:"required"`
}

// Store the next encryption key for the cluster
func KeyControllerStore(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&KeyStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	req, ok := input.(*KeyStoreRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"encryption_key.required": "The encryption key field is required.",
		"signature.required":      "The signature field is required",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	// Calculate expected HMAC signature
	hash := hmac.New(sha256.New, []byte(request.cluster.Config.EncryptionKey))
	hash.Write([]byte(req.EncryptionKey))
	expectedSignature := fmt.Sprintf("%x", hash.Sum(nil))

	if req.Signature != expectedSignature {
		return ForbiddenResponse(fmt.Errorf("invalid signature"))
	}

	err = auth.NextEncryptionKey(
		request.cluster.Auth,
		request.cluster.Config,
		req.EncryptionKey,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse("next encryption key stored successfully", nil, 200)
}
