package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

type KeyStoreRequest struct {
	EncryptionKey string `json:"encryption_key" validate:"required"`
	Signature     string `json:"signature" validate:"required"`
}

func KeyStoreController(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&KeyStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"encryption_key.required": "The encryption key field is required.",
		"signature.required":      "The signature field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	// Calculate expected HMAC signature
	hash := hmac.New(sha256.New, []byte(request.cluster.Config.EncryptionKey))
	hash.Write([]byte(input.(*KeyStoreRequest).EncryptionKey))
	expectedSignature := fmt.Sprintf("%x", hash.Sum(nil))

	if input.(*KeyStoreRequest).Signature != expectedSignature {
		return ForbiddenResponse(fmt.Errorf("invalid signature"))
	}

	err = auth.NextEncryptionKey(
		request.cluster.Auth,
		request.cluster.Config,
		input.(*KeyStoreRequest).EncryptionKey,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse("next encryption key stored successfully", map[string]any{}, 200)
}
