package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

// Array of access keys for list operations
type AccessKeyIndexResponse struct {
	AccessKeyID string `json:"access_key_id"`
}

// Array of access keys for list operations
type AccessKeyListResponse []AccessKeyIndexResponse

// List all access keys
func AccessKeyControllerIndex(ctx context.Context, request *Request) Response {
	err := request.Authorize(
		[]string{"*", "access-key:*"},
		[]auth.Privilege{auth.AccessKeyPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	accessKeysIds, err := request.accessKeyManager.AllAccessKeyIds()

	if err != nil {
		return ServerErrorResponse(errors.New("access keys could not be retrieved"))
	}

	var response AccessKeyListResponse

	for _, accessKeyId := range accessKeysIds {
		response = append(response, AccessKeyIndexResponse{
			AccessKeyID: accessKeyId,
		})
	}

	return SuccessResponse("Access keys retrieved successfully", response, 200)
}

type AccessKeyShowResponse struct {
	AccessKeyID string           `json:"access_key_id"`
	Description string           `json:"description"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
	Statements  []auth.Statement `json:"statements"`
}

// Show details of a specific access key
func AccessKeyControllerShow(ctx context.Context, request *Request) Response {
	accessKeyId := request.Param("accessKeyId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	accessKey, err := request.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return NotFoundResponse(errors.New("access key could not be found"))
	}

	err = request.Authorize(
		[]string{"*", "access-key:*", fmt.Sprintf("access-key:%s", accessKey.AccessKeyID)},
		[]auth.Privilege{auth.AccessKeyPrivilegeRead},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	response := AccessKeyShowResponse{
		AccessKeyID: accessKey.AccessKeyID,
		Description: accessKey.Description,
		CreatedAt:   accessKey.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   accessKey.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		Statements:  accessKey.Statements,
	}

	return SuccessResponse(
		"Access key retrieved successfully",
		response,
		200,
	)
}

type AccessKeyStoreRequest struct {
	Description string           `json:"description" validate:"omitempty,max=255"`
	Statements  []auth.Statement `json:"statements" validate:"required,min=1,max=100,dive,validateFn=IsValid"`
}

type AccessKeyStoreResponse struct {
	AccessKeyID     string           `json:"access_key_id"`
	AccessKeySecret string           `json:"access_key_secret"`
	Description     string           `json:"description"`
	CreatedAt       string           `json:"created_at"`
	UpdatedAt       string           `json:"updated_at"`
	Statements      []auth.Statement `json:"statements"`
}

// Create a new access key
func AccessKeyControllerStore(ctx context.Context, request *Request) Response {
	// Authorize the request for access key creation
	err := request.Authorize(
		[]string{"*", "access-key:*"},
		[]auth.Privilege{auth.AccessKeyPrivilegeCreate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Map the request body to the AccessKeyStoreRequest struct
	input, err := request.Input(&AccessKeyStoreRequest{})

	if err != nil {
		return BadRequestResponse(errors.New("the request input is invalid"))
	}

	// Validate the input
	validationErrors := request.Validate(input, map[string]string{
		"description.max":                  "The description field must be at most 255 characters long",
		"statements.max":                   "The statements field must contain at most 100 items",
		"statements.min":                   "The statements field must contain at least 1 item",
		"statements.required":              "The statements field is required",
		"statements.*.validateFn":          "This statement is not valid. All actions must match the resource.",
		"statements.*.effect.required":     "Each statement must have an effect",
		"statements.*.effect.validateFn":   "The effect of the statement must be one of 'Allow' or 'Deny'",
		"statements.*.resource.required":   "This statement is missing a resource",
		"statements.*.resource.validateFn": "This resource is not valid",
		"statements.*.actions.required":    "This statement is missing actions",
		"statements.*.actions.min":         "Each statement must have at least one action",
		"statements.*.actions.max":         "Each statement can have at most 100 actions",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	// Create the access key
	accessKey, err := request.accessKeyManager.Create(
		input.(*AccessKeyStoreRequest).Description,
		input.(*AccessKeyStoreRequest).Statements,
	)

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("access key could not be created: %s", err.Error()))
	}

	response := AccessKeyStoreResponse{
		AccessKeyID:     accessKey.AccessKeyID,
		AccessKeySecret: accessKey.AccessKeySecret,
		Description:     accessKey.Description,
		CreatedAt:       accessKey.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:       accessKey.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		Statements:      accessKey.Statements,
	}

	return SuccessResponse(
		"Access key created successfully",
		response,
		201,
	)
}

type AccessKeyUpdateRequest struct {
	Description string           `json:"description" validate:"omitempty,max=255"`
	Statements  []auth.Statement `json:"statements" validate:"required,min=1,max=100,dive,validateFn=IsValid"`
}

type AccessKeyUpdateResponse struct {
	AccessKeyID string           `json:"access_key_id"`
	Description string           `json:"description"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
	Statements  []auth.Statement `json:"statements"`
}

// Update an existing access key
func AccessKeyControllerUpdate(ctx context.Context, request *Request) Response {
	// Get the access key ID from the request parameters
	accessKeyId := request.Param("accessKeyId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	accessKey, err := request.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return NotFoundResponse(errors.New("access key could not be found"))
	}

	err = request.Authorize(
		[]string{"*", "access-key:*", fmt.Sprintf("access-key:%s", accessKey.AccessKeyID)},
		[]auth.Privilege{auth.AccessKeyPrivilegeUpdate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&AccessKeyUpdateRequest{})

	if err != nil {
		return BadRequestResponse(fmt.Errorf("invalid input: %s", err.Error()))
	}

	// Validate the input
	validationErrors := request.Validate(input, map[string]string{
		"statements.max":                   "The statements field must contain at most 100 items",
		"statements.min":                   "The statements field must contain at least 1 item",
		"statements.required":              "The statements field is required",
		"statements.*.validateFn":          "This statement is not valid. All actions must match the resource.",
		"statements.*.effect.required":     "Each statement must have an effect",
		"statements.*.effect.validateFn":   "The effect of the statement must be one of 'Allow' or 'Deny'",
		"statements.*.resource.required":   "This statement is missing a resource",
		"statements.*.resource.validateFn": "This resource is not valid",
		"statements.*.actions.required":    "This statement is missing actions",
		"statements.*.actions.min":         "Each statement must have at least one action",
		"statements.*.actions.max":         "Each statement can have at most 100 actions",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	description := accessKey.Description

	if input.(*AccessKeyUpdateRequest).Description != "" {
		description = input.(*AccessKeyUpdateRequest).Description
	}

	err = accessKey.Update(
		description,
		input.(*AccessKeyUpdateRequest).Statements,
	)

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("access key could not be updated: %s", err.Error()))
	}

	response := AccessKeyUpdateResponse{
		AccessKeyID: accessKey.AccessKeyID,
		Description: accessKey.Description,
		CreatedAt:   accessKey.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   accessKey.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		Statements:  accessKey.Statements,
	}

	return SuccessResponse(
		"Access key updated successfully.",
		response,
		200,
	)
}

// Delete an access key
func AccessKeyControllerDestroy(ctx context.Context, request *Request) Response {
	// Get the access key ID from the request parameters
	accessKeyId := request.Param("accessKeyId")

	// Authorize the request for access key deletion
	err := request.Authorize(
		[]string{"*", "access-key:*", fmt.Sprintf("access-key:%s", accessKeyId)},
		[]auth.Privilege{auth.AccessKeyPrivilegeDelete},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	credential := request.Credential()

	if credential.Type() == auth.CredentialTypeAccessKey && accessKeyId == credential.AccessKey().AccessKeyID {
		return ForbiddenResponse(errors.New("cannot delete current access key"))
	}

	accessKey, err := request.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	err = accessKey.Delete()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be deleted",
		}, 500, nil)
	}

	return SuccessResponse(
		"Access key deleted successfully.",
		nil,
		200,
	)
}
