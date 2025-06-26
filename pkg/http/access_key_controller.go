package http

import (
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

// List all access keys
func AccessKeyControllerIndex(request *Request) Response {
	err := request.Authorize(
		[]string{"*", "access-key:*"},
		[]auth.Privilege{auth.AccessKeyPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	accessKeysIds, err := request.accessKeyManager.AllAccessKeyIds()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access keys could not be retrieved",
		}, 500, nil)
	}

	accessKeys := []map[string]any{}

	for _, accessKeyId := range accessKeysIds {
		accessKeys = append(accessKeys, map[string]any{
			"access_key_id": accessKeyId,
		})
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access keys retrieved successfully",
		"data":    accessKeys,
	}, 200, nil)
}

// Show details of a specific access key
func AccessKeyControllerShow(request *Request) Response {
	accessKeyId := request.Param("accessKeyId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	accessKey, err := request.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
	}

	err = request.Authorize(
		[]string{"*", "access-key:*", fmt.Sprintf("access-key:%s", accessKey.AccessKeyID)},
		[]auth.Privilege{auth.AccessKeyPrivilegeRead},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key retrieved successfully",
		"data":    accessKey.ToResponse(),
	}, 200, nil)
}

type AccessKeyStoreRequest struct {
	Description string                    `json:"description" validate:"omitempty,max=255"`
	Statements  []auth.AccessKeyStatement `json:"statements" validate:"required,min=1,max=100,dive,validateFn=IsValid"`
}

// Create a new access key
func AccessKeyControllerStore(request *Request) Response {
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
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Access key could not be created: %s", err.Error()),
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key created successfully",
		"data":    accessKey,
	}, 201, nil)
}

type AccessKeyUpdateRequest struct {
	Description string                    `json:"description" validate:"omitempty,max=255"`
	Statements  []auth.AccessKeyStatement `json:"statements" validate:"required,min=1,max=100,dive,validateFn=IsValid"`
}

// Update an existing access key
func AccessKeyControllerUpdate(request *Request) Response {
	// Get the access key ID from the request parameters
	accessKeyId := request.Param("accessKeyId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	accessKey, err := request.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be found",
		}, 404, nil)
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
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Invalid input: %s", err.Error()),
		}, 400, nil)
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
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Access key could not be updated",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key updated successfully.",
		"data":    accessKey.ToResponse(),
	}, 200, nil)
}

func AccessKeyControllerDestroy(request *Request) Response {
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

	if accessKeyId == request.RequestToken("Authorization").AccessKeyID {
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

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Access key deleted successfully.",
	}, 200, nil)
}
