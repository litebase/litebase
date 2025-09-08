package http

import (
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

// List all tokens
func TokenControllerIndex(request *Request) Response {
	err := request.Authorize(
		[]string{"*", "token:*"},
		[]auth.Privilege{auth.TokenPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	tokenIds, err := request.cluster.Auth.TokenManager.AllTokenIDs()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Tokens could not be retrieved",
		}, 500, nil)
	}

	tokens := []map[string]any{}

	for _, tokenId := range tokenIds {
		tokens = append(tokens, map[string]any{
			"token_id": tokenId,
		})
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Tokens retrieved successfully",
		"data":    tokens,
	}, 200, nil)
}

// Show details of a specific token
func TokenControllerShow(request *Request) Response {
	tokenId := request.Param("tokenId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	token, err := request.cluster.Auth.TokenManager.Get(tokenId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Token could not be found",
		}, 404, nil)
	}

	err = request.Authorize(
		[]string{"*", "token:*", fmt.Sprintf("token:%s", token.TokenID)},
		[]auth.Privilege{auth.TokenPrivilegeRead},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Token retrieved successfully",
		"data":    token.ToResponse(),
	}, 200, nil)
}

// Create a new token
func TokenControllerStore(request *Request) Response {
	err := request.Authorize(
		[]string{"*", "token:*"},
		[]auth.Privilege{auth.TokenPrivilegeCreate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&AccessKeyStoreRequest{})

	if err != nil {
		return BadRequestResponse(errors.New("the request input is invalid"))
	}

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

	token, err := request.cluster.Auth.TokenManager.Create(
		input.(*AccessKeyStoreRequest).Description,
		input.(*AccessKeyStoreRequest).Statements,
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Token could not be created: %s", err.Error()),
		}, 500, nil)
	}

	tokenValue, err := token.Value()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Token could not be created: %s", err.Error()),
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Token created successfully",
		"data": auth.TokenResponse{
			TokenID:     token.TokenID,
			Token:       tokenValue,
			Statements:  token.Statements,
			Description: token.Description,
			CreatedAt:   token.CreatedAt,
			UpdatedAt:   token.UpdatedAt,
		},
	}, 201, nil)
}

// Update an existing token
func TokenControllerUpdate(request *Request) Response {
	tokenId := request.Param("tokenId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	token, err := request.cluster.Auth.TokenManager.Get(tokenId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Token could not be found",
		}, 404, nil)
	}

	err = request.Authorize(
		[]string{"*", "token:*", fmt.Sprintf("token:%s", token.TokenID)},
		[]auth.Privilege{auth.TokenPrivilegeUpdate},
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
	description := token.Description

	if input.(*AccessKeyUpdateRequest).Description != "" {
		description = input.(*AccessKeyUpdateRequest).Description
	}

	err = token.Update(
		description,
		input.(*AccessKeyUpdateRequest).Statements,
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Token could not be updated",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Token updated successfully.",
		"data":    token.ToResponse(),
	}, 200, nil)
}

// Delete a token
func TokenControllerDestroy(request *Request) Response {
	tokenId := request.Param("tokenId")

	err := request.Authorize(
		[]string{"*", "token:*", fmt.Sprintf("token:%s", tokenId)},
		[]auth.Privilege{auth.TokenPrivilegeDelete},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	credential := request.Credential()

	if credential.IsToken() && tokenId == credential.Token().TokenID {
		return ForbiddenResponse(errors.New("cannot delete current token"))
	}

	token, err := request.cluster.Auth.TokenManager.Get(tokenId)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Token could not be found",
		}, 404, nil)
	}

	err = token.Delete()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Token could not be deleted",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Token deleted successfully.",
	}, 200, nil)
}
