package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

type TokenControllerIndexResponse []auth.TokenResponse

// List all tokens
func TokenControllerIndex(ctx context.Context, request *Request) Response {
	err := request.Authorize(
		[]string{"*", "token:*"},
		[]auth.Privilege{auth.TokenPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	tokens, err := request.cluster.Auth.TokenManager.AllTokens()

	if err != nil {
		return ServerErrorResponse(errors.New("tokens could not be retrieved"))
	}

	var responseTokens TokenControllerIndexResponse

	for _, token := range tokens {
		responseTokens = append(responseTokens, *token.ToResponse())
	}

	return SuccessResponse(
		"Tokens retrieved successfully",
		responseTokens,
		200,
	)
}

type TokenControllerShowResponse struct {
	TokenID     string           `json:"tokenId"`
	Description string           `json:"description"`
	CreatedAt   string           `json:"createdAt"`
	UpdatedAt   string           `json:"updatedAt"`
	Statements  []auth.Statement `json:"statements"`
}

// Show details of a specific token
func TokenControllerShow(ctx context.Context, request *Request) Response {
	tokenId := request.Param("tokenId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	token, err := request.cluster.Auth.TokenManager.Get(tokenId)

	if err != nil {
		return NotFoundResponse(errors.New("token could not be found"))
	}

	err = request.Authorize(
		[]string{"*", "token:*", fmt.Sprintf("token:%s", token.TokenID)},
		[]auth.Privilege{auth.TokenPrivilegeRead},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	return SuccessResponse("Token retrieved successfully", TokenControllerShowResponse{
		TokenID:     token.TokenID,
		Description: token.Description,
		CreatedAt:   token.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   token.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		Statements:  token.Statements,
	}, 200)
}

type TokenStoreRequest struct {
	Description string           `json:"description" validate:"max=255"`
	Statements  []auth.Statement `json:"statements" validate:"required,min=1,max=100,dive"`
}

type TokenStoreResponse struct {
	TokenID     string           `json:"tokenId"`
	Token       string           `json:"token"`
	Description string           `json:"description"`
	Statements  []auth.Statement `json:"statements"`
	CreatedAt   string           `json:"createdAt"`
	UpdatedAt   string           `json:"updatedAt"`
}

// Create a new token
func TokenControllerStore(ctx context.Context, request *Request) Response {
	err := request.Authorize(
		[]string{"*", "token:*"},
		[]auth.Privilege{auth.TokenPrivilegeCreate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&TokenStoreRequest{})

	if err != nil {
		return BadRequestResponse(errors.New("the request input is invalid"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"description.max":                  "The description field must be at most 255 characters long",
		"statements.max":                   "The statements field must contain at most 100 items",
		"statements.min":                   "The statements field must contain at least 1 item",
		"statements.required":              "The statements field is required",
		"statements.*.validateFn":          "This statement is not valid. All actions must match the resource",
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
		input.(*TokenStoreRequest).Description,
		input.(*TokenStoreRequest).Statements,
	)

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("token could not be created: %s", err.Error()))
	}

	tokenValue, err := token.Value()

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("token could not be created: %s", err.Error()))
	}

	return SuccessResponse("Token created successfully", auth.TokenCreatedResponse{
		TokenID:     token.TokenID,
		Token:       tokenValue,
		Statements:  token.Statements,
		Description: token.Description,
		CreatedAt:   token.CreatedAt,
		UpdatedAt:   token.UpdatedAt,
	}, 201)
}

type TokenUpdateRequest struct {
	Description string           `json:"description" validate:"max=255"`
	Statements  []auth.Statement `json:"statements" validate:"required,min=1,max=100,dive"`
}

type TokenUpdateResponse struct {
	TokenID     string           `json:"tokenId"`
	Description string           `json:"description"`
	Statements  []auth.Statement `json:"statements"`
	CreatedAt   string           `json:"createdAt"`
	UpdatedAt   string           `json:"updatedAt"`
}

// Update an existing token
func TokenControllerUpdate(ctx context.Context, request *Request) Response {
	tokenId := request.Param("tokenId")

	err := request.cluster.Auth.SecretsManager.Init()

	if err != nil {
		return ServerErrorResponse(err)
	}

	token, err := request.cluster.Auth.TokenManager.Get(tokenId)

	if err != nil {
		return NotFoundResponse(errors.New("token could not be found"))
	}

	err = request.Authorize(
		[]string{"*", "token:*", fmt.Sprintf("token:%s", token.TokenID)},
		[]auth.Privilege{auth.TokenPrivilegeUpdate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&TokenUpdateRequest{})

	if err != nil {
		return BadRequestResponse(errors.New("the request input is invalid"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"statements.max":                   "The statements field must contain at most 100 items",
		"statements.min":                   "The statements field must contain at least 1 item",
		"statements.required":              "The statements field is required",
		"statements.*.validateFn":          "This statement is not valid. All actions must match the resource",
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

	if input.(*TokenUpdateRequest).Description != "" {
		description = input.(*TokenUpdateRequest).Description
	}

	err = token.Update(
		description,
		input.(*TokenUpdateRequest).Statements,
	)

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("token could not be updated: %s", err.Error()))
	}

	return SuccessResponse("Token updated successfully", TokenUpdateResponse{
		TokenID:     token.TokenID,
		Description: token.Description,
		Statements:  token.Statements,
		CreatedAt:   token.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:   token.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}, 200)
}

// Delete a token
func TokenControllerDestroy(ctx context.Context, request *Request) Response {
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
		return NotFoundResponse(errors.New("token could not be found"))
	}

	err = token.Delete()

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("token could not be deleted: %s", err.Error()))
	}

	return SuccessResponse("Token deleted successfully", nil, 200)

}
