package http

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

type UserControllerIndexResponse struct {
	Data []*auth.UserResponse `json:"users"`
}

// List all users
func UserControllerIndex(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	users := request.cluster.Auth.UserManager.All()

	userResponses := make([]*auth.UserResponse, 0, len(users))

	for _, user := range users {
		userResponses = append(userResponses, &auth.UserResponse{
			Username:   user.Username,
			Statements: user.Statements,
			CreatedAt:  user.CreatedAt,
			UpdatedAt:  user.UpdatedAt,
		})
	}

	return SuccessResponse(
		"Users retrieved successfully",
		&UserControllerIndexResponse{
			Data: userResponses,
		},
		200,
	)
}

type UserControllerStoreRequest struct {
	Description string           `json:"description" validate:"max=255"`
	Password    string           `json:"password" validate:"required,min=8"`
	Statements  []auth.Statement `json:"statements" validate:"required"`
	Username    string           `json:"username" validate:"required"`
}

type UserControllerShowResponse struct {
	Username    string           `json:"username" example:"admin" description:"The username"`
	Description string           `json:"description" example:"Administrator user" description:"The user description"`
	Statements  []auth.Statement `json:"statements" description:"List of permission statements defining what the user can access"`
	CreatedAt   string           `json:"created_at" description:"Creation timestamp"`
	UpdatedAt   string           `json:"updated_at" description:"Last update timestamp"`
}

// Create a new user
func UserControllerShow(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	user, err := request.cluster.Auth.UserManager.Get(username)

	if err != nil {
		return NotFoundResponse(fmt.Errorf("the user was not found"))
	}

	return SuccessResponse(
		fmt.Sprintf("User '%s' retrieved successfully", username),
		UserControllerShowResponse{
			Username:   user.Username,
			Statements: user.Statements,
			CreatedAt:  user.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt:  user.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		},
		200,
	)
}

type UserControllerStoreResponse struct {
	Username    string           `json:"username"`
	Description string           `json:"description"`
	Statements  []auth.Statement `json:"statements"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
}

// Create a new user
func UserControllerStore(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&UserControllerStoreRequest{})

	if err != nil {
		return BadRequestResponse(fmt.Errorf("invalid input: %w", err))
	}

	validationErrors := request.Validate(input, map[string]string{
		"username.required":                "The username field is required.",
		"password.required":                "The password field is required.",
		"password.min":                     "The password field should be a minimum of 8 characters.",
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

	if input.(*UserControllerStoreRequest).Username == "root" {
		return BadRequestResponse(fmt.Errorf("the username is invalid, 'root' is reserved"))
	}

	user, err := request.cluster.Auth.UserManager.Get(input.(*UserControllerStoreRequest).Username)

	if err != nil && err != auth.ErrUserNotFound {
		return ServerErrorResponse(fmt.Errorf("failed to check if user exists: %w", err))
	}

	if user != nil {
		return BadRequestResponse(fmt.Errorf("the username already exists"))
	}

	data := input.(*UserControllerStoreRequest)

	user, err = request.cluster.Auth.UserManager.Create(
		data.Username,
		data.Password,
		"",
		data.Statements,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"User created successfully",
		UserControllerStoreResponse{
			Username:    user.Username,
			Description: user.Description,
			Statements:  user.Statements,
			CreatedAt:   user.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt:   user.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		},
		201,
	)
}

type UserControllerUpdateRequest struct {
	Description string           `json:"description" validate:"max=255"`
	Statements  []auth.Statement `json:"statements" validate:"required"`
}

type UserControllerUpdateResponse struct {
	Username    string           `json:"username" example:"admin" description:"The username"`
	Description string           `json:"description" example:"Administrator user" description:"The user description"`
	Statements  []auth.Statement `json:"statements" description:"List of permission statements defining what the user can access"`
	CreatedAt   string           `json:"created_at" description:"Creation timestamp"`
	UpdatedAt   string           `json:"updated_at" description:"Last update timestamp"`
}

// Update an existing user
func UserControllerUpdate(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	user, err := request.cluster.Auth.UserManager.Get(username)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(fmt.Errorf("the user was not found"))
		}

		return ServerErrorResponse(fmt.Errorf("failed to retrieve user: %w", err))
	}

	input, err := request.Input(&UserControllerUpdateRequest{})

	if err != nil {
		return BadRequestResponse(fmt.Errorf("invalid input: %w", err))
	}

	validationErrors := request.Validate(input, map[string]string{
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

	data := input.(*UserControllerUpdateRequest)

	// Update the user
	user.Statements = data.Statements

	if err := request.cluster.Auth.UserManager.Update(user); err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		fmt.Sprintf("User '%s' updated successfully", username),
		UserControllerUpdateResponse{
			Username:   user.Username,
			Statements: user.Statements,
		},
		200,
	)
}

func UserControllerDestroy(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.ID)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	if username == "root" {
		return BadRequestResponse(fmt.Errorf("the root user cannot be deleted"))
	}

	credential := request.Credential()

	if credential.IsBasicAuth() && credential.User().Username == username {
		return ForbiddenResponse(fmt.Errorf("the user cannot be deleted"))
	}

	_, err = request.cluster.Auth.UserManager.Get(username)

	if err != nil {
		if err == auth.ErrUserNotFound {
			return NotFoundResponse(fmt.Errorf("the user was not found"))
		}

		return ServerErrorResponse(fmt.Errorf("failed to retrieve user: %w", err))
	}

	err = request.cluster.Auth.UserManager.Remove(username)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse("", nil, 204)
}
