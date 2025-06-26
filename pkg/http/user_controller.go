package http

import (
	"fmt"

	"github.com/litebase/litebase/pkg/auth"
)

func UserControllerIndex(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	users := request.cluster.Auth.UserManager().All()

	userResponses := make([]*auth.UserResponse, 0, len(users))

	for _, user := range users {
		userResponses = append(userResponses, &auth.UserResponse{
			Username:   user.Username,
			Statements: user.Statements,
			CreatedAt:  user.CreatedAt,
			UpdatedAt:  user.UpdatedAt,
		})
	}

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"data": userResponses,
		},
	}
}

type UserControllerStoreRequest struct {
	Username   string                    `json:"username" validate:"required"`
	Password   string                    `json:"password" validate:"required,min=8"`
	Statements []auth.AccessKeyStatement `json:"statements" validate:"required"`
}

func UserControllerShow(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	user := request.cluster.Auth.UserManager().Get(username)

	if user == nil {
		return NotFoundResponse(fmt.Errorf("the user was not found"))
	}

	userResponse := &auth.UserResponse{
		Username:   user.Username,
		Statements: user.Statements,
		CreatedAt:  user.CreatedAt,
		UpdatedAt:  user.UpdatedAt,
	}

	return SuccessResponse(
		fmt.Sprintf("User '%s' retrieved successfully", username),
		userResponse,
		200,
	)
}

func UserControllerStore(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
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

	if request.cluster.Auth.UserManager().Get(input.(*UserControllerStoreRequest).Username) != nil {
		return BadRequestResponse(fmt.Errorf("the username already exists"))
	}

	data := input.(*UserControllerStoreRequest)

	err = request.cluster.Auth.UserManager().Add(
		data.Username,
		data.Password,
		data.Statements,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	user := request.cluster.Auth.UserManager().Get(data.Username)

	if user == nil {
		return ServerErrorResponse(fmt.Errorf("the user could not be created"))
	}

	// Convert the user to a response format
	userResponse := &auth.UserResponse{
		Username:   user.Username,
		Statements: user.Statements,
		CreatedAt:  user.CreatedAt,
		UpdatedAt:  user.UpdatedAt,
	}

	return SuccessResponse(
		"User created successfully",
		userResponse,
		201,
	)
}

type UserControllerUpdateRequest struct {
	Statements []auth.AccessKeyStatement `json:"statements" validate:"required"`
}

func UserControllerUpdate(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	user := request.cluster.Auth.UserManager().Get(username)

	if user == nil {
		return NotFoundResponse(fmt.Errorf("the user was not found"))
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

	if err := request.cluster.Auth.UserManager().Update(user); err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		fmt.Sprintf("User '%s' updated successfully", username),
		user,
		200,
	)
}

func UserControllerDestroy(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"*", fmt.Sprintf("cluster:%s", request.cluster.Id)},
		[]auth.Privilege{auth.ClusterPrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	username := request.Param("username")

	if username == "root" {
		return BadRequestResponse(fmt.Errorf("the root user cannot be deleted"))
	}

	if request.cluster.Auth.UserManager().Get(username) == nil {
		return BadRequestResponse(fmt.Errorf("the username is invalid"))
	}

	err = request.cluster.Auth.UserManager().Remove(username)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse("", nil, 204)
}
