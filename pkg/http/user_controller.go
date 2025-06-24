package http

import (
	"fmt"
	"log"

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

	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"data": users,
		},
	}
}

type UserControllerStoreRequest struct {
	Username   string                    `json:"username" validate:"required"`
	Password   string                    `json:"password" validate:"required,min=8"`
	Statements []auth.AccessKeyStatement `json:"statements" validate:"required"`
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
		"username.required":   "The username field is required.",
		"password.required":   "The password field is required.",
		"password.min":        "The password field should be a minimum of 8 characters.",
		"privileges.required": "The privileges field is required.",
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

	log.Println("User created successfully:", user)
	return SuccessResponse(
		"User created successfully",
		user,
		201)
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
