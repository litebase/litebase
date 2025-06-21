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
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"username.required":   "The username field is required.",
		"password.required":   "The password field is required.",
		"password.min":        "The password field should be a minimum of 8 characters.",
		"privileges.required": "The privileges field is required.",
	})

	if validationErrors != nil {
		return Response{
			StatusCode: 422,
			Body: map[string]any{
				"errors": validationErrors,
			},
		}
	}

	if input.(*UserControllerStoreRequest).Username == "root" {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "This username is invalid.",
			},
		}
	}

	if request.cluster.Auth.UserManager().Get(input.(*UserControllerStoreRequest).Username) != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "This username is already in use.",
			},
		}
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

	return SuccessResponse(
		"User created successfully",
		map[string]any{
			"username":   data.Username,
			"statements": data.Statements,
		},
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
