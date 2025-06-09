package http

import "github.com/litebase/litebase/server/auth"

type UserControllerStoreRequest struct {
	Username   string                    `json:"username" validate:"required"`
	Password   string                    `json:"password" validate:"required,min=8"`
	Statements []auth.AccessKeyStatement `json:"statements" validate:"required"`
}

func UserControllerIndex(request *Request) Response {
	users := request.cluster.Auth.UserManager().All()

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"data": users,
		},
	}
}

func UserControllerStore(request *Request) Response {
	input, err := request.Input(&UserControllerStoreRequest{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	validationErrors := request.Validate(input, map[string]string{
		"username.required":   "The username field is required.",
		"password.required":   "The password field is required.",
		"password.min":        "The password field should be a minimum of 8 characters.",
		"privileges.required": "The privileges field is required.",
		// "privileges.array":    "The privileges field must be an array.",
	})

	if validationErrors != nil {
		return Response{
			StatusCode: 422,
			Body: map[string]interface{}{
				"errors": validationErrors,
			},
		}
	}

	if input.(*UserControllerStoreRequest).Username == "root" {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "This username is invalid.",
			},
		}
	}

	if request.cluster.Auth.UserManager().Get(input.(*UserControllerStoreRequest).Username) != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "This username is already in use.",
			},
		}
	}

	data := input.(*UserControllerStoreRequest)

	request.cluster.Auth.UserManager().Add(
		data.Username,
		data.Password,
		data.Statements,
	)

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"message": "User created successfully",
		},
	}
}

func UserControllerDestroy(request *Request) Response {
	username := request.Param("username")

	if username == "root" {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "The username is invalid.",
			},
		}
	}

	if request.cluster.Auth.UserManager().Get(username) == nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "The username is invalid.",
			},
		}
	}

	err := request.cluster.Auth.UserManager().Remove(username)

	if err != nil {
		return Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"message": err.Error(),
			},
		}
	}

	return Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"message": "User deleted successfully",
		},
	}
}
