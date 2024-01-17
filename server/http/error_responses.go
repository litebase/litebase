package http

import "fmt"

func BadRequestResponse(err error) *Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	}, 400, nil)
}

func ServerErrorResponse(err error) *Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	}, 500, nil)
}

func ValidationErrorResponse(errors map[string][]string) *Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": "Error: the request input is invalid",
		"errors":  errors,
	}, 422, nil)
}
