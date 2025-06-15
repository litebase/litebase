package http

import (
	"encoding/json"
	"fmt"
)

func JsonStringError(err error) []byte {
	jsonData, _ := json.Marshal(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	})

	return jsonData
}

func JsonStringErrorWithData(err error, data map[string]interface{}) []byte {
	jsonData, _ := json.Marshal(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
		"data":    data,
	})

	return jsonData
}

func JsonNewLineError(err error) []byte {
	return append(JsonStringError(err), "\n"...)
}

func JsonNewLineErrorWithData(err error, data map[string]interface{}) []byte {
	return append(JsonStringErrorWithData(err, data), "\n"...)
}

func BadRequestResponse(err error) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	}, 400, nil)
}

func ForbiddenResponse(err error) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	}, 403, nil)
}

func ServerErrorResponse(err error) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": fmt.Sprintf("Error: %s", err.Error()),
	}, 500, nil)
}

func ValidationErrorResponse(errors map[string][]string) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "error",
		"message": "Error: the request input is invalid",
		"errors":  errors,
	}, 422, nil)
}
