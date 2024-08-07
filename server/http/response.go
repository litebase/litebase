package http

import (
	"net/http"
)

type Response struct {
	StatusCode int `json:"statusCode"`
	Stream     func(http.ResponseWriter)
	Headers    map[string]string      `json:"headers"`
	Body       map[string]interface{} `json:"body"`
}

func JsonResponse(body map[string]interface{}, statusCode int, headers map[string]string) Response {
	responseHeaders := make(map[string]string, len(headers)+1)
	responseHeaders["Content-Type"] = "application/json"

	for key, value := range headers {
		responseHeaders[key] = value
	}

	return Response{
		StatusCode: statusCode,
		Headers:    responseHeaders,
		Body:       body,
	}
}

func SuccessResponse(message string, data interface{}, statusCode int) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": message,
		"data":    data,
	}, statusCode, nil)
}
