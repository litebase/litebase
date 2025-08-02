package http

import (
	"maps"
	"net/http"
)

type Response struct {
	StatusCode int `json:"statusCode"`
	Stream     func(http.ResponseWriter)
	Headers    map[string]string `json:"headers"`
	Body       map[string]any    `json:"body"`
}

func JsonResponse(body map[string]any, statusCode int, headers map[string]string) Response {
	responseHeaders := make(map[string]string, len(headers)+1)
	responseHeaders["Content-Type"] = "application/json"

	maps.Copy(responseHeaders, headers)

	return Response{
		StatusCode: statusCode,
		Headers:    responseHeaders,
		Body:       body,
	}
}

func SuccessResponse(message string, data any, statusCode int) Response {
	return JsonResponse(map[string]any{
		"status":  "success",
		"message": message,
		"data":    data,
	}, statusCode, nil)
}

func (r Response) IsEmpty() bool {
	return r.StatusCode == 0 && r.Stream == nil && len(r.Headers) == 0 && r.Body == nil
}
