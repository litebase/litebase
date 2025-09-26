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

func (r Response) WithHeader(key, value string) Response {
	if r.Headers == nil {
		r.Headers = make(map[string]string)
	}

	r.Headers[key] = value

	return r
}

func (r Response) WithHeaders(headers map[string]string) Response {
	if r.Headers == nil {
		r.Headers = make(map[string]string, len(headers))
	}

	maps.Copy(r.Headers, headers)

	return r
}

func (r Response) WithMeta(key string, value any) Response {
	if r.Body == nil {
		r.Body = make(map[string]any)
	}

	meta, ok := r.Body["meta"].(map[string]any)

	if !ok {
		meta = make(map[string]any)
		r.Body["meta"] = meta
	}

	meta[key] = value

	return r
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

func UnauthorizedResponse() Response {
	return JsonResponse(map[string]any{
		"status":  "error",
		"message": "Unauthorized",
	}, 401, nil)
}

func (r Response) IsEmpty() bool {
	return r.StatusCode == 0 && r.Stream == nil && len(r.Headers) == 0 && r.Body == nil
}
