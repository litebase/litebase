package http

import (
	"context"
	"errors"
	"slices"
)

func RequireContentType(ctx context.Context, req *Request) (*Request, Response) {
	contentTypes := []string{"application/gob", "application/json", "application/octet-stream"}
	contentType := req.BaseRequest.Header.Get("Content-Type")

	if contentType == "" {
		return req, BadRequestResponse(errors.New("missing Content-Type header"))
	}

	if slices.Contains(contentTypes, contentType) {
		return req, Response{}
	}

	return req, Response{
		StatusCode: 415,
		Body: map[string]any{
			"status":  "error",
			"message": "Unsupported Content-Type",
		},
	}
}
