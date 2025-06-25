package http

import "errors"

func RequireContentType(req *Request) (*Request, Response) {
	contentTypes := []string{"application/gob", "application/json", "application/octet-stream"}
	contentType := req.BaseRequest.Header.Get("Content-Type")

	if contentType == "" {
		return req, BadRequestResponse(errors.New("missing Content-Type header"))
	}

	for _, ct := range contentTypes {
		if contentType == ct {
			return req, Response{}
		}
	}

	return req, Response{
		StatusCode: 415,
		Body: map[string]any{
			"status":  "error",
			"message": "Unsupported Content-Type",
		},
	}
}
