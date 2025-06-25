package http

import "errors"

func RequireContentType(req *Request) (*Request, Response) {
	contentType := req.BaseRequest.Header.Get("Content-Type")

	if contentType == "" {
		return req, BadRequestResponse(errors.New("missing Content-Type header"))
	}

	if contentType != "application/json" {
		return req, Response{
			StatusCode: 415,
			Body: map[string]any{
				"status":  "error",
				"message": "Unsupported Content-Type. Only application/json is allowed.",
			},
		}
	}

	return req, Response{}
}
