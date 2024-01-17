package http

func RequireSubdomain(request *Request) (*Request, *Response) {
	if len(request.Subdomains()) < 2 {
		return nil, &Response{
			StatusCode: 403,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Forbidden",
			},
		}
	}

	return request, nil
}
