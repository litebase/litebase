package http

func RequireHost(host string) Middleware {
	return func(request *Request) (*Request, Response) {
		if request.Headers().Get("Host") != host {
			return request, Response{
				StatusCode: 403,
				Body: map[string]any{
					"status":  "error",
					"message": "Forbidden",
				},
			}
		}

		return request, Response{}
	}
}
