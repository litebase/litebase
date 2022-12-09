package http

type Response struct {
	StatusCode int                    `json:"statusCode"`
	Headers    map[string]string      `json:"headers"`
	Body       map[string]interface{} `json:"body"`
}

func JsonResponse(body map[string]interface{}, statusCode int, headers map[string]string) *Response {
	responseHeaders := map[string]string{
		"Content-Type": "application/json",
	}

	for key, value := range headers {
		responseHeaders[key] = value
	}

	return &Response{
		StatusCode: statusCode,
		Headers:    responseHeaders,
		Body:       body,
	}
}
