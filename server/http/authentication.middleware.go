package http

import (
	"strconv"
	"time"
)

func basicAuth(request *Request) bool {
	username, password, ok := request.BaseRequest.BasicAuth()

	if ok {
		return request.cluster.Auth.UserManager().Authenticate(username, password)
	}

	return false
}

func Authentication(request *Request) (*Request, Response) {
	if basicAuth(request) {
		return request, Response{}
	}

	if !ensureRequestHasAnAuthorizationHeader(request) ||
		!ensureRequestIsProperlySigned(request) {
		return request, Response{
			StatusCode: 401,
			Body: map[string]any{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	if !ensureRequestIsNotExpired(request) {
		return request, Response{
			StatusCode: 401,
			Body: map[string]any{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	return request, Response{}
}

func ensureRequestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func ensureRequestIsNotExpired(request *Request) bool {
	dateHeader := request.Headers().Get("X-Lbdb-Date")

	if dateHeader == "" {
		return false
	}

	parseInt, err := strconv.ParseInt(dateHeader, 10, 64)

	if err != nil {
		return false
	}

	parsedTime := time.Unix(parseInt, 0)

	return time.Since(parsedTime) < 10*time.Second
}

func ensureRequestIsProperlySigned(request *Request) bool {
	return RequestSignatureValidator(request, "Authorization")
}
