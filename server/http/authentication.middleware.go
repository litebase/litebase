package http

import (
	"strconv"
	"time"
)

func Authentication(request Request) (Request, Response) {
	if !ensureReuestHasAnAuthorizationHeader(request) ||
		!ensureRequestIsProperlySigned(request) {
		return request, Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	if !ensureRequestIsNotExpired(request) {
		return request, Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	return request, Response{}
}

func ensureReuestHasAnAuthorizationHeader(request Request) bool {
	return request.Headers().Has("Authorization")
}

func ensureRequestIsNotExpired(request Request) bool {
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

func ensureRequestIsProperlySigned(request Request) bool {
	return RequestSignatureValidator(request, "Authorization")
}
