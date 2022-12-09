package http

import (
	"strconv"
	"time"
)

func AdminAuth(request *Request) (*Request, *Response) {
	if !ensureRequestHasAnAuthorizationHeader(request) ||
		!ensureRequestIsProperlySigned(request) ||
		ensureRequestHasAValidToken(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"message": "Unauthorized",
			},
		}
	}

	if !ensureRequestIsNotExpired(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	return request, nil
}

/**
 *  Ensure that there is an authorization header
 */
func ensureRequestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Get("Authorization") != ""
}

func ensureRequestIsProperlySigned(request *Request) bool {
	return AdminRequestSignatureValidator(request)
}

func ensureRequestHasAValidToken(request *Request) bool {
	return AdminRequestTokenValidator(request)
}

func ensureRequestIsNotExpired(request *Request) bool {
	dateHeader := request.Headers().Get("X-Lbdb-Date")

	if dateHeader == "" {
		return false
	}

	date, err := strconv.ParseInt(dateHeader, 10, 64)

	if err != nil {
		return false
	}

	return time.Since(time.Unix(date, 0)) < 10*time.Second
}
