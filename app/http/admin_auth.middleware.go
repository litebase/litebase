package http

import (
	"strconv"
	"time"
)

func AdminAuth(request *Request) (*Request, *Response) {
	if !ensureAdminRequestHasAnAuthorizationHeader(request) ||
		!ensureAdminRequestIsProperlySigned(request) ||
		ensureAdminRequestHasAValidToken(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"message": "Unauthorized",
			},
		}
	}

	if !ensureAdminRequestIsNotExpired(request) {
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

/*
Ensure that there is an authorization header
*/
func ensureAdminRequestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Get("Authorization") != ""
}

func ensureAdminRequestIsProperlySigned(request *Request) bool {
	return AdminRequestSignatureValidator(request)
}

func ensureAdminRequestHasAValidToken(request *Request) bool {
	return AdminRequestTokenValidator(request)
}

func ensureAdminRequestIsNotExpired(request *Request) bool {
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
