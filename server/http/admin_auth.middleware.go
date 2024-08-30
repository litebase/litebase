package http

import (
	"litebase/server/auth"
	"strconv"
	"time"
)

func AdminAuth(request *Request) (*Request, Response) {
	if basicAuth(request) {
		return request, Response{}
	}

	if !ensureAdminRequestHasAnAuthorizationHeader(request) ||
		!ensureAdminRequestIsProperlySigned(request) ||
		ensureAdminRequestHasAValidToken(request) {
		return request, Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"message": "Unauthorized",
			},
		}
	}

	if !ensureAdminRequestIsNotExpired(request) {
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

func basicAuth(request *Request) bool {
	username, password, ok := request.BaseRequest.BasicAuth()

	if ok {
		return auth.UserManager().Authenticate(username, password)
	}

	return false
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
