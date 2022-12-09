package http

import (
	"litebasedb/router/auth"
	"strconv"
	"time"
)

func Auth(request *Request) (*Request, *Response) {
	if !ensureReuestHasAnAuthorizationHeader(request) || !ensureRequestCanAccessDatabase(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Unauthorized",
			},
		}
	}

	if !ensureAuthRequestIsNotExpired(request) {
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

func ensureRequestCanAccessDatabase(request *Request) bool {
	token := auth.CaptureRequestToken(request.Headers().Get("Authorization"))

	if token == nil {
		return false
	}

	databaseKey := request.Subdomains()[0]

	return auth.SecretsManager().HasAccessKey(databaseKey, token.AccessKeyId)
}

func ensureReuestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Get("Authorization") != ""
}

func ensureAuthRequestIsNotExpired(request *Request) bool {
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
