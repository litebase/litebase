package http

import (
	"strconv"
	"time"
)

type ConnectionAuthMiddleware struct{}

func (middleware *ConnectionAuthMiddleware) Handle(request *Request) (*Request, *Response) {
	if !middleware.ensureReuestHasAnAuthorizationHeader(request) ||
		!middleware.ensureRequestIsProperlySigned(request) {
		return nil, &Response{
			StatusCode: 401,
		}
	}

	if !middleware.ensureRequestIsNotExpired(request) {
		return nil, &Response{
			StatusCode: 401,
		}
	}

	return request, nil
}

func (middleware *ConnectionAuthMiddleware) ensureReuestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func (middleware *ConnectionAuthMiddleware) ensureRequestIsNotExpired(request *Request) bool {
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

func (middleware *ConnectionAuthMiddleware) ensureRequestIsProperlySigned(request *Request) bool {
	return HandleRequestSignatureValidation(request, "Authorization", false, true)
}
