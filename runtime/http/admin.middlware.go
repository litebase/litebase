package http

import (
	"strconv"
	"time"
)

type AdminMiddleware struct{}

func (middleware *AdminMiddleware) Handle(request *Request) (*Request, *Response) {
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

func (middleware *AdminMiddleware) ensureReuestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func (middleware *AdminMiddleware) ensureRequestIsNotExpired(request *Request) bool {
	dateHeader := request.Headers().Get("X-LBDB-Date")

	if dateHeader == "" {
		return false
	}

	date, err := strconv.ParseInt(dateHeader, 10, 64)

	if err != nil {
		return false
	}

	return time.Since(time.Unix(date, 0)) < 10*time.Second
}

func (middleware *AdminMiddleware) ensureRequestIsProperlySigned(request *Request) bool {
	return HandleAdminRequestSignatureValidation(request)
}
