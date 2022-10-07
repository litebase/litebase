package http

import (
	"log"
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

	parsedTime, err := time.Parse("20060102", dateHeader)

	if err != nil {
		log.Println(err)

		return false
	}

	return time.Since(parsedTime) < 10*time.Second
}

func (middleware *AdminMiddleware) ensureRequestIsProperlySigned(request *Request) bool {
	return HandleAdminRequestSignatureValidation(request)
}
