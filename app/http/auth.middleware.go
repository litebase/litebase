package http

import (
	"log"
	"time"
)

type AuthMiddleware struct {
}

func (middleware *AuthMiddleware) Handle(request *Request) (*Request, *Response) {
	if !middleware.ensureReuestHasAnAuthorizationHeader(request) ||
		!middleware.ensureRequestIsProperlySigned(request) ||
		!middleware.ensureRequestIsProperlySignedByServer(request) {
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

func (middleware *AuthMiddleware) ensureReuestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func (middleware *AuthMiddleware) ensureRequestIsNotExpired(request *Request) bool {
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

func (middleware *AuthMiddleware) ensureRequestIsProperlySigned(request *Request) bool {
	return HandleRequestSignatureValidation(request, "Authorization", false, false)
}

func (middleware *AuthMiddleware) ensureRequestIsProperlySignedByServer(request *Request) bool {
	return HandleRequestSignatureValidation(request, "Server-Authorization", true, false)
}
