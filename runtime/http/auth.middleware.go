package http

import (
	_auth "litebasedb/internal/auth"
	"litebasedb/runtime/auth"
	"log"
	"strconv"
	"time"
)

type AuthMiddleware struct {
}

func (middleware *AuthMiddleware) Handle(request *Request) (*Request, *Response) {
	if !middleware.ensureReuestHasAnAuthorizationHeader(request) ||
		!middleware.ensureRequestIsProperlySigned(request) ||
		!middleware.ensureRequestIsProperlySignedByServer(request) ||
		!middleware.ensureRequestPassesChallenge(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"message": "Unauthorized",
			},
		}
	}

	if !middleware.ensureRequestIsNotExpired(request) {
		return nil, &Response{
			StatusCode: 401,
			Body: map[string]interface{}{
				"message": "Unauthorized",
			},
		}
	}

	return request, nil
}

func (middleware *AuthMiddleware) ensureReuestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func (middleware *AuthMiddleware) ensureRequestIsNotExpired(request *Request) bool {
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

func (middleware *AuthMiddleware) ensureRequestIsProperlySigned(request *Request) bool {
	return HandleRequestSignatureValidation(request, "Authorization", false, false)
}

func (middleware *AuthMiddleware) ensureRequestIsProperlySignedByServer(request *Request) bool {
	return HandleRequestSignatureValidation(request, "Server-Authorization", true, false)
}

func (middleware *AuthMiddleware) ensureRequestPassesChallenge(request *Request) bool {
	if request.headers.Get("X-Lbdb-Challenge") == "" {
		return false
	}

	challenge, err := auth.SecretsManager().Decrypt(
		_auth.FindSignature(request.headers.Get("X-Lbdb-Signature")),
		request.headers.Get("X-Lbdb-Challenge"),
	)

	if err != nil {
		log.Println(err)
		return false
	}

	return challenge["value"] == request.headers.Get("X-Lbdb-Date")
}
