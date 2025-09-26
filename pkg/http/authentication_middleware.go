package http

import (
	"context"
	"strconv"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

func basicAuth(request *Request) bool {
	username, password, ok := request.BaseRequest.BasicAuth()

	if ok {
		return request.cluster.Auth.UserManager.Authenticate(username, password)
	}

	return false
}

func tokenAuth(credential *auth.Credential) bool {
	token := credential.Token()

	if token == nil {
		return false
	}

	return token.Authenticate(credential.CredentialString)
}

func Authentication(ctx context.Context, request *Request) (*Request, Response) {
	credential := request.Credential()

	if credential == nil {
		return request, UnauthorizedResponse()
	}

	switch credential.Type() {
	case auth.CredentialTypeBasicAuth:
		if !basicAuth(request) {
			return request, UnauthorizedResponse()
		}
	case auth.CredentialTypeToken:
		if !tokenAuth(credential) {
			return request, UnauthorizedResponse()
		}
	case auth.CredentialTypeAccessKey:
		if !ensureRequestHasAnAuthorizationHeader(request) ||
			!ensureRequestIsProperlySigned(request) {
			return request, UnauthorizedResponse()
		}

		if !ensureRequestIsNotExpired(request) {
			return request, UnauthorizedResponse()
		}
	default:
		return request, UnauthorizedResponse()
	}

	return request, Response{}
}

func ensureRequestHasAnAuthorizationHeader(request *Request) bool {
	return request.Headers().Has("Authorization")
}

func ensureRequestIsNotExpired(request *Request) bool {
	dateHeader := request.Headers().Get("X-Litebase-Date")

	if dateHeader == "" {
		return false
	}

	parseInt, err := strconv.ParseInt(dateHeader, 10, 64)

	if err != nil {
		return false
	}

	parsedTime := time.Unix(parseInt, 0).UTC()

	return time.Since(parsedTime) < 10*time.Second
}

func ensureRequestIsProperlySigned(request *Request) bool {
	return RequestSignatureValidator(request)
}
