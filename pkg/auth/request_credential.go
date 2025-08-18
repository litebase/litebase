package auth

import (
	"encoding/base64"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

type RequestCredentialType string

const (
	RequestCredentialTypeAccessKey RequestCredentialType = "access_key"
	RequestCredentialTypeBasicAuth RequestCredentialType = "basic_auth"
	RequestCredentialTypeToken     RequestCredentialType = "token"
	RequestCredentialTypeUnknown   RequestCredentialType = "unknown"
)

type RequestCredential struct {
	accessKey    *AccessKey
	auth         *Auth
	CredentialID string `json:"credential_id"`
	// accessKeyManager *AccessKeyManager
	Scheme        string   `json:"scheme"`
	SignedHeaders []string `json:"signed_headers"`
	Signature     string   `json:"signature"`
	token         *Token
	user          *User
}

// Capture request credentials from the request.
func CaptureRequestCredential(auth *Auth, authorizationHeader string) RequestCredential {
	if authorizationHeader == "" {
		return RequestCredential{}
	}

	authorizationHeaderParts := strings.SplitN(authorizationHeader, " ", 2)

	if len(authorizationHeaderParts) != 2 {
		return RequestCredential{}
	}

	scheme := strings.TrimSpace(authorizationHeaderParts[0])
	credentialValue := strings.TrimSpace(authorizationHeaderParts[1])

	if scheme == "" {
		return RequestCredential{}
	}

	if scheme == "Basic" {
		// base64_decode the authorization header
		rawDecodedText, err := base64.StdEncoding.DecodeString(credentialValue)

		if err != nil {
			return RequestCredential{}
		}

		parts := strings.SplitN(string(rawDecodedText), ":", 2)

		if len(parts) != 2 {
			return RequestCredential{}
		}

		username := strings.TrimSpace(parts[0])

		user, err := auth.UserManager.Get(username)

		if err != nil {
			return RequestCredential{}
		}

		return RequestCredential{
			auth:         auth,
			CredentialID: username,
			Scheme:       scheme,
			user:         user,
		}
	}

	if scheme == "Bearer" {
		//tokenID = lbdbtk_ + 32 characters
		tokenIDLength := len("lbdbtk_") + 32
		tokenID := credentialValue[0:tokenIDLength]
		tokenSecret := strings.TrimSpace(credentialValue[tokenIDLength:])

		token, err := auth.TokenManager.Get(tokenID)

		if err != nil {
			return RequestCredential{}
		}

		// Use bcrypt to compare the token hash
		if bcrypt.CompareHashAndPassword([]byte(token.Hash()), []byte(tokenSecret)) != nil {
			return RequestCredential{}
		}

		return RequestCredential{
			auth:         auth,
			CredentialID: tokenID,
			Scheme:       scheme,
			token:        token,
		}
	}

	// base64_decode the authorization header
	rawDecodedText, err := base64.StdEncoding.DecodeString(credentialValue)

	if err != nil {
		return RequestCredential{}
	}

	headerParts := strings.Split(string(rawDecodedText), ";")
	token := map[string]string{}

	for _, headerPart := range headerParts {
		headerPartParts := strings.Split(headerPart, "=")

		if len(headerPartParts) != 2 {
			return RequestCredential{}
		}

		token[headerPartParts[0]] = headerPartParts[1]
	}

	if _, ok := token["credential"]; !ok {
		return RequestCredential{}
	}

	if _, ok := token["signed_headers"]; !ok {
		return RequestCredential{}
	}

	if _, ok := token["signature"]; !ok {
		return RequestCredential{}
	}

	return RequestCredential{
		auth:          auth,
		CredentialID:  token["credential"],
		Scheme:        scheme,
		SignedHeaders: strings.Split(token["signed_headers"], ","),
		Signature:     token["signature"],
	}
}

// Return an AccessKey if it exists on the request.
func (requestCredential RequestCredential) AccessKey() *AccessKey {
	if requestCredential.accessKey != nil {
		return requestCredential.accessKey
	}

	data, err := requestCredential.auth.AccessKeyManager.Get(requestCredential.CredentialID)

	if err != nil {
		return nil
	}

	requestCredential.accessKey = data

	return requestCredential.accessKey
}

// Check if the request credential is invalid.
func (requestCredential RequestCredential) Invalid() bool {
	return requestCredential.CredentialID == ""
}

// Determine if the request credential is an AccessKey.
func (requestCredential RequestCredential) IsAccessKey() bool {
	return requestCredential.Type() == RequestCredentialTypeAccessKey
}

// Determine if the request credential is a BasicAuth.
func (requestCredential RequestCredential) IsBasicAuth() bool {
	return requestCredential.Type() == RequestCredentialTypeBasicAuth
}

// Determine if the request credential is a Token.
func (requestCredential RequestCredential) IsToken() bool {
	return requestCredential.Type() == RequestCredentialTypeToken
}

// Return a Token if it exists on the request.
func (requestCredential RequestCredential) Token() *Token {
	if requestCredential.token != nil {
		return requestCredential.token
	}

	return nil
}

// Determine the type of the request credential.
func (requestCredential RequestCredential) Type() RequestCredentialType {
	switch requestCredential.Scheme {
	case "Litebase-HMAC-SHA256":
		return RequestCredentialTypeAccessKey
	case "Bearer":
		return RequestCredentialTypeToken
	case "Basic":
		return RequestCredentialTypeBasicAuth
	}

	return RequestCredentialTypeUnknown
}

// Return a User if it exists on the request.
func (requestCredential RequestCredential) User() *User {
	if requestCredential.user != nil {
		return requestCredential.user
	}

	return nil
}

// Determine if the request credential is valid.
func (requestCredential RequestCredential) Valid() bool {
	switch requestCredential.Type() {
	case RequestCredentialTypeAccessKey:
		return requestCredential.CredentialID != "" && len(requestCredential.SignedHeaders) > 0 && requestCredential.Signature != ""
	case RequestCredentialTypeToken:
		return requestCredential.CredentialID != ""
	case RequestCredentialTypeBasicAuth:
		return requestCredential.CredentialID != ""
	}

	return false
}
