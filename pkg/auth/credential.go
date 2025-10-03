package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

type CredentialType string

const (
	CredentialTypeAccessKey CredentialType = "access_key"
	CredentialTypeBasicAuth CredentialType = "basic_auth"
	CredentialTypeToken     CredentialType = "token"
	CredentialTypeUnknown   CredentialType = "unknown"
)

type Credential struct {
	accessKey        *AccessKey
	auth             *Auth
	CredentialID     string `json:"credential_id"`
	CredentialString string `json:"credential_string"`
	hash             [32]byte
	Scheme           string   `json:"scheme"`
	SignedHeaders    []string `json:"signed_headers"`
	token            *Token
	user             *User
}

// Capture credentials from a request authorization header.
func CaptureCredential(auth *Auth, authorizationHeader string) *Credential {
	if authorizationHeader == "" {
		return nil
	}

	authorizationHeaderParts := strings.SplitN(authorizationHeader, " ", 2)

	if len(authorizationHeaderParts) != 2 {
		return nil
	}

	scheme := strings.TrimSpace(authorizationHeaderParts[0])
	credentialValue := strings.TrimSpace(authorizationHeaderParts[1])

	if scheme == "" {
		return nil
	}

	if scheme == "Basic" {
		// base64_decode the authorization header
		rawDecodedText, err := base64.StdEncoding.DecodeString(credentialValue)

		if err != nil {
			return nil
		}

		parts := strings.SplitN(string(rawDecodedText), ":", 2)

		if len(parts) != 2 {
			return nil
		}

		username := strings.TrimSpace(parts[0])

		user, err := auth.UserManager.Get(username)

		if err != nil {
			return nil
		}

		return &Credential{
			auth:         auth,
			CredentialID: username,
			Scheme:       scheme,
			user:         user,
		}
	}

	if scheme == "Bearer" {
		//tokenID = lbtk_ + 16 characters
		tokenIDLength := len("lbtk_") + 16
		tokenID := credentialValue[0:tokenIDLength]
		tokenSecret := strings.TrimSpace(credentialValue[tokenIDLength:])

		token, err := auth.TokenManager.Get(tokenID)

		if err != nil {
			return nil
		}

		return &Credential{
			auth:             auth,
			CredentialID:     tokenID,
			CredentialString: tokenSecret,
			Scheme:           scheme,
			token:            token,
		}
	}

	// base64_decode the authorization header
	rawDecodedText, err := base64.StdEncoding.DecodeString(credentialValue)

	if err != nil {
		return nil
	}

	headerParts := strings.Split(string(rawDecodedText), ";")
	token := map[string]string{}

	for _, headerPart := range headerParts {
		headerPartParts := strings.Split(headerPart, "=")

		if len(headerPartParts) != 2 {
			return nil
		}

		token[headerPartParts[0]] = headerPartParts[1]
	}

	if _, ok := token["credential"]; !ok {
		return nil
	}

	if _, ok := token["signed_headers"]; !ok {
		return nil
	}

	if _, ok := token["signature"]; !ok {
		return nil
	}

	return &Credential{
		auth:             auth,
		CredentialID:     token["credential"],
		Scheme:           scheme,
		SignedHeaders:    strings.Split(token["signed_headers"], ","),
		CredentialString: token["signature"],
	}
}

// Return an AccessKey if it exists on the request.
func (c Credential) AccessKey() *AccessKey {
	if c.accessKey != nil {
		return c.accessKey
	}

	data, err := c.auth.AccessKeyManager.Get(c.CredentialID)

	if err != nil {
		return nil
	}

	c.accessKey = data

	return c.accessKey
}

// Return the hash of the Credential.
func (c *Credential) Hash() [32]byte {
	if c.hash != [32]byte{} {
		return c.hash
	}

	statementsJSON, _ := json.Marshal(c.Statements())

	c.hash = sha256.Sum256(fmt.Appendf(nil, "%s:%s", c.CredentialID, statementsJSON))

	return c.hash
}

// Check if the request credential is invalid.
func (c Credential) Invalid() bool {
	return c.CredentialID == ""
}

// Determine if the request credential is an AccessKey.
func (c Credential) IsAccessKey() bool {
	return c.Type() == CredentialTypeAccessKey
}

// Determine if the request credential is a BasicAuth.
func (c Credential) IsBasicAuth() bool {
	return c.Type() == CredentialTypeBasicAuth
}

// Determine if the request credential is a Token.
func (c Credential) IsToken() bool {
	return c.Type() == CredentialTypeToken
}

// Return the statements associated with the credential.
func (c Credential) Statements() []Statement {
	if c.Type() == CredentialTypeBasicAuth && c.User() != nil {
		return c.User().Statements
	}

	if c.Type() == CredentialTypeToken && c.Token() != nil {
		return c.Token().Statements
	}

	if c.Type() == CredentialTypeAccessKey && c.AccessKey() != nil {
		return c.AccessKey().Statements
	}

	return nil
}

// Return a Token if it exists.
func (c Credential) Token() *Token {
	if c.token != nil {
		return c.token
	}

	return nil
}

// Determine the type of credential.
func (c Credential) Type() CredentialType {
	switch c.Scheme {
	case "Litebase-HMAC-SHA256":
		return CredentialTypeAccessKey
	case "Bearer":
		return CredentialTypeToken
	case "Basic":
		return CredentialTypeBasicAuth
	}

	return CredentialTypeUnknown
}

// Return a User if it exists.
func (c Credential) User() *User {
	if c.user != nil {
		return c.user
	}

	return nil
}

// Determine if the credential is valid.
func (c Credential) Valid() bool {
	switch c.Type() {
	case CredentialTypeAccessKey:
		return c.CredentialID != "" && len(c.SignedHeaders) > 0 && c.CredentialString != ""
	case CredentialTypeToken:
		return c.CredentialID != ""
	case CredentialTypeBasicAuth:
		return c.CredentialID != ""
	}

	return false
}

// Set the AccessKey for the credential.
func (c *Credential) WithAccessKey(accessKey *AccessKey) *Credential {
	c.CredentialID = accessKey.AccessKeyID
	c.Scheme = "Litebase-HMAC-SHA256"
	c.accessKey = accessKey

	return c
}

// Set the Token for the credential.
func (c *Credential) WithToken(token *Token) *Credential {
	c.CredentialID = token.TokenID
	c.Scheme = "Bearer"
	c.token = token

	return c
}

// Set the User for the credential.
func (c *Credential) WithUser(user *User) *Credential {
	c.CredentialID = user.Username
	c.Scheme = "Basic"
	c.user = user

	return c
}
