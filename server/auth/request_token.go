package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

type RequestToken struct {
	accessKey     AccessKey
	AccessKeyId   string   `json:"access_key_id"`
	SignedHeaders []string `json:"signed_headers"`
	Signature     string   `json:"signature"`
}

func CaptureRequestToken(authorizationHeader string) RequestToken {
	if authorizationHeader == "" {
		return RequestToken{}
	}

	// base64_decode the authorization header
	rawDecodedText, err := base64.StdEncoding.DecodeString(authorizationHeader)

	if err != nil {
		return RequestToken{}
	}

	headerParts := strings.Split(string(rawDecodedText), ";")
	token := map[string]string{}

	for _, headerPart := range headerParts {
		headerPartParts := strings.Split(headerPart, "=")

		if len(headerPartParts) != 2 {
			return RequestToken{}
		}

		token[headerPartParts[0]] = headerPartParts[1]
	}

	if _, ok := token["credential"]; !ok {
		return RequestToken{}
	}

	if _, ok := token["signed_headers"]; !ok {
		return RequestToken{}
	}

	if _, ok := token["signature"]; !ok {
		return RequestToken{}
	}

	return RequestToken{
		AccessKeyId:   token["credential"],
		SignedHeaders: strings.Split(token["signed_headers"], ","),
		Signature:     token["signature"],
	}
}

func (requestToken RequestToken) AccessKey(databaseUuid string) AccessKey {
	if requestToken.accessKey.AccessKeyId != "" {
		return requestToken.accessKey
	}

	data, err := AccessKeyManager().Get(requestToken.AccessKeyId)

	if err != nil {
		return AccessKey{}
	}

	requestToken.accessKey = data

	return requestToken.accessKey
}

func RequestTokenFromMap(input map[string]string) RequestToken {
	return RequestToken{
		AccessKeyId:   input["access_key_id"],
		SignedHeaders: strings.Split(input["signed_headers"], ","),
		Signature:     input["signature"],
	}
}

// func (requestToken *RequestToken) GetDatabaseKey(databaseUuid string) (string, error) {
// 	return SecretsManager().GetDatabaseKey(databaseUuid, requestToken.AccessKeyId)
// }

func (requestToken RequestToken) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"access_key_id":  requestToken.AccessKeyId,
		"signed_headers": requestToken.SignedHeaders,
		"signature":      requestToken.Signature,
	}
}

func (requestToken RequestToken) ToJson() string {
	jsonValue, err := json.Marshal(requestToken.ToMap())

	if err != nil {
		return ""
	}

	return string(jsonValue)
}

func (requestToken RequestToken) Valid() bool {
	return requestToken.AccessKeyId != "" && len(requestToken.SignedHeaders) > 0 && requestToken.Signature != ""
}
