package auth

import (
	"encoding/base64"
	"encoding/json"
	"litebasedb/runtime/app/secrets"
	"strings"
)

type RequestToken struct {
	accessKey     *AccessKey
	AccessKeyId   string   `json:"access_key_id"`
	SignedHeaders []string `json:"signed_headers"`
	Signature     string   `json:"signature"`
}

func CaptureRequestToken(authorizationHeader string) *RequestToken {
	if authorizationHeader == "" {
		return nil
	}

	// base64_decode the authorization header
	rawDecodedText, err := base64.StdEncoding.DecodeString(authorizationHeader)

	if err != nil {
		return nil
	}

	headerParts := strings.Split(string(rawDecodedText), ";")
	token := map[string]string{}

	for _, headerPart := range headerParts {
		headerPartParts := strings.Split(headerPart, "=")
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

	return &RequestToken{
		AccessKeyId:   token["credential"],
		SignedHeaders: strings.Split(token["signed_headers"], ","),
		Signature:     token["signature"],
	}
}

func (requestToken *RequestToken) AccessKey() *AccessKey {
	if requestToken.accessKey != nil {
		return requestToken.accessKey
	}

	data := secrets.Manager().GetAccessKey(requestToken.AccessKeyId)

	requestToken.accessKey = &AccessKey{
		DatabaseUuid:          data.(map[string]string)["database_uuid"],
		BranchUuid:            data.(map[string]string)["branch_uuid"],
		AccessKeyId:           data.(map[string]string)["access_key_id"],
		AccessKeySecret:       data.(map[string]string)["access_key_secret"],
		ServerAccessKeySecret: data.(map[string]string)["server_access_key_secret"],
		Privileges:            map[string][]string{},
	}

	return requestToken.accessKey
}

func RequestTokenFromMap(input map[string]string) *RequestToken {
	return &RequestToken{
		AccessKeyId:   input["access_key_id"],
		SignedHeaders: strings.Split(input["signed_headers"], ","),
		Signature:     input["signature"],
	}
}

func (requestToken *RequestToken) GetDatabaseKey() string {
	return secrets.Manager().GetDatabaseKey(requestToken.AccessKeyId)
}

func (requestToken *RequestToken) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"access_key_id":  requestToken.AccessKeyId,
		"signed_headers": requestToken.SignedHeaders,
		"signature":      requestToken.Signature,
	}
}

func (requestToken *RequestToken) ToJson() string {
	jsonValue, err := json.Marshal(requestToken.ToMap())

	if err != nil {
		return ""
	}

	return string(jsonValue)
}
