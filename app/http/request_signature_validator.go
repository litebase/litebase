package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/secrets"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

type RequestSignatureValidator struct{}

func HandleRequestSignatureValidation(
	request *Request,
	header string,
	serverToken bool,
	connectionToken bool,
) bool {
	if request.RequestToken() == nil {
		return false
	}

	body := request.All()

	// Change all the keys to lower case
	for key, value := range body {
		delete(body, key)
		body[strings.ToLower(key)] = value
	}

	queryParams := request.QueryParams

	// Change all the keys to lower case
	for key, value := range queryParams {
		delete(queryParams, key)
		queryParams[strings.ToLower(key)] = value
	}

	headers := request.Headers().All()

	// Change all the keys to lower case
	for key, value := range headers {
		delete(headers, key)
		headers[strings.ToLower(key)] = value
	}

	// Remove headers that are not signed
	for key := range headers {
		if !slices.Contains(request.RequestToken().SignedHeaders, key) {
			delete(headers, key)
		}
	}

	jsonHeaders, err := json.Marshal(headers)
	var jsonQueryParams []byte
	var jsonBody []byte

	if len(queryParams) > 0 {
		jsonQueryParams, err = json.Marshal(queryParams)

		if err != nil {
			panic(err)
		}
	} else {
		jsonQueryParams = []byte("{}")
	}

	if len(body) > 0 {
		jsonBody, err = json.Marshal(queryParams)

		if err != nil {
			panic(err)
		}
	} else {
		jsonBody = []byte("{}")
	}

	if err != nil {
		panic(err)
	}

	requestString := strings.Join([]string{
		request.Method,
		"/" + strings.TrimLeft(request.Path, "/"),
		string(jsonHeaders),
		string(jsonQueryParams),
		string(jsonBody),
	}, "")

	secret := getSecret(
		request.RequestToken(),
		serverToken,
		connectionToken,
	)

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := string(signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(time.Now().Format("20060102")))
	dateHash.Write([]byte(secret))
	date := string(dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte("litebasedb_request"))
	serviceHash.Write([]byte(date))
	service := string(serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(signedRequest))
	signatureHash.Write([]byte(service))
	signature := string(signatureHash.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(signature), []byte(request.RequestToken().Signature)) == 1
}

func getSecret(requestToken *auth.RequestToken, serverToken bool, connectionToken bool) string {
	if serverToken {
		return secrets.Manager().GetServerSecret(requestToken.AccessKeyId)
	}

	if connectionToken {
		hash := sha256.New()
		hash.Write([]byte(
			strings.Join([]string{
				secrets.Manager().GetConnectionKey(
					config.Get("database_uuid"),
					config.Get("branc_uuid"),
				),
			}, ":")))

		return string(hash.Sum(nil))
	}

	return secrets.Manager().GetSecret(requestToken.AccessKeyId)
}
