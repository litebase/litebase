package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/internal/utils"
	"strings"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type AdminRequestSignatureValidator struct{}

func HandleAdminRequestSignatureValidation(request *Request) bool {
	if request.RequestToken("Authorization") == nil {
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

	headers := make(map[string]string)
	maps.Copy(headers, request.Headers().All())

	// Change all the keys to lower case
	for key, value := range headers {
		delete(headers, key)
		headers[utils.TransformHeaderKey(key)] = value
	}

	// Remove headers that are not signed
	for key := range headers {
		if !slices.Contains(request.RequestToken("Authorization").SignedHeaders, key) {
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
		jsonBody, err = json.Marshal(body)

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

	hashString := strings.Join([]string{
		config.Get("encryption_key"),
		config.Get("region"),
		config.Get("app_host"),
		config.Get("env"),
	}, ":")

	requestHash := sha256.New()
	requestHash.Write([]byte(hashString))
	secret := fmt.Sprintf("%x", requestHash.Sum(nil))

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := fmt.Sprintf("%x", signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(secret))
	dateHash.Write([]byte(time.Now().UTC().Format("20060102")))
	date := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(date))
	serviceHash.Write([]byte("litebasedb_request"))
	service := fmt.Sprintf("%x", serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(service))
	signatureHash.Write([]byte(signedRequest))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(signature), []byte(request.RequestToken("Authorization").Signature)) == 1
}
