package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/litebase/litebase/internal/utils"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

func RequestSignatureValidator(
	request *Request,
	header string,
) bool {
	if !request.RequestToken(header).Valid() {
		return false
	}

	var body map[string]any

	// Check the length of the content length header to determine if we should
	// attempt to read the body. Otherwise, this may preemptively read the body
	// of streaming requests.
	if request.Headers().Get("Content-Length") != "" {
		body = request.All()
	}

	// Get the body hash that was calculated when the body was first read
	bodyHash := request.BodyHash()

	if bodyHash == "" {
		// Hash of empty body if no body was provided
		emptyBodyHashSum := sha256.Sum256(nil)
		bodyHash = fmt.Sprintf("%x", emptyBodyHashSum)
	}

	// Change all the keys to lower case for other processing
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
		if !slices.Contains(request.RequestToken(header).SignedHeaders, key) {
			delete(headers, key)
		}
	}

	jsonHeaders, err := json.Marshal(headers)
	var jsonQueryParams []byte

	if len(queryParams) > 0 {
		jsonQueryParams, err = json.Marshal(queryParams)

		if err != nil {
			return false
		}
	} else {
		jsonQueryParams = []byte("{}")
	}

	if err != nil {
		panic(err)
	}

	requestString := strings.Join([]string{
		request.Method,
		"/" + strings.TrimLeft(request.Path(), "/"),
		string(jsonHeaders),
		string(jsonQueryParams),
		bodyHash,
	}, "")

	secret, err := request.cluster.Auth.SecretsManager.GetAccessKeySecret(request.RequestToken(header).AccessKeyID)

	if err != nil {
		return false
	}

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := fmt.Sprintf("%x", signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(secret))
	dateHash.Write([]byte(headers["x-lbdb-date"]))
	date := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(date))
	serviceHash.Write([]byte("litebase_request"))
	service := fmt.Sprintf("%x", serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(service))
	signatureHash.Write([]byte(signedRequest))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	return subtle.ConstantTimeCompare([]byte(signature), []byte(request.RequestToken(header).Signature)) == 1
}
