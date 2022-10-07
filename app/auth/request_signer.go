package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

func SignRequest(accessKeyID string,
	accessKeySecret string,
	method string,
	path string,
	headers map[string]string,
	data map[string]string,
	queryParams map[string]string,
) string {
	for key, value := range headers {
		delete(headers, key)

		headers[strings.ToLower(key)] = value
	}

	for key := range headers {
		if !slices.Contains([]string{"content-type", "host", "x-lbdb-date"}, key) {
			delete(data, key)
		}
	}

	for key, value := range queryParams {
		delete(queryParams, key)

		queryParams[strings.ToLower(key)] = value
	}

	for key, value := range data {
		delete(data, key)

		data[strings.ToLower(key)] = value
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

	if len(data) > 0 {
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
		method,
		"/" + strings.TrimLeft(path, "/"),
		string(jsonHeaders),
		string(jsonQueryParams),
		string(jsonBody),
	}, "")

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := string(signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(time.Now().Format("20060102")))
	dateHash.Write([]byte(accessKeySecret))
	date := string(dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte("litebasedb_request"))
	serviceHash.Write([]byte(date))
	service := string(serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(signedRequest))
	signatureHash.Write([]byte(service))
	signature := string(signatureHash.Sum(nil))

	token := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("credential=%s;signed_headers=content-type,host,x-lbdb-date;signature=%s", accessKeyID, signature)),
	)

	return token
}
