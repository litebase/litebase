package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/litebase/litebase/internal/utils"

	"golang.org/x/exp/slices"
)

func SignRequest(
	accessKeyID string,
	accessKeySecret string,
	method string,
	path string,
	headers map[string]string,
	data map[string]any,
	queryParams map[string]string,
) string {
	for key, value := range headers {
		delete(headers, key)

		headers[utils.TransformHeaderKey(key)] = value
	}

	for key := range headers {
		if !slices.Contains([]string{"content-type", "host", "x-lbdb-date"}, key) {
			delete(headers, key)
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
			log.Fatal(err)
		}
	} else {
		jsonQueryParams = []byte("{}")
	}

	if len(data) > 0 {
		jsonBody, err = json.Marshal(data)

		if err != nil {
			log.Fatal(err)
		}
	} else {
		jsonBody = []byte("{}")
	}

	if err != nil {
		log.Fatal(err)
	}

	requestString := strings.Join([]string{
		method,
		fmt.Sprintf("/%s", strings.TrimLeft(path, "/")),
		string(jsonHeaders),
		string(jsonQueryParams),
		string(jsonBody),
	}, "")

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := fmt.Sprintf("%x", signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(accessKeySecret))
	dateHash.Write([]byte(headers["x-lbdb-date"]))
	date := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(date))
	serviceHash.Write([]byte("litebasedb_request"))
	service := fmt.Sprintf("%x", serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(service))
	signatureHash.Write([]byte(signedRequest))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	token := base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("credential=%s;signed_headers=content-type,host,x-lbdb-date;signature=%s", accessKeyID, signature)),
	)

	return token
}
