package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/litebase/litebase/server/auth"
)

type TestClient struct {
	AccessKey *auth.AccessKey
	Username  string
	Password  string
	URL       string
}

func (c *TestClient) Send(path string, method string, data map[string]any) (map[string]any, int, error) {
	request, err := http.NewRequest(method, c.URL+path, nil)

	if err != nil {
		return nil, 0, err
	}

	var jsonData []byte

	if data != nil {
		// Add JSON body
		jsonData, err = json.Marshal(data)

		if err != nil {
			return nil, 0, err
		}

		request.Body = io.NopCloser(bytes.NewReader(jsonData))
		request.ContentLength = int64(len(jsonData))
	}

	headers := map[string]string{
		"Host":         request.URL.Host,
		"Content-Type": "application/json",
		"X-LBDB-Date":  fmt.Sprintf("%d", time.Now().Unix()),
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	if c.AccessKey != nil {
		signature := auth.SignRequest(
			c.AccessKey.AccessKeyId,
			c.AccessKey.AccessKeySecret,
			method,
			path,
			headers,
			data,
			map[string]string{},
		)

		request.Header.Set("Authorization", signature)
	} else if c.AccessKey == nil {
		request.SetBasicAuth(
			c.Username,
			c.Password,
		)
	}

	client := &http.Client{}

	response, err := client.Do(request)

	if err != nil {
		return nil, 0, err
	}

	defer response.Body.Close()

	var responseData map[string]any

	if err := json.NewDecoder(response.Body).Decode(&responseData); err != nil {
		return nil, 0, err
	}

	return responseData, response.StatusCode, nil
}
