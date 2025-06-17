package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

type TestClient struct {
	AccessKey *auth.AccessKey
	Username  string
	Password  string
	URL       string
}

func (c *TestClient) Send(path string, method string, data any) (map[string]any, int, error) {
	var url string
	if !strings.Contains(path, "http://") && !strings.Contains(path, "https://") {
		url = c.URL + path
	} else {
		url = path
	}

	request, err := http.NewRequest(method, url, nil)

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
		"X-LBDB-Date":  fmt.Sprintf("%d", time.Now().UTC().Unix()),
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	jsonData, err = json.Marshal(data)
	if err != nil {
		return nil, 0, err
	}

	jsonMap := make(map[string]any)
	if jsonData != nil {
		if err := json.Unmarshal(jsonData, &jsonMap); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal JSON data: %w", err)
		}
	}

	if c.AccessKey != nil {
		signature := auth.SignRequest(
			c.AccessKey.AccessKeyId,
			c.AccessKey.AccessKeySecret,
			method,
			request.URL.Path,
			headers,
			jsonMap,
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
