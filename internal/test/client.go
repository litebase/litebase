package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

type TestClient struct {
	AccessKey *auth.AccessKey
	Password  string
	Token     *auth.Token
	Username  string
	URL       string
}

func (c *TestClient) Send(path string, method string, data any) (map[string]any, int, error) {
	var requestURL string

	if !strings.Contains(path, "http://") && !strings.Contains(path, "https://") {
		requestURL = c.URL + path
	} else {
		requestURL = path
	}

	// Parse the URL to separate path and query parameters
	parsedURL, err := url.Parse(requestURL)

	if err != nil {
		return nil, 0, err
	}

	request, err := http.NewRequest(method, requestURL, nil)

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
		"Host":            request.URL.Host,
		"Content-Type":    "application/json",
		"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	if c.AccessKey != nil {
		// Parse query parameters for request signing
		queryParams := make(map[string]string)

		for key, values := range parsedURL.Query() {
			if len(values) > 0 {
				queryParams[key] = values[0] // Use the first value if multiple values exist
			}
		}

		signature := auth.SignRequest(
			c.AccessKey.AccessKeyID,
			c.AccessKey.AccessKeySecret,
			method,
			parsedURL.Path,
			headers,
			jsonData,
			queryParams,
		)

		request.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))
	} else if c.Token != nil {
		value, err := c.Token.Value()

		if err != nil {
			return nil, 0, err
		}

		request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", value))
	} else {
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

	if response.Header.Get("Content-Length") == "0" || response.StatusCode == 204 {
		// No content response, return nil body
		return nil, response.StatusCode, nil
	}

	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("Error closing response body:", "error", err)
		}
	}()

	var responseData map[string]any

	if err := json.NewDecoder(response.Body).Decode(&responseData); err != nil {
		return nil, 0, err
	}

	return responseData, response.StatusCode, nil
}
