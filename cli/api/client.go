package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"litebase/cli/config"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	BaseUrl        string
	defaultHeaders map[string]string
	httpClient     *http.Client
}

type Errors map[string][]string

func NewClient() (*Client, error) {
	defaultHeaders := map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	}

	if shouldUseBasicAuth() {
		defaultHeaders["Authorization"] = basicAuthHeader()
	}

	clusterUrl, err := clusterUrl()

	if err != nil {
		return nil, err
	}

	return &Client{
		BaseUrl:        clusterUrl,
		defaultHeaders: defaultHeaders,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}, nil
}

func (c *Client) Request(method, path string, data map[string]interface{}) (map[string]interface{}, Errors, error) {
	url := fmt.Sprintf("%s/%s", c.BaseUrl, strings.TrimLeft(path, "/"))

	jsonData, err := json.Marshal(data)

	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest(method, url, strings.NewReader(string(jsonData)))

	if err != nil {
		return nil, nil, err
	}

	for key, value := range c.defaultHeaders {
		req.Header.Set(key, value)
	}

	res, err := c.httpClient.Do(req)

	if err != nil {
		return nil, nil, err
	}

	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)

	if err != nil {
		return nil, nil, err
	}

	responseData := make(map[string]interface{})

	if len(body) != 0 {
		err = json.Unmarshal(body, &responseData)

		if err != nil {
			return nil, nil, err
		}
	}

	if res.StatusCode != 200 {
		if responseData["message"] != nil {
			return nil, nil, fmt.Errorf("%s", responseData["message"].(string))
		}

		if responseData["errors"] != nil {
			var errors = make(map[string][]string)

			for key, value := range responseData["errors"].(map[string]interface{}) {

				errors[key] = []string{}
				for _, v := range value.([]interface{}) {
					errors[key] = append(errors[key], v.(string))
				}
			}

			return nil, errors, nil
		}

		return nil, nil, fmt.Errorf("Request Error: %s", res.Status)
	}

	return responseData, nil, nil
}

func basicAuthHeader() string {
	var (
		username string
		password string
	)

	if config.GetUsername() != "" && config.GetPassword() != "" {
		username = config.GetUsername()
		password = config.GetPassword()
	} else {
		profile, err := config.GetCurrentProfile()

		if err != nil {
			return err.Error()
		}

		username = profile.Credentials.Username
		password = profile.Credentials.Password
	}

	return fmt.Sprintf(
		"Basic %s",
		base64.StdEncoding.EncodeToString(
			[]byte(fmt.Sprintf("%s:%s", username, password)),
		),
	)
}

func clusterUrl() (string, error) {
	if config.GetUrl() != "" {
		return config.GetUrl(), nil
	}

	profile, err := config.GetCurrentProfile()

	if err != nil {
		return "", err
	}

	return profile.Cluster, nil
}

func shouldUseBasicAuth() bool {
	if config.GetUsername() != "" && config.GetPassword() != "" {
		return true
	}

	profile, err := config.GetCurrentProfile()

	if err != nil {
		return false
	}

	return profile.Type == config.ProfileType(config.ProfileTypeBasicAuth)
}
