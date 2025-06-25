package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/config"
)

type Client struct {
	BaseURL        *url.URL
	Config         *config.Configuration
	defaultHeaders map[string]string
	httpClient     *http.Client
}

type Errors map[string][]string

func NewClient(configuration *config.Configuration) (*Client, error) {
	c := &Client{
		Config: configuration,
		defaultHeaders: map[string]string{
			"Content-Type": "application/json",
			"Accept":       "application/json",
		},
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}

	clusterURL, err := c.clusterURL()

	if err != nil {
		return nil, err
	}

	c.BaseURL = clusterURL

	return c, nil
}

func (c *Client) Request(method, path string, data map[string]any) (map[string]any, Errors, error) {
	url := fmt.Sprintf("%s/%s", c.BaseURL, strings.TrimLeft(path, "/"))

	var jsonData []byte
	var err error

	if data != nil {
		jsonData, err = json.Marshal(data)

		if err != nil {
			return nil, nil, err
		}
	}

	if c.shouldUseAccessKey() {
		host := c.BaseURL.Hostname()

		if c.BaseURL.Port() != "" {
			host = fmt.Sprintf("%s:%s", c.BaseURL.Hostname(), c.BaseURL.Port())
		}

		c.defaultHeaders["X-LBDB-Date"] = fmt.Sprintf("%d", time.Now().UTC().Unix())
		c.defaultHeaders["Host"] = host
		c.defaultHeaders["Authorization"] = c.accessKeyHeader(
			method,
			path,
			c.defaultHeaders,
			jsonData,
		)
	} else if c.shouldUseBasicAuth() {
		c.defaultHeaders["Authorization"] = c.basicAuthHeader()
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

	responseData := make(map[string]any)

	if len(body) != 0 {
		err = json.Unmarshal(body, &responseData)

		if err != nil {
			return nil, nil, err
		}
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		if responseData["message"] != nil {
			return nil, nil, fmt.Errorf("%s", responseData["message"].(string))
		}

		if responseData["errors"] != nil {
			var errors = make(map[string][]string)

			for key, value := range responseData["errors"].(map[string]any) {

				errors[key] = []string{}
				for _, v := range value.([]any) {
					errors[key] = append(errors[key], v.(string))
				}
			}

			return nil, errors, nil
		}

		return nil, nil, fmt.Errorf("Request Error: %s", res.Status)
	}

	return responseData, nil, nil
}

func (c *Client) accessKeyHeader(method, path string, headers map[string]string, body []byte) string {
	return auth.SignRequest(
		c.Config.GetAccessKeyId(),
		c.Config.GetAccessKeySecret(),
		method,
		path,
		headers,
		body,
		map[string]string{},
	)
}

func (c *Client) basicAuthHeader() string {
	var (
		username string
		password string
	)

	if c.Config.GetUsername() != "" && c.Config.GetPassword() != "" {
		username = c.Config.GetUsername()
		password = c.Config.GetPassword()
	} else {
		profile, err := c.Config.GetCurrentProfile()

		if err != nil {
			return err.Error()
		}

		username = profile.Credentials.Username
		password = profile.Credentials.Password
	}

	return fmt.Sprintf(
		"Basic %s",
		base64.StdEncoding.EncodeToString(
			fmt.Appendf(nil, "%s:%s", username, password),
		),
	)
}

func (c *Client) clusterURL() (*url.URL, error) {
	if c.Config.GetUrl() == "" && (c.Config.GetAccessKeyId() != "" || c.Config.GetUsername() != "") {
		return nil, config.ErrMissingClusterURL
	}

	if c.Config.GetUrl() != "" {
		url, err := url.Parse(c.Config.GetUrl())

		if err != nil {
			return nil, err
		}

		return url, nil
	}

	profile, err := c.Config.GetCurrentProfile()

	if err != nil {
		return nil, err
	}

	if profile.Cluster == "" {
		return nil, fmt.Errorf("cluster URL not found")
	}

	return url.Parse(profile.Cluster)
}

func (c *Client) shouldUseAccessKey() bool {
	return c.Config.GetAccessKeyId() != "" && c.Config.GetAccessKeySecret() != ""
}

func (c *Client) shouldUseBasicAuth() bool {
	if c.Config.GetUsername() != "" && c.Config.GetPassword() != "" {
		return true
	}

	profile, err := c.Config.GetCurrentProfile()

	if err != nil {
		return false
	}

	return profile.Type == config.ProfileType(config.ProfileTypeBasicAuth)
}
