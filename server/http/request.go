package http

import (
	"encoding/json"
	"fmt"
	"litebase/internal/validation"
	"litebase/server/auth"
	"litebase/server/database"
	"net/http"
	"strings"
)

type Request struct {
	BaseRequest  *http.Request
	Body         map[string]interface{}
	headers      Headers
	Method       string
	Path         string
	QueryParams  map[string]string
	requestToken auth.RequestToken
	Route        Route
	subdomains   []string
}

func NewRequest(request *http.Request) *Request {
	// ctx := request.Context()
	headers := make(map[string]string, len(request.Header))

	for key, value := range request.Header {
		headers[key] = value[0]
	}

	query := request.URL.Query()
	queryParams := make(map[string]string, len(query))

	for key, value := range query {
		queryParams[key] = value[0]
	}

	headers["host"] = request.Host

	// Parse the subdomains once
	parts := strings.Split(headers["host"], ".")

	subdomains := parts[0:1]

	if len(parts) >= 4 {
		subdomains = parts[0:2]
	}

	return &Request{
		BaseRequest: request,
		Body:        nil,
		Method:      request.Method,
		Path:        request.URL.Path,
		headers:     NewHeaders(headers),
		QueryParams: queryParams,
		subdomains:  subdomains,
	}
}

func (r *Request) All() map[string]interface{} {
	if r.Body == nil {
		body := make(map[string]interface{})
		json.NewDecoder(r.BaseRequest.Body).Decode(&body)
		r.BaseRequest.Body.Close()

		r.Body = body
	}

	return r.Body
}

func (r *Request) DatabaseKey() database.DatabaseKey {
	// Get the database key from the subdomain
	key := r.Subdomains()[0]

	if key == "" || len(r.Subdomains()) != 2 {
		return database.DatabaseKey{}
	}

	databaseKey, err := database.GetDatabaseKey(key)

	if err != nil {
		return database.DatabaseKey{}
	}

	return databaseKey
}

func (r *Request) Get(key string) interface{} {
	return r.All()[key]
}

func (request *Request) Headers() Headers {
	return request.headers
}

func (request *Request) Input(input any) (interface{}, error) {
	jsonData, err := json.Marshal(request.All())

	if err != nil {
		return nil, err
	}

	json.Unmarshal(jsonData, &input)

	return input, nil
}

func (request *Request) Param(key string) string {
	return request.BaseRequest.PathValue(key)
}

func (request *Request) QueryParam(key string, defaultValue ...string) string {
	value := request.QueryParams[key]

	if value == "" && len(defaultValue) > 0 {
		return defaultValue[0]
	}

	return value
}

func (request *Request) RequestToken(header string) auth.RequestToken {
	if !request.requestToken.Valid() {
		request.requestToken = auth.CaptureRequestToken(request.headers.Get(header))
	}

	return request.requestToken
}

func (request *Request) SetRoute(route Route) *Request {
	request.Route = route

	return request
}

func (request *Request) Subdomains() []string {
	return request.subdomains
}

func (request *Request) Validate(
	input interface{},
	messages map[string]string,
) map[string][]string {
	if err := validation.Validate(input); err != nil {
		var e map[string][]string = make(map[string][]string)
		for _, x := range err {
			if e[x.Field()] == nil {
				e[x.Field()] = []string{}
			}

			key := fmt.Sprintf("%s.%s", x.Field(), x.Tag())

			if messages[key] == "" {
				continue
			}

			e[x.Field()] = append(e[x.Field()], messages[key])
		}

		return e
	}

	return nil
}
