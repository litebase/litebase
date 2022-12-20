package http

import (
	"encoding/json"
	"litebasedb/router/auth"
	"net/http"
	"strings"
)

type Request struct {
	BaseRequest *http.Request
	Body        map[string]interface{}
	headers     *Headers
	Method      string
	Path        string
	QueryParams map[string]string
	Route       *Route
}

func NewRequest(request *http.Request) *Request {
	headers := map[string]string{}

	for key, value := range request.Header {
		headers[key] = value[0]
	}

	query := request.URL.Query()
	queryParams := map[string]string{}

	for key, value := range query {
		queryParams[key] = value[0]
	}

	headers["host"] = request.Host

	body := make(map[string]interface{})

	if request.Body != nil {
		decoder := json.NewDecoder(request.Body)
		decoder.Decode(&body)
	}

	return &Request{
		BaseRequest: request,
		Body:        body,
		Method:      request.Method,
		Path:        request.URL.Path,
		headers:     NewHeaders(headers),
		QueryParams: queryParams,
	}
}

func (r *Request) All() map[string]interface{} {
	return r.Body
}

func (r *Request) Get(key string) interface{} {
	return r.Body[key]
}

func (request *Request) Headers() *Headers {
	return request.headers
}

func (request *Request) Param(key string) string {
	return request.Route.Get(key)
}

func (request *Request) QueryParam(key string) string {
	return request.QueryParams[key]
}

func (request *Request) RequestToken(header string) *auth.RequestToken {
	return auth.CaptureRequestToken(request.headers.Get(header))
}

func (request *Request) SetRoute(route *Route) *Request {
	request.Route = route

	return request
}

func (request *Request) Subdomains() []string {
	parts := strings.Split(request.Headers().Get("host"), ".")

	if len(parts) >= 4 {
		return parts[0:2]
	}
	return parts[0:1]
}
