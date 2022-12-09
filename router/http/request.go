package http

import (
	"encoding/json"
	"io"
	"litebasedb/router/auth"
	"strings"
)

type Request struct {
	Body        map[string]interface{}
	headers     *Headers
	Method      string
	Path        string
	QueryParams map[string]string
	Route       *Route
}

func NewRequest(Headers map[string]string, Method string, Path string, Body io.ReadCloser, QueryParams map[string]string) *Request {
	body := make(map[string]interface{})

	if Body != nil {
		decoder := json.NewDecoder(Body)
		decoder.Decode(&body)
	}

	return &Request{
		Body:        body,
		Method:      Method,
		Path:        Path,
		headers:     NewHeaders(Headers),
		QueryParams: QueryParams,
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
