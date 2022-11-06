package http

import (
	"encoding/json"
	"litebasedb/runtime/auth"
)

type Request struct {
	headers     *Headers
	Body        map[string]interface{}
	Method      string
	Path        string
	QueryParams map[string]string
	Route       *Route
}

func NewRequest(Headers map[string]string, Method string, Path string, Body string, QueryParams map[string]string) *Request {
	body := map[string]interface{}{}

	json.Unmarshal([]byte(Body), &body)

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
