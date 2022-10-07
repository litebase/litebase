package http

import (
	"litebasedb/runtime/app/auth"
)

type Request struct {
	headers     *Headers
	Body        map[string]string
	Method      string
	Path        string
	QueryParams map[string]string
	Route       *Route
}

func NewRequest(Headers map[string]string, Method string, Path string) *Request {
	return &Request{
		Method:  Method,
		Path:    Path,
		headers: NewHeaders(Headers),
	}
}

func (r *Request) All() map[string]string {
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

func (request *Request) RequestToken() *auth.RequestToken {
	return auth.CaptureRequestToken(request.headers.Get("Authorization"))
}

func (request Request) SetRoute(route *Route) Request {
	request.Route = route

	return request
}
