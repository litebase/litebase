package http

import (
	"strings"
)

type Route struct {
	Params               map[string]string
	Handler              func(request *Request) *Response
	Method               string
	Path                 string
	RegisteredMiddleware []Middleware
}

func (route *Route) Get(key string) string {
	return route.Params[key]
}

func (route *Route) Handle(request *Request) *Response {
	route.setParams(request.SetRoute(route))
	var response *Response

	for _, registeredMiddleware := range route.RegisteredMiddleware {
		request, response = registeredMiddleware(request)

		if response != nil {
			return response
		}
	}

	return route.Handler(request)
}

func (route *Route) Middleware(middleware []Middleware) *Route {
	route.RegisteredMiddleware = append(route.RegisteredMiddleware, middleware...)

	return route
}

func (route *Route) setParams(request *Request) {
	var params = make(map[string]string)
	pathSegments := strings.Split(strings.TrimPrefix(route.Path, "/"), "/")
	segments := strings.Split(strings.TrimPrefix(request.Path, "/"), "/")

	for index, segment := range pathSegments {
		// Check if the segment starts with :
		if strings.HasPrefix(segment, ":") {
			var key = strings.TrimPrefix(segment, ":")
			var value = segments[index]

			params[key] = value
		}
	}

	route.Params = params
}
