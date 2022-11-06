package http

import (
	"litebasedb/runtime/event"
	"regexp"
	"sort"
	"strings"
)

type RouterInstance struct {
	DefaultRoute *Route
	Initialized  bool
	Keys         map[string][]RouteKey
	Routes       map[string]map[string]*Route
}

type RouteKey struct {
	Route string
	Regex string
}

var StaticRouter *RouterInstance

func Router() *RouterInstance {
	if StaticRouter == nil {
		StaticRouter = &RouterInstance{}
	}

	return StaticRouter
}

func (router *RouterInstance) compileKeys(method string) []RouteKey {
	if router.Keys[method] != nil {
		return router.Keys[method]
	}

	var keys = make([]string, 0, len(router.Routes[method]))

	for key := range router.Routes[method] {
		keys = append(keys, key)
	}

	var compiledKeys []RouteKey

	for _, key := range keys {
		var parts = strings.Split(key, "/")

		for index, part := range parts {
			if strings.HasPrefix(part, ":") {
				parts[index] = "(.*)"
			}
		}

		compiledKeys = append(compiledKeys, RouteKey{
			Route: key,
			Regex: strings.Join(parts, "/"),
		})
	}

	// Sort the keys by length, so that the longest one gets matched first
	sort.SliceStable(compiledKeys, func(i, j int) bool {
		return len(compiledKeys[i].Route) > len(compiledKeys[j].Route)
	})

	router.Keys[method] = compiledKeys

	return router.Keys[method]
}

func (router *RouterInstance) Delete(path string, handler func(request *Request) *Response) *Route {
	return router.request("DELETE", path, handler)
}

func (router *RouterInstance) Fallback(callback func(request *Request) *Response) {
	router.DefaultRoute = &Route{
		Handler: callback,
	}
}

func (router *RouterInstance) Dispatch(event *event.Event) *Response {
	request := PrepareRequest(event)

	if !router.Initialized {
		router.Init()
		LoadRoutes(router)
		router.Initialized = true
	}

	return router.findRoute(request.Method, request.Path).Handle(request)
}

func (router *RouterInstance) findRoute(method, path string) *Route {
	if router.Routes[method] == nil {
		return router.DefaultRoute
	}

	for _, routeKey := range router.compileKeys(method) {
		r, _ := regexp.Compile(routeKey.Regex)

		for _, match := range r.FindStringSubmatch(path) {
			if match != "" {
				route := router.Routes[method][routeKey.Route]

				return route
			}
		}
	}

	return router.DefaultRoute
}

func (router *RouterInstance) Get(path string, handler func(request *Request) *Response) *Route {
	return router.request("GET", path, handler)
}

func (router *RouterInstance) Init() {
	router.Keys = make(map[string][]RouteKey)

	router.Routes = map[string]map[string]*Route{
		"GET":    nil,
		"POST":   nil,
		"PUT":    nil,
		"PATCH":  nil,
		"DELETE": nil,
	}
}

func (router *RouterInstance) Path(path string, handler func(request *Request) *Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Post(path string, handler func(request *Request) *Response) *Route {
	return router.request("POST", path, handler)
}

func (router *RouterInstance) Patch(path string, handler func(request *Request) *Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Put(path string, handler func(request *Request) *Response) *Route {
	return router.request("PUT", path, handler)
}

func PrepareRequest(event *event.Event) *Request {
	headers := map[string]string{}

	for key, value := range event.Server {
		if strings.HasPrefix(key, "HTTP_") {
			headers[strings.ReplaceAll(key, "HTTP_", "")] = value
		}
	}

	queryParams := map[string]string{}

	if event.Server["QUERY_STRING"] != "" {
		queryParamsString := strings.Split(event.Server["QUERY_STRING"], "&")

		for _, param := range queryParamsString {
			parts := strings.Split(param, "=")

			if len(parts) == 2 {
				queryParams[parts[0]] = parts[1]
			}
		}
	}

	return NewRequest(headers, event.Method, event.Path, event.Body, queryParams)
}

func (router *RouterInstance) request(method string, path string, handler func(request *Request) *Response) *Route {
	if router.Routes[method] == nil {
		router.Routes[method] = make(map[string]*Route)
	}

	router.Routes[method][path] = &Route{
		Handler: handler,
		Path:    path,
	}

	route := router.Routes[method][path]

	return route
}
