package http

import (
	"regexp"
	"sort"
	"strings"
)

type Router struct {
	DefaultRoute *Route
	Initialized  bool
	Keys         map[string][]RouteKey
	Routes       map[string]map[string]*Route
}

type RouteKey struct {
	Route string
	Regex string
}

func (router *Router) compileKeys(method string) []RouteKey {
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

func (router *Router) Delete(path string, handler func(request *Request) *Response) *Route {
	return router.request("DELETE", path, handler)
}

func (router *Router) Fallback(callback func(request *Request) *Response) {
	router.DefaultRoute = &Route{
		Handler: callback,
	}
}

func (router *Router) Dispatch(request *Request) *Response {
	if !router.Initialized {
		router.Init()
		LoadRoutes(router)
		router.Initialized = true
	}

	return router.findRoute(request.Method, request.Path).Handle(request)
}

func (router *Router) findRoute(method, path string) *Route {
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

func (router *Router) Get(path string, handler func(request *Request) *Response) *Route {
	return router.request("GET", path, handler)
}

func (router *Router) Init() {
	router.Keys = make(map[string][]RouteKey)

	router.Routes = map[string]map[string]*Route{
		"GET":    nil,
		"POST":   nil,
		"PUT":    nil,
		"PATCH":  nil,
		"DELETE": nil,
	}
}

func (router *Router) Path(path string, handler func(request *Request) *Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *Router) Post(path string, handler func(request *Request) *Response) *Route {
	return router.request("POST", path, handler)
}

func (router *Router) Patch(path string, handler func(request *Request) *Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *Router) Put(path string, handler func(request *Request) *Response) *Route {
	return router.request("PUT", path, handler)
}

func (router *Router) request(method string, path string, handler func(request *Request) *Response) *Route {
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
