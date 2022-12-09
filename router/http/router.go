package http

import (
	// "litebasedb/router/event"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
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

func (router *RouterInstance) Dispatch(request *http.Request) *Response {
	req := PrepareRequest(request)

	if !router.Initialized {
		router.Init()
		LoadRoutes(router)
		router.Initialized = true
	}

	return router.findRoute(request.Method, req.Path).Handle(req)
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

func PrepareRequest(request *http.Request) *Request {
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

	return NewRequest(headers, request.Method, request.URL.Path, request.Body, queryParams)
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

func (router *RouterInstance) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var response *Response
	var wg = sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()
		response = Router().Dispatch(r)
	}()

	wg.Wait()

	for key, value := range response.Headers {
		w.Header().Set(key, value)
	}

	w.WriteHeader(response.StatusCode)

	if response.Body == nil {
		w.Write([]byte(""))
	} else {
		jsonBody, err := json.Marshal(response.Body)

		if err != nil {
			panic(err)
		}

		w.Write([]byte(jsonBody))
	}
}
