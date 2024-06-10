package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
)

type RouterInstance struct {
	DefaultRoute Route
	HttpServer   *http.Server
	Routes       map[string]map[string]Route
}

type RouteKey struct {
	Route string
	Regex *regexp.Regexp
}

var StaticRouter *RouterInstance

func Router() *RouterInstance {
	if StaticRouter == nil {
		StaticRouter = &RouterInstance{
			Routes: map[string]map[string]Route{
				"GET":    nil,
				"POST":   nil,
				"PUT":    nil,
				"PATCH":  nil,
				"DELETE": nil,
			},
		}
	}

	return StaticRouter
}

func (router *RouterInstance) Delete(path string, handler func(request *Request) Response) Route {
	return router.request("DELETE", path, handler)
}

func (router *RouterInstance) Fallback(callback func(request *Request) Response) {
	router.DefaultRoute = Route{
		Handler: callback,
	}
}

func (router *RouterInstance) Get(path string, handler func(request *Request) Response) Route {
	return router.request("GET", path, handler)
}

func (router *RouterInstance) Path(path string, handler func(request *Request) Response) Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Post(path string, handler func(request *Request) Response) Route {
	return router.request("POST", path, handler)
}

func (router *RouterInstance) Patch(path string, handler func(request *Request) Response) Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Put(path string, handler func(request *Request) Response) Route {
	return router.request("PUT", path, handler)
}

func PrepareRequest(request *http.Request) *Request {
	return NewRequest(request)
}

func (router *RouterInstance) request(method string, path string, handler func(request *Request) Response) Route {
	// path = strings.TrimLeft(path, "/")
	// path = strings.TrimRight(path, "/")
	// // path = strings.ReplaceAll(path, "{", "\\{")
	// // path = strings.ReplaceAll(path, "}", "\\}")
	// path = fmt.Sprintf("/%s{/?}", path)
	if router.Routes[method] == nil {
		router.Routes[method] = make(map[string]Route)
	}

	router.Routes[method][path] = Route{Handler: handler}

	return router.Routes[method][path]
}

func (router *RouterInstance) Server(serveMux *http.ServeMux) {
	LoadRoutes(router)

	serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		response := router.DefaultRoute.Handler(PrepareRequest(r))
		w.WriteHeader(response.StatusCode)
	})

	for method := range router.Routes {
		for path, route := range router.Routes[method] {
			serveMux.HandleFunc(fmt.Sprintf("%s %s", method, path), func(w http.ResponseWriter, r *http.Request) {
				response := route.Handle(PrepareRequest(r))

				if response.StatusCode == 0 {
					return
				}

				if response.Stream != nil {
					response.Stream(w)
					return
				}

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
			})
		}
	}
}
