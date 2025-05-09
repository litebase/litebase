package http

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/logs"
)

type RouterInstance struct {
	DefaultRoute Route
	HttpServer   *http.Server
	Routes       map[string]map[string]*Route
}

type RouteKey struct {
	Route string
	Regex *regexp.Regexp
}

var StaticRouter *RouterInstance

func Router() *RouterInstance {
	if StaticRouter == nil {
		StaticRouter = &RouterInstance{
			Routes: map[string]map[string]*Route{
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

func (router *RouterInstance) Delete(path string, handler func(request *Request) Response) *Route {
	return router.request("DELETE", path, handler)
}

func (router *RouterInstance) Fallback(callback func(request *Request) Response) {
	router.DefaultRoute = Route{
		Handler: callback,
	}
}

func (router *RouterInstance) Get(path string, handler func(request *Request) Response) *Route {
	return router.request("GET", path, handler)
}

func (router *RouterInstance) Path(path string, handler func(request *Request) Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Post(path string, handler func(request *Request) Response) *Route {
	return router.request("POST", path, handler)
}

func (router *RouterInstance) Patch(path string, handler func(request *Request) Response) *Route {
	return router.request("PATCH", path, handler)
}

func (router *RouterInstance) Put(path string, handler func(request *Request) Response) *Route {
	return router.request("PUT", path, handler)
}

func (router *RouterInstance) request(method string, path string, handler func(request *Request) Response) *Route {
	if router.Routes[method] == nil {
		router.Routes[method] = make(map[string]*Route)
	}

	router.Routes[method][path] = NewRoute(handler)

	return router.Routes[method][path]
}

func (router *RouterInstance) Server(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	serveMux *http.ServeMux,
) {
	LoadRoutes(router)

	serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		response := router.DefaultRoute.Handler(
			NewRequest(cluster, databaseManager, logManager, r),
		)
		w.WriteHeader(response.StatusCode)
	})

	for method := range router.Routes {
		for path, route := range router.Routes[method] {
			serveMux.HandleFunc(fmt.Sprintf("%s %s", method, path), func(w http.ResponseWriter, r *http.Request) {
				response := route.Handle(NewRequest(cluster, databaseManager, logManager, r))

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

				if response.StatusCode >= 400 {
					w.Header().Set("Connection", "close")
				}

				w.WriteHeader(response.StatusCode)

				if response.Body == nil {
					w.Write([]byte(""))
				} else {
					if response.Headers["Content-Encoding"] == "gzip" {
						gw := gzip.NewWriter(w)
						defer gw.Close()

						err := json.NewEncoder(gw).Encode(response.Body)

						if err != nil {
							panic(err)
						}
					} else {
						jsonBody, err := json.Marshal(response.Body)

						if err != nil {
							panic(err)
						}

						w.Write([]byte(jsonBody))
					}
				}
			})
		}
	}
}
