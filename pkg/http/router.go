package http

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/logs"
)

type Router struct {
	DefaultRoute     Route
	HttpServer       *http.Server
	GlobalMiddleware []Middleware
	Routes           map[string]map[string]*Route
}

type RouteKey struct {
	Route string
	Regex *regexp.Regexp
}

// Create a new Router instance
func NewRouter() *Router {
	return &Router{
		GlobalMiddleware: []Middleware{
			RequireHost,
			RequireContentType,
			NodeTick,
		},
		Routes: map[string]map[string]*Route{
			"GET":    nil,
			"POST":   nil,
			"PUT":    nil,
			"PATCH":  nil,
			"DELETE": nil,
		},
	}
}

// Add a DELETE route to the router
func (router *Router) Delete(path string, handler func(request *Request) Response) *Route {
	return router.request("DELETE", path, handler)
}

// Set the Fallback route to the router
func (router *Router) Fallback(callback func(request *Request) Response) {
	router.DefaultRoute = Route{
		Handler: callback,
		router:  router,
		timeout: 0,
	}
}

// Add a GET route on the router
func (router *Router) Get(path string, handler func(request *Request) Response) *Route {
	return router.request("GET", path, handler)
}

// Add a PATCH route to the router
func (router *Router) Patch(path string, handler func(request *Request) Response) *Route {
	return router.request("PATCH", path, handler)
}

// Add a POST route to the router
func (router *Router) Post(path string, handler func(request *Request) Response) *Route {
	return router.request("POST", path, handler)
}

// Add a PUT route to the router
func (router *Router) Put(path string, handler func(request *Request) Response) *Route {
	return router.request("PUT", path, handler)
}

// Resolve an incoming request using a route from the Router
func (router *Router) request(method string, path string, handler func(request *Request) Response) *Route {
	if router.Routes[method] == nil {
		router.Routes[method] = make(map[string]*Route)
	}

	path = strings.TrimRight(path, "/")

	router.Routes[method][path] = NewRoute(router, handler)

	return router.Routes[method][path]
}

// Create a server handler for the Router.
func (router *Router) Server(
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

		jsonBody, err := json.Marshal(response.Body)

		if err != nil {
			panic(err)
		}

		// Set the content type to application/json
		w.Header().Set("Content-Type", "application/json")

		// Set the content length
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonBody)))

		_, err = w.Write(jsonBody)

		if err != nil {
			slog.Error("Error writing response", "error", err)
		}
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

				if response.StatusCode == 204 {
					// If the response body is nil and the status code is 204, we write an empty response.
					w.Header().Set("Content-Length", "0")
					return
				}

				if response.Body == nil {
					_, err := w.Write([]byte(""))

					if err != nil {
						slog.Error("Error writing empty response", "error", err)
					}
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

						_, err = w.Write(jsonBody)

						if err != nil {
							slog.Error("Error writing response", "error", err)
						}
					}
				}
			})
		}
	}
}
