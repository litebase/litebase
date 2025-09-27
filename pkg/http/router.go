package http

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
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
func (router *Router) Delete(path string, handler RouteHandler) *Route {
	return router.request("DELETE", path, handler)
}

// Set the Fallback route to the router
func (router *Router) Fallback(handler RouteHandler) {
	router.DefaultRoute = Route{
		Handler: handler,
		router:  router,
		timeout: 0,
	}
}

// Add a GET route on the router
func (router *Router) Get(path string, handler RouteHandler) *Route {
	return router.request("GET", path, handler)
}

// Add a PATCH route to the router
func (router *Router) Patch(path string, handler RouteHandler) *Route {
	return router.request("PATCH", path, handler)
}

// Add a POST route to the router
func (router *Router) Post(path string, handler RouteHandler) *Route {
	return router.request("POST", path, handler)
}

// Add a PUT route to the router
func (router *Router) Put(path string, handler RouteHandler) *Route {
	return router.request("PUT", path, handler)
}

// Resolve an incoming request using a route from the Router
func (router *Router) request(method string, path string, handler RouteHandler) *Route {
	if router.Routes[method] == nil {
		router.Routes[method] = make(map[string]*Route)
	}

	path = strings.TrimRight(path, "/")

	route := NewRoute(router, handler)

	// Extract handler function name for code analysis
	if handlerName := getFunctionName(handler); handlerName != "" {
		route.SetHandlerName(handlerName)
	}

	router.Routes[method][path] = route

	return router.Routes[method][path]
}

// getFunctionName extracts the function name from a function value
func getFunctionName(fn interface{}) string {
	if fn == nil {
		return ""
	}

	// Get the function name using runtime
	fnPtr := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())

	if fnPtr != nil {
		fullName := fnPtr.Name()

		// Extract just the function name (remove package path)
		parts := strings.Split(fullName, ".")

		if len(parts) > 0 {
			return parts[len(parts)-1]
		}

		return fullName
	}

	return ""
}

// Create a server handler for the Router.
func (router *Router) Server(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	serveMux *http.ServeMux,
) {
	router.PublicServer(cluster, databaseManager, logManager, serveMux)
}

// Create a public server handler for the Router.
func (router *Router) PublicServer(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	serveMux *http.ServeMux,
) {
	LoadPublicRoutes(router)
	router.setupServerHandlers(cluster, databaseManager, logManager, serveMux)
}

// Create a private server handler for the Router.
func (router *Router) PrivateServer(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	serveMux *http.ServeMux,
) {
	LoadPrivateRoutes(router)
	router.setupServerHandlers(cluster, databaseManager, logManager, serveMux)
}

// setupServerHandlers sets up the HTTP handlers for the router
func (router *Router) setupServerHandlers(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	serveMux *http.ServeMux,
) {

	serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		response := router.DefaultRoute.Handler(
			r.Context(),
			NewRequest(cluster, databaseManager, logManager, r),
		)

		jsonBody, err := json.Marshal(response.Body)

		if err != nil {
			panic(err)
		}

		// Set the content type to application/json
		w.Header().Set("Content-Type", "application/json")

		// Set the content length
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(jsonBody)))

		w.WriteHeader(response.StatusCode)

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

						defer func() {
							if err := gw.Close(); err != nil {
								slog.Error("Error closing gzip writer", "error", err)
							}
						}()

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

// GetRoutes returns all registered routes for OpenAPI generation
func (router *Router) GetRoutes() map[string]map[string]*Route {
	return router.Routes
}
