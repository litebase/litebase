package http

import (
	"context"
	"time"
)

type Route struct {
	Handler              RouteHandler
	RegisteredMiddleware []Middleware
	router               *Router
	timeout              time.Duration
	// OpenAPIMetadata      *openapi.OpenAPIMetadata
	handlerName string // Store handler function name for analysis
}

type RouteHandler func(ctx context.Context, request *Request) Response

// Create a new Route instance
func NewRoute(router *Router, handler func(ctx context.Context, request *Request) Response) *Route {
	return &Route{
		Handler: handler,
		router:  router,
		timeout: 5 * time.Second,
	}
}

// Handle the Route with an incoming request
func (route *Route) Handle(request *Request) Response {
	var response Response
	ctx := request.BaseRequest.Context()

	for _, middleware := range route.router.GlobalMiddleware {
		request, response = middleware(ctx, request)

		if response.StatusCode > 0 {
			return response
		}
	}

	for _, middleware := range route.RegisteredMiddleware {
		request, response = middleware(ctx, request)

		if response.StatusCode > 0 {
			return response
		}
	}

	// The route has no timeout
	if route.timeout == 0 {
		return route.Handler(ctx, request)
	}

	ctx, cancel := context.WithTimeout(ctx, route.timeout)
	defer cancel()

	handlerResponse := make(chan Response)

	go func() {
		handlerResponse <- route.Handler(ctx, request)
	}()

	select {
	case response = <-handlerResponse:
		return response
	case <-ctx.Done():
		response.StatusCode = 408

		if response.Body == nil {
			response.Body = make(map[string]interface{})
		}

		response.Body["status"] = "error"
		response.Body["message"] = "Request timed out"

		return response
	}
}

// Add middleware to the Route
func (route *Route) Middleware(middleware []Middleware) *Route {
	route.RegisteredMiddleware = append(route.RegisteredMiddleware, middleware...)

	return route
}

// Update the timeout duration for the Route
func (route *Route) Timeout(duration time.Duration) *Route {
	route.timeout = duration

	return route
}

// GetHandlerName returns the handler function name for code analysis
func (route *Route) GetHandlerName() string {
	return route.handlerName
}

// SetHandlerName sets the handler function name (used internally by router)
func (route *Route) SetHandlerName(name string) {
	route.handlerName = name
}
