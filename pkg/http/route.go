package http

import (
	"context"
	"time"
)

type Route struct {
	Handler              func(request *Request) Response
	RegisteredMiddleware []Middleware
	router               *Router
	timeout              time.Duration
}

func NewRoute(router *Router, handler func(request *Request) Response) *Route {
	return &Route{
		Handler: handler,
		router:  router,
		timeout: 5 * time.Second,
	}
}

func (route *Route) Handle(request *Request) Response {
	var response Response

	for _, middleware := range route.router.GlobalMiddleware {
		request, response = middleware(request)

		if response.StatusCode > 0 {
			return response
		}
	}

	for _, middleware := range route.RegisteredMiddleware {
		request, response = middleware(request)

		if response.StatusCode > 0 {
			return response
		}
	}

	// The route has no timeout
	if route.timeout == 0 {
		return route.Handler(request)
	}

	ctx, cancel := context.WithTimeout(request.BaseRequest.Context(), route.timeout)
	defer cancel()

	handlerResponse := make(chan Response)

	go func() {
		handlerResponse <- route.Handler(request)
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

func (route *Route) Middleware(middleware []Middleware) *Route {
	route.RegisteredMiddleware = append(route.RegisteredMiddleware, middleware...)

	return route
}

func (route *Route) Timeout(duration time.Duration) *Route {
	route.timeout = duration

	return route
}
