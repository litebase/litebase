package http

import (
	"context"
	"time"
)

type Route struct {
	Handler              func(request *Request) Response
	RegisteredMiddleware []Middleware
	timeout              time.Duration
}

func NewRoute(handler func(request *Request) Response) *Route {
	return &Route{
		Handler: handler,
		timeout: 5 * time.Second,
	}
}

func (route *Route) Handle(request *Request) Response {
	var response Response

	for _, registeredMiddleware := range route.RegisteredMiddleware {
		request, response = registeredMiddleware(request)

		// Check if the middleware has returned a response by checking the status code
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
