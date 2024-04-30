package http

type Route struct {
	Handler              func(request *Request) Response
	RegisteredMiddleware []Middleware
}

func (route Route) Handle(request *Request) Response {
	var response Response

	for _, registeredMiddleware := range route.RegisteredMiddleware {
		request, response = registeredMiddleware(request)

		if response.StatusCode > 0 {
			return response
		}
	}

	return route.Handler(request)
}

func (route Route) Middleware(middleware []Middleware) Route {
	route.RegisteredMiddleware = append(route.RegisteredMiddleware, middleware...)

	return route
}
