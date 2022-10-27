package http

type Middleware interface {
	Handle(request *Request) (*Request, *Response)
}
