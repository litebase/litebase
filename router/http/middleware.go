package http

type Middleware func(request *Request) (*Request, *Response)
