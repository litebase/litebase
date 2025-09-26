package http

import "context"

type Middleware func(ctx context.Context, request *Request) (*Request, Response)
