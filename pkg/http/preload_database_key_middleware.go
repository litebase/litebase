package http

import "context"

func PreloadDatabaseKey(ctx context.Context, request *Request) (*Request, Response) {
	request.loadDatabaseKey()

	return request, Response{}
}
