package http

import "context"

func NodeTick(ctx context.Context, request *Request) (*Request, Response) {
	request.cluster.Node().Tick()

	return request, Response{}
}
