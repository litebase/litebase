package http

import "litebase/server/cluster"

func NodeTick(request *Request) (*Request, Response) {
	cluster.Node().Tick()

	return request, Response{}
}
