package http

import (
	"litebase/server/node"
)

func NodeTick(request *Request) (*Request, Response) {
	node.Node().Tick()

	return request, Response{}
}
