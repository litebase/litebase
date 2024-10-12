package http

func NodeTick(request *Request) (*Request, Response) {
	request.cluster.Node().Tick()

	return request, Response{}
}
