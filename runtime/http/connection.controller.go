package http

type ConnectionController struct {
}

func (controller *ConnectionController) Store(request *Request) *Response {
	go controller.createConnection(request)

	return &Response{
		StatusCode: 200,
	}
}

func (controller *ConnectionController) createConnection(request *Request) {
	ConnectionManager().Listen(request.All()["host"].(string))
}
