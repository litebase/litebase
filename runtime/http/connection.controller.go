package http

type ConnectionController struct {
}

func (controller *ConnectionController) Store(request *Request) *Response {
	controller.createConnection(request)

	return &Response{
		StatusCode: 200,
	}
}

func (controller *ConnectionController) createConnection(request *Request) {
	SecretsManager().Listen()
}
