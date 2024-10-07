package http

import (
	"litebase/server/node"
)

func EventStoreController(request *Request) Response {
	input, err := request.Input(&node.EventMessage{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	node.ReceiveEvent(input.(*node.EventMessage))

	return Response{
		StatusCode: 200,
	}
}
