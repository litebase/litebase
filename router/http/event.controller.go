package http

import (
	"litebasedb/router/events"
	"litebasedb/router/node"
)

func EventStoreController(request *Request) *Response {
	input, err := request.Input(&node.NodeEvent{})

	if err != nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	events.ReceiveEvent(input.(*node.NodeEvent))

	return &Response{
		StatusCode: 200,
	}
}
