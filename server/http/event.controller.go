package http

import (
	"litebasedb/server/events"
	"litebasedb/server/node"
)

func EventStoreController(request Request) Response {
	input, err := request.Input(&node.NodeEvent{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"errors": err,
			},
		}
	}

	events.ReceiveEvent(input.(*node.NodeEvent))

	return Response{
		StatusCode: 200,
	}
}
