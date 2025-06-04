package http

import (
	"github.com/litebase/litebase/server/cluster"
)

func EventStoreController(request *Request) Response {
	input, err := request.Input(&cluster.EventMessage{})

	if err != nil {
		return Response{
			StatusCode: 400,
			Body: map[string]any{
				"errors": err,
			},
		}
	}

	request.cluster.ReceiveEvent(input.(*cluster.EventMessage))

	return Response{
		StatusCode: 200,
	}
}
