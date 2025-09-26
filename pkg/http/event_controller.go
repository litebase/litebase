package http

import (
	"context"

	"github.com/litebase/litebase/pkg/cluster"
)

func EventStoreController(ctx context.Context, request *Request) Response {
	input, err := request.Input(&cluster.EventMessage{})

	if err != nil {
		return BadRequestResponse(err)
	}

	request.cluster.ReceiveEvent(input.(*cluster.EventMessage))

	return SuccessResponse("Event received", nil, 200)
}
