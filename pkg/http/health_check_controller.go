package http

import "context"

func HealthCheckControllerShow(ctx context.Context, request *Request) Response {
	return SuccessResponse(
		"Service is healthy.",
		nil,
		200,
	)
}
