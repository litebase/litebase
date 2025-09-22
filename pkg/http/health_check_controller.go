package http

import "context"

func HealthCheckControllerShow(ctx context.Context, request *Request) Response {
	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}
