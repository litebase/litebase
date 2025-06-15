package http

func HealthCheckController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Body:       nil,
	}
}
