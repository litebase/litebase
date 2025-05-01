package http

func PingController(request *Request) Response {
	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "pong",
	}, 200, nil)
}
