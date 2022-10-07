package http

type DatabaseDirectoryController struct {
}

func (controller *DatabaseDirectoryController) Index(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
