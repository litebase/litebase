package http

type DatabaseRestoreController struct {
}

func (controller *DatabaseRestoreController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
