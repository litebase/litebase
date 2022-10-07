package http

type DatabaseSettingsController struct {
}

func (controller *DatabaseSettingsController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}

func (controller *DatabaseSettingsController) Destroy(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
