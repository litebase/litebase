package http

type DatabaseBackupController struct {
}

func (controller *DatabaseBackupController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}

func (controller *DatabaseBackupController) Show(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
