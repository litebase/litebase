package http

type DatabaseBackupArchiveController struct {
}

func (controller *DatabaseBackupArchiveController) Destroy(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}

func (controller *DatabaseBackupArchiveController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
