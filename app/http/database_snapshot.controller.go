package http

type DatabaseSnapshotController struct {
}

func (controller *DatabaseSnapshotController) Show(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
