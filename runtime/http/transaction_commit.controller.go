package http

type TransactionCommitController struct{}

func (controller *TransactionCommitController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
