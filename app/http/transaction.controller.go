package http

type TransactionController struct{}

func (controller *TransactionController) Destroy(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
func (controller *TransactionController) Store(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
func (controller *TransactionController) Update(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
