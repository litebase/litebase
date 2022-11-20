package http

import "log"

type TransactionController struct{}

func (controller *TransactionController) Destroy(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
func (controller *TransactionController) Store(request *Request) *Response {
	log.Println("TransactionController.Store")
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
func (controller *TransactionController) Update(request *Request) *Response {
	return JsonResponse(map[string]interface{}{}, 200, nil)
}
