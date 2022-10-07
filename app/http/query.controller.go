package http

type QueryController struct {
}

func (controller *QueryController) Store(request *Request) *Response {
	code := 200
	response := map[string]interface{}{}
	// resolver := query.Resolver{}

	return JsonResponse(response, code, nil)
}
