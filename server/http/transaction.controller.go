package http

func TrasactionControllerStore(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction stored successfully",
		},
	}
}

func TrasactionControllerUpdate(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction updated successfully",
		},
	}
}

func TrasactionControllerDestroy(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction deleted successfully",
		},
	}
}
