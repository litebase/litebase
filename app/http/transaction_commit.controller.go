package http

func TransactionCommitController(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"status":  "success",
			"message": "Transaction committed successfully",
		},
	}
}
