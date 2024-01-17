package http

func ClusterStatusController(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Body: map[string]interface{}{
			"message": "OK",
			"data": map[string]interface{}{
				"regions":    []string{"us-east-1"},
				"node_count": 1,
			},
		},
	}
}
