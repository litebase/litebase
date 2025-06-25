package http

func ClusterStatusController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status":  "ok",
			"message": "Cluster is functioning normally",
			"data": map[string]any{
				"node_count": len(request.cluster.Nodes()),
			},
		},
	}
}
