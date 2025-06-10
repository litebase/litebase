package http

func ClusterStatusController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Body: map[string]any{
			"status": "success",
			"data": map[string]any{
				"region":     []string{request.cluster.Config.Region},
				"node_count": len(request.cluster.Nodes()),
			},
		},
	}
}
