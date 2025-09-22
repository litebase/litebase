package http

import "context"

func ClusterStatusControllerIndex(ctx context.Context, request *Request) Response {
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
