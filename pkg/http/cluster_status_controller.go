package http

import "context"

type ClusterStatusResponse struct {
	NodeCount int `json:"node_count"`
}

func ClusterStatusControllerIndex(ctx context.Context, request *Request) Response {
	return SuccessResponse(
		"Cluster status",
		ClusterStatusResponse{
			NodeCount: len(request.cluster.Nodes()),
		},
		200,
	)
}
