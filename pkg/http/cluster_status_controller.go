package http

import "context"

// Response payload for cluster status
type ClusterStatusResponse struct {
	NodeCount int `json:"node_count"`
}

// Returns the current status of the cluster
func ClusterStatusControllerIndex(ctx context.Context, request *Request) Response {
	return SuccessResponse(
		"Cluster status",
		ClusterStatusResponse{
			NodeCount: len(request.cluster.Nodes()),
		},
		200,
	)
}
