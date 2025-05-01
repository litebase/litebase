package http

import (
	"github.com/litebase/litebase/common/config"
)

// This middleware function checks if the node is a query node.
// If it is not, it returns a 400 status code with a message.
func QueryNode(request *Request) (*Request, Response) {
	if request.cluster.Config.NodeType != config.NodeTypeQuery {
		return request, Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "This type of request can only be perfomed on a query node",
			},
		}
	}

	return request, Response{}
}
