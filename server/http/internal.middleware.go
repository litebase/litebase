package http

import (
	"litebase/internal/config"
	"log"
	"time"
)

func Internal(request *Request) (*Request, Response) {
	nodeHeader := request.Headers().Get("X-Lbdb-Node")
	nodeTimestamp := request.Headers().Get("X-Lbdb-Node-Timestamp")

	var nodeIp string

	if nodeHeader != "" {
		nodeIpDecrypted, err := request.cluster.Auth.SecretsManager().Decrypt(
			config.Get().Signature,
			nodeHeader,
		)

		if err != nil {
			return request, Response{
				StatusCode: 401,
			}
		}

		nodeIp = nodeIpDecrypted["value"]
	}

	if nodeTimestamp == "" {
		return request, Response{
			StatusCode: 401,
		}
	}

	parsedTimestamp, err := time.Parse(time.RFC3339, nodeTimestamp)

	if err != nil {
		return request, Response{
			StatusCode: 500,
			Body: map[string]interface{}{
				"message": err.Error(),
			},
		}
	}

	if nodeIp == "" || !request.cluster.IsMember(nodeIp, parsedTimestamp) {
		log.Println("Unauthorized node connection attempt: ", nodeIp)

		return request, Response{
			StatusCode: 401,
		}
	}

	return request, Response{}
}
