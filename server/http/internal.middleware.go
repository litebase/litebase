package http

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"log"
)

func Internal(request *Request) (*Request, Response) {
	nodeHeader := request.Headers().Get("X-Lbdb-Node")
	var nodeIp string

	if nodeHeader != "" {
		nodeIpDecrypted, err := auth.SecretsManager().Decrypt(
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

	if nodeIp == "" || !cluster.Get().IsMember(nodeIp) {
		log.Println("Unauthorized node connection attempt: ", nodeIp)

		return request, Response{
			StatusCode: 401,
		}
	}

	return request, Response{}
}
