package http

import (
	"litebasedb/router/auth"
	"litebasedb/router/config"
	"litebasedb/router/node"
)

func Internal(request *Request) (*Request, *Response) {
	nodeHeader := request.Headers().Get("X-Lbdb-Node")
	var nodeIp string

	if nodeHeader != "" {
		nodeIpDecrypted, err := auth.SecretsManager().Decrypt(
			config.Get("signature"),
			nodeHeader,
		)

		if err != nil {
			return nil, &Response{
				StatusCode: 401,
			}
		}

		nodeIp = nodeIpDecrypted["value"]
	}

	if nodeIp == "" || !node.Has(nodeIp) {
		return nil, &Response{
			StatusCode: 401,
		}
	}

	return request, nil
}
