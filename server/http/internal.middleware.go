package http

import (
	"log"
	"strconv"
	"time"
)

func Internal(request *Request) (*Request, Response) {
	nodeHeader := request.Headers().Get("X-Lbdb-Node")
	nodeTimestamp := request.Headers().Get("X-Lbdb-Node-Timestamp")

	var nodeIp string

	address, err := request.cluster.Node().Address()

	if err != nil {
		log.Printf("Error getting node address: %s - %s", err, address)
		return request, Response{
			StatusCode: 500,
		}
	}

	if nodeHeader != "" {
		nodeIpDecrypted, err := request.cluster.Auth.SecretsManager.Decrypt(
			request.cluster.Config.Signature,
			[]byte(nodeHeader),
		)

		if err != nil {
			log.Printf("Error decrypting node header: %s - %s", err, address)
			return request, Response{
				StatusCode: 401,
			}
		}

		nodeIp = nodeIpDecrypted.Value
	}
	if nodeTimestamp == "" {
		return request, Response{
			StatusCode: 401,
		}
	}

	// Convert the Unix nano timestamp to int64
	timestamp, err := strconv.ParseInt(nodeTimestamp, 10, 64)

	if err != nil {
		log.Println("Error parsing Unix nano timestamp:", err)
		return request, Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"message": "Invalid timestamp",
			},
		}
	}

	// Create a time.Time object from the Unix nano timestamp
	parsedTimestamp := time.Unix(0, timestamp)

	if !request.cluster.Initialized || !request.cluster.Node().Initialized {
		return request, Response{
			StatusCode: 504,
		}
	}

	if nodeIp == "" || !request.cluster.IsMember(nodeIp, parsedTimestamp) {
		return request, Response{
			StatusCode: 401,
		}
	}

	return request, Response{}
}
