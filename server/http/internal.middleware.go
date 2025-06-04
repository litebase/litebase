package http

import (
	"log"
	"strconv"
	"time"
)

func Internal(request *Request) (*Request, Response) {
	nodeHeader := request.Headers().Get("X-Lbdb-Node")
	nodeTimestamp := request.Headers().Get("X-Lbdb-Node-Timestamp")

	var nodeAddress string

	if nodeHeader != "" {
		nodeAddressDecrypted, err := request.cluster.Auth.SecretsManager.Decrypt(
			request.cluster.Config.Signature,
			[]byte(nodeHeader),
		)

		if err != nil {
			return request, Response{
				StatusCode: 401,
			}
		}

		nodeAddress = nodeAddressDecrypted.Value
	}

	if nodeTimestamp == "" {
		return request, Response{
			StatusCode: 401,
		}
	}

	// Convert the string timestamp to int64
	nanoseconds, err := strconv.ParseInt(nodeTimestamp, 10, 64)

	if err != nil {
		return request, Response{
			StatusCode: 400,
			Body: map[string]any{
				"message": "Invalid timestamp",
			},
		}
	}

	timestamp := time.Unix(0, nanoseconds)

	// Check if the timestamp is not older than 1 second
	if time.Since(timestamp) > time.Second {
		log.Fatalln("Internal request timestamp is too old:", timestamp, "Current time:", time.Now(), time.Since(timestamp))
		// Return 401 Unauthorized if the timestamp is too old
		return request, Response{
			StatusCode: 401,
		}
	}

	if !request.cluster.Initialized || !request.cluster.Node().Initialized {
		return request, Response{
			StatusCode: 504,
		}
	}

	if nodeAddress == "" || !request.cluster.IsMember(nodeAddress, timestamp) {
		return request, Response{
			StatusCode: 401,
		}
	}

	return request, Response{}
}
