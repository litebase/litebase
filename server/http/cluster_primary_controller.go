package http

import (
	"encoding/gob"
	"errors"
	"litebase/server/cluster"
	"litebase/server/node"
	"log"
	"net/http"
)

func ClusterPrimaryController(request *Request) Response {
	if node.Node().Membership != cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		return ForbiddenResponse(errors.New("not a primary node"))
	}

	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Content-Type", "application/gob")

			defer request.BaseRequest.Body.Close()

			var message node.NodeMessage
			decoder := gob.NewDecoder(request.BaseRequest.Body)
			err := decoder.Decode(&message)

			if err != nil {
				log.Println("Failed to decode message: ", err)

				return
			}

			responseMessage, err := node.Node().Primary().HandleMessage(message)

			if err != nil {
				log.Println("Failed to handle message: ", err)
				return
			}

			encoder := gob.NewEncoder(w)

			err = encoder.Encode(responseMessage)

			if err != nil {
				log.Println("Failed to encode response: ", err)

				return
			}
		},
	}
}
