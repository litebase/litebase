package http

import (
	"encoding/gob"
	"errors"
	"litebase/server/cluster"
	"litebase/server/node"
	"log"
	"net/http"
)

func ClusterReplicaController(request *Request) Response {
	if node.Node().Membership != cluster.CLUSTER_MEMBERSHIP_REPLICA {
		return ForbiddenResponse(errors.New("not a replica node"))
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
				http.Error(w, "Failed to decode message", http.StatusBadRequest)
				return
			}

			responseMessage, err := node.Node().Replica().HandleMessage(message)

			if err != nil {
				log.Println("Failed to handle message: ", err)
				http.Error(w, "Failed to handle message", http.StatusInternalServerError)
				return
			}

			encoder := gob.NewEncoder(w)

			err = encoder.Encode(responseMessage)

			if err != nil {
				log.Println("Failed to encode response: ", err)
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
				return
			}
		},
	}
}
