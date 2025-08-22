package http

import (
	"encoding/gob"
	"errors"
	"log"
	"log/slog"
	"net/http"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
)

func ClusterPrimaryController(request *Request) Response {
	if request.cluster.Node().GetMembership() != cluster.ClusterMembershipPrimary {
		return ForbiddenResponse(errors.New("not a primary node"))
	}

	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Content-Type", "application/gob")

			defer func() {
				if err := request.BaseRequest.Body.Close(); err != nil {
					slog.Error("Error closing request body", "error", err)
				}
			}()

			var message messages.NodeMessage
			decoder := gob.NewDecoder(request.BaseRequest.Body)
			err := decoder.Decode(&message)

			if err != nil {
				log.Println("Failed to decode message: ", err)

				return
			}

			responseMessage, err := request.cluster.Node().HandleMessage(message)

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
