package http

import (
	"errors"
	"litebase/server/cluster"
	"litebase/server/node"
	"log"
	"net/http"
)

func ClusterConnectionController(request *Request) Response {
	if node.Node().Membership != cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		return ForbiddenResponse(errors.New("not a primary node"))
	}

	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Content-Type", "application/gob")
			w.Header().Set("Connection", "keep-alive")

			log.Fatalln("Connection started")
			// err := node.Node().Primary().OpenConnection(w, request.BaseRequest)

			// if err != nil {
			// 	log.Println("Connection closed: ", err)
			// 	return
			// }

			log.Println("Connection finished")
		},
	}
}
