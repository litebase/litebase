package http

import (
	"litebasedb/server/storage"
	"log"
	"net/http"
)

func StorageConnectionController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			// Find the connection by ID
			connectionHash := request.Param("connectionHash")
			id := request.Param("id")
			connection, err := storage.LambdaConnectionManager().Find(connectionHash, id)

			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if connection == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			// Activate the connection
			err = storage.LambdaConnectionManager().Activate(connectionHash, connection, w, request.BaseRequest)

			if err != nil {
				log.Println(err)
				storage.LambdaConnectionManager().Remove(connectionHash, connection)
				return
			}
		},
	}
}
