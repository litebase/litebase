package http

import (
	"litebase/server/storage"
	"log"
	"net/http"
)

func StorageConnectionController(request *Request) Response {
	return Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/gob")
			w.Header().Set("Transfer-Encoding", "chunked")

			connection, err := storage.StorageConnectionManager().Get(request.Param("id"))

			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if connection == nil {
				log.Println("Connection not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			// Activate the connection
			err = storage.StorageConnectionManager().Activate(connection, w, request.BaseRequest)

			if err != nil {
				connection.Close()
				return
			}

			log.Println("Connection activated")
		},
	}
}
