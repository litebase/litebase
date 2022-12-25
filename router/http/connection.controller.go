package http

import (
	"litebasedb/router/auth"
	"litebasedb/router/connections"
	"log"
	"net/http"
)

func ConnectionController(request *Request) *Response {
	connectionKey, err := auth.SecretsManager().GetConnectionKey(
		request.Param("databaseUuid"),
		request.Param("branchUuid"),
	)

	// TODO: Handle error
	if err != nil {
		log.Fatal(err)
	}

	return &Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Connection", "keep-alive")

			connections.CreateConnection(
				request.Param("databaseUuid"),
				request.Param("branchUuid"),
				connectionKey,
				request.BaseRequest,
				w,
			).Listen()
		},
	}
}
