package http

import (
	"bufio"
	"litebasedb/server"
	"log"
	"net/http"
)

func ReplicationController(request *Request) *Response {
	return &Response{
		StatusCode: 200,
		Stream: func(w http.ResponseWriter) {
			log.Println("Replication request")
			close := make(chan bool)
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Transfer-Encoding", "chunked")
			w.Header().Set("Connection", "close")
			server.Static().Primary().AddReplica(request.Headers().Get("X-Replica-Id"), request.Headers().Get("X-Replica-Id"))
			flusher := w.(http.Flusher)

			go func() {
				<-request.BaseRequest.Context().Done()
				log.Println("Connection closed")
				server.Static().Primary().RemoveReplica(request.Headers().Get("X-Replica-Id"))
				close <- true
			}()

			go func() {
				scanner := bufio.NewScanner(request.BaseRequest.Body)

				for scanner.Scan() {
					line := scanner.Text()
					server.Static().Primary().WriteFromReplica([]byte(line))
				}
			}()

			go func() {
				server.Static().Primary().Write([]byte("ok"))
				flusher.Flush()
			}()
			for {
				select {
				case <-close:
					return
				case message := <-server.Static().Primary().Read():
					log.Println("Replicating")
					w.Write(message)
					flusher.Flush()
				}
			}
		},
	}
}
