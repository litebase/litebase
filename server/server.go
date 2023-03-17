package server

import (
	"bufio"
	"litebasedb/app"
	"log"
	"net/http"
	"os"
)

type Server struct {
	node Node
}

func NewServer() *Server {
	server := &Server{}

	if server.isPrimary() {
		server.node = NewPrimary()
	} else {
		server.node = NewReplica()
	}

	return server
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if r.URL.Path == "/replication" {
				log.Println("Replication request")
				close := make(chan bool)
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Transfer-Encoding", "chunked")
				w.Header().Set("Connection", "close")
				s.node.(*Primary).AddReplica(r.Header["X-Replica-Id"][0], r.Header["X-Replica-Id"][0])
				flusher := w.(http.Flusher)

				go func() {
					<-r.Context().Done()
					log.Println("Connection closed")
					s.node.(*Primary).RemoveReplica(r.Header["X-Replica-Id"][0])
					close <- true
				}()

				go func() {
					scanner := bufio.NewScanner(r.Body)

					for scanner.Scan() {
						line := scanner.Text()
						s.node.(*Primary).WriteFromReplica([]byte(line))
					}
				}()

				go func() {
					s.node.Write([]byte("ok"))
					flusher.Flush()
				}()
				for {
					select {
					case <-close:
						return
					case message := <-s.node.Read():
						log.Println("Replicating")
						w.Write(message)
						flusher.Flush()
					}
				}
			} else {
				w.WriteHeader(http.StatusOK)
				w.Header().Set("Content-Type", "application/json")
				data := make([]byte, r.ContentLength)
				r.Body.Read(data)
				s.node.Write(data)
				w.Write([]byte(`{"status": "ok"}`))
			}
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})
}

func (s *Server) isPrimary() bool {
	return os.Getenv("PRIMARY") == ""
}

func (s *Server) Start() {
	port := os.Getenv("PORT")

	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  0,
	}

	s.node.Run()
	app.NewApp(server).Run()
	log.Println("Server running on port", port)
	log.Fatal(server.ListenAndServe())
}
