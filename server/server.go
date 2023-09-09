package server

import (
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
)

type Server struct {
	HttpServer *http.Server
	Node       Node
}

var staticServer *Server

func NewServer() *Server {
	godotenv.Load(".env")

	server := &Server{}

	if server.isPrimary() {
		server.Node = NewPrimary()
	} else {
		server.Node = NewReplica()
	}

	return server
}

func (s *Server) isPrimary() bool {
	return os.Getenv("PRIMARY") == ""
}

func (s *Server) Primary() *Primary {
	return s.Node.(*Primary)
}

func (s *Server) Start(serverHook func(*Server)) {
	port := os.Getenv("LITEBASEDB_PORT")

	s.HttpServer = &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  0,
	}

	// s.Node.Run()

	if serverHook != nil {
		serverHook(s)
	}

	log.Println("LitebaseDB running on port", port)

	log.Fatal(s.HttpServer.ListenAndServe())
}

func Static() *Server {
	return staticServer
}
