package server

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/joho/godotenv"
)

type ServerInstance struct {
	HttpServer *http.Server
	Node       Node
}

var serverInstance *ServerInstance

func NewServer() *ServerInstance {
	godotenv.Load(".env")

	server := &ServerInstance{}

	if server.isPrimary() {
		server.Node = NewPrimary()
	} else {
		server.Node = NewReplica()
	}

	return server
}

func (s *ServerInstance) isPrimary() bool {
	return os.Getenv("PRIMARY") == ""
}

func (s *ServerInstance) Primary() *Primary {
	return s.Node.(*Primary)
}

func (s *ServerInstance) Start(serverHook func(*ServerInstance)) {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	port := os.Getenv("LITEBASEDB_PORT")

	s.HttpServer = &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// s.Node.Run()

	if serverHook != nil {
		serverHook(s)
	}

	log.Println("LitebaseDB running on port", port)

	log.Fatal(s.HttpServer.ListenAndServe())
}

func Server() *ServerInstance {
	return serverInstance
}
