package server

import (
	"context"
	"fmt"
	"litebasedb/server/database"
	"litebasedb/server/node"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ServerInstance struct {
	HttpServer *http.Server
	Node       Node
	ServeMux   *http.ServeMux
}

var serverInstance *ServerInstance

func NewServer() *ServerInstance {
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

	port := os.Getenv("LITEBASEDB_QUERY_NODE_PORT")

	s.ServeMux = http.NewServeMux()

	s.HttpServer = &http.Server{
		Addr: fmt.Sprintf(":%s", port),
		// ReadTimeout:  30 * time.Second,
		// WriteTimeout: 30 * time.Second,
		// IdleTimeout:  60 * time.Second,
		Handler: s.ServeMux,
	}

	// s.Node.Run()

	if serverHook != nil {
		serverHook(s)
	}

	log.Println("LitebaseDB running on port", port)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		if err := s.HttpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	for {
		sig := <-signalChannel
		switch sig {
		case os.Interrupt:
			s.Shutdown()
			<-serverDone
			return
		case syscall.SIGTERM:
			s.Shutdown()
			<-serverDone
			os.Exit(0)
			return
		}
	}
}

func (s *ServerInstance) Shutdown() {
	fmt.Println("")
	node.Unregister()
	database.ConnectionManager().Shutdown()

	// Create a context with a timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := s.HttpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
}

func Server() *ServerInstance {
	return serverInstance
}
