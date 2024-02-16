package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

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
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	port := os.Getenv("LITEBASEDB_PORT")

	s.HttpServer = &http.Server{
		Addr: fmt.Sprintf(":%s", port),
		// ReadTimeout:  30 * time.Second,
		// WriteTimeout: 30 * time.Second,
		// IdleTimeout:  60 * time.Second,
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
			fmt.Println("interrupt")
			if err := s.HttpServer.Shutdown(context.Background()); err != nil {
				log.Printf("HTTP server Shutdown: %v", err)
			}
			<-serverDone
			return
		case syscall.SIGTERM:
			fmt.Println("sigterm")
			return
		}
	}
}

func Server() *ServerInstance {
	return serverInstance
}
