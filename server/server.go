package server

import (
	"context"
	"fmt"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/storage"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ServerInstance struct {
	cancel     context.CancelFunc
	context    context.Context
	HttpServer *http.Server
	ServeMux   *http.ServeMux
}

func NewServer() *ServerInstance {
	ctx, cancel := context.WithCancel(context.Background())
	server := &ServerInstance{
		cancel:  cancel,
		context: ctx,
	}

	return server
}

func (s *ServerInstance) Start(serverHook func(*ServerInstance)) {
	// TODO: Add TLS support using autocert or certmagic if this is a query node
	// TODO: Wait until a primary node is elected before starting the server with TLS
	// TODO: Ensure only the primary can renew the TLS certificate
	port := os.Getenv("LITEBASE_PORT")
	tlsCertPath := os.Getenv("LITEBASE_TLS_CERT_PATH")
	tlsKeyPath := os.Getenv("LITEBASE_TLS_KEY_PATH")

	s.ServeMux = http.NewServeMux()

	s.HttpServer = &http.Server{
		Addr: fmt.Sprintf(":%s", port),
		// ReadTimeout:  3 * time.Second,
		// WriteTimeout: 3 * time.Second,
		// IdleTimeout:  60 * time.Second,
		Handler: s.ServeMux,
	}

	log.Println("Litebase Server running on port", port)

	if serverHook != nil {
		serverHook(s)
	}

	err := cluster.Node().Start()

	if err != nil {
		log.Fatalf("Node start: %v", err)
	}

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		var err error

		if tlsCertPath != "" && tlsKeyPath != "" {
			err = s.HttpServer.ListenAndServeTLS(tlsCertPath, tlsKeyPath)
		} else {
			err = s.HttpServer.ListenAndServe()
		}

		if err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	signalChannel := make(chan os.Signal, 2)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	// Wait for a signal to shutdown the server
	sig := <-signalChannel
	log.Println("Received signal", sig)
	cluster.Node().Shutdown()

	s.Shutdown(s.context)
	// Wait for the server to shutdown
	<-serverDone

	os.Exit(0)
}

func (s *ServerInstance) Shutdown(ctx context.Context) {
	fmt.Println("")
	s.cancel()

	// Shutdown all connections
	database.ConnectionManager().Shutdown()

	// Shutdown any storage resources
	storage.Shutdown()

	// Create a context with a timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)

	defer cancel()

	if err := s.HttpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
}
