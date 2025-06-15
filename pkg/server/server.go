package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/pkg/storage"
)

type Server struct {
	cancel     context.CancelFunc
	config     *config.Config
	context    context.Context
	HttpServer *http.Server
	ServeMux   *http.ServeMux
}

func NewServer(c *config.Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		cancel:  cancel,
		config:  c,
		context: ctx,
	}

	return server
}

func (s *Server) Start(startHook func(*http.ServeMux), shutdownHook func()) {
	// TODO: Add TLS support using autocert or certmagic if this is a query node
	// TODO: Wait until a primary node is elected before starting the server with TLS
	// TODO: Ensure only the primary can renew the TLS certificate
	port := s.config.Port
	tlsCertPath := os.Getenv("LITEBASE_TLS_CERT_PATH")
	tlsKeyPath := os.Getenv("LITEBASE_TLS_KEY_PATH")

	s.ServeMux = http.NewServeMux()

	s.HttpServer = &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: s.ServeMux,
	}

	if startHook != nil {
		startHook(s.ServeMux)
	}

	log.Println("Litebase Server running on port", port)
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

	if shutdownHook != nil {
		shutdownHook()
	}

	s.Shutdown(s.context)

	// Wait for the server to shutdown
	<-serverDone

	os.Exit(0)
}

func (s *Server) Shutdown(ctx context.Context) {
	fmt.Println("")

	s.cancel()

	// Shutdown any storage resources
	storage.Shutdown(s.config)

	// Create a context with a timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)

	defer cancel()

	if err := s.HttpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
}
