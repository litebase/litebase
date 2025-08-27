package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

type Server struct {
	cancel     context.CancelFunc
	config     *config.Config
	context    context.Context
	HttpServer *http.Server
	onStarted  func()
	ServeMux   *http.ServeMux
}

// Create a new Server instance.
func NewServer(c *config.Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		cancel:  cancel,
		config:  c,
		context: ctx,
	}

	return server
}

// OnStarted sets a callback function that is called when the server has
// successfully started and is ready to accept connections.
func (s *Server) OnStarted(f func()) *Server {
	s.onStarted = f

	return s
}

// Start the server instance.
func (s *Server) Start(startHook func(*http.ServeMux), shutdownHook func()) {
	port := s.config.Port
	tlsCertPath := os.Getenv("LITEBASE_TLS_CERT_PATH")
	tlsKeyPath := os.Getenv("LITEBASE_TLS_KEY_PATH")

	s.ServeMux = http.NewServeMux()

	s.HttpServer = &http.Server{
		Addr:              fmt.Sprintf(":%s", port),
		Handler:           s.ServeMux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	if startHook != nil {
		startHook(s.ServeMux)
	}

	serverDone := make(chan struct{}, 1)
	serverStarted := make(chan struct{}, 1)

	go func() {
		defer close(serverDone)
		var err error

		// Create a listener first to detect when server actually starts
		var listener net.Listener

		if tlsCertPath != "" && tlsKeyPath != "" {
			listener, err = net.Listen("tcp", s.HttpServer.Addr)

			if err == nil {
				serverStarted <- struct{}{} // Signal that server has started
				err = s.HttpServer.ServeTLS(listener, tlsCertPath, tlsKeyPath)
			}
		} else {
			listener, err = net.Listen("tcp", s.HttpServer.Addr)

			if err == nil {
				serverStarted <- struct{}{} // Signal that server has started
				err = s.HttpServer.Serve(listener)
			}
		}

		if err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	<-serverStarted

	if s.onStarted != nil {
		s.onStarted()
	}

	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	// Wait for a signal to shutdown the server
	sig := <-signalChannel

	fmt.Println("\n\nLitebase Server received signal", sig)

	if shutdownHook != nil {
		shutdownHook()
	}

	s.Shutdown(s.context)

	// Wait for the server to shutdown
	<-serverDone

	os.Exit(0)
}

// Shutdown the server instance.
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
