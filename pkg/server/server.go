package server

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

type Server struct {
	cancel          context.CancelFunc
	config          *config.Config
	context         context.Context
	HttpServer      *http.Server
	onStarted       func()
	PrivatePort     int // Store the actual assigned private port
	PrivateServeMux *http.ServeMux
	PrivateServer   *http.Server
	ServeMux        *http.ServeMux
}

// findAvailablePort finds an available port starting from a given port number
func findAvailablePort(startPort int) (int, error) {
	if startPort == 0 {
		// Let the system assign a port
		listener, err := net.Listen("tcp", ":0")

		if err != nil {
			return 0, err
		}

		defer func() {
			err := listener.Close()

			if err != nil {
				slog.Error("Failed to close listener", "error", err)
			}
		}()

		addr := listener.Addr().(*net.TCPAddr)

		return addr.Port, nil
	}

	// Try the specified port first, then increment if needed
	for port := startPort; port < startPort+1000; port++ {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

		if err == nil {
			err := listener.Close()

			if err != nil {
				return 0, err
			}

			return port, nil
		}
	}

	return 0, fmt.Errorf("no available port found in range %d-%d", startPort, startPort+999)
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

// GetPrivatePort returns the actual port number assigned to the private server
func (s *Server) GetPrivatePort() int {
	return s.PrivatePort
}

// GetPrivateAddress returns the full address of the private server
func (s *Server) GetPrivateAddress() string {
	return fmt.Sprintf(":%d", s.PrivatePort)
}

// Start the server instance.
func (s *Server) Start(startHook func(*http.ServeMux), shutdownHook func()) {
	s.StartWithPrivateServer(startHook, nil, shutdownHook)
}

// StartWithPrivateServer starts both public and private servers
func (s *Server) StartWithPrivateServer(startHook func(*http.ServeMux), privateStartHook func(*http.ServeMux), shutdownHook func()) {
	port := s.config.Port

	// Parse the private port configuration
	privatePortConfig, err := strconv.Atoi(s.config.PrivatePort)

	if err != nil {
		log.Fatalf("Invalid private port configuration: %v", err)
	}

	// Find an available port for the private server
	privatePort, err := findAvailablePort(privatePortConfig)

	if err != nil {
		log.Fatalf("Failed to find available port for private server: %v", err)
	}

	s.PrivatePort = privatePort // Store the assigned port
	log.Println("Private PORT:", s.PrivatePort)
	tlsCertPath := os.Getenv("LITEBASE_TLS_CERT_PATH")
	tlsKeyPath := os.Getenv("LITEBASE_TLS_KEY_PATH")

	// Setup public server
	s.ServeMux = http.NewServeMux()
	s.HttpServer = &http.Server{
		Addr:              fmt.Sprintf(":%s", port),
		Handler:           s.ServeMux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	// Setup private server
	s.PrivateServeMux = http.NewServeMux()
	s.PrivateServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", privatePort),
		Handler:           s.PrivateServeMux,
		ReadHeaderTimeout: 2 * time.Second,
	}

	log.Printf("Starting public server on port %s", port)
	log.Printf("Starting private server on port %d", privatePort)

	if startHook != nil {
		startHook(s.ServeMux)
	}

	if privateStartHook != nil {
		privateStartHook(s.PrivateServeMux)
	}

	serverDone := make(chan struct{}, 1)
	privateServerDone := make(chan struct{}, 1)
	serverStarted := make(chan struct{}, 1)
	privateServerStarted := make(chan struct{}, 1)

	// Start public server
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
			log.Fatalf("Public server ListenAndServe(): %v", err)
		}
	}()

	// Start private server (no TLS for internal cluster communication)
	go func() {
		defer close(privateServerDone)
		var err error

		listener, err := net.Listen("tcp", s.PrivateServer.Addr)

		if err == nil {
			privateServerStarted <- struct{}{} // Signal that private server has started
			err = s.PrivateServer.Serve(listener)
		}

		if err != http.ErrServerClosed {
			log.Fatalf("Private server ListenAndServe(): %v", err)
		}
	}()

	// Wait for both servers to start
	<-serverStarted
	<-privateServerStarted

	if s.onStarted != nil {
		s.onStarted()
	}

	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	// Wait for a signal to shutdown the server
	sig := <-signalChannel

	fmt.Printf("\n\nLitebase Server received signal %v\n", sig)
	fmt.Printf("Public server was running on port %s\n", port)
	fmt.Printf("Private server was running on port %d\n", privatePort)

	if shutdownHook != nil {
		shutdownHook()
	}

	s.Shutdown(s.context)

	// Wait for both servers to shutdown
	<-serverDone
	<-privateServerDone

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

	// Shutdown both servers
	if s.HttpServer != nil {
		if err := s.HttpServer.Shutdown(ctx); err != nil {
			log.Printf("HTTP server Shutdown: %v", err)
		}
	}

	if s.PrivateServer != nil {
		if err := s.PrivateServer.Shutdown(ctx); err != nil {
			log.Printf("Private server Shutdown: %v", err)
		}
	}
}

// StartWithPrivateRouting starts the server with both public and private servers,
// automatically setting up the private port provider for cluster communication.
// This is the recommended way to start a Litebase server.
func (s *Server) StartWithPrivateRouting(
	publicSetup func(*http.ServeMux, *App),
	privateSetup func(*http.ServeMux, *App),
	shutdownHook func(*App),
) {
	var app *App

	// Set up private port provider BEFORE creating the app
	cluster.SetPrivatePortProvider(func() int {
		return s.GetPrivatePort()
	})

	s.StartWithPrivateServer(
		// Public server setup
		func(publicMux *http.ServeMux) {
			app = NewApp(s.config, publicMux)
			if publicSetup != nil {
				publicSetup(publicMux, app)
			}
		},
		// Private server setup
		func(privateMux *http.ServeMux) {
			if privateSetup != nil && app != nil {
				privateSetup(privateMux, app)
			}
		},
		// Shutdown hook
		func() {
			if shutdownHook != nil && app != nil {
				shutdownHook(app)
			}
		},
	)
}
