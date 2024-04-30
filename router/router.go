package router

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

type Router struct {
	Cancel     context.CancelFunc
	Context    context.Context
	HttpServer *http.Server
}

func getNodeIpAddress() string {
	nodeIpAddresses := []string{}

	path := fmt.Sprintf("%s/%s", os.Getenv("LITEBASEDB_DATA_PATH"), "/nodes/query")
	os.MkdirAll(path, 0755)
	entries, err := os.ReadDir(path)

	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		parts := strings.Split(entry.Name(), "_")

		if len(parts) != 2 {
			continue
		}

		nodeIpAddresses = append(nodeIpAddresses, fmt.Sprintf("http://%s:%s", parts[0], parts[1]))
	}

	return nodeIpAddresses[0]
}

func NewRouter() *Router {
	godotenv.Load(".env")

	return &Router{}
}

func (router *Router) Start() {
	port := os.Getenv("LITEBASEDB_ROUTER_NODE_PORT")
	ctx := context.Background()
	router.Context, router.Cancel = context.WithCancel(ctx)

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			targetURL, _ := url.Parse(getNodeIpAddress())
			r.URL.Scheme = targetURL.Scheme
			r.URL.Host = targetURL.Host
		},
		ErrorLog: log.New(io.Discard, "", 0),
		ModifyResponse: func(res *http.Response) error {
			return nil
		},
	}

	router.HttpServer = &http.Server{
		Addr: fmt.Sprintf(":%s", port),
		// ReadTimeout: 1 * time.Second,
		// WriteTimeout:      1 * time.Second,
		IdleTimeout: 1 * time.Second,
		// ReadHeaderTimeout: 1 * time.Second,
		Handler: proxy,
	}

	log.Println("Litebase Router running on port", port)

	go func() {
		if err := router.HttpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	signalChannel := make(chan os.Signal, 2)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	for {
		switch <-signalChannel {
		case syscall.SIGTERM:
		case os.Interrupt:
			close(signalChannel)
			router.Shutdown()
			os.Exit(0)
		}
	}
}

func (router *Router) Shutdown() {
	fmt.Println("")

	if err := router.HttpServer.Shutdown(router.Context); err != nil {
		log.Printf("HTTP server Shutdown: %v", err)
	}
}
