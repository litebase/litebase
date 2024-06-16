package router

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	// _ "net/http/pprof"
)

type Router struct {
	Cancel     context.CancelFunc
	Context    context.Context
	HttpServer *http.Server
}

func NewRouter() *Router {
	return &Router{}
}

func (router *Router) Start() {
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	port := os.Getenv("LITEBASE_ROUTER_NODE_PORT")
	ctx := context.Background()
	router.Context, router.Cancel = context.WithCancel(ctx)

	router.HttpServer = &http.Server{
		Addr:    fmt.Sprintf(":%s", port),
		Handler: RouterHandler(),
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
