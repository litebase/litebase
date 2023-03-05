package main

import (
	"context"
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/router/actions"
	"litebasedb/router/auth"
	"litebasedb/router/events"
	"litebasedb/router/http"
	"litebasedb/router/node"
	netHttp "net/http"
	"os"
	"time"
)

type App struct {
	server *netHttp.Server
}

func NewApp() *App {
	app := &App{}

	auth.KeyManagerInit()
	config.Init()

	auth.SecretsManager().Init()
	events.EventsManager().Init()
	node.Init()

	auth.Broadcaster(events.EventsManager().Hook())

	return app
}

func (app *App) runTasks() {
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				actions.RunAutoScaling()
				node.HealthCheck()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (app *App) register() {
	node.Register()
}

func (app *App) Serve() {
	app.server = &netHttp.Server{
		Addr:              fmt.Sprintf(`:%s`, os.Getenv("LITEBASEDB_PORT")),
		IdleTimeout:       0,
		ReadTimeout:       0,
		WriteTimeout:      0,
		ReadHeaderTimeout: 0,
	}

	app.server.Handler = http.Router()
	app.register()
	go app.runTasks()

	err := app.server.ListenAndServe()

	if err != nil {
		panic(err)
	}
}

func (app *App) Shutdown() {
	node.Unregister()
	app.server.Shutdown(context.TODO())
}
