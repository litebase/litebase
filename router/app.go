package main

import (
	"fmt"
	"litebasedb/router/actions"
	"litebasedb/router/auth"
	"litebasedb/router/http"
	netHttp "net/http"
	"os"
	"time"
)

type App struct {
	server *netHttp.Server
}

func NewApp() *App {
	app := &App{}
	auth.SecretsManager().Init()
	auth.KeyManagerInit()

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
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

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

	go app.runTasks()

	app.server.ListenAndServe()
}
