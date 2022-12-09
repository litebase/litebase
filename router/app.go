package main

import (
	"fmt"
	"litebasedb/router/auth"
	"litebasedb/router/http"
	netHttp "net/http"
	"os"
	"time"
)

type App struct {
	// intervals map[string]time.Timer
	server *netHttp.Server
}

func NewApp() *App {
	app := &App{}
	auth.SecretsManager().Init()
	auth.KeyManagerInit()

	return app
}

func (app *App) Serve() {
	app.server = &netHttp.Server{
		Addr:        fmt.Sprintf(`:%s`, os.Getenv("LITEBASEDB_PORT")),
		IdleTimeout: 60 * time.Second,
	}

	app.server.Handler = http.Router()

	app.server.ListenAndServe()
}
