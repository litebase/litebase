package app

import (
	"litebasedb/app/actions"
	"litebasedb/app/auth"
	"litebasedb/app/events"
	"litebasedb/app/http"
	"litebasedb/app/node"
	"litebasedb/internal/config"
	netHttp "net/http"
	"time"
)

type App struct {
	server *netHttp.Server
}

func NewApp(server *netHttp.Server) *App {
	app := &App{server}

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

	go func() {
		for range ticker.C {
			actions.RunAutoScaling()
			node.HealthCheck()
		}
	}()
}

func (app *App) Run() {
	app.server.Handler = http.Router()
	node.Register()
	go app.runTasks()
}
