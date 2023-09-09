package app

import (
	"litebasedb/app/actions"
	"litebasedb/app/auth"
	"litebasedb/app/events"
	"litebasedb/app/http"
	"litebasedb/app/node"
	"litebasedb/internal/config"
	"litebasedb/server"
	"time"

	"github.com/joho/godotenv"
)

type App struct {
	server *server.Server
}

var staticApp *App

func NewApp(server *server.Server) *App {
	godotenv.Load(".env")

	app := &App{server}
	auth.KeyManagerInit()
	config.Init()

	auth.SecretsManager().Init()
	events.EventsManager().Init()
	auth.UserManager().Init()
	node.Init()

	auth.Broadcaster(events.EventsManager().Hook())

	staticApp = app

	return app
}

func Container() *App {
	if staticApp == nil {
		panic("App container is not initialized")
	}

	return staticApp
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
	app.server.HttpServer.Handler = http.Router()
	node.Register()
	go app.runTasks()
}

func (app *App) Server() *server.Server {
	return app.server
}
