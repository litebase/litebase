package server

import (
	"litebasedb/internal/config"
	"litebasedb/server/auth"
	"litebasedb/server/cluster"
	"litebasedb/server/database"
	"litebasedb/server/events"
	"litebasedb/server/http"
	"litebasedb/server/node"
	"time"

	"github.com/joho/godotenv"
)

type App struct {
	server *ServerInstance
}

var staticApp *App

func NewApp(server *ServerInstance) *App {
	godotenv.Load(".env")

	app := &App{server}
	config.Init()
	cluster.Init()
	auth.KeyManagerInit()

	auth.SecretsManager().Init()
	events.EventsManager().Init()
	auth.UserManager().Init()
	node.Init()
	database.Init()

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
			// actions.RunAutoScaling()
			// node.HealthCheck()
		}
	}()
}

func (app *App) Run() {
	app.server.HttpServer.Handler = http.Router()
	node.Register()
	go app.runTasks()
}

func (app *App) Server() *ServerInstance {
	return app.server
}
