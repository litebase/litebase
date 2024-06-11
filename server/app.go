package server

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/events"
	"litebase/server/http"
	"litebase/server/node"
)

type App struct {
	server *ServerInstance
}

var staticApp *App

func NewApp(server *ServerInstance) *App {
	app := &App{server}
	config.Init()

	_, err := cluster.Init()

	if err != nil {
		panic(err)
	}

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
	// ticker := time.NewTicker(1 * time.Second)

	// go func() {
	// for range ticker.C {
	// actions.RunAutoScaling()
	// node.HealthCheck()
	// }
	// }()
}

func (app *App) Run() {
	// app.server.HttpServer.Handler = http.Router()
	http.Router().Server(app.server.ServeMux)
	node.Register()
	go app.runTasks()
}

func (app *App) Server() *ServerInstance {
	return app.server
}
