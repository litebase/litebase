package app

import (
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/event"
	"litebasedb/runtime/app/http"
	"strings"
)

type App struct {
	Config *config.Config
	Router *http.Router
}

var Container *App

func NewApp() *App {
	Container = &App{
		Config: config.NewConfig(),
		Router: &http.Router{},
	}

	return Container
}

func (app *App) Handler(event event.Event) *http.Response {
	return app.Router.Dispatch(PrepareRequest(event))
}

func PrepareRequest(event event.Event) *http.Request {
	headers := map[string]string{}

	for key, value := range event.Server {
		if strings.HasPrefix(key, "HTTP_") {
			headers[strings.ReplaceAll(key, "HTTP_", "")] = value
		}
	}

	return http.NewRequest(headers, event.Method, event.Path)
}
