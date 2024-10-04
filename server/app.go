package server

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/events"
	"litebase/server/http"
	"litebase/server/node"
	"litebase/server/query"
	"litebase/server/storage"
)

type App struct {
	initialized bool
	Server      *ServerInstance
}

func attempSecretInitialization() bool {
	if config.Get().Env == config.ENV_TEST {
		return true
	}

	return config.Get().NodeType == config.NODE_TYPE_QUERY && node.Node().IsPrimary()
}

func NewApp(server *ServerInstance) *App {
	app := &App{
		Server: server,
	}

	err := config.Init()

	if err != nil {
		panic(err)
	}

	storage.Init(
		node.Node().Address(),
		auth.SecretsManager(),
	)

	if attempSecretInitialization() {
		err := auth.InitSignature()

		if err != nil {
			panic(err)
		}
	}

	_, err = cluster.Init()

	if err != nil {
		panic(err)
	}

	storage.SetDiscoveryProvider(cluster.Get())

	if attempSecretInitialization() {
		err = auth.KeyManagerInit()

		if err != nil {
			panic(err)
		}

		auth.SecretsManager().Init()
		auth.UserManager().Init()
		database.Init()
	}

	node.Init(
		query.NewQueryBuilder(),
		database.NewDatabaseCheckpointer(),
		database.NewDatabaseWalSynchronizer(),
	)
	events.EventsManager().Init()

	auth.Broadcaster(events.EventsManager().Hook())
	storage.SetStorageContext(node.Node().Context())

	app.initialized = true

	return app
}

func (app *App) IsInitialized() bool {
	return app.initialized
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
	http.Router().Server(app.Server.ServeMux)

	go app.runTasks()
}
