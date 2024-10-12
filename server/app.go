package server

import (
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/database"
	"litebase/server/http"
	"litebase/server/query"
	"litebase/server/storage"

	netHttp "net/http"
)

type App struct {
	initialized     bool
	Auth            *auth.Auth
	DatabaseManager *database.DatabaseManager
	Cluster         *cluster.Cluster
	Config          *config.Config
	// Server      *Server
	ServeMux *netHttp.ServeMux
}

func attemptSecretInitialization() bool {
	if config.Get().Env == config.ENV_TEST {
		return true
	}

	return config.Get().NodeType == config.NODE_TYPE_QUERY
}

func NewApp(serveMux *netHttp.ServeMux) *App {
	Auth := auth.NewAuth()

	c, err := cluster.Init(Auth)

	if err != nil {
		panic(err)
	}

	app := &App{
		Auth:            Auth,
		Cluster:         c,
		Config:          config.NewConfig(),
		DatabaseManager: database.NewDatabaseManager(c, Auth.SecretsManager()),
		ServeMux:        serveMux,
	}

	storage.Init(
		app.Cluster.Node().Address(),
		app.Auth.SecretsManager(),
	)

	if attemptSecretInitialization() {
		err := auth.InitSignature()

		if err != nil {
			panic(err)
		}
	}

	storage.SetDiscoveryProvider(app.Cluster)

	if attemptSecretInitialization() {
		err = auth.KeyManagerInit(app.Auth.SecretsManager())

		if err != nil {
			panic(err)
		}

		app.Auth.SecretsManager().Init()
		auth.UserManager().Init()
		database.Init()
	}

	app.Cluster.Node().Init(
		query.NewQueryBuilder(app.Cluster, app.Auth.AccessKeyManager(), app.DatabaseManager),
		database.NewDatabaseWalSynchronizer(app.DatabaseManager),
	)
	app.Cluster.EventsManager().Init()

	auth.Broadcaster(app.Cluster.EventsManager().Hook())
	storage.SetStorageContext(app.Cluster.Node().Context())
	storage.SetStorageTimestamp(app.Cluster.Node().Timestamp())

	app.initialized = true

	return app
}

func (app *App) IsInitialized() bool {
	return app.initialized
}

func (app *App) Run() {
	http.Router().Server(app.Cluster, app.DatabaseManager, app.ServeMux)
}
